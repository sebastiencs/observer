//! Versioned, CRC32C-protected commit descriptor for one published generation.
//!
//! The descriptor is the publication record. Parquet files are visible to a scan only after this
//! file is durable. Replaying the same sequence range writes the same path.

use std::{
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::layout::{commit_path, hour_end_unix_nano, sync_ancestors, tenant_directory};
use crate::{EventHour, Generation, PROJECTION_VERSION};

const MAGIC: &[u8; 8] = b"OBS-CMT1";
const COMMIT_VERSION: u16 = 1;

/// Where a durability test stops a commit write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitFault {
    /// Fail before creating the temporary descriptor.
    Create,
    /// Fail after the temporary descriptor is written, before `sync_data`.
    SyncFile,
    /// Fail after `sync_data`, before the temporary descriptor is renamed.
    Rename,
    /// Fail after rename, before the directories are fsynced.
    SyncDir,
}

/// Options for one descriptor write.
#[derive(Clone, Debug, Default)]
pub struct CommitWriteOptions {
    /// Stop at this durability boundary and leave the partial descriptor in place.
    pub fault: Option<CommitFault>,
}

/// One hour file recorded by a commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitFile {
    /// UTC hour covered by the file.
    pub hour: EventHour,
    /// Path relative to `tenants/<tenant>`.
    pub relative_path: String,
    /// Rows stored in the file.
    pub rows: u64,
}

/// Durable publication record for one exclusive WAL sequence range.
#[derive(Clone, Debug, PartialEq)]
pub struct Commit {
    /// Tenant that owns the generation.
    pub tenant: String,
    /// [`PROJECTION_VERSION`] at publication.
    pub projection_version: u32,
    /// BLAKE3 fingerprint of [`Commit::schema`].
    pub fingerprint: String,
    /// Union schema of the published generation.
    pub schema: SchemaRef,
    /// Inclusive first WAL sequence.
    pub first_sequence: u64,
    /// Exclusive next WAL sequence.
    pub next_sequence: u64,
    /// Rows across every hour file.
    pub rows: u64,
    /// Hour files in UTC hour order. Empty when the generation stored no rows.
    pub files: Vec<CommitFile>,
    /// Inclusive start of the earliest hour. Absent when [`Commit::files`] is empty.
    pub start_unix_nano: Option<u64>,
    /// Exclusive end of the latest hour. Absent when [`Commit::files`] is empty.
    pub end_unix_nano: Option<u64>,
}

/// Why a commit descriptor could not be written or read.
#[derive(Debug)]
pub enum CommitError {
    /// Filesystem failure.
    Io(io::Error),
    /// The descriptor bytes are not a supported commit.
    Invalid(String),
    /// The CRC32C trailer does not match the body.
    Checksum,
    /// [`CommitWriteOptions::fault`] stopped the write.
    Fault(CommitFault),
}

impl std::fmt::Display for CommitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "commit io: {error}"),
            Self::Invalid(detail) => write!(formatter, "invalid commit: {detail}"),
            Self::Checksum => formatter.write_str("commit checksum mismatch"),
            Self::Fault(step) => write!(formatter, "injected commit fault at {step:?}"),
        }
    }
}

impl std::error::Error for CommitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Invalid(_) | Self::Checksum | Self::Fault(_) => None,
        }
    }
}

impl From<io::Error> for CommitError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Build the descriptor for a generation and the hour files already written for it.
///
/// # Errors
///
/// Returns [`CommitError`] when the sequence range is empty or a file path escapes the tenant
/// directory.
pub fn commit_for(
    root: &Path,
    tenant: &str,
    generation: &Generation,
    files: &[crate::parquet::ParquetFile],
) -> Result<Commit, CommitError> {
    if generation.next_sequence <= generation.first_sequence {
        return Err(CommitError::Invalid(
            "generation sequence range is empty".to_owned(),
        ));
    }
    let tenant_dir = tenant_directory(root, tenant);
    let mut recorded = Vec::with_capacity(files.len());
    let mut rows = 0_u64;
    for file in files {
        let relative = file.path.strip_prefix(&tenant_dir).map_err(|_| {
            CommitError::Invalid(format!(
                "parquet path {} is outside the tenant directory",
                file.path.display()
            ))
        })?;
        let relative_path = relative_path(relative)?;
        rows = rows
            .checked_add(file.rows)
            .ok_or_else(|| CommitError::Invalid("published row count exceeds u64".to_owned()))?;
        recorded.push(CommitFile {
            hour: file.hour,
            relative_path,
            rows: file.rows,
        });
    }
    if rows != generation.rows {
        return Err(CommitError::Invalid(format!(
            "parquet rows {rows} do not match generation rows {}",
            generation.rows
        )));
    }
    let (start_unix_nano, end_unix_nano) = time_bounds(&recorded);
    Ok(Commit {
        tenant: tenant.to_owned(),
        projection_version: PROJECTION_VERSION,
        fingerprint: generation.fingerprint.clone(),
        schema: Arc::clone(&generation.schema),
        first_sequence: generation.first_sequence,
        next_sequence: generation.next_sequence,
        rows,
        files: recorded,
        start_unix_nano,
        end_unix_nano,
    })
}

/// Write `commit` to its deterministic path, replacing any previous descriptor for that range.
///
/// # Errors
///
/// Returns [`CommitError`] when encoding or a durability step fails.
pub fn write_commit(
    root: &Path,
    commit: &Commit,
    options: &CommitWriteOptions,
) -> Result<PathBuf, CommitError> {
    let path = commit_path(
        root,
        &commit.tenant,
        commit.first_sequence,
        commit.next_sequence,
    );
    let directory = path.parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "commit path has no directory")
    })?;
    fs::create_dir_all(directory)?;
    let temporary = path.with_extension("commit.tmp");
    fail(options, CommitFault::Create)?;
    let bytes = encode(commit)?;
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temporary)?;
    let synced = file.try_clone()?;
    let mut writer = io::BufWriter::new(file);
    writer.write_all(&bytes)?;
    writer.flush()?;
    drop(writer);
    fail(options, CommitFault::SyncFile)?;
    synced.sync_data()?;
    fail(options, CommitFault::Rename)?;
    fs::rename(&temporary, &path)?;
    fail(options, CommitFault::SyncDir)?;
    sync_ancestors(directory, root)?;
    Ok(path)
}

/// Read and validate one commit descriptor.
///
/// # Errors
///
/// Returns [`CommitError`] when the file is missing, truncated, or fails its checksum.
pub fn read_commit(path: &Path) -> Result<Commit, CommitError> {
    let bytes = fs::read(path)?;
    decode(&bytes)
}

fn time_bounds(files: &[CommitFile]) -> (Option<u64>, Option<u64>) {
    (
        files.iter().map(|file| file.hour.start_unix_nano()).min(),
        files.iter().map(|file| hour_end_unix_nano(file.hour)).max(),
    )
}

fn relative_path(path: &Path) -> Result<String, CommitError> {
    if !path
        .components()
        .all(|component| matches!(component, Component::Normal(_)))
    {
        return Err(CommitError::Invalid(format!(
            "parquet path {} is not a relative tenant path",
            path.display()
        )));
    }
    path.to_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| CommitError::Invalid("parquet path is not utf-8".to_owned()))
}

fn fail(options: &CommitWriteOptions, step: CommitFault) -> Result<(), CommitError> {
    if options.fault == Some(step) {
        Err(CommitError::Fault(step))
    } else {
        Ok(())
    }
}

fn encode(commit: &Commit) -> Result<Vec<u8>, CommitError> {
    let mut body = Vec::new();
    write_str(&mut body, &commit.tenant);
    body.extend_from_slice(&commit.projection_version.to_le_bytes());
    write_str(&mut body, &commit.fingerprint);
    encode_schema(&mut body, commit.schema.as_ref())?;
    body.extend_from_slice(&commit.first_sequence.to_le_bytes());
    body.extend_from_slice(&commit.next_sequence.to_le_bytes());
    body.extend_from_slice(&commit.rows.to_le_bytes());
    write_option_u64(&mut body, commit.start_unix_nano);
    write_option_u64(&mut body, commit.end_unix_nano);
    let file_count = u32::try_from(commit.files.len())
        .map_err(|_| CommitError::Invalid("too many hour files".to_owned()))?;
    body.extend_from_slice(&file_count.to_le_bytes());
    for file in &commit.files {
        body.extend_from_slice(&file.hour.start_unix_nano().to_le_bytes());
        body.extend_from_slice(&file.rows.to_le_bytes());
        write_str(&mut body, &file.relative_path);
    }
    let mut bytes = Vec::with_capacity(8 + 2 + body.len() + 4);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&COMMIT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&body);
    let crc = crc32c::crc32c(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    Ok(bytes)
}

fn decode(bytes: &[u8]) -> Result<Commit, CommitError> {
    if bytes.len() < MAGIC.len() + 2 + 4 {
        return Err(CommitError::Invalid("descriptor is truncated".to_owned()));
    }
    let (body, trailer) = bytes.split_at(bytes.len() - 4);
    let stored = u32::from_le_bytes(trailer.try_into().expect("crc width"));
    if stored != crc32c::crc32c(body) {
        return Err(CommitError::Checksum);
    }
    if body[0..8] != MAGIC[..] {
        return Err(CommitError::Invalid("unrecognized magic".to_owned()));
    }
    let version = u16::from_le_bytes([body[8], body[9]]);
    if version != COMMIT_VERSION {
        return Err(CommitError::Invalid(format!(
            "unsupported commit version {version}"
        )));
    }
    let mut reader = Reader {
        data: &body[10..],
        pos: 0,
    };
    let tenant = reader.string()?;
    let projection_version = reader.u32()?;
    let fingerprint = reader.string()?;
    let schema = reader.schema()?;
    let first_sequence = reader.u64()?;
    let next_sequence = reader.u64()?;
    let rows = reader.u64()?;
    let start_unix_nano = reader.option_u64()?;
    let end_unix_nano = reader.option_u64()?;
    let file_count = reader.u32()?;
    let mut files = Vec::with_capacity(usize::try_from(file_count).unwrap_or(0));
    for _ in 0..file_count {
        let hour = EventHour::containing(reader.u64()?);
        let file_rows = reader.u64()?;
        let relative_path = reader.string()?;
        if Path::new(&relative_path)
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(CommitError::Invalid(
                "commit file path is not relative".to_owned(),
            ));
        }
        files.push(CommitFile {
            hour,
            relative_path,
            rows: file_rows,
        });
    }
    if reader.data.len() != reader.pos {
        return Err(CommitError::Invalid(
            "descriptor has trailing bytes".to_owned(),
        ));
    }
    if next_sequence <= first_sequence {
        return Err(CommitError::Invalid(
            "commit sequence range is empty".to_owned(),
        ));
    }
    let (expected_start, expected_end) = time_bounds(&files);
    if start_unix_nano != expected_start || end_unix_nano != expected_end {
        return Err(CommitError::Invalid(
            "commit time bounds do not match its files".to_owned(),
        ));
    }
    let summed = files.iter().try_fold(0_u64, |total, file| {
        total
            .checked_add(file.rows)
            .ok_or_else(|| CommitError::Invalid("published row count exceeds u64".to_owned()))
    })?;
    if summed != rows {
        return Err(CommitError::Invalid(
            "commit row count does not match its files".to_owned(),
        ));
    }
    Ok(Commit {
        tenant,
        projection_version,
        fingerprint,
        schema,
        first_sequence,
        next_sequence,
        rows,
        files,
        start_unix_nano,
        end_unix_nano,
    })
}

fn encode_schema(body: &mut Vec<u8>, schema: &Schema) -> Result<(), CommitError> {
    write_metadata(body, schema.metadata())?;
    let field_count = u32::try_from(schema.fields().len())
        .map_err(|_| CommitError::Invalid("too many schema fields".to_owned()))?;
    body.extend_from_slice(&field_count.to_le_bytes());
    for field in schema.fields() {
        write_str(body, field.name());
        body.push(u8::from(field.is_nullable()));
        encode_type(body, field.data_type())?;
        write_metadata(body, field.metadata())?;
    }
    Ok(())
}

fn encode_type(body: &mut Vec<u8>, data_type: &DataType) -> Result<(), CommitError> {
    let tag = match data_type {
        DataType::Boolean => 1,
        DataType::Int32 => 2,
        DataType::Int64 => 3,
        DataType::UInt16 => 4,
        DataType::UInt32 => 5,
        DataType::UInt64 => 6,
        DataType::Float64 => 7,
        DataType::Binary => 8,
        DataType::Utf8 => 9,
        DataType::FixedSizeBinary(_) => 10,
        other => {
            return Err(CommitError::Invalid(format!(
                "unsupported arrow type {other}"
            )));
        }
    };
    body.push(tag);
    if let DataType::FixedSizeBinary(length) = data_type {
        body.extend_from_slice(&length.to_le_bytes());
    }
    Ok(())
}

fn write_metadata(
    body: &mut Vec<u8>,
    metadata: &arrow_schema::Metadata,
) -> Result<(), CommitError> {
    let count = u32::try_from(metadata.len())
        .map_err(|_| CommitError::Invalid("too much schema metadata".to_owned()))?;
    body.extend_from_slice(&count.to_le_bytes());
    for (key, value) in metadata.iter() {
        write_str(body, key);
        write_str(body, value);
    }
    Ok(())
}

fn write_option_u64(body: &mut Vec<u8>, value: Option<u64>) {
    match value {
        None => body.push(0),
        Some(value) => {
            body.push(1);
            body.extend_from_slice(&value.to_le_bytes());
        }
    }
}

fn write_str(body: &mut Vec<u8>, value: &str) {
    let length = u32::try_from(value.len()).expect("commit string length");
    body.extend_from_slice(&length.to_le_bytes());
    body.extend_from_slice(value.as_bytes());
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
    fn take(&mut self, length: usize) -> Result<&[u8], CommitError> {
        let end = self
            .pos
            .checked_add(length)
            .ok_or_else(|| CommitError::Invalid("descriptor is truncated".to_owned()))?;
        if end > self.data.len() {
            return Err(CommitError::Invalid("descriptor is truncated".to_owned()));
        }
        let start = self.pos;
        self.pos = end;
        Ok(&self.data[start..end])
    }

    fn u8(&mut self) -> Result<u8, CommitError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, CommitError> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes(bytes.try_into().expect("u32 width")))
    }

    fn u64(&mut self) -> Result<u64, CommitError> {
        let bytes = self.take(8)?;
        Ok(u64::from_le_bytes(bytes.try_into().expect("u64 width")))
    }

    fn string(&mut self) -> Result<String, CommitError> {
        let length = usize::try_from(self.u32()?)
            .map_err(|_| CommitError::Invalid("string length exceeds usize".to_owned()))?;
        let bytes = self.take(length)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| CommitError::Invalid("string is not utf-8".to_owned()))
    }

    fn option_u64(&mut self) -> Result<Option<u64>, CommitError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.u64()?)),
            _ => Err(CommitError::Invalid(
                "optional integer tag is invalid".to_owned(),
            )),
        }
    }

    fn metadata(&mut self) -> Result<std::collections::HashMap<String, String>, CommitError> {
        let count = self.u32()?;
        let mut metadata =
            std::collections::HashMap::with_capacity(usize::try_from(count).unwrap_or(0));
        for _ in 0..count {
            let key = self.string()?;
            let value = self.string()?;
            metadata.insert(key, value);
        }
        Ok(metadata)
    }

    fn schema(&mut self) -> Result<SchemaRef, CommitError> {
        let metadata = self.metadata()?;
        let count = self.u32()?;
        let mut fields = Vec::with_capacity(usize::try_from(count).unwrap_or(0));
        for _ in 0..count {
            let name = self.string()?;
            let nullable = match self.u8()? {
                0 => false,
                1 => true,
                _ => {
                    return Err(CommitError::Invalid(
                        "nullability flag is invalid".to_owned(),
                    ));
                }
            };
            let data_type = self.data_type()?;
            let field_metadata = self.metadata()?;
            fields.push(Field::new(name, data_type, nullable).with_metadata(field_metadata));
        }
        Ok(Arc::new(Schema::new(fields).with_metadata(metadata)))
    }

    fn data_type(&mut self) -> Result<DataType, CommitError> {
        Ok(match self.u8()? {
            1 => DataType::Boolean,
            2 => DataType::Int32,
            3 => DataType::Int64,
            4 => DataType::UInt16,
            5 => DataType::UInt32,
            6 => DataType::UInt64,
            7 => DataType::Float64,
            8 => DataType::Binary,
            9 => DataType::Utf8,
            10 => DataType::FixedSizeBinary(self.i32()?),
            tag => {
                return Err(CommitError::Invalid(format!(
                    "unsupported arrow type tag {tag}"
                )));
            }
        })
    }

    fn i32(&mut self) -> Result<i32, CommitError> {
        Ok(i32::from_le_bytes(self.u32()?.to_le_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::{Commit, CommitFile, decode, encode};
    use crate::{EventHour, core_logs_schema};
    use arrow_schema::{DataType, Field, Schema};
    use std::{collections::HashMap, sync::Arc};

    #[test]
    fn descriptor_round_trips_schema_metadata_and_rejects_a_bad_checksum() {
        let mut metadata = HashMap::new();
        metadata.insert("observer.attribute.path".to_owned(), r#"["a"]"#.to_owned());
        let schema = Arc::new(Schema::new(vec![
            core_logs_schema().fields()[0].as_ref().clone(),
            Field::new("log_a_i64", DataType::Int64, true).with_metadata(metadata),
        ]));
        let commit = Commit {
            tenant: "tenant-a".to_owned(),
            projection_version: 1,
            fingerprint: "abc".to_owned(),
            schema,
            first_sequence: 4,
            next_sequence: 6,
            rows: 2,
            files: vec![CommitFile {
                hour: EventHour::containing(0),
                relative_path: "date=1970-01-01/hour=00/4-6.parquet".to_owned(),
                rows: 2,
            }],
            start_unix_nano: Some(0),
            end_unix_nano: Some(3_600_000_000_000),
        };
        let bytes = encode(&commit).expect("encode");
        let decoded = decode(&bytes).expect("decode");
        assert_eq!(decoded, commit);
        let mut corrupt = bytes.clone();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0x01;
        assert!(decode(&corrupt).is_err());
    }
}
