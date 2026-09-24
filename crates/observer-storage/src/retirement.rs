//! CRC32C retirement descriptor for one published generation.
//!
//! The descriptor hides listed Parquet files from new scans. It does not delete those files or
//! change the publication commit.

use std::{
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Component, Path, PathBuf},
};

use crate::layout::{retirement_path, sync_ancestors};

const MAGIC: &[u8; 8] = b"OBS-RET1";
const RETIREMENT_VERSION: u16 = 1;

/// Where a durability test stops a retirement write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetirementFault {
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
pub struct RetirementWriteOptions {
    /// Stop at this durability boundary and leave the partial descriptor in place.
    pub fault: Option<RetirementFault>,
}

/// One hour file hidden by a retirement descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetiredFile {
    /// Path relative to `tenants/<tenant>`.
    pub relative_path: String,
    /// Rows recorded for that file by the publication commit.
    pub rows: u64,
}

/// Durable record of files no longer visible from one exclusive WAL sequence range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Retirement {
    /// Tenant that owns the generation.
    pub tenant: String,
    /// Inclusive first WAL sequence of the published generation.
    pub first_sequence: u64,
    /// Exclusive next WAL sequence of the published generation.
    pub next_sequence: u64,
    /// Files were eligible because their maximum receive time was below this instant.
    pub received_before_unix_nano: u64,
    /// Hour files hidden from new scans.
    pub files: Vec<RetiredFile>,
}

/// Why a retirement descriptor could not be written or read.
#[derive(Debug)]
pub enum RetirementError {
    /// Filesystem failure.
    Io(io::Error),
    /// The descriptor bytes are not a supported retirement record.
    Invalid(String),
    /// The CRC32C trailer does not match the body.
    Checksum,
    /// [`RetirementWriteOptions::fault`] stopped the write.
    Fault(RetirementFault),
}

impl std::fmt::Display for RetirementError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "retirement io: {error}"),
            Self::Invalid(detail) => write!(formatter, "invalid retirement: {detail}"),
            Self::Checksum => formatter.write_str("retirement checksum mismatch"),
            Self::Fault(step) => write!(formatter, "injected retirement fault at {step:?}"),
        }
    }
}

impl std::error::Error for RetirementError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Invalid(_) | Self::Checksum | Self::Fault(_) => None,
        }
    }
}

impl From<io::Error> for RetirementError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Write `retirement` to its deterministic path, replacing any previous descriptor for that range.
///
/// # Errors
///
/// Returns [`RetirementError`] when the range or a path is invalid, or a durability step fails.
pub fn write_retirement(
    root: &Path,
    retirement: &Retirement,
    options: &RetirementWriteOptions,
) -> Result<PathBuf, RetirementError> {
    validate(retirement)?;
    let path = retirement_path(
        root,
        &retirement.tenant,
        retirement.first_sequence,
        retirement.next_sequence,
    );
    let directory = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "retirement path has no directory",
        )
    })?;
    fs::create_dir_all(directory)?;
    let temporary = path.with_extension("retire.tmp");
    fail(options, RetirementFault::Create)?;
    let bytes = encode(retirement)?;
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
    fail(options, RetirementFault::SyncFile)?;
    synced.sync_data()?;
    fail(options, RetirementFault::Rename)?;
    fs::rename(&temporary, &path)?;
    fail(options, RetirementFault::SyncDir)?;
    sync_ancestors(directory, root)?;
    Ok(path)
}

/// Read and validate one retirement descriptor.
///
/// # Errors
///
/// Returns [`RetirementError`] when the file is missing, truncated, or fails its checksum.
pub fn read_retirement(path: &Path) -> Result<Retirement, RetirementError> {
    decode(&fs::read(path)?)
}

fn validate(retirement: &Retirement) -> Result<(), RetirementError> {
    if retirement.next_sequence <= retirement.first_sequence {
        return Err(RetirementError::Invalid(
            "retirement sequence range is empty".to_owned(),
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    for file in &retirement.files {
        if !seen.insert(file.relative_path.as_str()) {
            return Err(RetirementError::Invalid(format!(
                "retirement lists {} more than once",
                file.relative_path
            )));
        }
        if !Path::new(&file.relative_path)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
        {
            return Err(RetirementError::Invalid(format!(
                "retirement path {} is not relative",
                file.relative_path
            )));
        }
    }
    Ok(())
}

fn fail(options: &RetirementWriteOptions, step: RetirementFault) -> Result<(), RetirementError> {
    if options.fault == Some(step) {
        Err(RetirementError::Fault(step))
    } else {
        Ok(())
    }
}

fn encode(retirement: &Retirement) -> Result<Vec<u8>, RetirementError> {
    validate(retirement)?;
    let mut body = Vec::new();
    write_str(&mut body, &retirement.tenant)?;
    body.extend_from_slice(&retirement.first_sequence.to_le_bytes());
    body.extend_from_slice(&retirement.next_sequence.to_le_bytes());
    body.extend_from_slice(&retirement.received_before_unix_nano.to_le_bytes());
    let count = u32::try_from(retirement.files.len())
        .map_err(|_| RetirementError::Invalid("too many retired files".to_owned()))?;
    body.extend_from_slice(&count.to_le_bytes());
    for file in &retirement.files {
        write_str(&mut body, &file.relative_path)?;
        body.extend_from_slice(&file.rows.to_le_bytes());
    }
    let mut bytes = Vec::with_capacity(8 + 2 + body.len() + 4);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&RETIREMENT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&body);
    let crc = crc32c::crc32c(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    Ok(bytes)
}

fn decode(bytes: &[u8]) -> Result<Retirement, RetirementError> {
    if bytes.len() < MAGIC.len() + 2 + 4 {
        return Err(RetirementError::Invalid(
            "descriptor is truncated".to_owned(),
        ));
    }
    let (body, trailer) = bytes.split_at(bytes.len() - 4);
    let stored = u32::from_le_bytes(trailer.try_into().expect("crc width"));
    if stored != crc32c::crc32c(body) {
        return Err(RetirementError::Checksum);
    }
    if body[0..8] != MAGIC[..] {
        return Err(RetirementError::Invalid("unrecognized magic".to_owned()));
    }
    let version = u16::from_le_bytes([body[8], body[9]]);
    if version != RETIREMENT_VERSION {
        return Err(RetirementError::Invalid(format!(
            "unsupported retirement version {version}"
        )));
    }
    let mut reader = Reader {
        data: &body[10..],
        pos: 0,
    };
    let retirement = Retirement {
        tenant: reader.string()?,
        first_sequence: reader.u64()?,
        next_sequence: reader.u64()?,
        received_before_unix_nano: reader.u64()?,
        files: {
            let count = reader.u32()?;
            let mut files = Vec::with_capacity(usize::try_from(count).unwrap_or(0));
            for _ in 0..count {
                files.push(RetiredFile {
                    relative_path: reader.string()?,
                    rows: reader.u64()?,
                });
            }
            files
        },
    };
    validate(&retirement)?;
    Ok(retirement)
}

fn write_str(body: &mut Vec<u8>, value: &str) -> Result<(), RetirementError> {
    let length = u32::try_from(value.len())
        .map_err(|_| RetirementError::Invalid("string is too long".to_owned()))?;
    body.extend_from_slice(&length.to_le_bytes());
    body.extend_from_slice(value.as_bytes());
    Ok(())
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
    fn take(&mut self, length: usize) -> Result<&[u8], RetirementError> {
        let end = self
            .pos
            .checked_add(length)
            .ok_or_else(|| RetirementError::Invalid("descriptor is truncated".to_owned()))?;
        if end > self.data.len() {
            return Err(RetirementError::Invalid(
                "descriptor is truncated".to_owned(),
            ));
        }
        let start = self.pos;
        self.pos = end;
        Ok(&self.data[start..end])
    }

    fn u32(&mut self) -> Result<u32, RetirementError> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes(bytes.try_into().expect("u32 width")))
    }

    fn u64(&mut self) -> Result<u64, RetirementError> {
        let bytes = self.take(8)?;
        Ok(u64::from_le_bytes(bytes.try_into().expect("u64 width")))
    }

    fn string(&mut self) -> Result<String, RetirementError> {
        let length = usize::try_from(self.u32()?)
            .map_err(|_| RetirementError::Invalid("string length exceeds usize".to_owned()))?;
        let bytes = self.take(length)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| RetirementError::Invalid("string is not utf-8".to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        RetiredFile, Retirement, RetirementFault, RetirementWriteOptions, decode, encode,
        write_retirement,
    };
    use crate::retirement_path;

    fn sample() -> Retirement {
        Retirement {
            tenant: "tenant-a".to_owned(),
            first_sequence: 0,
            next_sequence: 2,
            received_before_unix_nano: 50,
            files: vec![RetiredFile {
                relative_path: "date=1970-01-01/hour=00/0-2.parquet".to_owned(),
                rows: 2,
            }],
        }
    }

    #[test]
    fn descriptor_round_trips_and_rejects_a_bad_checksum() {
        let retirement = sample();
        let bytes = encode(&retirement).expect("encode");
        assert_eq!(decode(&bytes).expect("decode"), retirement);
        let mut corrupt = bytes;
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0x01;
        assert!(decode(&corrupt).is_err());
    }

    #[test]
    fn a_crash_before_rename_leaves_no_durable_descriptor() {
        let directory = tempfile::tempdir().expect("tempdir");
        let error = write_retirement(
            directory.path(),
            &sample(),
            &RetirementWriteOptions {
                fault: Some(RetirementFault::Rename),
            },
        )
        .expect_err("fault");
        assert!(matches!(
            error,
            super::RetirementError::Fault(RetirementFault::Rename)
        ));
        let path = retirement_path(directory.path(), "tenant-a", 0, 2);
        assert!(!path.exists());
        assert!(path.with_extension("retire.tmp").exists());
    }

    #[test]
    fn an_empty_range_or_a_bad_path_is_rejected() {
        let mut retirement = sample();
        retirement.next_sequence = retirement.first_sequence;
        assert!(encode(&retirement).is_err());
        retirement.next_sequence = 2;
        retirement.files[0].relative_path = "../outside.parquet".to_owned();
        assert!(encode(&retirement).is_err());
    }
}
