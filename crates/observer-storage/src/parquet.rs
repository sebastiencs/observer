//! Durable local Parquet files for one frozen generation.
//!
//! Each hour partition becomes one Snappy file. Batches are aligned to the generation schema
//! before they are written, so missing dynamic columns are typed nulls. The file is flushed and
//! `sync_data`'d, renamed into place, and then every directory down from the hour directory to the
//! data root is fsynced. Nothing here checkpoints the WAL.

use std::{
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::layout::{hour_end_unix_nano, sync_ancestors};
use crate::{EventHour, Generation, MemtableError, PROJECTION_VERSION, align_batch, parquet_path};

/// Arrow schema metadata key for [`PROJECTION_VERSION`].
pub const META_PROJECTION_VERSION: &str = "observer.projection_version";

/// Arrow schema metadata key for the generation schema fingerprint.
pub const META_FINGERPRINT: &str = "observer.schema_fingerprint";

/// Arrow schema metadata key for the inclusive first WAL sequence.
pub const META_WAL_FIRST_SEQUENCE: &str = "observer.wal_first_sequence";

/// Arrow schema metadata key for the exclusive next WAL sequence.
pub const META_WAL_NEXT_SEQUENCE: &str = "observer.wal_next_sequence";

/// Arrow schema metadata key for the number of rows in this hour file.
pub const META_ROW_COUNT: &str = "observer.row_count";

/// Arrow schema metadata key for the inclusive UTC hour start.
pub const META_HOUR_START_UNIX_NANO: &str = "observer.hour_start_unix_nano";

/// Arrow schema metadata key for the exclusive UTC hour end.
pub const META_HOUR_END_UNIX_NANO: &str = "observer.hour_end_unix_nano";

/// Where a durability test stops a write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParquetFault {
    /// Fail before creating the temporary file.
    Create,
    /// Fail after the temporary file is written, before `sync_data`.
    SyncFile,
    /// Fail after `sync_data`, before the temporary file is renamed.
    Rename,
    /// Fail after rename, before the directories are fsynced.
    SyncDir,
}

/// Options for one generation write.
#[derive(Clone, Debug, Default)]
pub struct ParquetWriteOptions {
    /// Stop at this durability boundary and leave the partial file in place.
    pub fault: Option<ParquetFault>,
}

/// One hour file written for a generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParquetFile {
    /// UTC hour covered by the file.
    pub hour: EventHour,
    /// Final Parquet path.
    pub path: PathBuf,
    /// Rows stored in the file.
    pub rows: u64,
}

/// Why a generation could not be written or read.
#[derive(Debug)]
pub enum ParquetError {
    /// Filesystem failure.
    Io(io::Error),
    /// Parquet or Arrow rejected the file.
    Storage(String),
    /// A batch could not be aligned to the generation schema.
    Align(MemtableError),
    /// [`ParquetWriteOptions::fault`] stopped the write.
    Fault(ParquetFault),
}

impl std::fmt::Display for ParquetError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "parquet io: {error}"),
            Self::Storage(detail) => write!(formatter, "parquet storage: {detail}"),
            Self::Align(error) => write!(formatter, "parquet align: {error}"),
            Self::Fault(step) => write!(formatter, "injected parquet fault at {step:?}"),
        }
    }
}

impl std::error::Error for ParquetError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Align(error) => Some(error),
            Self::Storage(_) | Self::Fault(_) => None,
        }
    }
}

impl From<io::Error> for ParquetError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Write every non-empty hour partition of `generation` under `root`.
///
/// An empty generation writes no files. Writing the same generation again replaces each hour file.
///
/// # Errors
///
/// Returns [`ParquetError`] when alignment, encoding, or a durability step fails. A fault leaves
/// the temporary or renamed file from that step on disk and does not write later hours.
pub fn write_generation(
    root: &Path,
    tenant: &str,
    generation: &Generation,
    options: &ParquetWriteOptions,
) -> Result<Vec<ParquetFile>, ParquetError> {
    if generation
        .partitions
        .iter()
        .any(|partition| partition.batches.iter().any(|batch| batch.num_rows() > 0))
        && generation.next_sequence <= generation.first_sequence
    {
        return Err(ParquetError::Storage(
            "generation sequence range is empty".to_owned(),
        ));
    }
    let mut written = Vec::new();
    for partition in &generation.partitions {
        let rows = row_count(&partition.batches)?;
        if rows == 0 {
            continue;
        }
        let path = parquet_path(
            root,
            tenant,
            partition.hour,
            generation.first_sequence,
            generation.next_sequence,
        );
        let schema = file_schema(generation, partition.hour, rows);
        let batches = partition
            .batches
            .iter()
            .map(|batch| align_batch(batch, Arc::clone(&schema)).map_err(ParquetError::Align))
            .collect::<Result<Vec<_>, _>>()?;
        write_hour(root, &path, &schema, &batches, options)?;
        written.push(ParquetFile {
            hour: partition.hour,
            path,
            rows,
        });
    }
    Ok(written)
}

/// Arrow schema stored in a Parquet file written by [`write_generation`].
///
/// # Errors
///
/// Returns [`ParquetError`] when the file cannot be opened or decoded.
pub fn read_parquet_schema(path: &Path) -> Result<SchemaRef, ParquetError> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| ParquetError::Storage(error.to_string()))?;
    Ok(Arc::clone(builder.schema()))
}

/// Read every row group of a Parquet file written by [`write_generation`].
///
/// # Errors
///
/// Returns [`ParquetError`] when the file cannot be opened or decoded.
pub fn read_parquet_batches(path: &Path) -> Result<Vec<RecordBatch>, ParquetError> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| ParquetError::Storage(error.to_string()))?;
    // The row reader drops schema metadata. The builder keeps the Arrow schema stored in the file.
    let schema = Arc::clone(builder.schema());
    let reader = builder
        .build()
        .map_err(|error| ParquetError::Storage(error.to_string()))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| ParquetError::Storage(error.to_string()))?
        .into_iter()
        .map(|batch| {
            RecordBatch::try_new(Arc::clone(&schema), batch.columns().to_vec())
                .map_err(|error| ParquetError::Storage(error.to_string()))
        })
        .collect()
}

fn file_schema(generation: &Generation, hour: EventHour, rows: u64) -> SchemaRef {
    let mut metadata = generation.schema.metadata().clone();
    metadata.insert(
        META_PROJECTION_VERSION.to_owned(),
        PROJECTION_VERSION.to_string(),
    );
    metadata.insert(META_FINGERPRINT.to_owned(), generation.fingerprint.clone());
    metadata.insert(
        META_WAL_FIRST_SEQUENCE.to_owned(),
        generation.first_sequence.to_string(),
    );
    metadata.insert(
        META_WAL_NEXT_SEQUENCE.to_owned(),
        generation.next_sequence.to_string(),
    );
    metadata.insert(META_ROW_COUNT.to_owned(), rows.to_string());
    metadata.insert(
        META_HOUR_START_UNIX_NANO.to_owned(),
        hour.start_unix_nano().to_string(),
    );
    metadata.insert(
        META_HOUR_END_UNIX_NANO.to_owned(),
        hour_end_unix_nano(hour).to_string(),
    );
    Arc::new(Schema::clone(generation.schema.as_ref()).with_metadata(metadata))
}

fn row_count(batches: &[Arc<RecordBatch>]) -> Result<u64, ParquetError> {
    batches.iter().try_fold(0_u64, |total, batch| {
        let rows = u64::try_from(batch.num_rows())
            .map_err(|_| ParquetError::Storage("row count exceeds u64".to_owned()))?;
        total
            .checked_add(rows)
            .ok_or_else(|| ParquetError::Storage("row count exceeds u64".to_owned()))
    })
}

fn write_hour(
    root: &Path,
    path: &Path,
    schema: &SchemaRef,
    batches: &[RecordBatch],
    options: &ParquetWriteOptions,
) -> Result<(), ParquetError> {
    let directory = path.parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "parquet path has no directory")
    })?;
    fs::create_dir_all(directory)?;
    let temporary = path.with_extension("parquet.tmp");
    fail(options, ParquetFault::Create)?;
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temporary)?;
    let synced = file.try_clone()?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, Arc::clone(schema), Some(properties))
        .map_err(|error| ParquetError::Storage(error.to_string()))?;
    for batch in batches {
        writer
            .write(batch)
            .map_err(|error| ParquetError::Storage(error.to_string()))?;
    }
    writer
        .close()
        .map_err(|error| ParquetError::Storage(error.to_string()))?;
    fail(options, ParquetFault::SyncFile)?;
    synced.sync_data()?;
    fail(options, ParquetFault::Rename)?;
    fs::rename(&temporary, path)?;
    fail(options, ParquetFault::SyncDir)?;
    sync_ancestors(directory, root)?;
    Ok(())
}

fn fail(options: &ParquetWriteOptions, step: ParquetFault) -> Result<(), ParquetError> {
    if options.fault == Some(step) {
        Err(ParquetError::Fault(step))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ParquetError, ParquetFault, ParquetWriteOptions, read_parquet_batches, write_generation,
    };
    use crate::{
        COLUMN_BODY, COLUMN_WAL_SEQUENCE, DynamicLimits, EventHour, META_FINGERPRINT,
        META_HOUR_END_UNIX_NANO, META_HOUR_START_UNIX_NANO, META_PROJECTION_VERSION,
        META_ROW_COUNT, META_WAL_FIRST_SEQUENCE, META_WAL_NEXT_SEQUENCE, ManualClock, Memtable,
        MemtableConfig, PROJECTION_VERSION, decode_logs_frame, hour_directory, parquet_file_name,
        parquet_path,
    };
    use arrow_array::{Array, Int64Array, StringArray, UInt64Array};
    use bytes::Bytes;
    use observer_protocol::otlp::{
        AnyValue, ExportLogsServiceRequest, KeyValue, LogRecord, ResourceLogs, ScopeLogs, any_value,
    };
    use observer_wal::{Frame, FrameSignal};
    use prost::Message;
    use std::{path::Path, sync::Arc, time::Duration};

    const HOUR: u64 = 3_600_000_000_000;

    fn config() -> MemtableConfig {
        MemtableConfig {
            max_rows: 100,
            max_bytes: u64::MAX,
            max_age: Duration::from_secs(60),
            max_frozen: 2,
            max_dynamic_columns: 32,
        }
    }

    fn attribute(key: &str, value: i64) -> KeyValue {
        KeyValue {
            key: key.to_owned(),
            value: Some(AnyValue {
                value: Some(any_value::Value::IntValue(value)),
            }),
            ..Default::default()
        }
    }

    fn logs(sequence: u64, time: u64, attributes: Vec<KeyValue>) -> crate::DecodedLogs {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: time,
                        attributes,
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(format!("row-{time}"))),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        decode_logs_frame(
            &Frame {
                sequence,
                signal: FrameSignal::Logs,
                received_at_unix_nanos: 1,
                tenant_id: "tenant-a".to_owned(),
                payload: Bytes::from(request.encode_to_vec()),
            },
            DynamicLimits {
                max_depth: 4,
                max_columns: 32,
            },
        )
        .expect("decode")
    }

    fn empty_logs(sequence: u64) -> crate::DecodedLogs {
        let request = ExportLogsServiceRequest::default();
        decode_logs_frame(
            &Frame {
                sequence,
                signal: FrameSignal::Logs,
                received_at_unix_nanos: 1,
                tenant_id: "tenant-a".to_owned(),
                payload: Bytes::from(request.encode_to_vec()),
            },
            DynamicLimits {
                max_depth: 4,
                max_columns: 32,
            },
        )
        .expect("decode empty")
    }

    fn seal(frames: &[crate::DecodedLogs]) -> crate::Generation {
        let clock = ManualClock::new(0);
        let mut table = Memtable::new("tenant-a", config(), Arc::new(clock)).expect("memtable");
        for frame in frames {
            table.append(frame).expect("append");
        }
        table.rotate().expect("rotate").expect("sealed");
        table.release_oldest_frozen().expect("frozen")
    }

    fn string_values(batches: &[arrow_array::RecordBatch], name: &str) -> Vec<Option<String>> {
        batches
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column_by_name(name)
                    .unwrap_or_else(|| panic!("missing {name}"))
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap_or_else(|| panic!("{name} is not utf8"));
                (0..column.len())
                    .map(|row| (!column.is_null(row)).then(|| column.value(row).to_owned()))
            })
            .collect()
    }

    fn i64_values(batches: &[arrow_array::RecordBatch], name: &str) -> Vec<Option<i64>> {
        batches
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column_by_name(name)
                    .unwrap_or_else(|| panic!("missing {name}"))
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap_or_else(|| panic!("{name} is not i64"));
                (0..column.len()).map(|row| (!column.is_null(row)).then(|| column.value(row)))
            })
            .collect()
    }

    #[test]
    fn hour_paths_use_the_sequence_range() {
        let hour = EventHour::containing(0);
        let root = Path::new("/data");
        assert_eq!(
            hour_directory(root, "tenant-a", hour),
            Path::new("/data/tenants/tenant-a/date=1970-01-01/hour=00")
        );
        assert_eq!(parquet_file_name(4, 9), "4-9.parquet");
        assert_eq!(
            parquet_path(root, "tenant-a", hour, 4, 9),
            Path::new("/data/tenants/tenant-a/date=1970-01-01/hour=00/4-9.parquet")
        );
    }

    #[test]
    fn empty_generation_writes_no_files() {
        let directory = tempfile::tempdir().expect("tempdir");
        let generation = seal(&[empty_logs(3)]);
        let written = write_generation(
            directory.path(),
            "tenant-a",
            &generation,
            &ParquetWriteOptions::default(),
        )
        .expect("write");
        assert!(written.is_empty());
        assert!(!directory.path().join("tenants").exists());
    }

    #[test]
    fn round_trip_preserves_order_schema_and_hour_files() {
        let directory = tempfile::tempdir().expect("tempdir");
        let generation = seal(&[
            logs(0, 0, vec![attribute("a", 1)]),
            logs(1, 1, vec![attribute("b", 2)]),
            logs(2, HOUR, vec![attribute("a", 3)]),
        ]);
        let written = write_generation(
            directory.path(),
            "tenant-a",
            &generation,
            &ParquetWriteOptions::default(),
        )
        .expect("write");
        assert_eq!(written.len(), 2);
        assert_eq!(written[0].rows, 2);
        assert_eq!(written[1].rows, 1);
        assert_eq!(
            written[0].path,
            parquet_path(directory.path(), "tenant-a", EventHour::containing(0), 0, 3)
        );
        assert_eq!(
            written[1].path,
            parquet_path(
                directory.path(),
                "tenant-a",
                EventHour::containing(HOUR),
                0,
                3
            )
        );

        let first = read_parquet_batches(&written[0].path).expect("read");
        assert_eq!(
            string_values(&first, COLUMN_BODY),
            vec![Some("row-0".to_owned()), Some("row-1".to_owned())]
        );
        let sequences: Vec<u64> = first
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column_by_name(COLUMN_WAL_SEQUENCE)
                    .expect("sequence")
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("u64");
                (0..column.len()).map(|row| column.value(row))
            })
            .collect();
        assert_eq!(sequences, [0, 1]);
        assert_eq!(i64_values(&first, "log_a_i64"), vec![Some(1), None]);
        assert_eq!(i64_values(&first, "log_b_i64"), vec![None, Some(2)]);
        let schema = first[0].schema();
        let metadata = schema.metadata();
        assert_eq!(
            metadata.get(META_PROJECTION_VERSION).map(String::as_str),
            Some("1")
        );
        assert_eq!(PROJECTION_VERSION.to_string(), "1");
        assert_eq!(
            metadata.get(META_FINGERPRINT).map(String::as_str),
            Some(generation.fingerprint.as_str())
        );
        assert_eq!(
            metadata.get(META_WAL_FIRST_SEQUENCE).map(String::as_str),
            Some("0")
        );
        assert_eq!(
            metadata.get(META_WAL_NEXT_SEQUENCE).map(String::as_str),
            Some("3")
        );
        assert_eq!(metadata.get(META_ROW_COUNT).map(String::as_str), Some("2"));
        assert_eq!(
            metadata.get(META_HOUR_START_UNIX_NANO).map(String::as_str),
            Some("0")
        );
        let hour_end = HOUR.to_string();
        assert_eq!(
            metadata.get(META_HOUR_END_UNIX_NANO).map(String::as_str),
            Some(hour_end.as_str())
        );
        let field = schema.field_with_name("log_a_i64").expect("field");
        assert_eq!(
            field.metadata().get(crate::FIELD_PATH).map(String::as_str),
            Some(r#"["a"]"#)
        );

        let second = read_parquet_batches(&written[1].path).expect("read");
        assert_eq!(i64_values(&second, "log_a_i64"), vec![Some(3)]);
        assert_eq!(i64_values(&second, "log_b_i64"), vec![None]);

        write_generation(
            directory.path(),
            "tenant-a",
            &generation,
            &ParquetWriteOptions::default(),
        )
        .expect("rewrite");
        assert_eq!(
            string_values(
                &read_parquet_batches(&written[0].path).expect("reread"),
                COLUMN_BODY
            ),
            vec![Some("row-0".to_owned()), Some("row-1".to_owned())]
        );
        assert!(!written[0].path.with_extension("parquet.tmp").exists());
    }

    #[test]
    fn injected_faults_stop_at_each_durability_boundary() {
        let generation = seal(&[logs(0, 0, vec![attribute("a", 1)])]);
        let cases = [
            (ParquetFault::Create, false, false),
            (ParquetFault::SyncFile, true, false),
            (ParquetFault::Rename, true, false),
            (ParquetFault::SyncDir, false, true),
        ];
        for (fault, temporary_exists, final_exists) in cases {
            let directory = tempfile::tempdir().expect("tempdir");
            let error = write_generation(
                directory.path(),
                "tenant-a",
                &generation,
                &ParquetWriteOptions { fault: Some(fault) },
            )
            .expect_err("fault");
            assert_eq!(error.to_string(), ParquetError::Fault(fault).to_string());
            let path = parquet_path(directory.path(), "tenant-a", EventHour::containing(0), 0, 1);
            assert_eq!(
                path.with_extension("parquet.tmp").exists(),
                temporary_exists
            );
            assert_eq!(path.exists(), final_exists);
        }
    }
}
