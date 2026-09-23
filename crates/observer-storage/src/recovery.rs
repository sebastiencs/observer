//! Startup recovery for one tenant's published generations.
//!
//! Valid commit descriptors become the catalog. Temporary files, checksum-invalid descriptors, and
//! Parquet files that no valid descriptor names are removed. A valid descriptor whose Parquet file
//! is missing or disagrees with its metadata is left in place and reported as a fatal error.

use std::{
    fs, io,
    path::{Path, PathBuf},
};

use crate::catalog::{Catalog, CatalogError};
use crate::commit::{Commit, CommitError, read_commit};
use crate::layout::{hour_end_unix_nano, tenant_directory};
use crate::parquet::{
    META_FINGERPRINT, META_HOUR_END_UNIX_NANO, META_HOUR_START_UNIX_NANO, META_PROJECTION_VERSION,
    META_ROW_COUNT, META_WAL_FIRST_SEQUENCE, META_WAL_NEXT_SEQUENCE, ParquetError,
    read_parquet_schema,
};

/// Catalog reconstructed from durable commit descriptors.
#[derive(Debug)]
pub struct Recovered {
    /// Contiguous published commits.
    pub catalog: Catalog,
    /// Temporary files removed before the catalog was loaded.
    pub removed_temporary: Vec<PathBuf>,
    /// Checksum-invalid or truncated descriptors that were removed.
    pub removed_corrupt: Vec<PathBuf>,
    /// Parquet files that no loaded descriptor named.
    pub removed_orphans: Vec<PathBuf>,
}

/// Why recovery could not produce a catalog.
#[derive(Debug)]
pub enum RecoveryError {
    /// Filesystem failure.
    Io(io::Error),
    /// A descriptor could not be decoded for a reason other than corruption.
    Commit(CommitError),
    /// Loaded descriptors are not one contiguous sequence range.
    Catalog(CatalogError),
    /// A published Parquet file is missing or does not match its descriptor.
    Parquet(String),
}

impl std::fmt::Display for RecoveryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "recovery io: {error}"),
            Self::Commit(error) => write!(formatter, "recovery commit: {error}"),
            Self::Catalog(error) => write!(formatter, "recovery catalog: {error}"),
            Self::Parquet(detail) => write!(formatter, "recovery parquet: {detail}"),
        }
    }
}

impl std::error::Error for RecoveryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Commit(error) => Some(error),
            Self::Catalog(error) => Some(error),
            Self::Parquet(_) => None,
        }
    }
}

impl From<io::Error> for RecoveryError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<CatalogError> for RecoveryError {
    fn from(error: CatalogError) -> Self {
        Self::Catalog(error)
    }
}

/// Load `tenant`'s commits under `root` and delete incomplete publication files.
///
/// # Errors
///
/// Returns [`RecoveryError`] when a descriptor uses an unsupported version, the published ranges
/// are not contiguous, or a named Parquet file is missing or inconsistent.
pub fn recover(root: &Path, tenant: &str) -> Result<Recovered, RecoveryError> {
    let tenant_dir = tenant_directory(root, tenant);
    let mut paths = Vec::new();
    collect_files(&tenant_dir, &mut paths)?;

    let mut removed_temporary = Vec::new();
    let mut removed_corrupt = Vec::new();
    let mut commits = Vec::new();
    let mut parquet_paths = Vec::new();
    for path in paths {
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.ends_with(".parquet.tmp") || name.ends_with(".commit.tmp") {
            fs::remove_file(&path)?;
            removed_temporary.push(path);
            continue;
        }
        if name.ends_with(".commit") {
            match classify_commit(&path)? {
                Some(commit) => commits.push(commit),
                None => {
                    fs::remove_file(&path)?;
                    removed_corrupt.push(path);
                }
            }
            continue;
        }
        if name.ends_with(".parquet") {
            parquet_paths.push(path);
        }
    }

    for commit in &mut commits {
        validate_parquet(&tenant_dir, commit)?;
    }
    let catalog = Catalog::load(commits)?;
    let referenced = catalog
        .commits()
        .iter()
        .flat_map(|commit| {
            commit
                .files
                .iter()
                .map(|file| tenant_dir.join(&file.relative_path))
        })
        .collect::<std::collections::HashSet<_>>();
    let mut removed_orphans = Vec::new();
    for path in parquet_paths {
        if !referenced.contains(&path) {
            fs::remove_file(&path)?;
            removed_orphans.push(path);
        }
    }
    Ok(Recovered {
        catalog,
        removed_temporary,
        removed_corrupt,
        removed_orphans,
    })
}

fn collect_files(directory: &Path, paths: &mut Vec<PathBuf>) -> io::Result<()> {
    if !directory.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_files(&path, paths)?;
        } else {
            paths.push(path);
        }
    }
    Ok(())
}

fn classify_commit(path: &Path) -> Result<Option<Commit>, RecoveryError> {
    match read_commit(path) {
        Ok(commit) => Ok(Some(commit)),
        Err(CommitError::Checksum) => Ok(None),
        Err(CommitError::Invalid(detail)) if corrupt_descriptor(&detail) => Ok(None),
        Err(error) => Err(RecoveryError::Commit(error)),
    }
}

fn corrupt_descriptor(detail: &str) -> bool {
    !detail.starts_with("unsupported commit version")
}

fn validate_parquet(tenant_dir: &Path, commit: &mut Commit) -> Result<(), RecoveryError> {
    for file in &mut commit.files {
        let path = tenant_dir.join(&file.relative_path);
        let schema = read_parquet_schema(&path).map_err(|error| match error {
            ParquetError::Io(io_error) if io_error.kind() == io::ErrorKind::NotFound => {
                RecoveryError::Parquet(format!("missing published file {}", path.display()))
            }
            other => RecoveryError::Parquet(other.to_string()),
        })?;
        let metadata = schema.metadata();
        let expected = [
            (
                META_PROJECTION_VERSION,
                commit.projection_version.to_string(),
            ),
            (META_FINGERPRINT, commit.fingerprint.clone()),
            (META_WAL_FIRST_SEQUENCE, commit.first_sequence.to_string()),
            (META_WAL_NEXT_SEQUENCE, commit.next_sequence.to_string()),
            (META_ROW_COUNT, file.rows.to_string()),
            (
                META_HOUR_START_UNIX_NANO,
                file.hour.start_unix_nano().to_string(),
            ),
            (
                META_HOUR_END_UNIX_NANO,
                hour_end_unix_nano(file.hour).to_string(),
            ),
        ];
        for (key, value) in expected {
            if metadata.get(key).map(String::as_str) != Some(value.as_str()) {
                return Err(RecoveryError::Parquet(format!(
                    "{} metadata {key} does not match the commit",
                    path.display()
                )));
            }
        }
        let len = fs::metadata(&path)?.len();
        if file
            .statistics
            .as_ref()
            .is_some_and(|statistics| statistics.size_bytes != len)
        {
            file.statistics = None;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use observer_protocol::otlp::{
        AnyValue, ExportLogsServiceRequest, LogRecord, ResourceLogs, ScopeLogs, any_value,
    };
    use observer_wal::{Frame, FrameSignal};
    use prost::Message;

    use crate::{
        CommitWriteOptions, DynamicLimits, ManualClock, MemtableConfig, PublishOptions, Store,
        commit_path, decode_logs_frame, read_commit, recover, write_commit,
    };

    #[test]
    fn size_mismatch_clears_statistics_and_keeps_the_file() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = Store::open(
            directory.path(),
            "tenant-a",
            MemtableConfig {
                max_rows: 100,
                max_bytes: u64::MAX,
                max_age: Duration::from_secs(60),
                max_frozen: 4,
                max_dynamic_columns: 32,
            },
            Arc::new(ManualClock::new(0)),
        )
        .expect("open");
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: 1,
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("row".to_owned())),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let logs = decode_logs_frame(
            &Frame {
                sequence: 0,
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
        .expect("decode");
        store.append(&logs).expect("append");
        store.rotate().expect("rotate");
        let published = store
            .publish(&PublishOptions::default())
            .expect("publish")
            .expect("commit");
        assert!(published.files[0].statistics.is_some());
        drop(store);

        let path = commit_path(directory.path(), "tenant-a", 0, 1);
        let mut commit = read_commit(&path).expect("read");
        commit.files[0]
            .statistics
            .as_mut()
            .expect("stats")
            .size_bytes = 1;
        write_commit(directory.path(), &commit, &CommitWriteOptions::default()).expect("rewrite");

        let recovered = recover(directory.path(), "tenant-a").expect("recover");
        let file = &recovered.catalog.commits()[0].files[0];
        assert!(file.statistics.is_none());
        assert_eq!(file.rows, 1);
    }
}
