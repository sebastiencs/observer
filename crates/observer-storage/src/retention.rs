//! Receive-time retirement and lease-aware garbage collection.
//!
//! [`Store::retire_before`] writes a cumulative descriptor before hiding files. [`Store::collect_retired`]
//! unlinks a retired file only when no snapshot still leases it.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs, io,
    path::{Path, PathBuf},
};

use crate::commit::Commit;
use crate::layout::{sync_ancestors, tenant_directory};
use crate::retirement::{
    RetiredFile, Retirement, RetirementWriteOptions, read_retirement, write_retirement,
};
use crate::snapshot::{Store, StoreError};
use crate::statistics::eligible_for_received_retention;

/// Files newly hidden by one [`Store::retire_before`] call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetentionReport {
    /// Tenant-relative paths hidden by this call.
    pub paths: Vec<String>,
    /// Rows in those files.
    pub rows: u64,
    /// Byte length recorded for those files.
    pub bytes: u64,
}

/// Descriptor-write options for one retirement pass.
#[derive(Clone, Debug, Default)]
pub struct RetireOptions {
    /// Durability fault injected while writing a descriptor.
    pub write: RetirementWriteOptions,
}

/// Where a durability test stops garbage collection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CollectFault {
    /// Fail before unlinking any file.
    Unlink,
    /// Fail after unlinks, before directory fsync.
    SyncDir,
}

/// Options for one garbage-collection pass.
#[derive(Clone, Debug, Default)]
pub struct CollectOptions {
    /// Stop at this durability boundary.
    pub fault: Option<CollectFault>,
}

/// Retired files unlinked by one [`Store::collect_retired`] call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectReport {
    /// Tenant-relative paths removed, including ones that were already absent.
    pub removed: Vec<String>,
}

/// Why garbage collection stopped.
#[derive(Debug)]
pub enum CollectError {
    /// Filesystem failure.
    Io(io::Error),
    /// [`CollectOptions::fault`] stopped the pass.
    Fault(CollectFault),
}

impl std::fmt::Display for CollectError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "collect io: {error}"),
            Self::Fault(step) => write!(formatter, "injected collect fault at {step:?}"),
        }
    }
}

impl std::error::Error for CollectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Fault(_) => None,
        }
    }
}

impl From<io::Error> for CollectError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

struct Selected {
    retirement: Option<Retirement>,
    paths: Vec<String>,
    rows: u64,
    bytes: u64,
}

impl Store {
    /// Hide every visible file whose usable receive-time maximum is strictly before `cutoff`.
    ///
    /// Descriptors are written before the overlay changes. A write fault leaves that generation
    /// unchanged in memory. Already retired files stay in the cumulative descriptor.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when a descriptor cannot be written.
    pub fn retire_before(
        &self,
        cutoff_unix_nano: u64,
        options: &RetireOptions,
    ) -> Result<RetentionReport, StoreError> {
        let selected = {
            let state = self.lock()?;
            select(
                &self.root,
                &self.tenant,
                state.catalog.commits(),
                &state.retired,
                cutoff_unix_nano,
            )?
        };
        let mut paths = Vec::new();
        let mut rows = 0_u64;
        let mut bytes = 0_u64;
        for batch in selected {
            if let Some(retirement) = &batch.retirement {
                write_retirement(&self.root, retirement, &options.write)?;
            }
            self.install(&batch.paths)?;
            paths.extend(batch.paths);
            rows = rows.saturating_add(batch.rows);
            bytes = bytes.saturating_add(batch.bytes);
        }
        Ok(RetentionReport { paths, rows, bytes })
    }

    /// Unlink retired files that no snapshot leases, then fsync the affected directories.
    ///
    /// An already missing file counts as removed. Empty event-hour and date directories are
    /// removed. Commit and retirement descriptors stay.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when an unlink or directory sync fails.
    pub fn collect_retired(&self, options: &CollectOptions) -> Result<CollectReport, StoreError> {
        let paths = {
            let state = self.lock()?;
            state.collectible.iter().cloned().collect::<Vec<_>>()
        };
        if paths.is_empty() {
            return Ok(CollectReport {
                removed: Vec::new(),
            });
        }
        if options.fault == Some(CollectFault::Unlink) {
            return Err(CollectError::Fault(CollectFault::Unlink).into());
        }
        let tenant_dir = tenant_directory(&self.root, &self.tenant);
        let mut parents = BTreeSet::new();
        for path in &paths {
            let absolute = tenant_dir.join(path);
            match fs::remove_file(&absolute) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(CollectError::Io(error).into()),
            }
            if let Some(parent) = absolute.parent() {
                parents.insert(parent.to_path_buf());
            }
        }
        if options.fault == Some(CollectFault::SyncDir) {
            return Err(CollectError::Fault(CollectFault::SyncDir).into());
        }
        prune_empty_directories(&self.root, &tenant_dir, &parents)?;
        let mut state = self.lock()?;
        for path in &paths {
            state.collectible.remove(path);
        }
        Ok(CollectReport { removed: paths })
    }

    fn install(&self, paths: &[String]) -> Result<(), StoreError> {
        let mut state = self.lock()?;
        for path in paths {
            state.retired.insert(path.clone());
            let leased = state.leases.get(path).copied().unwrap_or(0) > 0;
            if !leased {
                state.collectible.insert(path.clone());
            }
        }
        state.epoch = state.epoch.saturating_add(1);
        Ok(())
    }
}

fn select(
    root: &Path,
    tenant: &str,
    commits: &[Commit],
    retired: &BTreeSet<String>,
    cutoff_unix_nano: u64,
) -> Result<Vec<Selected>, StoreError> {
    let mut selected = Vec::new();
    for commit in commits {
        let Some(batch) = select_commit(root, tenant, commit, retired, cutoff_unix_nano)? else {
            continue;
        };
        selected.push(batch);
    }
    Ok(selected)
}

fn select_commit(
    root: &Path,
    tenant: &str,
    commit: &Commit,
    retired: &BTreeSet<String>,
    cutoff_unix_nano: u64,
) -> Result<Option<Selected>, StoreError> {
    let descriptor =
        crate::retirement_path(root, tenant, commit.first_sequence, commit.next_sequence);
    let existing = if descriptor.exists() {
        Some(read_retirement(&descriptor)?)
    } else {
        None
    };
    let mut files = BTreeMap::<String, u64>::new();
    let mut received_before = cutoff_unix_nano;
    if let Some(existing) = existing {
        received_before = received_before.max(existing.received_before_unix_nano);
        for file in existing.files {
            files.insert(file.relative_path, file.rows);
        }
    }
    let mut paths = Vec::new();
    let mut rows = 0_u64;
    let mut bytes = 0_u64;
    for file in &commit.files {
        if retired.contains(&file.relative_path) || files.contains_key(&file.relative_path) {
            continue;
        }
        if !eligible_for_received_retention(
            file.statistics.as_ref(),
            commit.schema.as_ref(),
            file.rows,
            file.hour,
            commit.first_sequence,
            commit.next_sequence,
            cutoff_unix_nano,
        ) {
            continue;
        }
        let size = file
            .statistics
            .as_ref()
            .map(|stats| stats.size_bytes)
            .unwrap_or(0);
        files.insert(file.relative_path.clone(), file.rows);
        paths.push(file.relative_path.clone());
        rows = rows.saturating_add(file.rows);
        bytes = bytes.saturating_add(size);
    }
    for path in files.keys() {
        if !retired.contains(path) && !paths.contains(path) {
            paths.push(path.clone());
        }
    }
    if paths.is_empty() {
        return Ok(None);
    }
    let wrote_new = rows > 0;
    Ok(Some(Selected {
        retirement: wrote_new.then(|| Retirement {
            tenant: tenant.to_owned(),
            first_sequence: commit.first_sequence,
            next_sequence: commit.next_sequence,
            received_before_unix_nano: received_before,
            files: files
                .into_iter()
                .map(|(relative_path, rows)| RetiredFile {
                    relative_path,
                    rows,
                })
                .collect(),
        }),
        paths,
        rows,
        bytes,
    }))
}

fn prune_empty_directories(
    root: &Path,
    tenant_dir: &Path,
    parents: &BTreeSet<PathBuf>,
) -> Result<(), CollectError> {
    let mut synced = BTreeSet::new();
    for parent in parents {
        let mut current = parent.clone();
        while current.starts_with(tenant_dir) && current != *tenant_dir {
            if !current.exists() {
                current = match current.parent() {
                    Some(parent) => parent.to_path_buf(),
                    None => break,
                };
                continue;
            }
            let empty = fs::read_dir(&current)?.next().is_none();
            if !empty {
                synced.insert(current);
                break;
            }
            let parent = current.parent().map(Path::to_path_buf);
            fs::remove_dir(&current)?;
            let Some(parent) = parent else { break };
            current = parent;
        }
    }
    for directory in &synced {
        sync_ancestors(directory, root)?;
    }
    if synced.is_empty() {
        sync_ancestors(tenant_dir, root)?;
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

    use super::{CollectFault, CollectOptions, RetireOptions};
    use crate::{
        COLUMN_BODY, DynamicLimits, ManualClock, MemtableConfig, PublishOptions, RetirementFault,
        RetirementWriteOptions, Scan, Store, StoreError, decode_logs_frame,
    };

    const HOUR: u64 = 3_600_000_000_000;

    fn open(directory: &std::path::Path) -> Store {
        Store::open(
            directory,
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
        .expect("open")
    }

    fn publish_two_receive_times(directory: &std::path::Path) -> Store {
        let store = open(directory);
        for (sequence, time, received, body) in [(0, 1, 100, "early"), (1, HOUR, 300, "later")] {
            let request = ExportLogsServiceRequest {
                resource_logs: vec![ResourceLogs {
                    scope_logs: vec![ScopeLogs {
                        log_records: vec![LogRecord {
                            time_unix_nano: time,
                            body: Some(AnyValue {
                                value: Some(any_value::Value::StringValue(body.to_owned())),
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
                    sequence,
                    signal: FrameSignal::Logs,
                    received_at_unix_nanos: received,
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
        }
        store.rotate().expect("rotate");
        store
            .publish(&PublishOptions::default())
            .expect("publish")
            .expect("commit");
        store
    }

    fn bodies(store: &Store) -> Vec<String> {
        let snapshot = store.pin().expect("pin");
        let batches = store.scan(&snapshot, &Scan::default()).expect("scan");
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name(COLUMN_BODY)
                    .expect("body")
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .expect("utf8")
                    .iter()
                    .flatten()
                    .map(str::to_owned)
            })
            .collect()
    }

    #[test]
    fn retirement_hides_one_hour_and_leaves_the_durable_sequence() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = publish_two_receive_times(directory.path());
        assert_eq!(store.durable_sequence().expect("sequence"), Some(2));
        let report = store
            .retire_before(200, &RetireOptions::default())
            .expect("retire");
        assert_eq!(report.paths.len(), 1);
        assert_eq!(report.rows, 1);
        assert!(report.bytes > 0);
        assert_eq!(bodies(&store), vec!["later".to_owned()]);
        assert_eq!(store.durable_sequence().expect("sequence"), Some(2));
        let again = store
            .retire_before(200, &RetireOptions::default())
            .expect("repeat");
        assert!(again.paths.is_empty());
        let later = store
            .retire_before(301, &RetireOptions::default())
            .expect("second file");
        assert_eq!(later.paths.len(), 1);
        assert!(bodies(&store).is_empty());
        assert_eq!(store.durable_sequence().expect("sequence"), Some(2));
    }

    #[test]
    fn collection_waits_for_the_last_lease_and_is_idempotent() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = publish_two_receive_times(directory.path());
        let pinned = store.pin().expect("pin");
        let report = store
            .retire_before(200, &RetireOptions::default())
            .expect("retire");
        let hidden = directory
            .path()
            .join("tenants/tenant-a")
            .join(&report.paths[0]);
        let held = store
            .collect_retired(&CollectOptions::default())
            .expect("held");
        assert!(held.removed.is_empty());
        assert!(hidden.exists());
        drop(pinned);
        let removed = store
            .collect_retired(&CollectOptions::default())
            .expect("collect");
        assert_eq!(removed.removed, report.paths);
        assert!(!hidden.exists());
        assert!(!hidden.parent().expect("hour").exists());
        let again = store
            .collect_retired(&CollectOptions::default())
            .expect("again");
        assert!(again.removed.is_empty());
        drop(store);
        let reopened = open(directory.path());
        assert_eq!(bodies(&reopened), vec!["later".to_owned()]);
        assert_eq!(reopened.durable_sequence().expect("sequence"), Some(2));
    }

    #[test]
    fn a_crash_before_rename_leaves_both_files_visible() {
        for fault in [RetirementFault::Create, RetirementFault::SyncFile] {
            let directory = tempfile::tempdir().expect("tempdir");
            let store = publish_two_receive_times(directory.path());
            let error = store
                .retire_before(
                    200,
                    &RetireOptions {
                        write: RetirementWriteOptions { fault: Some(fault) },
                    },
                )
                .expect_err("fault");
            assert!(matches!(
                error,
                StoreError::Retirement(crate::RetirementError::Fault(step)) if step == fault
            ));
            drop(store);
            let reopened = open(directory.path());
            let mut visible = bodies(&reopened);
            visible.sort();
            assert_eq!(visible, vec!["early".to_owned(), "later".to_owned()]);
        }
    }

    #[test]
    fn a_descriptor_fault_never_hides_a_missing_file() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = publish_two_receive_times(directory.path());
        let error = store
            .retire_before(
                200,
                &RetireOptions {
                    write: RetirementWriteOptions {
                        fault: Some(RetirementFault::Rename),
                    },
                },
            )
            .expect_err("rename");
        assert!(matches!(
            error,
            StoreError::Retirement(crate::RetirementError::Fault(RetirementFault::Rename))
        ));
        drop(store);
        let reopened = open(directory.path());
        let mut visible = bodies(&reopened);
        visible.sort();
        assert_eq!(visible, vec!["early".to_owned(), "later".to_owned()]);

        let error = reopened
            .retire_before(
                200,
                &RetireOptions {
                    write: RetirementWriteOptions {
                        fault: Some(RetirementFault::SyncDir),
                    },
                },
            )
            .expect_err("sync");
        assert!(matches!(
            error,
            StoreError::Retirement(crate::RetirementError::Fault(RetirementFault::SyncDir))
        ));
        drop(reopened);
        let hidden = open(directory.path());
        assert_eq!(bodies(&hidden), vec!["later".to_owned()]);
        assert!(
            directory
                .path()
                .join("tenants/tenant-a")
                .read_dir()
                .expect("tenant")
                .any(|entry| {
                    entry
                        .expect("entry")
                        .path()
                        .to_string_lossy()
                        .contains("date=")
                })
        );
    }

    #[test]
    fn an_unlink_fault_keeps_the_file_and_a_sync_fault_still_hides_it() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = publish_two_receive_times(directory.path());
        let report = store
            .retire_before(200, &RetireOptions::default())
            .expect("retire");
        let hidden = directory
            .path()
            .join("tenants/tenant-a")
            .join(&report.paths[0]);
        let error = store
            .collect_retired(&CollectOptions {
                fault: Some(CollectFault::Unlink),
            })
            .expect_err("unlink");
        assert!(matches!(
            error,
            StoreError::Collect(super::CollectError::Fault(CollectFault::Unlink))
        ));
        assert!(hidden.exists());
        let error = store
            .collect_retired(&CollectOptions {
                fault: Some(CollectFault::SyncDir),
            })
            .expect_err("sync");
        assert!(matches!(
            error,
            StoreError::Collect(super::CollectError::Fault(CollectFault::SyncDir))
        ));
        assert!(!hidden.exists());
        drop(store);
        let reopened = open(directory.path());
        assert_eq!(bodies(&reopened), vec!["later".to_owned()]);
    }
}
