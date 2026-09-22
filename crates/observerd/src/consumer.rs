//! One blocking WAL consumer per configured tenant.
//!
//! Startup reconciles the published sequence with the WAL checkpoint. The worker then decodes
//! complete logs frames into the memtable, publishes a frozen generation, and only afterwards
//! checkpoints that generation's exclusive cursor and retains sealed WAL segments. Shutdown drains
//! the tail, rotates the active generation, and flushes it. A failed flush leaves the checkpoint
//! where the last durable commit put it.

use std::{
    io,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, Sender},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use observer_storage::{
    DecodeError, DynamicLimits, MemtableConfig, MemtableError, PublishOptions, Store, StoreError,
    SystemClock, decode_logs_frame,
};
use observer_wal::{WalCheckpoint, WalError, WalReader, retain_committed};

use crate::config::{Config, StorageConfig};

/// Consumers for every configured tenant.
#[derive(Debug)]
pub struct ConsumerSet {
    failed: Arc<AtomicBool>,
    flush: Vec<Sender<()>>,
    joins: Vec<JoinHandle<Result<(), ConsumerError>>>,
}

/// Why a tenant consumer stopped before a clean flush.
#[derive(Debug)]
pub enum ConsumerError {
    /// The store or its Parquet publication failed.
    Storage(StoreError),
    /// The WAL could not be read, checkpointed, or retained.
    Wal(WalError),
    /// A frame could not be projected into the memtable.
    Decode(DecodeError),
    /// The WAL checkpoint is ahead of the published catalog.
    StorageBehind {
        /// Exclusive sequence recorded by the catalog.
        durable: u64,
        /// Exclusive sequence recorded by the WAL checkpoint.
        committed: u64,
    },
    /// The frozen queue is full and nothing could be published.
    FrozenStuck,
    /// The worker thread panicked.
    Crashed,
    /// The worker thread could not be started.
    Spawn(io::Error),
}

impl std::fmt::Display for ConsumerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(error) => write!(formatter, "storage consumer failed: {error}"),
            Self::Wal(error) => write!(formatter, "WAL consumer failed: {error}"),
            Self::Decode(error) => write!(formatter, "log decode failed: {error}"),
            Self::StorageBehind { durable, committed } => write!(
                formatter,
                "published sequence {durable} is behind WAL checkpoint {committed}"
            ),
            Self::FrozenStuck => {
                formatter.write_str("frozen generation limit is full and nothing is publishable")
            }
            Self::Crashed => formatter.write_str("storage consumer panicked"),
            Self::Spawn(error) => write!(formatter, "failed to start storage consumer: {error}"),
        }
    }
}

impl std::error::Error for ConsumerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            Self::Wal(error) => Some(error),
            Self::Decode(error) => Some(error),
            Self::Spawn(error) => Some(error),
            Self::StorageBehind { .. } | Self::FrozenStuck | Self::Crashed => None,
        }
    }
}

impl From<StoreError> for ConsumerError {
    fn from(error: StoreError) -> Self {
        Self::Storage(error)
    }
}

impl From<WalError> for ConsumerError {
    fn from(error: WalError) -> Self {
        Self::Wal(error)
    }
}

impl From<DecodeError> for ConsumerError {
    fn from(error: DecodeError) -> Self {
        Self::Decode(error)
    }
}

impl ConsumerSet {
    /// Recover every tenant store, then start its consumer thread.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError`] when recovery fails or a thread cannot be spawned. No thread is
    /// left running when recovery fails.
    pub fn start(
        config: &Config,
        tenants: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<Self, ConsumerError> {
        let failed = Arc::new(AtomicBool::new(false));
        let limits = dynamic_limits(&config.storage);
        let memtable = memtable_config(&config.storage);
        let mut prepared = Vec::new();
        for tenant in tenants {
            let tenant = tenant.as_ref();
            let directory = observer_wal::tenant_wal_directory(&config.wal_directory, tenant);
            std::fs::create_dir_all(&directory)
                .map_err(|error| ConsumerError::Wal(WalError::Io(error)))?;
            let store = Store::open(
                &config.data_directory,
                tenant,
                memtable,
                Arc::new(SystemClock),
            )?;
            let durable = store.durable_sequence()?.unwrap_or(0);
            let committed = WalCheckpoint::load(&directory)?.cursor().next_sequence();
            if durable < committed {
                return Err(ConsumerError::StorageBehind { durable, committed });
            }
            prepared.push((directory, store));
        }

        let mut flush: Vec<Sender<()>> = Vec::new();
        let mut joins: Vec<JoinHandle<Result<(), ConsumerError>>> = Vec::new();
        for (directory, store) in prepared {
            let (sender, receiver) = mpsc::channel();
            let failed = Arc::clone(&failed);
            let poll_interval = config.storage.poll_interval;
            let handle = match thread::Builder::new()
                .name("obs-consumer".to_owned())
                .spawn(move || {
                    let result = run(store, directory, receiver, poll_interval, limits);
                    if result.is_err() {
                        failed.store(true, Ordering::SeqCst);
                    }
                    result
                }) {
                Ok(handle) => handle,
                Err(error) => {
                    for sender in flush {
                        let _ = sender.send(());
                    }
                    for handle in joins {
                        let _ = handle.join();
                    }
                    return Err(ConsumerError::Spawn(error));
                }
            };
            flush.push(sender);
            joins.push(handle);
        }
        Ok(Self {
            failed,
            flush,
            joins,
        })
    }

    /// Shared flag set when any consumer returns an error.
    #[must_use]
    pub fn failed_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.failed)
    }

    /// Ask every consumer to drain and flush, then wait for it.
    ///
    /// # Errors
    ///
    /// Returns the first consumer error. A failed flush does not move the WAL checkpoint.
    pub fn shutdown(self) -> Result<(), ConsumerError> {
        for sender in self.flush {
            let _ = sender.send(());
        }
        let mut error = None;
        for handle in self.joins {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(consumer_error)) => {
                    self.failed.store(true, Ordering::SeqCst);
                    if error.is_none() {
                        error = Some(consumer_error);
                    }
                }
                Err(_) => {
                    self.failed.store(true, Ordering::SeqCst);
                    if error.is_none() {
                        error = Some(ConsumerError::Crashed);
                    }
                }
            }
        }
        match error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

fn memtable_config(storage: &StorageConfig) -> MemtableConfig {
    MemtableConfig {
        max_rows: storage.max_rows,
        max_bytes: storage.max_bytes,
        max_age: storage.max_age,
        max_frozen: storage.max_frozen,
        max_dynamic_columns: storage.max_dynamic_columns,
    }
}

fn dynamic_limits(storage: &StorageConfig) -> DynamicLimits {
    DynamicLimits {
        max_depth: storage.max_depth,
        max_columns: storage.max_dynamic_columns,
    }
}

fn run(
    store: Store,
    directory: PathBuf,
    flush: Receiver<()>,
    poll_interval: Duration,
    limits: DynamicLimits,
) -> Result<(), ConsumerError> {
    let (mut checkpoint, mut reader) = reconcile(&directory, &store)?;
    let mut flushing = false;
    loop {
        match reader.next_record()? {
            Some(record) => {
                let decoded = decode_logs_frame(&record.frame, limits)?;
                ingest(&store, &decoded, &directory, &mut checkpoint)?;
            }
            None if flushing => {
                finish(&store, &directory, &mut checkpoint)?;
                return Ok(());
            }
            None => {
                reader.refresh()?;
                match flush.recv_timeout(poll_interval) {
                    Ok(()) | Err(RecvTimeoutError::Disconnected) => flushing = true,
                    Err(RecvTimeoutError::Timeout) => {}
                }
            }
        }
    }
}

fn reconcile(
    directory: &std::path::Path,
    store: &Store,
) -> Result<(WalCheckpoint, WalReader), ConsumerError> {
    let mut checkpoint = WalCheckpoint::load(directory)?;
    let durable = store.durable_sequence()?.unwrap_or(0);
    let committed = checkpoint.cursor().next_sequence();
    if durable < committed {
        return Err(ConsumerError::StorageBehind { durable, committed });
    }
    if durable > committed {
        advance_checkpoint(directory, &mut checkpoint, durable)?;
    }
    let reader = WalReader::open_at(directory, checkpoint.cursor())?;
    Ok((checkpoint, reader))
}

fn ingest(
    store: &Store,
    decoded: &observer_storage::DecodedLogs,
    directory: &std::path::Path,
    checkpoint: &mut WalCheckpoint,
) -> Result<(), ConsumerError> {
    loop {
        match store.append(decoded) {
            Ok(appended) => {
                if appended.sealed {
                    publish_ready(store, directory, checkpoint)?;
                }
                return Ok(());
            }
            Err(StoreError::Memtable(MemtableError::FrozenLimit)) => {
                if !publish_ready(store, directory, checkpoint)? {
                    return Err(ConsumerError::FrozenStuck);
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn finish(
    store: &Store,
    directory: &std::path::Path,
    checkpoint: &mut WalCheckpoint,
) -> Result<(), ConsumerError> {
    loop {
        match store.rotate() {
            Ok(_) => break,
            Err(StoreError::Memtable(MemtableError::FrozenLimit)) => {
                if !publish_ready(store, directory, checkpoint)? {
                    return Err(ConsumerError::FrozenStuck);
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
    publish_ready(store, directory, checkpoint)?;
    Ok(())
}

fn publish_ready(
    store: &Store,
    directory: &std::path::Path,
    checkpoint: &mut WalCheckpoint,
) -> Result<bool, ConsumerError> {
    let mut published = false;
    loop {
        match store.publish(&PublishOptions::default())? {
            None => return Ok(published),
            Some(commit) => {
                advance_checkpoint(directory, checkpoint, commit.next_sequence)?;
                published = true;
            }
        }
    }
}

fn advance_checkpoint(
    directory: &std::path::Path,
    checkpoint: &mut WalCheckpoint,
    sequence: u64,
) -> Result<(), ConsumerError> {
    if checkpoint.cursor().next_sequence() == sequence {
        return Ok(());
    }
    let positioned = WalReader::open_from_sequence(directory, sequence)?;
    checkpoint.commit(positioned.cursor())?;
    retain_committed(directory)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ConsumerError, ConsumerSet};
    use crate::config::Config;
    use arrow_array::Array;
    use bytes::Bytes;
    use observer_protocol::otlp::{
        AnyValue, ExportLogsServiceRequest, LogRecord, ResourceLogs, ScopeLogs, any_value,
    };
    use observer_protocol::{AcceptedBatch, Signal};
    use observer_storage::{
        DynamicLimits, PublishOptions, Scan, Store, SystemClock, decode_logs_frame,
    };
    use observer_wal::{Wal, WalCheckpoint, WalConfig, WalReader, tenant_wal_directory};
    use prost::Message;
    use std::{
        path::Path,
        sync::Arc,
        thread,
        time::{Duration, Instant},
    };

    fn config(root: &Path, max_rows: u64) -> Config {
        Config::parse(&format!(
            r#"
wal_directory = "{wal}"
data_directory = "{data}"
[listen]
grpc = "127.0.0.1:1"
http = "127.0.0.1:2"
admin = "127.0.0.1:3"
[tokens]
"secret-a" = "tenant-a"
"secret-b" = "tenant-b"
[storage]
max_rows = {max_rows}
max_bytes = 67108864
max_age_ms = 3600000
max_frozen = 2
max_dynamic_columns = 32
max_depth = 4
poll_interval_ms = 10
[readiness]
min_free_bytes = 0
"#,
            wal = root.join("wal").display(),
            data = root.join("data").display(),
        ))
        .expect("config")
    }

    fn write_payload(directory: &Path, tenant: &str, payload: Vec<u8>) {
        let mut wal = Wal::open(WalConfig {
            directory: directory.to_path_buf(),
            max_entry_bytes: 1024 * 1024,
            target_segment_bytes: 64 * 1024 * 1024,
        })
        .expect("wal");
        wal.append(AcceptedBatch {
            tenant_id: tenant.to_owned(),
            signal: Signal::Logs,
            received_at_unix_nanos: 1,
            payload: Bytes::from(payload),
        })
        .expect("append");
    }

    fn logs_payload(body: &str) -> Vec<u8> {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: 1,
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(body.to_owned())),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
        .encode_to_vec()
    }

    fn tenant_dir(root: &Path, tenant: &str) -> std::path::PathBuf {
        tenant_wal_directory(root.join("wal"), tenant)
    }

    fn bodies(data: &Path, tenant: &str) -> Vec<Option<String>> {
        let store = Store::open(
            data,
            tenant,
            observer_storage::MemtableConfig {
                max_rows: 100,
                max_bytes: u64::MAX,
                max_age: Duration::from_secs(60),
                max_frozen: 2,
                max_dynamic_columns: 32,
            },
            Arc::new(SystemClock),
        )
        .expect("store");
        let snapshot = store.snapshot().expect("snapshot");
        let batches = store.scan(&snapshot, &Scan::default()).expect("scan");
        batches
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column_by_name(observer_storage::COLUMN_BODY)
                    .expect("body")
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .expect("utf8");
                (0..column.len())
                    .map(|row| (!column.is_null(row)).then(|| column.value(row).to_owned()))
            })
            .collect()
    }

    fn wait_for_sequence(directory: &Path, sequence: u64) {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            if WalCheckpoint::load(directory)
                .ok()
                .is_some_and(|checkpoint| checkpoint.cursor().next_sequence() == sequence)
            {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("checkpoint did not reach {sequence}");
    }

    #[test]
    fn tenants_flush_their_own_tails() {
        let root = tempfile::tempdir().expect("tempdir");
        let config = config(root.path(), 100);
        let a = tenant_dir(root.path(), "tenant-a");
        let b = tenant_dir(root.path(), "tenant-b");
        write_payload(&a, "tenant-a", logs_payload("alpha"));
        write_payload(&b, "tenant-b", logs_payload("beta"));
        let consumers = ConsumerSet::start(&config, ["tenant-a", "tenant-b"]).expect("start");
        consumers.shutdown().expect("flush");
        assert_eq!(
            WalCheckpoint::load(&a)
                .expect("a checkpoint")
                .cursor()
                .next_sequence(),
            1
        );
        assert_eq!(
            WalCheckpoint::load(&b)
                .expect("b checkpoint")
                .cursor()
                .next_sequence(),
            1
        );
        assert_eq!(
            bodies(&config.data_directory, "tenant-a"),
            vec![Some("alpha".to_owned())]
        );
        assert_eq!(
            bodies(&config.data_directory, "tenant-b"),
            vec![Some("beta".to_owned())]
        );
    }

    #[test]
    fn polling_publishes_a_frame_written_after_startup() {
        let root = tempfile::tempdir().expect("tempdir");
        let config = config(root.path(), 0);
        let directory = tenant_dir(root.path(), "tenant-a");
        let consumers = ConsumerSet::start(&config, ["tenant-a"]).expect("start");
        write_payload(&directory, "tenant-a", logs_payload("late"));
        wait_for_sequence(&directory, 1);
        consumers.shutdown().expect("flush");
        assert_eq!(
            bodies(&config.data_directory, "tenant-a"),
            vec![Some("late".to_owned())]
        );
    }

    #[test]
    fn storage_ahead_of_the_checkpoint_is_reconciled_without_duplicating_rows() {
        let root = tempfile::tempdir().expect("tempdir");
        let config = config(root.path(), 100);
        let directory = tenant_dir(root.path(), "tenant-a");
        write_payload(&directory, "tenant-a", logs_payload("once"));
        let store = Store::open(
            &config.data_directory,
            "tenant-a",
            super::memtable_config(&config.storage),
            Arc::new(SystemClock),
        )
        .expect("store");
        let mut reader = WalReader::open(&directory).expect("reader");
        let record = reader.next_record().expect("read").expect("frame");
        let decoded = decode_logs_frame(
            &record.frame,
            DynamicLimits {
                max_depth: 4,
                max_columns: 32,
            },
        )
        .expect("decode");
        store.append(&decoded).expect("append");
        store.rotate().expect("rotate");
        store.publish(&PublishOptions::default()).expect("publish");
        drop(store);
        assert_eq!(
            WalCheckpoint::load(&directory)
                .expect("checkpoint")
                .cursor()
                .next_sequence(),
            0
        );
        let consumers = ConsumerSet::start(&config, ["tenant-a"]).expect("start");
        wait_for_sequence(&directory, 1);
        consumers.shutdown().expect("flush");
        assert_eq!(
            bodies(&config.data_directory, "tenant-a"),
            vec![Some("once".to_owned())]
        );
    }

    #[test]
    fn a_corrupt_frame_stops_the_consumer_without_advancing_the_checkpoint() {
        let root = tempfile::tempdir().expect("tempdir");
        let config = config(root.path(), 0);
        let directory = tenant_dir(root.path(), "tenant-a");
        write_payload(&directory, "tenant-a", logs_payload("good"));
        write_payload(&directory, "tenant-a", b"not otlp".to_vec());
        let consumers = ConsumerSet::start(&config, ["tenant-a"]).expect("start");
        let error = consumers.shutdown().expect_err("corrupt");
        assert!(matches!(error, ConsumerError::Decode(_)));
        assert_eq!(
            WalCheckpoint::load(&directory)
                .expect("checkpoint")
                .cursor()
                .next_sequence(),
            1
        );
        assert_eq!(
            bodies(&config.data_directory, "tenant-a"),
            vec![Some("good".to_owned())]
        );
    }

    #[test]
    fn storage_behind_the_checkpoint_is_fatal() {
        let root = tempfile::tempdir().expect("tempdir");
        let config = config(root.path(), 0);
        let directory = tenant_dir(root.path(), "tenant-a");
        write_payload(&directory, "tenant-a", logs_payload("good"));
        let consumers = ConsumerSet::start(&config, ["tenant-a"]).expect("start");
        consumers.shutdown().expect("publish");
        let commit = config
            .data_directory
            .join("tenants/tenant-a/commits/0-1.commit");
        std::fs::remove_file(commit).expect("remove commit");
        let error = ConsumerSet::start(&config, ["tenant-a"]).expect_err("behind");
        assert!(matches!(
            error,
            ConsumerError::StorageBehind {
                durable: 0,
                committed: 1
            }
        ));
    }
}
