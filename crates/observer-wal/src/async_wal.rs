use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use observer_protocol::{AcceptedBatch, AppendError, IngestSink};
use tokio::sync::{
    Semaphore,
    mpsc::{self, UnboundedReceiver, error::TryRecvError},
    oneshot,
};

use crate::{
    FrameError, MAX_PAYLOAD_LEN, MAX_TENANT_LEN, Receipt, Wal, WalConfig, WalError,
    encoded_frame_size,
};

#[cfg(any(test, feature = "test-util"))]
use crate::WalIoHooks;

/// Defaults from the durable ingestion plan.
const DEFAULT_TARGET_SEGMENT_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_MAX_QUEUED_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_GROUP_COMMIT_BYTES: usize = 1024 * 1024;
const DEFAULT_GROUP_COMMIT_DEADLINE: Duration = Duration::from_millis(2);

/// Configuration for the async WAL writer and byte-bounded admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalWriterConfig {
    pub wal: WalConfig,
    pub max_queued_bytes: usize,
    pub group_commit_bytes: usize,
    pub group_commit_deadline: Duration,
}

impl WalWriterConfig {
    #[must_use]
    pub fn new(directory: impl Into<std::path::PathBuf>) -> Self {
        Self {
            wal: WalConfig {
                directory: directory.into(),
                max_entry_bytes: encoded_frame_size(MAX_TENANT_LEN, MAX_PAYLOAD_LEN)
                    .expect("maximum frame size fits"),
                target_segment_bytes: DEFAULT_TARGET_SEGMENT_BYTES,
            },
            max_queued_bytes: DEFAULT_MAX_QUEUED_BYTES,
            group_commit_bytes: DEFAULT_GROUP_COMMIT_BYTES,
            group_commit_deadline: DEFAULT_GROUP_COMMIT_DEADLINE,
        }
    }
}

struct Submission {
    batch: AcceptedBatch,
    cost: usize,
    reply: oneshot::Sender<Result<Receipt, AppendError>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

struct Shared {
    tx: Mutex<Option<mpsc::UnboundedSender<Submission>>>,
    permits: Arc<Semaphore>,
    max_queued_bytes: usize,
    max_entry_bytes: usize,
    failed: Arc<AtomicBool>,
    done: Mutex<Option<oneshot::Receiver<Result<(), WalError>>>>,
}

/// Cloneable handle that admits batches and waits for durable acknowledgement.
#[derive(Clone)]
pub struct AsyncWal {
    shared: Arc<Shared>,
}

impl AsyncWal {
    /// Recover the WAL and start the writer. Must run inside a Tokio runtime.
    pub fn open(config: WalWriterConfig) -> Result<Self, WalError> {
        validate_writer_config(&config)?;
        let wal = Wal::open(config.wal.clone())?;
        #[cfg(any(test, feature = "test-util"))]
        let started = Self::start(wal, &config, None);
        #[cfg(not(any(test, feature = "test-util")))]
        let started = Self::start(wal, &config);
        Ok(started)
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn open_with_hooks(
        config: WalWriterConfig,
        hooks: Arc<WalIoHooks>,
    ) -> Result<Self, WalError> {
        validate_writer_config(&config)?;
        let mut wal = Wal::open(config.wal.clone())?;
        wal.set_io_hooks(Arc::clone(&hooks));
        Ok(Self::start(wal, &config, Some(hooks)))
    }

    fn start(
        wal: Wal,
        config: &WalWriterConfig,
        #[cfg(any(test, feature = "test-util"))] hooks: Option<Arc<WalIoHooks>>,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let (done_tx, done_rx) = oneshot::channel();
        let failed = Arc::new(AtomicBool::new(false));
        let writer_failed = Arc::clone(&failed);
        let group_commit_bytes = config.group_commit_bytes;
        let group_commit_deadline = config.group_commit_deadline;
        tokio::spawn(async move {
            let result = writer_loop(
                wal,
                rx,
                group_commit_bytes,
                group_commit_deadline,
                writer_failed,
                #[cfg(any(test, feature = "test-util"))]
                hooks,
            )
            .await;
            let _ = done_tx.send(result);
        });

        Self {
            shared: Arc::new(Shared {
                tx: Mutex::new(Some(tx)),
                permits: Arc::new(Semaphore::new(config.max_queued_bytes)),
                max_queued_bytes: config.max_queued_bytes,
                max_entry_bytes: config.wal.max_entry_bytes,
                failed,
                done: Mutex::new(Some(done_rx)),
            }),
        }
    }

    /// Admit `batch` and wait until it is fsynced (or the writer fails).
    pub async fn submit(&self, batch: AcceptedBatch) -> Result<Receipt, AppendError> {
        if self.shared.failed.load(Ordering::SeqCst) {
            return Err(AppendError::unavailable(WalError::Failed.to_string()));
        }

        let cost = admission_bytes(&batch, self.shared.max_entry_bytes)?;
        if cost > self.shared.max_queued_bytes {
            return Err(AppendError::resource_exhausted(
                "WAL admission queue is full",
            ));
        }
        let cost_u32 = u32::try_from(cost)
            .map_err(|_| AppendError::invalid_argument("admission cost exceeds u32"))?;
        let permit = Arc::clone(&self.shared.permits)
            .try_acquire_many_owned(cost_u32)
            .map_err(|_| AppendError::resource_exhausted("WAL admission queue is full"))?;

        let (reply_tx, reply_rx) = oneshot::channel();
        let submission = Submission {
            batch,
            cost,
            reply: reply_tx,
            _permit: permit,
        };
        let tx = self
            .shared
            .tx
            .lock()
            .expect("WAL sender lock poisoned")
            .clone();
        let Some(tx) = tx else {
            return Err(AppendError::unavailable("WAL writer is shut down"));
        };
        if tx.send(submission).is_err() {
            self.shared.failed.store(true, Ordering::SeqCst);
            return Err(AppendError::unavailable("WAL writer is shut down"));
        }
        reply_rx
            .await
            .map_err(|_| AppendError::unavailable("WAL writer closed"))?
    }

    /// Stop accepting work, flush queued batches, and join the writer.
    pub async fn shutdown(&self) -> Result<(), WalError> {
        self.shared
            .tx
            .lock()
            .expect("WAL sender lock poisoned")
            .take();
        let done = self
            .shared
            .done
            .lock()
            .expect("WAL shutdown lock poisoned")
            .take();
        let Some(done) = done else {
            return if self.shared.failed.load(Ordering::SeqCst) {
                Err(WalError::Failed)
            } else {
                Ok(())
            };
        };
        match done.await {
            Ok(result) => {
                if result.is_err() {
                    self.shared.failed.store(true, Ordering::SeqCst);
                }
                result
            }
            Err(_) => {
                self.shared.failed.store(true, Ordering::SeqCst);
                Err(WalError::Failed)
            }
        }
    }

    #[must_use]
    pub fn queued_bytes(&self) -> usize {
        self.shared
            .max_queued_bytes
            .saturating_sub(self.shared.permits.available_permits())
    }
}

#[tonic::async_trait]
impl IngestSink for AsyncWal {
    async fn append(&self, batch: AcceptedBatch) -> Result<(), AppendError> {
        self.submit(batch).await.map(|_| ())
    }
}

fn validate_writer_config(config: &WalWriterConfig) -> Result<(), WalError> {
    if config.max_queued_bytes == 0 {
        return Err(WalError::InvalidConfig("max_queued_bytes must be non-zero"));
    }
    if config.max_queued_bytes > Semaphore::MAX_PERMITS {
        return Err(WalError::InvalidConfig(
            "max_queued_bytes exceeds semaphore limit",
        ));
    }
    if config.group_commit_bytes == 0 {
        return Err(WalError::InvalidConfig(
            "group_commit_bytes must be non-zero",
        ));
    }
    Ok(())
}

fn admission_bytes(batch: &AcceptedBatch, max_entry_bytes: usize) -> Result<usize, AppendError> {
    let tenant_len = batch.tenant_id.len();
    if tenant_len > MAX_TENANT_LEN {
        return Err(AppendError::invalid_argument(
            FrameError::TenantTooLong { length: tenant_len }.to_string(),
        ));
    }
    let payload_len = batch.payload.len();
    if payload_len > MAX_PAYLOAD_LEN {
        return Err(AppendError::invalid_argument(
            FrameError::PayloadTooLong {
                length: payload_len,
            }
            .to_string(),
        ));
    }
    let size = encoded_frame_size(tenant_len, payload_len)
        .map_err(|error| AppendError::invalid_argument(error.to_string()))?;
    if size > max_entry_bytes {
        return Err(AppendError::invalid_argument(
            WalError::EntryTooLarge {
                size,
                max: max_entry_bytes,
            }
            .to_string(),
        ));
    }
    Ok(size)
}

fn append_error_from_wal(error: &WalError) -> AppendError {
    match error {
        WalError::Frame(FrameError::TenantTooLong { .. })
        | WalError::Frame(FrameError::PayloadTooLong { .. })
        | WalError::EntryTooLarge { .. } => AppendError::invalid_argument(error.to_string()),
        WalError::Failed | WalError::Io(_) | WalError::ShortWrite { .. } => {
            AppendError::unavailable(error.to_string())
        }
        _ => AppendError::internal(error.to_string()),
    }
}

enum CollectReason {
    ByteLimit,
    Deadline,
    Shutdown,
}

async fn collect_group(
    rx: &mut UnboundedReceiver<Submission>,
    group: &mut Vec<Submission>,
    group_commit_bytes: usize,
    deadline: Duration,
) -> CollectReason {
    let mut bytes: usize = group.iter().map(|item| item.cost).sum();
    if bytes >= group_commit_bytes {
        return CollectReason::ByteLimit;
    }

    while bytes < group_commit_bytes {
        match rx.try_recv() {
            Ok(item) => {
                bytes += item.cost;
                group.push(item);
            }
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Disconnected) => return CollectReason::Shutdown,
        }
    }
    if bytes >= group_commit_bytes {
        return CollectReason::ByteLimit;
    }

    let sleep = tokio::time::sleep(deadline);
    tokio::pin!(sleep);
    loop {
        tokio::select! {
            biased;
            next = rx.recv() => {
                match next {
                    Some(item) => {
                        bytes += item.cost;
                        group.push(item);
                        if bytes >= group_commit_bytes {
                            return CollectReason::ByteLimit;
                        }
                    }
                    None => return CollectReason::Shutdown,
                }
            }
            _ = &mut sleep => return CollectReason::Deadline,
        }
    }
}

async fn writer_loop(
    mut wal: Wal,
    mut rx: UnboundedReceiver<Submission>,
    group_commit_bytes: usize,
    group_commit_deadline: Duration,
    failed: Arc<AtomicBool>,
    #[cfg(any(test, feature = "test-util"))] hooks: Option<Arc<WalIoHooks>>,
) -> Result<(), WalError> {
    loop {
        #[cfg(any(test, feature = "test-util"))]
        if let Some(hooks) = &hooks {
            hooks.wait_admission_if_held().await;
        }

        let Some(first) = rx.recv().await else {
            return Ok(());
        };
        let mut group = vec![first];
        let reason = collect_group(
            &mut rx,
            &mut group,
            group_commit_bytes,
            group_commit_deadline,
        )
        .await;

        let batches: Vec<AcceptedBatch> = group.iter().map(|item| item.batch.clone()).collect();
        let commit = tokio::task::spawn_blocking(move || {
            let result = wal.append_group(batches);
            (wal, result)
        })
        .await;

        match commit {
            Ok((next_wal, Ok(receipts))) => {
                wal = next_wal;
                for (item, receipt) in group.into_iter().zip(receipts) {
                    let _ = item.reply.send(Ok(receipt));
                }
            }
            Ok((next_wal, Err(error))) => {
                let _ = next_wal;
                failed.store(true, Ordering::SeqCst);
                let append_error = append_error_from_wal(&error);
                reject_group(group, &append_error);
                reject_queued(&mut rx, &append_error);
                return Err(error);
            }
            Err(join_error) => {
                failed.store(true, Ordering::SeqCst);
                let append_error =
                    AppendError::internal(format!("WAL writer task failed: {join_error}"));
                reject_group(group, &append_error);
                reject_queued(&mut rx, &append_error);
                return Err(WalError::Failed);
            }
        }

        if matches!(reason, CollectReason::Shutdown) {
            return Ok(());
        }
    }
}

fn reject_group(group: Vec<Submission>, error: &AppendError) {
    for item in group {
        let _ = item.reply.send(Err(error.clone()));
    }
}

fn reject_queued(rx: &mut UnboundedReceiver<Submission>, error: &AppendError) {
    while let Ok(item) = rx.try_recv() {
        let _ = item.reply.send(Err(error.clone()));
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path, time::Instant};

    use bytes::Bytes;
    use observer_protocol::{AcceptedBatch, AppendErrorKind, IngestSink, Signal};
    use tokio::time::timeout;

    use super::*;
    use crate::{SEGMENT_HEADER_SIZE, decode};

    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    fn batch(tenant: &str, payload: &[u8]) -> AcceptedBatch {
        AcceptedBatch {
            tenant_id: tenant.to_owned(),
            signal: Signal::Logs,
            received_at_unix_nanos: 1,
            payload: Bytes::copy_from_slice(payload),
        }
    }

    fn cost(batch: &AcceptedBatch) -> usize {
        encoded_frame_size(batch.tenant_id.len(), batch.payload.len()).expect("cost")
    }

    fn test_config(directory: &Path) -> WalWriterConfig {
        let mut config = WalWriterConfig::new(directory);
        config.group_commit_deadline = Duration::from_millis(50);
        config
    }

    fn frames_on_disk(directory: &Path) -> Vec<crate::Frame> {
        let path = directory.join("lane-0000/00000000000000000000.open");
        let bytes = fs::read(path).expect("read segment");
        let mut offset = SEGMENT_HEADER_SIZE;
        let mut frames = Vec::new();
        while offset < bytes.len() {
            let (frame, consumed) = decode(&bytes[offset..]).expect("decode");
            frames.push(frame);
            offset += consumed;
        }
        frames
    }

    async fn wait_until(mut predicate: impl FnMut() -> bool) {
        timeout(TEST_TIMEOUT, async {
            while !predicate() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("condition was not met");
    }

    #[tokio::test]
    async fn acknowledges_after_sync_not_after_enqueue() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hooks = Arc::new(WalIoHooks::new());
        hooks.hold_next_sync();
        let wal =
            AsyncWal::open_with_hooks(test_config(dir.path()), Arc::clone(&hooks)).expect("open");

        let submit = tokio::spawn({
            let wal = wal.clone();
            async move { wal.submit(batch("tenant-a", b"one")).await }
        });

        timeout(TEST_TIMEOUT, hooks.sync_started().notified())
            .await
            .expect("sync did not start");
        assert!(hooks.is_sync_waiting());
        assert!(
            !submit.is_finished(),
            "submit completed before sync finished"
        );

        hooks.release_sync();
        let receipt = timeout(TEST_TIMEOUT, submit)
            .await
            .expect("submit did not finish")
            .expect("join")
            .expect("submit");
        assert_eq!(receipt.sequence, 0);
        wal.shutdown().await.expect("shutdown");
        assert_eq!(frames_on_disk(dir.path()).len(), 1);
    }

    #[tokio::test]
    async fn concurrent_callers_receive_matching_receipts() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = AsyncWal::open(test_config(dir.path())).expect("open");
        let payloads = [b"alpha".as_slice(), b"bravo", b"charlie", b"delta"];
        let mut tasks = Vec::new();
        for payload in payloads {
            let wal = wal.clone();
            tasks.push(tokio::spawn(async move {
                wal.submit(batch("tenant-a", payload)).await
            }));
        }

        let mut receipts = Vec::new();
        for task in tasks {
            receipts.push(task.await.expect("join").expect("submit"));
        }
        receipts.sort_by_key(|receipt| receipt.sequence);
        assert_eq!(
            receipts
                .iter()
                .map(|receipt| receipt.sequence)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );

        wal.shutdown().await.expect("shutdown");
        let frames = frames_on_disk(dir.path());
        let mut recovered: Vec<&[u8]> = frames.iter().map(|frame| frame.payload.as_ref()).collect();
        recovered.sort();
        let mut expected = payloads.to_vec();
        expected.sort();
        assert_eq!(recovered, expected);
    }

    #[tokio::test]
    async fn one_fsync_acknowledges_a_complete_group() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hooks = Arc::new(WalIoHooks::new());
        hooks.hold_admission();
        let mut config = test_config(dir.path());
        config.group_commit_deadline = Duration::from_secs(60);
        config.group_commit_bytes = 1024 * 1024;
        let wal = AsyncWal::open_with_hooks(config, Arc::clone(&hooks)).expect("open");

        timeout(TEST_TIMEOUT, hooks.admission_started().notified())
            .await
            .expect("writer did not pause");

        let first = batch("tenant-a", b"one");
        let second = batch("tenant-a", b"two");
        let third = batch("tenant-a", b"three");
        let expected_queued = cost(&first) + cost(&second) + cost(&third);
        let tasks: Vec<_> = [first, second, third]
            .into_iter()
            .map(|item| {
                let wal = wal.clone();
                tokio::spawn(async move { wal.submit(item).await })
            })
            .collect();

        wait_until(|| wal.queued_bytes() == expected_queued).await;
        hooks.release_admission();

        let mut receipts = Vec::new();
        for task in tasks {
            receipts.push(task.await.expect("join").expect("submit"));
        }
        assert_eq!(hooks.sync_count(), 1);
        receipts.sort_by_key(|receipt| receipt.sequence);
        assert_eq!(
            receipts
                .iter()
                .map(|receipt| receipt.sequence)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        wal.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn permits_are_released_on_success_and_error_paths() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hooks = Arc::new(WalIoHooks::new());
        let mut config = test_config(dir.path());
        config.max_queued_bytes = 1024;
        let wal = AsyncWal::open_with_hooks(config, Arc::clone(&hooks)).expect("open");
        assert_eq!(wal.queued_bytes(), 0);

        wal.submit(batch("tenant-a", b"one"))
            .await
            .expect("success");
        assert_eq!(wal.queued_bytes(), 0);

        let error = wal
            .submit(AcceptedBatch {
                tenant_id: "a".repeat(MAX_TENANT_LEN + 1),
                signal: Signal::Logs,
                received_at_unix_nanos: 1,
                payload: Bytes::from_static(b"x"),
            })
            .await
            .expect_err("oversized");
        assert_eq!(error.kind(), AppendErrorKind::InvalidArgument);
        assert_eq!(wal.queued_bytes(), 0);

        hooks.fail_next_sync();
        let error = wal
            .submit(batch("tenant-a", b"two"))
            .await
            .expect_err("sync fail");
        assert_eq!(error.kind(), AppendErrorKind::Unavailable);
        assert_eq!(wal.queued_bytes(), 0);
    }

    #[tokio::test]
    async fn saturation_rejects_without_waiting() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hooks = Arc::new(WalIoHooks::new());
        hooks.hold_admission();
        let item = batch("tenant-a", b"one");
        let mut config = test_config(dir.path());
        config.max_queued_bytes = cost(&item);
        let wal = AsyncWal::open_with_hooks(config, Arc::clone(&hooks)).expect("open");

        timeout(TEST_TIMEOUT, hooks.admission_started().notified())
            .await
            .expect("writer did not pause");

        let first = tokio::spawn({
            let wal = wal.clone();
            let item = item.clone();
            async move { wal.submit(item).await }
        });
        wait_until(|| wal.queued_bytes() == cost(&item)).await;

        let started = Instant::now();
        let error = wal
            .submit(batch("tenant-a", b"two"))
            .await
            .expect_err("saturated");
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(error.kind(), AppendErrorKind::ResourceExhausted);
        assert_eq!(wal.queued_bytes(), cost(&item));

        hooks.release_admission();
        first.await.expect("join").expect("first");
        assert_eq!(wal.queued_bytes(), 0);
        wal.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn writer_failure_propagates_to_queued_and_future_requests() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hooks = Arc::new(WalIoHooks::new());
        hooks.hold_admission();
        hooks.fail_next_sync();
        let wal =
            AsyncWal::open_with_hooks(test_config(dir.path()), Arc::clone(&hooks)).expect("open");

        timeout(TEST_TIMEOUT, hooks.admission_started().notified())
            .await
            .expect("writer did not pause");

        let first = tokio::spawn({
            let wal = wal.clone();
            async move { wal.submit(batch("tenant-a", b"one")).await }
        });
        let second = tokio::spawn({
            let wal = wal.clone();
            async move { wal.submit(batch("tenant-a", b"two")).await }
        });
        wait_until(|| {
            wal.queued_bytes()
                == cost(&batch("tenant-a", b"one")) + cost(&batch("tenant-a", b"two"))
        })
        .await;
        hooks.release_admission();

        let first = first.await.expect("join first").expect_err("first failed");
        let second = second
            .await
            .expect("join second")
            .expect_err("second failed");
        assert_eq!(first.kind(), AppendErrorKind::Unavailable);
        assert_eq!(second.kind(), AppendErrorKind::Unavailable);
        assert_eq!(wal.queued_bytes(), 0);

        let third = wal
            .submit(batch("tenant-a", b"three"))
            .await
            .expect_err("future");
        assert_eq!(third.kind(), AppendErrorKind::Unavailable);
        assert!(matches!(
            wal.shutdown().await,
            Err(WalError::Failed | WalError::Io(_))
        ));
    }

    #[tokio::test]
    async fn graceful_shutdown_flushes_accepted_requests() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hooks = Arc::new(WalIoHooks::new());
        hooks.hold_admission();
        let wal =
            AsyncWal::open_with_hooks(test_config(dir.path()), Arc::clone(&hooks)).expect("open");

        timeout(TEST_TIMEOUT, hooks.admission_started().notified())
            .await
            .expect("writer did not pause");

        let first = tokio::spawn({
            let wal = wal.clone();
            async move { wal.submit(batch("tenant-a", b"one")).await }
        });
        let second = tokio::spawn({
            let wal = wal.clone();
            async move { wal.submit(batch("tenant-a", b"two")).await }
        });
        wait_until(|| wal.queued_bytes() == cost(&batch("tenant-a", b"one")) * 2).await;

        hooks.release_admission();
        wal.shutdown().await.expect("shutdown");
        first.await.expect("join first").expect("first flushed");
        second.await.expect("join second").expect("second flushed");
        assert_eq!(frames_on_disk(dir.path()).len(), 2);

        let error = wal
            .submit(batch("tenant-a", b"three"))
            .await
            .expect_err("closed");
        assert_eq!(error.kind(), AppendErrorKind::Unavailable);
    }

    #[tokio::test]
    async fn ingest_sink_succeeds_only_after_durable_append() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = AsyncWal::open(test_config(dir.path())).expect("open");
        IngestSink::append(&wal, batch("tenant-a", b"one"))
            .await
            .expect("sink");
        wal.shutdown().await.expect("shutdown");
        assert_eq!(frames_on_disk(dir.path())[0].payload.as_ref(), b"one");
    }
}
