//! Internal SQL execution over one tenant snapshot.
//!
//! [`QueryEngine::execute`] pins one [`Store`] snapshot, registers a single `logs` table, and
//! returns an Arrow stream. Each event hour is one partition. That partition unions the hour's
//! active and frozen batches with the Parquet files named by published commits. A file that is not
//! named by a commit is not read.
//!
//! The scan projects only the physical columns DataFusion asks for. In-memory batches keep their
//! generation arrays until that projection is known; typed nulls are then created only for selected
//! columns a generation lacks. Event-time comparisons drop hours that cannot match, and committed
//! file statistics drop Parquet files that cannot match.
//! DataFusion then prunes Parquet row groups and pages with the same predicates. Every pushed
//! filter still runs again above the scan.
//!
//! Published files and in-memory batches are ordered by `event_time_unix_nano DESC`, then
//! `wal_sequence DESC`. A newest-event `ORDER BY ... LIMIT` with no other predicate can skip older
//! hours and files. Results have no order unless the SQL contains `ORDER BY`. The tenant is the
//! store passed to `execute`, never a value inside the SQL. Only one `SELECT` statement is accepted.
//!
//! One engine shares a bounded memory pool, a spill directory, a Parquet metadata cache, a query
//! runtime, and a limit on how many queries run at once. Each query uses its own session and the
//! timeout and returned-row cap in [`QueryOptions`]. That timeout bounds planning and reading. A
//! query that cannot obtain a permit before the deadline is busy. Dropping or cancelling the result
//! stream stops the query and releases its permit. [`QueryBatchStream::metrics`] reports the
//! admission wait, planning and execution time, files and row groups skipped, rows returned, the
//! shared pool's memory high-water mark, spill bytes, and whether the query timed out or was
//! cancelled.
//!
//! # Contract
//!
//! The caller selects the tenant by passing that tenant's [`Store`]. [`QueryEngine::execute`] pins
//! the snapshot at the start of the call and returns an Arrow [`QueryBatchStream`]. Rows appended
//! or published afterward stay out of that stream. The only table is `logs`, so SQL cannot name
//! another tenant.
//!
//! ```sql
//! SELECT wal_sequence, body
//! FROM logs
//! WHERE event_time_unix_nano >= 0
//! ORDER BY wal_sequence
//! LIMIT 100
//! ```
//!
//! `LIMIT` applies to the ordered rows when `ORDER BY` is present. A newest-event order can satisfy
//! that limit from the newest hours and files when the scan has no other predicate. A dynamic column is
//! `{source}_{normalized_path}_{type}` (for example `log_user_id_i64`). A generation that lacks
//! that column contributes nulls. An event-time bound can skip whole hours; DataFusion still
//! applies the nanosecond predicate.
//!
//! This crate does not expose an HTTP or gRPC query API.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::{Array, RecordBatch};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::datasource::MemTable;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::CachedParquetFileReaderFactory;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::DataFusionError;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::memory_pool::{FairSpillPool, MemoryPool, PeakRecordingPool};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::logical_expr::{
    Expr, LogicalPlan, Operator, Sort, SortExpr, TableProviderFilterPushDown, TableType, col,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, SendableRecordBatchStream,
};
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::Statement;
use futures::{Stream, StreamExt};
use observer_storage::{
    COLUMN_EVENT_TIME_UNIX_NANO, COLUMN_WAL_SEQUENCE, ColumnStatistics, EventHour, FileStatistics,
    PublishedFile, Scan, StatValue, Store, StoreError, StoreSnapshot, align_projected,
    sort_by_newest_event,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;

const LOGS_TABLE: &str = "logs";
const NANOS_PER_HOUR: u64 = 3_600_000_000_000;

/// Shared DataFusion resources for tenant-scoped SQL execution.
pub struct QueryEngine {
    runtime: Option<Arc<RuntimeEnv>>,
    spill: Option<Arc<SpillDirectory>>,
    threads: Arc<QueryThreads>,
    admission: Arc<Semaphore>,
    config: QueryEngineConfig,
}

/// Shared limits for every query on one engine.
///
/// [`Default`] derives the concurrency and partition cap once from the process parallelism. The
/// spill directory is a private path; the engine creates it when it is built and removes it when
/// the engine and its queries are finished.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryEngineConfig {
    /// Bytes available to every query that shares this engine.
    pub memory_pool_bytes: usize,
    /// Queries that may plan and read at the same time.
    pub max_concurrent_queries: usize,
    /// DataFusion partition target for each query.
    pub target_partitions_per_query: usize,
    /// Rows DataFusion places in one record batch.
    pub batch_size: usize,
    /// Bytes reserved for the shared Parquet footer and page-index cache.
    pub metadata_cache_bytes: usize,
    /// Directory that receives spill files for this engine.
    pub spill_directory: PathBuf,
    /// Maximum bytes of spill files this engine may keep at once.
    pub spill_budget_bytes: u64,
    /// Bytes an external sort reserves before it can spill.
    pub sort_spill_reservation_bytes: usize,
}

impl Default for QueryEngineConfig {
    fn default() -> Self {
        let parallelism = query_worker_threads();
        Self {
            memory_pool_bytes: QueryEngine::MEMORY_POOL_BYTES,
            max_concurrent_queries: parallelism,
            target_partitions_per_query: parallelism,
            batch_size: QueryEngine::BATCH_SIZE,
            metadata_cache_bytes: QueryEngine::METADATA_CACHE_BYTES,
            spill_directory: default_spill_directory(),
            spill_budget_bytes: QueryEngine::SPILL_BUDGET_BYTES,
            sort_spill_reservation_bytes: QueryEngine::SORT_SPILL_RESERVATION_BYTES,
        }
    }
}

/// Removes a private spill directory after the engine and its queries release it.
struct SpillDirectory {
    path: PathBuf,
}

impl Drop for SpillDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Drops the query runtime before the spill directory so spill files are closed first.
struct QueryKeepalive {
    runtime: Option<Arc<RuntimeEnv>>,
    spill: Option<Arc<SpillDirectory>>,
}

impl Drop for QueryKeepalive {
    fn drop(&mut self) {
        self.runtime.take();
        self.spill.take();
    }
}

fn default_spill_directory() -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "observer-query-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
}

/// Timeout and returned-row cap for one query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryOptions {
    /// Maximum time from the start of [`QueryEngine::execute`] until the result stream finishes.
    pub timeout: Duration,
    /// Maximum number of rows the result stream may yield.
    pub max_rows: usize,
}

impl QueryOptions {
    /// Default query timeout.
    pub const TIMEOUT: Duration = Duration::from_secs(30);
    /// Default maximum number of returned rows.
    pub const MAX_ROWS: usize = 10_000;
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            timeout: Self::TIMEOUT,
            max_rows: Self::MAX_ROWS,
        }
    }
}

/// Why a query could not be planned or executed.
#[derive(Debug)]
pub enum QueryError {
    /// The runtime could not be built, or the shared memory pool is exhausted.
    Resources(String),
    /// The pinned snapshot could not be read or aligned.
    Storage(StoreError),
    /// SQL could not be parsed or planned.
    Planning(DataFusionError),
    /// A planned query failed while executing.
    Execution(DataFusionError),
    /// The SQL is not a single `SELECT` statement.
    Statement(String),
    /// The query exceeded [`QueryOptions::timeout`].
    Timeout,
    /// The query could not obtain an admission permit before its deadline.
    Busy,
    /// The result stream was cancelled.
    Cancelled,
    /// The result would exceed [`QueryOptions::max_rows`].
    RowLimit {
        /// The cap that was exceeded.
        max_rows: usize,
    },
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Resources(detail) => write!(formatter, "query runtime: {detail}"),
            Self::Storage(error) => write!(formatter, "{error}"),
            Self::Planning(error) => write!(formatter, "query planning: {error}"),
            Self::Execution(error) => write!(formatter, "query execution: {error}"),
            Self::Statement(detail) => formatter.write_str(detail),
            Self::Timeout => formatter.write_str("query timed out"),
            Self::Busy => formatter.write_str("query engine is busy"),
            Self::Cancelled => formatter.write_str("query cancelled"),
            Self::RowLimit { max_rows } => {
                write!(formatter, "query exceeded {max_rows} returned rows")
            }
        }
    }
}

impl std::error::Error for QueryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            Self::Planning(error) | Self::Execution(error) => Some(error),
            Self::Resources(_)
            | Self::Statement(_)
            | Self::Timeout
            | Self::Busy
            | Self::Cancelled
            | Self::RowLimit { .. } => None,
        }
    }
}

impl QueryEngine {
    /// Bytes available to every query that shares this engine.
    pub const MEMORY_POOL_BYTES: usize = 256 * 1024 * 1024;
    /// Rows in one DataFusion record batch.
    pub const BATCH_SIZE: usize = 8192;
    /// Shared Parquet metadata cache, kept apart from the execution memory pool.
    pub const METADATA_CACHE_BYTES: usize = 32 * 1024 * 1024;
    /// Spill files this engine may keep at once.
    pub const SPILL_BUDGET_BYTES: u64 = 1024 * 1024 * 1024;
    /// Memory an external sort keeps available so it can spill the rest.
    pub const SORT_SPILL_RESERVATION_BYTES: usize = 10 * 1024 * 1024;
    /// Bytes read from the end of a Parquet file when looking for the footer.
    pub const PARQUET_FOOTER_HINT_BYTES: usize = 512 * 1024;
    /// Smallest file scan DataFusion may split across partitions.
    pub const FILE_SCAN_MIN_BYTES: usize = 1024 * 1024;

    /// Build a runtime from `config`.
    ///
    /// Planning and execution run on threads named `observer-query`. At most
    /// [`QueryEngineConfig::max_concurrent_queries`] queries hold an admission permit at once.
    /// A result stream keeps the query threads and its permit alive until the query finishes or
    /// the stream is dropped. Spill files live under [`QueryEngineConfig::spill_directory`] and
    /// are removed when the engine and its queries finish.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::Resources`] when the runtime or spill directory cannot be constructed,
    /// or when a concurrency setting or the batch size is zero.
    pub fn new(config: QueryEngineConfig) -> Result<Self, QueryError> {
        Self::with_threads(config, QueryThreads::start()?)
    }

    fn with_threads(config: QueryEngineConfig, threads: QueryThreads) -> Result<Self, QueryError> {
        let config = config.checked()?;
        std::fs::create_dir_all(&config.spill_directory).map_err(|error| {
            QueryError::Resources(format!(
                "create spill directory {}: {error}",
                config.spill_directory.display()
            ))
        })?;
        let spill = Arc::new(SpillDirectory {
            path: config.spill_directory.clone(),
        });
        let pool: Arc<dyn MemoryPool> = Arc::new(PeakRecordingPool::new(Arc::new(
            FairSpillPool::new(config.memory_pool_bytes),
        )));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(pool)
            .with_temp_file_path(config.spill_directory.clone())
            .with_max_temp_directory_size(config.spill_budget_bytes)
            .with_metadata_cache_limit(config.metadata_cache_bytes)
            .build()
            .map_err(|error| QueryError::Resources(error.to_string()))?;
        Ok(Self {
            runtime: Some(Arc::new(runtime)),
            spill: Some(spill),
            threads: Arc::new(threads),
            admission: Arc::new(Semaphore::new(config.max_concurrent_queries)),
            config,
        })
    }

    /// Build an engine whose query clock starts paused.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::Resources`] when the runtime cannot be constructed, or when a
    /// concurrency setting is zero.
    #[cfg(test)]
    fn paused(config: QueryEngineConfig) -> Result<Self, QueryError> {
        Self::with_threads(config, QueryThreads::start_paused()?)
    }

    /// Advance the query runtime's clock.
    #[cfg(test)]
    async fn advance_time(&self, duration: Duration) {
        let (finished, finished_rx) = oneshot::channel();
        self.threads.spawn(async move {
            tokio::time::advance(duration).await;
            let _ = finished.send(());
        });
        finished_rx.await.expect("query clock");
    }

    /// Shared execution resources for every query-local session.
    #[must_use]
    pub fn runtime(&self) -> &RuntimeEnv {
        self.runtime.as_ref().expect("query runtime")
    }

    fn spill_directory(&self) -> Arc<SpillDirectory> {
        Arc::clone(self.spill.as_ref().expect("spill directory"))
    }

    /// Run one `SELECT` against the rows visible in `store` at call time.
    ///
    /// The returned stream has no order unless `sql` contains `ORDER BY`. `options.timeout` is one
    /// deadline for validation, planning, stream creation, and reading. `options.max_rows` is the
    /// most rows the stream yields before it returns [`QueryError::RowLimit`].
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::Statement`] for an empty string, multiple statements, or any statement
    /// other than `SELECT`. [`QueryError::Busy`] means the admission permit was not available
    /// before the deadline. Storage, planning, execution, timeout, and resource failures use the
    /// matching variant. Failures that happen before the stream is ready are returned here.
    pub async fn execute(
        &self,
        store: Arc<Store>,
        sql: &str,
        options: QueryOptions,
    ) -> Result<QueryBatchStream, QueryError> {
        self.execute_input(QueryInput::Store(store), sql, options)
            .await
    }

    /// Run `sql` against a test table instead of a tenant snapshot.
    #[cfg(test)]
    async fn execute_provider(
        &self,
        provider: Arc<dyn TableProvider>,
        sql: &str,
        options: QueryOptions,
    ) -> Result<QueryBatchStream, QueryError> {
        self.execute_input(QueryInput::Provider(provider), sql, options)
            .await
    }

    async fn execute_input(
        &self,
        input: QueryInput,
        sql: &str,
        options: QueryOptions,
    ) -> Result<QueryBatchStream, QueryError> {
        if options.timeout.is_zero() {
            return Err(QueryError::Timeout);
        }
        let (ready_tx, ready_rx) = oneshot::channel();
        let (pull_tx, pull_rx) = mpsc::channel(1);
        let runtime = Arc::clone(self.runtime.as_ref().expect("query runtime"));
        let keepalive = QueryKeepalive {
            runtime: Some(Arc::clone(&runtime)),
            spill: Some(self.spill_directory()),
        };
        let admission = Arc::clone(&self.admission);
        let session = session_config(&self.config);
        let sql = sql.to_owned();
        let max_rows = options.max_rows;
        let timeout = options.timeout;
        let record = Arc::new(QueryRecord::default());
        let recorded = Arc::clone(&record);
        let mut task = TaskGuard(Some(self.threads.spawn(async move {
            let _keepalive = keepalive;
            match prepare(
                Arc::clone(&runtime),
                admission,
                session,
                input,
                sql,
                timeout,
                Arc::clone(&recorded),
            )
            .await
            {
                Err(error) => {
                    let _ = ready_tx.send(Err(error));
                }
                Ok((stream, deadline, permit, plan)) => {
                    if ready_tx.send(Ok(permit)).is_err() {
                        recorded.mark_cancelled();
                        return;
                    }
                    forward(stream, pull_rx, deadline, max_rows, plan, recorded, runtime).await;
                }
            }
        })));
        match ready_rx.await {
            Ok(Ok(permit)) => Ok(QueryBatchStream {
                pull_tx: Some(pull_tx),
                pending: None,
                task: task.0.take(),
                terminal: None,
                finished: false,
                permit: Some(permit),
                threads: Arc::clone(&self.threads),
                record,
            }),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(query_task_stopped()),
        }
    }
}

impl Drop for QueryEngine {
    fn drop(&mut self) {
        self.runtime.take();
        self.spill.take();
    }
}

/// Tokio runtime that plans and executes queries away from the caller.
struct QueryThreads {
    handle: tokio::runtime::Handle,
    shutdown: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl QueryThreads {
    fn start() -> Result<Self, QueryError> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(query_worker_threads())
            .thread_name("observer-query")
            .enable_all()
            .build()
            .map_err(|error| QueryError::Resources(error.to_string()))?;
        let handle = runtime.handle().clone();
        Ok(Self {
            handle,
            shutdown: Mutex::new(Some(Box::new(move || runtime.shutdown_background()))),
        })
    }

    /// A paused clock is only available on a current-thread runtime, so a dedicated thread drives it.
    #[cfg(test)]
    fn start_paused() -> Result<Self, QueryError> {
        let (handle_tx, handle_rx) = std::sync::mpsc::sync_channel(1);
        let (stop_tx, stop_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let driver = thread::Builder::new()
            .name("observer-query".to_owned())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .start_paused(true)
                    .build()
                    .expect("paused query runtime");
                handle_tx
                    .send(runtime.handle().clone())
                    .expect("query handle");
                runtime.block_on(async move {
                    // A running blocking task keeps the paused clock from jumping to the next timer.
                    let inhibit = tokio::task::spawn_blocking(move || {
                        let _ = release_rx.recv();
                    });
                    let _ = stop_rx.await;
                    drop(release_tx);
                    let _ = inhibit.await;
                });
            })
            .map_err(|error| QueryError::Resources(error.to_string()))?;
        let handle = handle_rx
            .recv()
            .map_err(|error| QueryError::Resources(error.to_string()))?;
        Ok(Self {
            handle,
            shutdown: Mutex::new(Some(Box::new(move || {
                let _ = stop_tx.send(());
                let _ = driver.join();
            }))),
        })
    }

    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }
}

impl Drop for QueryThreads {
    fn drop(&mut self) {
        let shutdown = self
            .shutdown
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(shutdown) = shutdown {
            shutdown();
        }
    }
}

/// Aborts a query task unless the result stream has taken ownership of it.
struct TaskGuard(Option<JoinHandle<()>>);

impl Drop for TaskGuard {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

enum QueryInput {
    Store(Arc<Store>),
    #[cfg(test)]
    Provider(Arc<dyn TableProvider>),
}

type BatchReply = Option<Result<RecordBatch, QueryError>>;

struct Pull {
    reply: oneshot::Sender<BatchReply>,
}

fn query_worker_threads() -> usize {
    thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1)
}

async fn prepare(
    runtime: Arc<RuntimeEnv>,
    admission: Arc<Semaphore>,
    session: SessionConfig,
    input: QueryInput,
    sql: String,
    timeout: Duration,
    record: Arc<QueryRecord>,
) -> Result<
    (
        SendableRecordBatchStream,
        tokio::time::Instant,
        OwnedSemaphorePermit,
        Arc<dyn ExecutionPlan>,
    ),
    QueryError,
> {
    let started = tokio::time::Instant::now();
    let deadline = started + timeout;
    ensure_query(&sql)?;
    if tokio::time::Instant::now() >= deadline {
        return Err(QueryError::Timeout);
    }
    let permit = match tokio::time::timeout_at(deadline, admission.acquire_owned()).await {
        Ok(Ok(permit)) => permit,
        Ok(Err(_closed)) => return Err(QueryError::Busy),
        Err(_elapsed) => return Err(QueryError::Busy),
    };
    let admitted = tokio::time::Instant::now();
    let admission_wait = admitted.saturating_duration_since(started);
    let prepared = tokio::time::timeout_at(deadline, async move {
        let (provider, files) = table_provider(input)?;
        let context = SessionContext::new_with_config_rt(session, runtime);
        context.add_optimizer_rule(Arc::new(NewestEventLimit));
        context
            .register_table(LOGS_TABLE, provider)
            .map_err(QueryError::Planning)?;
        let frame = context.sql(&sql).await.map_err(QueryError::Planning)?;
        let plan = frame
            .create_physical_plan()
            .await
            .map_err(execution_error)?;
        let stream = datafusion::physical_plan::execute_stream(
            Arc::clone(&plan),
            Arc::new(frame.task_ctx()),
        )
        .map_err(execution_error)?;
        let (scanned, pruned) = files.snapshot();
        record.set_planned(admission_wait, admitted.elapsed(), scanned, pruned);
        Ok((stream, plan))
    })
    .await;
    match prepared {
        Ok(Ok((stream, plan))) => Ok((stream, deadline, permit, plan)),
        Ok(Err(error)) => Err(error),
        Err(_elapsed) => Err(QueryError::Timeout),
    }
}

impl QueryEngineConfig {
    fn checked(self) -> Result<Self, QueryError> {
        if self.max_concurrent_queries == 0 {
            return Err(QueryError::Resources(
                "max_concurrent_queries must be at least 1".to_owned(),
            ));
        }
        if self.target_partitions_per_query == 0 {
            return Err(QueryError::Resources(
                "target_partitions_per_query must be at least 1".to_owned(),
            ));
        }
        if self.batch_size == 0 {
            return Err(QueryError::Resources(
                "batch_size must be at least 1".to_owned(),
            ));
        }
        if self.spill_directory.as_os_str().is_empty() {
            return Err(QueryError::Resources(
                "spill_directory must be set".to_owned(),
            ));
        }
        Ok(self)
    }
}

fn session_config(config: &QueryEngineConfig) -> SessionConfig {
    let mut session = SessionConfig::new()
        .with_batch_size(config.batch_size)
        .with_target_partitions(config.target_partitions_per_query)
        .with_collect_statistics(true)
        .with_repartition_file_scans(true)
        .with_repartition_file_min_size(QueryEngine::FILE_SCAN_MIN_BYTES)
        .with_parquet_pruning(true)
        .with_parquet_page_index_pruning(true)
        .with_parquet_bloom_filter_pruning(false)
        .with_sort_spill_reservation_bytes(config.sort_spill_reservation_bytes);
    let parquet = &mut session.options_mut().execution.parquet;
    parquet.pushdown_filters = true;
    parquet.reorder_filters = true;
    parquet.metadata_size_hint = Some(QueryEngine::PARQUET_FOOTER_HINT_BYTES);
    session
}

fn table_provider(
    input: QueryInput,
) -> Result<(Arc<dyn TableProvider>, Arc<FileCounts>), QueryError> {
    match input {
        QueryInput::Store(store) => {
            let snapshot = store.snapshot().map_err(QueryError::Storage)?;
            let provider = ObserverTableProvider::new(store, snapshot)?;
            let files = Arc::clone(&provider.files);
            Ok((Arc::new(provider), files))
        }
        #[cfg(test)]
        QueryInput::Provider(provider) => Ok((provider, Arc::new(FileCounts::default()))),
    }
}

async fn forward(
    mut stream: SendableRecordBatchStream,
    mut pulls: mpsc::Receiver<Pull>,
    deadline: tokio::time::Instant,
    max_rows: usize,
    plan: Arc<dyn ExecutionPlan>,
    record: Arc<QueryRecord>,
    runtime: Arc<RuntimeEnv>,
) {
    let started = tokio::time::Instant::now();
    record.note_execution_start(started);
    let mut emitted = 0usize;
    loop {
        let pull = tokio::select! {
            biased;
            () = tokio::time::sleep_until(deadline) => {
                record.finish(QueryStop::Timeout, emitted, &plan, &runtime, started);
                deliver_timeout(&mut pulls).await;
                return;
            }
            pull = pulls.recv() => {
                let Some(pull) = pull else {
                    record.finish(QueryStop::Cancelled, emitted, &plan, &runtime, started);
                    return;
                };
                pull
            }
        };
        if tokio::time::Instant::now() >= deadline {
            record.finish(QueryStop::Timeout, emitted, &plan, &runtime, started);
            let _ = pull.reply.send(Some(Err(QueryError::Timeout)));
            return;
        }
        let next = stream.next();
        tokio::pin!(next);
        let item = tokio::select! {
            biased;
            () = tokio::time::sleep_until(deadline) => Err(QueryError::Timeout),
            batch = &mut next => match batch {
                None => {
                    record.finish(QueryStop::Finished, emitted, &plan, &runtime, started);
                    let _ = pull.reply.send(None);
                    return;
                }
                Some(Ok(batch)) => Ok(batch),
                Some(Err(error)) => Err(execution_error(error)),
            },
        };
        match item {
            Ok(batch) => {
                let rows = batch.num_rows();
                if rows > 0 && emitted.saturating_add(rows) > max_rows {
                    record.finish(QueryStop::Finished, emitted, &plan, &runtime, started);
                    let _ = pull
                        .reply
                        .send(Some(Err(QueryError::RowLimit { max_rows })));
                    return;
                }
                emitted += rows;
                record.observe(emitted, &plan, &runtime, started);
                if pull.reply.send(Some(Ok(batch))).is_err() {
                    record.finish(QueryStop::Cancelled, emitted, &plan, &runtime, started);
                    return;
                }
            }
            Err(error) => {
                let stop = if matches!(error, QueryError::Timeout) {
                    QueryStop::Timeout
                } else {
                    QueryStop::Finished
                };
                record.finish(stop, emitted, &plan, &runtime, started);
                let _ = pull.reply.send(Some(Err(error)));
                return;
            }
        }
    }
}

async fn deliver_timeout(pulls: &mut mpsc::Receiver<Pull>) {
    if let Some(pull) = pulls.recv().await {
        let _ = pull.reply.send(Some(Err(QueryError::Timeout)));
    }
}

fn query_task_stopped() -> QueryError {
    QueryError::Execution(DataFusionError::Execution("query task stopped".into()))
}

/// Counters for one query.
///
/// `memory_peak_bytes` is the high-water mark of the engine's shared pool since the engine was
/// built. Concurrent queries share that pool, so the mark is not isolated to this query. Spill
/// bytes and pruned row groups are the totals DataFusion recorded on the physical plan. File
/// counts are the Parquet files this scan kept or dropped before reading.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueryMetrics {
    /// Time spent waiting for an admission permit.
    pub admission_wait: Duration,
    /// Time from acquiring the permit until the stream is ready.
    pub planning: Duration,
    /// Time spent reading after the stream is ready.
    pub execution: Duration,
    /// Parquet files included in the scan.
    pub files_scanned: u64,
    /// Parquet files dropped before the scan.
    pub files_pruned: u64,
    /// Parquet row groups DataFusion skipped with statistics.
    pub row_groups_pruned: u64,
    /// Rows the stream has yielded.
    pub rows_returned: u64,
    /// High-water mark of the shared memory pool, in bytes.
    pub memory_peak_bytes: u64,
    /// Bytes DataFusion operators wrote to spill files.
    pub spill_bytes: u64,
    /// The query hit [`QueryOptions::timeout`].
    pub timed_out: bool,
    /// The caller cancelled the stream or dropped it before it finished.
    pub cancelled: bool,
}

struct FileCounts {
    scanned: AtomicU64,
    pruned: AtomicU64,
}

impl Default for FileCounts {
    fn default() -> Self {
        Self {
            scanned: AtomicU64::new(0),
            pruned: AtomicU64::new(0),
        }
    }
}

impl FileCounts {
    fn snapshot(&self) -> (u64, u64) {
        (
            self.scanned.load(Ordering::Relaxed),
            self.pruned.load(Ordering::Relaxed),
        )
    }
}

enum QueryStop {
    Finished,
    Timeout,
    Cancelled,
}

struct QueryRecord {
    metrics: Mutex<QueryMetrics>,
    execution_started: Mutex<Option<tokio::time::Instant>>,
}

impl Default for QueryRecord {
    fn default() -> Self {
        Self {
            metrics: Mutex::new(QueryMetrics::default()),
            execution_started: Mutex::new(None),
        }
    }
}

impl QueryRecord {
    fn lock(&self) -> std::sync::MutexGuard<'_, QueryMetrics> {
        self.metrics
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    fn set_planned(&self, admission_wait: Duration, planning: Duration, scanned: u64, pruned: u64) {
        let mut metrics = self.lock();
        metrics.admission_wait = admission_wait;
        metrics.planning = planning;
        metrics.files_scanned = scanned;
        metrics.files_pruned = pruned;
    }

    fn note_execution_start(&self, started: tokio::time::Instant) {
        *self
            .execution_started
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(started);
    }

    fn mark_cancelled(&self) {
        if let Some(started) = *self
            .execution_started
            .lock()
            .unwrap_or_else(|error| error.into_inner())
        {
            self.lock().execution = started.elapsed();
        }
        self.finish_flags(QueryStop::Cancelled);
    }

    fn finish_flags(&self, stop: QueryStop) {
        let mut metrics = self.lock();
        match stop {
            QueryStop::Finished => {}
            QueryStop::Timeout => metrics.timed_out = true,
            QueryStop::Cancelled if !metrics.timed_out => metrics.cancelled = true,
            QueryStop::Cancelled => {}
        }
    }

    fn observe(
        &self,
        rows: usize,
        plan: &Arc<dyn ExecutionPlan>,
        runtime: &RuntimeEnv,
        started: tokio::time::Instant,
    ) {
        let mut metrics = self.lock();
        metrics.rows_returned = u64::try_from(rows).unwrap_or(u64::MAX);
        metrics.execution = started.elapsed();
        apply_plan_metrics(&mut metrics, plan);
        metrics.memory_peak_bytes = memory_peak(runtime);
    }

    fn finish(
        &self,
        stop: QueryStop,
        rows: usize,
        plan: &Arc<dyn ExecutionPlan>,
        runtime: &RuntimeEnv,
        started: tokio::time::Instant,
    ) {
        self.observe(rows, plan, runtime, started);
        self.finish_flags(stop);
    }
}

fn apply_plan_metrics(metrics: &mut QueryMetrics, plan: &Arc<dyn ExecutionPlan>) {
    let mut spill = 0u64;
    let mut row_groups = 0u64;
    collect_operator_metrics(plan, &mut spill, &mut row_groups);
    metrics.spill_bytes = spill;
    metrics.row_groups_pruned = row_groups;
}

fn collect_operator_metrics(plan: &Arc<dyn ExecutionPlan>, spill: &mut u64, row_groups: &mut u64) {
    if let Some(metrics) = plan.metrics() {
        if let Some(bytes) = metrics.spilled_bytes() {
            *spill += u64::try_from(bytes).unwrap_or(u64::MAX);
        }
        if let Some(MetricValue::PruningMetrics {
            pruning_metrics, ..
        }) = metrics.sum_by_name("row_groups_pruned_statistics")
        {
            *row_groups += u64::try_from(pruning_metrics.pruned()).unwrap_or(u64::MAX);
        }
    }
    for child in plan.children() {
        collect_operator_metrics(child, spill, row_groups);
    }
}

fn memory_peak(runtime: &RuntimeEnv) -> u64 {
    PeakRecordingPool::from_pool(runtime.memory_pool.as_ref())
        .map(|pool| u64::try_from(pool.max_reserved()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// Arrow batches for one query, with the engine's timeout and row cap applied.
pub struct QueryBatchStream {
    pull_tx: Option<mpsc::Sender<Pull>>,
    pending: Option<oneshot::Receiver<BatchReply>>,
    task: Option<JoinHandle<()>>,
    terminal: Option<QueryError>,
    finished: bool,
    permit: Option<OwnedSemaphorePermit>,
    /// Keeps the query threads alive until this stream is dropped.
    #[allow(dead_code)]
    threads: Arc<QueryThreads>,
    record: Arc<QueryRecord>,
}

impl QueryBatchStream {
    /// Counters recorded for this query.
    ///
    /// Counts grow as batches are read. After the stream finishes, times out, or is cancelled,
    /// the flags and the plan counters stay at the values recorded then.
    #[must_use]
    pub fn metrics(&self) -> QueryMetrics {
        *self.record.lock()
    }

    /// Stop the query. The next poll returns [`QueryError::Cancelled`] and no further batches.
    pub fn cancel(&mut self) {
        if self.finished || self.terminal.is_some() {
            return;
        }
        self.record.mark_cancelled();
        self.terminal = Some(QueryError::Cancelled);
        self.permit.take();
        self.stop_task();
    }

    fn stop_task(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
        self.pull_tx = None;
    }

    fn finish(&mut self) {
        self.finished = true;
        self.permit.take();
        self.stop_task();
    }
}

impl Drop for QueryBatchStream {
    fn drop(&mut self) {
        if !self.finished && self.terminal.is_none() {
            self.record.mark_cancelled();
        }
        self.stop_task();
    }
}

impl Stream for QueryBatchStream {
    type Item = Result<RecordBatch, QueryError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let stream = self.get_mut();
        if stream.finished {
            return Poll::Ready(None);
        }
        if let Some(error) = stream.terminal.take() {
            stream.finish();
            return Poll::Ready(Some(Err(error)));
        }
        if stream.pending.is_none() {
            let (reply, receiver) = oneshot::channel();
            let sent = stream
                .pull_tx
                .as_ref()
                .is_some_and(|pull_tx| pull_tx.try_send(Pull { reply }).is_ok());
            if !sent {
                stream.finish();
                return Poll::Ready(Some(Err(query_task_stopped())));
            }
            stream.pending = Some(receiver);
        }
        let polled = stream
            .pending
            .as_mut()
            .map(|pending| Pin::new(pending).poll(context));
        match polled {
            Some(Poll::Pending) => Poll::Pending,
            Some(Poll::Ready(Ok(item))) => {
                stream.pending = None;
                if item.as_ref().is_none_or(Result::is_err) {
                    stream.finish();
                }
                Poll::Ready(item)
            }
            Some(Poll::Ready(Err(_))) | None => {
                stream.pending = None;
                stream.finish();
                Poll::Ready(Some(Err(query_task_stopped())))
            }
        }
    }
}

fn execution_error(error: DataFusionError) -> QueryError {
    if resource_limit(&error) {
        QueryError::Resources(error.to_string())
    } else {
        QueryError::Execution(error)
    }
}

fn resource_limit(error: &DataFusionError) -> bool {
    match error {
        DataFusionError::ResourcesExhausted(_) => true,
        DataFusionError::Context(_, inner) | DataFusionError::Diagnostic(_, inner) => {
            resource_limit(inner)
        }
        DataFusionError::Shared(inner) => resource_limit(inner),
        DataFusionError::Collection(errors) => errors.iter().any(resource_limit),
        DataFusionError::External(inner) => {
            inner
                .downcast_ref::<DataFusionError>()
                .is_some_and(resource_limit)
                || spill_budget_exhausted(&inner.to_string())
        }
        other => spill_budget_exhausted(&other.to_string()),
    }
}

fn spill_budget_exhausted(text: &str) -> bool {
    text.contains("exceeded the allowable limit")
}

/// One event hour's visible batches and commit-referenced Parquet files.
struct HourPartition {
    hour: EventHour,
    files: Vec<PublishedFile>,
    batches: Vec<Arc<RecordBatch>>,
}

/// DataFusion table for one pinned tenant snapshot.
///
/// Each [`HourPartition`] becomes one output partition. Memory batches stay in the generation
/// schema they were stored with. A scan aligns the columns DataFusion projects, after hours and
/// files that cannot match have been dropped.
pub struct ObserverTableProvider {
    store: Arc<Store>,
    snapshot: StoreSnapshot,
    schema: SchemaRef,
    hours: Vec<HourPartition>,
    files: Arc<FileCounts>,
}

impl std::fmt::Debug for ObserverTableProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObserverTableProvider")
            .field("epoch", &self.snapshot.epoch)
            .field("hours", &self.hours.len())
            .field("store", &Arc::as_ptr(&self.store))
            .finish_non_exhaustive()
    }
}

impl ObserverTableProvider {
    /// Keep the snapshot's in-memory batches and the commit-referenced Parquet paths.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::Storage`] when the snapshot schema is incompatible.
    pub fn new(store: Arc<Store>, snapshot: StoreSnapshot) -> Result<Self, QueryError> {
        let schema = snapshot.schema().map_err(QueryError::Storage)?;
        let mut hours = Vec::new();
        for hour in store.sources(&snapshot, &Scan::default()) {
            if hour.files.is_empty() && hour.batches.is_empty() {
                continue;
            }
            hours.push(HourPartition {
                hour: hour.hour,
                files: hour.files,
                batches: hour.batches,
            });
        }
        Ok(Self {
            store,
            snapshot,
            schema,
            hours,
            files: Arc::new(FileCounts::default()),
        })
    }

    fn record_files(&self, pieces: &[ScanPiece]) {
        let scanned = pieces
            .iter()
            .filter(|piece| matches!(piece.source, ScanSource::File(_)))
            .count();
        let total: usize = self.hours.iter().map(|hour| hour.files.len()).sum();
        self.files.scanned.store(
            u64::try_from(scanned).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.files.pruned.store(
            u64::try_from(total.saturating_sub(scanned)).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }
}

#[async_trait]
impl TableProvider for ObserverTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if prunable(filter) || time_predicate(filter).is_some() {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let window = hour_window(filters);
        let mut pieces = Vec::new();
        for hour in &self.hours {
            if !window.contains(hour.hour) {
                continue;
            }
            pieces.extend(hour_pieces(hour, filters, &self.schema, projection)?);
        }
        let pieces = keep_newest_prefix(pieces, limit, filters.is_empty());
        self.record_files(&pieces);
        if pieces.is_empty() {
            let table = MemTable::try_new(output_schema(&self.schema, projection)?, vec![vec![]])?;
            return table.scan(state, None, &[], None).await;
        }
        let advertise = pieces.iter().all(|piece| piece.ordered)
            && order_columns_projected(self.schema.as_ref(), projection);
        let read_limit = filters.is_empty().then_some(limit).flatten();
        let files_only = pieces
            .iter()
            .all(|piece| matches!(piece.source, ScanSource::File(_)));
        if advertise && files_only {
            let files = pieces
                .into_iter()
                .filter_map(|piece| match piece.source {
                    ScanSource::File(file) => Some(file),
                    ScanSource::Memory(_) => None,
                })
                .collect::<Vec<_>>();
            let file_limit = (files.len() == 1).then_some(read_limit).flatten();
            let plan = parquet_plan(
                state,
                &self.schema,
                &files,
                projection,
                filters,
                file_limit,
                true,
            )
            .await?;
            if files.len() > 1
                && let Some(ordering) = projected_newest_ordering(self.schema.as_ref(), projection)
            {
                return Ok(Arc::new(
                    SortPreservingMergeExec::new(ordering, plan)
                        .with_round_robin_repartition(false),
                ));
            }
            return Ok(plan);
        }
        let mut plans = Vec::with_capacity(pieces.len());
        for piece in pieces {
            plans.push(
                piece
                    .plan(
                        state,
                        &self.schema,
                        projection,
                        filters,
                        read_limit,
                        advertise,
                    )
                    .await?,
            );
        }
        one_partition(UnionExec::try_new(plans)?)
    }
}

/// Inclusive start and exclusive end of the event hours a predicate can touch.
#[derive(Clone, Copy)]
struct HourWindow {
    from: Option<EventHour>,
    to: Option<EventHour>,
    impossible: bool,
}

impl HourWindow {
    fn unbounded() -> Self {
        Self {
            from: None,
            to: None,
            impossible: false,
        }
    }

    fn contains(self, hour: EventHour) -> bool {
        !self.impossible
            && self.from.is_none_or(|start| hour >= start)
            && self.to.is_none_or(|end| hour < end)
    }

    fn intersect(&mut self, other: Self) {
        if other.impossible {
            self.impossible = true;
            return;
        }
        self.from = match (self.from, other.from) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (Some(bound), None) | (None, Some(bound)) => Some(bound),
            (None, None) => None,
        };
        self.to = match (self.to, other.to) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(bound), None) | (None, Some(bound)) => Some(bound),
            (None, None) => None,
        };
        if let (Some(from), Some(to)) = (self.from, self.to)
            && from >= to
        {
            self.impossible = true;
        }
    }
}

fn prunable(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            prunable(&binary.left) && prunable(&binary.right)
        }
        Expr::Between(between) if !between.negated => {
            column_name(&between.expr).is_some()
                && literal_stat(&between.low).is_some()
                && literal_stat(&between.high).is_some()
        }
        Expr::IsNull(inner) => column_name(inner).is_some(),
        Expr::BinaryExpr(binary) if is_order_comparison(binary.op) || binary.op == Operator::Eq => {
            compared_column(binary).is_some()
        }
        _ => false,
    }
}

fn parquet_predicate(
    state: &dyn Session,
    schema: &SchemaRef,
    filters: &[Expr],
) -> datafusion::error::Result<Option<Arc<dyn PhysicalExpr>>> {
    let mut combined: Option<Expr> = None;
    for filter in filters {
        if !prunable(filter) {
            continue;
        }
        combined = Some(match combined {
            None => filter.clone(),
            Some(existing) => existing.and(filter.clone()),
        });
    }
    let Some(expr) = combined else {
        return Ok(None);
    };
    let Ok(df_schema) = DFSchema::try_from(schema.as_ref().clone()) else {
        return Ok(None);
    };
    Ok(state.create_physical_expr(expr, &df_schema).ok())
}

fn file_may_match(statistics: Option<&FileStatistics>, filters: &[Expr]) -> bool {
    let Some(statistics) = statistics else {
        return true;
    };
    filters
        .iter()
        .all(|filter| predicate_may_match(statistics, filter))
}

fn predicate_may_match(statistics: &FileStatistics, expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            predicate_may_match(statistics, &binary.left)
                && predicate_may_match(statistics, &binary.right)
        }
        Expr::Between(between) if !between.negated => {
            let Some(name) = column_name(&between.expr) else {
                return true;
            };
            let (Some(low), Some(high)) = (literal_stat(&between.low), literal_stat(&between.high))
            else {
                return true;
            };
            !between_excludes(statistics, column_by_name(statistics, &name), &low, &high)
        }
        Expr::IsNull(inner) => {
            let Some(name) = column_name(inner) else {
                return true;
            };
            match column_by_name(statistics, &name) {
                ColumnLookup::Absent => true,
                ColumnLookup::Present(column) => column.null_count > 0,
            }
        }
        Expr::BinaryExpr(binary) if is_order_comparison(binary.op) || binary.op == Operator::Eq => {
            let Some((name, operator, literal)) = compared_column(binary) else {
                return true;
            };
            !comparison_excludes(
                statistics,
                column_by_name(statistics, &name),
                operator,
                &literal,
            )
        }
        _ => true,
    }
}

enum ColumnLookup<'a> {
    Absent,
    Present(&'a ColumnStatistics),
}

fn column_by_name<'a>(statistics: &'a FileStatistics, name: &str) -> ColumnLookup<'a> {
    match statistics.columns.iter().find(|column| column.name == name) {
        Some(column) => ColumnLookup::Present(column),
        None => ColumnLookup::Absent,
    }
}

fn between_excludes(
    statistics: &FileStatistics,
    column: ColumnLookup<'_>,
    low: &StatValue,
    high: &StatValue,
) -> bool {
    if low.less_than(high) == Some(false) && low != high {
        return true;
    }
    let ColumnLookup::Present(column) = column else {
        return true;
    };
    if column.null_count == statistics.rows {
        return true;
    }
    let Some((min, max)) = column.bounds.as_ref() else {
        return false;
    };
    let Some(low) = coerce_stat(low, min) else {
        return false;
    };
    let Some(high) = coerce_stat(high, min) else {
        return false;
    };
    max.less_than(&low) == Some(true) || high.less_than(min) == Some(true)
}

fn comparison_excludes(
    statistics: &FileStatistics,
    column: ColumnLookup<'_>,
    operator: Operator,
    literal: &StatValue,
) -> bool {
    let ColumnLookup::Present(column) = column else {
        return true;
    };
    if column.null_count == statistics.rows {
        return true;
    }
    let Some((min, max)) = column.bounds.as_ref() else {
        return false;
    };
    let Some(literal) = coerce_stat(literal, min) else {
        return false;
    };
    match operator {
        Operator::Eq => {
            literal.less_than(min) == Some(true) || max.less_than(&literal) == Some(true)
        }
        Operator::Gt => max.less_than(&literal) == Some(true) || max == &literal,
        Operator::GtEq => max.less_than(&literal) == Some(true),
        Operator::Lt => literal.less_than(min) == Some(true) || &literal == min,
        Operator::LtEq => literal.less_than(min) == Some(true),
        _ => false,
    }
}

fn coerce_stat(literal: &StatValue, target: &StatValue) -> Option<StatValue> {
    if literal.data_type() == target.data_type() {
        return Some(literal.clone());
    }
    let number = match literal {
        StatValue::Int32(value) => i128::from(*value),
        StatValue::Int64(value) => i128::from(*value),
        StatValue::UInt16(value) => i128::from(*value),
        StatValue::UInt32(value) => i128::from(*value),
        StatValue::UInt64(value) => i128::from(*value),
        _ => return None,
    };
    match target {
        StatValue::Int32(_) => i32::try_from(number).ok().map(StatValue::Int32),
        StatValue::Int64(_) => i64::try_from(number).ok().map(StatValue::Int64),
        StatValue::UInt16(_) => u16::try_from(number).ok().map(StatValue::UInt16),
        StatValue::UInt32(_) => u32::try_from(number).ok().map(StatValue::UInt32),
        StatValue::UInt64(_) => u64::try_from(number).ok().map(StatValue::UInt64),
        _ => None,
    }
}

fn compared_column(
    binary: &datafusion::logical_expr::BinaryExpr,
) -> Option<(String, Operator, StatValue)> {
    let (name, operator, literal) = match (
        column_name(&binary.left),
        binary.op,
        literal_stat(&binary.right),
    ) {
        (Some(name), operator, Some(literal)) => (name, operator, literal),
        _ => {
            let name = column_name(&binary.right)?;
            let literal = literal_stat(&binary.left)?;
            let operator = flip_comparison(binary.op)?;
            (name, operator, literal)
        }
    };
    Some((name, operator, literal))
}

fn column_name(expr: &Expr) -> Option<String> {
    bare(expr).try_as_col().map(|column| column.name.clone())
}

fn literal_stat(expr: &Expr) -> Option<StatValue> {
    let Expr::Literal(value, _) = bare(expr) else {
        return None;
    };
    match value {
        ScalarValue::Boolean(Some(value)) => Some(StatValue::Bool(*value)),
        ScalarValue::Int32(Some(value)) => Some(StatValue::Int32(*value)),
        ScalarValue::Int64(Some(value)) => Some(StatValue::Int64(*value)),
        ScalarValue::UInt16(Some(value)) => Some(StatValue::UInt16(*value)),
        ScalarValue::UInt32(Some(value)) => Some(StatValue::UInt32(*value)),
        ScalarValue::UInt64(Some(value)) => Some(StatValue::UInt64(*value)),
        ScalarValue::Float64(Some(value)) if value.is_finite() => Some(StatValue::Float64(*value)),
        ScalarValue::Utf8(Some(value)) | ScalarValue::Utf8View(Some(value)) => {
            Some(StatValue::Utf8(value.clone()))
        }
        _ => None,
    }
}

fn hour_window(filters: &[Expr]) -> HourWindow {
    let mut window = HourWindow::unbounded();
    for filter in filters {
        if let Some(bound) = time_predicate(filter) {
            window.intersect(bound);
        }
    }
    window
}

fn time_predicate(expr: &Expr) -> Option<HourWindow> {
    let mut window = HourWindow::unbounded();
    time_predicate_into(expr, &mut window).then_some(window)
}

fn time_predicate_into(expr: &Expr, window: &mut HourWindow) -> bool {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            time_predicate_into(&binary.left, window) && time_predicate_into(&binary.right, window)
        }
        Expr::Between(between) if !between.negated => {
            let Some(low) = literal_u64(&between.low) else {
                return false;
            };
            let Some(high) = literal_u64(&between.high) else {
                return false;
            };
            if !is_event_time(&between.expr) {
                return false;
            }
            window.intersect(bound_window(Operator::GtEq, low));
            window.intersect(bound_window(Operator::LtEq, high));
            true
        }
        Expr::BinaryExpr(binary) => {
            let compared = match (bare(&binary.left), binary.op, bare(&binary.right)) {
                (column, operator, literal)
                    if is_event_time(column) && is_order_comparison(operator) =>
                {
                    literal_u64(literal).map(|timestamp| (operator, timestamp))
                }
                (literal, operator, column)
                    if is_event_time(column) && is_order_comparison(operator) =>
                {
                    flip_comparison(operator).and_then(|operator| {
                        literal_u64(literal).map(|timestamp| (operator, timestamp))
                    })
                }
                _ => None,
            };
            let Some((operator, timestamp)) = compared else {
                return false;
            };
            window.intersect(bound_window(operator, timestamp));
            true
        }
        _ => false,
    }
}

fn bound_window(operator: Operator, timestamp: u64) -> HourWindow {
    let mut window = HourWindow::unbounded();
    match operator {
        Operator::GtEq => window.from = Some(EventHour::containing(timestamp)),
        Operator::Gt => match timestamp.checked_add(1) {
            Some(next) => window.from = Some(EventHour::containing(next)),
            None => window.impossible = true,
        },
        Operator::LtEq => {
            window.to = hour_after(EventHour::containing(timestamp));
        }
        Operator::Lt => match timestamp.checked_sub(1) {
            Some(previous) => window.to = hour_after(EventHour::containing(previous)),
            None => window.impossible = true,
        },
        Operator::Eq => {
            let hour = EventHour::containing(timestamp);
            window.from = Some(hour);
            window.to = hour_after(hour);
        }
        _ => window.impossible = true,
    }
    if let (Some(from), Some(to)) = (window.from, window.to)
        && from >= to
    {
        window.impossible = true;
    }
    window
}

fn hour_after(hour: EventHour) -> Option<EventHour> {
    hour.start_unix_nano()
        .checked_add(NANOS_PER_HOUR)
        .map(EventHour::containing)
}

fn is_event_time(expr: &Expr) -> bool {
    bare(expr)
        .try_as_col()
        .is_some_and(|column| column.name == COLUMN_EVENT_TIME_UNIX_NANO)
}

fn bare(expr: &Expr) -> &Expr {
    match expr {
        Expr::Cast(cast) => bare(&cast.expr),
        Expr::TryCast(cast) => bare(&cast.expr),
        other => other,
    }
}

fn literal_u64(expr: &Expr) -> Option<u64> {
    let Expr::Literal(value, _) = bare(expr) else {
        return None;
    };
    match value {
        ScalarValue::UInt64(Some(value)) => Some(*value),
        ScalarValue::UInt32(Some(value)) => Some(u64::from(*value)),
        ScalarValue::UInt16(Some(value)) => Some(u64::from(*value)),
        ScalarValue::UInt8(Some(value)) => Some(u64::from(*value)),
        ScalarValue::Int64(Some(value)) => u64::try_from(*value).ok(),
        ScalarValue::Int32(Some(value)) => u64::try_from(*value).ok(),
        ScalarValue::Int16(Some(value)) => u64::try_from(*value).ok(),
        ScalarValue::Int8(Some(value)) => u64::try_from(*value).ok(),
        _ => None,
    }
}

fn is_order_comparison(operator: Operator) -> bool {
    matches!(
        operator,
        Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
    )
}

fn flip_comparison(operator: Operator) -> Option<Operator> {
    match operator {
        Operator::Lt => Some(Operator::Gt),
        Operator::LtEq => Some(Operator::GtEq),
        Operator::Gt => Some(Operator::Lt),
        Operator::GtEq => Some(Operator::LtEq),
        Operator::Eq => Some(Operator::Eq),
        _ => None,
    }
}

struct KnownSpan {
    rows: u64,
    event_min: u64,
    event_max: u64,
}

enum ScanSource {
    File(PublishedFile),
    Memory(Vec<RecordBatch>),
}

struct ScanPiece {
    span: Option<KnownSpan>,
    ordered: bool,
    source: ScanSource,
}

impl ScanPiece {
    async fn plan(
        self,
        state: &dyn Session,
        schema: &SchemaRef,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        advertise_order: bool,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        match self.source {
            ScanSource::File(file) => {
                parquet_plan(
                    state,
                    schema,
                    std::slice::from_ref(&file),
                    projection,
                    filters,
                    limit,
                    advertise_order,
                )
                .await
            }
            ScanSource::Memory(batches) => {
                let batches = limit_batches(batches, limit);
                let memory_schema = match batches.first() {
                    Some(batch) => batch.schema(),
                    None => output_schema(schema, projection)?,
                };
                let mut table = MemTable::try_new(memory_schema, vec![batches])?;
                if advertise_order {
                    table = table.with_sort_order(vec![newest_sort_exprs()]);
                }
                table.scan(state, None, &[], None).await
            }
        }
    }
}

fn hour_pieces(
    hour: &HourPartition,
    filters: &[Expr],
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> datafusion::error::Result<Vec<ScanPiece>> {
    let mut pieces = Vec::new();
    for file in &hour.files {
        if !file_may_match(file.statistics.as_ref(), filters) {
            continue;
        }
        let span = file.statistics.as_ref().and_then(|statistics| {
            statistics.ordered.then_some(KnownSpan {
                rows: statistics.rows,
                event_min: statistics.event_time_min,
                event_max: statistics.event_time_max,
            })
        });
        pieces.push(ScanPiece {
            ordered: span.is_some(),
            span,
            source: ScanSource::File(file.clone()),
        });
    }
    if !hour.batches.is_empty() {
        let columns = projection.map(Vec::as_slice);
        let working = alignment_projection(schema.as_ref(), columns);
        let mut aligned = Vec::with_capacity(hour.batches.len());
        for batch in &hour.batches {
            aligned.push(project_batch(batch, schema, working.as_deref())?);
        }
        let sorted = sort_by_newest_event(&aligned[0].schema(), &aligned)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let span = batch_span(&sorted);
        let projected = if working.as_deref() == columns {
            sorted
        } else {
            project_batch(&sorted, schema, columns)?
        };
        pieces.push(ScanPiece {
            ordered: span.is_some(),
            span,
            source: ScanSource::Memory(split_sorted(projected, &hour.batches)),
        });
    }
    Ok(pieces)
}

fn alignment_projection(
    schema: &arrow_schema::Schema,
    projection: Option<&[usize]>,
) -> Option<Vec<usize>> {
    let projection = projection?;
    let mut indices = projection.to_vec();
    for name in [COLUMN_EVENT_TIME_UNIX_NANO, COLUMN_WAL_SEQUENCE] {
        let Ok(index) = schema.index_of(name) else {
            continue;
        };
        if !indices.contains(&index) {
            indices.push(index);
        }
    }
    Some(indices)
}

fn output_schema(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> datafusion::error::Result<SchemaRef> {
    match projection {
        None => Ok(Arc::clone(schema)),
        Some(indices) => schema
            .project(indices)
            .map(Arc::new)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None)),
    }
}

fn project_batch(
    batch: &RecordBatch,
    schema: &SchemaRef,
    projection: Option<&[usize]>,
) -> datafusion::error::Result<RecordBatch> {
    align_projected(batch, Arc::clone(schema), projection)
        .map_err(|error| DataFusionError::External(Box::new(error)))
}

fn split_sorted(sorted: RecordBatch, originals: &[Arc<RecordBatch>]) -> Vec<RecordBatch> {
    if originals.len() == 1 && originals[0].num_rows() == sorted.num_rows() {
        return vec![sorted];
    }
    let mut offset = 0;
    let mut batches = Vec::new();
    for original in originals {
        let rows = original.num_rows();
        if rows == 0 {
            continue;
        }
        batches.push(sorted.slice(offset, rows));
        offset += rows;
    }
    if batches.is_empty() {
        batches.push(sorted);
    }
    batches
}

fn limit_batches(batches: Vec<RecordBatch>, limit: Option<usize>) -> Vec<RecordBatch> {
    let Some(limit) = limit else {
        return batches;
    };
    let mut remaining = limit;
    let mut limited = Vec::new();
    for batch in batches {
        if remaining == 0 {
            break;
        }
        if batch.num_rows() <= remaining {
            remaining -= batch.num_rows();
            limited.push(batch);
        } else {
            limited.push(batch.slice(0, remaining));
            break;
        }
    }
    limited
}

fn batch_span(batch: &RecordBatch) -> Option<KnownSpan> {
    let rows = u64::try_from(batch.num_rows()).ok()?;
    if rows == 0 {
        return None;
    }
    let event = batch
        .column_by_name(COLUMN_EVENT_TIME_UNIX_NANO)?
        .as_primitive::<UInt64Type>();
    if event.null_count() != 0 {
        return None;
    }
    Some(KnownSpan {
        rows,
        event_max: event.value(0),
        event_min: event.value(event.len() - 1),
    })
}

fn keep_newest_prefix(
    mut pieces: Vec<ScanPiece>,
    limit: Option<usize>,
    exact: bool,
) -> Vec<ScanPiece> {
    let Some(limit) = limit else {
        return pieces;
    };
    if !exact || pieces.iter().any(|piece| piece.span.is_none()) {
        return pieces;
    }
    let limit = u64::try_from(limit).unwrap_or(u64::MAX);
    let mut order: Vec<usize> = (0..pieces.len()).collect();
    order.sort_by(|&left, &right| {
        let left = pieces[left].span.as_ref().expect("span");
        let right = pieces[right].span.as_ref().expect("span");
        right.event_max.cmp(&left.event_max)
    });
    let mut covered = 0_u64;
    let mut prefix_min = u64::MAX;
    let mut keep = vec![false; pieces.len()];
    for index in order {
        let span = pieces[index].span.as_ref().expect("span");
        if covered >= limit && span.event_max < prefix_min {
            continue;
        }
        prefix_min = prefix_min.min(span.event_min);
        covered = covered.saturating_add(span.rows);
        keep[index] = true;
    }
    let mut kept = Vec::new();
    for (piece, keep) in pieces.drain(..).zip(keep) {
        if keep {
            kept.push(piece);
        }
    }
    kept
}

fn newest_sort_exprs() -> Vec<SortExpr> {
    vec![
        SortExpr {
            expr: col(COLUMN_EVENT_TIME_UNIX_NANO),
            asc: false,
            nulls_first: true,
        },
        SortExpr {
            expr: col(COLUMN_WAL_SEQUENCE),
            asc: false,
            nulls_first: true,
        },
    ]
}

fn newest_ordering(schema: &arrow_schema::Schema) -> Option<LexOrdering> {
    let event = schema.index_of(COLUMN_EVENT_TIME_UNIX_NANO).ok()?;
    let wal = schema.index_of(COLUMN_WAL_SEQUENCE).ok()?;
    sort_ordering(event, wal)
}

fn projected_newest_ordering(
    schema: &arrow_schema::Schema,
    projection: Option<&Vec<usize>>,
) -> Option<LexOrdering> {
    let event = schema.index_of(COLUMN_EVENT_TIME_UNIX_NANO).ok()?;
    let wal = schema.index_of(COLUMN_WAL_SEQUENCE).ok()?;
    let (event, wal) = match projection {
        None => (event, wal),
        Some(indices) => (
            indices.iter().position(|index| *index == event)?,
            indices.iter().position(|index| *index == wal)?,
        ),
    };
    sort_ordering(event, wal)
}

fn sort_ordering(event: usize, wal: usize) -> Option<LexOrdering> {
    LexOrdering::new(vec![
        PhysicalSortExpr::new_default(Arc::new(Column::new(COLUMN_EVENT_TIME_UNIX_NANO, event)))
            .desc()
            .nulls_first(),
        PhysicalSortExpr::new_default(Arc::new(Column::new(COLUMN_WAL_SEQUENCE, wal)))
            .desc()
            .nulls_first(),
    ])
}

fn order_columns_projected(schema: &arrow_schema::Schema, projection: Option<&Vec<usize>>) -> bool {
    let Some(projection) = projection else {
        return true;
    };
    [COLUMN_EVENT_TIME_UNIX_NANO, COLUMN_WAL_SEQUENCE]
        .into_iter()
        .all(|name| {
            schema
                .index_of(name)
                .is_ok_and(|index| projection.contains(&index))
        })
}

async fn parquet_plan(
    state: &dyn Session,
    schema: &SchemaRef,
    files: &[PublishedFile],
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
    advertise_order: bool,
) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    let url = ObjectStoreUrl::local_filesystem();
    let store = state.runtime_env().object_store(&url)?;
    let cache = state.runtime_env().cache_manager.get_file_metadata_cache();
    let parquet = &state.config().options().execution.parquet;
    let mut source = ParquetSource::new(Arc::clone(schema))
        .with_parquet_file_reader_factory(Arc::new(CachedParquetFileReaderFactory::new(
            store, cache,
        )))
        .with_pushdown_filters(parquet.pushdown_filters)
        .with_reorder_filters(parquet.reorder_filters)
        .with_enable_page_index(parquet.enable_page_index)
        .with_bloom_filter_on_read(parquet.bloom_filter_on_read);
    if let Some(hint) = parquet.metadata_size_hint {
        source = source.with_metadata_size_hint(hint);
    }
    if let Some(predicate) = parquet_predicate(state, schema, filters)? {
        source = source.with_predicate(predicate);
    }
    let mut groups = Vec::with_capacity(files.len());
    for file in files {
        groups.push(FileGroup::new(vec![partitioned_file(&file.path).await?]));
    }
    let mut builder = FileScanConfigBuilder::new(url, Arc::new(source))
        .with_file_groups(groups)
        .with_limit(limit);
    if advertise_order && let Some(ordering) = newest_ordering(schema.as_ref()) {
        builder = builder.with_output_ordering(vec![ordering]);
    }
    if let Some(indices) = projection {
        builder = builder.with_projection_indices(Some(indices.clone()))?;
    }
    Ok(DataSourceExec::from_data_source(builder.build()))
}

#[derive(Debug)]
struct NewestEventLimit;

impl OptimizerRule for NewestEventLimit {
    fn name(&self) -> &str {
        "newest_event_limit"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        let LogicalPlan::Sort(sort) = plan else {
            return Ok(Transformed::no(plan));
        };
        let Sort { expr, input, fetch } = sort;
        let Some(fetch) = fetch else {
            return Ok(Transformed::no(LogicalPlan::Sort(Sort {
                expr,
                input,
                fetch,
            })));
        };
        if !newest_event_sort(&expr) {
            return Ok(Transformed::no(LogicalPlan::Sort(Sort {
                expr,
                input,
                fetch: Some(fetch),
            })));
        }
        let (pushed, changed) = push_newest_fetch(Arc::unwrap_or_clone(input), fetch)?;
        if !changed {
            return Ok(Transformed::no(LogicalPlan::Sort(Sort {
                expr,
                input: Arc::new(pushed),
                fetch: Some(fetch),
            })));
        }
        Ok(Transformed::yes(LogicalPlan::Sort(Sort {
            expr,
            input: Arc::new(pushed),
            fetch: Some(fetch),
        })))
    }
}

fn newest_event_sort(expr: &[SortExpr]) -> bool {
    let Some(event_time) = expr.first() else {
        return false;
    };
    if !descending_column(event_time, COLUMN_EVENT_TIME_UNIX_NANO) {
        return false;
    }
    match expr.get(1) {
        None => true,
        Some(wal) => expr.len() == 2 && descending_column(wal, COLUMN_WAL_SEQUENCE),
    }
}

fn descending_column(sort: &SortExpr, name: &str) -> bool {
    !sort.asc
        && sort
            .expr
            .try_as_col()
            .is_some_and(|column| column.name == name)
}

fn push_newest_fetch(
    plan: LogicalPlan,
    fetch: usize,
) -> Result<(LogicalPlan, bool), DataFusionError> {
    match plan {
        LogicalPlan::TableScan(mut scan) if scan.filters.is_empty() => {
            let next = Some(scan.fetch.map_or(fetch, |current| current.min(fetch)));
            if scan.fetch == next {
                return Ok((LogicalPlan::TableScan(scan), false));
            }
            scan.fetch = next;
            Ok((LogicalPlan::TableScan(scan), true))
        }
        LogicalPlan::Projection(mut projection) => {
            let (input, changed) =
                push_newest_fetch(Arc::unwrap_or_clone(projection.input), fetch)?;
            projection.input = Arc::new(input);
            Ok((LogicalPlan::Projection(projection), changed))
        }
        LogicalPlan::SubqueryAlias(mut alias) => {
            let (input, changed) = push_newest_fetch(Arc::unwrap_or_clone(alias.input), fetch)?;
            alias.input = Arc::new(input);
            Ok((LogicalPlan::SubqueryAlias(alias), changed))
        }
        other => Ok((other, false)),
    }
}

async fn partitioned_file(path: &Path) -> datafusion::error::Result<PartitionedFile> {
    let path = path.to_path_buf();
    let text = path
        .to_str()
        .ok_or_else(|| {
            DataFusionError::External(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "parquet path is not utf-8",
            )))
        })?
        .to_owned();
    let length =
        tokio::task::spawn_blocking(move || std::fs::metadata(&path).map(|meta| meta.len()))
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
    Ok(PartitionedFile::new(text, length))
}

fn one_partition(
    plan: Arc<dyn ExecutionPlan>,
) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    if plan.output_partitioning().partition_count() > 1 {
        Ok(Arc::new(CoalescePartitionsExec::new(plan)))
    } else {
        Ok(plan)
    }
}

fn ensure_query(sql: &str) -> Result<(), QueryError> {
    let statements = DFParser::parse_sql(sql).map_err(QueryError::Planning)?;
    let mut statements = statements.into_iter();
    let Some(statement) = statements.next() else {
        return Err(QueryError::Statement(
            "SQL contains no statement".to_owned(),
        ));
    };
    if statements.next().is_some() {
        return Err(QueryError::Statement(
            "SQL contains more than one statement".to_owned(),
        ));
    }
    let DFStatement::Statement(statement) = statement else {
        return Err(QueryError::Statement(
            "only a single SELECT statement is supported".to_owned(),
        ));
    };
    if !matches!(statement.as_ref(), Statement::Query(_)) {
        return Err(QueryError::Statement(
            "only a single SELECT statement is supported".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::path::Path;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use arrow_array::{Array, Int64Array, RecordBatch, StringArray, UInt64Array};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use async_trait::async_trait;
    use bytes::Bytes;
    use datafusion::catalog::{Session, TableProvider};
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::datasource::MemTable;
    use datafusion::datasource::physical_plan::FileScanConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::error::DataFusionError;
    use datafusion::execution::TaskContext;
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr::{Expr, TableType};
    use datafusion::logical_expr::{col, lit};
    use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::metrics::MetricValue;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream,
    };
    use futures::Stream;
    use observer_protocol::otlp::{
        AnyValue, ExportLogsServiceRequest, KeyValue, LogRecord, ResourceLogs, ScopeLogs, any_value,
    };
    use observer_storage::{
        ColumnStatistics, DynamicLimits, FileStatistics, ManualClock, MemtableConfig,
        ParquetWriteOptions, PublishOptions, Scan, StatValue, Store, decode_logs_frame,
    };
    use observer_wal::{Frame, FrameSignal};
    use prost::Message;
    use tokio_stream::StreamExt;

    use super::{ObserverTableProvider, QueryEngine, QueryEngineConfig, QueryError, QueryOptions};

    const HOUR: u64 = 3_600_000_000_000;

    fn open_at(path: &Path) -> Arc<Store> {
        let opened = Store::open(
            path,
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
        Arc::new(opened)
    }

    fn store() -> Arc<Store> {
        let directory = tempfile::tempdir().expect("tempdir");
        open_at(&directory.keep())
    }

    fn frame(sequence: u64, time: u64, attributes: Vec<KeyValue>) -> observer_storage::DecodedLogs {
        frame_body(sequence, time, &format!("row-{sequence}"), attributes)
    }

    fn frame_body(
        sequence: u64,
        time: u64,
        body: &str,
        attributes: Vec<KeyValue>,
    ) -> observer_storage::DecodedLogs {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: time,
                        attributes,
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

    fn empty_frame(sequence: u64) -> observer_storage::DecodedLogs {
        decode_logs_frame(
            &Frame {
                sequence,
                signal: FrameSignal::Logs,
                received_at_unix_nanos: 1,
                tenant_id: "tenant-a".to_owned(),
                payload: Bytes::from(ExportLogsServiceRequest::default().encode_to_vec()),
            },
            DynamicLimits {
                max_depth: 4,
                max_columns: 32,
            },
        )
        .expect("decode empty")
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

    fn engine() -> QueryEngine {
        QueryEngine::new(QueryEngineConfig::default()).expect("engine")
    }

    fn engine_with(max_concurrent_queries: usize) -> QueryEngine {
        QueryEngine::new(QueryEngineConfig {
            max_concurrent_queries,
            ..QueryEngineConfig::default()
        })
        .expect("engine")
    }

    fn paused_engine() -> QueryEngine {
        QueryEngine::paused(QueryEngineConfig::default()).expect("paused engine")
    }

    async fn batches(store: Arc<Store>, sql: &str) -> Vec<arrow_array::RecordBatch> {
        let mut stream = engine()
            .execute(store, sql, QueryOptions::default())
            .await
            .expect("execute");
        let mut rows = Vec::new();
        while let Some(batch) = stream.next().await {
            rows.push(batch.expect("batch"));
        }
        rows
    }

    fn u64_column(batches: &[arrow_array::RecordBatch], name: &str) -> Vec<u64> {
        batches
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column_by_name(name)
                    .unwrap_or_else(|| panic!("missing {name}"))
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap_or_else(|| panic!("{name} is not u64"));
                (0..column.len()).map(|row| column.value(row))
            })
            .collect()
    }

    fn i64_column(batches: &[arrow_array::RecordBatch], name: &str) -> Vec<Option<i64>> {
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

    fn bodies(batches: &[arrow_array::RecordBatch]) -> Vec<String> {
        batches
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column_by_name("body")
                    .expect("body")
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("utf8");
                (0..column.len()).map(|row| column.value(row).to_owned())
            })
            .collect()
    }

    #[tokio::test]
    async fn sql_reads_active_and_frozen_rows() {
        let store = store();
        let first = frame(0, 0, Vec::new());
        store.append(&first).expect("append");
        store.rotate().expect("rotate");
        store.append(&frame(1, HOUR, Vec::new())).expect("active");

        let rows = batches(
            Arc::clone(&store),
            "SELECT wal_sequence, body FROM logs ORDER BY wal_sequence",
        )
        .await;
        assert_eq!(u64_column(&rows, "wal_sequence"), [0, 1]);
        assert_eq!(bodies(&rows), ["row-0", "row-1"]);

        let unordered = batches(Arc::clone(&store), "SELECT wal_sequence FROM logs").await;
        let mut sequences = u64_column(&unordered, "wal_sequence");
        sequences.sort_unstable();
        assert_eq!(sequences, [0, 1]);

        let filtered = batches(
            Arc::clone(&store),
            "SELECT count(*) AS rows FROM logs WHERE wal_sequence = 1",
        )
        .await;
        assert_eq!(i64_column(&filtered, "rows"), vec![Some(1)]);

        let limited = batches(
            store,
            "SELECT wal_sequence FROM logs ORDER BY wal_sequence LIMIT 1",
        )
        .await;
        assert_eq!(u64_column(&limited, "wal_sequence"), [0]);
    }

    #[tokio::test]
    async fn sql_null_fills_dynamic_columns_and_groups_them() {
        let store = store();
        store
            .append(&frame(0, 0, vec![attribute("a", 1)]))
            .expect("append");
        store.rotate().expect("rotate");
        store
            .append(&frame(1, 0, vec![attribute("b", 2)]))
            .expect("active");

        let rows = batches(
            Arc::clone(&store),
            "SELECT wal_sequence, log_a_i64, log_b_i64 FROM logs ORDER BY wal_sequence",
        )
        .await;
        assert_eq!(u64_column(&rows, "wal_sequence"), [0, 1]);
        assert_eq!(i64_column(&rows, "log_a_i64"), vec![Some(1), None]);
        assert_eq!(i64_column(&rows, "log_b_i64"), vec![None, Some(2)]);

        let grouped = batches(
            store,
            "SELECT count(*) AS rows FROM logs GROUP BY log_a_i64",
        )
        .await;
        let mut counts = i64_column(&grouped, "rows");
        counts.sort();
        assert_eq!(counts, vec![Some(1), Some(1)]);
    }

    #[tokio::test]
    async fn sql_pins_the_snapshot_taken_at_execute() {
        let store = store();
        store.append(&frame(0, 0, Vec::new())).expect("append");
        let engine = engine();
        let mut stream = engine
            .execute(
                Arc::clone(&store),
                "SELECT wal_sequence FROM logs",
                QueryOptions::default(),
            )
            .await
            .expect("execute");
        store.append(&frame(1, 0, Vec::new())).expect("later");
        let mut rows = Vec::new();
        while let Some(batch) = stream.next().await {
            rows.push(batch.expect("batch"));
        }
        assert_eq!(u64_column(&rows, "wal_sequence"), [0]);
    }

    #[tokio::test]
    async fn sql_reads_each_snapshot_row_once() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = open_at(directory.path());
        store.append(&frame(0, 0, Vec::new())).expect("append");
        store.rotate().expect("rotate");
        store.publish(&PublishOptions::default()).expect("publish");
        store
            .append(&frame(1, HOUR, vec![attribute("a", 1)]))
            .expect("frozen");
        store.rotate().expect("rotate");
        store
            .append(&frame(2, HOUR * 2, vec![attribute("b", 2)]))
            .expect("active");

        let rows = batches(
            Arc::clone(&store),
            "SELECT wal_sequence, log_a_i64, log_b_i64 FROM logs ORDER BY wal_sequence",
        )
        .await;
        assert_eq!(u64_column(&rows, "wal_sequence"), [0, 1, 2]);
        assert_eq!(i64_column(&rows, "log_a_i64"), vec![None, Some(1), None]);
        assert_eq!(i64_column(&rows, "log_b_i64"), vec![None, None, Some(2)]);

        let published = store.sources(&store.snapshot().expect("snapshot"), &Scan::default());
        let committed = &published[0].files[0].path;
        std::fs::copy(committed, committed.with_file_name("stray.parquet")).expect("stray");
        let once = batches(Arc::clone(&store), "SELECT wal_sequence FROM logs").await;
        let mut sequences = u64_column(&once, "wal_sequence");
        sequences.sort_unstable();
        assert_eq!(sequences, [0, 1, 2]);

        drop(store);
        let reopened = open_at(directory.path());
        let durable = batches(
            reopened,
            "SELECT wal_sequence FROM logs ORDER BY wal_sequence",
        )
        .await;
        assert_eq!(u64_column(&durable, "wal_sequence"), [0]);
    }

    #[tokio::test]
    async fn sql_empty_commit_adds_no_rows() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = open_at(directory.path());
        store.append(&empty_frame(4)).expect("append");
        store.rotate().expect("rotate");
        store.publish(&PublishOptions::default()).expect("publish");
        let rows = batches(Arc::clone(&store), "SELECT count(*) AS rows FROM logs").await;
        assert_eq!(i64_column(&rows, "rows"), vec![Some(0)]);
        drop(store);
        let reopened = batches(
            open_at(directory.path()),
            "SELECT count(*) AS rows FROM logs",
        )
        .await;
        assert_eq!(i64_column(&reopened, "rows"), vec![Some(0)]);
    }

    async fn physical_plan(store: Arc<Store>, sql: &str) -> Arc<dyn ExecutionPlan> {
        let snapshot = store.snapshot().expect("snapshot");
        let provider = ObserverTableProvider::new(store, snapshot).expect("provider");
        let context = SessionContext::new();
        context.add_optimizer_rule(Arc::new(super::NewestEventLimit));
        context
            .register_table("logs", Arc::new(provider))
            .expect("register");
        let frame = context.sql(sql).await.expect("sql");
        frame.create_physical_plan().await.expect("plan")
    }

    struct ScanShape {
        files: Vec<String>,
        schemas: Vec<Vec<String>>,
        memory_scans: usize,
    }

    fn scan_shape(plan: &Arc<dyn ExecutionPlan>) -> ScanShape {
        let mut shape = ScanShape {
            files: Vec::new(),
            schemas: Vec::new(),
            memory_scans: 0,
        };
        collect_scans(plan, &mut shape);
        shape
    }

    fn collect_scans(plan: &Arc<dyn ExecutionPlan>, shape: &mut ScanShape) {
        if let Some(exec) = plan.downcast_ref::<DataSourceExec>() {
            shape.schemas.push(
                exec.schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect(),
            );
            if let Some(config) = exec.data_source().downcast_ref::<FileScanConfig>() {
                for group in &config.file_groups {
                    for file in group.files() {
                        shape.files.push(file.object_meta.location.to_string());
                    }
                }
            } else {
                shape.memory_scans += 1;
            }
        }
        for child in plan.children() {
            collect_scans(child, shape);
        }
    }

    #[tokio::test]
    async fn projection_reads_only_selected_columns() {
        let store = store();
        store.append(&frame(0, 0, Vec::new())).expect("append");
        let plan = physical_plan(Arc::clone(&store), "SELECT wal_sequence FROM logs").await;
        let shape = scan_shape(&plan);
        assert!(!shape.schemas.is_empty());
        for schema in &shape.schemas {
            assert_eq!(schema.as_slice(), ["wal_sequence"]);
        }
        let rows = batches(store, "SELECT wal_sequence FROM logs").await;
        assert_eq!(u64_column(&rows, "wal_sequence"), [0]);
    }

    #[tokio::test]
    async fn event_time_bounds_skip_hours_and_keep_boundary_rows() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = open_at(directory.path());
        store.append(&frame(0, 1, Vec::new())).expect("append");
        store
            .append(&frame(1, HOUR - 1, Vec::new()))
            .expect("hour boundary");
        store.rotate().expect("rotate");
        store.publish(&PublishOptions::default()).expect("publish");
        store.append(&frame(2, HOUR, Vec::new())).expect("append");
        store.rotate().expect("rotate");
        store.publish(&PublishOptions::default()).expect("publish");
        store.append(&frame(3, 2, Vec::new())).expect("hour tail");

        let lower = format!(
            "SELECT wal_sequence FROM logs WHERE event_time_unix_nano >= {HOUR} ORDER BY wal_sequence"
        );
        let shape = scan_shape(&physical_plan(Arc::clone(&store), &lower).await);
        let files = shape.files.join(" ");
        assert!(files.contains("hour=01"), "{files}");
        assert!(!files.contains("hour=00"), "{files}");
        assert_eq!(shape.memory_scans, 0);
        for schema in &shape.schemas {
            assert!(schema.iter().any(|name| name == "wal_sequence"));
            assert!(schema.iter().any(|name| name == "event_time_unix_nano"));
            assert!(!schema.iter().any(|name| name == "body"));
        }
        let rows = batches(Arc::clone(&store), &lower).await;
        assert_eq!(u64_column(&rows, "wal_sequence"), [2]);

        let inside =
            "SELECT wal_sequence FROM logs WHERE event_time_unix_nano > 1 ORDER BY wal_sequence";
        let shape = scan_shape(&physical_plan(Arc::clone(&store), inside).await);
        let files = shape.files.join(" ");
        assert!(files.contains("hour=00"), "{files}");
        assert!(files.contains("hour=01"), "{files}");
        assert!(shape.memory_scans > 0);
        let rows = batches(Arc::clone(&store), inside).await;
        assert_eq!(u64_column(&rows, "wal_sequence"), [1, 2, 3]);

        let upper = format!(
            "SELECT wal_sequence FROM logs WHERE event_time_unix_nano < {HOUR} ORDER BY wal_sequence"
        );
        let shape = scan_shape(&physical_plan(Arc::clone(&store), &upper).await);
        let files = shape.files.join(" ");
        assert!(files.contains("hour=00"), "{files}");
        assert!(!files.contains("hour=01"), "{files}");
        let rows = batches(Arc::clone(&store), &upper).await;
        assert_eq!(u64_column(&rows, "wal_sequence"), [0, 1, 3]);

        let past_boundary = format!(
            "SELECT wal_sequence FROM logs WHERE event_time_unix_nano > {} ORDER BY wal_sequence",
            HOUR - 1
        );
        let shape = scan_shape(&physical_plan(Arc::clone(&store), &past_boundary).await);
        let files = shape.files.join(" ");
        assert!(!files.contains("hour=00"), "{files}");
        assert!(files.contains("hour=01"), "{files}");
        let rows = batches(Arc::clone(&store), &past_boundary).await;
        assert_eq!(u64_column(&rows, "wal_sequence"), [2]);

        let equal = format!(
            "SELECT wal_sequence FROM logs WHERE event_time_unix_nano = {HOUR} ORDER BY wal_sequence"
        );
        let shape = scan_shape(&physical_plan(Arc::clone(&store), &equal).await);
        let files = shape.files.join(" ");
        assert!(!files.contains("hour=00"), "{files}");
        let rows = batches(Arc::clone(&store), &equal).await;
        assert_eq!(u64_column(&rows, "wal_sequence"), [2]);

        let narrowed = format!(
            "SELECT wal_sequence FROM logs WHERE event_time_unix_nano >= {HOUR} AND body = 'row-2' ORDER BY wal_sequence"
        );
        let shape = scan_shape(&physical_plan(Arc::clone(&store), &narrowed).await);
        let files = shape.files.join(" ");
        assert!(!files.contains("hour=00"), "{files}");
        assert!(
            shape
                .schemas
                .iter()
                .any(|schema| schema.iter().any(|name| name == "body"))
        );
        let rows = batches(store, &narrowed).await;
        assert_eq!(u64_column(&rows, "wal_sequence"), [2]);
    }

    #[test]
    fn missing_statistics_keep_every_file() {
        let selective = col("log_a_i64").eq(lit(1_i64));
        assert!(super::file_may_match(
            None,
            std::slice::from_ref(&selective)
        ));
        let excluded = FileStatistics {
            size_bytes: 8,
            rows: 1,
            event_time_min: 1,
            event_time_max: 1,
            wal_min: 0,
            wal_max: 0,
            ordered: true,
            columns: vec![ColumnStatistics {
                name: "log_a_i64".to_owned(),
                null_count: 0,
                bounds: Some((StatValue::Int64(2), StatValue::Int64(2))),
            }],
        };
        assert!(!super::file_may_match(Some(&excluded), &[selective]));
    }

    #[tokio::test]
    async fn file_statistics_drop_generations_that_cannot_match() {
        let store = store();
        publish_row(&store, frame(0, 1, vec![attribute("a", 1)]));
        publish_row(&store, frame(1, 2, vec![attribute("a", 2)]));

        let equal = "SELECT wal_sequence FROM logs WHERE log_a_i64 = 1 ORDER BY wal_sequence";
        let files = scan_files(&physical_plan(Arc::clone(&store), equal).await);
        assert!(
            files.iter().any(|file| file.contains("0-1.parquet")),
            "{files:?}"
        );
        assert!(
            files.iter().all(|file| !file.contains("1-2.parquet")),
            "{files:?}"
        );
        assert_eq!(
            u64_column(&batches(Arc::clone(&store), equal).await, "wal_sequence"),
            [0]
        );

        let between = "SELECT wal_sequence FROM logs WHERE log_a_i64 BETWEEN 2 AND 2";
        let files = scan_files(&physical_plan(Arc::clone(&store), between).await);
        assert!(
            files.iter().all(|file| !file.contains("0-1.parquet")),
            "{files:?}"
        );
        assert!(
            files.iter().any(|file| file.contains("1-2.parquet")),
            "{files:?}"
        );

        let either = "SELECT wal_sequence FROM logs WHERE log_a_i64 = 1 OR log_a_i64 = 2 ORDER BY wal_sequence";
        let files = scan_files(&physical_plan(Arc::clone(&store), either).await);
        assert!(
            files.iter().any(|file| file.contains("0-1.parquet")),
            "{files:?}"
        );
        assert!(
            files.iter().any(|file| file.contains("1-2.parquet")),
            "{files:?}"
        );
        assert_eq!(
            u64_column(&batches(store, either).await, "wal_sequence"),
            [0, 1]
        );
    }

    #[tokio::test]
    async fn absent_dynamic_column_is_all_null_for_that_file() {
        let store = store();
        publish_row(&store, frame(0, 1, vec![attribute("a", 1)]));
        publish_row(&store, frame(1, 2, vec![attribute("b", 2)]));

        let equal = "SELECT wal_sequence FROM logs WHERE log_a_i64 = 1 ORDER BY wal_sequence";
        let files = scan_files(&physical_plan(Arc::clone(&store), equal).await);
        assert!(
            files.iter().any(|file| file.contains("0-1.parquet")),
            "{files:?}"
        );
        assert!(
            files.iter().all(|file| !file.contains("1-2.parquet")),
            "{files:?}"
        );
        assert_eq!(
            u64_column(&batches(Arc::clone(&store), equal).await, "wal_sequence"),
            [0]
        );

        let missing = "SELECT wal_sequence FROM logs WHERE log_a_i64 IS NULL ORDER BY wal_sequence";
        let files = scan_files(&physical_plan(Arc::clone(&store), missing).await);
        assert!(
            files.iter().all(|file| !file.contains("0-1.parquet")),
            "{files:?}"
        );
        assert!(
            files.iter().any(|file| file.contains("1-2.parquet")),
            "{files:?}"
        );
        assert_eq!(
            u64_column(&batches(store, missing).await, "wal_sequence"),
            [1]
        );
    }

    #[tokio::test]
    async fn null_and_range_bounds_keep_only_files_that_can_match() {
        let store = store();
        publish_row(&store, frame_severity(0, 1, 9));
        publish_row(&store, frame_severity(1, 2, 0));

        let equal = "SELECT wal_sequence FROM logs WHERE severity_number = 9";
        let files = scan_files(&physical_plan(Arc::clone(&store), equal).await);
        assert!(
            files.iter().any(|file| file.contains("0-1.parquet")),
            "{files:?}"
        );
        assert!(
            files.iter().all(|file| !file.contains("1-2.parquet")),
            "{files:?}"
        );
        assert_eq!(
            u64_column(&batches(Arc::clone(&store), equal).await, "wal_sequence"),
            [0]
        );

        let nulls = "SELECT wal_sequence FROM logs WHERE severity_number IS NULL";
        let files = scan_files(&physical_plan(Arc::clone(&store), nulls).await);
        assert!(
            files.iter().all(|file| !file.contains("0-1.parquet")),
            "{files:?}"
        );
        assert!(
            files.iter().any(|file| file.contains("1-2.parquet")),
            "{files:?}"
        );
        assert_eq!(
            u64_column(&batches(Arc::clone(&store), nulls).await, "wal_sequence"),
            [1]
        );

        let outside = "SELECT wal_sequence FROM logs WHERE severity_number = 1";
        let files = scan_files(&physical_plan(Arc::clone(&store), outside).await);
        assert!(files.is_empty(), "{files:?}");
        assert!(u64_column(&batches(store, outside).await, "wal_sequence").is_empty());
    }

    #[tokio::test]
    async fn parquet_row_groups_prune_without_changing_rows() {
        let store = store();
        store
            .append(&frame(0, 1, vec![attribute("a", 1)]))
            .expect("append");
        store
            .append(&frame(1, 2, vec![attribute("a", 2)]))
            .expect("append");
        store.rotate().expect("rotate");
        store
            .publish(&PublishOptions {
                parquet: ParquetWriteOptions {
                    max_row_group_rows: Some(1),
                    ..ParquetWriteOptions::default()
                },
                ..PublishOptions::default()
            })
            .expect("publish");

        let sql = "SELECT wal_sequence FROM logs WHERE log_a_i64 = 1 ORDER BY wal_sequence";
        let (rows, plan) = executed_plan(Arc::clone(&store), sql).await;
        assert_eq!(u64_column(&rows, "wal_sequence"), [0]);
        assert!(
            row_groups_pruned(&plan) >= 1,
            "pruned {}",
            row_groups_pruned(&plan)
        );
        let mut streamed = engine()
            .execute(Arc::clone(&store), sql, QueryOptions::default())
            .await
            .expect("execute");
        while streamed.next().await.transpose().expect("batch").is_some() {}
        assert!(streamed.metrics().row_groups_pruned >= 1);
        assert_eq!(u64_column(&batches(store, sql).await, "wal_sequence"), [0]);
    }

    #[tokio::test]
    async fn query_metrics_count_pruned_files_rows_and_cancellation() {
        let store = store();
        publish_row(&store, frame(0, 1, vec![attribute("a", 1)]));
        publish_row(&store, frame(1, 2, vec![attribute("a", 2)]));
        let engine = engine();
        let mut stream = engine
            .execute(
                Arc::clone(&store),
                "SELECT wal_sequence FROM logs WHERE log_a_i64 = 1",
                QueryOptions::default(),
            )
            .await
            .expect("execute");
        let mut rows = Vec::new();
        while let Some(batch) = stream.next().await {
            rows.push(batch.expect("batch"));
        }
        assert_eq!(u64_column(&rows, "wal_sequence"), [0]);
        let metrics = stream.metrics();
        assert_eq!(metrics.rows_returned, 1);
        assert_eq!(metrics.files_scanned, 1);
        assert_eq!(metrics.files_pruned, 1);
        assert!(!metrics.timed_out);
        assert!(!metrics.cancelled);

        let mut cancelled = engine
            .execute(
                store,
                "SELECT wal_sequence FROM logs",
                QueryOptions::default(),
            )
            .await
            .expect("execute");
        cancelled.cancel();
        let error = cancelled.next().await.expect("cancelled");
        assert!(matches!(error, Err(QueryError::Cancelled)));
        assert!(cancelled.metrics().cancelled);
    }

    #[tokio::test]
    async fn concurrent_ingest_publish_and_queries_keep_each_row_once() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = open_at(directory.path());
        let engine = engine_with(4);
        let rows = 24u64;

        let ingest = async {
            for sequence in 0..rows {
                store
                    .append(&frame(sequence, sequence + 1, Vec::new()))
                    .expect("append");
                if sequence % 4 == 3 {
                    store.rotate().expect("rotate");
                    store.publish(&PublishOptions::default()).expect("publish");
                }
                tokio::task::yield_now().await;
            }
            store.rotate().expect("rotate");
            store.publish(&PublishOptions::default()).expect("publish");
        };

        let read = async {
            for _ in 0..6 {
                let mut stream = engine
                    .execute(
                        Arc::clone(&store),
                        "SELECT wal_sequence FROM logs",
                        QueryOptions::default(),
                    )
                    .await
                    .expect("execute");
                let mut sequences = Vec::new();
                while let Some(batch) = stream.next().await {
                    sequences.extend(u64_column(&[batch.expect("batch")], "wal_sequence"));
                }
                let mut unique = sequences.clone();
                unique.sort_unstable();
                unique.dedup();
                assert_eq!(unique.len(), sequences.len());
                assert!(unique.iter().all(|sequence| *sequence < rows));
                assert_eq!(stream.metrics().rows_returned, sequences.len() as u64);
                tokio::task::yield_now().await;
            }
        };

        let timeout_engine =
            QueryEngine::new(QueryEngineConfig::default()).expect("timeout engine");
        let timeout = async {
            let error = timeout_engine
                .execute(
                    Arc::clone(&store),
                    "SELECT wal_sequence FROM logs ORDER BY wal_sequence",
                    QueryOptions {
                        timeout: Duration::from_nanos(1),
                        max_rows: QueryOptions::MAX_ROWS,
                    },
                )
                .await;
            assert!(matches!(error, Err(QueryError::Timeout)));
        };

        let cancel = async {
            let mut stream = engine
                .execute(
                    Arc::clone(&store),
                    "SELECT wal_sequence FROM logs",
                    QueryOptions::default(),
                )
                .await
                .expect("execute");
            stream.cancel();
            let error = stream.next().await.expect("cancelled");
            assert!(matches!(error, Err(QueryError::Cancelled)));
            assert!(stream.metrics().cancelled);
        };

        let spill = async {
            spilled_sort_reports_bytes().await;
        };

        tokio::join!(ingest, read, timeout, cancel, spill);

        let mut stream = engine
            .execute(
                store,
                "SELECT wal_sequence FROM logs ORDER BY wal_sequence",
                QueryOptions::default(),
            )
            .await
            .expect("final");
        let mut sequences = Vec::new();
        while let Some(batch) = stream.next().await {
            sequences.extend(u64_column(&[batch.expect("batch")], "wal_sequence"));
        }
        assert_eq!(sequences, (0..rows).collect::<Vec<_>>());
        assert_eq!(stream.metrics().rows_returned, rows);
        assert!(!stream.metrics().cancelled);
    }

    async fn spilled_sort_reports_bytes() {
        let store = store();
        for sequence in 0..40 {
            store
                .append(&frame_body(
                    sequence,
                    sequence + 1,
                    &"x".repeat(64 * 1024),
                    Vec::new(),
                ))
                .expect("append");
        }
        let spill = tempfile::tempdir().expect("spill");
        let engine = QueryEngine::new(QueryEngineConfig {
            memory_pool_bytes: 3 * 1024 * 1024,
            sort_spill_reservation_bytes: 64 * 1024,
            spill_directory: spill.path().to_path_buf(),
            spill_budget_bytes: 64 * 1024 * 1024,
            ..QueryEngineConfig::default()
        })
        .expect("engine");
        let mut stream = engine
            .execute(
                store,
                "SELECT body FROM logs ORDER BY body",
                QueryOptions::default(),
            )
            .await
            .expect("execute");
        let mut rows = 0u64;
        while let Some(batch) = stream.next().await {
            rows += u64::try_from(batch.expect("batch").num_rows()).expect("rows");
        }
        assert_eq!(rows, 40);
        assert!(stream.metrics().spill_bytes > 0);
        assert_eq!(engine.runtime().disk_manager.used_disk_space(), 0);
    }

    fn scan_files(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
        scan_shape(plan).files
    }

    fn publish_row(store: &Store, logs: observer_storage::DecodedLogs) {
        store.append(&logs).expect("append");
        store.rotate().expect("rotate");
        store.publish(&PublishOptions::default()).expect("publish");
    }

    fn frame_severity(sequence: u64, time: u64, severity: i32) -> observer_storage::DecodedLogs {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: time,
                        severity_number: severity,
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(format!("row-{sequence}"))),
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

    async fn executed_plan(
        store: Arc<Store>,
        sql: &str,
    ) -> (Vec<RecordBatch>, Arc<dyn ExecutionPlan>) {
        let snapshot = store.snapshot().expect("snapshot");
        let provider = ObserverTableProvider::new(store, snapshot).expect("provider");
        let context =
            SessionContext::new_with_config(super::session_config(&QueryEngineConfig::default()));
        context.add_optimizer_rule(Arc::new(super::NewestEventLimit));
        context
            .register_table("logs", Arc::new(provider))
            .expect("register");
        let frame = context.sql(sql).await.expect("sql");
        let plan = frame.create_physical_plan().await.expect("plan");
        let task = context.task_ctx();
        let mut rows = Vec::new();
        for partition in 0..plan.output_partitioning().partition_count() {
            let mut stream = plan.execute(partition, Arc::clone(&task)).expect("execute");
            while let Some(batch) = stream.next().await {
                rows.push(batch.expect("batch"));
            }
        }
        (rows, plan)
    }

    #[tokio::test]
    async fn newest_limit_skips_older_hours_until_a_filter_is_present() {
        let store = store();
        publish_row(&store, frame(0, 1, Vec::new()));
        publish_row(&store, frame(1, HOUR + 1, Vec::new()));

        let newest = "SELECT wal_sequence FROM logs ORDER BY event_time_unix_nano DESC, wal_sequence DESC LIMIT 1";
        let files = scan_files(&physical_plan(Arc::clone(&store), newest).await);
        assert!(
            files.iter().any(|file| file.contains("1-2.parquet")),
            "{files:?}"
        );
        assert!(
            files.iter().all(|file| !file.contains("0-1.parquet")),
            "{files:?}"
        );
        assert_eq!(
            u64_column(&batches(Arc::clone(&store), newest).await, "wal_sequence"),
            [1]
        );

        let oldest = "SELECT wal_sequence FROM logs ORDER BY event_time_unix_nano ASC, wal_sequence ASC LIMIT 1";
        let files = scan_files(&physical_plan(Arc::clone(&store), oldest).await);
        assert!(
            files.iter().any(|file| file.contains("0-1.parquet")),
            "{files:?}"
        );
        assert!(
            files.iter().any(|file| file.contains("1-2.parquet")),
            "{files:?}"
        );
        assert_eq!(
            u64_column(&batches(Arc::clone(&store), oldest).await, "wal_sequence"),
            [0]
        );

        let filtered = "SELECT wal_sequence FROM logs WHERE event_time_unix_nano >= 1 ORDER BY event_time_unix_nano DESC, wal_sequence DESC LIMIT 1";
        let files = scan_files(&physical_plan(Arc::clone(&store), filtered).await);
        assert!(
            files.iter().any(|file| file.contains("0-1.parquet")),
            "{files:?}"
        );
        assert!(
            files.iter().any(|file| file.contains("1-2.parquet")),
            "{files:?}"
        );
        assert_eq!(
            u64_column(&batches(store, filtered).await, "wal_sequence"),
            [1]
        );
    }

    #[tokio::test]
    async fn equal_event_time_breaks_ties_with_wal_sequence() {
        let store = store();
        store.append(&frame(0, 5, Vec::new())).expect("append");
        store.append(&frame(1, 5, Vec::new())).expect("append");
        store.rotate().expect("rotate");
        store.publish(&PublishOptions::default()).expect("publish");
        let descending =
            "SELECT wal_sequence FROM logs ORDER BY event_time_unix_nano DESC, wal_sequence DESC";
        assert_eq!(
            u64_column(
                &batches(Arc::clone(&store), descending).await,
                "wal_sequence"
            ),
            [1, 0]
        );
        let ascending =
            "SELECT wal_sequence FROM logs ORDER BY event_time_unix_nano ASC, wal_sequence ASC";
        assert_eq!(
            u64_column(&batches(store, ascending).await, "wal_sequence"),
            [0, 1]
        );
    }

    #[tokio::test]
    async fn mixed_memory_and_parquet_follow_newest_event_order() {
        let store = store();
        publish_row(&store, frame(0, 1, Vec::new()));
        store
            .append(&frame(1, HOUR + 1, Vec::new()))
            .expect("active");
        let ordered =
            "SELECT wal_sequence FROM logs ORDER BY event_time_unix_nano DESC, wal_sequence DESC";
        assert_eq!(
            u64_column(&batches(Arc::clone(&store), ordered).await, "wal_sequence"),
            [1, 0]
        );
        let limited = "SELECT wal_sequence FROM logs ORDER BY event_time_unix_nano DESC, wal_sequence DESC LIMIT 1";
        let shape = scan_shape(&physical_plan(Arc::clone(&store), limited).await);
        assert!(shape.files.is_empty(), "{:?}", shape.files);
        assert!(shape.memory_scans > 0);
        assert_eq!(
            u64_column(&batches(store, limited).await, "wal_sequence"),
            [1]
        );
    }

    #[test]
    fn provider_keeps_generation_batches_until_a_scan_projects_them() {
        let store = store();
        let wide: Vec<_> = (0..6)
            .map(|index| attribute(&format!("unused{index}"), index))
            .chain(std::iter::once(attribute("keep", 1)))
            .collect();
        store
            .append(&frame_body(0, 1, "old", wide))
            .expect("append");
        store.rotate().expect("rotate");
        store
            .append(&frame_body(1, HOUR + 1, "new", vec![attribute("keep", 2)]))
            .expect("active");
        let snapshot = store.snapshot().expect("snapshot");
        let original = Arc::clone(
            &snapshot
                .frozen
                .first()
                .expect("frozen")
                .partitions
                .first()
                .expect("partition")
                .batches[0],
        );
        let old_body = Arc::clone(
            original
                .column_by_name(observer_storage::COLUMN_BODY)
                .expect("body"),
        );
        let body_count = Arc::strong_count(&old_body);
        let provider = ObserverTableProvider::new(Arc::clone(&store), snapshot).expect("provider");
        assert!(Arc::ptr_eq(&provider.hours[0].batches[0], &original));
        assert_eq!(Arc::strong_count(&old_body), body_count);

        let body = provider
            .schema
            .index_of(observer_storage::COLUMN_BODY)
            .expect("body");
        let event = provider
            .schema
            .index_of(observer_storage::COLUMN_EVENT_TIME_UNIX_NANO)
            .expect("event");
        let keep = provider.schema.index_of("log_keep_i64").expect("keep");
        let projection = vec![body, event, keep];
        let selected = &provider.hours[1];
        let selected_body = Arc::clone(
            selected.batches[0]
                .column_by_name(observer_storage::COLUMN_BODY)
                .expect("body"),
        );
        let selected_keep = Arc::clone(
            selected.batches[0]
                .column_by_name("log_keep_i64")
                .expect("keep"),
        );
        let pieces =
            super::hour_pieces(selected, &[], &provider.schema, Some(&projection)).expect("pieces");
        assert_eq!(Arc::strong_count(&old_body), body_count);
        let super::ScanSource::Memory(batches) = &pieces[0].source else {
            panic!("memory piece");
        };
        let names: Vec<_> = batches[0]
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        assert!(names.contains(&"body".to_owned()));
        assert!(names.contains(&"log_keep_i64".to_owned()));
        assert!(names.iter().all(|name| !name.starts_with("log_unused")));
        assert!(Arc::ptr_eq(
            batches[0]
                .column_by_name(observer_storage::COLUMN_BODY)
                .expect("body"),
            &selected_body
        ));
        assert!(Arc::ptr_eq(
            batches[0].column_by_name("log_keep_i64").expect("keep"),
            &selected_keep
        ));
        assert!(batches[0].column_by_name("log_unused0_i64").is_none());
    }

    #[tokio::test]
    async fn projected_columns_match_for_active_frozen_and_published_rows() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = open_at(directory.path());
        let wide: Vec<_> = (0..6)
            .map(|index| attribute(&format!("unused{index}"), index))
            .chain(std::iter::once(attribute("keep", 1)))
            .collect();
        store
            .append(&frame_body(0, 1, "old", wide))
            .expect("frozen");
        store.rotate().expect("rotate");
        store
            .append(&frame_body(1, HOUR + 1, "new", vec![attribute("keep", 2)]))
            .expect("active");
        let sql = format!(
            "SELECT body FROM logs WHERE event_time_unix_nano >= {HOUR} AND log_keep_i64 = 2"
        );

        let active = batches(Arc::clone(&store), &sql).await;
        assert_eq!(utf8_column(&active, "body"), vec![Some("new".to_owned())]);
        let shape = scan_shape(&physical_plan(Arc::clone(&store), &sql).await);
        assert!(shape.files.is_empty(), "{:?}", shape.files);
        assert!(shape.memory_scans > 0);
        for schema in &shape.schemas {
            assert!(schema.contains(&"body".to_owned()), "{schema:?}");
            assert!(schema.contains(&"log_keep_i64".to_owned()), "{schema:?}");
            assert!(
                schema.iter().all(|name| !name.starts_with("log_unused")),
                "{schema:?}"
            );
        }

        store
            .publish(&PublishOptions::default())
            .expect("publish frozen");
        let mixed = batches(Arc::clone(&store), &sql).await;
        assert_eq!(utf8_column(&mixed, "body"), vec![Some("new".to_owned())]);

        store.rotate().expect("rotate");
        store
            .publish(&PublishOptions::default())
            .expect("publish active");
        let published = batches(store, &sql).await;
        assert_eq!(
            utf8_column(&published, "body"),
            vec![Some("new".to_owned())]
        );
    }

    fn utf8_column(batches: &[arrow_array::RecordBatch], name: &str) -> Vec<Option<String>> {
        batches
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column_by_name(name)
                    .expect("column")
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("utf8");
                (0..column.len())
                    .map(move |row| (!column.is_null(row)).then(|| column.value(row).to_owned()))
            })
            .collect()
    }

    fn row_groups_pruned(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let mut pruned = 0;
        let _ = plan.apply(|node| {
            if let Some(metrics) = node.metrics()
                && let Some(MetricValue::PruningMetrics {
                    pruning_metrics, ..
                }) = metrics.sum_by_name("row_groups_pruned_statistics")
            {
                pruned += pruning_metrics.pruned();
            }
            Ok(TreeNodeRecursion::Continue)
        });
        pruned
    }

    #[tokio::test]
    async fn sql_reads_only_the_store_passed_to_execute() {
        let left = store();
        let right = store();
        left.append(&frame(0, 1, vec![attribute("user-id", 7)]))
            .expect("append");
        right.append(&frame(0, 1, Vec::new())).expect("append");
        right.append(&frame(1, 2, Vec::new())).expect("append");

        let left_rows = batches(
            Arc::clone(&left),
            "SELECT wal_sequence, log_user_id_i64 FROM logs ORDER BY wal_sequence",
        )
        .await;
        assert_eq!(u64_column(&left_rows, "wal_sequence"), [0]);
        assert_eq!(i64_column(&left_rows, "log_user_id_i64"), vec![Some(7)]);

        let right_rows = batches(Arc::clone(&right), "SELECT count(*) AS rows FROM logs").await;
        assert_eq!(i64_column(&right_rows, "rows"), vec![Some(2)]);

        let crossed = batches(
            Arc::clone(&left),
            "SELECT wal_sequence FROM logs WHERE tenant_id = 'tenant-b'",
        )
        .await;
        assert!(u64_column(&crossed, "wal_sequence").is_empty());

        let missing_table = match engine()
            .execute(left, "SELECT body FROM other", QueryOptions::default())
            .await
        {
            Ok(_) => panic!("another table was accepted"),
            Err(error) => error,
        };
        assert!(matches!(missing_table, QueryError::Planning(_)));
    }

    #[tokio::test]
    async fn sql_on_an_empty_snapshot_returns_no_rows() {
        let rows = batches(store(), "SELECT count(*) AS rows FROM logs").await;
        assert_eq!(i64_column(&rows, "rows"), vec![Some(0)]);
    }

    #[tokio::test]
    async fn sql_rejects_non_query_statements() {
        let engine = engine();
        let store = store();
        for sql in [
            "",
            "INSERT INTO logs VALUES (1)",
            "EXPLAIN SELECT * FROM logs",
            "SELECT * FROM logs; SELECT * FROM logs",
        ] {
            let error = match engine
                .execute(Arc::clone(&store), sql, QueryOptions::default())
                .await
            {
                Ok(_) => panic!("{sql} was accepted"),
                Err(error) => error,
            };
            assert!(matches!(error, QueryError::Statement(_)), "{sql}: {error}");
        }
        let missing = match engine
            .execute(store, "SELECT missing FROM logs", QueryOptions::default())
            .await
        {
            Ok(_) => panic!("missing column was accepted"),
            Err(error) => error,
        };
        assert!(matches!(missing, QueryError::Planning(_)));
    }

    #[test]
    fn builds_a_shared_runtime() {
        let engine = engine();
        assert_eq!(engine.runtime().memory_pool.reserved(), 0);
        assert_eq!(engine.runtime().memory_pool.name(), "fair");
        assert_eq!(
            engine.runtime().cache_manager.get_metadata_cache_limit(),
            QueryEngine::METADATA_CACHE_BYTES
        );
        assert!(engine.runtime().disk_manager.tmp_files_enabled());
    }

    #[tokio::test]
    async fn timeout_and_row_limit_stop_the_query() {
        let store = store();
        store.append(&frame(0, 1, Vec::new())).expect("append");
        store.append(&frame(1, 2, Vec::new())).expect("append");
        let timed_out = engine()
            .execute(
                Arc::clone(&store),
                "SELECT wal_sequence FROM logs",
                QueryOptions {
                    timeout: Duration::ZERO,
                    max_rows: QueryOptions::MAX_ROWS,
                },
            )
            .await;
        assert!(matches!(timed_out, Err(QueryError::Timeout)));

        let mut stream = engine()
            .execute(
                store,
                "SELECT wal_sequence FROM logs",
                QueryOptions {
                    timeout: QueryOptions::TIMEOUT,
                    max_rows: 1,
                },
            )
            .await
            .expect("execute");
        let mut yielded = 0;
        let mut limited = false;
        while let Some(item) = stream.next().await {
            match item {
                Ok(batch) => yielded += batch.num_rows(),
                Err(QueryError::RowLimit { max_rows: 1 }) => {
                    limited = true;
                    break;
                }
                Err(error) => panic!("{error}"),
            }
        }
        assert!(yielded <= 1, "yielded {yielded} rows");
        assert!(limited);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn a_query_times_out_while_its_stream_is_open() {
        let store = store();
        store.append(&frame(0, 1, Vec::new())).expect("append");
        let engine = paused_engine();
        let mut stream = engine
            .execute(
                store,
                "SELECT wal_sequence FROM logs",
                QueryOptions {
                    timeout: Duration::from_secs(5),
                    max_rows: QueryOptions::MAX_ROWS,
                },
            )
            .await
            .expect("execute");
        engine.advance_time(Duration::from_secs(5)).await;
        let expired = stream.next().await.expect("timeout");
        assert!(matches!(expired, Err(QueryError::Timeout)));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn a_query_times_out_while_planning() {
        let blocked = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(false));
        let engine = paused_engine();
        let execute = engine.execute_provider(
            blocking_provider(BlockStage::Scan, &blocked, &stopped),
            "SELECT wal_sequence FROM logs",
            short_timeout(),
        );
        tokio::pin!(execute);
        wait_until_blocked(&blocked, &mut execute).await;
        engine.advance_time(Duration::from_secs(5)).await;
        let error = match execute.await {
            Ok(_stream) => panic!("query started"),
            Err(error) => error,
        };
        assert!(matches!(error, QueryError::Timeout), "{error}");
    }

    #[tokio::test]
    async fn a_query_times_out_before_the_first_batch() {
        let blocked = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(false));
        let engine = paused_engine();
        let mut stream = engine
            .execute_provider(
                blocking_provider(BlockStage::BeforeBatch, &blocked, &stopped),
                "SELECT wal_sequence FROM logs",
                short_timeout(),
            )
            .await
            .expect("execute");
        let next = stream.next();
        tokio::pin!(next);
        wait_until_blocked(&blocked, &mut next).await;
        engine.advance_time(Duration::from_secs(5)).await;
        let expired = next.await.expect("timeout");
        assert!(matches!(expired, Err(QueryError::Timeout)));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn a_query_times_out_between_batches() {
        let blocked = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(false));
        let engine = paused_engine();
        let mut stream = engine
            .execute_provider(
                blocking_provider(BlockStage::AfterBatch, &blocked, &stopped),
                "SELECT wal_sequence FROM logs",
                short_timeout(),
            )
            .await
            .expect("execute");
        let first = stream.next().await.expect("batch").expect("row");
        assert_eq!(first.num_rows(), 1);
        assert!(!blocked.load(Ordering::SeqCst));
        let next = stream.next();
        tokio::pin!(next);
        wait_until_blocked(&blocked, &mut next).await;
        engine.advance_time(Duration::from_secs(5)).await;
        let expired = next.await.expect("timeout");
        assert!(matches!(expired, Err(QueryError::Timeout)));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn dropping_a_running_query_stops_its_task() {
        let blocked = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(false));
        let engine = engine();
        let mut stream = engine
            .execute_provider(
                blocking_provider(BlockStage::BeforeBatch, &blocked, &stopped),
                "SELECT wal_sequence FROM logs",
                QueryOptions::default(),
            )
            .await
            .expect("execute");
        {
            let next = stream.next();
            tokio::pin!(next);
            wait_until_blocked(&blocked, &mut next).await;
        }
        drop(stream);
        wait_until(&stopped).await;
    }

    #[tokio::test]
    async fn dropping_execute_stops_planning() {
        let blocked = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(false));
        let engine = engine();
        {
            let execute = engine.execute_provider(
                blocking_provider(BlockStage::Scan, &blocked, &stopped),
                "SELECT wal_sequence FROM logs",
                QueryOptions::default(),
            );
            tokio::pin!(execute);
            wait_until_blocked(&blocked, &mut execute).await;
        }
        wait_until(&stopped).await;
    }

    #[tokio::test]
    async fn a_query_is_busy_when_admission_expires_before_work_starts() {
        let engine = engine_with(1);
        let held = Arc::new(AtomicBool::new(false));
        let started = Arc::new(AtomicBool::new(false));
        let hold = engine.execute_provider(
            blocking_provider(BlockStage::Scan, &held, &Arc::new(AtomicBool::new(false))),
            "SELECT wal_sequence FROM logs",
            QueryOptions::default(),
        );
        tokio::pin!(hold);
        wait_until_blocked(&held, &mut hold).await;
        let error = engine
            .execute_provider(
                blocking_provider(
                    BlockStage::Enter,
                    &started,
                    &Arc::new(AtomicBool::new(false)),
                ),
                "SELECT wal_sequence FROM logs",
                brief_timeout(),
            )
            .await;
        let error = match error {
            Ok(_stream) => panic!("query started"),
            Err(error) => error,
        };
        assert!(matches!(error, QueryError::Busy), "{error}");
        assert!(!started.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn the_next_query_starts_after_the_permit_is_released() {
        let engine = engine_with(1);
        let held = Arc::new(AtomicBool::new(false));
        let started = Arc::new(AtomicBool::new(false));
        let second = engine.execute_provider(
            blocking_provider(
                BlockStage::Enter,
                &started,
                &Arc::new(AtomicBool::new(false)),
            ),
            "SELECT wal_sequence FROM logs",
            QueryOptions::default(),
        );
        tokio::pin!(second);
        {
            let hold = engine.execute_provider(
                blocking_provider(BlockStage::Scan, &held, &Arc::new(AtomicBool::new(false))),
                "SELECT wal_sequence FROM logs",
                QueryOptions::default(),
            );
            tokio::pin!(hold);
            wait_until_blocked(&held, &mut hold).await;
            let deadline = std::time::Instant::now() + Duration::from_millis(50);
            loop {
                tokio::select! {
                    biased;
                    _ = &mut second => panic!("second query started while the permit was held"),
                    () = tokio::task::yield_now() => {
                        if std::time::Instant::now() >= deadline {
                            break;
                        }
                    }
                }
            }
            assert!(!started.load(Ordering::SeqCst));
        }
        second.await.expect("second query");
        assert!(started.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn a_finished_query_releases_its_permit() {
        let engine = engine_with(1);
        let store = store();
        store.append(&frame(0, 1, Vec::new())).expect("append");
        let mut stream = engine
            .execute(
                store,
                "SELECT wal_sequence FROM logs",
                QueryOptions::default(),
            )
            .await
            .expect("execute");
        while stream.next().await.transpose().expect("batch").is_some() {}
        assert!(following_query_starts(&engine).await);
    }

    #[tokio::test]
    async fn a_planning_error_releases_its_permit() {
        let engine = engine_with(1);
        let error = engine
            .execute(store(), "SELECT missing FROM logs", QueryOptions::default())
            .await;
        let error = match error {
            Ok(_stream) => panic!("missing column was accepted"),
            Err(error) => error,
        };
        assert!(matches!(error, QueryError::Planning(_)), "{error}");
        assert!(following_query_starts(&engine).await);
    }

    #[tokio::test]
    async fn a_planning_timeout_releases_its_permit() {
        let engine = engine_with(1);
        let error = engine
            .execute_provider(
                blocking_provider(
                    BlockStage::Scan,
                    &Arc::new(AtomicBool::new(false)),
                    &Arc::new(AtomicBool::new(false)),
                ),
                "SELECT wal_sequence FROM logs",
                brief_timeout(),
            )
            .await;
        let error = match error {
            Ok(_stream) => panic!("blocked scan finished"),
            Err(error) => error,
        };
        assert!(matches!(error, QueryError::Timeout), "{error}");
        assert!(following_query_starts(&engine).await);
    }

    #[tokio::test]
    async fn cancelling_or_dropping_releases_the_permit() {
        let engine = engine_with(1);
        let mut stream = engine
            .execute_provider(
                blocking_provider(
                    BlockStage::BeforeBatch,
                    &Arc::new(AtomicBool::new(false)),
                    &Arc::new(AtomicBool::new(false)),
                ),
                "SELECT wal_sequence FROM logs",
                QueryOptions::default(),
            )
            .await
            .expect("execute");
        stream.cancel();
        assert!(following_query_starts(&engine).await);

        let stream = engine
            .execute_provider(
                blocking_provider(
                    BlockStage::BeforeBatch,
                    &Arc::new(AtomicBool::new(false)),
                    &Arc::new(AtomicBool::new(false)),
                ),
                "SELECT wal_sequence FROM logs",
                QueryOptions::default(),
            )
            .await
            .expect("execute");
        drop(stream);
        assert!(following_query_starts(&engine).await);
    }

    #[tokio::test]
    async fn invalid_sql_does_not_take_a_permit() {
        let engine = engine_with(1);
        let held = Arc::new(AtomicBool::new(false));
        let hold = engine.execute_provider(
            blocking_provider(BlockStage::Scan, &held, &Arc::new(AtomicBool::new(false))),
            "SELECT wal_sequence FROM logs",
            QueryOptions::default(),
        );
        tokio::pin!(hold);
        wait_until_blocked(&held, &mut hold).await;
        let error = engine.execute(store(), "", brief_timeout()).await;
        let error = match error {
            Ok(_stream) => panic!("empty SQL was accepted"),
            Err(error) => error,
        };
        assert!(matches!(error, QueryError::Statement(_)), "{error}");
    }

    #[test]
    fn concurrency_settings_must_be_at_least_one() {
        let no_queries = QueryEngine::new(QueryEngineConfig {
            max_concurrent_queries: 0,
            ..QueryEngineConfig::default()
        });
        assert!(matches!(no_queries, Err(QueryError::Resources(_))));
        let no_partitions = QueryEngine::new(QueryEngineConfig {
            target_partitions_per_query: 0,
            ..QueryEngineConfig::default()
        });
        assert!(matches!(no_partitions, Err(QueryError::Resources(_))));
    }

    #[tokio::test]
    async fn each_query_uses_the_configured_partition_cap() {
        let partitions = Arc::new(AtomicUsize::new(0));
        let engine = QueryEngine::new(QueryEngineConfig {
            target_partitions_per_query: 3,
            ..QueryEngineConfig::default()
        })
        .expect("engine");
        engine
            .execute_provider(
                provider_with(
                    BlockStage::Enter,
                    &Arc::new(AtomicBool::new(false)),
                    &Arc::new(AtomicBool::new(false)),
                    Arc::clone(&partitions),
                ),
                "SELECT wal_sequence FROM logs",
                QueryOptions::default(),
            )
            .await
            .expect("execute");
        assert_eq!(partitions.load(Ordering::SeqCst), 3);
    }

    async fn following_query_starts(engine: &QueryEngine) -> bool {
        let started = Arc::new(AtomicBool::new(false));
        let executed = engine
            .execute_provider(
                blocking_provider(
                    BlockStage::Enter,
                    &started,
                    &Arc::new(AtomicBool::new(false)),
                ),
                "SELECT wal_sequence FROM logs",
                QueryOptions {
                    timeout: Duration::from_secs(2),
                    max_rows: QueryOptions::MAX_ROWS,
                },
            )
            .await;
        executed.is_ok() && started.load(Ordering::SeqCst)
    }

    #[tokio::test]
    async fn cancelling_the_stream_stops_the_query() {
        let store = store();
        store.append(&frame(0, 1, Vec::new())).expect("append");
        let engine = engine();
        let mut stream = engine
            .execute(
                store,
                "SELECT wal_sequence FROM logs ORDER BY wal_sequence",
                QueryOptions::default(),
            )
            .await
            .expect("execute");
        stream.cancel();
        let error = stream.next().await.expect("cancelled");
        assert!(matches!(error, Err(QueryError::Cancelled)));
        assert!(stream.next().await.is_none());
        assert_eq!(engine.runtime().memory_pool.reserved(), 0);
    }

    #[tokio::test]
    async fn a_tiny_memory_pool_rejects_a_sorting_query() {
        let store = store();
        store.append(&frame(0, 1, Vec::new())).expect("append");
        store.append(&frame(1, 2, Vec::new())).expect("append");
        let engine = QueryEngine::new(QueryEngineConfig {
            memory_pool_bytes: 1,
            spill_budget_bytes: 0,
            ..QueryEngineConfig::default()
        })
        .expect("engine");
        let executed = engine
            .execute(
                store,
                "SELECT wal_sequence FROM logs ORDER BY wal_sequence",
                QueryOptions::default(),
            )
            .await;
        let error = match executed {
            Ok(mut stream) => stream.next().await.expect("exhausted"),
            Err(error) => Err(error),
        };
        assert!(matches!(error, Err(QueryError::Resources(_))), "{error:?}");
    }

    #[test]
    fn batch_size_must_be_at_least_one() {
        let engine = QueryEngine::new(QueryEngineConfig {
            batch_size: 0,
            ..QueryEngineConfig::default()
        });
        assert!(matches!(engine, Err(QueryError::Resources(_))));
    }

    #[tokio::test]
    async fn each_query_uses_the_configured_session() {
        let seen = Arc::new(Mutex::new(None));
        let engine = QueryEngine::new(QueryEngineConfig {
            batch_size: 128,
            target_partitions_per_query: 3,
            ..QueryEngineConfig::default()
        })
        .expect("engine");
        engine
            .execute_provider(
                Arc::new(SessionProbe {
                    schema: probe_schema(),
                    seen: Arc::clone(&seen),
                    pools: Arc::new(Mutex::new(Vec::new())),
                }),
                "SELECT wal_sequence FROM logs",
                QueryOptions::default(),
            )
            .await
            .expect("execute");
        let seen = seen.lock().expect("session").clone().expect("recorded");
        assert_eq!(seen.batch_size, 128);
        assert_eq!(seen.target_partitions, 3);
        assert!(seen.pushdown_filters);
        assert!(seen.reorder_filters);
        assert!(seen.pruning);
        assert!(seen.enable_page_index);
        assert!(!seen.bloom_filter_on_read);
        assert_eq!(
            seen.metadata_size_hint,
            Some(QueryEngine::PARQUET_FOOTER_HINT_BYTES)
        );
        assert!(seen.collect_statistics);
        assert!(seen.repartition_file_scans);
        assert_eq!(
            seen.repartition_file_min_size,
            QueryEngine::FILE_SCAN_MIN_BYTES
        );
    }

    #[tokio::test]
    async fn concurrent_queries_share_one_memory_pool() {
        let pools = Arc::new(Mutex::new(Vec::new()));
        let engine = engine_with(2);
        let provider = Arc::new(SessionProbe {
            schema: probe_schema(),
            seen: Arc::new(Mutex::new(None)),
            pools: Arc::clone(&pools),
        });
        let first = engine.execute_provider(
            Arc::clone(&provider) as Arc<dyn TableProvider>,
            "SELECT wal_sequence FROM logs",
            QueryOptions::default(),
        );
        let second = engine.execute_provider(
            provider,
            "SELECT wal_sequence FROM logs",
            QueryOptions::default(),
        );
        let (first, second) = tokio::join!(first, second);
        first.expect("first");
        second.expect("second");
        let pools = pools.lock().expect("pools");
        assert_eq!(pools.len(), 2);
        let shared = Arc::as_ptr(&engine.runtime().memory_pool) as *const () as usize;
        assert!(pools.iter().all(|pool| *pool == shared));
        assert_eq!(engine.runtime().memory_pool.reserved(), 0);
    }

    #[tokio::test]
    async fn a_small_pool_spills_and_clears_the_spill_files() {
        let store = store();
        let body = "x".repeat(64 * 1024);
        for sequence in 0..40 {
            store
                .append(&frame_body(sequence, 1, &body, Vec::new()))
                .expect("append");
        }
        let blocked = tempfile::tempdir().expect("blocked spill");
        let blocked_engine = QueryEngine::new(QueryEngineConfig {
            memory_pool_bytes: 1024 * 1024,
            sort_spill_reservation_bytes: 64 * 1024,
            spill_directory: blocked.path().to_path_buf(),
            spill_budget_bytes: 1,
            ..QueryEngineConfig::default()
        })
        .expect("engine");
        let blocked_query = blocked_engine
            .execute(
                Arc::clone(&store),
                "SELECT body FROM logs ORDER BY body",
                QueryOptions::default(),
            )
            .await;
        let blocked_error = match blocked_query {
            Ok(mut stream) => stream.next().await.expect("exhausted"),
            Err(error) => Err(error),
        };
        assert!(
            matches!(blocked_error, Err(QueryError::Resources(_))),
            "{blocked_error:?}"
        );

        let spill = tempfile::tempdir().expect("spill");
        let engine = QueryEngine::new(QueryEngineConfig {
            memory_pool_bytes: 3 * 1024 * 1024,
            sort_spill_reservation_bytes: 64 * 1024,
            spill_directory: spill.path().to_path_buf(),
            spill_budget_bytes: 64 * 1024 * 1024,
            ..QueryEngineConfig::default()
        })
        .expect("engine");
        let mut stream = engine
            .execute(
                store,
                "SELECT body FROM logs ORDER BY body",
                QueryOptions::default(),
            )
            .await
            .expect("execute");
        let mut rows = 0usize;
        while let Some(batch) = stream.next().await {
            rows += batch.expect("batch").num_rows();
        }
        assert_eq!(rows, 40);
        let metrics = stream.metrics();
        assert_eq!(metrics.rows_returned, 40);
        assert!(metrics.spill_bytes > 0, "{}", metrics.spill_bytes);
        assert!(metrics.memory_peak_bytes > 0);
        assert!(!metrics.timed_out);
        assert!(!metrics.cancelled);
        drop(stream);
        assert_eq!(engine.runtime().disk_manager.used_disk_space(), 0);
        assert_eq!(spill_file_count(spill.path()), 0);
        drop(engine);
        assert_eq!(spill_file_count(spill.path()), 0);
    }

    #[tokio::test]
    async fn parquet_metadata_is_reused_across_queries() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = open_at(directory.path());
        store.append(&frame(0, 1, Vec::new())).expect("append");
        store.rotate().expect("rotate");
        store.publish(&PublishOptions::default()).expect("publish");
        let engine = engine();
        let cache = engine.runtime().cache_manager.get_file_metadata_cache();
        assert_eq!(cache.len(), 0);
        for _ in 0..2 {
            let mut stream = engine
                .execute(
                    Arc::clone(&store),
                    "SELECT wal_sequence FROM logs",
                    QueryOptions::default(),
                )
                .await
                .expect("execute");
            while let Some(batch) = stream.next().await {
                batch.expect("batch");
            }
        }
        assert!(!cache.is_empty(), "metadata cache stayed empty");
        let reused = cache.list_entries().values().any(|entry| entry.hits > 0);
        assert!(reused, "second query did not hit the metadata cache");
    }

    fn spill_file_count(path: &Path) -> usize {
        let mut count = 0usize;
        let mut pending = vec![path.to_path_buf()];
        while let Some(directory) = pending.pop() {
            let Ok(entries) = std::fs::read_dir(&directory) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    pending.push(path);
                } else {
                    count += 1;
                }
            }
        }
        count
    }

    fn probe_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "wal_sequence",
            DataType::UInt64,
            false,
        )]))
    }

    #[derive(Clone, Debug)]
    struct ProbeSession {
        batch_size: usize,
        target_partitions: usize,
        pushdown_filters: bool,
        reorder_filters: bool,
        pruning: bool,
        enable_page_index: bool,
        bloom_filter_on_read: bool,
        metadata_size_hint: Option<usize>,
        collect_statistics: bool,
        repartition_file_scans: bool,
        repartition_file_min_size: usize,
    }

    #[derive(Debug)]
    struct SessionProbe {
        schema: SchemaRef,
        seen: Arc<Mutex<Option<ProbeSession>>>,
        pools: Arc<Mutex<Vec<usize>>>,
    }

    #[async_trait]
    impl TableProvider for SessionProbe {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            let options = state.config().options();
            let execution = &options.execution;
            let parquet = &execution.parquet;
            let optimizer = &options.optimizer;
            *self.seen.lock().expect("session") = Some(ProbeSession {
                batch_size: execution.batch_size.get(),
                target_partitions: execution.target_partitions,
                pushdown_filters: parquet.pushdown_filters,
                reorder_filters: parquet.reorder_filters,
                pruning: parquet.pruning,
                enable_page_index: parquet.enable_page_index,
                bloom_filter_on_read: parquet.bloom_filter_on_read,
                metadata_size_hint: parquet.metadata_size_hint,
                collect_statistics: execution.collect_statistics,
                repartition_file_scans: optimizer.repartition_file_scans,
                repartition_file_min_size: optimizer.repartition_file_min_size,
            });
            self.pools
                .lock()
                .expect("pools")
                .push(Arc::as_ptr(&state.runtime_env().memory_pool) as *const () as usize);
            let table = MemTable::try_new(Arc::clone(&self.schema), vec![vec![]])?;
            table.scan(state, projection, &[], None).await
        }
    }

    fn brief_timeout() -> QueryOptions {
        QueryOptions {
            timeout: Duration::from_millis(50),
            max_rows: QueryOptions::MAX_ROWS,
        }
    }

    fn short_timeout() -> QueryOptions {
        QueryOptions {
            timeout: Duration::from_secs(5),
            max_rows: QueryOptions::MAX_ROWS,
        }
    }

    async fn wait_until(flag: &AtomicBool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !flag.load(Ordering::SeqCst) {
            assert!(
                std::time::Instant::now() < deadline,
                "query task was not dropped"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    async fn wait_until_blocked<F: std::future::Future>(
        blocked: &AtomicBool,
        pending: &mut Pin<&mut F>,
    ) {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            tokio::select! {
                biased;
                _ = pending.as_mut() => panic!("query finished before it blocked"),
                () = tokio::task::yield_now() => {
                    if blocked.load(Ordering::SeqCst) {
                        return;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "query did not block"
                    );
                }
            }
        }
    }

    fn blocking_provider(
        stage: BlockStage,
        blocked: &Arc<AtomicBool>,
        stopped: &Arc<AtomicBool>,
    ) -> Arc<dyn TableProvider> {
        provider_with(stage, blocked, stopped, Arc::new(AtomicUsize::new(0)))
    }

    fn provider_with(
        stage: BlockStage,
        blocked: &Arc<AtomicBool>,
        stopped: &Arc<AtomicBool>,
        partitions: Arc<AtomicUsize>,
    ) -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "wal_sequence",
            DataType::UInt64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt64Array::from(vec![1_u64]))],
        )
        .expect("batch");
        Arc::new(BlockingProvider {
            schema,
            batch,
            stage,
            blocked: Arc::clone(blocked),
            stopped: Arc::clone(stopped),
            partitions,
        })
    }

    #[derive(Clone, Copy, Debug)]
    enum BlockStage {
        Scan,
        Enter,
        BeforeBatch,
        AfterBatch,
    }

    struct StopGuard(Arc<AtomicBool>);

    impl Drop for StopGuard {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[derive(Debug)]
    struct BlockingProvider {
        schema: SchemaRef,
        batch: RecordBatch,
        stage: BlockStage,
        blocked: Arc<AtomicBool>,
        stopped: Arc<AtomicBool>,
        partitions: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl TableProvider for BlockingProvider {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            self.partitions.store(
                state.config().options().execution.target_partitions,
                Ordering::SeqCst,
            );
            if matches!(self.stage, BlockStage::Scan | BlockStage::Enter) {
                self.blocked.store(true, Ordering::SeqCst);
                if matches!(self.stage, BlockStage::Scan) {
                    let stop = StopGuard(Arc::clone(&self.stopped));
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    drop(stop);
                }
                let table = MemTable::try_new(Arc::clone(&self.schema), vec![vec![]])?;
                return table.scan(state, projection, &[], None).await;
            }
            Ok(Arc::new(DelayExec {
                batch: self.batch.clone(),
                stage: self.stage,
                blocked: Arc::clone(&self.blocked),
                stopped: Arc::clone(&self.stopped),
                properties: Arc::new(PlanProperties::new(
                    EquivalenceProperties::new(Arc::clone(&self.schema)),
                    Partitioning::UnknownPartitioning(1),
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                )),
                schema: Arc::clone(&self.schema),
            }))
        }
    }

    struct DelayExec {
        schema: SchemaRef,
        batch: RecordBatch,
        stage: BlockStage,
        blocked: Arc<AtomicBool>,
        stopped: Arc<AtomicBool>,
        properties: Arc<PlanProperties>,
    }

    impl std::fmt::Debug for DelayExec {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("DelayExec")
                .field("stage", &self.stage)
                .finish_non_exhaustive()
        }
    }

    impl DisplayAs for DelayExec {
        fn fmt_as(
            &self,
            _format_type: DisplayFormatType,
            formatter: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            formatter.write_str("DelayExec")
        }
    }

    impl ExecutionPlan for DelayExec {
        fn name(&self) -> &str {
            "DelayExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            Vec::new()
        }

        fn apply_expressions(
            &self,
            _expressions: &mut dyn FnMut(
                &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
            )
                -> datafusion::error::Result<TreeNodeRecursion>,
        ) -> datafusion::error::Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }

        #[allow(deprecated)]
        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> datafusion::error::Result<SendableRecordBatchStream> {
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                DelayStream {
                    batch: Some(self.batch.clone()),
                    sleep: None,
                    delay_before: matches!(self.stage, BlockStage::BeforeBatch),
                    emitted: false,
                    blocked: Arc::clone(&self.blocked),
                    _stop: StopGuard(Arc::clone(&self.stopped)),
                },
            )))
        }
    }

    struct DelayStream {
        batch: Option<RecordBatch>,
        sleep: Option<Pin<Box<tokio::time::Sleep>>>,
        delay_before: bool,
        emitted: bool,
        blocked: Arc<AtomicBool>,
        _stop: StopGuard,
    }

    impl Stream for DelayStream {
        type Item = Result<RecordBatch, DataFusionError>;

        fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let stream = self.get_mut();
            if stream.delay_before && !stream.emitted {
                if stream.sleep_ready(context).is_pending() {
                    return Poll::Pending;
                }
                stream.emitted = true;
                return Poll::Ready(Some(Ok(stream.batch.take().expect("batch"))));
            }
            if !stream.emitted {
                stream.emitted = true;
                return Poll::Ready(Some(Ok(stream.batch.take().expect("batch"))));
            }
            if stream.delay_before || stream.sleep_ready(context).is_ready() {
                return Poll::Ready(None);
            }
            Poll::Pending
        }
    }

    impl DelayStream {
        fn sleep_ready(&mut self, context: &mut Context<'_>) -> Poll<()> {
            if self.sleep.is_none() {
                self.blocked.store(true, Ordering::SeqCst);
                self.sleep = Some(Box::pin(tokio::time::sleep(Duration::from_secs(60))));
            }
            self.sleep.as_mut().expect("sleep").as_mut().poll(context)
        }
    }
}
