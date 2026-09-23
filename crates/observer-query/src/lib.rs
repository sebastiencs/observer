//! Internal SQL execution over one tenant snapshot.
//!
//! [`QueryEngine::execute`] pins one [`Store`] snapshot, registers a single `logs` table, and
//! returns an Arrow stream. Each event hour is one partition. That partition unions the hour's
//! active and frozen batches with the Parquet files named by published commits. A file that is not
//! named by a commit is not read.
//!
//! The scan projects only the physical columns DataFusion asks for. Comparisons on
//! `event_time_unix_nano` drop event hours that cannot match; DataFusion applies that nanosecond
//! predicate again. Every other filter stays above the scan.
//!
//! Results have no order unless the SQL contains `ORDER BY`. The tenant is the store passed to
//! `execute`, never a value inside the SQL. Only one `SELECT` statement is accepted.
//!
//! One engine shares a bounded memory pool, a query runtime, and a limit on how many queries run at
//! once. Each query uses its own session and the timeout and returned-row cap in [`QueryOptions`].
//! That timeout bounds planning and reading. A query that cannot obtain a permit before the deadline
//! is busy. Dropping or cancelling the result stream stops the query and releases its permit.
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
//! `LIMIT` applies to the ordered rows when `ORDER BY` is present. A dynamic column is
//! `{source}_{normalized_path}_{type}` (for example `log_user_id_i64`). A generation that lacks
//! that column contributes nulls. An event-time bound can skip whole hours; DataFusion still
//! applies the nanosecond predicate.
//!
//! This crate does not expose an HTTP or gRPC query API.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::DataFusionError;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, SendableRecordBatchStream,
};
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::Statement;
use futures::{Stream, StreamExt};
use observer_storage::{
    COLUMN_EVENT_TIME_UNIX_NANO, EventHour, Scan, Store, StoreError, StoreSnapshot, align_batch,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;

const LOGS_TABLE: &str = "logs";
const NANOS_PER_HOUR: u64 = 3_600_000_000_000;

/// Shared DataFusion resources for tenant-scoped SQL execution.
pub struct QueryEngine {
    runtime: Arc<RuntimeEnv>,
    threads: Arc<QueryThreads>,
    admission: Arc<Semaphore>,
    target_partitions: usize,
}

/// Shared limits for every query on one engine.
///
/// [`Default`] derives the concurrency and partition cap once from the process parallelism.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryEngineConfig {
    /// Bytes available to every query that shares this engine.
    pub memory_pool_bytes: usize,
    /// Queries that may plan and read at the same time.
    pub max_concurrent_queries: usize,
    /// DataFusion partition target for each query.
    pub target_partitions_per_query: usize,
}

impl Default for QueryEngineConfig {
    fn default() -> Self {
        let parallelism = query_worker_threads();
        Self {
            memory_pool_bytes: QueryEngine::MEMORY_POOL_BYTES,
            max_concurrent_queries: parallelism,
            target_partitions_per_query: parallelism,
        }
    }
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

    /// Build a runtime from `config`.
    ///
    /// Planning and execution run on threads named `observer-query`. At most
    /// [`QueryEngineConfig::max_concurrent_queries`] queries hold an admission permit at once.
    /// A result stream keeps the query threads and its permit alive until the query finishes or
    /// the stream is dropped.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::Resources`] when the runtime cannot be constructed, or when a
    /// concurrency setting is zero.
    pub fn new(config: QueryEngineConfig) -> Result<Self, QueryError> {
        Self::with_threads(config.checked()?, QueryThreads::start()?)
    }

    fn with_threads(config: QueryEngineConfig, threads: QueryThreads) -> Result<Self, QueryError> {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(config.memory_pool_bytes)))
            .build()
            .map_err(|error| QueryError::Resources(error.to_string()))?;
        Ok(Self {
            runtime: Arc::new(runtime),
            threads: Arc::new(threads),
            admission: Arc::new(Semaphore::new(config.max_concurrent_queries)),
            target_partitions: config.target_partitions_per_query,
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
        Self::with_threads(config.checked()?, QueryThreads::start_paused()?)
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
        &self.runtime
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
        let runtime = Arc::clone(&self.runtime);
        let admission = Arc::clone(&self.admission);
        let target_partitions = self.target_partitions;
        let sql = sql.to_owned();
        let max_rows = options.max_rows;
        let timeout = options.timeout;
        let mut task = TaskGuard(Some(self.threads.spawn(async move {
            match prepare(runtime, admission, target_partitions, input, sql, timeout).await {
                Err(error) => {
                    let _ = ready_tx.send(Err(error));
                }
                Ok((stream, deadline, permit)) => {
                    if ready_tx.send(Ok(permit)).is_err() {
                        return;
                    }
                    forward(stream, pull_rx, deadline, max_rows).await;
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
            }),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(query_task_stopped()),
        }
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
    target_partitions: usize,
    input: QueryInput,
    sql: String,
    timeout: Duration,
) -> Result<
    (
        SendableRecordBatchStream,
        tokio::time::Instant,
        OwnedSemaphorePermit,
    ),
    QueryError,
> {
    let deadline = tokio::time::Instant::now() + timeout;
    ensure_query(&sql)?;
    if tokio::time::Instant::now() >= deadline {
        return Err(QueryError::Timeout);
    }
    let permit = match tokio::time::timeout_at(deadline, admission.acquire_owned()).await {
        Ok(Ok(permit)) => permit,
        Ok(Err(_closed)) => return Err(QueryError::Busy),
        Err(_elapsed) => return Err(QueryError::Busy),
    };
    let prepared = tokio::time::timeout_at(deadline, async move {
        let provider = table_provider(input)?;
        let context = SessionContext::new_with_config_rt(
            SessionConfig::new().with_target_partitions(target_partitions),
            runtime,
        );
        context
            .register_table(LOGS_TABLE, provider)
            .map_err(QueryError::Planning)?;
        let frame = context.sql(&sql).await.map_err(QueryError::Planning)?;
        frame.execute_stream().await.map_err(execution_error)
    })
    .await;
    match prepared {
        Ok(stream) => stream.map(|stream| (stream, deadline, permit)),
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
        Ok(self)
    }
}

fn table_provider(input: QueryInput) -> Result<Arc<dyn TableProvider>, QueryError> {
    match input {
        QueryInput::Store(store) => {
            let snapshot = store.snapshot().map_err(QueryError::Storage)?;
            Ok(Arc::new(ObserverTableProvider::new(store, snapshot)?))
        }
        #[cfg(test)]
        QueryInput::Provider(provider) => Ok(provider),
    }
}

async fn forward(
    mut stream: SendableRecordBatchStream,
    mut pulls: mpsc::Receiver<Pull>,
    deadline: tokio::time::Instant,
    max_rows: usize,
) {
    let mut emitted = 0usize;
    loop {
        let pull = tokio::select! {
            biased;
            () = tokio::time::sleep_until(deadline) => {
                deliver_timeout(&mut pulls).await;
                return;
            }
            pull = pulls.recv() => {
                let Some(pull) = pull else {
                    return;
                };
                pull
            }
        };
        if tokio::time::Instant::now() >= deadline {
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
                    let _ = pull
                        .reply
                        .send(Some(Err(QueryError::RowLimit { max_rows })));
                    return;
                }
                emitted += rows;
                if pull.reply.send(Some(Ok(batch))).is_err() {
                    return;
                }
            }
            Err(error) => {
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
}

impl QueryBatchStream {
    /// Stop the query. The next poll returns [`QueryError::Cancelled`] and no further batches.
    pub fn cancel(&mut self) {
        if self.finished || self.terminal.is_some() {
            return;
        }
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
    if memory_exhausted(&error) {
        QueryError::Resources(error.to_string())
    } else {
        QueryError::Execution(error)
    }
}

fn memory_exhausted(error: &DataFusionError) -> bool {
    match error {
        DataFusionError::ResourcesExhausted(_) => true,
        DataFusionError::Context(_, inner) | DataFusionError::Diagnostic(_, inner) => {
            memory_exhausted(inner)
        }
        DataFusionError::Shared(inner) => memory_exhausted(inner),
        DataFusionError::Collection(errors) => errors.iter().any(memory_exhausted),
        DataFusionError::External(error) => error
            .downcast_ref::<DataFusionError>()
            .is_some_and(memory_exhausted),
        _ => false,
    }
}

/// One event hour's visible batches and commit-referenced Parquet files.
struct HourPartition {
    hour: EventHour,
    files: Vec<std::path::PathBuf>,
    batches: Vec<RecordBatch>,
}

/// DataFusion table for one pinned tenant snapshot.
///
/// Each [`HourPartition`] becomes one output partition. Within that partition, memory batches and
/// commit-referenced Parquet files are aligned to [`StoreSnapshot::schema`]. Missing nullable
/// columns are typed nulls.
pub struct ObserverTableProvider {
    store: Arc<Store>,
    snapshot: StoreSnapshot,
    schema: SchemaRef,
    hours: Vec<HourPartition>,
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
    /// Align the snapshot's in-memory batches and keep the commit-referenced Parquet paths.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::Storage`] when the snapshot schema is incompatible or a batch cannot
    /// be aligned.
    pub fn new(store: Arc<Store>, snapshot: StoreSnapshot) -> Result<Self, QueryError> {
        let schema = snapshot.schema().map_err(QueryError::Storage)?;
        let mut hours = Vec::new();
        for hour in store.sources(&snapshot, &Scan::default()) {
            if hour.files.is_empty() && hour.batches.is_empty() {
                continue;
            }
            let mut batches = Vec::with_capacity(hour.batches.len());
            for batch in &hour.batches {
                batches.push(
                    align_batch(batch, Arc::clone(&schema))
                        .map_err(|error| QueryError::Storage(StoreError::from(error)))?,
                );
            }
            hours.push(HourPartition {
                hour: hour.hour,
                files: hour.files,
                batches,
            });
        }
        Ok(Self {
            store,
            snapshot,
            schema,
            hours,
        })
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
                if time_predicate(filter).is_some() {
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
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let window = hour_window(filters);
        let selected: Vec<_> = self
            .hours
            .iter()
            .filter(|hour| window.contains(hour.hour))
            .collect();
        if selected.is_empty() {
            let table = MemTable::try_new(Arc::clone(&self.schema), vec![vec![]])?;
            return table.scan(state, projection, &[], None).await;
        }
        let mut hour_plans = Vec::with_capacity(selected.len());
        for hour in selected {
            hour_plans.push(hour_plan(state, &self.schema, hour, projection).await?);
        }
        UnionExec::try_new(hour_plans)
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

async fn hour_plan(
    state: &dyn Session,
    schema: &SchemaRef,
    hour: &HourPartition,
    projection: Option<&Vec<usize>>,
) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    let mut inputs = Vec::new();
    if !hour.files.is_empty() {
        inputs.push(parquet_plan(schema, &hour.files, projection).await?);
    }
    if !hour.batches.is_empty() {
        let table = MemTable::try_new(Arc::clone(schema), vec![hour.batches.clone()])?;
        inputs.push(table.scan(state, projection, &[], None).await?);
    }
    one_partition(UnionExec::try_new(inputs)?)
}

async fn parquet_plan(
    schema: &SchemaRef,
    files: &[std::path::PathBuf],
    projection: Option<&Vec<usize>>,
) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    let mut groups = Vec::with_capacity(files.len());
    for file in files {
        groups.push(FileGroup::new(vec![partitioned_file(file).await?]));
    }
    let source = Arc::new(ParquetSource::new(Arc::clone(schema)));
    let mut builder = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
        .with_file_groups(groups);
    if let Some(indices) = projection {
        builder = builder.with_projection_indices(Some(indices.clone()))?;
    }
    Ok(DataSourceExec::from_data_source(builder.build()))
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use arrow_array::{Array, Int64Array, RecordBatch, StringArray, UInt64Array};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use async_trait::async_trait;
    use bytes::Bytes;
    use datafusion::catalog::{Session, TableProvider};
    use datafusion::common::tree_node::TreeNodeRecursion;
    use datafusion::datasource::MemTable;
    use datafusion::datasource::physical_plan::FileScanConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::error::DataFusionError;
    use datafusion::execution::TaskContext;
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr::{Expr, TableType};
    use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    };
    use futures::Stream;
    use observer_protocol::otlp::{
        AnyValue, ExportLogsServiceRequest, KeyValue, LogRecord, ResourceLogs, ScopeLogs, any_value,
    };
    use observer_storage::{
        DynamicLimits, ManualClock, MemtableConfig, PublishOptions, Scan, Store, decode_logs_frame,
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
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: time,
                        attributes,
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
        let committed = &published[0].files[0];
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
        assert_eq!(engine.runtime().memory_pool.name(), "greedy");
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
