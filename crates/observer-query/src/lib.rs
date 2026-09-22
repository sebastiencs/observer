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
//! One engine shares a bounded memory pool. Each query uses its own session and the timeout and
//! returned-row cap in [`QueryOptions`]. Dropping or cancelling the result stream stops the query.
//!
//! This crate does not expose an HTTP or gRPC query API.

use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
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
use futures::Stream;
use observer_storage::{
    COLUMN_EVENT_TIME_UNIX_NANO, EventHour, Scan, Store, StoreError, StoreSnapshot, align_batch,
};

const LOGS_TABLE: &str = "logs";
const NANOS_PER_HOUR: u64 = 3_600_000_000_000;

/// Shared DataFusion resources for tenant-scoped SQL execution.
pub struct QueryEngine {
    runtime: Arc<RuntimeEnv>,
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
            | Self::Cancelled
            | Self::RowLimit { .. } => None,
        }
    }
}

impl QueryEngine {
    /// Bytes available to every query that shares this engine.
    pub const MEMORY_POOL_BYTES: usize = 256 * 1024 * 1024;

    /// Build a runtime whose queries share one greedy memory pool of `memory_pool_bytes`.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::Resources`] when the runtime cannot be constructed.
    pub fn new(memory_pool_bytes: usize) -> Result<Self, QueryError> {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(memory_pool_bytes)))
            .build()
            .map_err(|error| QueryError::Resources(error.to_string()))?;
        Ok(Self {
            runtime: Arc::new(runtime),
        })
    }

    /// Shared execution resources for every query-local session.
    #[must_use]
    pub fn runtime(&self) -> &RuntimeEnv {
        &self.runtime
    }

    /// Run one `SELECT` against the rows visible in `store` at call time.
    ///
    /// The returned stream has no order unless `sql` contains `ORDER BY`. `options.timeout` covers
    /// planning and reading the stream. `options.max_rows` is the most rows the stream yields
    /// before it returns [`QueryError::RowLimit`].
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::Statement`] for an empty string, multiple statements, or any statement
    /// other than `SELECT`. Storage, planning, execution, timeout, and resource failures use the
    /// matching variant.
    pub async fn execute(
        &self,
        store: Arc<Store>,
        sql: &str,
        options: QueryOptions,
    ) -> Result<QueryBatchStream, QueryError> {
        if options.timeout.is_zero() {
            return Err(QueryError::Timeout);
        }
        let deadline = tokio::time::Instant::now() + options.timeout;
        ensure_query(sql)?;
        if tokio::time::Instant::now() >= deadline {
            return Err(QueryError::Timeout);
        }
        let snapshot = store.snapshot().map_err(QueryError::Storage)?;
        let provider = ObserverTableProvider::new(Arc::clone(&store), snapshot)?;
        let context =
            SessionContext::new_with_config_rt(SessionConfig::new(), Arc::clone(&self.runtime));
        context
            .register_table(LOGS_TABLE, Arc::new(provider))
            .map_err(QueryError::Planning)?;
        let frame = context.sql(sql).await.map_err(QueryError::Planning)?;
        if tokio::time::Instant::now() >= deadline {
            return Err(QueryError::Timeout);
        }
        let inner = frame.execute_stream().await.map_err(execution_error)?;
        Ok(QueryBatchStream {
            inner: Some(inner),
            deadline: Box::pin(tokio::time::sleep_until(deadline)),
            max_rows: options.max_rows,
            emitted: 0,
            pending: None,
        })
    }
}

/// Arrow batches for one query, with the engine's timeout and row cap applied.
pub struct QueryBatchStream {
    inner: Option<SendableRecordBatchStream>,
    deadline: Pin<Box<tokio::time::Sleep>>,
    max_rows: usize,
    emitted: usize,
    pending: Option<QueryError>,
}

impl QueryBatchStream {
    /// Stop the query. The next poll returns [`QueryError::Cancelled`] and no further batches.
    pub fn cancel(&mut self) {
        if self.inner.take().is_some() {
            self.pending = Some(QueryError::Cancelled);
        }
    }
}

impl Stream for QueryBatchStream {
    type Item = Result<RecordBatch, QueryError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let stream = self.get_mut();
        if let Some(error) = stream.pending.take() {
            return Poll::Ready(Some(Err(error)));
        }
        if stream.inner.is_none() {
            return Poll::Ready(None);
        }
        if stream.deadline.as_mut().poll(context).is_ready() {
            stream.inner = None;
            return Poll::Ready(Some(Err(QueryError::Timeout)));
        }
        let polled = stream
            .inner
            .as_mut()
            .map(|inner| Pin::new(inner).poll_next(context));
        match polled {
            Some(Poll::Pending) => Poll::Pending,
            None | Some(Poll::Ready(None)) => {
                stream.inner = None;
                Poll::Ready(None)
            }
            Some(Poll::Ready(Some(Err(error)))) => {
                stream.inner = None;
                Poll::Ready(Some(Err(execution_error(error))))
            }
            Some(Poll::Ready(Some(Ok(batch)))) => {
                let rows = batch.num_rows();
                if rows > 0 && stream.emitted.saturating_add(rows) > stream.max_rows {
                    stream.inner = None;
                    Poll::Ready(Some(Err(QueryError::RowLimit {
                        max_rows: stream.max_rows,
                    })))
                } else {
                    stream.emitted += rows;
                    Poll::Ready(Some(Ok(batch)))
                }
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
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use arrow_array::{Array, Int64Array, StringArray, UInt64Array};
    use bytes::Bytes;
    use datafusion::datasource::physical_plan::FileScanConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_plan::ExecutionPlan;
    use observer_protocol::otlp::{
        AnyValue, ExportLogsServiceRequest, KeyValue, LogRecord, ResourceLogs, ScopeLogs, any_value,
    };
    use observer_storage::{
        DynamicLimits, ManualClock, MemtableConfig, PublishOptions, Scan, Store, decode_logs_frame,
    };
    use observer_wal::{Frame, FrameSignal};
    use prost::Message;
    use tokio_stream::StreamExt;

    use super::{ObserverTableProvider, QueryEngine, QueryError, QueryOptions};

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
        QueryEngine::new(QueryEngine::MEMORY_POOL_BYTES).expect("engine")
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
    async fn sql_on_an_empty_snapshot_returns_no_rows() {
        let rows = batches(store(), "SELECT count(*) AS rows FROM logs").await;
        assert_eq!(i64_column(&rows, "rows"), vec![Some(0)]);
    }

    #[tokio::test]
    async fn sql_rejects_non_query_statements() {
        let engine = engine();
        let store = store();
        for sql in [
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

    #[tokio::test(start_paused = true)]
    async fn a_query_times_out_while_its_stream_is_open() {
        let store = store();
        store.append(&frame(0, 1, Vec::new())).expect("append");
        let mut stream = engine()
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
        tokio::time::advance(Duration::from_secs(5)).await;
        let expired = stream.next().await.expect("timeout");
        assert!(matches!(expired, Err(QueryError::Timeout)));
        assert!(stream.next().await.is_none());
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
        let engine = QueryEngine::new(1).expect("engine");
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
}
