//! Internal SQL execution over one tenant snapshot.
//!
//! [`QueryEngine::execute`] pins one [`Store`] snapshot, registers a single `logs` table, and
//! returns an Arrow stream. Each event hour is one partition. That partition unions the hour's
//! active and frozen batches with the Parquet files named by published commits. A file that is not
//! named by a commit is not read.
//!
//! Results have no order unless the SQL contains `ORDER BY`. The tenant is the store passed to
//! `execute`, never a value inside the SQL. Only one `SELECT` statement is accepted.
//!
//! This crate does not expose an HTTP or gRPC query API.

use std::path::Path;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::DataFusionError;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, SendableRecordBatchStream,
};
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::Statement;
use observer_storage::{Scan, Store, StoreError, StoreSnapshot, align_batch};

const LOGS_TABLE: &str = "logs";

/// Shared DataFusion resources for tenant-scoped SQL execution.
pub struct QueryEngine {
    runtime: Arc<RuntimeEnv>,
}

/// Why a query could not be planned or executed.
#[derive(Debug)]
pub enum QueryError {
    /// The DataFusion runtime could not be built.
    Resources(String),
    /// The pinned snapshot could not be read or aligned.
    Storage(StoreError),
    /// SQL could not be parsed or planned.
    Planning(DataFusionError),
    /// A planned query failed while executing.
    Execution(DataFusionError),
    /// The SQL is not a single `SELECT` statement.
    Statement(String),
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Resources(detail) => write!(formatter, "query runtime: {detail}"),
            Self::Storage(error) => write!(formatter, "{error}"),
            Self::Planning(error) => write!(formatter, "query planning: {error}"),
            Self::Execution(error) => write!(formatter, "query execution: {error}"),
            Self::Statement(detail) => formatter.write_str(detail),
        }
    }
}

impl std::error::Error for QueryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            Self::Planning(error) | Self::Execution(error) => Some(error),
            Self::Resources(_) | Self::Statement(_) => None,
        }
    }
}

impl QueryEngine {
    /// Build a runtime with DataFusion's default memory pool.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::Resources`] when the runtime cannot be constructed.
    pub fn new() -> Result<Self, QueryError> {
        let runtime = RuntimeEnvBuilder::new()
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
    /// The returned stream has no order unless `sql` contains `ORDER BY`.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::Statement`] for an empty string, multiple statements, or any statement
    /// other than `SELECT`. Storage, planning, and execution failures use the matching variant.
    pub async fn execute(
        &self,
        store: Arc<Store>,
        sql: &str,
    ) -> Result<SendableRecordBatchStream, QueryError> {
        ensure_query(sql)?;
        let snapshot = store.snapshot().map_err(QueryError::Storage)?;
        let provider = ObserverTableProvider::new(Arc::clone(&store), snapshot)?;
        let context =
            SessionContext::new_with_config_rt(SessionConfig::new(), Arc::clone(&self.runtime));
        context
            .register_table(LOGS_TABLE, Arc::new(provider))
            .map_err(QueryError::Planning)?;
        let frame = context.sql(sql).await.map_err(QueryError::Planning)?;
        frame.execute_stream().await.map_err(QueryError::Execution)
    }
}

/// One event hour's visible batches and commit-referenced Parquet files.
struct HourPartition {
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

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if self.hours.is_empty() {
            let table = MemTable::try_new(Arc::clone(&self.schema), vec![vec![]])?;
            return table.scan(state, projection, &[], None).await;
        }
        let mut hour_plans = Vec::with_capacity(self.hours.len());
        for hour in &self.hours {
            hour_plans.push(hour_plan(state, &self.schema, hour, projection).await?);
        }
        UnionExec::try_new(hour_plans)
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
        inputs.push(parquet_plan(schema, &hour.files, projection)?);
    }
    if !hour.batches.is_empty() {
        let table = MemTable::try_new(Arc::clone(schema), vec![hour.batches.clone()])?;
        inputs.push(table.scan(state, projection, &[], None).await?);
    }
    one_partition(UnionExec::try_new(inputs)?)
}

fn parquet_plan(
    schema: &SchemaRef,
    files: &[std::path::PathBuf],
    projection: Option<&Vec<usize>>,
) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    let mut groups = Vec::with_capacity(files.len());
    for file in files {
        groups.push(FileGroup::new(vec![partitioned_file(file)?]));
    }
    let source = Arc::new(ParquetSource::new(Arc::clone(schema)));
    let mut builder = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
        .with_file_groups(groups);
    if let Some(indices) = projection {
        builder = builder.with_projection_indices(Some(indices.clone()))?;
    }
    Ok(DataSourceExec::from_data_source(builder.build()))
}

fn partitioned_file(path: &Path) -> datafusion::error::Result<PartitionedFile> {
    let metadata =
        std::fs::metadata(path).map_err(|error| DataFusionError::External(Box::new(error)))?;
    let path = path.to_str().ok_or_else(|| {
        DataFusionError::External(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "parquet path is not utf-8",
        )))
    })?;
    Ok(PartitionedFile::new(path, metadata.len()))
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
    use datafusion::physical_plan::common::collect;
    use observer_protocol::otlp::{
        AnyValue, ExportLogsServiceRequest, KeyValue, LogRecord, ResourceLogs, ScopeLogs, any_value,
    };
    use observer_storage::{
        DynamicLimits, ManualClock, MemtableConfig, PublishOptions, Scan, Store, decode_logs_frame,
    };
    use observer_wal::{Frame, FrameSignal};
    use prost::Message;

    use super::{QueryEngine, QueryError};

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

    async fn batches(store: Arc<Store>, sql: &str) -> Vec<arrow_array::RecordBatch> {
        let engine = QueryEngine::new().expect("engine");
        let stream = engine.execute(store, sql).await.expect("execute");
        collect(stream).await.expect("collect")
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
        let engine = QueryEngine::new().expect("engine");
        let stream = engine
            .execute(Arc::clone(&store), "SELECT wal_sequence FROM logs")
            .await
            .expect("execute");
        store.append(&frame(1, 0, Vec::new())).expect("later");
        let rows = collect(stream).await.expect("collect");
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

    #[tokio::test]
    async fn sql_on_an_empty_snapshot_returns_no_rows() {
        let rows = batches(store(), "SELECT count(*) AS rows FROM logs").await;
        assert_eq!(i64_column(&rows, "rows"), vec![Some(0)]);
    }

    #[tokio::test]
    async fn sql_rejects_non_query_statements() {
        let engine = QueryEngine::new().expect("engine");
        let store = store();
        for sql in [
            "INSERT INTO logs VALUES (1)",
            "EXPLAIN SELECT * FROM logs",
            "SELECT * FROM logs; SELECT * FROM logs",
        ] {
            let error = match engine.execute(Arc::clone(&store), sql).await {
                Ok(_) => panic!("{sql} was accepted"),
                Err(error) => error,
            };
            assert!(matches!(error, QueryError::Statement(_)), "{sql}: {error}");
        }
        let missing = match engine.execute(store, "SELECT missing FROM logs").await {
            Ok(_) => panic!("missing column was accepted"),
            Err(error) => error,
        };
        assert!(matches!(missing, QueryError::Planning(_)));
    }

    #[test]
    fn builds_a_shared_runtime() {
        let engine = QueryEngine::new().expect("runtime");
        assert_eq!(engine.runtime().memory_pool.reserved(), 0);
    }
}
