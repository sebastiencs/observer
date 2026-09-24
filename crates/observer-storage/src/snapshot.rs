//! Pinned view of active memory, frozen memory, and published Parquet.
//!
//! Publication writes every hour file, then the commit descriptor, and only then — under the store
//! lock — adds the commit to the catalog and drops the frozen generation. A snapshot taken under
//! that same lock therefore sees the generation in memory or in the catalog, and a scan reads each
//! row once. [`Store::sources`] reports those same rows grouped by event hour: memory batches plus
//! the absolute paths of files named by published commits.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard},
};

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{Field, Schema, SchemaRef};

use crate::catalog::{Catalog, CatalogError};
use crate::commit::{Commit, CommitError, CommitWriteOptions, commit_for, write_commit};
use crate::layout::tenant_directory;
use crate::parquet::{ParquetError, ParquetWriteOptions, read_parquet_batches, write_generation};
use crate::recovery::{RecoveryError, recover};
use crate::statistics::FileStatistics;
use crate::{
    Appended, Clock, DecodedLogs, EventHour, Generation, Memtable, MemtableConfig, MemtableError,
    align_batch, core_logs_schema,
};

/// Stop publication after the commit descriptor is durable and before memory is released.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublishFault {
    /// The descriptor is on disk. The frozen generation is still in memory.
    Swap,
}

/// Durability faults injected around one publication.
#[derive(Clone, Debug, Default)]
pub struct PublishOptions {
    /// Fault injected while writing Parquet files.
    pub parquet: ParquetWriteOptions,
    /// Fault injected while writing the commit descriptor.
    pub commit: CommitWriteOptions,
    /// Fault injected after both writes have succeeded.
    pub fault: Option<PublishFault>,
}

/// Columns and hour bounds for one scan.
#[derive(Clone, Debug, Default)]
pub struct Scan {
    /// Physical column names to keep, in this order. `None` keeps the reconciled schema.
    pub columns: Option<Vec<String>>,
    /// Inclusive lower bound on the event hour.
    pub from_hour: Option<EventHour>,
    /// Exclusive upper bound on the event hour.
    pub to_hour: Option<EventHour>,
}

/// One Parquet file named by a published commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishedFile {
    /// Absolute path.
    pub path: PathBuf,
    /// Bounds copied from the commit. Absent when the file must be scanned.
    pub statistics: Option<FileStatistics>,
}

/// Memory batches and commit-referenced Parquet files for one event hour.
///
/// Files are named by published commits, oldest commit first. Batches are frozen generations and
/// then the active generation, oldest first. Empty batches are omitted. A directory listing is
/// never a source.
#[derive(Clone, Debug, PartialEq)]
pub struct HourSources {
    /// UTC hour these sources cover.
    pub hour: EventHour,
    /// Published Parquet files.
    pub files: Vec<PublishedFile>,
    /// Active and frozen batches that contain at least one row.
    pub batches: Vec<Arc<RecordBatch>>,
}

/// Point-in-time view of one tenant. Published ranges are not also present as frozen batches.
#[derive(Clone, Debug, PartialEq)]
pub struct StoreSnapshot {
    /// Epoch after the last append, rotation, or publication.
    pub epoch: u64,
    /// Active generation after it has accepted at least one frame.
    pub active: Option<Generation>,
    /// Frozen generations, oldest first.
    pub frozen: Vec<Generation>,
    /// Published commits, oldest range first.
    pub published: Vec<Commit>,
    /// Tenant-relative Parquet paths hidden by durable retirement descriptors.
    pub retired: BTreeSet<String>,
}

impl StoreSnapshot {
    /// Union of every visible generation: core columns, then dynamic columns by physical name.
    ///
    /// Hour bounds do not remove a generation from this schema. A column that one generation lacks
    /// stays in the union so a scan can fill it with typed nulls.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Incompatible`] when two generations give one physical name different
    /// Arrow types.
    pub fn schema(&self) -> Result<SchemaRef, StoreError> {
        union_schema(visible_schemas(self))
    }
}

/// Why the store could not append, publish, recover, or scan.
#[derive(Debug)]
pub enum StoreError {
    /// The store lock was poisoned.
    Poisoned,
    /// The memtable rejected the frame.
    Memtable(MemtableError),
    /// A Parquet file could not be written or read.
    Parquet(ParquetError),
    /// A commit descriptor could not be written.
    Commit(CommitError),
    /// The published ranges are not contiguous.
    Catalog(CatalogError),
    /// Startup recovery failed.
    Recovery(RecoveryError),
    /// A projected column is not present in any included generation.
    MissingColumn(String),
    /// Two generations use different Arrow types for one physical name.
    Incompatible(String),
    /// [`PublishOptions::fault`] stopped the publication after the descriptor was durable.
    Fault(PublishFault),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Poisoned => formatter.write_str("store lock is poisoned"),
            Self::Memtable(error) => write!(formatter, "{error}"),
            Self::Parquet(error) => write!(formatter, "{error}"),
            Self::Commit(error) => write!(formatter, "{error}"),
            Self::Catalog(error) => write!(formatter, "{error}"),
            Self::Recovery(error) => write!(formatter, "{error}"),
            Self::MissingColumn(name) => {
                write!(formatter, "scan column {name} is not in the schema")
            }
            Self::Incompatible(detail) => write!(formatter, "scan schema: {detail}"),
            Self::Fault(step) => write!(formatter, "injected publish fault at {step:?}"),
        }
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Memtable(error) => Some(error),
            Self::Parquet(error) => Some(error),
            Self::Commit(error) => Some(error),
            Self::Catalog(error) => Some(error),
            Self::Recovery(error) => Some(error),
            Self::Poisoned | Self::MissingColumn(_) | Self::Incompatible(_) | Self::Fault(_) => {
                None
            }
        }
    }
}

impl From<MemtableError> for StoreError {
    fn from(error: MemtableError) -> Self {
        Self::Memtable(error)
    }
}

impl From<ParquetError> for StoreError {
    fn from(error: ParquetError) -> Self {
        Self::Parquet(error)
    }
}

impl From<CommitError> for StoreError {
    fn from(error: CommitError) -> Self {
        Self::Commit(error)
    }
}

impl From<CatalogError> for StoreError {
    fn from(error: CatalogError) -> Self {
        Self::Catalog(error)
    }
}

impl From<RecoveryError> for StoreError {
    fn from(error: RecoveryError) -> Self {
        Self::Recovery(error)
    }
}

struct State {
    memtable: Memtable,
    catalog: Catalog,
    retired: BTreeSet<String>,
    leases: BTreeMap<String, u64>,
    collectible: BTreeSet<String>,
    epoch: u64,
}

/// One pin of the Parquet files visible in a snapshot.
///
/// Clones share the pin. The last drop decrements each path under the store lock and, when a
/// retired path reaches zero, marks it collectible.
#[derive(Clone)]
pub struct SnapshotLease {
    inner: Arc<LeaseInner>,
}

impl std::fmt::Debug for SnapshotLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SnapshotLease")
            .field("paths", &self.inner.paths)
            .finish()
    }
}

struct LeaseInner {
    state: Arc<Mutex<State>>,
    paths: Vec<String>,
}

impl std::fmt::Debug for LeaseInner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LeaseInner")
            .field("paths", &self.paths)
            .finish_non_exhaustive()
    }
}

impl Drop for LeaseInner {
    fn drop(&mut self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        for path in &self.paths {
            let Some(count) = state.leases.get_mut(path) else {
                continue;
            };
            *count = count.saturating_sub(1);
            if *count > 0 {
                continue;
            }
            state.leases.remove(path);
            if state.retired.contains(path) {
                state.collectible.insert(path.clone());
            }
        }
    }
}

/// Snapshot whose visible Parquet files stay leased until the last lease handle is dropped.
#[derive(Clone, Debug)]
pub struct PinnedSnapshot {
    snapshot: StoreSnapshot,
    lease: SnapshotLease,
}

impl std::ops::Deref for PinnedSnapshot {
    type Target = StoreSnapshot;

    fn deref(&self) -> &Self::Target {
        &self.snapshot
    }
}

impl PinnedSnapshot {
    /// Lease handle for this pin. Clones keep the same files leased.
    #[must_use]
    pub fn lease(&self) -> SnapshotLease {
        self.lease.clone()
    }
}

/// Memtable plus the durable catalog for one tenant.
pub struct Store {
    root: PathBuf,
    tenant: String,
    state: Arc<Mutex<State>>,
}

impl Store {
    /// Recover published commits, then open an empty memtable positioned at the durable sequence.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when recovery fails or the memtable cannot resume.
    pub fn open(
        root: impl Into<PathBuf>,
        tenant: impl Into<String>,
        config: MemtableConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, StoreError> {
        let root = root.into();
        let tenant = tenant.into();
        let recovered = recover(&root, &tenant)?;
        let mut memtable = Memtable::new(tenant.clone(), config, clock)?;
        if let Some(sequence) = recovered.catalog.durable_sequence() {
            memtable.resume(sequence)?;
        }
        Ok(Self {
            root,
            tenant,
            state: Arc::new(Mutex::new(State {
                memtable,
                catalog: recovered.catalog,
                retired: recovered
                    .retirements
                    .iter()
                    .flat_map(|retirement| {
                        retirement
                            .files
                            .iter()
                            .map(|file| file.relative_path.clone())
                    })
                    .collect(),
                leases: BTreeMap::new(),
                collectible: BTreeSet::new(),
                epoch: 0,
            })),
        })
    }

    /// Exclusive end of the contiguous published prefix.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Poisoned`] when the store lock is poisoned.
    pub fn durable_sequence(&self) -> Result<Option<u64>, StoreError> {
        Ok(self.lock()?.catalog.durable_sequence())
    }

    /// Store one decoded frame.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the memtable rejects the frame.
    pub fn append(&self, logs: &DecodedLogs) -> Result<Appended, StoreError> {
        let mut state = self.lock()?;
        let mut appended = state.memtable.append(logs)?;
        state.epoch = state.epoch.saturating_add(1);
        appended.epoch = state.epoch;
        Ok(appended)
    }

    /// Freeze the active generation.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the frozen queue is full.
    pub fn rotate(&self) -> Result<Option<u64>, StoreError> {
        let mut state = self.lock()?;
        let sealed = state.memtable.rotate()?;
        if sealed.is_some() {
            state.epoch = state.epoch.saturating_add(1);
        }
        Ok(sealed)
    }

    /// Publish the oldest frozen generation.
    ///
    /// Parquet files and the commit descriptor are written before the lock is taken again. The
    /// catalog insert and frozen release then happen together.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the generation is not the next range, a durability step fails,
    /// or the descriptor does not match an existing commit for that range.
    pub fn publish(&self, options: &PublishOptions) -> Result<Option<Commit>, StoreError> {
        let generation = {
            let state = self.lock()?;
            let Some(generation) = state.memtable.oldest_frozen() else {
                return Ok(None);
            };
            ensure_publishable(
                &state.catalog,
                generation.first_sequence,
                generation.next_sequence,
            )?;
            generation
        };
        let files = write_generation(&self.root, &self.tenant, &generation, &options.parquet)?;
        let commit = commit_for(&self.root, &self.tenant, &generation, &files)?;
        write_commit(&self.root, &commit, &options.commit)?;
        if options.fault == Some(PublishFault::Swap) {
            return Err(StoreError::Fault(PublishFault::Swap));
        }
        let mut state = self.lock()?;
        state.catalog.insert(commit.clone())?;
        let matches = state.memtable.oldest_frozen().is_some_and(|oldest| {
            oldest.first_sequence == generation.first_sequence
                && oldest.next_sequence == generation.next_sequence
        });
        if matches {
            let _ = state.memtable.release_oldest_frozen();
        }
        state.epoch = state.epoch.saturating_add(1);
        Ok(Some(commit))
    }

    /// Pin the visible generations and the Parquet files a scan of them can read.
    ///
    /// Retired files are omitted and are not leased. Each leased path's count increases by one.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Poisoned`] when the store lock is poisoned.
    pub fn pin(&self) -> Result<PinnedSnapshot, StoreError> {
        let mut state = self.lock()?;
        let memory = state.memtable.snapshot();
        let published = state.catalog.commits().to_vec();
        let retired = state.retired.clone();
        let mut paths = BTreeSet::new();
        for commit in &published {
            for file in &commit.files {
                if !retired.contains(&file.relative_path) {
                    paths.insert(file.relative_path.clone());
                }
            }
        }
        for path in &paths {
            *state.leases.entry(path.clone()).or_insert(0) += 1;
            state.collectible.remove(path);
        }
        let lease = SnapshotLease {
            inner: Arc::new(LeaseInner {
                state: Arc::clone(&self.state),
                paths: paths.into_iter().collect(),
            }),
        };
        Ok(PinnedSnapshot {
            snapshot: StoreSnapshot {
                epoch: state.epoch,
                active: memory.active,
                frozen: memory.frozen,
                published,
                retired,
            },
            lease,
        })
    }

    /// Relative paths retired and held by no snapshot lease.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Poisoned`] when the store lock is poisoned.
    pub fn collectible(&self) -> Result<BTreeSet<String>, StoreError> {
        Ok(self.lock()?.collectible.clone())
    }

    /// Relative Parquet paths pinned by at least one snapshot, with their pin counts.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Poisoned`] when the store lock is poisoned.
    pub fn leased_paths(&self) -> Result<BTreeMap<String, u64>, StoreError> {
        Ok(self.lock()?.leases.clone())
    }

    /// Read `snapshot` as one `RecordBatch` stream in WAL order.
    ///
    /// Hour bounds skip partitions. Columns absent from a generation are typed nulls. Each
    /// published or frozen row is read from exactly one side of the snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when a Parquet file or a projected column cannot be read.
    pub fn scan(
        &self,
        snapshot: &PinnedSnapshot,
        scan: &Scan,
    ) -> Result<Vec<RecordBatch>, StoreError> {
        let sources = visible_sources(snapshot, scan);
        if sources.is_empty() {
            return Ok(Vec::new());
        }
        let schema = projected_schema(&snapshot.schema()?, scan.columns.as_deref())?;
        let tenant_dir = tenant_directory(&self.root, &self.tenant);
        let mut batches = Vec::new();
        for source in sources {
            match source {
                Visible::Memory {
                    batches: source, ..
                } => {
                    for batch in source {
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        batches.push(align_batch(batch, Arc::clone(&schema))?);
                    }
                }
                Visible::File { relative_path, .. } => {
                    for batch in read_parquet_batches(&tenant_dir.join(relative_path))? {
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        batches.push(align_batch(&batch, Arc::clone(&schema))?);
                    }
                }
            }
        }
        Ok(batches)
    }

    /// Hour-grouped memory batches and commit-referenced Parquet paths in `snapshot`.
    ///
    /// Hours outside `scan` are omitted. Paths are absolute and come only from published commits.
    #[must_use]
    pub fn sources(&self, snapshot: &PinnedSnapshot, scan: &Scan) -> Vec<HourSources> {
        let mut hours = BTreeMap::<EventHour, HourSources>::new();
        for source in visible_sources(snapshot, scan) {
            match source {
                Visible::File {
                    hour,
                    relative_path,
                    statistics,
                } => {
                    hour_sources(&mut hours, hour).files.push(PublishedFile {
                        path: tenant_directory(&self.root, &self.tenant).join(relative_path),
                        statistics: statistics.cloned(),
                    });
                }
                Visible::Memory { hour, batches } => {
                    let kept: Vec<_> = batches
                        .iter()
                        .filter(|batch| batch.num_rows() > 0)
                        .cloned()
                        .collect();
                    if kept.is_empty() {
                        continue;
                    }
                    hour_sources(&mut hours, hour).batches.extend(kept);
                }
            }
        }
        hours.into_values().collect()
    }

    fn lock(&self) -> Result<MutexGuard<'_, State>, StoreError> {
        self.state.lock().map_err(|_| StoreError::Poisoned)
    }

    /// Hide `relative_paths` from new pins. A path with no lease becomes collectible.
    #[cfg(test)]
    fn conceal(&self, relative_paths: &[String]) -> Result<(), StoreError> {
        let mut state = self.lock()?;
        for path in relative_paths {
            state.retired.insert(path.clone());
            let leased = state.leases.get(path).copied().unwrap_or(0) > 0;
            if !leased {
                state.collectible.insert(path.clone());
            }
        }
        Ok(())
    }
}

fn ensure_publishable(catalog: &Catalog, first: u64, next: u64) -> Result<(), CatalogError> {
    if catalog
        .commits()
        .iter()
        .any(|commit| commit.first_sequence == first && commit.next_sequence == next)
    {
        return Ok(());
    }
    match catalog.durable_sequence() {
        Some(expected) if first != expected && first < expected => Err(CatalogError::Overlap {
            first_sequence: first,
            next_sequence: next,
        }),
        Some(expected) if first != expected => Err(CatalogError::Gap {
            expected,
            actual: first,
        }),
        Some(_) | None => Ok(()),
    }
}

enum Visible<'a> {
    Memory {
        hour: EventHour,
        batches: &'a [Arc<RecordBatch>],
    },
    File {
        hour: EventHour,
        relative_path: &'a str,
        statistics: Option<&'a FileStatistics>,
    },
}

fn visible_sources<'a>(snapshot: &'a StoreSnapshot, scan: &Scan) -> Vec<Visible<'a>> {
    let mut sources = Vec::new();
    for commit in &snapshot.published {
        for file in &commit.files {
            if snapshot.retired.contains(&file.relative_path) {
                continue;
            }
            if hour_selected(file.hour, scan) {
                sources.push(Visible::File {
                    hour: file.hour,
                    relative_path: &file.relative_path,
                    statistics: file.statistics.as_ref(),
                });
            }
        }
    }
    for generation in snapshot.frozen.iter().chain(snapshot.active.iter()) {
        for partition in &generation.partitions {
            if hour_selected(partition.hour, scan) {
                sources.push(Visible::Memory {
                    hour: partition.hour,
                    batches: &partition.batches,
                });
            }
        }
    }
    sources
}

fn hour_sources(hours: &mut BTreeMap<EventHour, HourSources>, hour: EventHour) -> &mut HourSources {
    hours.entry(hour).or_insert_with(|| HourSources {
        hour,
        files: Vec::new(),
        batches: Vec::new(),
    })
}

fn visible_schemas(snapshot: &StoreSnapshot) -> impl Iterator<Item = &SchemaRef> {
    snapshot
        .published
        .iter()
        .map(|commit| &commit.schema)
        .chain(snapshot.frozen.iter().map(|generation| &generation.schema))
        .chain(snapshot.active.iter().map(|generation| &generation.schema))
}

fn hour_selected(hour: EventHour, scan: &Scan) -> bool {
    scan.from_hour.is_none_or(|start| hour >= start) && scan.to_hour.is_none_or(|end| hour < end)
}

fn union_schema<'a>(schemas: impl Iterator<Item = &'a SchemaRef>) -> Result<SchemaRef, StoreError> {
    let core = core_logs_schema();
    let mut fields: Vec<Field> = core
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    let mut dynamic = BTreeMap::<String, Field>::new();
    for schema in schemas {
        for field in schema.fields() {
            if core.field_with_name(field.name()).is_ok() {
                let existing = fields
                    .iter()
                    .find(|candidate| candidate.name() == field.name())
                    .expect("core field");
                if existing.data_type() != field.data_type() {
                    return Err(StoreError::Incompatible(format!(
                        "column {} has conflicting types",
                        field.name()
                    )));
                }
            } else {
                match dynamic.get(field.name()) {
                    Some(existing) if existing.data_type() != field.data_type() => {
                        return Err(StoreError::Incompatible(format!(
                            "column {} has conflicting types",
                            field.name()
                        )));
                    }
                    Some(_) => {}
                    None => {
                        dynamic.insert(field.name().clone(), field.as_ref().clone());
                    }
                }
            }
        }
    }
    fields.extend(dynamic.into_values());
    Ok(Arc::new(Schema::new(fields)))
}

/// Align `batch` to `projection` of `schema`.
///
/// Columns that already exist are reused. A typed null array is created only for a projected
/// column the batch does not contain. `None` aligns to every column of `schema`.
///
/// # Errors
///
/// Returns [`StoreError::Incompatible`] when a projected index is outside `schema` or a present
/// column has a different Arrow type, and [`StoreError::Memtable`] when a required column is
/// missing.
pub fn align_projected(
    batch: &RecordBatch,
    schema: SchemaRef,
    projection: Option<&[usize]>,
) -> Result<RecordBatch, StoreError> {
    let schema = match projection {
        Some(indices) => Arc::new(
            schema
                .project(indices)
                .map_err(|error| StoreError::Incompatible(error.to_string()))?,
        ),
        None => schema,
    };
    if schema.fields().is_empty() {
        return RecordBatch::try_new_with_options(
            schema,
            Vec::new(),
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )
        .map_err(|error| StoreError::Incompatible(error.to_string()));
    }
    align_batch(batch, schema).map_err(StoreError::from)
}

fn projected_schema(
    union: &SchemaRef,
    columns: Option<&[String]>,
) -> Result<SchemaRef, StoreError> {
    let Some(columns) = columns else {
        return Ok(Arc::clone(union));
    };
    let mut fields = Vec::with_capacity(columns.len());
    for name in columns {
        let field = union
            .field_with_name(name)
            .map_err(|_| StoreError::MissingColumn(name.clone()))?;
        fields.push(field.clone());
    }
    Ok(Arc::new(Schema::new(fields)))
}

#[cfg(test)]
mod tests {
    use super::{PublishFault, PublishOptions, Scan, Store, StoreError};
    use crate::commit::CommitWriteOptions;
    use crate::{
        COLUMN_BODY, COLUMN_WAL_SEQUENCE, CommitFault, DynamicLimits, EventHour, ManualClock,
        MemtableConfig, MemtableError, ParquetFault, ParquetWriteOptions, commit_path,
        decode_logs_frame,
    };
    use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use observer_protocol::otlp::{
        AnyValue, ExportLogsServiceRequest, KeyValue, LogRecord, ResourceLogs, ScopeLogs, any_value,
    };
    use observer_wal::{Frame, FrameSignal};
    use prost::Message;
    use std::{fs, sync::Arc, time::Duration};

    const HOUR: u64 = 3_600_000_000_000;

    fn config() -> MemtableConfig {
        MemtableConfig {
            max_rows: 100,
            max_bytes: u64::MAX,
            max_age: Duration::from_secs(60),
            max_frozen: 4,
            max_dynamic_columns: 32,
        }
    }

    fn open_store(directory: &tempfile::TempDir) -> Store {
        Store::open(
            directory.path(),
            "tenant-a",
            config(),
            Arc::new(ManualClock::new(0)),
        )
        .expect("open")
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

    fn frame(sequence: u64, time: u64, attributes: Vec<KeyValue>) -> crate::DecodedLogs {
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

    fn empty_frame(sequence: u64) -> crate::DecodedLogs {
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

    fn seal(store: &Store, logs: crate::DecodedLogs) {
        store.append(&logs).expect("append");
        store.rotate().expect("rotate");
    }

    fn sequences(batches: &[impl std::borrow::Borrow<arrow_array::RecordBatch>]) -> Vec<u64> {
        batches
            .iter()
            .flat_map(|batch| {
                let batch = batch.borrow();
                let column = batch
                    .column_by_name(COLUMN_WAL_SEQUENCE)
                    .expect("sequence")
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("u64");
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

    fn bodies(batches: &[arrow_array::RecordBatch]) -> Vec<Option<String>> {
        batches
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column_by_name(COLUMN_BODY)
                    .expect("body")
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("utf8");
                (0..column.len())
                    .map(|row| (!column.is_null(row)).then(|| column.value(row).to_owned()))
            })
            .collect()
    }

    #[test]
    fn scan_reads_active_frozen_and_published_rows_once() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = open_store(&directory);
        seal(&store, frame(0, 0, Vec::new()));
        store.publish(&PublishOptions::default()).expect("publish");
        seal(&store, frame(1, 0, Vec::new()));
        store.append(&frame(2, 0, Vec::new())).expect("active");
        let snapshot = store.pin().expect("snapshot");
        assert_eq!(snapshot.published.len(), 1);
        assert_eq!(snapshot.frozen.len(), 1);
        assert!(snapshot.active.is_some());
        assert_eq!(
            sequences(&store.scan(&snapshot, &Scan::default()).expect("scan")),
            [0, 1, 2]
        );
    }

    #[test]
    fn publication_moves_rows_from_memory_to_parquet_exactly_once() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = open_store(&directory);
        seal(&store, frame(0, 0, vec![attribute("a", 1)]));
        seal(&store, frame(1, 1, vec![attribute("b", 2)]));
        let before = store.pin().expect("snapshot");
        assert!(before.published.is_empty());
        assert_eq!(before.frozen.len(), 2);
        let before_rows = store.scan(&before, &Scan::default()).expect("scan memory");
        assert_eq!(sequences(&before_rows), [0, 1]);

        store.publish(&PublishOptions::default()).expect("publish");
        let after = store.pin().expect("snapshot");
        assert_eq!(after.frozen.len(), 1);
        assert_eq!(after.published.len(), 1);
        assert!(after.epoch > before.epoch);
        let after_rows = store.scan(&after, &Scan::default()).expect("scan mixed");
        assert_eq!(sequences(&after_rows), [0, 1]);
        assert_eq!(i64_column(&after_rows, "log_a_i64"), vec![Some(1), None]);
        assert_eq!(i64_column(&after_rows, "log_b_i64"), vec![None, Some(2)]);
        assert_eq!(
            bodies(&after_rows),
            vec![Some("row-0".to_owned()), Some("row-1".to_owned())]
        );

        let restarted = open_store(&directory);
        assert_eq!(restarted.durable_sequence().expect("durable"), Some(1));
        let visible = restarted.pin().expect("recovered");
        assert!(visible.frozen.is_empty());
        assert!(visible.active.is_none());
        assert_eq!(
            sequences(&restarted.scan(&visible, &Scan::default()).expect("scan")),
            [0]
        );
        let error = restarted
            .append(&frame(0, 0, Vec::new()))
            .expect_err("old sequence");
        assert!(matches!(
            error,
            StoreError::Memtable(MemtableError::NonContiguous {
                expected: 1,
                actual: 0
            })
        ));
        restarted
            .append(&frame(1, 1, vec![attribute("b", 2)]))
            .expect("continue");
    }

    #[test]
    fn scan_prunes_hours_and_null_fills_dynamic_columns() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = open_store(&directory);
        seal(&store, frame(0, 0, vec![attribute("a", 1)]));
        store.publish(&PublishOptions::default()).expect("publish");
        seal(&store, frame(1, HOUR, vec![attribute("b", 2)]));
        let snapshot = store.pin().expect("snapshot");
        let pruned = store
            .scan(
                &snapshot,
                &Scan {
                    columns: Some(vec![
                        COLUMN_WAL_SEQUENCE.to_owned(),
                        "log_a_i64".to_owned(),
                        "log_b_i64".to_owned(),
                    ]),
                    from_hour: Some(EventHour::containing(HOUR)),
                    to_hour: None,
                },
            )
            .expect("scan");
        assert_eq!(pruned[0].num_columns(), 3);
        assert_eq!(sequences(&pruned), [1]);
        assert_eq!(i64_column(&pruned, "log_a_i64"), vec![None]);
        assert_eq!(i64_column(&pruned, "log_b_i64"), vec![Some(2)]);
    }

    #[test]
    fn sources_describe_each_visible_generation_once() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = open_store(&directory);

        store.append(&frame(0, 0, Vec::new())).expect("active");
        let active = store.pin().expect("active snapshot");
        let active_sources = store.sources(&active, &Scan::default());
        assert_eq!(active_sources.len(), 1);
        assert!(active_sources[0].files.is_empty());
        assert_eq!(active_sources[0].batches.len(), 1);
        assert_eq!(sequences(&active_sources[0].batches), [0]);

        seal(&store, frame(1, 0, Vec::new()));
        let frozen = store.pin().expect("frozen snapshot");
        let frozen_sources = store.sources(&frozen, &Scan::default());
        assert_eq!(frozen_sources.len(), 1);
        assert!(frozen_sources[0].files.is_empty());
        assert_eq!(sequences(&frozen_sources[0].batches), [0, 1]);

        store.publish(&PublishOptions::default()).expect("publish");
        store
            .append(&frame(2, HOUR, Vec::new()))
            .expect("later active");
        let mixed = store.pin().expect("mixed snapshot");
        let mixed_sources = store.sources(&mixed, &Scan::default());
        assert_eq!(mixed_sources.len(), 2);
        assert!(mixed_sources[0].batches.is_empty());
        assert_eq!(mixed_sources[0].files.len(), 1);
        assert_eq!(
            mixed_sources[0].files[0].path,
            crate::parquet_path(directory.path(), "tenant-a", EventHour::containing(0), 0, 2)
        );
        assert!(mixed_sources[0].files[0].path.is_file());
        let statistics = mixed_sources[0].files[0]
            .statistics
            .as_ref()
            .expect("file statistics");
        assert_eq!(statistics.rows, 2);
        assert_eq!(statistics.wal_min, 0);
        assert_eq!(statistics.wal_max, 1);
        assert!(statistics.ordered);
        assert!(mixed_sources[1].files.is_empty());
        assert_eq!(sequences(&mixed_sources[1].batches), [2]);

        let stray = crate::hour_directory(directory.path(), "tenant-a", EventHour::containing(0))
            .join("stray.parquet");
        fs::write(&stray, b"not a commit").expect("stray");
        let published = store.sources(&mixed, &Scan::default());
        assert_eq!(published[0].files, mixed_sources[0].files);

        let selected = store.sources(
            &mixed,
            &Scan {
                from_hour: Some(EventHour::containing(HOUR)),
                ..Scan::default()
            },
        );
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].hour, EventHour::containing(HOUR));
        assert!(selected[0].files.is_empty());
    }

    #[test]
    fn source_schema_keeps_dynamic_columns_a_generation_lacks() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = open_store(&directory);
        seal(&store, frame(0, 0, vec![attribute("a", 1)]));
        store.publish(&PublishOptions::default()).expect("publish");
        seal(&store, frame(1, 0, vec![attribute("b", 2)]));
        let snapshot = store.pin().expect("snapshot");
        let schema = snapshot.schema().expect("schema");
        assert!(schema.field_with_name("log_a_i64").is_ok());
        assert!(schema.field_with_name("log_b_i64").is_ok());
        assert!(
            snapshot.published[0]
                .schema
                .field_with_name("log_b_i64")
                .is_err()
        );
        assert!(
            snapshot.frozen[0]
                .schema
                .field_with_name("log_a_i64")
                .is_err()
        );
        let sources = store.sources(&snapshot, &Scan::default());
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].files.len(), 1);
        assert_eq!(sources[0].batches.len(), 1);
        assert!(
            sources[0].batches[0]
                .schema()
                .field_with_name("log_a_i64")
                .is_err()
        );
    }

    #[test]
    fn empty_generation_advances_the_durable_sequence() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = open_store(&directory);
        seal(&store, empty_frame(4));
        let commit = store.publish(&PublishOptions::default()).expect("publish");
        let commit = commit.expect("commit");
        assert!(commit.files.is_empty());
        assert_eq!(commit.first_sequence, 4);
        assert_eq!(commit.next_sequence, 5);
        assert_eq!(store.durable_sequence().expect("durable"), Some(5));
        let restarted = open_store(&directory);
        assert_eq!(restarted.durable_sequence().expect("durable"), Some(5));
        assert!(
            restarted
                .scan(&restarted.pin().expect("snapshot"), &Scan::default())
                .expect("scan")
                .is_empty()
        );
    }

    #[test]
    fn replaying_a_durable_range_publishes_it_once() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = open_store(&directory);
        seal(&store, frame(0, 0, vec![attribute("a", 1)]));
        let error = store
            .publish(&PublishOptions {
                fault: Some(PublishFault::Swap),
                ..PublishOptions::default()
            })
            .expect_err("swap");
        assert!(matches!(error, StoreError::Fault(PublishFault::Swap)));
        let during = store.pin().expect("during");
        assert!(during.published.is_empty());
        assert_eq!(during.frozen.len(), 1);
        store.publish(&PublishOptions::default()).expect("retry");
        let after = store.pin().expect("after");
        assert!(after.frozen.is_empty());
        assert_eq!(after.published.len(), 1);
        assert_eq!(
            sequences(&store.scan(&after, &Scan::default()).expect("scan")),
            [0]
        );
        store.publish(&PublishOptions::default()).expect("idle");
        assert_eq!(store.pin().expect("still").published.len(), 1);
    }

    #[test]
    fn publication_faults_recover_to_the_old_or_new_generation() {
        let cases = [
            (Some(ParquetFault::Create), None, false),
            (Some(ParquetFault::SyncFile), None, false),
            (Some(ParquetFault::Rename), None, false),
            (Some(ParquetFault::SyncDir), None, false),
            (None, Some(CommitFault::Create), false),
            (None, Some(CommitFault::SyncFile), false),
            (None, Some(CommitFault::Rename), false),
            (None, Some(CommitFault::SyncDir), true),
        ];
        for (parquet_fault, commit_fault, published) in cases {
            let directory = tempfile::tempdir().expect("tempdir");
            let store = open_store(&directory);
            seal(&store, frame(0, 0, vec![attribute("a", 1)]));
            let error = store
                .publish(&PublishOptions {
                    parquet: ParquetWriteOptions {
                        fault: parquet_fault,
                        ..ParquetWriteOptions::default()
                    },
                    commit: CommitWriteOptions {
                        fault: commit_fault,
                    },
                    fault: None,
                })
                .expect_err("fault");
            assert!(error.to_string().contains("fault"));
            drop(store);
            let recovered = open_store(&directory);
            let snapshot = recovered.pin().expect("snapshot");
            let rows = recovered.scan(&snapshot, &Scan::default()).expect("scan");
            let path = commit_path(directory.path(), "tenant-a", 0, 1);
            if published {
                assert_eq!(recovered.durable_sequence().expect("durable"), Some(1));
                assert!(path.exists());
                assert_eq!(sequences(&rows), [0]);
            } else {
                assert_eq!(recovered.durable_sequence().expect("durable"), None);
                assert!(!path.exists());
                assert!(rows.is_empty());
                assert!(!path.with_extension("commit.tmp").exists());
            }
        }
    }

    #[test]
    fn a_corrupt_latest_commit_reverts_to_the_previous_generation() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = open_store(&directory);
        seal(&store, frame(0, 0, vec![attribute("a", 1)]));
        store.publish(&PublishOptions::default()).expect("first");
        seal(&store, frame(1, 0, vec![attribute("b", 2)]));
        store.publish(&PublishOptions::default()).expect("second");
        let path = commit_path(directory.path(), "tenant-a", 1, 2);
        let mut bytes = fs::read(&path).expect("commit");
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;
        fs::write(&path, bytes).expect("corrupt");
        drop(store);
        let recovered = open_store(&directory);
        assert_eq!(recovered.durable_sequence().expect("durable"), Some(1));
        let rows = recovered
            .scan(&recovered.pin().expect("snapshot"), &Scan::default())
            .expect("scan");
        assert_eq!(sequences(&rows), [0]);
        assert!(!path.exists());
    }

    #[test]
    fn align_projected_reuses_present_arrays_and_nulls_only_requested_columns() {
        let keep = Arc::new(Int64Array::from(vec![Some(1), None]));
        let skipped = Arc::new(Int64Array::from(vec![Some(3), Some(4)]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("keep", DataType::Int64, true),
                Field::new("skip", DataType::Int64, true),
            ])),
            vec![
                Arc::clone(&keep) as ArrayRef,
                Arc::clone(&skipped) as ArrayRef,
            ],
        )
        .expect("batch");
        let schema = Arc::new(Schema::new(vec![
            Field::new("keep", DataType::Int64, true),
            Field::new("missing", DataType::Utf8, true),
            Field::new("skip", DataType::Int64, true),
        ]));
        let keep_array: ArrayRef = Arc::clone(&keep) as ArrayRef;
        let skipped_array: ArrayRef = Arc::clone(&skipped) as ArrayRef;
        let skipped_count = Arc::strong_count(&skipped);
        let aligned =
            super::align_projected(&batch, Arc::clone(&schema), Some(&[0, 1])).expect("project");
        assert_eq!(
            aligned
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["keep", "missing"]
        );
        assert!(Arc::ptr_eq(aligned.column(0), &keep_array));
        assert_eq!(aligned.column(1).null_count(), 2);
        assert_eq!(Arc::strong_count(&skipped), skipped_count);

        let full = super::align_projected(&batch, schema, None).expect("full");
        assert_eq!(full.num_columns(), 3);
        assert!(Arc::ptr_eq(full.column(0), &keep_array));
        assert_eq!(full.column(1).null_count(), 2);
        assert!(Arc::ptr_eq(full.column(2), &skipped_array));

        let empty = super::align_projected(&batch, full.schema(), Some(&[])).expect("empty");
        assert_eq!(empty.num_columns(), 0);
        assert_eq!(empty.num_rows(), batch.num_rows());
    }

    #[test]
    fn a_retired_file_stays_leased_until_the_last_pin_drops() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = open_store(&directory);
        store.append(&frame(0, 0, Vec::new())).expect("append");
        store.append(&frame(1, HOUR, Vec::new())).expect("append");
        store.rotate().expect("rotate");
        let commit = store
            .publish(&PublishOptions::default())
            .expect("publish")
            .expect("commit");
        assert_eq!(commit.files.len(), 2);
        let hidden = commit.files[0].relative_path.clone();
        let kept = commit.files[1].relative_path.clone();
        let first = store.pin().expect("first");
        let second = store.pin().expect("second");
        assert_eq!(
            store.leased_paths().expect("leases").get(&hidden).copied(),
            Some(2)
        );
        store
            .conceal(std::slice::from_ref(&hidden))
            .expect("conceal");
        let later = store.pin().expect("later");
        let later_paths: Vec<_> = store
            .sources(&later, &Scan::default())
            .into_iter()
            .flat_map(|hour| hour.files)
            .map(|file| file.path)
            .collect();
        assert!(later_paths.iter().all(|path| !path.ends_with(&hidden)));
        assert!(
            store
                .sources(&first, &Scan::default())
                .iter()
                .flat_map(|hour| &hour.files)
                .any(|file| file.path.ends_with(&hidden))
        );
        assert!(store.collectible().expect("collectible").is_empty());
        drop(first);
        assert!(store.collectible().expect("collectible").is_empty());
        assert_eq!(
            store.leased_paths().expect("leases").get(&hidden).copied(),
            Some(1)
        );
        drop(second);
        let collectible = store.collectible().expect("collectible");
        assert!(collectible.contains(&hidden));
        assert!(!collectible.contains(&kept));
        drop(later);
        assert!(
            directory
                .path()
                .join("tenants/tenant-a")
                .join(&hidden)
                .exists()
        );
    }
}
