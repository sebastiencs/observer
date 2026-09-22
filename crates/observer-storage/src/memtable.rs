//! Per-tenant active and frozen Arrow generations.
//!
//! The caller decodes a WAL frame, then appends the resulting [`DecodedLogs`]. A generation owns a
//! contiguous exclusive sequence range and one dynamic schema. New attribute fields are admitted
//! until the generation column cap; later fields stay in the JSON fallback columns. A normalized
//! name collision renames every member of that group and rebuilds batch schemas around the same
//! arrays. Freezing aligns every batch to the union schema, inserting typed nulls for columns a
//! batch does not contain. The frame that crosses the row or byte limit stays in the active
//! generation, and that generation then freezes. Age freezes a generation that already holds frames
//! before the next frame starts a new window. An empty generation does not rotate.

use std::{
    collections::{BTreeMap, VecDeque},
    error::Error,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arrow_array::{RecordBatch, new_null_array};
use arrow_schema::{DataType, Schema, SchemaRef};

use crate::{
    DecodedLogs, DynamicError, DynamicField, DynamicIdentity, EventHour, core_logs_schema,
    discover_dynamic_schema, dynamic_identity, logs_batch_schema,
};

/// Upper bounds that rotate or pause one memtable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemtableConfig {
    /// Freeze after an append makes the active generation contain more than this many rows.
    pub max_rows: u64,
    /// Freeze after an append makes the active generation use more than this many Arrow buffer bytes.
    pub max_bytes: u64,
    /// Freeze a generation that already holds frames when it is at least this old.
    pub max_age: Duration,
    /// Maximum number of frozen generations waiting to be released.
    pub max_frozen: usize,
    /// Maximum number of dynamic attribute columns admitted into one generation.
    ///
    /// Fields past this limit are removed from the stored batches and remain in the JSON fallback
    /// columns. Already admitted fields stay when a later frame arrives.
    pub max_dynamic_columns: usize,
}

/// Clock used to decide age rotation.
pub trait Clock: Send + Sync {
    /// Current time in nanoseconds since the Unix epoch.
    fn unix_nanos(&self) -> u64;
}

/// Clock that tests and callers can move explicitly.
#[derive(Clone, Debug)]
pub struct ManualClock {
    now: Arc<AtomicU64>,
}

impl ManualClock {
    /// Clock fixed at `unix_nanos` until [`ManualClock::set`].
    #[must_use]
    pub fn new(unix_nanos: u64) -> Self {
        Self {
            now: Arc::new(AtomicU64::new(unix_nanos)),
        }
    }

    /// Move the clock to `unix_nanos`.
    pub fn set(&self, unix_nanos: u64) {
        self.now.store(unix_nanos, Ordering::Relaxed);
    }
}

impl Clock for ManualClock {
    fn unix_nanos(&self) -> u64 {
        self.now.load(Ordering::Relaxed)
    }
}

/// Wall clock in Unix nanoseconds.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn unix_nanos(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| {
                u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
            })
    }
}

/// Why a memtable operation was rejected.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MemtableError {
    /// `max_frozen` was zero, so a generation could never freeze.
    FrozenLimitZero,
    /// The decoded frame belongs to another tenant.
    TenantMismatch { expected: String, actual: String },
    /// The frame sequence is not the next exclusive sequence.
    NonContiguous { expected: u64, actual: u64 },
    /// The frame sequence has no following exclusive bound.
    SequenceOverflow { sequence: u64 },
    /// [`Memtable::resume`] was called after the memtable had already accepted a sequence.
    NotResumable,
    /// Freezing this append or rotation would exceed `max_frozen`. Nothing was stored.
    FrozenLimit,
    /// A batch does not use the core logs schema, or a dynamic column has no identity metadata.
    IncompatibleBatch(String),
    /// Collision suffixes could not be assigned for the generation schema.
    Dynamic(DynamicError),
}

impl fmt::Display for MemtableError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrozenLimitZero => {
                formatter.write_str("frozen generation limit must be non-zero")
            }
            Self::TenantMismatch { expected, actual } => {
                write!(
                    formatter,
                    "memtable tenant is {expected}, frame tenant is {actual}"
                )
            }
            Self::NonContiguous { expected, actual } => {
                write!(
                    formatter,
                    "WAL sequence {actual} does not continue from {expected}"
                )
            }
            Self::SequenceOverflow { sequence } => {
                write!(
                    formatter,
                    "WAL sequence {sequence} has no exclusive successor"
                )
            }
            Self::NotResumable => {
                formatter.write_str("memtable already has a sequence and cannot resume")
            }
            Self::FrozenLimit => formatter.write_str("frozen generation limit has been reached"),
            Self::IncompatibleBatch(detail) => {
                write!(formatter, "incompatible log batch: {detail}")
            }
            Self::Dynamic(error) => write!(formatter, "dynamic log columns: {error}"),
        }
    }
}

impl From<DynamicError> for MemtableError {
    fn from(error: DynamicError) -> Self {
        Self::Dynamic(error)
    }
}

impl Error for MemtableError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Dynamic(error) => Some(error),
            Self::FrozenLimitZero
            | Self::TenantMismatch { .. }
            | Self::NonContiguous { .. }
            | Self::SequenceOverflow { .. }
            | Self::NotResumable
            | Self::FrozenLimit
            | Self::IncompatibleBatch(_) => None,
        }
    }
}

/// Result of storing one decoded frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Appended {
    /// Store epoch after this append.
    pub epoch: u64,
    /// Generation that received the frame.
    pub generation_id: u64,
    /// The receiving generation was frozen because the row or byte limit was crossed.
    pub sealed: bool,
}

/// Point-in-time view of every generation that still owns rows or sequence progress.
#[derive(Clone, Debug, PartialEq)]
pub struct Snapshot {
    /// Epoch at which the view was taken.
    pub epoch: u64,
    /// Frozen generations, oldest first.
    pub frozen: Vec<Generation>,
    /// Active generation after it has accepted at least one frame.
    pub active: Option<Generation>,
}

/// One active or frozen generation.
#[derive(Clone, Debug, PartialEq)]
pub struct Generation {
    /// Identifier assigned when the generation was opened.
    pub id: u64,
    /// First WAL sequence stored in this generation.
    pub first_sequence: u64,
    /// Exclusive WAL sequence following the last stored frame.
    pub next_sequence: u64,
    /// Rows currently referenced by this generation.
    pub rows: u64,
    /// Arrow buffer bytes currently referenced by this generation.
    pub bytes: u64,
    /// Clock time when this generation accepted its first frame.
    pub opened_at_unix_nanos: u64,
    /// Admitted dynamic columns in physical-name order.
    pub dynamic_fields: Vec<DynamicField>,
    /// Core columns plus [`Generation::dynamic_fields`].
    ///
    /// While the generation is active, an individual batch can omit a dynamic column. Freezing
    /// aligns every batch to this schema.
    pub schema: SchemaRef,
    /// BLAKE3 of [`Generation::schema`]. The same admitted fields produce the same fingerprint.
    pub fingerprint: String,
    /// Hour batches in UTC hour order. Each batch list preserves append order.
    pub partitions: Vec<GenerationPartition>,
}

/// Batches that share one derived UTC event hour.
#[derive(Clone, Debug, PartialEq)]
pub struct GenerationPartition {
    pub hour: EventHour,
    pub batches: Vec<Arc<RecordBatch>>,
}

/// In-memory active generation plus a bounded frozen queue for one tenant.
pub struct Memtable {
    tenant_id: String,
    config: MemtableConfig,
    clock: Arc<dyn Clock>,
    next_id: u64,
    next_sequence: Option<u64>,
    epoch: u64,
    active: ActiveGeneration,
    frozen: VecDeque<ActiveGeneration>,
}

impl Memtable {
    /// Open an empty memtable. The first appended sequence establishes the range start.
    pub fn new(
        tenant_id: impl Into<String>,
        config: MemtableConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, MemtableError> {
        if config.max_frozen == 0 {
            return Err(MemtableError::FrozenLimitZero);
        }
        let mut table = Self {
            tenant_id: tenant_id.into(),
            config,
            clock,
            next_id: 1,
            next_sequence: None,
            epoch: 0,
            active: ActiveGeneration::empty(0),
            frozen: VecDeque::new(),
        };
        table.active = table.open_generation();
        Ok(table)
    }

    /// Store one decoded frame.
    ///
    /// On [`MemtableError::FrozenLimit`], [`MemtableError::NonContiguous`],
    /// [`MemtableError::TenantMismatch`], or [`MemtableError::SequenceOverflow`], the memtable is
    /// unchanged and the same frame can be retried when the rejection condition is gone.
    pub fn append(&mut self, logs: &DecodedLogs) -> Result<Appended, MemtableError> {
        if logs.tenant_id != self.tenant_id {
            return Err(MemtableError::TenantMismatch {
                expected: self.tenant_id.clone(),
                actual: logs.tenant_id.clone(),
            });
        }
        let next_sequence =
            logs.sequence
                .checked_add(1)
                .ok_or(MemtableError::SequenceOverflow {
                    sequence: logs.sequence,
                })?;
        if let Some(expected) = self.next_sequence
            && logs.sequence != expected
        {
            return Err(MemtableError::NonContiguous {
                expected,
                actual: logs.sequence,
            });
        }

        let now = self.clock.unix_nanos();
        let age_freeze = self.active.has_frames() && self.age_reached(now);
        let state = if age_freeze {
            SchemaState::initial()
        } else {
            self.active.schema_state()
        };
        let projected = state.project(logs, self.config.max_dynamic_columns)?;
        let frame_rows = projected.rows();
        let frame_bytes = projected.bytes();
        let rows_before = if age_freeze { 0 } else { self.active.rows };
        let bytes_before = if age_freeze { 0 } else { self.active.bytes };
        let size_freeze = rows_before.saturating_add(frame_rows) > self.config.max_rows
            || bytes_before.saturating_add(frame_bytes) > self.config.max_bytes;
        let slots_needed = usize::from(age_freeze) + usize::from(size_freeze);
        if self.frozen.len() + slots_needed > self.config.max_frozen {
            return Err(MemtableError::FrozenLimit);
        }

        if age_freeze {
            self.seal_active();
        }
        if !self.active.has_frames() {
            self.active.opened_at_unix_nanos = now;
            self.active.first_sequence = logs.sequence;
        }
        self.active.next_sequence = next_sequence;
        self.active.rows = self.active.rows.saturating_add(frame_rows);
        self.active.bytes = self.active.bytes.saturating_add(frame_bytes);
        self.active.store_projected(projected);
        self.next_sequence = Some(next_sequence);
        let generation_id = self.active.id;
        if size_freeze {
            self.seal_active();
        }
        self.epoch = self.epoch.saturating_add(1);
        Ok(Appended {
            epoch: self.epoch,
            generation_id,
            sealed: size_freeze,
        })
    }

    /// Freeze the active generation even when it is below the row, byte, and age limits.
    ///
    /// An active generation with no frames stays open. A full frozen queue leaves the active
    /// generation unchanged.
    pub fn rotate(&mut self) -> Result<Option<u64>, MemtableError> {
        if !self.active.has_frames() {
            return Ok(None);
        }
        if self.frozen.len() >= self.config.max_frozen {
            return Err(MemtableError::FrozenLimit);
        }
        let generation_id = self.active.id;
        self.seal_active();
        self.epoch = self.epoch.saturating_add(1);
        Ok(Some(generation_id))
    }

    /// Require the next appended sequence to be `next_sequence`.
    ///
    /// Used after recovery so new frames continue past the published high-water mark. The memtable
    /// must not already hold a sequence.
    ///
    /// # Errors
    ///
    /// Returns [`MemtableError::NotResumable`] when a frame was already accepted or [`Self::resume`]
    /// was already called.
    pub fn resume(&mut self, next_sequence: u64) -> Result<(), MemtableError> {
        if self.next_sequence.is_some() || self.active.has_frames() || !self.frozen.is_empty() {
            return Err(MemtableError::NotResumable);
        }
        self.next_sequence = Some(next_sequence);
        Ok(())
    }

    /// Oldest frozen generation, still owned by the memtable.
    #[must_use]
    pub fn oldest_frozen(&self) -> Option<Generation> {
        self.frozen.front().map(ActiveGeneration::view)
    }

    /// Remove the oldest frozen generation after a later phase has finished with it.
    #[must_use]
    pub fn release_oldest_frozen(&mut self) -> Option<Generation> {
        let generation = self.frozen.pop_front()?;
        self.epoch = self.epoch.saturating_add(1);
        Some(generation.view())
    }

    /// Clone batch handles from the active and frozen generations at the current epoch.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            epoch: self.epoch,
            frozen: self.frozen.iter().map(ActiveGeneration::view).collect(),
            active: self.active.has_frames().then(|| self.active.view()),
        }
    }

    fn age_reached(&self, now: u64) -> bool {
        let max_age_nanos = u64::try_from(self.config.max_age.as_nanos()).unwrap_or(u64::MAX);
        now.saturating_sub(self.active.opened_at_unix_nanos) >= max_age_nanos
    }

    fn seal_active(&mut self) {
        debug_assert!(self.active.has_frames());
        self.active.align_batches();
        let replacement = self.open_generation();
        let sealed = std::mem::replace(&mut self.active, replacement);
        self.frozen.push_back(sealed);
    }

    fn open_generation(&mut self) -> ActiveGeneration {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        ActiveGeneration::empty(id)
    }
}

struct ActiveGeneration {
    id: u64,
    first_sequence: u64,
    next_sequence: u64,
    rows: u64,
    bytes: u64,
    opened_at_unix_nanos: u64,
    admitted: Vec<DynamicIdentity>,
    fields: Vec<DynamicField>,
    schema: SchemaRef,
    fingerprint: String,
    partitions: BTreeMap<EventHour, Vec<Arc<RecordBatch>>>,
}

impl ActiveGeneration {
    fn empty(id: u64) -> Self {
        let state = SchemaState::initial();
        Self {
            id,
            first_sequence: 0,
            next_sequence: 0,
            rows: 0,
            bytes: 0,
            opened_at_unix_nanos: 0,
            admitted: state.admitted,
            fields: state.fields,
            schema: state.schema,
            fingerprint: state.fingerprint,
            partitions: BTreeMap::new(),
        }
    }

    fn schema_state(&self) -> SchemaState {
        SchemaState {
            admitted: self.admitted.clone(),
            fields: self.fields.clone(),
            schema: Arc::clone(&self.schema),
            fingerprint: self.fingerprint.clone(),
        }
    }

    fn store_projected(&mut self, projected: ProjectedFrame) {
        let renamed = names_changed(&self.fields, &projected.fields);
        self.admitted = projected.admitted;
        self.fields = projected.fields;
        self.schema = projected.schema;
        self.fingerprint = projected.fingerprint;
        if renamed {
            let fields = self.fields.clone();
            for batches in self.partitions.values_mut() {
                for batch in batches.iter_mut() {
                    *batch = Arc::new(rename_batch(batch, &fields));
                }
            }
        }
        for (hour, batch) in projected.partitions {
            self.partitions
                .entry(hour)
                .or_default()
                .push(Arc::new(batch));
        }
    }

    fn align_batches(&mut self) {
        let schema = Arc::clone(&self.schema);
        let mut bytes = 0u64;
        for batches in self.partitions.values_mut() {
            for batch in batches.iter_mut() {
                if batch.schema().as_ref() != schema.as_ref() {
                    let aligned = align_batch(batch, Arc::clone(&schema))
                        .expect("generation batches align to the union schema");
                    *batch = Arc::new(aligned);
                }
                bytes += u64::try_from(batch.get_array_memory_size()).expect("batch bytes");
            }
        }
        self.bytes = bytes;
    }

    fn has_frames(&self) -> bool {
        self.next_sequence != self.first_sequence
    }

    fn view(&self) -> Generation {
        Generation {
            id: self.id,
            first_sequence: self.first_sequence,
            next_sequence: self.next_sequence,
            rows: self.rows,
            bytes: self.bytes,
            opened_at_unix_nanos: self.opened_at_unix_nanos,
            dynamic_fields: self.fields.clone(),
            schema: Arc::clone(&self.schema),
            fingerprint: self.fingerprint.clone(),
            partitions: self
                .partitions
                .iter()
                .map(|(hour, batches)| GenerationPartition {
                    hour: *hour,
                    batches: batches.clone(),
                })
                .collect(),
        }
    }
}

impl fmt::Debug for Memtable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Memtable")
            .field("tenant_id", &self.tenant_id)
            .field("epoch", &self.epoch)
            .field("next_sequence", &self.next_sequence)
            .finish_non_exhaustive()
    }
}

struct SchemaState {
    admitted: Vec<DynamicIdentity>,
    fields: Vec<DynamicField>,
    schema: SchemaRef,
    fingerprint: String,
}

impl SchemaState {
    fn initial() -> Self {
        let schema = core_logs_schema();
        let fingerprint = schema_fingerprint(&schema);
        Self {
            admitted: Vec::new(),
            fields: Vec::new(),
            schema,
            fingerprint,
        }
    }

    fn project(
        &self,
        logs: &DecodedLogs,
        max_columns: usize,
    ) -> Result<ProjectedFrame, MemtableError> {
        let mut admitted = self.admitted.clone();
        let mut overflow = Vec::new();
        for partition in &logs.partitions {
            if !core_columns_match(&partition.batch) {
                return Err(MemtableError::IncompatibleBatch(
                    "batch does not start with the core logs schema".to_owned(),
                ));
            }
            for field in partition
                .batch
                .schema()
                .fields()
                .iter()
                .skip(core_logs_schema().fields().len())
            {
                let Some(identity) = dynamic_identity(field) else {
                    return Err(MemtableError::IncompatibleBatch(format!(
                        "dynamic column {} has no attribute identity",
                        field.name()
                    )));
                };
                if admitted.iter().any(|admitted| admitted == &identity)
                    || overflow.iter().any(|skipped| skipped == &identity)
                {
                    continue;
                }
                if admitted.len() >= max_columns {
                    overflow.push(identity);
                } else {
                    admitted.push(identity);
                }
            }
        }
        let named = discover_dynamic_schema(admitted.clone(), admitted.len())?;
        let schema = logs_batch_schema(
            &named
                .fields
                .iter()
                .map(DynamicField::arrow_field)
                .collect::<Vec<_>>(),
        );
        let partitions = logs
            .partitions
            .iter()
            .map(|partition| {
                Ok((
                    partition.hour,
                    project_batch(&partition.batch, &named.fields)?,
                ))
            })
            .collect::<Result<Vec<_>, MemtableError>>()?;
        Ok(ProjectedFrame {
            admitted,
            fields: named.fields,
            fingerprint: schema_fingerprint(&schema),
            schema,
            partitions,
        })
    }
}

struct ProjectedFrame {
    admitted: Vec<DynamicIdentity>,
    fields: Vec<DynamicField>,
    schema: SchemaRef,
    fingerprint: String,
    partitions: Vec<(EventHour, RecordBatch)>,
}

impl ProjectedFrame {
    fn rows(&self) -> u64 {
        self.partitions
            .iter()
            .map(|(_, batch)| u64::try_from(batch.num_rows()).expect("row count"))
            .sum()
    }

    fn bytes(&self) -> u64 {
        self.partitions
            .iter()
            .map(|(_, batch)| u64::try_from(batch.get_array_memory_size()).expect("batch bytes"))
            .sum()
    }
}

/// Reorder `batch` to `schema` and fill missing nullable columns with typed nulls.
///
/// # Errors
///
/// Returns [`MemtableError::IncompatibleBatch`] when a required column is missing or a present
/// column has a different Arrow type.
pub fn align_batch(batch: &RecordBatch, schema: SchemaRef) -> Result<RecordBatch, MemtableError> {
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        if let Some(column) = batch.column_by_name(field.name()) {
            if column.data_type() != field.data_type() {
                return Err(MemtableError::IncompatibleBatch(format!(
                    "column {} has type {:?}, schema expects {:?}",
                    field.name(),
                    column.data_type(),
                    field.data_type()
                )));
            }
            columns.push(Arc::clone(column));
        } else if field.is_nullable() {
            columns.push(new_null_array(field.data_type(), batch.num_rows()));
        } else {
            return Err(MemtableError::IncompatibleBatch(format!(
                "missing required column {}",
                field.name()
            )));
        }
    }
    RecordBatch::try_new(schema, columns)
        .map_err(|error| MemtableError::IncompatibleBatch(error.to_string()))
}

fn project_batch(
    batch: &RecordBatch,
    fields: &[DynamicField],
) -> Result<RecordBatch, MemtableError> {
    let mut columns = Vec::new();
    let mut projected = Vec::new();
    for (index, field) in batch.schema().fields().iter().enumerate() {
        if let Some(identity) = dynamic_identity(field) {
            let Some(named) = fields
                .iter()
                .find(|candidate| candidate.identity == identity)
            else {
                continue;
            };
            projected.push(named.arrow_field());
        } else {
            projected.push((**field).clone());
        }
        columns.push(Arc::clone(batch.column(index)));
    }
    if projected_fields_match(batch, &projected) {
        return Ok(batch.clone());
    }
    RecordBatch::try_new(Arc::new(Schema::new(projected)), columns)
        .map_err(|error| MemtableError::IncompatibleBatch(error.to_string()))
}

fn projected_fields_match(batch: &RecordBatch, projected: &[arrow_schema::Field]) -> bool {
    batch.schema().fields().len() == projected.len()
        && batch
            .schema()
            .fields()
            .iter()
            .zip(projected)
            .all(|(current, projected)| current.as_ref() == projected)
}

fn rename_batch(batch: &RecordBatch, fields: &[DynamicField]) -> RecordBatch {
    project_batch(batch, fields).expect("renamed batch keeps its admitted columns")
}

fn names_changed(current: &[DynamicField], updated: &[DynamicField]) -> bool {
    current.iter().any(|field| {
        updated
            .iter()
            .find(|candidate| candidate.identity == field.identity)
            .is_none_or(|candidate| candidate.physical_name != field.physical_name)
    })
}

fn core_columns_match(batch: &RecordBatch) -> bool {
    let core = core_logs_schema();
    batch.schema().fields().len() >= core.fields().len()
        && batch
            .schema()
            .fields()
            .iter()
            .zip(core.fields())
            .all(|(actual, expected)| actual.as_ref() == expected.as_ref())
}

fn schema_fingerprint(schema: &Schema) -> String {
    let mut hasher = blake3::Hasher::new();
    let count = u32::try_from(schema.fields().len()).expect("field count");
    hasher.update(&count.to_be_bytes());
    for field in schema.fields() {
        write_fingerprint_str(&mut hasher, field.name());
        write_fingerprint_type(&mut hasher, field.data_type());
        hasher.update(&[u8::from(field.is_nullable())]);
        let mut metadata: Vec<_> = field.metadata().iter().collect();
        metadata.sort_by(|left, right| left.0.cmp(right.0));
        let meta_count = u32::try_from(metadata.len()).expect("metadata count");
        hasher.update(&meta_count.to_be_bytes());
        for (key, value) in metadata {
            write_fingerprint_str(&mut hasher, key);
            write_fingerprint_str(&mut hasher, value);
        }
    }
    hasher.finalize().to_hex().to_string()
}

fn write_fingerprint_str(hasher: &mut blake3::Hasher, value: &str) {
    let length = u32::try_from(value.len()).expect("fingerprint string length");
    hasher.update(&length.to_be_bytes());
    hasher.update(value.as_bytes());
}

fn write_fingerprint_type(hasher: &mut blake3::Hasher, data_type: &DataType) {
    let tag = match data_type {
        DataType::Boolean => [1],
        DataType::Int32 => [2],
        DataType::Int64 => [3],
        DataType::UInt16 => [4],
        DataType::UInt32 => [5],
        DataType::UInt64 => [6],
        DataType::Float64 => [7],
        DataType::Binary => [8],
        DataType::Utf8 => [9],
        DataType::FixedSizeBinary(_) => [10],
        _ => [11],
    };
    hasher.update(&tag);
    if let DataType::FixedSizeBinary(length) = data_type {
        hasher.update(&length.to_be_bytes());
    } else if !matches!(
        data_type,
        DataType::Boolean
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float64
            | DataType::Binary
            | DataType::Utf8
            | DataType::FixedSizeBinary(_)
    ) {
        write_fingerprint_str(hasher, &data_type.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Appended, Clock, Generation, ManualClock, Memtable, MemtableConfig, MemtableError,
        SystemClock,
    };
    use crate::{
        COLUMN_LOG_ATTRIBUTES, DynamicIdentity, DynamicLimits, EventHour,
        canonical_attributes_json, decode_logs_frame, discover_dynamic_schema, dynamic_identity,
    };
    use arrow_array::{Array, Int64Array, StringArray};
    use bytes::Bytes;
    use observer_protocol::otlp::{
        AnyValue, ExportLogsServiceRequest, KeyValue, KeyValueList, LogRecord, ResourceLogs,
        ScopeLogs, any_value,
    };
    use observer_wal::{Frame, FrameSignal};
    use proptest::prelude::*;
    use prost::Message;
    use std::{sync::Arc, time::Duration};

    const HOUR: u64 = 3_600_000_000_000;

    fn config(max_rows: u64, max_bytes: u64, max_frozen: usize) -> MemtableConfig {
        MemtableConfig {
            max_rows,
            max_bytes,
            max_age: Duration::from_secs(60),
            max_frozen,
            max_dynamic_columns: 32,
        }
    }

    fn open(config: MemtableConfig, clock: &ManualClock) -> Memtable {
        Memtable::new("tenant-a", config, Arc::new(clock.clone())).expect("memtable")
    }

    fn logs(sequence: u64, times: &[u64]) -> crate::DecodedLogs {
        let log_records = times
            .iter()
            .map(|time| LogRecord {
                time_unix_nano: *time,
                body: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(format!("row-{time}"))),
                }),
                ..Default::default()
            })
            .collect();
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records,
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

    fn append(table: &mut Memtable, sequence: u64, time: u64) -> Appended {
        table.append(&logs(sequence, &[time])).expect("append")
    }

    fn bodies(generation: &Generation) -> Vec<String> {
        generation
            .partitions
            .iter()
            .flat_map(|partition| partition.batches.iter())
            .flat_map(|batch| {
                let column = batch
                    .column_by_name(crate::COLUMN_BODY)
                    .expect("body")
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("utf8 body");
                (0..column.len())
                    .map(|row| column.value(row).to_owned())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[test]
    fn sequence_range_includes_empty_frames_and_rejects_gaps() {
        let clock = ManualClock::new(0);
        let mut table = open(config(100, u64::MAX, 2), &clock);
        let empty = logs(4, &[]);
        assert_eq!(empty.record_count, 0);
        let appended = table.append(&empty).expect("empty");
        assert!(!appended.sealed);
        let snapshot = table.snapshot();
        let active = snapshot.active.as_ref().expect("active");
        assert_eq!(active.first_sequence, 4);
        assert_eq!(active.next_sequence, 5);
        assert_eq!(active.rows, 0);
        assert!(active.partitions.is_empty());

        let error = table.append(&logs(7, &[1])).expect_err("gap");
        assert_eq!(
            error,
            MemtableError::NonContiguous {
                expected: 5,
                actual: 7
            }
        );
        assert_eq!(table.snapshot(), snapshot);

        append(&mut table, 5, 1);
        let active = table.snapshot().active.expect("active");
        assert_eq!((active.first_sequence, active.next_sequence), (4, 6));
        assert_eq!(active.rows, 1);
    }

    #[test]
    fn crossing_frame_stays_in_the_generation_that_freezes() {
        let clock = ManualClock::new(0);
        let mut table = open(config(1, u64::MAX, 2), &clock);
        let first = append(&mut table, 0, 1);
        assert!(!first.sealed);
        assert!(table.snapshot().frozen.is_empty());

        let second = append(&mut table, 1, 2);
        assert!(second.sealed);
        assert_eq!(second.generation_id, first.generation_id);
        let snapshot = table.snapshot();
        assert!(snapshot.active.is_none());
        assert_eq!(snapshot.frozen.len(), 1);
        let frozen = &snapshot.frozen[0];
        assert_eq!((frozen.first_sequence, frozen.next_sequence), (0, 2));
        assert_eq!(frozen.rows, 2);
        assert_eq!(bodies(frozen), ["row-1", "row-2"]);

        let third = append(&mut table, 2, 3);
        assert!(!third.sealed);
        assert_ne!(third.generation_id, frozen.id);
        let snapshot = table.snapshot();
        assert_eq!(snapshot.active.expect("active").first_sequence, 2);
        assert_eq!(snapshot.frozen[0].next_sequence, 2);
    }

    #[test]
    fn one_oversized_frame_is_accepted_and_then_frozen() {
        let clock = ManualClock::new(0);
        let mut table = open(config(1, u64::MAX, 1), &clock);
        let decoded = logs(0, &[1, 2, 3]);
        let appended = table.append(&decoded).expect("oversized");
        assert!(appended.sealed);
        let snapshot = table.snapshot();
        assert!(snapshot.active.is_none());
        assert_eq!(snapshot.frozen[0].rows, 3);
        assert_eq!(snapshot.frozen[0].next_sequence, 1);
    }

    #[test]
    fn byte_limit_uses_arrow_buffer_size() {
        let clock = ManualClock::new(0);
        let decoded = logs(0, &[1]);
        let bytes = decoded
            .partitions
            .iter()
            .map(|partition| u64::try_from(partition.batch.get_array_memory_size()).expect("bytes"))
            .sum::<u64>();
        let mut table = open(config(u64::MAX, bytes - 1, 1), &clock);
        let appended = table.append(&decoded).expect("append");
        assert!(appended.sealed);
        assert_eq!(table.snapshot().frozen[0].bytes, bytes);
    }

    #[test]
    fn multi_hour_frame_stays_in_one_generation() {
        let clock = ManualClock::new(0);
        let mut table = open(config(10, u64::MAX, 1), &clock);
        table.append(&logs(0, &[HOUR, 0, HOUR])).expect("append");
        let active = table.snapshot().active.expect("active");
        assert_eq!(active.rows, 3);
        assert_eq!(active.partitions.len(), 2);
        assert_eq!(active.partitions[0].hour, EventHour::containing(0));
        assert_eq!(active.partitions[1].hour, EventHour::containing(HOUR));
        assert_eq!(
            bodies(&active),
            vec![
                "row-0".to_owned(),
                format!("row-{HOUR}"),
                format!("row-{HOUR}")
            ]
        );
    }

    #[test]
    fn same_hour_batches_preserve_append_order() {
        let clock = ManualClock::new(0);
        let mut table = open(config(10, u64::MAX, 1), &clock);
        append(&mut table, 0, 5);
        append(&mut table, 1, 6);
        let active = table.snapshot().active.expect("active");
        assert_eq!(active.partitions.len(), 1);
        assert_eq!(active.partitions[0].batches.len(), 2);
        assert_eq!(bodies(&active), ["row-5", "row-6"]);
    }

    #[test]
    fn age_freezes_before_the_next_frame() {
        let clock = ManualClock::new(1_000);
        let mut config = config(100, u64::MAX, 2);
        config.max_age = Duration::from_nanos(10);
        let mut table = open(config, &clock);
        append(&mut table, 0, 1);
        clock.set(1_009);
        append(&mut table, 1, 2);
        let still_one = table.snapshot();
        assert_eq!(still_one.frozen.len(), 0);
        assert_eq!(still_one.active.expect("active").rows, 2);

        clock.set(1_010);
        let appended = append(&mut table, 2, 3);
        assert!(!appended.sealed);
        let snapshot = table.snapshot();
        assert_eq!(snapshot.frozen.len(), 1);
        assert_eq!(snapshot.frozen[0].next_sequence, 2);
        assert_eq!(snapshot.frozen[0].rows, 2);
        let active = snapshot.active.expect("active");
        assert_eq!(active.id, appended.generation_id);
        assert_eq!((active.first_sequence, active.next_sequence), (2, 3));
        assert_eq!(active.opened_at_unix_nanos, 1_010);
        assert_eq!(active.rows, 1);
    }

    #[test]
    fn empty_generation_does_not_rotate_for_age() {
        let clock = ManualClock::new(0);
        let mut config = config(100, u64::MAX, 1);
        config.max_age = Duration::from_nanos(1);
        let mut table = open(config, &clock);
        clock.set(50);
        let appended = append(&mut table, 0, 1);
        assert!(!appended.sealed);
        let snapshot = table.snapshot();
        assert!(snapshot.frozen.is_empty());
        assert_eq!(snapshot.active.expect("active").opened_at_unix_nanos, 50);
    }

    #[test]
    fn frozen_limit_rejects_without_storing_and_release_makes_room() {
        let clock = ManualClock::new(0);
        let mut table = open(config(1, u64::MAX, 1), &clock);
        append(&mut table, 0, 1);
        append(&mut table, 1, 2);
        append(&mut table, 2, 3);
        let before = table.snapshot();
        assert_eq!(before.frozen.len(), 1);
        assert_eq!(before.active.as_ref().expect("active").rows, 1);

        let rejected = logs(3, &[4]);
        assert_eq!(
            table.append(&rejected).expect_err("full"),
            MemtableError::FrozenLimit
        );
        assert_eq!(table.snapshot(), before);

        let released = table.release_oldest_frozen().expect("frozen");
        assert_eq!((released.first_sequence, released.next_sequence), (0, 2));
        let appended = table.append(&rejected).expect("retry");
        assert!(appended.sealed);
        let snapshot = table.snapshot();
        assert!(snapshot.active.is_none());
        assert_eq!(snapshot.frozen.len(), 1);
        assert_eq!(snapshot.frozen[0].first_sequence, 2);
        assert_eq!(snapshot.frozen[0].rows, 2);
        assert!(table.release_oldest_frozen().is_some());
        assert!(table.release_oldest_frozen().is_none());
    }

    #[test]
    fn age_and_size_rotation_need_two_frozen_slots() {
        let clock = ManualClock::new(0);
        let mut config = config(1, u64::MAX, 1);
        config.max_age = Duration::from_nanos(5);
        let mut table = open(config, &clock);
        append(&mut table, 0, 1);
        clock.set(5);
        let before = table.snapshot();
        let rejected = table
            .append(&logs(1, &[2, 3]))
            .expect_err("age and size need two slots");
        assert_eq!(rejected, MemtableError::FrozenLimit);
        assert_eq!(table.snapshot(), before);

        clock.set(0);
        config.max_frozen = 2;
        let mut table = open(config, &clock);
        append(&mut table, 0, 1);
        clock.set(5);
        let appended = table
            .append(&logs(1, &[2, 3]))
            .expect("two slots available");
        assert!(appended.sealed);
        let snapshot = table.snapshot();
        assert!(snapshot.active.is_none());
        assert_eq!(snapshot.frozen.len(), 2);
        assert_eq!(snapshot.frozen[0].rows, 1);
        assert_eq!(snapshot.frozen[0].next_sequence, 1);
        assert_eq!(snapshot.frozen[1].rows, 2);
        assert_eq!(snapshot.frozen[1].id, appended.generation_id);
    }

    #[test]
    fn snapshot_keeps_the_batches_it_observed() {
        let clock = ManualClock::new(0);
        let mut table = open(config(10, u64::MAX, 2), &clock);
        append(&mut table, 0, 1);
        let first = table.snapshot();
        let first_active = first.active.as_ref().expect("active");
        let first_batch = Arc::clone(&first_active.partitions[0].batches[0]);
        append(&mut table, 1, 2);
        let second = table.snapshot();
        assert!(second.epoch > first.epoch);
        assert_eq!(first_active.partitions[0].batches.len(), 1);
        assert!(Arc::ptr_eq(
            &first_batch,
            &first_active.partitions[0].batches[0]
        ));
        let second_active = second.active.expect("active");
        assert_eq!(second_active.partitions[0].batches.len(), 2);
        assert!(Arc::ptr_eq(
            &first_batch,
            &second_active.partitions[0].batches[0]
        ));
    }

    #[test]
    fn shutdown_rotation_seals_the_active_generation_once() {
        let clock = ManualClock::new(0);
        let mut table = open(config(100, u64::MAX, 1), &clock);
        assert_eq!(table.rotate().expect("empty"), None);
        append(&mut table, 0, 1);
        let sealed = table.rotate().expect("rotate").expect("sealed");
        let snapshot = table.snapshot();
        assert!(snapshot.active.is_none());
        assert_eq!(snapshot.frozen[0].id, sealed);
        assert_eq!(snapshot.frozen[0].rows, 1);
        assert_eq!(table.rotate().expect("already empty"), None);
        assert_eq!(table.snapshot().epoch, snapshot.epoch);

        append(&mut table, 1, 2);
        assert_eq!(
            table.rotate().expect_err("full"),
            MemtableError::FrozenLimit
        );
        assert!(table.snapshot().active.is_some());
    }

    #[test]
    fn tenant_mismatch_and_zero_frozen_limit_are_rejected() {
        let clock = ManualClock::new(0);
        let error = Memtable::new("tenant-a", config(1, 1, 0), Arc::new(clock.clone()))
            .expect_err("config");
        assert_eq!(error, MemtableError::FrozenLimitZero);

        let mut table = open(config(10, u64::MAX, 1), &clock);
        let mut decoded = logs(0, &[1]);
        decoded.tenant_id = "tenant-b".to_owned();
        assert_eq!(
            table.append(&decoded).expect_err("tenant"),
            MemtableError::TenantMismatch {
                expected: "tenant-a".to_owned(),
                actual: "tenant-b".to_owned(),
            }
        );
        assert!(table.snapshot().active.is_none());
        let before = table.snapshot();
        assert_eq!(
            table.append(&logs(u64::MAX, &[1])).expect_err("overflow"),
            MemtableError::SequenceOverflow { sequence: u64::MAX }
        );
        assert_eq!(table.snapshot(), before);
    }

    #[test]
    fn system_clock_reports_unix_time() {
        assert!(SystemClock.unix_nanos() > 1_000_000_000_000_000_000);
    }

    #[test]
    fn schema_grows_across_frames_and_freeze_fills_missing_columns() {
        let clock = ManualClock::new(0);
        let mut table = open(config(10, u64::MAX, 1), &clock);
        table
            .append(&logs_with(0, 1, vec![int_attribute("a", 1)]))
            .expect("first");
        let before = table.snapshot();
        let before_active = before.active.as_ref().expect("active");
        let before_batch = Arc::clone(&before_active.partitions[0].batches[0]);
        let before_fingerprint = before_active.fingerprint.clone();
        table
            .append(&logs_with(1, 1, vec![int_attribute("b", 2)]))
            .expect("second");
        let active = table.snapshot().active.expect("active");
        assert_ne!(active.fingerprint, before_fingerprint);
        assert_eq!(active.dynamic_fields.len(), 2);
        assert!(before_batch.column_by_name("log_b_i64").is_none());
        assert!(Arc::ptr_eq(&before_batch, &active.partitions[0].batches[0]));
        assert!(active.schema.field_with_name("log_b_i64").is_ok());
        assert_eq!(
            optional_i64(&active.partitions[0].batches[1], "log_b_i64", 0),
            Some(2)
        );

        let fingerprint = active.fingerprint.clone();
        table.rotate().expect("rotate").expect("sealed");
        let frozen = &table.snapshot().frozen[0];
        assert_eq!(frozen.fingerprint, fingerprint);
        assert_eq!(frozen.partitions[0].batches.len(), 2);
        for batch in &frozen.partitions[0].batches {
            assert_eq!(batch.schema().as_ref(), frozen.schema.as_ref());
        }
        assert!(
            frozen.partitions[0].batches[0]
                .column_by_name("log_b_i64")
                .expect("b")
                .is_null(0)
        );
        assert_eq!(
            optional_i64(&frozen.partitions[0].batches[0], "log_a_i64", 0),
            Some(1)
        );
        assert_eq!(
            optional_i64(&frozen.partitions[0].batches[1], "log_b_i64", 0),
            Some(2)
        );
        assert!(
            frozen.partitions[0].batches[1]
                .column_by_name("log_a_i64")
                .expect("a")
                .is_null(0)
        );
    }

    #[test]
    fn late_collision_renames_stored_batches_and_preserves_snapshots() {
        let clock = ManualClock::new(0);
        let mut table = open(config(10, u64::MAX, 1), &clock);
        let dotted = vec![int_attribute("http.status", 1)];
        let nested = vec![attribute(
            "http",
            any_value::Value::KvlistValue(KeyValueList {
                values: vec![int_attribute("status", 2)],
            }),
        )];
        table
            .append(&logs_with(0, 1, dotted.clone()))
            .expect("dotted");
        let pinned = table.snapshot();
        let pinned_batch =
            Arc::clone(&pinned.active.as_ref().expect("active").partitions[0].batches[0]);
        assert_eq!(
            pinned_batch.schema().field(logs_core_len()).name(),
            "log_http_status_i64"
        );
        table
            .append(&logs_with(1, HOUR, nested.clone()))
            .expect("nested");
        let active = table.snapshot().active.expect("active");
        assert!(Arc::ptr_eq(
            &pinned_batch,
            &pinned.active.as_ref().expect("active").partitions[0].batches[0]
        ));
        assert!(!Arc::ptr_eq(
            &pinned_batch,
            &active.partitions[0].batches[0]
        ));
        assert_eq!(active.dynamic_fields.len(), 2);
        assert!(
            active
                .dynamic_fields
                .iter()
                .all(|field| { field.physical_name.starts_with("log_http_status_i64__") })
        );
        let renamed = &active.partitions[0].batches[0];
        let values: Vec<Option<i64>> = active
            .dynamic_fields
            .iter()
            .map(|field| {
                renamed
                    .column_by_name(&field.physical_name)
                    .map(|_| optional_i64(renamed, &field.physical_name, 0).expect("renamed value"))
            })
            .collect();
        assert!(values.contains(&Some(1)));
        assert!(values.contains(&None));
        let identities = identities_of(&[dotted, nested]);
        let expected = discover_dynamic_schema(identities, 2).expect("names");
        assert_eq!(active.dynamic_fields, expected.fields);

        table.rotate().expect("rotate").expect("sealed");
        let frozen = &table.snapshot().frozen[0];
        assert_eq!(frozen.partitions.len(), 2);
        for batch in frozen
            .partitions
            .iter()
            .flat_map(|partition| &partition.batches)
        {
            assert_eq!(batch.schema().as_ref(), frozen.schema.as_ref());
            assert_eq!(batch.num_columns(), frozen.schema.fields().len());
        }
    }

    #[test]
    fn generation_cap_keeps_overflow_in_json_only() {
        let clock = ManualClock::new(0);
        let mut config = config(10, u64::MAX, 1);
        config.max_dynamic_columns = 1;
        let mut table = open(config, &clock);
        table
            .append(&logs_with(
                0,
                1,
                vec![int_attribute("b", 2), int_attribute("a", 1)],
            ))
            .expect("capped");
        let active = table.snapshot().active.expect("active");
        assert_eq!(active.dynamic_fields.len(), 1);
        assert_eq!(active.dynamic_fields[0].physical_name, "log_a_i64");
        assert_eq!(
            optional_i64(&active.partitions[0].batches[0], "log_a_i64", 0),
            Some(1)
        );
        assert!(
            active.partitions[0].batches[0]
                .column_by_name("log_b_i64")
                .is_none()
        );
        assert_eq!(
            string_at(&active.partitions[0].batches[0], COLUMN_LOG_ATTRIBUTES, 0),
            r#"{"a":1,"b":2}"#
        );
        table
            .append(&logs_with(1, 1, vec![int_attribute("c", 3)]))
            .expect("overflow");
        let active = table.snapshot().active.expect("active");
        assert_eq!(active.dynamic_fields.len(), 1);
        assert!(
            active.partitions[0].batches[1]
                .column_by_name("log_c_i64")
                .is_none()
        );
        assert_eq!(
            string_at(&active.partitions[0].batches[1], COLUMN_LOG_ATTRIBUTES, 0),
            r#"{"c":3}"#
        );
        table.rotate().expect("rotate").expect("sealed");
        let frozen = &table.snapshot().frozen[0];
        assert!(
            frozen.partitions[0].batches[1]
                .column_by_name("log_a_i64")
                .expect("a")
                .is_null(0)
        );
        assert_eq!(frozen.schema.as_ref(), active.schema.as_ref());
    }

    #[test]
    fn fingerprint_matches_for_the_same_admitted_fields() {
        let clock = ManualClock::new(0);
        let mut first = open(config(10, u64::MAX, 1), &clock);
        let mut second = open(config(10, u64::MAX, 1), &clock);
        first
            .append(&logs_with(0, 1, vec![int_attribute("b", 1)]))
            .expect("b");
        first
            .append(&logs_with(1, 1, vec![int_attribute("a", 2)]))
            .expect("a");
        second
            .append(&logs_with(0, HOUR, vec![int_attribute("a", 9)]))
            .expect("a");
        second
            .append(&logs_with(1, 1, vec![int_attribute("b", 8)]))
            .expect("b");
        let left = first.snapshot().active.expect("first");
        let right = second.snapshot().active.expect("second");
        assert_eq!(left.fingerprint, right.fingerprint);
        assert_eq!(left.schema, right.schema);
        assert_eq!(left.dynamic_fields, right.dynamic_fields);
    }

    fn logs_with(sequence: u64, time: u64, attributes: Vec<KeyValue>) -> crate::DecodedLogs {
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
                max_columns: 64,
            },
        )
        .expect("decode")
    }

    fn attribute(key: &str, value: any_value::Value) -> KeyValue {
        KeyValue {
            key: key.to_owned(),
            value: Some(AnyValue { value: Some(value) }),
            ..Default::default()
        }
    }

    fn int_attribute(key: &str, value: i64) -> KeyValue {
        attribute(key, any_value::Value::IntValue(value))
    }

    fn identities_of(groups: &[Vec<KeyValue>]) -> Vec<DynamicIdentity> {
        groups
            .iter()
            .flat_map(|attributes| {
                logs_with(0, 1, attributes.clone())
                    .partitions
                    .into_iter()
                    .flat_map(|partition| {
                        partition
                            .batch
                            .schema()
                            .fields()
                            .iter()
                            .filter_map(|field| dynamic_identity(field))
                            .collect::<Vec<_>>()
                    })
            })
            .collect()
    }

    fn logs_core_len() -> usize {
        crate::core_logs_schema().fields().len()
    }

    fn optional_i64(batch: &arrow_array::RecordBatch, name: &str, row: usize) -> Option<i64> {
        let column = batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("missing {name}"))
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("{name} is not i64"));
        (!column.is_null(row)).then(|| column.value(row))
    }

    fn string_at(batch: &arrow_array::RecordBatch, name: &str, row: usize) -> String {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("missing {name}"))
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("{name} is not utf8"))
            .value(row)
            .to_owned()
    }

    fn fingerprint_for(keys: &[String]) -> (String, Vec<String>) {
        let clock = ManualClock::new(0);
        let mut table = open(config(20, u64::MAX, 1), &clock);
        for (index, key) in keys.iter().enumerate() {
            table
                .append(&logs_with(
                    u64::try_from(index).expect("index"),
                    1,
                    vec![int_attribute(key, i64::try_from(index).expect("value"))],
                ))
                .expect("append");
        }
        let active = table.snapshot().active.expect("active");
        let names = active
            .dynamic_fields
            .iter()
            .map(|field| field.physical_name.clone())
            .collect();
        (active.fingerprint, names)
    }

    proptest! {
        #[test]
        fn union_schema_is_independent_of_append_order(
            keys in prop::collection::hash_set("[a-z]{1,3}", 1..5)
        ) {
            let mut keys: Vec<_> = keys.into_iter().collect();
            keys.sort();
            let mut reversed = keys.clone();
            reversed.reverse();
            let mut rotated = keys.clone();
            rotated.rotate_left(1);
            let expected = fingerprint_for(&keys);
            assert_eq!(fingerprint_for(&reversed), expected);
            assert_eq!(fingerprint_for(&rotated), expected);
        }

        #[test]
        fn frozen_rows_keep_present_values_and_null_the_rest(
            keys in prop::collection::hash_set("[a-z]{1,3}", 1..4)
        ) {
            let mut keys: Vec<_> = keys.into_iter().collect();
            keys.sort();
            let clock = ManualClock::new(0);
            let mut table = open(config(20, u64::MAX, 1), &clock);
            for (index, key) in keys.iter().enumerate() {
                table
                    .append(&logs_with(
                        u64::try_from(index).expect("index"),
                        1,
                        vec![int_attribute(key, i64::try_from(index).expect("value"))],
                    ))
                    .expect("append");
            }
            table.rotate().expect("rotate").expect("sealed");
            let snapshot = table.snapshot();
            let frozen = &snapshot.frozen[0];
            assert_eq!(frozen.partitions[0].batches.len(), keys.len());
            for (index, key) in keys.iter().enumerate() {
                let batch = &frozen.partitions[0].batches[index];
                assert_eq!(batch.schema().as_ref(), frozen.schema.as_ref());
                for field in &frozen.dynamic_fields {
                    let value = optional_i64(batch, &field.physical_name, 0);
                    if field.identity.path == [key.clone()] {
                        assert_eq!(value, Some(i64::try_from(index).expect("value")));
                    } else {
                        assert_eq!(value, None);
                    }
                }
            }
        }

        #[test]
        fn generation_overflow_keeps_the_canonical_json(
            pairs in prop::collection::vec(("[a-z]{1,4}", any::<i64>()), 1..6)
        ) {
            let attributes: Vec<_> = pairs
                .iter()
                .map(|(key, value)| int_attribute(key, *value))
                .collect();
            let clock = ManualClock::new(0);
            let mut limits = config(20, u64::MAX, 1);
            limits.max_dynamic_columns = 1;
            let mut table = open(limits, &clock);
            table
                .append(&logs_with(0, 1, attributes.clone()))
                .expect("append");
            let active = table.snapshot().active.expect("active");
            assert!(active.dynamic_fields.len() <= 1);
            assert_eq!(
                string_at(&active.partitions[0].batches[0], COLUMN_LOG_ATTRIBUTES, 0),
                canonical_attributes_json(&attributes).expect("json")
            );
        }
    }
}
