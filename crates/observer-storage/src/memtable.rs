//! Per-tenant active and frozen Arrow generations.
//!
//! The caller decodes a WAL frame, then appends the resulting [`DecodedLogs`]. A generation owns a
//! contiguous exclusive sequence range. The frame that crosses the row or byte limit stays in the
//! active generation, and that generation then freezes. Age freezes a generation that already holds
//! frames before the next frame starts a new window. An empty generation does not rotate.

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

use arrow_array::RecordBatch;

use crate::{DecodedLogs, EventHour};

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
    /// Freezing this append or rotation would exceed `max_frozen`. Nothing was stored.
    FrozenLimit,
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
            Self::FrozenLimit => formatter.write_str("frozen generation limit has been reached"),
        }
    }
}

impl Error for MemtableError {}

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
        let frame_rows = frame_rows(logs);
        let frame_bytes = frame_bytes(logs);
        let age_freeze = self.active.has_frames() && self.age_reached(now);
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
        for partition in &logs.partitions {
            self.active
                .partitions
                .entry(partition.hour)
                .or_default()
                .push(Arc::new(partition.batch.clone()));
        }
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
    partitions: BTreeMap<EventHour, Vec<Arc<RecordBatch>>>,
}

impl ActiveGeneration {
    fn empty(id: u64) -> Self {
        Self {
            id,
            first_sequence: 0,
            next_sequence: 0,
            rows: 0,
            bytes: 0,
            opened_at_unix_nanos: 0,
            partitions: BTreeMap::new(),
        }
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

fn frame_rows(logs: &DecodedLogs) -> u64 {
    logs.partitions
        .iter()
        .map(|partition| u64::try_from(partition.batch.num_rows()).expect("row count"))
        .sum()
}

fn frame_bytes(logs: &DecodedLogs) -> u64 {
    logs.partitions
        .iter()
        .map(|partition| {
            u64::try_from(partition.batch.get_array_memory_size()).expect("batch bytes")
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::{
        Appended, Clock, Generation, ManualClock, Memtable, MemtableConfig, MemtableError,
        SystemClock,
    };
    use crate::{EventHour, decode_logs_frame};
    use arrow_array::{Array, StringArray};
    use bytes::Bytes;
    use observer_protocol::otlp::{
        AnyValue, ExportLogsServiceRequest, LogRecord, ResourceLogs, ScopeLogs, any_value,
    };
    use observer_wal::{Frame, FrameSignal};
    use prost::Message;
    use std::{sync::Arc, time::Duration};

    const HOUR: u64 = 3_600_000_000_000;

    fn config(max_rows: u64, max_bytes: u64, max_frozen: usize) -> MemtableConfig {
        MemtableConfig {
            max_rows,
            max_bytes,
            max_age: Duration::from_secs(60),
            max_frozen,
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
        decode_logs_frame(&Frame {
            sequence,
            signal: FrameSignal::Logs,
            received_at_unix_nanos: 1,
            tenant_id: "tenant-a".to_owned(),
            payload: Bytes::from(request.encode_to_vec()),
        })
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
}
