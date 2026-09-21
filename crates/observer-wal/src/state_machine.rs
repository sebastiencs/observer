use std::path::PathBuf;

use bytes::Bytes;
use observer_protocol::{AcceptedBatch, Signal};
use proptest::prelude::*;

use crate::{
    SEGMENT_HEADER_SIZE, Wal, WalCheckpoint, WalConfig, WalCursor, WalError, WalReader,
    crash_fs::CrashFs, lane_io::OpenMode, retention::retain_committed_io, segment::decode_header,
};

const CLOCK: u64 = 7;

#[derive(Clone, Debug)]
enum Op {
    Append { payload: Vec<u8> },
    AppendGroup { payloads: Vec<Vec<u8>> },
    ReadSome { max: u8 },
    Refresh,
    CommitObserved,
    Retain,
    CloseReopen,
    CrashAppend { payload: Vec<u8>, offset: u8 },
    CrashGroup { payloads: Vec<Vec<u8>>, offset: u8 },
    CrashCommit { offset: u8 },
    CrashRetain { offset: u8 },
}

struct Record {
    sequence: u64,
    payload: Vec<u8>,
}

struct World {
    fs: CrashFs,
    config: WalConfig,
    wal: Option<Wal>,
    reader: Option<WalReader>,
    /// Payloads by sequence, including a durable suffix that was never acknowledged.
    log: Vec<Vec<u8>>,
    acked: usize,
    committed: u64,
    observed: Vec<WalCursor>,
    crashes_left: u8,
}

fn proptest_config() -> ProptestConfig {
    let config = ProptestConfig::default();
    if std::env::var_os("PROPTEST_CASES").is_none() {
        // config.cases = 512;
    }
    config
}

fn payload_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..12)
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => payload_strategy().prop_map(|payload| Op::Append { payload }),
        2 => prop::collection::vec(payload_strategy(), 1..4)
            .prop_map(|payloads| Op::AppendGroup { payloads }),
        2 => (1u8..5).prop_map(|max| Op::ReadSome { max }),
        1 => Just(Op::Refresh),
        2 => Just(Op::CommitObserved),
        1 => Just(Op::Retain),
        1 => Just(Op::CloseReopen),
        1 => (payload_strategy(), 0u8..8).prop_map(|(payload, offset)| Op::CrashAppend {
            payload,
            offset
        }),
        1 => (prop::collection::vec(payload_strategy(), 1..3), 0u8..10).prop_map(
            |(payloads, offset)| Op::CrashGroup { payloads, offset }
        ),
        1 => (0u8..8u8).prop_map(|offset| Op::CrashCommit { offset }),
        1 => (0u8..8u8).prop_map(|offset| Op::CrashRetain { offset }),
    ]
}

fn batch(payload: Vec<u8>) -> AcceptedBatch {
    AcceptedBatch {
        tenant_id: "t".to_owned(),
        signal: Signal::Logs,
        received_at_unix_nanos: 1,
        payload: Bytes::from(payload),
    }
}

fn fail(world: &World, ops: &[Op], message: impl std::fmt::Display) -> TestCaseError {
    TestCaseError::fail(format!("{message}\nops={ops:?}\n{}", world.fs.dump()))
}

fn open_wal(world: &World) -> Result<Wal, WalError> {
    Wal::open_with_io(world.config.clone(), world.fs.shared(), CLOCK)
}

fn open_reader(fs: &CrashFs) -> Result<WalReader, WalError> {
    let io = fs.shared();
    let discovered = crate::checkpoint::discover_authorized(io.as_ref())?;
    if discovered.is_empty() {
        return WalReader::open_io(io);
    }
    let mut header_file = io.open(&discovered[0].name, OpenMode::Read)?;
    let mut buf = [0_u8; SEGMENT_HEADER_SIZE];
    header_file.read_exact(&mut buf)?;
    let first_sequence = decode_header(&buf)?.first_sequence;
    WalReader::open_from_sequence_io(io, first_sequence)
}

fn read_records(fs: &CrashFs) -> Result<Vec<Record>, WalError> {
    let mut reader = open_reader(fs)?;
    let mut records = Vec::new();
    while let Some(record) = reader.next_record()? {
        records.push(Record {
            sequence: record.frame.sequence,
            payload: record.frame.payload.to_vec(),
        });
    }
    Ok(records)
}

fn check(world: &mut World, ops: &[Op]) -> Result<(), TestCaseError> {
    let records = read_records(&world.fs).map_err(|error| fail(world, ops, error))?;
    let start = records
        .first()
        .map(|record| record.sequence)
        .unwrap_or(world.committed);
    if start > world.committed {
        return Err(fail(
            world,
            ops,
            format!(
                "retained prefix {start} passes checkpoint {}",
                world.committed
            ),
        ));
    }
    for (index, record) in records.iter().enumerate() {
        let sequence = start + u64::try_from(index).expect("index");
        if record.sequence != sequence {
            return Err(fail(world, ops, format!("sequence gap at {sequence}")));
        }
        let seq = usize::try_from(record.sequence).expect("seq");
        if seq < world.log.len() && world.log[seq] != record.payload {
            return Err(fail(world, ops, format!("payload mismatch at {seq}")));
        }
        if seq == world.log.len() {
            world.log.push(record.payload.clone());
        } else if seq > world.log.len() {
            return Err(fail(world, ops, format!("durable gap at {seq}")));
        }
    }
    for seq in usize::try_from(start).expect("start")..world.acked {
        if !records.iter().any(|record| record.sequence == seq as u64) {
            return Err(fail(
                world,
                ops,
                format!("missing acknowledged sequence {seq}"),
            ));
        }
    }
    let checkpoint =
        WalCheckpoint::load_io(world.fs.shared()).map_err(|error| fail(world, ops, error))?;
    if checkpoint.cursor().next_sequence() != world.committed {
        return Err(fail(
            world,
            ops,
            format!(
                "checkpoint {} != model {}",
                checkpoint.cursor().next_sequence(),
                world.committed
            ),
        ));
    }
    if world.committed as usize <= world.log.len() {
        WalReader::open_from_sequence_io(world.fs.shared(), world.committed)
            .map_err(|error| fail(world, ops, error))?;
    }
    Ok(())
}

fn ensure_wal<'a>(world: &'a mut World, ops: &[Op]) -> Result<&'a mut Wal, TestCaseError> {
    if world.wal.is_none() {
        let wal = match open_wal(world) {
            Ok(wal) => wal,
            Err(error) => return Err(fail(world, ops, error)),
        };
        world.wal = Some(wal);
    }
    Ok(world.wal.as_mut().expect("wal"))
}

fn drop_handles(world: &mut World) {
    world.wal = None;
    world.reader = None;
}

fn reopen(world: &mut World, ops: &[Op]) -> Result<(), TestCaseError> {
    drop_handles(world);
    let wal = match open_wal(world) {
        Ok(wal) => wal,
        Err(error) => return Err(fail(world, ops, error)),
    };
    world.wal = Some(wal);
    Ok(())
}

fn append_crashing(world: &mut World, payload: Vec<u8>) -> Result<bool, WalError> {
    if !ensure_open(world)? {
        return Ok(false);
    }
    let wal = world.wal.as_mut().expect("wal");
    match wal.append(batch(payload.clone())) {
        Ok(receipt) => {
            if receipt.sequence as usize != world.log.len() {
                return Err(WalError::Corrupt("append sequence"));
            }
            world.log.push(payload);
            world.acked = world.log.len();
            Ok(true)
        }
        Err(WalError::Io(_)) => Ok(false),
        Err(error) => Err(error),
    }
}

fn group_crashing(world: &mut World, payloads: Vec<Vec<u8>>) -> Result<bool, WalError> {
    if !ensure_open(world)? {
        return Ok(false);
    }
    let batches: Vec<_> = payloads.iter().cloned().map(batch).collect();
    let wal = world.wal.as_mut().expect("wal");
    match wal.append_group(batches) {
        Ok(receipts) => {
            for (receipt, payload) in receipts.into_iter().zip(payloads) {
                if receipt.sequence as usize != world.log.len() {
                    return Err(WalError::Corrupt("group sequence"));
                }
                world.log.push(payload);
            }
            world.acked = world.log.len();
            Ok(true)
        }
        Err(WalError::Io(_)) => Ok(false),
        Err(error) => Err(error),
    }
}

/// `false` means opening the writer hit the injected crash.
fn ensure_open(world: &mut World) -> Result<bool, WalError> {
    if world.wal.is_none() {
        match open_wal(world) {
            Ok(wal) => world.wal = Some(wal),
            Err(WalError::Io(_)) => return Ok(false),
            Err(error) => return Err(error),
        }
    }
    Ok(true)
}

fn note_append(
    world: &mut World,
    ops: &[Op],
    sequence: u64,
    payload: Vec<u8>,
) -> Result<(), TestCaseError> {
    if sequence as usize != world.log.len() {
        return Err(fail(
            world,
            ops,
            format!("append sequence {sequence} != log {}", world.log.len()),
        ));
    }
    world.log.push(payload);
    world.acked = world.log.len();
    Ok(())
}

fn crash_after(
    world: &mut World,
    ops: &[Op],
    offset: u8,
    body: impl FnOnce(&mut World) -> Result<bool, WalError>,
) -> Result<(), TestCaseError> {
    let before = world.fs.mutations();
    world.fs.arm_crash_after(before + u64::from(offset) + 1);
    let acked = body(world).map_err(|error| fail(world, ops, error))?;
    world.fs.disarm();
    drop_handles(world);
    world.fs.crash();
    reopen(world, ops)?;
    if !acked {
        // A commit can become durable and still return the injected crash.
        let loaded = WalCheckpoint::load_io(world.fs.shared())
            .map_err(|error| fail(world, ops, error))?
            .cursor()
            .next_sequence();
        if loaded != world.committed {
            if loaded < world.committed || loaded as usize > world.acked {
                return Err(fail(
                    world,
                    ops,
                    format!("checkpoint jumped from {} to {loaded}", world.committed),
                ));
            }
            world.committed = loaded;
        }
    }
    check(world, ops)
}

fn run(ops: &[Op]) -> Result<(), TestCaseError> {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut world = World {
        fs: CrashFs::new(),
        config: WalConfig {
            directory: PathBuf::from(dir.path()),
            max_entry_bytes: 1024 * 1024,
            target_segment_bytes: u64::try_from(SEGMENT_HEADER_SIZE).expect("header") + 96,
        },
        wal: None,
        reader: None,
        log: Vec::new(),
        acked: 0,
        committed: 0,
        observed: Vec::new(),
        crashes_left: 5,
    };

    for op in ops {
        match op {
            Op::Append { payload } => {
                let wal = ensure_wal(&mut world, ops)?;
                let receipt = wal
                    .append(batch(payload.clone()))
                    .map_err(|error| fail(&world, ops, error))?;
                note_append(&mut world, ops, receipt.sequence, payload.clone())?;
            }
            Op::AppendGroup { payloads } => {
                let batches: Vec<_> = payloads.iter().cloned().map(batch).collect();
                let wal = ensure_wal(&mut world, ops)?;
                let receipts = wal
                    .append_group(batches)
                    .map_err(|error| fail(&world, ops, error))?;
                for (receipt, payload) in receipts.into_iter().zip(payloads) {
                    note_append(&mut world, ops, receipt.sequence, payload.clone())?;
                }
            }
            Op::ReadSome { max } => {
                if world.reader.is_none() {
                    let reader = match open_reader(&world.fs) {
                        Ok(reader) => reader,
                        Err(error) => return Err(fail(&world, ops, error)),
                    };
                    world.reader = Some(reader);
                }
                for _ in 0..*max {
                    let next = world.reader.as_mut().expect("reader").next_record();
                    match next {
                        Ok(Some(record)) => {
                            let seq = usize::try_from(record.frame.sequence).expect("seq");
                            if seq < world.acked && world.log[seq] != record.frame.payload {
                                return Err(fail(&world, ops, format!("read mismatch at {seq}")));
                            }
                            if record.next_cursor.next_sequence() as usize <= world.acked {
                                world.observed.push(record.next_cursor);
                            }
                        }
                        Ok(None) => break,
                        Err(error) => return Err(fail(&world, ops, error)),
                    }
                }
            }
            Op::Refresh => {
                if let Some(reader) = world.reader.as_mut() {
                    reader.refresh().map_err(|error| fail(&world, ops, error))?;
                }
            }
            Op::CommitObserved => commit_observed(&mut world, ops)?,
            Op::Retain => {
                let first = retain_committed_io(world.fs.shared())
                    .map_err(|error| fail(&world, ops, error))?;
                let second = retain_committed_io(world.fs.shared())
                    .map_err(|error| fail(&world, ops, error))?;
                if !second.deleted_segments.is_empty() {
                    return Err(fail(
                        &world,
                        ops,
                        format!(
                            "second retain deleted {:?}; first {:?}",
                            second.deleted_segments, first.deleted_segments
                        ),
                    ));
                }
                world.reader = None;
            }
            Op::CloseReopen => reopen(&mut world, ops)?,
            Op::CrashAppend { payload, offset } if world.crashes_left > 0 => {
                world.crashes_left -= 1;
                let payload = payload.clone();
                let offset = *offset;
                crash_after(&mut world, ops, offset, |world| {
                    append_crashing(world, payload)
                })?;
            }
            Op::CrashGroup { payloads, offset } if world.crashes_left > 0 => {
                world.crashes_left -= 1;
                let payloads = payloads.clone();
                let offset = *offset;
                crash_after(&mut world, ops, offset, |world| {
                    group_crashing(world, payloads)
                })?;
            }
            Op::CrashCommit { offset } if world.crashes_left > 0 => {
                world.crashes_left -= 1;
                let offset = *offset;
                crash_after(&mut world, ops, offset, |world| {
                    let Some(cursor) = world
                        .observed
                        .iter()
                        .rev()
                        .find(|cursor| {
                            cursor.next_sequence() > world.committed
                                && cursor.next_sequence() as usize <= world.acked
                        })
                        .copied()
                    else {
                        return Ok(true);
                    };
                    let mut checkpoint = WalCheckpoint::load_io(world.fs.shared())?;
                    match checkpoint.commit(cursor) {
                        Ok(()) => {
                            world.committed = cursor.next_sequence();
                            Ok(true)
                        }
                        Err(WalError::Io(_)) => Ok(false),
                        Err(error) => Err(error),
                    }
                })?;
            }
            Op::CrashRetain { offset } if world.crashes_left > 0 => {
                world.crashes_left -= 1;
                let offset = *offset;
                crash_after(&mut world, ops, offset, |world| {
                    match retain_committed_io(world.fs.shared()) {
                        Ok(_) => Ok(true),
                        Err(WalError::Io(_)) => Ok(false),
                        Err(error) => Err(error),
                    }
                })?;
                let again = retain_committed_io(world.fs.shared())
                    .map_err(|error| fail(&world, ops, error))?;
                if !again.deleted_segments.is_empty() {
                    let third = retain_committed_io(world.fs.shared())
                        .map_err(|error| fail(&world, ops, error))?;
                    if !third.deleted_segments.is_empty() {
                        return Err(fail(&world, ops, "retention did not converge"));
                    }
                }
            }
            Op::CrashAppend { payload, .. } => {
                let wal = ensure_wal(&mut world, ops)?;
                let receipt = wal
                    .append(batch(payload.clone()))
                    .map_err(|error| fail(&world, ops, error))?;
                note_append(&mut world, ops, receipt.sequence, payload.clone())?;
            }
            Op::CrashGroup { payloads, .. } => {
                let batches: Vec<_> = payloads.iter().cloned().map(batch).collect();
                let wal = ensure_wal(&mut world, ops)?;
                let receipts = wal
                    .append_group(batches)
                    .map_err(|error| fail(&world, ops, error))?;
                for (receipt, payload) in receipts.into_iter().zip(payloads) {
                    note_append(&mut world, ops, receipt.sequence, payload.clone())?;
                }
            }
            Op::CrashCommit { .. } => commit_observed(&mut world, ops)?,
            Op::CrashRetain { .. } => {
                retain_committed_io(world.fs.shared()).map_err(|error| fail(&world, ops, error))?;
                let second = retain_committed_io(world.fs.shared())
                    .map_err(|error| fail(&world, ops, error))?;
                if !second.deleted_segments.is_empty() {
                    return Err(fail(&world, ops, "second retain deleted segments"));
                }
                world.reader = None;
            }
        }
        check(&mut world, ops)?;
    }
    Ok(())
}

fn commit_observed(world: &mut World, ops: &[Op]) -> Result<(), TestCaseError> {
    let Some(cursor) = world
        .observed
        .iter()
        .rev()
        .find(|cursor| {
            cursor.next_sequence() > world.committed
                && (cursor.next_sequence() as usize) <= world.acked
        })
        .copied()
    else {
        return Ok(());
    };
    let mut checkpoint =
        WalCheckpoint::load_io(world.fs.shared()).map_err(|error| fail(world, ops, error))?;
    checkpoint
        .commit(cursor)
        .map_err(|error| fail(world, ops, error))?;
    world.committed = cursor.next_sequence();
    Ok(())
}

proptest! {
    #![proptest_config(proptest_config())]
    #[test]
    fn synchronous_lifecycle_respects_crash_invariants(
        ops in prop::collection::vec(op_strategy(), 1..=40)
    ) {
        run(&ops)?;
    }
}

#[test]
fn fixed_trace_appends_commits_retains_and_crashes() {
    run(&[
        Op::Append {
            payload: b"one".to_vec(),
        },
        Op::Append {
            payload: b"two".to_vec(),
        },
        Op::ReadSome { max: 2 },
        Op::CommitObserved,
        Op::CrashAppend {
            payload: b"three".to_vec(),
            offset: 0,
        },
        Op::ReadSome { max: 4 },
        Op::CommitObserved,
        Op::Retain,
        Op::CrashRetain { offset: 0 },
        Op::Append {
            payload: b"four".to_vec(),
        },
        Op::CloseReopen,
    ])
    .expect("fixed trace");
}
