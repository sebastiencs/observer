use std::path::Path;

use bytes::Bytes;
use observer_protocol::{AcceptedBatch, Signal};

use crate::{
    SEGMENT_HEADER_SIZE, Wal, WalCheckpoint, WalConfig, WalCursor, WalError, WalReader,
    crash_fs::CrashFs, encoded_frame_size, lane_io::OpenMode, retention::retain_committed_io,
    segment::decode_header,
};

const CLOCK: u64 = 1;

fn config(dir: &Path, target_segment_bytes: u64) -> WalConfig {
    WalConfig {
        directory: dir.to_path_buf(),
        max_entry_bytes: 1024 * 1024,
        target_segment_bytes,
    }
}

fn batch(payload: &'static [u8]) -> AcceptedBatch {
    AcceptedBatch {
        tenant_id: "tenant-a".to_owned(),
        signal: Signal::Logs,
        received_at_unix_nanos: 1,
        payload: Bytes::from_static(payload),
    }
}

fn frame_len(payload: &[u8]) -> u64 {
    u64::try_from(encoded_frame_size("tenant-a".len(), payload.len()).expect("frame")).expect("u64")
}

fn open(fs: &CrashFs, config: &WalConfig) -> Result<Wal, WalError> {
    Wal::open_with_io(config.clone(), fs.shared(), CLOCK)
}

fn read_payloads(fs: &CrashFs) -> Result<Vec<Vec<u8>>, WalError> {
    let io = fs.shared();
    let discovered = crate::checkpoint::discover_authorized(io.as_ref())?;
    if discovered.is_empty() {
        return Ok(Vec::new());
    }
    let mut header_file = io.open(&discovered[0].name, OpenMode::Read)?;
    let mut buf = [0_u8; SEGMENT_HEADER_SIZE];
    header_file.read_exact(&mut buf)?;
    let first_sequence = decode_header(&buf)?.first_sequence;
    let mut reader = WalReader::open_from_sequence_io(io, first_sequence)?;
    let mut payloads = Vec::new();
    while let Some(record) = reader.next_record()? {
        payloads.push(record.frame.payload.to_vec());
    }
    Ok(payloads)
}

fn is_prefix(got: &[Vec<u8>], full: &[&[u8]]) -> bool {
    got.len() <= full.len() && got.iter().zip(full).all(|(got, expected)| got == expected)
}

fn starts_with(got: &[Vec<u8>], required: &[&[u8]]) -> bool {
    got.len() >= required.len()
        && required
            .iter()
            .zip(got)
            .all(|(expected, got)| got.as_slice() == *expected)
}

/// Crash after every mutation of `action`, then require a recoverable prefix.
fn enumerate_crashes(
    target_segment_bytes: u64,
    prepare: impl Fn(&CrashFs, &WalConfig) -> Result<(), WalError>,
    action: impl Fn(&CrashFs, &WalConfig) -> Result<(), WalError>,
    check: impl Fn(&CrashFs, &WalConfig, u64),
) {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = config(dir.path(), target_segment_bytes);
    let fs = CrashFs::new();
    prepare(&fs, &config).expect("prepare");
    let start = fs.mutations();
    action(&fs, &config).expect("action");
    let end = fs.mutations();
    assert!(
        end > start,
        "action produced no crash boundaries\n{}",
        fs.dump()
    );

    for step in (start + 1)..=end {
        let fs = CrashFs::new();
        prepare(&fs, &config).expect("prepare");
        assert_eq!(fs.mutations(), start, "prepare was not deterministic");
        fs.arm_crash_after(step);
        let _ = action(&fs, &config);
        fs.crash();
        check(&fs, &config, step);
    }
}

fn assert_payload_prefix(fs: &CrashFs, config: &WalConfig, required: &[&[u8]], full: &[&[u8]]) {
    let wal = open(fs, config).unwrap_or_else(|error| panic!("{error}\n{}", fs.dump()));
    drop(wal);
    let got = read_payloads(fs).unwrap_or_else(|error| panic!("{error}\n{}", fs.dump()));
    assert!(
        starts_with(&got, required) && is_prefix(&got, full),
        "recovered {got:?} required {required:?} full {full:?}\n{}",
        fs.dump()
    );

    let mut wal = open(fs, config).expect("continue");
    wal.append(batch(b"tail")).expect("tail");
    drop(wal);
    let continued = read_payloads(fs).expect("read continued");
    let mut expected = got.clone();
    expected.push(b"tail".to_vec());
    assert_eq!(continued, expected, "{}", fs.dump());
}

fn cursor_after(fs: &CrashFs, records: u64) -> Result<WalCursor, WalError> {
    let mut reader = WalReader::open_io(fs.shared())?;
    let mut cursor = WalCursor::start();
    for _ in 0..records {
        cursor = reader.next_record()?.expect("record").next_cursor;
    }
    Ok(cursor)
}

#[test]
fn acknowledged_append_survives_a_later_crash() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = config(dir.path(), 256 * 1024 * 1024);
    let fs = CrashFs::new();
    let mut wal = open(&fs, &config).expect("open");
    wal.append(batch(b"one")).expect("append");
    drop(wal);
    fs.crash();
    assert_eq!(read_payloads(&fs).expect("read"), [b"one".to_vec()]);
}

#[test]
fn crashes_during_segment_creation_reopen_empty() {
    enumerate_crashes(
        256 * 1024 * 1024,
        |_, _| Ok(()),
        |fs, config| {
            open(fs, config)?;
            Ok(())
        },
        |fs, config, _step| assert_payload_prefix(fs, config, &[], &[]),
    );
}

#[test]
fn crashes_during_append_keep_a_durable_prefix() {
    enumerate_crashes(
        256 * 1024 * 1024,
        |fs, config| {
            open(fs, config)?;
            Ok(())
        },
        |fs, config| {
            let mut wal = open(fs, config)?;
            wal.append(batch(b"one"))?;
            Ok(())
        },
        |fs, config, _step| assert_payload_prefix(fs, config, &[], &[b"one"]),
    );
}

#[test]
fn crashes_during_group_append_keep_a_durable_prefix() {
    enumerate_crashes(
        256 * 1024 * 1024,
        |fs, config| {
            open(fs, config)?;
            Ok(())
        },
        |fs, config| {
            let mut wal = open(fs, config)?;
            wal.append_group([batch(b"one"), batch(b"two")])?;
            Ok(())
        },
        |fs, config, _step| assert_payload_prefix(fs, config, &[], &[b"one", b"two"]),
    );
}

#[test]
fn crashes_during_rotation_keep_the_synced_prefix() {
    let target = u64::try_from(SEGMENT_HEADER_SIZE).expect("header") + frame_len(b"one") + 1;
    enumerate_crashes(
        target,
        |fs, config| {
            let mut wal = open(fs, config)?;
            wal.append(batch(b"one"))?;
            Ok(())
        },
        |fs, config| {
            let mut wal = open(fs, config)?;
            wal.append(batch(b"two"))?;
            Ok(())
        },
        |fs, config, _step| assert_payload_prefix(fs, config, &[b"one"], &[b"one", b"two"]),
    );
}

#[test]
fn crashes_during_checkpoint_commit_keep_a_complete_checkpoint() {
    let dir_for_size = tempfile::tempdir().expect("tempdir");
    let config = config(dir_for_size.path(), 256 * 1024 * 1024);
    enumerate_crashes(
        config.target_segment_bytes,
        |fs, config| {
            let mut wal = open(fs, config)?;
            wal.append(batch(b"one"))?;
            wal.append(batch(b"two"))?;
            drop(wal);
            let cursor = cursor_after(fs, 1)?;
            let mut checkpoint = WalCheckpoint::load_io(fs.shared())?;
            checkpoint.commit(cursor)?;
            Ok(())
        },
        |fs, _config| {
            let cursor = cursor_after(fs, 2)?;
            let mut checkpoint = WalCheckpoint::load_io(fs.shared())?;
            checkpoint.commit(cursor)?;
            Ok(())
        },
        |fs, config, step| {
            let checkpoint = WalCheckpoint::load_io(fs.shared())
                .unwrap_or_else(|error| panic!("step {step}: {error}\n{}", fs.dump()));
            let sequence = checkpoint.cursor().next_sequence();
            assert!(
                sequence == 1 || sequence == 2,
                "step {step}: checkpoint sequence {sequence}\n{}",
                fs.dump()
            );
            assert_payload_prefix(fs, config, &[b"one", b"two"], &[b"one", b"two"]);
        },
    );
}

#[test]
fn crashes_during_retention_never_pass_the_checkpoint() {
    let target = u64::try_from(SEGMENT_HEADER_SIZE).expect("header") + frame_len(b"one") + 1;
    enumerate_crashes(
        target,
        |fs, config| {
            let mut wal = open(fs, config)?;
            wal.append(batch(b"one"))?;
            wal.append(batch(b"two"))?;
            wal.append(batch(b"six"))?;
            drop(wal);
            let cursor = cursor_after(fs, 2)?;
            let mut checkpoint = WalCheckpoint::load_io(fs.shared())?;
            checkpoint.commit(cursor)?;
            Ok(())
        },
        |fs, _config| {
            retain_committed_io(fs.shared())?;
            Ok(())
        },
        |fs, config, step| {
            let wal = open(fs, config)
                .unwrap_or_else(|error| panic!("step {step}: {error}\n{}", fs.dump()));
            drop(wal);
            let got = read_payloads(fs)
                .unwrap_or_else(|error| panic!("step {step}: {error}\n{}", fs.dump()));
            assert!(
                starts_with(&got, &[b"six"])
                    || got == [b"one".to_vec(), b"two".to_vec(), b"six".to_vec()],
                "step {step}: {got:?}\n{}",
                fs.dump()
            );
            let checkpoint = WalCheckpoint::load_io(fs.shared()).expect("checkpoint");
            assert_eq!(checkpoint.cursor().next_sequence(), 2);
            let first = retain_committed_io(fs.shared()).expect("retain");
            let second = retain_committed_io(fs.shared()).expect("retain again");
            assert!(
                second.deleted_segments.is_empty(),
                "step {step}: second retain deleted {:?}\nfirst {:?}\n{}",
                second.deleted_segments,
                first.deleted_segments,
                fs.dump()
            );
            let after = read_payloads(fs).expect("after retain");
            assert_eq!(after, [b"six".to_vec()]);
        },
    );
}
