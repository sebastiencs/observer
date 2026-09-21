use std::{fs, path::Path};

use crate::{
    WalCheckpoint, WalError,
    fsutil::sync_directory,
    recovery::{
        SegmentKind, lane_directory, list_segments, scan_sealed_segment, validate_contiguous_suffix,
    },
    segment::decode_header,
};

/// Segments removed by a successful [`retain_committed`] pass.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RetentionReport {
    pub deleted_segments: Vec<u64>,
    pub bytes_reclaimed: u64,
}

/// Delete sealed segments whose exclusive end is at or before the durable
/// consumer checkpoint.
pub fn retain_committed(directory: impl AsRef<Path>) -> Result<RetentionReport, WalError> {
    let directory = directory.as_ref();
    let checkpoint = WalCheckpoint::load(directory)?;
    let committed = checkpoint.cursor().next_sequence();
    if committed == 0 {
        return Ok(RetentionReport::default());
    }

    let lane_dir = lane_directory(directory);
    let found = list_segments(&lane_dir)?;
    validate_contiguous_suffix(&found)?;

    let mut eligible = Vec::new();
    let mut expected_first = None;
    for segment in found {
        if segment.kind != SegmentKind::Sealed {
            break;
        }
        let bytes = fs::read(&segment.path)?;
        let header = decode_header(&bytes)?;
        if header.segment_id != segment.id {
            return Err(WalError::InvalidSegmentHeader(
                "segment id does not match file",
            ));
        }
        if let Some(expected) = expected_first
            && header.first_sequence != expected
        {
            return Err(WalError::Corrupt("sequence discontinuity"));
        }
        let recovered = scan_sealed_segment(&bytes, header.first_sequence)?;
        if recovered.next_sequence > committed {
            break;
        }
        eligible.push(segment);
        expected_first = Some(recovered.next_sequence);
    }

    let mut report = RetentionReport::default();
    for segment in eligible {
        let bytes = fs::metadata(&segment.path)?.len();
        fs::remove_file(&segment.path)?;
        report.deleted_segments.push(segment.id);
        report.bytes_reclaimed = report
            .bytes_reclaimed
            .checked_add(bytes)
            .ok_or(WalError::Corrupt("offset overflow"))?;
    }
    if !report.deleted_segments.is_empty() {
        sync_directory(&lane_dir)?;
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use bytes::Bytes;
    use observer_protocol::{AcceptedBatch, Signal};

    use super::*;
    use crate::{
        Frame, FrameSignal, Wal, WalCheckpoint, WalConfig, WalCursor, WalError, WalReader, encode,
        recovery::lane_directory,
        segment::{SEGMENT_HEADER_SIZE, sealed_segment_file_name},
    };

    fn temp_config() -> (tempfile::TempDir, WalConfig) {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = WalConfig {
            directory: dir.path().to_path_buf(),
            max_entry_bytes: 1024 * 1024,
            target_segment_bytes: 256 * 1024 * 1024,
        };
        (dir, config)
    }

    fn config_with_target(target_segment_bytes: u64) -> (tempfile::TempDir, WalConfig) {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = WalConfig {
            directory: dir.path().to_path_buf(),
            max_entry_bytes: 1024 * 1024,
            target_segment_bytes,
        };
        (dir, config)
    }

    fn batch(payload: &[u8]) -> AcceptedBatch {
        AcceptedBatch {
            tenant_id: "tenant-a".to_owned(),
            signal: Signal::Logs,
            received_at_unix_nanos: 1,
            payload: Bytes::copy_from_slice(payload),
        }
    }

    fn encoded_len(payload: &[u8]) -> usize {
        encode(&Frame {
            sequence: 0,
            signal: FrameSignal::Logs,
            received_at_unix_nanos: 1,
            tenant_id: "tenant-a".to_owned(),
            payload: Bytes::copy_from_slice(payload),
        })
        .expect("encode")
        .len()
    }

    fn header_offset() -> u64 {
        u64::try_from(SEGMENT_HEADER_SIZE).expect("header size")
    }

    fn rotating_wal() -> (tempfile::TempDir, WalConfig, Wal) {
        let frame = u64::try_from(encoded_len(b"one")).unwrap();
        let (dir, config) = config_with_target(header_offset() + frame + 8);
        let wal = Wal::open(config.clone()).expect("open wal");
        (dir, config, wal)
    }

    fn wide_wal() -> (tempfile::TempDir, WalConfig, Wal) {
        let frame = u64::try_from(encoded_len(b"one")).unwrap();
        let (dir, config) = config_with_target(header_offset() + frame * 2 + 8);
        let wal = Wal::open(config.clone()).expect("open wal");
        (dir, config, wal)
    }

    fn lane_names(directory: &Path) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(lane_directory(directory))
            .expect("read lane")
            .map(|entry| {
                entry
                    .expect("entry")
                    .file_name()
                    .into_string()
                    .expect("utf8")
            })
            .filter(|name| name.ends_with(".wal") || name.ends_with(".open"))
            .collect();
        names.sort();
        names
    }

    fn commit_through(directory: &Path, next_sequence: u64) -> WalCursor {
        let mut reader = WalReader::open(directory).expect("reader");
        let mut cursor = WalCursor::start();
        while cursor.next_sequence() < next_sequence {
            cursor = reader
                .next_record()
                .expect("next")
                .expect("present")
                .next_cursor;
        }
        let mut checkpoint = WalCheckpoint::load(directory).expect("load");
        checkpoint.commit(cursor).expect("commit");
        cursor
    }

    #[test]
    fn no_checkpoint_deletes_nothing() {
        let (_dir, config, mut wal) = rotating_wal();
        wal.append(batch(b"one")).expect("one");
        wal.append(batch(b"two")).expect("two");
        drop(wal);

        let before = lane_names(&config.directory);
        let report = retain_committed(&config.directory).expect("retain");
        assert_eq!(report, RetentionReport::default());
        assert_eq!(lane_names(&config.directory), before);
    }

    #[test]
    fn partial_checkpoint_deletes_nothing() {
        let (_dir, config, mut wal) = wide_wal();
        wal.append(batch(b"one")).expect("one");
        wal.append(batch(b"two")).expect("two");
        wal.append(batch(b"three")).expect("three");
        drop(wal);

        commit_through(&config.directory, 1);
        let before = lane_names(&config.directory);
        let first_sealed = lane_directory(&config.directory).join(sealed_segment_file_name(0));
        assert!(first_sealed.is_file());

        let report = retain_committed(&config.directory).expect("retain");
        assert_eq!(report, RetentionReport::default());
        assert_eq!(lane_names(&config.directory), before);
        assert!(first_sealed.is_file());
    }

    #[test]
    fn truncated_checkpoint_deletes_nothing() {
        let (_dir, config, mut wal) = rotating_wal();
        wal.append(batch(b"one")).expect("one");
        wal.append(batch(b"two")).expect("two");
        drop(wal);
        commit_through(&config.directory, 2);

        let path = lane_directory(&config.directory).join("consumer.checkpoint");
        let bytes = fs::read(&path).expect("read");
        fs::write(&path, &bytes[..bytes.len() / 2]).expect("truncate");
        let before = lane_names(&config.directory);

        assert!(matches!(
            retain_committed(&config.directory),
            Err(WalError::InvalidCheckpoint(_))
        ));
        assert_eq!(lane_names(&config.directory), before);
    }

    #[test]
    fn deletes_fully_consumed_sealed_prefix_and_reports_bytes() {
        let (_dir, config, mut wal) = rotating_wal();
        wal.append(batch(b"one")).expect("one");
        wal.append(batch(b"two")).expect("two");
        wal.append(batch(b"three")).expect("three");
        drop(wal);

        let first_sealed = lane_directory(&config.directory).join(sealed_segment_file_name(0));
        let first_len = fs::metadata(&first_sealed).expect("meta").len();
        commit_through(&config.directory, 1);

        let report = retain_committed(&config.directory).expect("retain");
        assert_eq!(report.deleted_segments, vec![0]);
        assert_eq!(report.bytes_reclaimed, first_len);
        assert!(!first_sealed.exists());
        assert_eq!(
            lane_names(&config.directory),
            vec![
                "00000000000000000001.wal".to_owned(),
                "00000000000000000002.open".to_owned()
            ]
        );
    }

    #[test]
    fn repeated_retention_is_a_noop() {
        let (_dir, config, mut wal) = rotating_wal();
        wal.append(batch(b"one")).expect("one");
        wal.append(batch(b"two")).expect("two");
        wal.append(batch(b"three")).expect("three");
        drop(wal);
        commit_through(&config.directory, 1);

        let first = retain_committed(&config.directory).expect("first");
        assert_eq!(first.deleted_segments, vec![0]);
        let second = retain_committed(&config.directory).expect("second");
        assert_eq!(second, RetentionReport::default());
        assert_eq!(
            lane_names(&config.directory),
            vec![
                "00000000000000000001.wal".to_owned(),
                "00000000000000000002.open".to_owned()
            ]
        );
    }

    #[test]
    fn writer_and_reader_reopen_after_deletion() {
        let (_dir, config, mut wal) = rotating_wal();
        wal.append(batch(b"one")).expect("one");
        wal.append(batch(b"two")).expect("two");
        wal.append(batch(b"three")).expect("three");
        drop(wal);

        commit_through(&config.directory, 1);
        retain_committed(&config.directory).expect("retain");

        assert!(matches!(
            WalReader::open(&config.directory),
            Err(WalError::Corrupt("cursor does not resolve to a segment"))
        ));

        let cursor = WalCheckpoint::load(&config.directory)
            .expect("reload")
            .cursor();
        let mut reader =
            WalReader::open_at(&config.directory, cursor).expect("reader at checkpoint");
        assert_eq!(
            reader
                .next_record()
                .expect("two")
                .expect("present")
                .frame
                .payload
                .as_ref(),
            b"two"
        );

        let mut wal = Wal::open(config.clone()).expect("reopen writer");
        let fourth = wal.append(batch(b"four")).expect("four");
        assert_eq!(fourth.sequence, 3);
    }

    #[test]
    fn missing_prefix_without_checkpoint_is_fatal() {
        let (_dir, config, mut wal) = rotating_wal();
        wal.append(batch(b"one")).expect("one");
        wal.append(batch(b"two")).expect("two");
        drop(wal);
        fs::remove_file(lane_directory(&config.directory).join(sealed_segment_file_name(0)))
            .expect("delete prefix");

        assert!(matches!(
            Wal::open(config.clone()),
            Err(WalError::Corrupt("missing or out-of-order segment id"))
        ));
    }

    #[test]
    fn missing_middle_segment_is_always_fatal() {
        let (_dir, config, mut wal) = rotating_wal();
        wal.append(batch(b"one")).expect("one");
        wal.append(batch(b"two")).expect("two");
        wal.append(batch(b"three")).expect("three");
        drop(wal);
        commit_through(&config.directory, 3);
        fs::remove_file(lane_directory(&config.directory).join(sealed_segment_file_name(1)))
            .expect("delete middle");

        assert!(matches!(
            Wal::open(config),
            Err(WalError::Corrupt("missing or out-of-order segment id"))
        ));
    }

    #[test]
    fn sealed_corruption_aborts_without_deleting() {
        let (_dir, config, mut wal) = rotating_wal();
        wal.append(batch(b"one")).expect("one");
        wal.append(batch(b"two")).expect("two");
        wal.append(batch(b"three")).expect("three");
        drop(wal);
        commit_through(&config.directory, 3);

        let second = lane_directory(&config.directory).join(sealed_segment_file_name(1));
        let mut bytes = fs::read(&second).expect("read");
        *bytes.last_mut().unwrap() ^= 0xff;
        fs::write(&second, bytes).expect("corrupt");
        let before = lane_names(&config.directory);

        assert!(retain_committed(&config.directory).is_err());
        assert_eq!(lane_names(&config.directory), before);
        assert!(
            lane_directory(&config.directory)
                .join(sealed_segment_file_name(0))
                .is_file()
        );
    }

    #[test]
    fn full_lifecycle_append_read_commit_retain_reopen() {
        let (_dir, config, mut wal) = rotating_wal();
        wal.append(batch(b"one")).expect("one");
        wal.append(batch(b"two")).expect("two");
        wal.append(batch(b"three")).expect("three");
        drop(wal);

        commit_through(&config.directory, 1);
        retain_committed(&config.directory).expect("retain");

        let mut wal = Wal::open(config.clone()).expect("writer");
        wal.append(batch(b"four")).expect("four");
        drop(wal);

        let cursor = WalCheckpoint::load(&config.directory)
            .expect("reload")
            .cursor();
        let mut reader = WalReader::open_at(&config.directory, cursor).expect("reader");
        let frames: Vec<_> = std::iter::from_fn(|| reader.next_record().expect("next")).collect();
        assert_eq!(
            frames
                .iter()
                .map(|record| record.frame.payload.as_ref())
                .collect::<Vec<_>>(),
            [b"two".as_slice(), b"three".as_slice(), b"four".as_slice()]
        );
    }

    #[test]
    fn start_cursor_after_retain_does_not_skip_data() {
        let (_dir, config) = temp_config();
        let mut wal = Wal::open(config.clone()).expect("open");
        wal.append(batch(b"one")).expect("one");
        drop(wal);
        commit_through(&config.directory, 1);
        assert_eq!(
            retain_committed(&config.directory).expect("retain"),
            RetentionReport::default()
        );
    }
}
