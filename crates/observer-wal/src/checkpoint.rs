use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use crate::{
    WalCursor, WalError,
    fsutil::sync_directory,
    reader::{rematerialize_sequence, validate_physical_hint},
    recovery::{lane_directory, list_segments, validate_contiguous_suffix},
    segment::LANE_ID,
};

const CHECKPOINT_MAGIC: &[u8; 8] = b"OBS-CKP1";
const CHECKPOINT_VERSION: u16 = 1;
const CHECKPOINT_SIZE: usize = 44;
const CHECKPOINT_NAME: &str = "consumer.checkpoint";
const CHECKPOINT_TMP_NAME: &str = "consumer.checkpoint.tmp";

/// One durable, monotonic consumer checkpoint for a WAL lane.
#[derive(Debug)]
pub struct WalCheckpoint {
    directory: PathBuf,
    lane_dir: PathBuf,
    committed: WalCursor,
}

impl WalCheckpoint {
    pub fn load(directory: impl Into<PathBuf>) -> Result<Self, WalError> {
        let directory = directory.into();
        let lane_dir = lane_directory(&directory);
        let raw = read_checkpoint_file(&lane_dir)?;
        let cursor = match raw {
            None => WalCursor::start(),
            Some(bytes) => decode_checkpoint(&bytes)?,
        };
        let committed = resolve_cursor(&directory, cursor)?;
        Ok(Self {
            directory,
            lane_dir,
            committed,
        })
    }

    #[must_use]
    pub fn cursor(&self) -> WalCursor {
        self.committed
    }

    pub fn commit(&mut self, cursor: WalCursor) -> Result<(), WalError> {
        if cursor.next_sequence() < self.committed.next_sequence() {
            return Err(WalError::CheckpointRegression {
                committed: self.committed.next_sequence(),
                attempted: cursor.next_sequence(),
            });
        }
        if cursor.next_sequence() == self.committed.next_sequence() {
            if cursor != self.committed {
                return Err(WalError::InvalidCheckpoint(
                    "physical cursor changed at the same sequence",
                ));
            }
            return Ok(());
        }

        let resolved = resolve_cursor(&self.directory, cursor)?;
        persist_checkpoint(&self.lane_dir, resolved)?;
        self.committed = resolved;
        Ok(())
    }
}

fn read_checkpoint_file(lane_dir: &Path) -> Result<Option<[u8; CHECKPOINT_SIZE]>, WalError> {
    let path = lane_dir.join(CHECKPOINT_NAME);
    match fs::read(&path) {
        Ok(bytes) => {
            if bytes.len() != CHECKPOINT_SIZE {
                return Err(WalError::InvalidCheckpoint("unexpected length"));
            }
            let mut buf = [0_u8; CHECKPOINT_SIZE];
            buf.copy_from_slice(&bytes);
            Ok(Some(buf))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn encode_checkpoint(cursor: WalCursor) -> [u8; CHECKPOINT_SIZE] {
    let mut buf = [0_u8; CHECKPOINT_SIZE];
    buf[0..8].copy_from_slice(CHECKPOINT_MAGIC);
    buf[8..10].copy_from_slice(&CHECKPOINT_VERSION.to_le_bytes());
    buf[10..12].copy_from_slice(&0_u16.to_le_bytes());
    buf[12..16].copy_from_slice(&LANE_ID.to_le_bytes());
    buf[16..24].copy_from_slice(&cursor.next_sequence().to_le_bytes());
    buf[24..32].copy_from_slice(&cursor.segment_id().to_le_bytes());
    buf[32..40].copy_from_slice(&cursor.offset().to_le_bytes());
    let crc = crc32c::crc32c(&buf[..40]);
    buf[40..44].copy_from_slice(&crc.to_le_bytes());
    buf
}

fn decode_checkpoint(data: &[u8]) -> Result<WalCursor, WalError> {
    if data.len() != CHECKPOINT_SIZE {
        return Err(WalError::InvalidCheckpoint("unexpected length"));
    }
    if &data[0..8] != CHECKPOINT_MAGIC {
        return Err(WalError::InvalidCheckpoint("unrecognized magic"));
    }
    let version = u16::from_le_bytes([data[8], data[9]]);
    if version != CHECKPOINT_VERSION {
        return Err(WalError::InvalidCheckpoint("unsupported version"));
    }
    let reserved = u16::from_le_bytes([data[10], data[11]]);
    if reserved != 0 {
        return Err(WalError::InvalidCheckpoint("reserved field must be zero"));
    }
    let stored_crc = u32::from_le_bytes([data[40], data[41], data[42], data[43]]);
    if stored_crc != crc32c::crc32c(&data[..40]) {
        return Err(WalError::InvalidCheckpoint("checksum mismatch"));
    }
    let lane_id = u32::from_le_bytes([data[12], data[13], data[14], data[15]]);
    if lane_id != LANE_ID {
        return Err(WalError::UnexpectedLane { lane_id });
    }
    Ok(WalCursor::at(
        u64::from_le_bytes(data[16..24].try_into().expect("8 bytes")),
        u64::from_le_bytes(data[24..32].try_into().expect("8 bytes")),
        u64::from_le_bytes(data[32..40].try_into().expect("8 bytes")),
    ))
}

fn resolve_cursor(directory: &Path, cursor: WalCursor) -> Result<WalCursor, WalError> {
    let found = list_segments(&lane_directory(directory))?;
    validate_contiguous_suffix(&found)?;

    if let Some(segment) = found
        .iter()
        .find(|segment| segment.id == cursor.segment_id())
    {
        validate_physical_hint(segment, cursor)?;
        return Ok(cursor);
    }

    rematerialize_sequence(&found, cursor.next_sequence())
}

fn persist_checkpoint(lane_dir: &Path, cursor: WalCursor) -> Result<(), WalError> {
    fs::create_dir_all(lane_dir)?;
    let tmp = lane_dir.join(CHECKPOINT_TMP_NAME);
    let path = lane_dir.join(CHECKPOINT_NAME);
    let encoded = encode_checkpoint(cursor);
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)?;
    file.write_all(&encoded)?;
    file.sync_data()?;
    fs::rename(&tmp, &path)?;
    sync_directory(lane_dir)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use bytes::Bytes;
    use observer_protocol::{AcceptedBatch, Signal};

    use super::*;
    use crate::{
        Frame, FrameSignal, Wal, WalConfig, WalReader, encode, recovery::lane_directory,
        segment::SEGMENT_HEADER_SIZE,
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

    fn write_checkpoint_bytes(directory: &Path, bytes: &[u8]) {
        let lane = lane_directory(directory);
        fs::create_dir_all(&lane).expect("lane");
        fs::write(lane.join(CHECKPOINT_NAME), bytes).expect("write checkpoint");
    }

    #[test]
    fn missing_checkpoint_starts_at_zero() {
        let dir = tempfile::tempdir().expect("tempdir");
        let checkpoint = WalCheckpoint::load(dir.path()).expect("load");
        assert_eq!(checkpoint.cursor(), WalCursor::start());
    }

    #[test]
    fn commit_round_trips_cursor_fields() {
        let (_dir, config) = temp_config();
        let mut wal = Wal::open(config.clone()).expect("open wal");
        wal.append(batch(b"one")).expect("one");
        wal.append(batch(b"two")).expect("two");

        let mut reader = WalReader::open(&config.directory).expect("reader");
        let first = reader.next_record().expect("first").expect("present");
        let mut checkpoint = WalCheckpoint::load(&config.directory).expect("load");
        checkpoint.commit(first.next_cursor).expect("commit");

        let reloaded = WalCheckpoint::load(&config.directory).expect("reload");
        assert_eq!(reloaded.cursor(), first.next_cursor);
        assert_eq!(reloaded.cursor().next_sequence(), 1);
        assert_eq!(reloaded.cursor().segment_id(), 0);
        assert!(reloaded.cursor().offset() > header_offset());
    }

    #[test]
    fn commit_and_reload_resume_at_first_unprocessed_frame() {
        let (_dir, config) = temp_config();
        let mut wal = Wal::open(config.clone()).expect("open wal");
        wal.append(batch(b"one")).expect("one");
        wal.append(batch(b"two")).expect("two");

        let mut reader = WalReader::open(&config.directory).expect("reader");
        let first = reader.next_record().expect("first").expect("present");
        let mut checkpoint = WalCheckpoint::load(&config.directory).expect("load");
        checkpoint.commit(first.next_cursor).expect("commit");

        let checkpoint = WalCheckpoint::load(&config.directory).expect("reload");
        let mut resumed =
            WalReader::open_at(&config.directory, checkpoint.cursor()).expect("resume");
        let second = resumed.next_record().expect("second").expect("present");
        assert_eq!(second.frame.payload.as_ref(), b"two");
        assert_eq!(second.frame.sequence, 1);
    }

    #[test]
    fn monotonic_commit_succeeds_and_regression_fails() {
        let (_dir, config) = temp_config();
        let mut wal = Wal::open(config.clone()).expect("open wal");
        wal.append(batch(b"one")).expect("one");
        wal.append(batch(b"two")).expect("two");

        let mut reader = WalReader::open(&config.directory).expect("reader");
        let first = reader.next_record().expect("first").expect("present");
        let second = reader.next_record().expect("second").expect("present");

        let mut checkpoint = WalCheckpoint::load(&config.directory).expect("load");
        checkpoint.commit(first.next_cursor).expect("first commit");
        checkpoint
            .commit(first.next_cursor)
            .expect("idempotent commit");
        checkpoint.commit(second.next_cursor).expect("advance");
        let error = checkpoint
            .commit(first.next_cursor)
            .expect_err("regression");
        assert!(matches!(
            error,
            WalError::CheckpointRegression {
                committed: 2,
                attempted: 1
            }
        ));
        assert_eq!(checkpoint.cursor(), second.next_cursor);
    }

    #[test]
    fn every_truncated_prefix_of_a_checkpoint_is_rejected() {
        let (_dir, config) = temp_config();
        let mut wal = Wal::open(config.clone()).expect("open wal");
        wal.append(batch(b"one")).expect("one");
        let mut reader = WalReader::open(&config.directory).expect("reader");
        let first = reader.next_record().expect("first").expect("present");
        let mut checkpoint = WalCheckpoint::load(&config.directory).expect("load");
        checkpoint.commit(first.next_cursor).expect("commit");
        let intact =
            fs::read(lane_directory(&config.directory).join(CHECKPOINT_NAME)).expect("read");

        for len in 0..intact.len() {
            write_checkpoint_bytes(&config.directory, &intact[..len]);
            assert!(
                matches!(
                    WalCheckpoint::load(&config.directory),
                    Err(WalError::InvalidCheckpoint(_))
                ),
                "len {len}"
            );
        }
    }

    #[test]
    fn leftover_tmp_is_ignored_and_complete_file_is_used() {
        let (_dir, config) = temp_config();
        let mut wal = Wal::open(config.clone()).expect("open wal");
        wal.append(batch(b"one")).expect("one");
        wal.append(batch(b"two")).expect("two");
        let mut reader = WalReader::open(&config.directory).expect("reader");
        let first = reader.next_record().expect("first").expect("present");
        let second = reader.next_record().expect("second").expect("present");

        let lane = lane_directory(&config.directory);
        fs::create_dir_all(&lane).expect("lane");
        fs::write(
            lane.join(CHECKPOINT_TMP_NAME),
            encode_checkpoint(first.next_cursor),
        )
        .expect("tmp only");
        let loaded = WalCheckpoint::load(&config.directory).expect("tmp only");
        assert_eq!(loaded.cursor(), WalCursor::start());

        fs::write(
            lane.join(CHECKPOINT_NAME),
            encode_checkpoint(second.next_cursor),
        )
        .expect("final");
        fs::write(lane.join(CHECKPOINT_TMP_NAME), b"partial").expect("garbage tmp");
        let loaded = WalCheckpoint::load(&config.directory).expect("final wins");
        assert_eq!(loaded.cursor(), second.next_cursor);
    }

    #[test]
    fn stale_physical_hint_to_deleted_segment_falls_back_by_sequence() {
        let frame = u64::try_from(encoded_len(b"one")).unwrap();
        let (_dir, config) = config_with_target(header_offset() + frame + 8);
        let mut wal = Wal::open(config.clone()).expect("open wal");
        wal.append(batch(b"one")).expect("one");
        wal.append(batch(b"two")).expect("two");

        let mut reader = WalReader::open(&config.directory).expect("reader");
        let first = reader.next_record().expect("first").expect("present");
        let second = reader.next_record().expect("second").expect("present");
        assert_eq!(second.frame.sequence, 1);
        assert!(second.next_cursor.segment_id() >= 1);

        let stale = WalCursor::at(
            second.next_cursor.next_sequence(),
            0,
            first.next_cursor.offset(),
        );
        write_checkpoint_bytes(&config.directory, &encode_checkpoint(stale));
        fs::remove_file(lane_directory(&config.directory).join("00000000000000000000.wal"))
            .expect("delete sealed prefix");

        let loaded = WalCheckpoint::load(&config.directory).expect("fallback");
        assert_eq!(loaded.cursor(), second.next_cursor);
    }

    #[test]
    fn physical_mismatch_on_existing_segment_is_fatal() {
        let (_dir, config) = temp_config();
        let mut wal = Wal::open(config.clone()).expect("open wal");
        wal.append(batch(b"one")).expect("one");
        wal.append(batch(b"two")).expect("two");

        let mut reader = WalReader::open(&config.directory).expect("reader");
        let first = reader.next_record().expect("first").expect("present");
        let second = reader.next_record().expect("second").expect("present");

        let mismatched = WalCursor::at(
            second.next_cursor.next_sequence(),
            0,
            first.next_cursor.offset(),
        );
        write_checkpoint_bytes(&config.directory, &encode_checkpoint(mismatched));

        assert!(matches!(
            WalCheckpoint::load(&config.directory),
            Err(WalError::Corrupt("cursor sequence mismatch"))
        ));
    }

    #[test]
    fn checkpoint_older_than_remaining_segments_is_fatal() {
        let frame = u64::try_from(encoded_len(b"one")).unwrap();
        let (_dir, config) = config_with_target(header_offset() + frame + 8);
        let mut wal = Wal::open(config.clone()).expect("open wal");
        wal.append(batch(b"one")).expect("one");
        wal.append(batch(b"two")).expect("two");

        write_checkpoint_bytes(&config.directory, &encode_checkpoint(WalCursor::start()));
        fs::remove_file(lane_directory(&config.directory).join("00000000000000000000.wal"))
            .expect("delete sealed prefix");

        assert!(matches!(
            WalCheckpoint::load(&config.directory),
            Err(WalError::InvalidCheckpoint(
                "checkpoint is older than retained data"
            ))
        ));
    }
}

#[cfg(test)]
mod properties {
    use super::*;
    use crate::{SEGMENT_HEADER_SIZE, WalCursor};
    use proptest::prelude::*;

    fn cursor_strategy() -> impl Strategy<Value = WalCursor> {
        (any::<u64>(), any::<u64>(), any::<u64>()).prop_map(|(sequence, segment, offset)| {
            WalCursor::at(
                sequence,
                segment,
                offset.max(u64::try_from(SEGMENT_HEADER_SIZE).expect("header")),
            )
        })
    }

    proptest! {
        #[test]
        fn encode_decode_round_trip(cursor in cursor_strategy()) {
            let encoded = encode_checkpoint(cursor);
            prop_assert_eq!(encoded.len(), CHECKPOINT_SIZE);
            prop_assert_eq!(decode_checkpoint(&encoded).expect("decode"), cursor);
        }

        #[test]
        fn every_truncated_prefix_fails(cursor in cursor_strategy(), len in 0_usize..CHECKPOINT_SIZE) {
            let encoded = encode_checkpoint(cursor);
            prop_assert!(decode_checkpoint(&encoded[..len]).is_err());
        }

        #[test]
        fn flipping_any_byte_is_rejected(
            cursor in cursor_strategy(),
            index in any::<prop::sample::Index>(),
            xor in 1_u8..=255,
        ) {
            let mut encoded = encode_checkpoint(cursor);
            let i = index.index(encoded.len());
            encoded[i] ^= xor;
            prop_assert!(decode_checkpoint(&encoded).is_err());
        }
    }
}
