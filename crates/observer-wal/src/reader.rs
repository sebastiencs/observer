use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
};

use crate::{
    Frame, FrameError, MAX_PAYLOAD_LEN, MAX_TENANT_LEN, SEGMENT_HEADER_SIZE, WalCursor, WalError,
    WalRecord, decode, encoded_frame_size,
    recovery::{
        FoundSegment, SegmentKind, classify_scan_error, discover_segments, is_torn_tail_at,
        lane_directory,
    },
    segment::decode_header,
};

/// Bounded, synchronous reader over a single WAL lane.
#[derive(Debug)]
pub struct WalReader {
    lane_dir: PathBuf,
    cursor: WalCursor,
    current: Option<CurrentSegment>,
}

#[derive(Debug)]
struct CurrentSegment {
    file: File,
    id: u64,
    kind: SegmentKind,
    header: crate::SegmentHeader,
}

enum FrameRead {
    Record(Frame, usize),
    Tail,
}

impl WalReader {
    pub fn open(directory: impl Into<PathBuf>) -> Result<Self, WalError> {
        Self::open_at(directory, WalCursor::start())
    }

    pub fn open_at(directory: impl Into<PathBuf>, cursor: WalCursor) -> Result<Self, WalError> {
        let directory = directory.into();
        let lane_dir = lane_directory(&directory);
        let discovered = discover_segments(&lane_dir)?;
        if discovered.is_empty() {
            if cursor == WalCursor::start() {
                return Ok(Self {
                    lane_dir,
                    cursor,
                    current: None,
                });
            }
            return Err(WalError::Corrupt("cursor does not resolve to a segment"));
        }

        let Some(found) = discovered
            .iter()
            .find(|segment| segment.id == cursor.segment_id())
        else {
            return Err(WalError::Corrupt("cursor does not resolve to a segment"));
        };
        let mut current = open_found(found)?;
        validate_cursor(&mut current, cursor)?;
        Ok(Self {
            lane_dir,
            cursor,
            current: Some(current),
        })
    }

    /// Scan headers and frames until `next_sequence` sits on a validated
    /// physical boundary, then open there.
    pub fn open_from_sequence(
        directory: impl Into<PathBuf>,
        next_sequence: u64,
    ) -> Result<Self, WalError> {
        if next_sequence == 0 {
            return Self::open(directory);
        }

        let directory = directory.into();
        let lane_dir = lane_directory(&directory);
        let discovered = discover_segments(&lane_dir)?;
        if discovered.is_empty() {
            return Err(WalError::Corrupt("sequence not found"));
        }

        for found in &discovered {
            let mut current = open_found(found)?;
            if next_sequence < current.header.first_sequence {
                return Err(WalError::Corrupt("sequence not found"));
            }

            let file_len = current.file.metadata()?.len();
            let mut offset = header_offset();
            let mut sequence = current.header.first_sequence;
            loop {
                if sequence == next_sequence {
                    let cursor = WalCursor::at(sequence, current.id, offset);
                    return Ok(Self {
                        lane_dir,
                        cursor,
                        current: Some(current),
                    });
                }
                match read_frame(&mut current.file, offset, file_len, current.kind)? {
                    FrameRead::Record(frame, consumed) => {
                        if frame.sequence != sequence {
                            return Err(WalError::Corrupt("sequence discontinuity"));
                        }
                        sequence = sequence
                            .checked_add(1)
                            .ok_or(WalError::Corrupt("sequence overflow"))?;
                        offset = offset
                            .checked_add(
                                u64::try_from(consumed).map_err(|_| FrameError::InvalidLength)?,
                            )
                            .ok_or(WalError::Corrupt("offset overflow"))?;
                    }
                    FrameRead::Tail => break,
                }
            }
        }

        Err(WalError::Corrupt("sequence not found"))
    }

    pub fn next_record(&mut self) -> Result<Option<WalRecord>, WalError> {
        loop {
            self.ensure_current()?;
            let Some(current) = self.current.as_mut() else {
                return Ok(None);
            };

            let file_len = current.file.metadata()?.len();
            match read_frame(
                &mut current.file,
                self.cursor.offset(),
                file_len,
                current.kind,
            )? {
                FrameRead::Record(frame, consumed) => {
                    if frame.sequence != self.cursor.next_sequence() {
                        return Err(WalError::Corrupt("sequence discontinuity"));
                    }
                    let next_offset = self
                        .cursor
                        .offset()
                        .checked_add(
                            u64::try_from(consumed).map_err(|_| FrameError::InvalidLength)?,
                        )
                        .ok_or(WalError::Corrupt("offset overflow"))?;
                    let next_sequence = frame
                        .sequence
                        .checked_add(1)
                        .ok_or(WalError::Corrupt("sequence overflow"))?;
                    let next_cursor =
                        WalCursor::at(next_sequence, self.cursor.segment_id(), next_offset);
                    self.cursor = next_cursor;
                    return Ok(Some(WalRecord { frame, next_cursor }));
                }
                FrameRead::Tail => {
                    if self.follow_next_segment()? {
                        continue;
                    }
                    return Ok(None);
                }
            }
        }
    }

    /// Rediscover segments so a later `next_record` can see a still-growing
    /// `.open` tail or a newly rotated segment.
    pub fn refresh(&mut self) -> Result<(), WalError> {
        discover_segments(&self.lane_dir)?;
        Ok(())
    }

    fn ensure_current(&mut self) -> Result<(), WalError> {
        if self
            .current
            .as_ref()
            .is_some_and(|current| current.id == self.cursor.segment_id())
        {
            return Ok(());
        }

        let discovered = discover_segments(&self.lane_dir)?;
        if discovered.is_empty() {
            self.current = None;
            return Ok(());
        }
        let Some(found) = discovered
            .iter()
            .find(|segment| segment.id == self.cursor.segment_id())
        else {
            if self.cursor == WalCursor::start() {
                self.current = None;
                return Ok(());
            }
            return Err(WalError::Corrupt("cursor does not resolve to a segment"));
        };
        let mut current = open_found(found)?;
        validate_cursor(&mut current, self.cursor)?;
        self.current = Some(current);
        Ok(())
    }

    fn follow_next_segment(&mut self) -> Result<bool, WalError> {
        let current_id = match &self.current {
            Some(current) => current.id,
            None => return Ok(false),
        };
        let next_id = match current_id.checked_add(1) {
            Some(id) => id,
            None => return Ok(false),
        };
        let discovered = discover_segments(&self.lane_dir)?;
        let Some(found) = discovered.iter().find(|segment| segment.id == next_id) else {
            return Ok(false);
        };
        let next = open_found(found)?;
        if next.header.first_sequence != self.cursor.next_sequence() {
            return Err(WalError::Corrupt("sequence discontinuity"));
        }
        self.cursor = WalCursor::at(self.cursor.next_sequence(), next.id, header_offset());
        self.current = Some(next);
        Ok(true)
    }
}

fn header_offset() -> u64 {
    u64::try_from(SEGMENT_HEADER_SIZE).expect("header size")
}

fn max_encoded_frame_len() -> usize {
    encoded_frame_size(MAX_TENANT_LEN, MAX_PAYLOAD_LEN).expect("max frame")
}

fn open_found(segment: &FoundSegment) -> Result<CurrentSegment, WalError> {
    let mut file = File::open(&segment.path)?;
    let file_len = file.metadata()?.len();
    if file_len < header_offset() {
        return Err(WalError::InvalidSegmentHeader("truncated header"));
    }
    let mut header_buf = [0_u8; SEGMENT_HEADER_SIZE];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut header_buf)?;
    let header = decode_header(&header_buf)?;
    if header.segment_id != segment.id {
        return Err(WalError::InvalidSegmentHeader(
            "segment id does not match file",
        ));
    }
    Ok(CurrentSegment {
        file,
        id: segment.id,
        kind: segment.kind,
        header,
    })
}

fn validate_cursor(current: &mut CurrentSegment, cursor: WalCursor) -> Result<(), WalError> {
    if cursor.segment_id() != current.header.segment_id {
        return Err(WalError::Corrupt("cursor segment does not match header"));
    }

    let file_len = current.file.metadata()?.len();
    let mut offset = header_offset();
    let mut sequence = current.header.first_sequence;
    loop {
        if offset == cursor.offset() {
            if sequence == cursor.next_sequence() {
                return Ok(());
            }
            return Err(WalError::Corrupt("cursor sequence mismatch"));
        }
        if offset > cursor.offset() {
            return Err(WalError::Corrupt("cursor offset is not a frame boundary"));
        }
        match read_frame(&mut current.file, offset, file_len, current.kind)? {
            FrameRead::Record(frame, consumed) => {
                if frame.sequence != sequence {
                    return Err(WalError::Corrupt("sequence discontinuity"));
                }
                sequence = sequence
                    .checked_add(1)
                    .ok_or(WalError::Corrupt("sequence overflow"))?;
                offset = offset
                    .checked_add(u64::try_from(consumed).map_err(|_| FrameError::InvalidLength)?)
                    .ok_or(WalError::Corrupt("offset overflow"))?;
            }
            FrameRead::Tail => {
                if offset == cursor.offset() && sequence == cursor.next_sequence() {
                    return Ok(());
                }
                return Err(WalError::Corrupt("cursor does not resolve to a boundary"));
            }
        }
    }
}

fn read_frame(
    file: &mut File,
    offset: u64,
    file_len: u64,
    kind: SegmentKind,
) -> Result<FrameRead, WalError> {
    if offset > file_len {
        return Err(WalError::Corrupt("cursor offset past end of segment"));
    }
    if offset == file_len {
        return Ok(FrameRead::Tail);
    }

    let remaining = file_len - offset;
    let to_read =
        usize::try_from(remaining.min(u64::try_from(max_encoded_frame_len()).expect("max frame")))
            .expect("read length");
    let mut buf = vec![0_u8; to_read];
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut buf)?;

    match decode(&buf) {
        Ok((frame, consumed)) => Ok(FrameRead::Record(frame, consumed)),
        Err(error) => {
            if kind == SegmentKind::Open && is_torn_tail_at(file_len, offset, &buf, &error) {
                return Ok(FrameRead::Tail);
            }
            Err(classify_scan_error(error))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use bytes::Bytes;
    use observer_protocol::{AcceptedBatch, Signal};

    use super::*;
    use crate::{
        FrameError, FrameSignal, Wal, WalConfig, encode,
        recovery::lane_directory,
        segment::{LANE_ID, SegmentHeader, encode_header, sealed_segment_file_name},
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

    fn collect(reader: &mut WalReader) -> Vec<Frame> {
        let mut frames = Vec::new();
        while let Some(record) = reader.next_record().expect("next") {
            frames.push(record.frame);
        }
        frames
    }

    fn payloads(frames: &[Frame]) -> Vec<&[u8]> {
        frames.iter().map(|frame| frame.payload.as_ref()).collect()
    }

    #[test]
    fn empty_directory_yields_no_records() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut reader = WalReader::open(dir.path()).expect("open");
        assert!(reader.next_record().expect("next").is_none());
    }

    #[test]
    fn reads_appended_frames_in_sequence() {
        let (_dir, config) = temp_config();
        let mut wal = Wal::open(config.clone()).expect("open wal");
        wal.append(batch(b"one")).expect("one");
        wal.append(batch(b"two")).expect("two");

        let mut reader = WalReader::open(&config.directory).expect("open reader");
        let frames = collect(&mut reader);
        assert_eq!(payloads(&frames), [b"one".as_slice(), b"two".as_slice()]);
        assert_eq!(frames[0].sequence, 0);
        assert_eq!(frames[1].sequence, 1);
        assert!(reader.next_record().expect("eof").is_none());
    }

    #[test]
    fn reads_across_rotation() {
        let frame = u64::try_from(encoded_len(b"one")).unwrap();
        let header = header_offset();
        let (_dir, config) = config_with_target(header + frame + 8);
        let mut wal = Wal::open(config.clone()).expect("open wal");
        wal.append(batch(b"one")).expect("one");
        wal.append(batch(b"two")).expect("two");
        wal.append(batch(b"three")).expect("three");

        let mut reader = WalReader::open(&config.directory).expect("open reader");
        let frames = collect(&mut reader);
        assert_eq!(
            payloads(&frames),
            [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()]
        );
        assert!(
            frames
                .windows(2)
                .all(|pair| pair[1].sequence == pair[0].sequence + 1)
        );
    }

    #[test]
    fn starts_from_zero_middle_physical_and_eof() {
        let (_dir, config) = temp_config();
        let mut wal = Wal::open(config.clone()).expect("open wal");
        wal.append(batch(b"zero")).expect("zero");
        wal.append(batch(b"one")).expect("one");
        wal.append(batch(b"two")).expect("two");

        let mut from_zero = WalReader::open(&config.directory).expect("zero");
        let first = from_zero.next_record().expect("first").expect("present");
        assert_eq!(first.frame.payload.as_ref(), b"zero");
        let middle_cursor = first.next_cursor;

        let mut from_middle =
            WalReader::open_from_sequence(&config.directory, 1).expect("middle seq");
        assert_eq!(
            collect(&mut from_middle)
                .iter()
                .map(|frame| frame.payload.as_ref())
                .collect::<Vec<_>>(),
            [b"one".as_slice(), b"two".as_slice()]
        );

        let mut from_physical =
            WalReader::open_at(&config.directory, middle_cursor).expect("physical");
        assert_eq!(
            from_physical
                .next_record()
                .expect("physical first")
                .expect("present")
                .frame
                .payload
                .as_ref(),
            b"one"
        );

        let mut from_eof = WalReader::open_from_sequence(&config.directory, 3).expect("eof");
        assert!(from_eof.next_record().expect("eof").is_none());
    }

    #[test]
    fn refresh_sees_later_appends_and_rotation() {
        let frame = u64::try_from(encoded_len(b"one")).unwrap();
        let header = header_offset();
        let (_dir, config) = config_with_target(header + frame + 8);
        let mut wal = Wal::open(config.clone()).expect("open wal");
        wal.append(batch(b"one")).expect("one");

        let mut reader = WalReader::open(&config.directory).expect("open reader");
        assert_eq!(
            reader
                .next_record()
                .expect("first")
                .expect("present")
                .frame
                .payload
                .as_ref(),
            b"one"
        );
        assert!(reader.next_record().expect("caught up").is_none());

        wal.append(batch(b"two")).expect("two");
        reader.refresh().expect("refresh append");
        assert_eq!(
            reader
                .next_record()
                .expect("second")
                .expect("present")
                .frame
                .payload
                .as_ref(),
            b"two"
        );

        wal.append(batch(b"three")).expect("three");
        reader.refresh().expect("refresh rotate");
        assert_eq!(
            reader
                .next_record()
                .expect("third")
                .expect("present")
                .frame
                .payload
                .as_ref(),
            b"three"
        );
    }

    #[test]
    fn torn_open_tail_is_hidden_and_not_truncated() {
        let (_dir, config) = temp_config();
        let path = {
            let mut wal = Wal::open(config.clone()).expect("open wal");
            wal.append(batch(b"one")).expect("one");
            wal.append(batch(b"two")).expect("two");
            wal.path().to_path_buf()
        };
        let mut bytes = fs::read(&path).expect("read");
        bytes.pop();
        fs::write(&path, &bytes).expect("write torn");
        let len_before = fs::metadata(&path).expect("meta").len();

        let mut reader = WalReader::open(&config.directory).expect("open reader");
        let frames = collect(&mut reader);
        assert_eq!(payloads(&frames), [b"one".as_slice()]);
        assert_eq!(fs::metadata(&path).expect("meta").len(), len_before);
    }

    #[test]
    fn sealed_incomplete_tail_is_fatal() {
        let (_dir, config) = temp_config();
        let path = {
            let mut wal = Wal::open(config.clone()).expect("open wal");
            wal.append(batch(b"one")).expect("one");
            wal.path().to_path_buf()
        };
        let mut bytes = fs::read(&path).expect("read");
        bytes.pop();
        let sealed = lane_directory(&config.directory).join(sealed_segment_file_name(0));
        fs::rename(&path, &sealed).expect("seal");
        fs::write(&sealed, &bytes).expect("write torn sealed");

        let error = WalReader::open(&config.directory)
            .and_then(|mut reader| reader.next_record().map(|_| ()))
            .expect_err("sealed incomplete");
        assert!(matches!(error, WalError::IncompleteSegment));
    }

    #[test]
    fn mid_segment_crc_corruption_is_fatal() {
        let (_dir, config) = temp_config();
        let path = {
            let mut wal = Wal::open(config.clone()).expect("open wal");
            wal.append(batch(b"one")).expect("one");
            wal.append(batch(b"two")).expect("two");
            wal.path().to_path_buf()
        };
        let mut bytes = fs::read(&path).expect("read");
        let first = encoded_len(b"one");
        bytes[SEGMENT_HEADER_SIZE + first - 1] ^= 0xff;
        fs::write(&path, &bytes).expect("corrupt");

        let error = WalReader::open(&config.directory)
            .and_then(|mut reader| reader.next_record().map(|_| ()))
            .expect_err("crc");
        assert!(matches!(
            error,
            WalError::Frame(FrameError::ChecksumMismatch)
        ));
    }

    #[test]
    fn unsupported_frame_version_is_fatal() {
        let (_dir, config) = temp_config();
        let path = {
            let mut wal = Wal::open(config.clone()).expect("open wal");
            wal.append(batch(b"one")).expect("one");
            wal.path().to_path_buf()
        };
        let mut bytes = fs::read(&path).expect("read");
        bytes[SEGMENT_HEADER_SIZE + 4] = 9;
        let crc_start = bytes.len() - 4;
        let crc = crc32c::crc32c(&bytes[SEGMENT_HEADER_SIZE + 4..crc_start]);
        bytes[crc_start..].copy_from_slice(&crc.to_le_bytes());
        fs::write(&path, &bytes).expect("write");

        let error = WalReader::open(&config.directory)
            .and_then(|mut reader| reader.next_record().map(|_| ()))
            .expect_err("version");
        assert!(matches!(
            error,
            WalError::Corrupt("unsupported frame version")
        ));
    }

    #[test]
    fn sequence_gap_is_fatal() {
        let (_dir, config) = temp_config();
        let path = {
            let mut wal = Wal::open(config.clone()).expect("open wal");
            wal.append(batch(b"one")).expect("one");
            wal.append(batch(b"two")).expect("two");
            wal.path().to_path_buf()
        };
        let mut bytes = fs::read(&path).expect("read");
        let first = encoded_len(b"one");
        let seq_at = SEGMENT_HEADER_SIZE + first + 8;
        bytes[seq_at..seq_at + 8].copy_from_slice(&5_u64.to_le_bytes());
        let frame_start = SEGMENT_HEADER_SIZE + first;
        let crc_start = bytes.len() - 4;
        let crc = crc32c::crc32c(&bytes[frame_start + 4..crc_start]);
        bytes[crc_start..].copy_from_slice(&crc.to_le_bytes());
        fs::write(&path, &bytes).expect("write");

        let error = WalReader::open(&config.directory)
            .and_then(|mut reader| {
                reader.next_record()?;
                reader.next_record().map(|_| ())
            })
            .expect_err("gap");
        assert!(matches!(error, WalError::Corrupt("sequence discontinuity")));
    }

    #[test]
    fn invalid_physical_cursor_is_fatal() {
        let (_dir, config) = temp_config();
        let mut wal = Wal::open(config.clone()).expect("open wal");
        wal.append(batch(b"one")).expect("one");

        let error = WalReader::open_at(&config.directory, WalCursor::at(0, 0, header_offset() + 1))
            .expect_err("bad cursor");
        assert!(matches!(error, WalError::Corrupt(_)));
    }

    #[test]
    fn open_at_rejects_sequence_mismatch_on_existing_segment() {
        let (_dir, config) = temp_config();
        let mut wal = Wal::open(config.clone()).expect("open wal");
        wal.append(batch(b"one")).expect("one");

        let error = WalReader::open_at(&config.directory, WalCursor::at(3, 0, header_offset()))
            .expect_err("seq mismatch");
        assert!(matches!(
            error,
            WalError::Corrupt("cursor sequence mismatch")
        ));
    }

    #[test]
    fn header_ids_must_match_filename() {
        let (_dir, config) = temp_config();
        let wal = Wal::open(config.clone()).expect("open wal");
        let path = wal.path().to_path_buf();
        drop(wal);
        let header = encode_header(&SegmentHeader {
            lane_id: LANE_ID,
            segment_id: 7,
            first_sequence: 0,
            created_at_unix_nanos: 1,
        });
        fs::write(&path, header).expect("write");

        assert!(matches!(
            WalReader::open(&config.directory),
            Err(WalError::InvalidSegmentHeader(
                "segment id does not match file"
            ))
        ));
    }
}

#[cfg(test)]
mod properties {
    use bytes::Bytes;
    use observer_protocol::{AcceptedBatch, Signal};
    use proptest::prelude::*;

    use super::*;
    use crate::{FrameSignal, Wal, WalConfig, encode};

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

    proptest! {
        #[test]
        fn appended_batches_are_read_exactly_once_in_sequence(
            payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..24), 1..8),
            rotate in any::<bool>(),
        ) {
            let dir = tempfile::tempdir().expect("tempdir");
            let frame = u64::try_from(encoded_len(b"x")).unwrap();
            let target = if rotate {
                header_offset() + frame + 8
            } else {
                256 * 1024 * 1024
            };
            let config = WalConfig {
                directory: dir.path().to_path_buf(),
                max_entry_bytes: 1024 * 1024,
                target_segment_bytes: target,
            };
            let mut wal = Wal::open(config.clone()).expect("open wal");
            for payload in &payloads {
                wal.append(batch(payload)).expect("append");
            }

            let mut reader = WalReader::open(&config.directory).expect("open reader");
            let mut previous: Option<WalCursor> = None;
            for (index, payload) in payloads.iter().enumerate() {
                let record = reader.next_record().expect("next").expect("present");
                prop_assert_eq!(record.frame.sequence, index as u64);
                prop_assert_eq!(record.frame.payload.as_ref(), payload.as_slice());
                prop_assert_eq!(record.next_cursor.next_sequence(), index as u64 + 1);
                if let Some(previous) = previous {
                    prop_assert!(
                        record.next_cursor.next_sequence() > previous.next_sequence()
                    );
                    prop_assert!(
                        record.next_cursor.segment_id() > previous.segment_id()
                            || (record.next_cursor.segment_id() == previous.segment_id()
                                && record.next_cursor.offset() > previous.offset())
                    );
                }
                previous = Some(record.next_cursor);
                prop_assert!(
                    encoded_frame_size(
                        record.frame.tenant_id.len(),
                        record.frame.payload.len()
                    )
                    .expect("size")
                        <= max_encoded_frame_len()
                );
            }
            prop_assert!(reader.next_record().expect("eof").is_none());
        }
    }
}
