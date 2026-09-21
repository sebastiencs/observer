use std::{
    io,
    path::{Path, PathBuf},
};

use crate::{
    FrameError, WalError, decode,
    frame::LENGTH_SIZE,
    lane_io::LaneIo,
    segment::{LANE_DIR_NAME, SEGMENT_HEADER_SIZE, decode_header},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SegmentKind {
    Open,
    Sealed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FoundSegment {
    pub id: u64,
    pub name: String,
    pub kind: SegmentKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecoveredScan {
    pub next_sequence: u64,
    pub valid_end: u64,
    pub truncated: bool,
}

pub(crate) fn lane_directory(root: &Path) -> PathBuf {
    root.join(LANE_DIR_NAME)
}

#[cfg(test)]
pub(crate) fn discover_segments(lane_dir: &Path) -> Result<Vec<FoundSegment>, WalError> {
    let io = crate::lane_io::StdLaneIo::new(lane_dir);
    let found = list_segments(&io)?;
    validate_discovered(&found)?;
    Ok(found)
}

pub(crate) fn list_segments(io: &dyn LaneIo) -> Result<Vec<FoundSegment>, WalError> {
    let names = io.list_files().map_err(|error| {
        if error.kind() == io::ErrorKind::InvalidData {
            WalError::InvalidSegmentHeader("non-utf8 segment file name")
        } else {
            WalError::Io(error)
        }
    })?;

    let mut found = Vec::new();
    for name in names {
        let Some(segment) = parse_segment_name(&name) else {
            continue;
        };
        found.push(FoundSegment {
            id: segment.0,
            name,
            kind: segment.1,
        });
    }

    found.sort_by_key(|segment| segment.id);
    Ok(found)
}

fn parse_segment_name(name: &str) -> Option<(u64, SegmentKind)> {
    let (id, kind) = if let Some(id) = name.strip_suffix(".open") {
        (id, SegmentKind::Open)
    } else {
        (name.strip_suffix(".wal")?, SegmentKind::Sealed)
    };
    if id.len() != 20 || !id.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    id.parse().ok().map(|id| (id, kind))
}

#[cfg(test)]
fn validate_discovered(found: &[FoundSegment]) -> Result<(), WalError> {
    if found.first().is_some_and(|segment| segment.id != 0) {
        return Err(WalError::Corrupt("missing or out-of-order segment id"));
    }
    validate_contiguous_suffix(found)
}

pub(crate) fn validate_contiguous_suffix(found: &[FoundSegment]) -> Result<(), WalError> {
    let mut previous: Option<u64> = None;
    let mut open_count = 0;
    for segment in found {
        if let Some(previous) = previous {
            if segment.id == previous {
                return Err(WalError::Corrupt("duplicate segment id"));
            }
            if segment.id != previous + 1 {
                return Err(WalError::Corrupt("missing or out-of-order segment id"));
            }
        }
        if segment.kind == SegmentKind::Open {
            open_count += 1;
        }
        previous = Some(segment.id);
    }
    if open_count > 1 {
        return Err(WalError::Corrupt("multiple active segments"));
    }
    if open_count == 1
        && found
            .last()
            .is_some_and(|segment| segment.kind != SegmentKind::Open)
    {
        return Err(WalError::Corrupt("active segment is not last"));
    }
    Ok(())
}

pub(crate) fn scan_open_segment(
    bytes: &[u8],
    first_sequence: u64,
) -> Result<RecoveredScan, WalError> {
    if bytes.len() < SEGMENT_HEADER_SIZE {
        return Err(WalError::InvalidSegmentHeader("truncated header"));
    }
    decode_header(bytes)?;
    scan_frames(bytes, first_sequence, true)
}

pub(crate) fn scan_sealed_segment(
    bytes: &[u8],
    first_sequence: u64,
) -> Result<RecoveredScan, WalError> {
    if bytes.len() < SEGMENT_HEADER_SIZE {
        return Err(WalError::InvalidSegmentHeader("truncated header"));
    }
    decode_header(bytes)?;
    let recovered = scan_frames(bytes, first_sequence, false)?;
    if recovered.truncated {
        return Err(WalError::IncompleteSegment);
    }
    Ok(recovered)
}

fn scan_frames(
    bytes: &[u8],
    first_sequence: u64,
    allow_tail_truncate: bool,
) -> Result<RecoveredScan, WalError> {
    let mut offset = SEGMENT_HEADER_SIZE;
    let mut next_sequence = first_sequence;
    while offset < bytes.len() {
        match decode(&bytes[offset..]) {
            Ok((frame, consumed)) => {
                if frame.sequence != next_sequence {
                    return Err(WalError::Corrupt("sequence discontinuity"));
                }
                next_sequence = next_sequence
                    .checked_add(1)
                    .ok_or(WalError::Corrupt("sequence overflow"))?;
                offset = offset
                    .checked_add(consumed)
                    .ok_or(WalError::Corrupt("offset overflow"))?;
            }
            Err(error) => {
                if allow_tail_truncate && is_torn_tail(bytes, offset, &error) {
                    return Ok(RecoveredScan {
                        next_sequence,
                        valid_end: u64::try_from(offset).expect("offset fits u64"),
                        truncated: true,
                    });
                }
                return Err(classify_scan_error(error));
            }
        }
    }
    Ok(RecoveredScan {
        next_sequence,
        valid_end: u64::try_from(offset).expect("offset fits u64"),
        truncated: false,
    })
}

pub(crate) fn classify_scan_error(error: FrameError) -> WalError {
    match error {
        FrameError::UnsupportedVersion { .. } => WalError::Corrupt("unsupported frame version"),
        FrameError::Incomplete => WalError::IncompleteSegment,
        other => WalError::Frame(other),
    }
}

pub(crate) fn is_torn_tail(bytes: &[u8], offset: usize, error: &FrameError) -> bool {
    match error {
        FrameError::Incomplete => true,
        FrameError::ChecksumMismatch => {
            declared_frame_end(bytes, offset).is_some_and(|end| end == bytes.len())
        }
        FrameError::LengthMismatch { declared, .. } => {
            frame_end(offset, *declared).is_none_or(|end| end >= bytes.len())
        }
        FrameError::InvalidLength
        | FrameError::TenantTooLong { .. }
        | FrameError::PayloadTooLong { .. } => {
            declared_frame_end(bytes, offset).is_none_or(|end| end >= bytes.len())
        }
        FrameError::UnsupportedVersion { .. }
        | FrameError::UnknownSignal { .. }
        | FrameError::UnknownFlags { .. }
        | FrameError::ReservedMustBeZero { .. }
        | FrameError::InvalidTenantUtf8 => false,
    }
}

fn frame_end(offset: usize, total_length: u32) -> Option<usize> {
    offset
        .checked_add(LENGTH_SIZE)?
        .checked_add(usize::try_from(total_length).ok()?)
}

fn declared_frame_end(bytes: &[u8], offset: usize) -> Option<usize> {
    let slice = bytes.get(offset..offset + LENGTH_SIZE)?;
    frame_end(
        offset,
        u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]),
    )
}

/// Like [`is_torn_tail`], but `prefix` is only the bytes at `offset` and
/// `file_len` is the physical end of the segment.
pub(crate) fn is_torn_tail_at(
    file_len: u64,
    offset: u64,
    prefix: &[u8],
    error: &FrameError,
) -> bool {
    let prefix_end = match u64::try_from(prefix.len())
        .ok()
        .and_then(|len| offset.checked_add(len))
    {
        Some(end) => end,
        None => return false,
    };
    match error {
        FrameError::Incomplete => prefix_end == file_len,
        FrameError::ChecksumMismatch => declared_frame_end(prefix, 0)
            .and_then(|end| offset.checked_add(u64::try_from(end).ok()?))
            .is_some_and(|end| end == file_len),
        FrameError::LengthMismatch { declared, .. } => frame_end(0, *declared)
            .and_then(|end| offset.checked_add(u64::try_from(end).ok()?))
            .is_none_or(|end| end >= file_len),
        FrameError::InvalidLength
        | FrameError::TenantTooLong { .. }
        | FrameError::PayloadTooLong { .. } => declared_frame_end(prefix, 0)
            .and_then(|end| offset.checked_add(u64::try_from(end).ok()?))
            .is_none_or(|end| end >= file_len),
        FrameError::UnsupportedVersion { .. }
        | FrameError::UnknownSignal { .. }
        | FrameError::UnknownFlags { .. }
        | FrameError::ReservedMustBeZero { .. }
        | FrameError::InvalidTenantUtf8 => false,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::{
        Frame, FrameSignal, encode,
        segment::{LANE_ID, SEGMENT_FORMAT_VERSION, SegmentHeader, encode_header},
    };
    use bytes::Bytes;

    fn header_and(frames: &[Frame]) -> Vec<u8> {
        let mut bytes = encode_header(&SegmentHeader {
            lane_id: LANE_ID,
            segment_id: 0,
            first_sequence: 0,
            created_at_unix_nanos: 1,
        })
        .to_vec();
        for frame in frames {
            bytes.extend_from_slice(&encode(frame).expect("encode"));
        }
        bytes
    }

    fn sample(sequence: u64) -> Frame {
        Frame {
            sequence,
            signal: FrameSignal::Logs,
            received_at_unix_nanos: 1,
            tenant_id: "tenant-a".to_owned(),
            payload: Bytes::from_static(b"payload"),
        }
    }

    #[test]
    fn discover_allows_sealed_only_after_interrupted_rotation() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(dir.path().join("00000000000000000000.wal"), []).expect("write");
        let found = discover_segments(dir.path()).expect("discover");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].kind, SegmentKind::Sealed);
        assert_eq!(found[0].id, 0);
    }

    #[test]
    fn discover_rejects_multiple_open_segments() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(dir.path().join("00000000000000000000.open"), []).expect("write");
        fs::write(dir.path().join("00000000000000000001.open"), []).expect("write");
        assert!(matches!(
            discover_segments(dir.path()),
            Err(WalError::Corrupt("multiple active segments"))
        ));
    }

    #[test]
    fn discover_rejects_open_before_sealed() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(dir.path().join("00000000000000000000.open"), []).expect("write");
        fs::write(dir.path().join("00000000000000000001.wal"), []).expect("write");
        assert!(matches!(
            discover_segments(dir.path()),
            Err(WalError::Corrupt("active segment is not last"))
        ));
    }

    #[test]
    fn parse_valid_names() {
        assert_eq!(
            parse_segment_name("00000000000000000000.open"),
            Some((0, SegmentKind::Open))
        );
        assert_eq!(
            parse_segment_name("00000000000000000003.wal"),
            Some((3, SegmentKind::Sealed))
        );
        assert_eq!(parse_segment_name("notes.txt"), None);
    }

    #[test]
    fn clean_scan() {
        let bytes = header_and(&[sample(0), sample(1)]);
        let recovered = scan_open_segment(&bytes, 0).expect("scan");
        assert_eq!(recovered.next_sequence, 2);
        assert_eq!(recovered.valid_end, u64::try_from(bytes.len()).unwrap());
        assert!(!recovered.truncated);
    }

    #[test]
    fn torn_tail_truncates() {
        let mut bytes = header_and(&[sample(0), sample(1)]);
        bytes.pop();
        let recovered = scan_open_segment(&bytes, 0).expect("scan");
        assert_eq!(recovered.next_sequence, 1);
        assert!(recovered.truncated);
        let first = encode(&sample(0)).unwrap().len();
        assert_eq!(
            recovered.valid_end,
            u64::try_from(SEGMENT_HEADER_SIZE + first).unwrap()
        );
    }

    #[test]
    fn checksum_mismatch_at_eof_truncates() {
        let mut bytes = header_and(&[sample(0), sample(1)]);
        *bytes.last_mut().unwrap() ^= 0xff;
        let recovered = scan_open_segment(&bytes, 0).expect("scan");
        assert_eq!(recovered.next_sequence, 1);
        assert!(recovered.truncated);
    }

    #[test]
    fn checksum_mismatch_before_tail_is_fatal() {
        let first = encode(&sample(0)).unwrap();
        let second = encode(&sample(1)).unwrap();
        let mut bytes = header_and(&[sample(0), sample(1), sample(2)]);
        let flip = SEGMENT_HEADER_SIZE + first.len() + second.len() - 1;
        bytes[flip] ^= 0xff;
        assert!(matches!(
            scan_open_segment(&bytes, 0),
            Err(WalError::Frame(FrameError::ChecksumMismatch))
        ));
    }

    #[test]
    fn sealed_incomplete_is_fatal() {
        let mut bytes = header_and(&[sample(0)]);
        bytes.pop();
        assert!(matches!(
            scan_sealed_segment(&bytes, 0),
            Err(WalError::IncompleteSegment)
        ));
    }

    #[test]
    fn unsupported_frame_version_is_fatal() {
        let mut bytes = header_and(&[sample(0)]);
        bytes[SEGMENT_HEADER_SIZE + 4] = 9;
        let crc_start = bytes.len() - 4;
        let crc = crc32c::crc32c(&bytes[SEGMENT_HEADER_SIZE + 4..crc_start]);
        bytes[crc_start..].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            scan_open_segment(&bytes, 0),
            Err(WalError::Corrupt("unsupported frame version"))
        ));
    }

    #[test]
    fn unsupported_segment_version_is_fatal() {
        let mut bytes = header_and(&[]);
        bytes[8..10].copy_from_slice(&(SEGMENT_FORMAT_VERSION + 1).to_le_bytes());
        let crc = crc32c::crc32c(&bytes[..40]);
        bytes[40..44].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            scan_open_segment(&bytes, 0),
            Err(WalError::UnsupportedSegmentVersion { version: 2 })
        ));
    }
}

#[cfg(test)]
mod properties {
    use super::*;
    use crate::{
        Frame, FrameSignal, encode,
        segment::{LANE_ID, SegmentHeader, encode_header},
    };
    use bytes::Bytes;
    use proptest::prelude::*;

    fn frame_body() -> impl Strategy<Value = Frame> {
        (
            prop::collection::vec(any::<u8>(), 0..24),
            "\\PC{0,16}",
            any::<u64>(),
        )
            .prop_map(|(payload, tenant_id, received_at_unix_nanos)| Frame {
                sequence: 0,
                signal: FrameSignal::Logs,
                received_at_unix_nanos,
                tenant_id,
                payload: Bytes::from(payload),
            })
    }

    fn frames_strategy() -> impl Strategy<Value = Vec<Frame>> {
        (0_u64..1024, prop::collection::vec(frame_body(), 1..5)).prop_map(|(first, mut frames)| {
            for (index, frame) in frames.iter_mut().enumerate() {
                frame.sequence = first + index as u64;
            }
            frames
        })
    }

    fn header_and(frames: &[Frame]) -> Vec<u8> {
        let first_sequence = frames.first().map(|frame| frame.sequence).unwrap_or(0);
        let mut bytes = encode_header(&SegmentHeader {
            lane_id: LANE_ID,
            segment_id: 0,
            first_sequence,
            created_at_unix_nanos: 1,
        })
        .to_vec();
        for frame in frames {
            bytes.extend_from_slice(&encode(frame).expect("encode"));
        }
        bytes
    }

    proptest! {
        #[test]
        fn clean_scan_recovers_every_frame(frames in frames_strategy()) {
            let first = frames[0].sequence;
            let bytes = header_and(&frames);
            let recovered = scan_open_segment(&bytes, first).expect("scan");
            prop_assert!(!recovered.truncated);
            prop_assert_eq!(recovered.next_sequence, first + frames.len() as u64);
            prop_assert_eq!(
                recovered.valid_end,
                u64::try_from(bytes.len()).expect("len")
            );
            prop_assert_eq!(
                scan_sealed_segment(&bytes, first).expect("sealed"),
                recovered
            );
        }

        #[test]
        fn torn_open_tail_truncates_to_last_complete_frame(
            frames in frames_strategy(),
            drop in 1_usize..=32,
        ) {
            let first = frames[0].sequence;
            let complete = header_and(&frames);
            let last_len = encode(frames.last().expect("frame")).expect("encode").len();
            let drop = drop.min(last_len);
            let mut bytes = complete.clone();
            bytes.truncate(bytes.len() - drop);
            let recovered = scan_open_segment(&bytes, first).expect("open torn tail");
            prop_assert!(recovered.truncated);
            prop_assert_eq!(
                recovered.next_sequence,
                first + (frames.len() as u64).saturating_sub(1)
            );
            prop_assert_eq!(
                recovered.valid_end,
                u64::try_from(complete.len() - last_len).expect("len")
            );
            prop_assert!(matches!(
                scan_sealed_segment(&bytes, first),
                Err(WalError::IncompleteSegment)
            ));
        }

        #[test]
        fn parse_round_trips_generated_segment_names(id in any::<u64>(), open in any::<bool>()) {
            let name = if open {
                format!("{id:020}.open")
            } else {
                format!("{id:020}.wal")
            };
            let kind = if open {
                SegmentKind::Open
            } else {
                SegmentKind::Sealed
            };
            prop_assert_eq!(parse_segment_name(&name), Some((id, kind)));
        }

        #[test]
        fn parse_rejects_names_that_are_not_segment_files(name in "\\PC{0,40}") {
            if let Some((id, kind)) = parse_segment_name(&name) {
                let expected = match kind {
                    SegmentKind::Open => format!("{id:020}.open"),
                    SegmentKind::Sealed => format!("{id:020}.wal"),
                };
                prop_assert_eq!(name, expected);
            }
        }
    }
}
