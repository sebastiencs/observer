use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::{
    FrameError, WalError, decode,
    frame::LENGTH_SIZE,
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
    pub path: PathBuf,
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

pub(crate) fn discover_segments(lane_dir: &Path) -> Result<Vec<FoundSegment>, WalError> {
    if !lane_dir.exists() {
        return Ok(Vec::new());
    }

    let mut found = Vec::new();
    for entry in fs::read_dir(lane_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            return Err(WalError::InvalidSegmentHeader("non-utf8 segment file name"));
        };
        let Some(segment) = parse_segment_name(name) else {
            continue;
        };
        found.push(FoundSegment {
            id: segment.0,
            path,
            kind: segment.1,
        });
    }

    found.sort_by_key(|segment| segment.id);
    validate_discovered(&found)?;
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

fn validate_discovered(found: &[FoundSegment]) -> Result<(), WalError> {
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
        } else if segment.id != 0 {
            return Err(WalError::Corrupt("missing or out-of-order segment id"));
        }
        if segment.kind == SegmentKind::Open {
            open_count += 1;
        }
        previous = Some(segment.id);
    }
    if open_count > 1 {
        return Err(WalError::Corrupt("multiple active segments"));
    }
    if !found.is_empty()
        && !found
            .iter()
            .any(|segment| segment.kind == SegmentKind::Open)
    {
        return Err(WalError::Corrupt("missing active segment"));
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

fn classify_scan_error(error: FrameError) -> WalError {
    match error {
        FrameError::UnsupportedVersion { .. } => WalError::Corrupt("unsupported frame version"),
        FrameError::Incomplete => WalError::IncompleteSegment,
        other => WalError::Frame(other),
    }
}

fn is_torn_tail(bytes: &[u8], offset: usize, error: &FrameError) -> bool {
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

#[cfg(test)]
mod tests {
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
