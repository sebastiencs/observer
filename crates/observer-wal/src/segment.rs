use crate::WalError;

pub(crate) const SEGMENT_MAGIC: &[u8; 8] = b"OBS-WAL1";
pub(crate) const SEGMENT_FORMAT_VERSION: u16 = 1;
pub const SEGMENT_HEADER_SIZE: usize = 44;
pub(crate) const LANE_ID: u32 = 0;
pub(crate) const LANE_DIR_NAME: &str = "lane-0000";

/// Parsed checksummed segment header.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentHeader {
    pub lane_id: u32,
    pub segment_id: u64,
    pub first_sequence: u64,
    pub created_at_unix_nanos: u64,
}

pub(crate) fn encode_header(header: &SegmentHeader) -> [u8; SEGMENT_HEADER_SIZE] {
    let mut buf = [0_u8; SEGMENT_HEADER_SIZE];
    buf[0..8].copy_from_slice(SEGMENT_MAGIC);
    buf[8..10].copy_from_slice(&SEGMENT_FORMAT_VERSION.to_le_bytes());
    buf[10..12].copy_from_slice(
        &u16::try_from(SEGMENT_HEADER_SIZE)
            .expect("header fits u16")
            .to_le_bytes(),
    );
    buf[12..16].copy_from_slice(&header.lane_id.to_le_bytes());
    buf[16..24].copy_from_slice(&header.segment_id.to_le_bytes());
    buf[24..32].copy_from_slice(&header.first_sequence.to_le_bytes());
    buf[32..40].copy_from_slice(&header.created_at_unix_nanos.to_le_bytes());
    let crc = crc32c::crc32c(&buf[..40]);
    buf[40..44].copy_from_slice(&crc.to_le_bytes());
    buf
}

pub(crate) fn decode_header(data: &[u8]) -> Result<SegmentHeader, WalError> {
    if data.len() < SEGMENT_HEADER_SIZE {
        return Err(WalError::InvalidSegmentHeader("truncated header"));
    }

    if &data[0..8] != SEGMENT_MAGIC {
        return Err(WalError::InvalidSegmentHeader("unrecognized magic"));
    }

    let version = u16::from_le_bytes([data[8], data[9]]);
    if version != SEGMENT_FORMAT_VERSION {
        return Err(WalError::UnsupportedSegmentVersion { version });
    }

    let header_length = u16::from_le_bytes([data[10], data[11]]);
    if usize::from(header_length) != SEGMENT_HEADER_SIZE {
        return Err(WalError::InvalidSegmentHeader("unexpected header length"));
    }

    let stored_crc = u32::from_le_bytes([data[40], data[41], data[42], data[43]]);
    let computed_crc = crc32c::crc32c(&data[..40]);
    if stored_crc != computed_crc {
        return Err(WalError::InvalidSegmentHeader("checksum mismatch"));
    }

    let header = SegmentHeader {
        lane_id: u32::from_le_bytes([data[12], data[13], data[14], data[15]]),
        segment_id: u64::from_le_bytes(data[16..24].try_into().expect("8 bytes")),
        first_sequence: u64::from_le_bytes(data[24..32].try_into().expect("8 bytes")),
        created_at_unix_nanos: u64::from_le_bytes(data[32..40].try_into().expect("8 bytes")),
    };
    if header.lane_id != LANE_ID {
        return Err(WalError::UnexpectedLane {
            lane_id: header.lane_id,
        });
    }
    Ok(header)
}

pub(crate) fn segment_file_name(segment_id: u64) -> String {
    format!("{segment_id:020}.open")
}

pub(crate) fn sealed_segment_file_name(segment_id: u64) -> String {
    format!("{segment_id:020}.wal")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_encode_segment_id_and_kind() {
        assert_eq!(segment_file_name(0), "00000000000000000000.open");
        assert_eq!(sealed_segment_file_name(3), "00000000000000000003.wal");
    }

    #[test]
    fn header_round_trip() {
        let header = SegmentHeader {
            lane_id: LANE_ID,
            segment_id: 7,
            first_sequence: 3,
            created_at_unix_nanos: 99,
        };
        let encoded = encode_header(&header);
        assert_eq!(encoded.len(), SEGMENT_HEADER_SIZE);
        assert_eq!(decode_header(&encoded).expect("decode"), header);
    }

    #[test]
    fn rejects_bad_magic() {
        let mut encoded = encode_header(&SegmentHeader {
            lane_id: LANE_ID,
            segment_id: 0,
            first_sequence: 0,
            created_at_unix_nanos: 1,
        });
        encoded[0] ^= 0xff;
        assert!(matches!(
            decode_header(&encoded),
            Err(WalError::InvalidSegmentHeader("unrecognized magic"))
        ));
    }
}

#[cfg(test)]
mod properties {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn header_round_trip(
            segment_id in any::<u64>(),
            first_sequence in any::<u64>(),
            created_at_unix_nanos in any::<u64>(),
        ) {
            let header = SegmentHeader {
                lane_id: LANE_ID,
                segment_id,
                first_sequence,
                created_at_unix_nanos,
            };
            let encoded = encode_header(&header);
            prop_assert_eq!(encoded.len(), SEGMENT_HEADER_SIZE);
            prop_assert_eq!(decode_header(&encoded).expect("decode"), header);
        }

        #[test]
        fn decode_header_never_panics(data in prop::collection::vec(any::<u8>(), 0..80)) {
            let _ = decode_header(&data);
        }

        #[test]
        fn flipping_any_header_byte_is_rejected(
            segment_id in any::<u64>(),
            first_sequence in any::<u64>(),
            created_at_unix_nanos in any::<u64>(),
            index in any::<prop::sample::Index>(),
            xor in 1_u8..=255,
        ) {
            let header = SegmentHeader {
                lane_id: LANE_ID,
                segment_id,
                first_sequence,
                created_at_unix_nanos,
            };
            let mut encoded = encode_header(&header);
            let i = index.index(encoded.len());
            encoded[i] ^= xor;
            prop_assert!(decode_header(&encoded).is_err());
        }
    }
}
