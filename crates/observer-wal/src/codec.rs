use bytes::Bytes;

use crate::{
    Frame, FrameError, FrameSignal,
    frame::{
        CRC_SIZE, FIXED_HEADER_SIZE, FORMAT_VERSION, LENGTH_SIZE, MAX_PAYLOAD_LEN, MAX_TENANT_LEN,
    },
};

fn read_u16(data: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([data[offset], data[offset + 1]])
}

fn read_u32(data: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ])
}

fn read_u64(data: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ])
}

fn expected_total_length(tenant_len: usize, payload_len: usize) -> Result<u32, FrameError> {
    let without_crc = FIXED_HEADER_SIZE
        .checked_add(tenant_len)
        .and_then(|length| length.checked_add(payload_len))
        .ok_or(FrameError::InvalidLength)?;
    let total = without_crc
        .checked_add(CRC_SIZE)
        .ok_or(FrameError::InvalidLength)?;
    u32::try_from(total).map_err(|_| FrameError::InvalidLength)
}

fn frame_len(total_length: u32) -> Result<usize, FrameError> {
    LENGTH_SIZE
        .checked_add(usize::try_from(total_length).map_err(|_| FrameError::InvalidLength)?)
        .ok_or(FrameError::InvalidLength)
}

/// Encoded size of a frame with the given tenant and payload lengths.
pub fn encoded_frame_size(tenant_len: usize, payload_len: usize) -> Result<usize, FrameError> {
    frame_len(expected_total_length(tenant_len, payload_len)?)
}

/// Encode `frame` as a complete checksummed WAL record.
pub fn encode(frame: &Frame) -> Result<Vec<u8>, FrameError> {
    let tenant = frame.tenant_id.as_bytes();
    if tenant.len() > MAX_TENANT_LEN {
        return Err(FrameError::TenantTooLong {
            length: tenant.len(),
        });
    }
    if frame.payload.len() > MAX_PAYLOAD_LEN {
        return Err(FrameError::PayloadTooLong {
            length: frame.payload.len(),
        });
    }

    let tenant_len = u16::try_from(tenant.len()).map_err(|_| FrameError::InvalidLength)?;
    let payload_len = u32::try_from(frame.payload.len()).map_err(|_| FrameError::InvalidLength)?;
    let total_length = expected_total_length(tenant.len(), frame.payload.len())?;
    let mut buf = Vec::new();
    buf.try_reserve(frame_len(total_length)?)
        .map_err(|_| FrameError::InvalidLength)?;

    buf.extend_from_slice(&total_length.to_le_bytes());
    buf.push(FORMAT_VERSION);
    buf.push(frame.signal.to_wire());
    buf.extend_from_slice(&0u16.to_le_bytes());
    buf.extend_from_slice(&frame.sequence.to_le_bytes());
    buf.extend_from_slice(&frame.received_at_unix_nanos.to_le_bytes());
    buf.extend_from_slice(&tenant_len.to_le_bytes());
    buf.extend_from_slice(&0u16.to_le_bytes());
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.extend_from_slice(tenant);
    buf.extend_from_slice(&frame.payload);

    let crc = crc32c::crc32c(&buf[LENGTH_SIZE..]);
    buf.extend_from_slice(&crc.to_le_bytes());
    Ok(buf)
}

/// Decode one frame from the front of `data`.
///
/// On success, returns the frame and the number of bytes consumed. Trailing
/// bytes are left unconsumed so callers can scan sequentially.
///
/// [`FrameError::Incomplete`] means `data` ends before a length-consistent
/// frame. Other errors mean the available header or complete frame is corrupt.
pub fn decode(data: &[u8]) -> Result<(Frame, usize), FrameError> {
    if data.len() < LENGTH_SIZE {
        return Err(FrameError::Incomplete);
    }

    let total_length = read_u32(data, 0);
    let consumed = frame_len(total_length)?;

    if data.len() < LENGTH_SIZE + FIXED_HEADER_SIZE {
        return Err(FrameError::Incomplete);
    }

    let header = &data[LENGTH_SIZE..LENGTH_SIZE + FIXED_HEADER_SIZE];
    let tenant_len = usize::from(read_u16(header, 20));
    let reserved = read_u16(header, 22);
    let payload_len = read_u32(header, 24) as usize;

    let expected = expected_total_length(tenant_len, payload_len)?;
    if expected != total_length {
        return Err(FrameError::LengthMismatch {
            declared: total_length,
            expected: u64::from(expected),
        });
    }
    if tenant_len > MAX_TENANT_LEN {
        return Err(FrameError::TenantTooLong { length: tenant_len });
    }
    if payload_len > MAX_PAYLOAD_LEN {
        return Err(FrameError::PayloadTooLong {
            length: payload_len,
        });
    }
    if data.len() < consumed {
        return Err(FrameError::Incomplete);
    }

    let version = header[0];
    if version != FORMAT_VERSION {
        return Err(FrameError::UnsupportedVersion { version });
    }

    let Some(signal) = FrameSignal::from_wire(header[1]) else {
        return Err(FrameError::UnknownSignal { signal: header[1] });
    };

    let flags = read_u16(header, 2);
    if flags != 0 {
        return Err(FrameError::UnknownFlags { flags });
    }
    if reserved != 0 {
        return Err(FrameError::ReservedMustBeZero { reserved });
    }

    let sequence = read_u64(header, 4);
    let received_at_unix_nanos = read_u64(header, 12);
    let tenant_start = LENGTH_SIZE + FIXED_HEADER_SIZE;
    let tenant_end = tenant_start
        .checked_add(tenant_len)
        .ok_or(FrameError::InvalidLength)?;
    let payload_end = tenant_end
        .checked_add(payload_len)
        .ok_or(FrameError::InvalidLength)?;
    let crc_end = payload_end
        .checked_add(CRC_SIZE)
        .ok_or(FrameError::InvalidLength)?;
    if crc_end != consumed {
        return Err(FrameError::InvalidLength);
    }

    let stored_crc = read_u32(data, payload_end);
    let computed_crc = crc32c::crc32c(&data[LENGTH_SIZE..payload_end]);
    if stored_crc != computed_crc {
        return Err(FrameError::ChecksumMismatch);
    }

    let tenant_id = std::str::from_utf8(&data[tenant_start..tenant_end])
        .map_err(|_| FrameError::InvalidTenantUtf8)?
        .to_owned();

    Ok((
        Frame {
            sequence,
            signal,
            received_at_unix_nanos,
            tenant_id,
            payload: Bytes::copy_from_slice(&data[tenant_end..payload_end]),
        },
        consumed,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::SIGNAL_LOGS;

    fn sample_frame() -> Frame {
        Frame {
            sequence: 42,
            signal: FrameSignal::Logs,
            received_at_unix_nanos: 1_700_000_000_000_000_000,
            tenant_id: "tenant-a".to_owned(),
            payload: Bytes::from_static(b"\x0a\x00"),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn encode_parts(
        version: u8,
        signal: u8,
        flags: u16,
        sequence: u64,
        received_at: u64,
        reserved: u16,
        tenant: &[u8],
        payload: &[u8],
        total_length: Option<u32>,
    ) -> Vec<u8> {
        let tenant_len = u16::try_from(tenant.len()).expect("tenant fits u16");
        let payload_len = u32::try_from(payload.len()).expect("payload fits u32");
        let computed = expected_total_length(tenant.len(), payload.len()).expect("length");
        let total_length = total_length.unwrap_or(computed);

        let mut buf = Vec::new();
        buf.extend_from_slice(&total_length.to_le_bytes());
        buf.push(version);
        buf.push(signal);
        buf.extend_from_slice(&flags.to_le_bytes());
        buf.extend_from_slice(&sequence.to_le_bytes());
        buf.extend_from_slice(&received_at.to_le_bytes());
        buf.extend_from_slice(&tenant_len.to_le_bytes());
        buf.extend_from_slice(&reserved.to_le_bytes());
        buf.extend_from_slice(&payload_len.to_le_bytes());
        buf.extend_from_slice(tenant);
        buf.extend_from_slice(payload);
        let crc = crc32c::crc32c(&buf[LENGTH_SIZE..]);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    fn set_crc(buf: &mut [u8]) {
        let payload_end = buf.len() - CRC_SIZE;
        let crc = crc32c::crc32c(&buf[LENGTH_SIZE..payload_end]);
        buf[payload_end..].copy_from_slice(&crc.to_le_bytes());
    }

    #[test]
    fn encoded_frame_size_matches_encode() {
        let frame = sample_frame();
        let encoded = encode(&frame).expect("encode");
        assert_eq!(
            encoded_frame_size(frame.tenant_id.len(), frame.payload.len()).expect("size"),
            encoded.len()
        );
    }

    #[test]
    fn encode_decode_round_trip() {
        let encoded = encode(&sample_frame()).expect("encode");
        let (decoded, consumed) = decode(&encoded).expect("decode");
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, sample_frame());
    }

    #[test]
    fn empty_payload_round_trip() {
        let mut frame = sample_frame();
        frame.payload = Bytes::new();
        let encoded = encode(&frame).expect("encode");
        let (decoded, consumed) = decode(&encoded).expect("decode");
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, frame);
    }

    #[test]
    fn reserved_signal_discriminants_round_trip() {
        for signal in [FrameSignal::Traces, FrameSignal::Metrics] {
            let mut frame = sample_frame();
            frame.signal = signal;
            let encoded = encode(&frame).expect("encode");
            let (decoded, _) = decode(&encoded).expect("decode");
            assert_eq!(decoded.signal, signal);
        }
    }

    #[test]
    fn utf8_tenant_round_trip() {
        let mut frame = sample_frame();
        frame.tenant_id = "组织/tenant".to_owned();
        let encoded = encode(&frame).expect("encode");
        let (decoded, _) = decode(&encoded).expect("decode");
        assert_eq!(decoded.tenant_id, "组织/tenant");
    }

    #[test]
    fn rejects_invalid_tenant_utf8() {
        let mut encoded = encode_parts(
            FORMAT_VERSION,
            SIGNAL_LOGS,
            0,
            1,
            1,
            0,
            &[0xff],
            b"payload",
            None,
        );
        set_crc(&mut encoded);
        assert_eq!(decode(&encoded), Err(FrameError::InvalidTenantUtf8));
    }

    #[test]
    fn accepts_maximum_tenant_length() {
        let mut frame = sample_frame();
        frame.tenant_id = "a".repeat(MAX_TENANT_LEN);
        let encoded = encode(&frame).expect("encode");
        let (decoded, _) = decode(&encoded).expect("decode");
        assert_eq!(decoded.tenant_id.len(), MAX_TENANT_LEN);
    }

    #[test]
    fn rejects_tenant_one_byte_over_limit() {
        let mut frame = sample_frame();
        frame.tenant_id = "a".repeat(MAX_TENANT_LEN + 1);
        assert_eq!(
            encode(&frame),
            Err(FrameError::TenantTooLong {
                length: MAX_TENANT_LEN + 1
            })
        );

        let encoded = encode_parts(
            FORMAT_VERSION,
            SIGNAL_LOGS,
            0,
            1,
            1,
            0,
            &vec![b'a'; MAX_TENANT_LEN + 1],
            b"",
            None,
        );
        assert_eq!(
            decode(&encoded),
            Err(FrameError::TenantTooLong {
                length: MAX_TENANT_LEN + 1
            })
        );
    }

    #[test]
    fn rejects_payload_one_byte_over_limit_without_large_allocation() {
        let mut header = [0_u8; LENGTH_SIZE + FIXED_HEADER_SIZE];
        let payload_len = u32::try_from(MAX_PAYLOAD_LEN + 1).expect("fits u32");
        let declared = expected_total_length(0, MAX_PAYLOAD_LEN + 1).expect("length");
        header[0..4].copy_from_slice(&declared.to_le_bytes());
        header[4] = FORMAT_VERSION;
        header[5] = SIGNAL_LOGS;
        header[28..32].copy_from_slice(&payload_len.to_le_bytes());

        assert_eq!(
            decode(&header),
            Err(FrameError::PayloadTooLong {
                length: MAX_PAYLOAD_LEN + 1
            })
        );

        let mut frame = sample_frame();
        frame.payload = Bytes::from(vec![0; MAX_PAYLOAD_LEN + 1]);
        assert_eq!(
            encode(&frame),
            Err(FrameError::PayloadTooLong {
                length: MAX_PAYLOAD_LEN + 1
            })
        );
    }

    #[test]
    fn accepts_maximum_payload_length() {
        let mut frame = sample_frame();
        frame.payload = Bytes::from(vec![0xab; MAX_PAYLOAD_LEN]);
        let encoded = encode(&frame).expect("encode");
        let (decoded, consumed) = decode(&encoded).expect("decode");
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded.payload.len(), MAX_PAYLOAD_LEN);
    }

    #[test]
    fn truncation_of_valid_frame_is_incomplete() {
        let encoded = encode(&sample_frame()).expect("encode");
        for len in 0..encoded.len() {
            assert_eq!(
                decode(&encoded[..len]),
                Err(FrameError::Incomplete),
                "prefix of {len} bytes"
            );
        }
    }

    #[test]
    fn declared_length_smaller_than_header_fields() {
        let encoded = encode_parts(
            FORMAT_VERSION,
            SIGNAL_LOGS,
            0,
            1,
            1,
            0,
            b"tenant",
            b"pay",
            Some(8),
        );
        assert_eq!(
            decode(&encoded),
            Err(FrameError::LengthMismatch {
                declared: 8,
                expected: u64::from(expected_total_length(6, 3).expect("length")),
            })
        );
    }

    #[test]
    fn declared_length_larger_than_header_fields() {
        let declared = expected_total_length(6, 3).expect("length") + 16;
        let encoded = encode_parts(
            FORMAT_VERSION,
            SIGNAL_LOGS,
            0,
            1,
            1,
            0,
            b"tenant",
            b"pay",
            Some(declared),
        );
        assert_eq!(
            decode(&encoded),
            Err(FrameError::LengthMismatch {
                declared,
                expected: u64::from(expected_total_length(6, 3).expect("length")),
            })
        );
    }

    #[test]
    fn declared_length_larger_than_available_bytes_is_incomplete() {
        let encoded = encode(&sample_frame()).expect("encode");
        assert_eq!(
            decode(&encoded[..encoded.len() - 1]),
            Err(FrameError::Incomplete)
        );
    }

    #[test]
    fn integer_overflow_in_declared_lengths() {
        let mut header = [0_u8; LENGTH_SIZE + FIXED_HEADER_SIZE];
        header[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
        header[24..26].copy_from_slice(&u16::MAX.to_le_bytes());
        header[28..32].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(decode(&header), Err(FrameError::InvalidLength));
    }

    #[test]
    fn rejects_unsupported_version() {
        let encoded = encode_parts(2, SIGNAL_LOGS, 0, 1, 1, 0, b"t", b"p", None);
        assert_eq!(
            decode(&encoded),
            Err(FrameError::UnsupportedVersion { version: 2 })
        );
    }

    #[test]
    fn rejects_unknown_signal() {
        let encoded = encode_parts(FORMAT_VERSION, 99, 0, 1, 1, 0, b"t", b"p", None);
        assert_eq!(
            decode(&encoded),
            Err(FrameError::UnknownSignal { signal: 99 })
        );
    }

    #[test]
    fn rejects_unknown_flags() {
        let encoded = encode_parts(
            FORMAT_VERSION,
            SIGNAL_LOGS,
            0x0001,
            1,
            1,
            0,
            b"t",
            b"p",
            None,
        );
        assert_eq!(
            decode(&encoded),
            Err(FrameError::UnknownFlags { flags: 0x0001 })
        );
    }

    #[test]
    fn rejects_nonzero_reserved() {
        let encoded = encode_parts(FORMAT_VERSION, SIGNAL_LOGS, 0, 1, 1, 7, b"t", b"p", None);
        assert_eq!(
            decode(&encoded),
            Err(FrameError::ReservedMustBeZero { reserved: 7 })
        );
    }

    #[test]
    fn checksum_mismatch_in_header_tenant_payload_and_crc() {
        let encoded = encode(&sample_frame()).expect("encode");

        let mut header_flip = encoded.clone();
        header_flip[LENGTH_SIZE + 4] ^= 0x01;
        assert_eq!(decode(&header_flip), Err(FrameError::ChecksumMismatch));

        let mut tenant_flip = encoded.clone();
        tenant_flip[LENGTH_SIZE + FIXED_HEADER_SIZE] ^= 0x01;
        assert_eq!(decode(&tenant_flip), Err(FrameError::ChecksumMismatch));

        let mut payload_flip = encoded.clone();
        payload_flip[encoded.len() - CRC_SIZE - 1] ^= 0x01;
        assert_eq!(decode(&payload_flip), Err(FrameError::ChecksumMismatch));

        let mut crc_flip = encoded;
        *crc_flip.last_mut().expect("crc byte") ^= 0x01;
        assert_eq!(decode(&crc_flip), Err(FrameError::ChecksumMismatch));
    }

    #[test]
    fn concatenated_frames_decode_sequentially() {
        let first = sample_frame();
        let second = Frame {
            sequence: 43,
            signal: FrameSignal::Logs,
            received_at_unix_nanos: 2,
            tenant_id: "tenant-b".to_owned(),
            payload: Bytes::from_static(b"next"),
        };
        let mut bytes = encode(&first).expect("encode first");
        let second_encoded = encode(&second).expect("encode second");
        bytes.extend_from_slice(&second_encoded);

        let (decoded_first, first_len) = decode(&bytes).expect("decode first");
        assert_eq!(decoded_first, first);
        assert_eq!(first_len, bytes.len() - second_encoded.len());

        let (decoded_second, second_len) = decode(&bytes[first_len..]).expect("decode second");
        assert_eq!(decoded_second, second);
        assert_eq!(second_len, second_encoded.len());
    }

    #[test]
    fn trailing_bytes_are_left_unconsumed() {
        let encoded = encode(&sample_frame()).expect("encode");
        let mut with_trailing = encoded.clone();
        with_trailing.extend_from_slice(&[0xde, 0xad]);

        let (decoded, consumed) = decode(&with_trailing).expect("decode");
        assert_eq!(decoded, sample_frame());
        assert_eq!(consumed, encoded.len());
        assert_eq!(&with_trailing[consumed..], &[0xde, 0xad]);
    }
}
