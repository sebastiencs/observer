use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
use std::io;

use observer_protocol::{AcceptedBatch, Signal};

use crate::{
    Frame, FrameError, FrameSignal, WalError, decode, encode,
    segment::{
        LANE_DIR_NAME, LANE_ID, SEGMENT_HEADER_SIZE, SegmentHeader, encode_header,
        segment_file_name,
    },
};

/// Configuration for a single-lane local WAL.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalConfig {
    pub directory: PathBuf,
    pub max_entry_bytes: usize,
    pub target_segment_bytes: u64,
}

/// Location of a durably appended frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Receipt {
    pub sequence: u64,
    pub segment_id: u64,
    pub offset: u64,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
enum WriteFault {
    Error(io::ErrorKind),
    Short(usize),
}

/// Durable, synchronous, single-lane write-ahead log.
pub struct Wal {
    config: WalConfig,
    file: File,
    path: PathBuf,
    header: SegmentHeader,
    next_sequence: u64,
    next_offset: u64,
    failed: bool,
    #[cfg(test)]
    write_fault: Option<WriteFault>,
    #[cfg(test)]
    sync_fault: Option<io::ErrorKind>,
}

impl Wal {
    pub fn open(config: WalConfig) -> Result<Self, WalError> {
        if config.max_entry_bytes == 0 {
            return Err(WalError::InvalidConfig("max_entry_bytes must be non-zero"));
        }
        if config.target_segment_bytes == 0 {
            return Err(WalError::InvalidConfig(
                "target_segment_bytes must be non-zero",
            ));
        }

        let lane_dir = lane_directory(&config.directory);
        fs::create_dir_all(&lane_dir)?;

        let segment_id = 0;
        let path = lane_dir.join(segment_file_name(segment_id));
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let metadata = file.metadata()?;
        let (header, next_sequence, next_offset) = if metadata.len() == 0 {
            let header = SegmentHeader {
                lane_id: LANE_ID,
                segment_id,
                first_sequence: 0,
                created_at_unix_nanos: unix_nanos()?,
            };
            file.write_all(&encode_header(&header))?;
            file.sync_data()?;
            (
                header,
                0,
                u64::try_from(SEGMENT_HEADER_SIZE).expect("header size"),
            )
        } else {
            let mut bytes = Vec::new();
            file.seek(SeekFrom::Start(0))?;
            file.read_to_end(&mut bytes)?;
            let header = crate::segment::decode_header(&bytes)?;
            if header.segment_id != segment_id {
                return Err(WalError::InvalidSegmentHeader(
                    "segment id does not match file",
                ));
            }
            let (next_sequence, next_offset) = scan_frames(&bytes, header.first_sequence)?;
            (header, next_sequence, next_offset)
        };

        file.seek(SeekFrom::Start(next_offset))?;

        Ok(Self {
            config,
            file,
            path,
            header,
            next_sequence,
            next_offset,
            failed: false,
            #[cfg(test)]
            write_fault: None,
            #[cfg(test)]
            sync_fault: None,
        })
    }

    #[must_use]
    pub fn target_segment_bytes(&self) -> u64 {
        self.config.target_segment_bytes
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    pub fn append(&mut self, batch: AcceptedBatch) -> Result<Receipt, WalError> {
        if self.failed {
            return Err(WalError::Failed);
        }

        let frame = Frame {
            sequence: self.next_sequence,
            signal: frame_signal(batch.signal),
            received_at_unix_nanos: batch.received_at_unix_nanos,
            tenant_id: batch.tenant_id,
            payload: batch.payload,
        };
        let encoded = encode(&frame)?;
        if encoded.len() > self.config.max_entry_bytes {
            return Err(WalError::EntryTooLarge {
                size: encoded.len(),
                max: self.config.max_entry_bytes,
            });
        }

        let offset = self.next_offset;
        self.write_complete(&encoded)?;
        self.sync_data()?;

        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(WalError::InvalidSegmentHeader("sequence overflow"))?;
        self.next_offset = self
            .next_offset
            .checked_add(u64::try_from(encoded.len()).map_err(|_| FrameError::InvalidLength)?)
            .ok_or(WalError::InvalidSegmentHeader("offset overflow"))?;

        Ok(Receipt {
            sequence: frame.sequence,
            segment_id: self.header.segment_id,
            offset,
        })
    }

    pub fn sync(&mut self) -> Result<(), WalError> {
        if self.failed {
            return Err(WalError::Failed);
        }
        self.sync_data()
    }

    fn write_complete(&mut self, buf: &[u8]) -> Result<(), WalError> {
        #[cfg(test)]
        if let Some(fault) = self.write_fault.take() {
            match fault {
                WriteFault::Error(kind) => {
                    self.failed = true;
                    return Err(WalError::io(kind, "injected write error"));
                }
                WriteFault::Short(written) => {
                    let written = written.min(buf.len());
                    if written > 0
                        && let Err(error) = self.file.write_all(&buf[..written])
                    {
                        self.failed = true;
                        return Err(error.into());
                    }
                    self.failed = true;
                    return Err(WalError::ShortWrite {
                        written,
                        expected: buf.len(),
                    });
                }
            }
        }

        if let Err(error) = self.file.write_all(buf) {
            self.failed = true;
            return Err(error.into());
        }
        Ok(())
    }

    fn sync_data(&mut self) -> Result<(), WalError> {
        #[cfg(test)]
        if let Some(kind) = self.sync_fault.take() {
            self.failed = true;
            return Err(WalError::io(kind, "injected sync error"));
        }
        if let Err(error) = self.file.sync_data() {
            self.failed = true;
            return Err(error.into());
        }
        Ok(())
    }
}

fn lane_directory(root: &Path) -> PathBuf {
    root.join(LANE_DIR_NAME)
}

fn unix_nanos() -> Result<u64, WalError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| WalError::InvalidSegmentHeader("system clock is before the Unix epoch"))?;
    u64::try_from(duration.as_nanos())
        .map_err(|_| WalError::InvalidSegmentHeader("timestamp exceeds u64"))
}

fn frame_signal(signal: Signal) -> FrameSignal {
    match signal {
        Signal::Logs => FrameSignal::Logs,
    }
}

fn scan_frames(bytes: &[u8], first_sequence: u64) -> Result<(u64, u64), WalError> {
    if bytes.len() < SEGMENT_HEADER_SIZE {
        return Err(WalError::InvalidSegmentHeader("truncated header"));
    }

    let mut offset = SEGMENT_HEADER_SIZE;
    let mut next_sequence = first_sequence;
    while offset < bytes.len() {
        match decode(&bytes[offset..]) {
            Ok((frame, consumed)) => {
                if frame.sequence != next_sequence {
                    return Err(WalError::InvalidSegmentHeader("sequence discontinuity"));
                }
                next_sequence = next_sequence
                    .checked_add(1)
                    .ok_or(WalError::InvalidSegmentHeader("sequence overflow"))?;
                offset = offset
                    .checked_add(consumed)
                    .ok_or(WalError::InvalidSegmentHeader("offset overflow"))?;
            }
            Err(FrameError::Incomplete) => return Err(WalError::IncompleteSegment),
            Err(error) => return Err(WalError::Frame(error)),
        }
    }
    Ok((
        next_sequence,
        u64::try_from(offset).expect("offset fits u64"),
    ))
}

#[cfg(test)]
mod tests {
    use std::io;

    use bytes::Bytes;
    use observer_protocol::{AcceptedBatch, Signal};

    use super::*;
    use crate::MAX_TENANT_LEN;

    fn temp_config() -> (tempfile::TempDir, WalConfig) {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = WalConfig {
            directory: dir.path().to_path_buf(),
            max_entry_bytes: 1024 * 1024,
            target_segment_bytes: 256 * 1024 * 1024,
        };
        (dir, config)
    }

    fn batch(tenant: &str, payload: &[u8]) -> AcceptedBatch {
        AcceptedBatch {
            tenant_id: tenant.to_owned(),
            signal: Signal::Logs,
            received_at_unix_nanos: 1,
            payload: Bytes::copy_from_slice(payload),
        }
    }

    fn frame_at(path: &Path, offset: u64) -> Frame {
        let bytes = fs::read(path).expect("read segment");
        let start = usize::try_from(offset).expect("offset");
        decode(&bytes[start..]).expect("decode frame").0
    }

    impl Wal {
        fn fail_next_write(&mut self, kind: io::ErrorKind) {
            self.write_fault = Some(WriteFault::Error(kind));
        }

        fn short_next_write(&mut self, written: usize) {
            self.write_fault = Some(WriteFault::Short(written));
        }

        fn fail_next_sync(&mut self, kind: io::ErrorKind) {
            self.sync_fault = Some(kind);
        }
    }

    #[test]
    fn creates_lane_directory_and_active_segment() {
        let (_dir, config) = temp_config();
        let wal = Wal::open(config.clone()).expect("open");
        assert!(wal.path().ends_with("lane-0000/00000000000000000000.open"));
        assert!(wal.path().is_file());
        assert_eq!(wal.header().lane_id, LANE_ID);
        assert_eq!(wal.header().segment_id, 0);
        assert_eq!(wal.header().first_sequence, 0);
        assert_eq!(wal.target_segment_bytes(), config.target_segment_bytes);

        let encoded = encode_header(wal.header());
        let on_disk = fs::read(wal.path()).expect("read");
        assert_eq!(&on_disk[..SEGMENT_HEADER_SIZE], encoded.as_slice());
        assert_eq!(
            crate::segment::decode_header(&on_disk).expect("header"),
            *wal.header()
        );
    }

    #[test]
    fn appends_preserve_sequence_and_decode_at_receipt_offset() {
        let (_dir, config) = temp_config();
        let mut wal = Wal::open(config).expect("open");

        let first = wal.append(batch("tenant-a", b"one")).expect("first");
        let second = wal.append(batch("tenant-a", b"two")).expect("second");

        assert_eq!(first.sequence, 0);
        assert_eq!(second.sequence, 1);
        assert_eq!(first.segment_id, 0);
        assert_eq!(first.offset, u64::try_from(SEGMENT_HEADER_SIZE).unwrap());
        assert!(second.offset > first.offset);

        let first_frame = frame_at(wal.path(), first.offset);
        assert_eq!(first_frame.sequence, 0);
        assert_eq!(first_frame.tenant_id, "tenant-a");
        assert_eq!(first_frame.payload.as_ref(), b"one");

        let second_frame = frame_at(wal.path(), second.offset);
        assert_eq!(second_frame.sequence, 1);
        assert_eq!(second_frame.payload.as_ref(), b"two");
    }

    #[test]
    fn oversized_entry_is_rejected_before_writing() {
        let (_dir, config) = temp_config();
        let mut wal = Wal::open(config).expect("open");
        let size_before = fs::metadata(wal.path()).expect("meta").len();

        let error = wal
            .append(AcceptedBatch {
                tenant_id: "a".repeat(MAX_TENANT_LEN + 1),
                signal: Signal::Logs,
                received_at_unix_nanos: 1,
                payload: Bytes::from_static(b"ok"),
            })
            .expect_err("oversized tenant");
        assert!(matches!(
            error,
            WalError::Frame(FrameError::TenantTooLong { length }) if length == MAX_TENANT_LEN + 1
        ));
        assert_eq!(fs::metadata(wal.path()).expect("meta").len(), size_before);

        let receipt = wal
            .append(batch("tenant-a", b"ok"))
            .expect("valid after reject");
        assert_eq!(receipt.sequence, 0);
    }

    #[test]
    fn config_entry_limit_is_rejected_before_writing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = WalConfig {
            directory: dir.path().to_path_buf(),
            max_entry_bytes: SEGMENT_HEADER_SIZE,
            target_segment_bytes: 1024,
        };
        let mut wal = Wal::open(config).expect("open");
        let size_before = fs::metadata(wal.path()).expect("meta").len();
        let error = wal
            .append(batch("tenant-a", b"payload"))
            .expect_err("entry exceeds config");
        assert!(matches!(error, WalError::EntryTooLarge { .. }));
        assert_eq!(fs::metadata(wal.path()).expect("meta").len(), size_before);
    }

    #[test]
    fn reopen_preserves_frames_and_next_sequence() {
        let (dir, config) = temp_config();
        let first_receipt = {
            let mut wal = Wal::open(config.clone()).expect("open");
            wal.append(batch("tenant-a", b"one")).expect("first");
            wal.append(batch("tenant-a", b"two")).expect("second")
        };

        let mut wal = Wal::open(config).expect("reopen");
        assert_eq!(wal.header().first_sequence, 0);
        let third = wal.append(batch("tenant-a", b"three")).expect("third");
        assert_eq!(third.sequence, 2);
        assert!(third.offset > first_receipt.offset);

        let frame = frame_at(wal.path(), third.offset);
        assert_eq!(frame.payload.as_ref(), b"three");
        drop(dir);
    }

    #[test]
    fn write_error_fails_wal_and_rejects_later_appends() {
        let (_dir, config) = temp_config();
        let mut wal = Wal::open(config).expect("open");
        wal.fail_next_write(io::ErrorKind::Other);

        let error = wal.append(batch("tenant-a", b"one")).expect_err("write");
        assert!(matches!(error, WalError::Io(_)));
        assert!(matches!(
            wal.append(batch("tenant-a", b"two")),
            Err(WalError::Failed)
        ));
    }

    #[test]
    fn short_write_fails_wal_without_receipt() {
        let (_dir, config) = temp_config();
        let mut wal = Wal::open(config).expect("open");
        wal.short_next_write(1);

        let error = wal.append(batch("tenant-a", b"one")).expect_err("short");
        assert!(matches!(
            error,
            WalError::ShortWrite {
                written: 1,
                expected
            } if expected > 1
        ));
        assert!(matches!(
            wal.append(batch("tenant-a", b"two")),
            Err(WalError::Failed)
        ));
    }

    #[test]
    fn sync_error_fails_wal_without_receipt() {
        let (_dir, config) = temp_config();
        let mut wal = Wal::open(config).expect("open");
        wal.fail_next_sync(io::ErrorKind::Other);

        let error = wal.append(batch("tenant-a", b"one")).expect_err("sync");
        assert!(matches!(error, WalError::Io(_)));
        assert!(matches!(wal.sync(), Err(WalError::Failed)));
        assert!(matches!(
            wal.append(batch("tenant-a", b"two")),
            Err(WalError::Failed)
        ));
    }

    #[test]
    fn incomplete_existing_segment_is_rejected_on_open() {
        let (_dir, config) = temp_config();
        let path = {
            let mut wal = Wal::open(config.clone()).expect("open");
            wal.append(batch("tenant-a", b"one")).expect("append");
            wal.path().to_path_buf()
        };
        let mut bytes = fs::read(&path).expect("read");
        bytes.pop();
        fs::write(&path, bytes).expect("truncate");

        assert!(matches!(
            Wal::open(config),
            Err(WalError::IncompleteSegment)
        ));
    }
}
