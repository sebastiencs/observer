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
    Frame, FrameError, FrameSignal, WalError, encode,
    fsutil::sync_directory,
    recovery::{
        SegmentKind, discover_segments, lane_directory, scan_open_segment, scan_sealed_segment,
    },
    segment::{
        LANE_ID, SEGMENT_HEADER_SIZE, SegmentHeader, encode_header, sealed_segment_file_name,
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

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RotateFault {
    SyncActive,
    Rename,
    SyncDirAfterRename,
    CreateNext,
    SyncNewHeader,
    SyncDirAfterCreate,
}

/// Durable, synchronous, single-lane write-ahead log.
pub struct Wal {
    config: WalConfig,
    file: File,
    path: PathBuf,
    lane_dir: PathBuf,
    header: SegmentHeader,
    next_sequence: u64,
    next_offset: u64,
    failed: bool,
    #[cfg(test)]
    write_fault: Option<WriteFault>,
    #[cfg(test)]
    sync_fault: Option<io::ErrorKind>,
    #[cfg(test)]
    rotate_fault: Option<RotateFault>,
    #[cfg(any(test, feature = "test-util"))]
    io_hooks: Option<std::sync::Arc<crate::io_hooks::WalIoHooks>>,
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
        let discovered = discover_segments(&lane_dir)?;
        let next_after_sealed = validate_sealed_segments(&discovered)?;
        let (path, segment_id, expected_first_sequence) =
            resolve_active_segment(&lane_dir, &discovered, next_after_sealed)?;

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let metadata = file.metadata()?;
        let (header, next_sequence, next_offset) = if metadata.len() < SEGMENT_HEADER_SIZE as u64 {
            let header = SegmentHeader {
                lane_id: LANE_ID,
                segment_id,
                first_sequence: expected_first_sequence,
                created_at_unix_nanos: unix_nanos()?,
            };
            file.set_len(0)?;
            file.seek(SeekFrom::Start(0))?;
            file.write_all(&encode_header(&header))?;
            file.sync_data()?;
            sync_directory(&lane_dir)?;
            (
                header,
                expected_first_sequence,
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
            if header.first_sequence != expected_first_sequence {
                return Err(WalError::Corrupt("sequence discontinuity"));
            }
            let recovered = scan_open_segment(&bytes, header.first_sequence)?;
            if recovered.truncated {
                file.set_len(recovered.valid_end)?;
                file.sync_data()?;
            }
            (header, recovered.next_sequence, recovered.valid_end)
        };

        file.seek(SeekFrom::Start(next_offset))?;

        Ok(Self {
            config,
            file,
            path,
            lane_dir,
            header,
            next_sequence,
            next_offset,
            failed: false,
            #[cfg(test)]
            write_fault: None,
            #[cfg(test)]
            sync_fault: None,
            #[cfg(test)]
            rotate_fault: None,
            #[cfg(any(test, feature = "test-util"))]
            io_hooks: None,
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
        let receipt = self.write(batch)?;
        self.sync_data()?;
        Ok(receipt)
    }

    /// Write every batch, then `sync_data` once. No receipt is returned unless
    /// the group sync succeeds.
    pub fn append_group<I>(&mut self, batches: I) -> Result<Vec<Receipt>, WalError>
    where
        I: IntoIterator<Item = AcceptedBatch>,
    {
        let mut receipts = Vec::new();
        for batch in batches {
            receipts.push(self.write(batch)?);
        }
        if !receipts.is_empty() {
            self.sync_data()?;
        }
        Ok(receipts)
    }

    pub fn sync(&mut self) -> Result<(), WalError> {
        if self.failed {
            return Err(WalError::Failed);
        }
        self.sync_data()
    }

    #[cfg(any(test, feature = "test-util"))]
    pub(crate) fn set_io_hooks(&mut self, hooks: std::sync::Arc<crate::io_hooks::WalIoHooks>) {
        self.io_hooks = Some(hooks);
    }

    fn write(&mut self, batch: AcceptedBatch) -> Result<Receipt, WalError> {
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

        if self.needs_rotation(encoded.len())? {
            self.rotate()?;
        }

        let offset = self.next_offset;
        self.write_complete(&encoded)?;

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
        #[cfg(any(test, feature = "test-util"))]
        if let Some(hooks) = &self.io_hooks
            && let Err(error) = hooks.on_sync()
        {
            self.failed = true;
            return Err(error);
        }
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

    fn needs_rotation(&self, encoded_len: usize) -> Result<bool, WalError> {
        let encoded_len = u64::try_from(encoded_len).map_err(|_| FrameError::InvalidLength)?;
        let would_end = self
            .next_offset
            .checked_add(encoded_len)
            .ok_or(WalError::InvalidSegmentHeader("offset overflow"))?;
        if would_end <= self.config.target_segment_bytes {
            return Ok(false);
        }
        let header_end = u64::try_from(SEGMENT_HEADER_SIZE).expect("header size");
        Ok(self.next_offset > header_end)
    }

    fn rotate(&mut self) -> Result<(), WalError> {
        #[cfg(test)]
        self.inject_rotate_fault(RotateFault::SyncActive)?;
        self.sync_data()?;

        let sealed_path = self
            .lane_dir
            .join(sealed_segment_file_name(self.header.segment_id));
        #[cfg(test)]
        self.inject_rotate_fault(RotateFault::Rename)?;
        if let Err(error) = fs::rename(&self.path, &sealed_path) {
            self.failed = true;
            return Err(error.into());
        }

        #[cfg(test)]
        self.inject_rotate_fault(RotateFault::SyncDirAfterRename)?;
        if let Err(error) = sync_directory(&self.lane_dir) {
            self.failed = true;
            return Err(error);
        }

        let next_id = match self.header.segment_id.checked_add(1) {
            Some(id) => id,
            None => {
                self.failed = true;
                return Err(WalError::InvalidSegmentHeader("segment id overflow"));
            }
        };
        let next_path = self.lane_dir.join(segment_file_name(next_id));
        let header = match unix_nanos() {
            Ok(created_at_unix_nanos) => SegmentHeader {
                lane_id: LANE_ID,
                segment_id: next_id,
                first_sequence: self.next_sequence,
                created_at_unix_nanos,
            },
            Err(error) => {
                self.failed = true;
                return Err(error);
            }
        };

        #[cfg(test)]
        self.inject_rotate_fault(RotateFault::CreateNext)?;
        let mut file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&next_path)
        {
            Ok(file) => file,
            Err(error) => {
                self.failed = true;
                return Err(error.into());
            }
        };

        if let Err(error) = file.write_all(&encode_header(&header)) {
            self.failed = true;
            return Err(error.into());
        }

        #[cfg(test)]
        self.inject_rotate_fault(RotateFault::SyncNewHeader)?;
        if let Err(error) = file.sync_data() {
            self.failed = true;
            return Err(error.into());
        }

        #[cfg(test)]
        self.inject_rotate_fault(RotateFault::SyncDirAfterCreate)?;
        if let Err(error) = sync_directory(&self.lane_dir) {
            self.failed = true;
            return Err(error);
        }

        self.file = file;
        self.path = next_path;
        self.header = header;
        self.next_offset = u64::try_from(SEGMENT_HEADER_SIZE).expect("header size");
        Ok(())
    }

    #[cfg(test)]
    fn inject_rotate_fault(&mut self, step: RotateFault) -> Result<(), WalError> {
        if self.rotate_fault == Some(step) {
            self.rotate_fault = None;
            self.failed = true;
            return Err(WalError::io(io::ErrorKind::Other, "injected rotate error"));
        }
        Ok(())
    }
}

fn resolve_active_segment(
    lane_dir: &Path,
    discovered: &[crate::recovery::FoundSegment],
    next_after_sealed: Option<u64>,
) -> Result<(PathBuf, u64, u64), WalError> {
    match discovered.last() {
        Some(segment) if segment.kind == SegmentKind::Open => Ok((
            segment.path.clone(),
            segment.id,
            next_after_sealed.unwrap_or(0),
        )),
        Some(segment) => {
            let next_id = segment
                .id
                .checked_add(1)
                .ok_or(WalError::InvalidSegmentHeader("segment id overflow"))?;
            Ok((
                lane_dir.join(segment_file_name(next_id)),
                next_id,
                next_after_sealed.ok_or(WalError::Corrupt("missing sealed sequence"))?,
            ))
        }
        None => Ok((lane_dir.join(segment_file_name(0)), 0, 0)),
    }
}

fn validate_sealed_segments(
    discovered: &[crate::recovery::FoundSegment],
) -> Result<Option<u64>, WalError> {
    let mut next_sequence = None;
    for segment in discovered {
        if segment.kind != SegmentKind::Sealed {
            continue;
        }
        let bytes = fs::read(&segment.path)?;
        let header = crate::segment::decode_header(&bytes)?;
        if header.segment_id != segment.id {
            return Err(WalError::InvalidSegmentHeader(
                "segment id does not match file",
            ));
        }
        if let Some(expected) = next_sequence
            && header.first_sequence != expected
        {
            return Err(WalError::Corrupt("sequence discontinuity"));
        }
        let recovered = scan_sealed_segment(&bytes, header.first_sequence)?;
        next_sequence = Some(recovered.next_sequence);
    }
    Ok(next_sequence)
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

#[cfg(test)]
mod tests {
    use std::io;

    use bytes::Bytes;
    use observer_protocol::{AcceptedBatch, Signal};

    use super::*;
    use crate::{MAX_TENANT_LEN, decode};

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

        fn fail_next_rotate(&mut self, step: RotateFault) {
            self.rotate_fault = Some(step);
        }
    }

    fn encoded_len(tenant: &str, payload: &[u8]) -> usize {
        encode(&Frame {
            sequence: 0,
            signal: FrameSignal::Logs,
            received_at_unix_nanos: 1,
            tenant_id: tenant.to_owned(),
            payload: Bytes::copy_from_slice(payload),
        })
        .expect("encode")
        .len()
    }

    fn lane_names(config: &WalConfig) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(lane_directory(&config.directory))
            .expect("read lane")
            .map(|entry| {
                entry
                    .expect("entry")
                    .file_name()
                    .into_string()
                    .expect("utf8")
            })
            .collect();
        names.sort();
        names
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
    fn append_group_writes_all_frames_then_syncs_once() {
        let (_dir, config) = temp_config();
        let hooks = std::sync::Arc::new(crate::WalIoHooks::new());
        let mut wal = Wal::open(config).expect("open");
        wal.set_io_hooks(std::sync::Arc::clone(&hooks));

        let receipts = wal
            .append_group([batch("tenant-a", b"one"), batch("tenant-a", b"two")])
            .expect("group");
        assert_eq!(receipts.len(), 2);
        assert_eq!(receipts[0].sequence, 0);
        assert_eq!(receipts[1].sequence, 1);
        assert_eq!(hooks.sync_count(), 1);
        assert_eq!(
            frame_at(wal.path(), receipts[0].offset).payload.as_ref(),
            b"one"
        );
        assert_eq!(
            frame_at(wal.path(), receipts[1].offset).payload.as_ref(),
            b"two"
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

    fn write_segment(path: &Path, bytes: &[u8]) {
        fs::write(path, bytes).expect("write fixture");
    }

    fn recovered_frames(path: &Path) -> Vec<Frame> {
        let bytes = fs::read(path).expect("read");
        let mut offset = SEGMENT_HEADER_SIZE;
        let mut frames = Vec::new();
        while offset < bytes.len() {
            let (frame, consumed) = decode(&bytes[offset..]).expect("frame after recovery");
            frames.push(frame);
            offset += consumed;
        }
        frames
    }

    #[test]
    fn recovers_torn_tail_and_continues_sequence() {
        let (_dir, config) = temp_config();
        let path = {
            let mut wal = Wal::open(config.clone()).expect("open");
            wal.append(batch("tenant-a", b"one")).expect("first");
            wal.append(batch("tenant-a", b"two")).expect("second");
            wal.path().to_path_buf()
        };
        let mut bytes = fs::read(&path).expect("read");
        bytes.pop();
        write_segment(&path, &bytes);

        let mut wal = Wal::open(config.clone()).expect("recover");
        assert_eq!(recovered_frames(wal.path()).len(), 1);
        let third = wal
            .append(batch("tenant-a", b"three"))
            .expect("after recover");
        assert_eq!(third.sequence, 1);

        let wal = Wal::open(config).expect("reopen recovered");
        let frames = recovered_frames(wal.path());
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].payload.as_ref(), b"one");
        assert_eq!(frames[1].payload.as_ref(), b"three");
    }

    #[test]
    fn recovery_of_torn_tail_is_idempotent() {
        let (_dir, config) = temp_config();
        let path = {
            let mut wal = Wal::open(config.clone()).expect("open");
            wal.append(batch("tenant-a", b"one")).expect("append");
            wal.path().to_path_buf()
        };
        let mut bytes = fs::read(&path).expect("read");
        bytes.truncate(bytes.len() - 3);
        write_segment(&path, &bytes);

        let first = Wal::open(config.clone()).expect("first recover");
        let first_len = fs::metadata(first.path()).expect("meta").len();
        drop(first);
        let second = Wal::open(config).expect("second recover");
        assert_eq!(fs::metadata(second.path()).expect("meta").len(), first_len);
        assert_eq!(recovered_frames(second.path()).len(), 0);
    }

    #[test]
    fn recovers_every_torn_prefix_of_a_valid_segment() {
        let (_dir, config) = temp_config();
        let original = {
            let mut wal = Wal::open(config.clone()).expect("open");
            wal.append(batch("tenant-a", b"one")).expect("first");
            wal.append(batch("tenant-a", b"two")).expect("second");
            fs::read(wal.path()).expect("read intact")
        };

        for len in 0..original.len() {
            let dir = tempfile::tempdir().expect("tempdir");
            let config = WalConfig {
                directory: dir.path().to_path_buf(),
                max_entry_bytes: 1024 * 1024,
                target_segment_bytes: 256 * 1024 * 1024,
            };
            let wal = Wal::open(config.clone()).expect("create");
            let path = wal.path().to_path_buf();
            drop(wal);
            write_segment(&path, &original[..len]);

            let wal = Wal::open(config).expect("recover prefix");
            let recovered = recovered_frames(wal.path());
            if len < SEGMENT_HEADER_SIZE {
                assert!(recovered.is_empty(), "len {len}");
            } else {
                let expected =
                    crate::recovery::scan_open_segment(&original[..len], 0).expect("scan prefix");
                assert_eq!(recovered.len() as u64, expected.next_sequence, "len {len}");
                assert_eq!(
                    fs::metadata(wal.path()).expect("meta").len(),
                    expected.valid_end,
                    "len {len}"
                );
            }
        }
    }

    #[test]
    fn checksum_mismatch_at_eof_truncates_last_frame() {
        let (_dir, config) = temp_config();
        let path = {
            let mut wal = Wal::open(config.clone()).expect("open");
            wal.append(batch("tenant-a", b"one")).expect("first");
            wal.append(batch("tenant-a", b"two")).expect("second");
            wal.path().to_path_buf()
        };
        let mut bytes = fs::read(&path).expect("read");
        *bytes.last_mut().unwrap() ^= 0xff;
        write_segment(&path, &bytes);

        let wal = Wal::open(config).expect("recover");
        let frames = recovered_frames(wal.path());
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].payload.as_ref(), b"one");
    }

    #[test]
    fn garbage_after_valid_frames_is_truncated() {
        let (_dir, config) = temp_config();
        let path = {
            let mut wal = Wal::open(config.clone()).expect("open");
            wal.append(batch("tenant-a", b"one")).expect("append");
            wal.path().to_path_buf()
        };
        let mut bytes = fs::read(&path).expect("read");
        bytes.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef, 0x00]);
        write_segment(&path, &bytes);

        let wal = Wal::open(config).expect("recover");
        let frames = recovered_frames(wal.path());
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].payload.as_ref(), b"one");
    }

    #[test]
    fn mid_segment_checksum_corruption_is_fatal() {
        let (_dir, config) = temp_config();
        let path = {
            let mut wal = Wal::open(config.clone()).expect("open");
            wal.append(batch("tenant-a", b"one")).expect("first");
            wal.append(batch("tenant-a", b"two")).expect("second");
            wal.append(batch("tenant-a", b"three")).expect("third");
            wal.path().to_path_buf()
        };
        let mut bytes = fs::read(&path).expect("read");
        let first = encode(&Frame {
            sequence: 0,
            signal: crate::FrameSignal::Logs,
            received_at_unix_nanos: 1,
            tenant_id: "tenant-a".to_owned(),
            payload: Bytes::from_static(b"one"),
        })
        .unwrap();
        bytes[SEGMENT_HEADER_SIZE + first.len() + 8] ^= 0xff;
        write_segment(&path, &bytes);

        assert!(matches!(
            Wal::open(config),
            Err(WalError::Frame(FrameError::ChecksumMismatch))
        ));
    }

    #[test]
    fn unsupported_frame_version_is_fatal() {
        let (_dir, config) = temp_config();
        let path = {
            let mut wal = Wal::open(config.clone()).expect("open");
            wal.append(batch("tenant-a", b"one")).expect("append");
            wal.path().to_path_buf()
        };
        let mut bytes = fs::read(&path).expect("read");
        bytes[SEGMENT_HEADER_SIZE + 4] = 9;
        let crc_start = bytes.len() - 4;
        let crc = crc32c::crc32c(&bytes[SEGMENT_HEADER_SIZE + 4..crc_start]);
        bytes[crc_start..].copy_from_slice(&crc.to_le_bytes());
        write_segment(&path, &bytes);

        assert!(matches!(
            Wal::open(config),
            Err(WalError::Corrupt("unsupported frame version"))
        ));
    }

    #[test]
    fn duplicate_segment_ids_are_fatal() {
        let (_dir, config) = temp_config();
        let path = {
            let wal = Wal::open(config.clone()).expect("open");
            wal.path().to_path_buf()
        };
        fs::copy(&path, path.with_extension("wal")).expect("duplicate");
        assert!(matches!(
            Wal::open(config),
            Err(WalError::Corrupt("duplicate segment id"))
        ));
    }

    #[test]
    fn missing_segment_id_is_fatal() {
        let (_dir, config) = temp_config();
        let path = {
            let wal = Wal::open(config.clone()).expect("open");
            wal.path().to_path_buf()
        };
        fs::rename(&path, path.with_file_name("00000000000000000002.open")).expect("rename");
        assert!(matches!(
            Wal::open(config),
            Err(WalError::Corrupt("missing or out-of-order segment id"))
        ));
    }

    #[test]
    fn torn_segment_header_is_rewritten() {
        let (_dir, config) = temp_config();
        let path = {
            let wal = Wal::open(config.clone()).expect("open");
            wal.path().to_path_buf()
        };
        write_segment(&path, &[0u8; 10]);
        let wal = Wal::open(config).expect("recover header");
        assert_eq!(recovered_frames(wal.path()).len(), 0);
        assert_eq!(
            fs::metadata(wal.path()).expect("meta").len(),
            u64::try_from(SEGMENT_HEADER_SIZE).unwrap()
        );
    }

    #[test]
    fn rotates_immediately_before_threshold_crossing() {
        let frame = u64::try_from(encoded_len("tenant-a", b"one")).unwrap();
        let header = u64::try_from(SEGMENT_HEADER_SIZE).unwrap();
        let (_dir, config) = config_with_target(header + frame + 1);
        let mut wal = Wal::open(config.clone()).expect("open");

        let first = wal.append(batch("tenant-a", b"one")).expect("first");
        assert_eq!(first.segment_id, 0);
        assert_eq!(
            lane_names(&config),
            vec!["00000000000000000000.open".to_owned()]
        );

        let second = wal.append(batch("tenant-a", b"two")).expect("second");
        assert_eq!(second.sequence, 1);
        assert_eq!(second.segment_id, 1);
        assert_eq!(second.offset, header);
        assert_eq!(
            lane_names(&config),
            vec![
                "00000000000000000000.wal".to_owned(),
                "00000000000000000001.open".to_owned()
            ]
        );

        let sealed = config.directory.join("lane-0000/00000000000000000000.wal");
        assert_eq!(recovered_frames(&sealed)[0].payload.as_ref(), b"one");
        assert_eq!(frame_at(wal.path(), second.offset).payload.as_ref(), b"two");
    }

    #[test]
    fn sequence_continues_across_rotated_segments() {
        let frame = u64::try_from(encoded_len("tenant-a", b"one")).unwrap();
        let header = u64::try_from(SEGMENT_HEADER_SIZE).unwrap();
        let (_dir, config) = config_with_target(header + frame * 2);
        let mut wal = Wal::open(config).expect("open");

        let first = wal.append(batch("tenant-a", b"one")).expect("first");
        let second = wal.append(batch("tenant-a", b"two")).expect("second");
        let third = wal.append(batch("tenant-a", b"three")).expect("third");

        assert_eq!(first.sequence, 0);
        assert_eq!(second.sequence, 1);
        assert_eq!(third.sequence, 2);
        assert_eq!(first.segment_id, 0);
        assert_eq!(second.segment_id, 0);
        assert_eq!(third.segment_id, 1);
        assert_eq!(wal.header().first_sequence, 2);
    }

    #[test]
    fn reopen_preserves_sealed_and_active_segments() {
        let frame = u64::try_from(encoded_len("tenant-a", b"one")).unwrap();
        let header = u64::try_from(SEGMENT_HEADER_SIZE).unwrap();
        let (_dir, config) = config_with_target(header + frame + 1);
        {
            let mut wal = Wal::open(config.clone()).expect("open");
            wal.append(batch("tenant-a", b"one")).expect("first");
            wal.append(batch("tenant-a", b"two")).expect("second");
        }

        let sealed = config.directory.join("lane-0000/00000000000000000000.wal");
        let mut wal = Wal::open(config.clone()).expect("reopen");
        assert_eq!(wal.header().segment_id, 1);
        assert_eq!(wal.header().first_sequence, 1);
        assert_eq!(recovered_frames(wal.path()).len(), 1);
        assert_eq!(recovered_frames(&sealed)[0].payload.as_ref(), b"one");

        let third = wal.append(batch("tenant-a", b"two")).expect("third");
        assert_eq!(third.sequence, 2);
        assert_eq!(third.segment_id, 2);
        assert_eq!(recovered_frames(&sealed).len(), 1);
        assert_eq!(recovered_frames(wal.path())[0].payload.as_ref(), b"two");
        assert_eq!(
            lane_names(&config),
            vec![
                "00000000000000000000.wal".to_owned(),
                "00000000000000000001.wal".to_owned(),
                "00000000000000000002.open".to_owned()
            ]
        );
    }

    #[test]
    fn empty_active_segment_is_recovered_after_rotation() {
        let (_dir, config) = temp_config();
        let sealed = {
            let mut wal = Wal::open(config.clone()).expect("open");
            wal.append(batch("tenant-a", b"one")).expect("append");
            let path = wal.path().to_path_buf();
            drop(wal);
            let sealed = path.with_extension("wal");
            fs::rename(&path, &sealed).expect("seal");
            sealed
        };

        let header = encode_header(&SegmentHeader {
            lane_id: LANE_ID,
            segment_id: 1,
            first_sequence: 1,
            created_at_unix_nanos: 1,
        });
        write_segment(
            &config.directory.join("lane-0000/00000000000000000001.open"),
            &header,
        );

        let mut wal = Wal::open(config).expect("reopen empty");
        assert_eq!(wal.header().segment_id, 1);
        assert_eq!(wal.header().first_sequence, 1);
        assert_eq!(recovered_frames(wal.path()).len(), 0);
        let next = wal.append(batch("tenant-a", b"two")).expect("continue");
        assert_eq!(next.sequence, 1);
        assert_eq!(recovered_frames(&sealed).len(), 1);
    }

    #[test]
    fn oversized_first_frame_may_exceed_target_without_rotating() {
        let frame = u64::try_from(encoded_len("tenant-a", b"one")).unwrap();
        let header = u64::try_from(SEGMENT_HEADER_SIZE).unwrap();
        let (_dir, config) = config_with_target(header + 10);
        let mut wal = Wal::open(config.clone()).expect("open");

        let first = wal.append(batch("tenant-a", b"one")).expect("first");
        assert_eq!(first.segment_id, 0);
        assert!(fs::metadata(wal.path()).expect("meta").len() > config.target_segment_bytes);
        assert_eq!(
            lane_names(&config),
            vec!["00000000000000000000.open".to_owned()]
        );

        let second = wal.append(batch("tenant-a", b"two")).expect("second");
        assert_eq!(second.segment_id, 1);
        assert_eq!(second.sequence, 1);
        assert!(frame > 10);
    }

    #[test]
    fn recovers_after_seal_before_next_segment_is_created() {
        let (_dir, config) = temp_config();
        let sealed = {
            let mut wal = Wal::open(config.clone()).expect("open");
            wal.append(batch("tenant-a", b"one")).expect("append");
            let path = wal.path().to_path_buf();
            drop(wal);
            let sealed = path.with_extension("wal");
            fs::rename(path, &sealed).expect("seal");
            sealed
        };

        let mut wal = Wal::open(config.clone()).expect("recover");
        assert_eq!(wal.header().segment_id, 1);
        assert_eq!(wal.header().first_sequence, 1);
        assert_eq!(
            lane_names(&config),
            vec![
                "00000000000000000000.wal".to_owned(),
                "00000000000000000001.open".to_owned()
            ]
        );
        let next = wal.append(batch("tenant-a", b"two")).expect("continue");
        assert_eq!(next.sequence, 1);
        assert_eq!(recovered_frames(&sealed)[0].payload.as_ref(), b"one");
        assert_eq!(recovered_frames(wal.path())[0].payload.as_ref(), b"two");
    }

    #[test]
    fn recovers_torn_next_header_after_new_segment_create() {
        let (_dir, config) = temp_config();
        {
            let mut wal = Wal::open(config.clone()).expect("open");
            wal.append(batch("tenant-a", b"one")).expect("append");
            let path = wal.path().to_path_buf();
            drop(wal);
            fs::rename(&path, path.with_extension("wal")).expect("seal");
        }
        write_segment(
            &config.directory.join("lane-0000/00000000000000000001.open"),
            &[0u8; 10],
        );

        let mut wal = Wal::open(config).expect("recover torn header");
        assert_eq!(wal.header().segment_id, 1);
        assert_eq!(wal.header().first_sequence, 1);
        assert_eq!(recovered_frames(wal.path()).len(), 0);
        let next = wal.append(batch("tenant-a", b"two")).expect("continue");
        assert_eq!(next.sequence, 1);
        assert_eq!(next.segment_id, 1);
    }

    #[test]
    fn duplicate_open_segments_are_fatal() {
        let (_dir, config) = temp_config();
        let path = {
            let wal = Wal::open(config.clone()).expect("open");
            wal.path().to_path_buf()
        };
        fs::copy(&path, path.with_file_name("00000000000000000001.open")).expect("duplicate open");
        assert!(matches!(
            Wal::open(config),
            Err(WalError::Corrupt("multiple active segments"))
        ));
    }

    #[test]
    fn sealed_sequence_discontinuity_is_fatal() {
        let (_dir, config) = temp_config();
        {
            let mut wal = Wal::open(config.clone()).expect("open");
            wal.append(batch("tenant-a", b"one")).expect("append");
            let path = wal.path().to_path_buf();
            drop(wal);
            fs::rename(&path, path.with_extension("wal")).expect("seal");
        }
        let mut second = encode_header(&SegmentHeader {
            lane_id: LANE_ID,
            segment_id: 1,
            first_sequence: 99,
            created_at_unix_nanos: 1,
        })
        .to_vec();
        second.extend_from_slice(
            &encode(&Frame {
                sequence: 99,
                signal: FrameSignal::Logs,
                received_at_unix_nanos: 1,
                tenant_id: "tenant-a".to_owned(),
                payload: Bytes::from_static(b"two"),
            })
            .unwrap(),
        );
        write_segment(
            &config.directory.join("lane-0000/00000000000000000001.wal"),
            &second,
        );

        assert!(matches!(
            Wal::open(config),
            Err(WalError::Corrupt("sequence discontinuity"))
        ));
    }

    #[test]
    fn directory_failure_after_rename_fails_wal_and_recovers_on_reopen() {
        let frame = u64::try_from(encoded_len("tenant-a", b"one")).unwrap();
        let header = u64::try_from(SEGMENT_HEADER_SIZE).unwrap();
        let (_dir, config) = config_with_target(header + frame + 1);
        let mut wal = Wal::open(config.clone()).expect("open");
        wal.append(batch("tenant-a", b"one")).expect("first");
        wal.fail_next_rotate(RotateFault::SyncDirAfterRename);

        let error = wal
            .append(batch("tenant-a", b"two"))
            .expect_err("rotate dir sync");
        assert!(matches!(error, WalError::Io(_)));
        assert!(matches!(
            wal.append(batch("tenant-a", b"three")),
            Err(WalError::Failed)
        ));
        drop(wal);

        let mut wal = Wal::open(config.clone()).expect("reopen after rotate fault");
        assert_eq!(
            lane_names(&config),
            vec![
                "00000000000000000000.wal".to_owned(),
                "00000000000000000001.open".to_owned()
            ]
        );
        let next = wal.append(batch("tenant-a", b"two")).expect("retry");
        assert_eq!(next.sequence, 1);
        assert_eq!(next.segment_id, 1);
    }

    #[test]
    fn directory_failure_creating_next_segment_fails_wal() {
        let frame = u64::try_from(encoded_len("tenant-a", b"one")).unwrap();
        let header = u64::try_from(SEGMENT_HEADER_SIZE).unwrap();
        let (_dir, config) = config_with_target(header + frame + 1);
        let mut wal = Wal::open(config.clone()).expect("open");
        wal.append(batch("tenant-a", b"one")).expect("first");
        wal.fail_next_rotate(RotateFault::CreateNext);

        assert!(matches!(
            wal.append(batch("tenant-a", b"two")),
            Err(WalError::Io(_))
        ));
        assert!(matches!(
            wal.append(batch("tenant-a", b"three")),
            Err(WalError::Failed)
        ));
        drop(wal);

        let mut wal = Wal::open(config).expect("reopen after create fault");
        let next = wal.append(batch("tenant-a", b"two")).expect("retry");
        assert_eq!(next.sequence, 1);
        assert_eq!(next.segment_id, 1);
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(24))]

        #[test]
        fn append_then_reopen_recovers_every_batch(
            batches in proptest::collection::vec(
                (
                    "\\PC{1,16}",
                    proptest::collection::vec(proptest::prelude::any::<u8>(), 0..48),
                    proptest::prelude::any::<u64>(),
                ),
                1..6,
            )
        ) {
            let (_dir, config) = temp_config();
            let mut wal = Wal::open(config.clone()).expect("open");
            let mut receipts = Vec::new();
            for (tenant, payload, received_at) in &batches {
                receipts.push(
                    wal.append(AcceptedBatch {
                        tenant_id: tenant.clone(),
                        signal: Signal::Logs,
                        received_at_unix_nanos: *received_at,
                        payload: Bytes::copy_from_slice(payload),
                    })
                    .expect("append"),
                );
            }
            let path = wal.path().to_owned();
            drop(wal);

            for (index, ((tenant, payload, received_at), receipt)) in
                batches.iter().zip(&receipts).enumerate()
            {
                proptest::prop_assert_eq!(receipt.sequence, index as u64);
                let frame = frame_at(&path, receipt.offset);
                proptest::prop_assert_eq!(frame.sequence, index as u64);
                proptest::prop_assert_eq!(&frame.tenant_id, tenant);
                proptest::prop_assert_eq!(frame.received_at_unix_nanos, *received_at);
                proptest::prop_assert_eq!(frame.payload.as_ref(), payload.as_slice());
            }

            let mut wal = Wal::open(config).expect("reopen");
            let next = wal
                .append(batch("reopen-probe", b"next"))
                .expect("append after reopen");
            proptest::prop_assert_eq!(next.sequence, batches.len() as u64);
        }
    }
}
