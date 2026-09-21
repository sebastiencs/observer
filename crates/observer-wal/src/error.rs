use std::{error::Error, fmt, io};

/// Errors produced while encoding or decoding a WAL frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FrameError {
    /// The buffer ends before a complete, length-consistent frame.
    Incomplete,
    /// Declared lengths overflow or cannot describe a valid frame.
    InvalidLength,
    /// Header lengths do not match `total_length`.
    LengthMismatch {
        declared: u32,
        expected: u64,
    },
    UnsupportedVersion {
        version: u8,
    },
    UnknownSignal {
        signal: u8,
    },
    UnknownFlags {
        flags: u16,
    },
    ReservedMustBeZero {
        reserved: u16,
    },
    TenantTooLong {
        length: usize,
    },
    PayloadTooLong {
        length: usize,
    },
    InvalidTenantUtf8,
    ChecksumMismatch,
}

impl fmt::Display for FrameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Incomplete => formatter.write_str("incomplete WAL frame"),
            Self::InvalidLength => formatter.write_str("invalid WAL frame length"),
            Self::LengthMismatch { declared, expected } => {
                write!(
                    formatter,
                    "WAL frame length mismatch: declared {declared}, expected {expected}"
                )
            }
            Self::UnsupportedVersion { version } => {
                write!(formatter, "unsupported WAL frame version {version}")
            }
            Self::UnknownSignal { signal } => {
                write!(formatter, "unknown WAL signal discriminant {signal}")
            }
            Self::UnknownFlags { flags } => {
                write!(formatter, "unknown WAL frame flags {flags:#06x}")
            }
            Self::ReservedMustBeZero { reserved } => {
                write!(formatter, "WAL reserved field must be zero, got {reserved}")
            }
            Self::TenantTooLong { length } => {
                write!(formatter, "WAL tenant id is too long ({length} bytes)")
            }
            Self::PayloadTooLong { length } => {
                write!(formatter, "WAL payload is too long ({length} bytes)")
            }
            Self::InvalidTenantUtf8 => formatter.write_str("WAL tenant id is not valid UTF-8"),
            Self::ChecksumMismatch => formatter.write_str("WAL frame checksum mismatch"),
        }
    }
}

impl Error for FrameError {}

/// Errors from WAL directory, segment, and append operations.
#[derive(Debug)]
pub enum WalError {
    /// A previous write or sync failed; the WAL must be reopened.
    Failed,
    Frame(FrameError),
    Io(io::Error),
    ShortWrite {
        written: usize,
        expected: usize,
    },
    EntryTooLarge {
        size: usize,
        max: usize,
    },
    IncompleteSegment,
    /// A complete frame or segment is corrupt before the physical tail.
    Corrupt(&'static str),
    InvalidSegmentHeader(&'static str),
    UnsupportedSegmentVersion {
        version: u16,
    },
    UnexpectedLane {
        lane_id: u32,
    },
    InvalidConfig(&'static str),
    /// A checkpoint file exists but is truncated, badly versioned, or corrupt.
    InvalidCheckpoint(&'static str),
    /// An attempted commit is behind the last durable checkpoint.
    CheckpointRegression {
        committed: u64,
        attempted: u64,
    },
}

#[cfg(test)]
impl WalError {
    pub(crate) fn io(kind: io::ErrorKind, message: impl Into<String>) -> Self {
        Self::Io(io::Error::new(kind, message.into()))
    }
}

impl fmt::Display for WalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failed => formatter.write_str("WAL is in a failed state and must be reopened"),
            Self::Frame(error) => write!(formatter, "{error}"),
            Self::Io(error) => write!(formatter, "WAL I/O error: {error}"),
            Self::ShortWrite { written, expected } => {
                write!(
                    formatter,
                    "short WAL write: wrote {written} of {expected} bytes"
                )
            }
            Self::EntryTooLarge { size, max } => {
                write!(formatter, "WAL entry is too large ({size} > {max} bytes)")
            }
            Self::IncompleteSegment => {
                formatter.write_str("WAL segment ends with an incomplete frame")
            }
            Self::Corrupt(reason) => write!(formatter, "WAL corruption: {reason}"),
            Self::InvalidSegmentHeader(reason) => {
                write!(formatter, "invalid WAL segment header: {reason}")
            }
            Self::UnsupportedSegmentVersion { version } => {
                write!(formatter, "unsupported WAL segment version {version}")
            }
            Self::UnexpectedLane { lane_id } => {
                write!(formatter, "unexpected WAL lane id {lane_id}")
            }
            Self::InvalidConfig(reason) => write!(formatter, "invalid WAL config: {reason}"),
            Self::InvalidCheckpoint(reason) => {
                write!(formatter, "invalid WAL checkpoint: {reason}")
            }
            Self::CheckpointRegression {
                committed,
                attempted,
            } => write!(
                formatter,
                "WAL checkpoint regression: attempted sequence {attempted} is behind committed {committed}"
            ),
        }
    }
}

impl Error for WalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Frame(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<FrameError> for WalError {
    fn from(error: FrameError) -> Self {
        Self::Frame(error)
    }
}

impl From<io::Error> for WalError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
