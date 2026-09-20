use std::{error::Error, fmt};

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
