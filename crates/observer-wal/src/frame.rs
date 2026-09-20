use bytes::Bytes;

/// On-disk WAL frame format version.
pub const FORMAT_VERSION: u8 = 1;

/// Maximum accepted tenant identifier length.
pub const MAX_TENANT_LEN: usize = 1024;

/// Maximum accepted raw OTLP payload length.
pub const MAX_PAYLOAD_LEN: usize = 16 * 1024 * 1024;

pub(crate) const LENGTH_SIZE: usize = 4;
pub(crate) const CRC_SIZE: usize = 4;
pub(crate) const FIXED_HEADER_SIZE: usize = 28;

pub(crate) const SIGNAL_LOGS: u8 = 1;
pub(crate) const SIGNAL_TRACES: u8 = 2;
pub(crate) const SIGNAL_METRICS: u8 = 3;

/// Telemetry signal stored in a WAL frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameSignal {
    Logs,
    Traces,
    Metrics,
}

impl FrameSignal {
    pub(crate) fn to_wire(self) -> u8 {
        match self {
            Self::Logs => SIGNAL_LOGS,
            Self::Traces => SIGNAL_TRACES,
            Self::Metrics => SIGNAL_METRICS,
        }
    }

    pub(crate) fn from_wire(value: u8) -> Option<Self> {
        match value {
            SIGNAL_LOGS => Some(Self::Logs),
            SIGNAL_TRACES => Some(Self::Traces),
            SIGNAL_METRICS => Some(Self::Metrics),
            _ => None,
        }
    }
}

/// A decoded WAL frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Frame {
    pub sequence: u64,
    pub signal: FrameSignal,
    pub received_at_unix_nanos: u64,
    pub tenant_id: String,
    pub payload: Bytes,
}
