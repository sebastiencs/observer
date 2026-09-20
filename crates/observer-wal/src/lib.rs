//! Durable, local write-ahead log primitives.

mod codec;
mod error;
mod frame;
mod recovery;
mod segment;
mod wal;

pub use codec::{decode, encode};
pub use error::{FrameError, WalError};
pub use frame::{FORMAT_VERSION, Frame, FrameSignal, MAX_PAYLOAD_LEN, MAX_TENANT_LEN};
pub use segment::SegmentHeader;
pub use wal::{Receipt, Wal, WalConfig};
