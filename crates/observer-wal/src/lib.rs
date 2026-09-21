//! Durable, local write-ahead log primitives.

mod async_wal;
mod checkpoint;
mod codec;
#[cfg(test)]
mod crash_fs;
#[cfg(test)]
mod crash_tests;
mod cursor;
mod error;
mod frame;
#[cfg(any(test, feature = "test-util"))]
mod io_hooks;
mod lane_io;
mod reader;
mod recovery;
mod retention;
mod segment;
#[cfg(test)]
mod state_machine;
mod tenant;
mod wal;

pub use async_wal::{AsyncWal, WalWriterConfig};
pub use checkpoint::WalCheckpoint;
pub use codec::{decode, encode, encoded_frame_size};
pub use cursor::{WalCursor, WalRecord};
pub use error::{FrameError, WalError};
pub use frame::{FORMAT_VERSION, Frame, FrameSignal, MAX_PAYLOAD_LEN, MAX_TENANT_LEN};
#[cfg(any(test, feature = "test-util"))]
pub use io_hooks::WalIoHooks;
pub use reader::WalReader;
pub use retention::{RetentionReport, retain_committed};
pub use segment::{SEGMENT_HEADER_SIZE, SegmentHeader};
pub use tenant::{TenantWalRouter, tenant_wal_directory};
pub use wal::{Receipt, Wal, WalConfig};
