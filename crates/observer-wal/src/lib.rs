//! Durable, local write-ahead log primitives.

mod codec;
mod error;
mod frame;

pub use codec::{decode, encode};
pub use error::FrameError;
pub use frame::{FORMAT_VERSION, Frame, FrameSignal, MAX_PAYLOAD_LEN, MAX_TENANT_LEN};
