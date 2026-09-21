use crate::{Frame, SEGMENT_HEADER_SIZE};

/// Exclusive position of the next unread WAL frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalCursor {
    next_sequence: u64,
    segment_id: u64,
    offset: u64,
}

impl WalCursor {
    /// The first unread position of an empty or newly created lane.
    #[must_use]
    pub fn start() -> Self {
        Self {
            next_sequence: 0,
            segment_id: 0,
            offset: u64::try_from(SEGMENT_HEADER_SIZE).expect("header size"),
        }
    }

    #[must_use]
    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    #[must_use]
    pub fn segment_id(&self) -> u64 {
        self.segment_id
    }

    #[must_use]
    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub(crate) fn at(next_sequence: u64, segment_id: u64, offset: u64) -> Self {
        Self {
            next_sequence,
            segment_id,
            offset,
        }
    }
}

/// One decoded frame and the exclusive cursor that follows it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalRecord {
    pub frame: Frame,
    pub next_cursor: WalCursor,
}
