//! Tombstone garbage collection: choosing how far the log and state can be compacted.
//!
//! A cutoff is safe when two conditions hold:
//!
//! 1. Every peer has acknowledged the operations at or before it. This is the stable frontier.
//! 2. No actor this replica knows about can still produce an unseen operation at or before it.
//!    Any such operation would carry a timestamp above that actor's latest seen watermark, so the
//!    cutoff is capped at the lowest watermark.
//!
//! Operations from an actor this replica has never seen are not covered by these checks. Those
//! operations are rejected once compaction has passed them, and the peer needs a snapshot to
//! rejoin.

use crate::clock::{Timestamp, VectorClock};
use crate::log::ReplicatedLog;

/// Latest timestamp that is safe to compact to, or `None` if nothing is safe yet.
pub fn cutoff(log: &ReplicatedLog, frontier: &VectorClock) -> Option<Timestamp> {
    let watermark_floor = log.watermarks().values().min().copied()?;
    let mut cut = None;
    for op in log.ops() {
        if op.timestamp > watermark_floor || op.seq > frontier.get(op.actor) {
            break;
        }
        cut = Some(op.timestamp);
    }
    cut
}
