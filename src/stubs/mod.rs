//! Local stand-ins for sibling TPT crates. Each sub-module mirrors the trait shape the engine
//! needs, so the real crates can replace them without touching the engine.
//!
//! | Stub | Stands in for | Notes |
//! |------|---------------|-------|
//! | [`ActorId`], [`UirNode`] | tpt-primitives, tpt-uir | Plain data |
//! | [`CrdtCompatible`], [`State`] | tpt-operations | Apply and state hash |
//! | [`crypto`] | tpt-crypto | **Not cryptographically secure** |
//! | [`SnapshotSink`] | tpt-version | In-memory sink |
//! | [`DataChannel`] | tpt-webrtc | Message-oriented channel |
//! | [`lww`] | tpt-operations | Last-writer-wins map with tombstones |

pub mod crypto;
pub mod lww;

use alloc::vec::Vec;

use crate::clock::Timestamp;
use crate::error::{ApplyError, SnapshotError, TransportError};

/// Identifies a replica. Totally ordered so it can break timestamp ties.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ActorId(pub u64);

/// Opaque serialised operation payload. Its meaning is defined by the state's `apply`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UirNode {
    /// Serialised payload bytes.
    pub bytes: Vec<u8>,
}

/// Operations that know how to merge themselves into a state.
pub trait CrdtCompatible {
    /// Applies one operation stamped with `stamp`.
    ///
    /// Must be idempotent for a repeated `(stamp, op)`. Must leave the state unchanged when it
    /// returns an error, because the merge engine relies on that for batch atomicity.
    fn apply(&mut self, stamp: &Timestamp, op: &UirNode) -> Result<(), ApplyError>;
}

/// A replicated state the engine can drive.
pub trait State: CrdtCompatible + Clone {
    /// Hash of the observable state. Two replicas with equal observable state hash equal, even if
    /// one has compacted metadata the other still holds.
    fn state_hash(&self) -> u64;

    /// Drops metadata that is no longer needed for ordering at or before `cutoff`, such as
    /// tombstones.
    fn compact(&mut self, cutoff: &Timestamp);

    /// Serialises the full state, tombstones included, for snapshot transfer. Must round-trip
    /// through [`State::decode_state`] to a state with the same [`State::state_hash`].
    fn encode_state(&self) -> Vec<u8>;

    /// Rebuilds a state from bytes made by [`State::encode_state`]. Returns an error for malformed
    /// input rather than panicking, because the bytes come from a peer.
    fn decode_state(bytes: &[u8]) -> Result<Self, SnapshotError>
    where
        Self: Sized;
}

/// Summary of a point in the log that is safe to compact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// Every operation at or below this timestamp is covered by the snapshot.
    pub cutoff: Timestamp,
    /// [`State::state_hash`] at the time of the snapshot.
    pub state_hash: u64,
    /// Number of log entries before compaction.
    pub op_count: u64,
}

/// Persists snapshots before the log compacts. Compaction only runs after `persist` succeeds.
pub trait SnapshotSink {
    /// Stores a snapshot durably. `state` is the encoded state at the snapshot point, from
    /// [`State::encode_state`]. A restart or a late joiner needs it.
    fn persist(&mut self, snapshot: &Snapshot, state: &[u8]) -> Result<(), SnapshotError>;
}

/// In-memory [`SnapshotSink`] for tests and examples.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MemorySnapshotSink {
    /// Every snapshot persisted so far, oldest first.
    pub snapshots: Vec<Snapshot>,
}

impl SnapshotSink for MemorySnapshotSink {
    fn persist(&mut self, snapshot: &Snapshot, _state: &[u8]) -> Result<(), SnapshotError> {
        self.snapshots.push(snapshot.clone());
        Ok(())
    }
}

/// Message-oriented data channel, as provided by a WebRTC stack.
pub trait DataChannel {
    /// Sends one message.
    fn send(&mut self, data: &[u8]) -> Result<(), TransportError>;
    /// Returns the next received message, if any, without blocking.
    fn try_recv(&mut self) -> Option<Vec<u8>>;
    /// Whether the channel is open.
    fn is_open(&self) -> bool;
}
