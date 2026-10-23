//! The replicated operation log: an append-only, timestamp-ordered record of every operation this
//! replica has applied or is holding for its peers.

use alloc::collections::BTreeMap;
use alloc::vec::Vec;

use crate::clock::{Timestamp, VectorClock};
use crate::codec::Writer;
use crate::stubs::{ActorId, UirNode};

/// An operation wrapped with its causal metadata and author signature.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StampedOperation {
    /// HLC timestamp issued by the author when the operation was created.
    pub timestamp: Timestamp,
    /// The author. Must equal `timestamp.actor_id`.
    pub actor: ActorId,
    /// The author's sequence number, starting at 1 and contiguous per actor.
    pub seq: u64,
    /// What the author had applied when it created this operation. Peers apply the operation only
    /// after every entry here is present.
    pub deps: VectorClock,
    /// The serialised operation.
    pub op: UirNode,
    /// Author's signature over [`StampedOperation::signing_bytes`].
    pub signature: Vec<u8>,
}

impl StampedOperation {
    /// Canonical bytes covered by the signature: every field except the signature itself.
    pub fn signing_bytes(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.unsigned_op(self);
        w.finish()
    }
}

/// Causally ordered operation log for one replica.
///
/// Entries are sorted by timestamp. `clock` records the highest sequence number per actor that
/// has been applied. `watermarks` records the latest timestamp seen per actor. `compacted`
/// records the highest sequence number removed by compaction, so the log can tell when it can no
/// longer serve a peer.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReplicatedLog {
    ops: Vec<StampedOperation>,
    clock: VectorClock,
    compacted: VectorClock,
    watermarks: BTreeMap<ActorId, Timestamp>,
}

impl ReplicatedLog {
    /// Creates an empty log.
    pub fn new() -> Self {
        Self::default()
    }

    /// Rebuilds a log from stored entries and the compacted floor. Used when restoring a replica.
    /// The entries may come in any order.
    pub fn from_parts(ops: Vec<StampedOperation>, compacted: VectorClock) -> Self {
        let mut log = Self {
            clock: compacted.clone(),
            compacted,
            ..Self::default()
        };
        for op in ops {
            log.insert(op);
        }
        log
    }

    /// Entries in timestamp order.
    pub fn ops(&self) -> &[StampedOperation] {
        &self.ops
    }

    /// Number of retained entries.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Whether the log holds no entries.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Highest applied sequence number per actor.
    pub fn clock(&self) -> &VectorClock {
        &self.clock
    }

    /// Latest timestamp seen per actor.
    pub fn watermarks(&self) -> &BTreeMap<ActorId, Timestamp> {
        &self.watermarks
    }

    /// Highest sequence number per actor removed by compaction.
    pub fn compacted(&self) -> &VectorClock {
        &self.compacted
    }

    /// Inserts an entry in timestamp order and advances the clock and watermarks.
    ///
    /// The caller must have checked that `(actor, seq)` is not already present.
    pub fn insert(&mut self, op: StampedOperation) {
        let position = self
            .ops
            .partition_point(|entry| entry.timestamp < op.timestamp);
        self.clock.observe(op.actor, op.seq);
        self.watermarks
            .entry(op.actor)
            .and_modify(|latest| {
                if op.timestamp > *latest {
                    *latest = op.timestamp;
                }
            })
            .or_insert(op.timestamp);
        self.ops.insert(position, op);
    }

    /// Recomputes the clock from the retained entries and the compacted floor. Used to check that
    /// the incrementally maintained clock stays correct.
    pub fn derive_clock(&self) -> VectorClock {
        let mut clock = self.compacted.clone();
        for op in &self.ops {
            clock.observe(op.actor, op.seq);
        }
        clock
    }

    /// Whether a peer with `peer_clock` can be brought up to date from this log. It cannot if
    /// compaction removed operations the peer still needs.
    pub fn can_serve(&self, peer_clock: &VectorClock) -> bool {
        self.compacted
            .peers
            .iter()
            .all(|(actor, seq)| peer_clock.get(*actor) >= *seq)
    }

    /// Removes every entry at or before `cutoff` and returns how many were removed. The caller
    /// must have persisted a snapshot first, and must only pass a cutoff that every peer has
    /// acknowledged.
    pub fn compact(&mut self, cutoff: &Timestamp) -> usize {
        let split = self.ops.partition_point(|entry| entry.timestamp <= *cutoff);
        for op in self.ops.drain(..split) {
            self.compacted.observe(op.actor, op.seq);
        }
        split
    }
}
