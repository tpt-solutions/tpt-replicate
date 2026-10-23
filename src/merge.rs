//! The CRDT executor. It validates incoming operations, holds those whose causes are missing, and
//! applies the rest in timestamp order.
//!
//! A batch is all-or-nothing for validation and quota failures: the merger works on copies of the
//! pending buffer and clock, and state is committed only when the whole batch is accepted. An
//! operation the state itself refuses is different. It is quarantined: logged so its author's
//! sequence numbers keep advancing, skipped by the state, and reported in [`MergeReport::rejected`].
//! Every replica makes the same decision for the same operation, so they stay in agreement.

use alloc::collections::BTreeMap;
use alloc::vec::Vec;

use crate::clock::{Timestamp, VectorClock, DEFAULT_MAX_DRIFT_MS};
use crate::codec::{MAX_DEPS, MAX_PAYLOAD};
use crate::error::{ApplyError, MergeError};
use crate::log::{ReplicatedLog, StampedOperation};
use crate::stubs::crypto::Verifier;
use crate::stubs::{ActorId, State, UirNode};

/// Default lifetime of a causally incomplete operation before it is dropped, in milliseconds.
pub const DEFAULT_PENDING_AGE_MS: u64 = 3_600_000;

/// Bounds the merger enforces on incoming operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeLimits {
    /// Largest number of operations held waiting for causes, across all authors.
    pub max_pending: usize,
    /// Largest number of held operations from any single author. Stops one author from crowding
    /// out the others.
    pub max_pending_per_actor: usize,
    /// Largest total payload size held waiting for causes, in bytes.
    pub max_pending_bytes: usize,
    /// How long a held operation may wait before it is dropped, in milliseconds. A dropped
    /// operation is repaired by the next anti-entropy exchange.
    pub max_pending_age_ms: u64,
    /// How far ahead of local time a remote timestamp may be, in milliseconds.
    pub max_drift_ms: u64,
    /// Largest accepted number of dependency entries per operation.
    pub max_deps: usize,
    /// Largest accepted payload per operation, in bytes.
    pub max_payload: usize,
}

impl Default for MergeLimits {
    fn default() -> Self {
        Self {
            max_pending: 4096,
            max_pending_per_actor: 1024,
            max_pending_bytes: 16 << 20,
            max_pending_age_ms: DEFAULT_PENDING_AGE_MS,
            max_drift_ms: DEFAULT_MAX_DRIFT_MS,
            max_deps: MAX_DEPS,
            max_payload: MAX_PAYLOAD,
        }
    }
}

/// An operation the state refused. It is in the log, so its sequence number is consumed, but its
/// effect is not in the state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rejection {
    /// The author of the rejected operation.
    pub actor: ActorId,
    /// The author's sequence number for the rejected operation.
    pub seq: u64,
    /// Why the state refused it.
    pub error: ApplyError,
}

/// Counts and details from one successfully applied batch.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MergeReport {
    /// Operations applied to the state.
    pub applied: usize,
    /// Operations logged but not applied because they fall at or below the compaction floor.
    pub stale: usize,
    /// Operations already present, which were ignored.
    pub duplicates: usize,
    /// Operations still waiting for causes after this batch.
    pub buffered: usize,
    /// Operations the state refused. They are quarantined, not retried.
    pub rejected: Vec<Rejection>,
}

/// Decides whether an author may apply an operation, and whether the operation is valid for the
/// application. Consulted before the state sees an operation, for local and remote operations
/// alike, so every replica must use the same policy.
///
/// A refusal is handled the same way as a state refusal. Remote operations are quarantined and
/// reported in [`MergeReport::rejected`]. This means a refused operation never stalls its author's
/// later operations.
pub trait OpPolicy {
    /// Returns `Ok` to let `op` through, or the reason it is refused.
    fn check(&self, author: ActorId, op: &UirNode) -> Result<(), ApplyError>;
}

/// The default policy: every operation is allowed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AllowAll;

impl OpPolicy for AllowAll {
    fn check(&self, _author: ActorId, _op: &UirNode) -> Result<(), ApplyError> {
        Ok(())
    }
}

/// Orders pending operations by timestamp, then by author and sequence. The author and sequence
/// keep entries distinct even if a hostile author reuses a timestamp.
type PendingKey = (Timestamp, ActorId, u64);

#[derive(Clone, Debug)]
struct Held {
    op: StampedOperation,
    arrived_ms: u64,
}

/// Causally incomplete operations, indexed by apply order and by `(actor, seq)` for deduplication.
/// Keeps per-author counts and a byte total so quotas are checked without rescanning.
#[derive(Clone, Debug, Default)]
struct Pending {
    by_key: BTreeMap<PendingKey, Held>,
    by_slot: BTreeMap<(ActorId, u64), PendingKey>,
    per_actor: BTreeMap<ActorId, usize>,
    bytes: usize,
}

impl Pending {
    fn len(&self) -> usize {
        self.by_key.len()
    }

    fn get(&self, actor: ActorId, seq: u64) -> Option<&StampedOperation> {
        self.by_slot
            .get(&(actor, seq))
            .and_then(|key| self.by_key.get(key))
            .map(|held| &held.op)
    }

    fn insert(&mut self, op: StampedOperation, now_ms: u64) {
        let key = (op.timestamp, op.actor, op.seq);
        self.by_slot.insert((op.actor, op.seq), key);
        *self.per_actor.entry(op.actor).or_insert(0) += 1;
        self.bytes += op.op.bytes.len();
        self.by_key.insert(
            key,
            Held {
                op,
                arrived_ms: now_ms,
            },
        );
    }

    fn remove(&mut self, key: &PendingKey) -> Option<StampedOperation> {
        let op = self.by_key.remove(key)?.op;
        self.by_slot.remove(&(op.actor, op.seq));
        if let Some(count) = self.per_actor.get_mut(&op.actor) {
            *count -= 1;
            if *count == 0 {
                self.per_actor.remove(&op.actor);
            }
        }
        self.bytes -= op.op.bytes.len();
        Some(op)
    }

    /// Keys of every operation whose causes are all applied, in apply order.
    fn ready_keys(&self, clock: &VectorClock) -> Vec<PendingKey> {
        self.by_key
            .iter()
            .filter(|(_, held)| is_ready(&held.op, clock))
            .map(|(key, _)| *key)
            .collect()
    }

    /// Drops operations that arrived before `cutoff_ms`.
    fn expire(&mut self, cutoff_ms: u64) {
        let stale: Vec<PendingKey> = self
            .by_key
            .iter()
            .filter(|(_, held)| held.arrived_ms < cutoff_ms)
            .map(|(key, _)| *key)
            .collect();
        for key in stale {
            self.remove(&key);
        }
    }

    fn check_quotas(&self, limits: &MergeLimits) -> Result<(), MergeError> {
        if self.len() > limits.max_pending {
            return Err(MergeError::PendingOverflow {
                limit: limits.max_pending,
            });
        }
        if self.bytes > limits.max_pending_bytes {
            return Err(MergeError::PendingBytesExceeded {
                limit: limits.max_pending_bytes,
            });
        }
        for (actor, count) in &self.per_actor {
            if *count > limits.max_pending_per_actor {
                return Err(MergeError::PendingActorOverflow {
                    actor: *actor,
                    limit: limits.max_pending_per_actor,
                });
            }
        }
        Ok(())
    }
}

/// An operation is ready when it is the next in its author's sequence and every operation it
/// depends on has been applied.
fn is_ready(op: &StampedOperation, clock: &VectorClock) -> bool {
    clock.get(op.actor).checked_add(1) == Some(op.seq)
        && op
            .deps
            .peers
            .iter()
            .all(|(actor, seq)| clock.get(*actor) >= *seq)
}

/// Executes remote batches against a replica's state and log.
#[derive(Clone, Debug, Default)]
pub struct Merger {
    limits: MergeLimits,
    pending: Pending,
    floor: Option<Timestamp>,
}

impl Merger {
    /// Creates a merger with the given limits.
    pub fn new(limits: MergeLimits) -> Self {
        Self {
            limits,
            ..Self::default()
        }
    }

    /// Replaces the limits. Keeps the pending buffer.
    pub fn set_limits(&mut self, limits: MergeLimits) {
        self.limits = limits;
    }

    /// Operations at or below this timestamp are logged but not applied. Set by compaction.
    pub fn floor(&self) -> Option<Timestamp> {
        self.floor
    }

    /// Raises the compaction floor. It never lowers it.
    pub fn set_floor(&mut self, cutoff: Timestamp) {
        match self.floor {
            Some(floor) if cutoff <= floor => {}
            _ => self.floor = Some(cutoff),
        }
    }

    /// Operations waiting for causes.
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// Validates and applies a batch of remote operations.
    ///
    /// `now_ms` is the local physical time, used for the drift check and for expiring held
    /// operations. On error, `state`, `log` and the pending buffer are all unchanged. Quota checks
    /// run after ready operations are drained, so a batch that unblocks its own backlog is not
    /// refused for the size of that backlog.
    pub fn apply_remote_ops<S: State>(
        &mut self,
        verifier: &dyn Verifier,
        policy: &dyn OpPolicy,
        now_ms: u64,
        state: &mut S,
        log: &mut ReplicatedLog,
        incoming: Vec<StampedOperation>,
    ) -> Result<MergeReport, MergeError> {
        for op in &incoming {
            self.validate(verifier, now_ms, op)?;
        }

        let mut pending = self.pending.clone();
        let mut clock = log.clock().clone();
        let mut report = MergeReport::default();

        pending.expire(now_ms.saturating_sub(self.limits.max_pending_age_ms));
        for op in incoming {
            if op.seq <= clock.get(op.actor) {
                report.duplicates += 1;
                continue;
            }
            if let Some(known) = pending.get(op.actor, op.seq) {
                if *known != op {
                    return Err(MergeError::Conflict {
                        actor: op.actor,
                        seq: op.seq,
                    });
                }
                report.duplicates += 1;
                continue;
            }
            pending.insert(op, now_ms);
        }

        // The state is cloned only when something will be applied, so duplicate-only batches are
        // cheap. Each pass applies every operation that is ready at its start. Operations that
        // become ready during a pass are picked up by the next one.
        let mut staged: Option<S> = None;
        let mut ready = Vec::new();
        loop {
            let keys = pending.ready_keys(&clock);
            if keys.is_empty() {
                break;
            }
            for key in keys {
                let Some(op) = pending.remove(&key) else {
                    continue;
                };
                clock.observe(op.actor, op.seq);
                match self.floor {
                    Some(floor) if op.timestamp <= floor => report.stale += 1,
                    _ => {
                        let outcome = policy.check(op.actor, &op.op).and_then(|()| {
                            let target = staged.get_or_insert_with(|| state.clone());
                            target.apply(&op.timestamp, &op.op)
                        });
                        match outcome {
                            Ok(()) => report.applied += 1,
                            Err(error) => report.rejected.push(Rejection {
                                actor: op.actor,
                                seq: op.seq,
                                error,
                            }),
                        }
                    }
                }
                ready.push(op);
            }
        }

        pending.check_quotas(&self.limits)?;
        report.buffered = pending.len();
        for op in ready {
            log.insert(op);
        }
        if let Some(staged) = staged {
            *state = staged;
        }
        self.pending = pending;
        Ok(report)
    }

    fn validate(
        &self,
        verifier: &dyn Verifier,
        now_ms: u64,
        op: &StampedOperation,
    ) -> Result<(), MergeError> {
        let (actor, seq) = (op.actor, op.seq);
        let malformed = |reason: &'static str| MergeError::Malformed { actor, seq, reason };
        if seq == 0 {
            return Err(malformed("sequence number is zero"));
        }
        if op.timestamp.actor_id != actor {
            return Err(malformed("timestamp actor does not match author"));
        }
        // The top counter value is refused so folding the timestamp into a local clock can never
        // overflow after the batch has been committed.
        if op.timestamp.logical_counter == u32::MAX {
            return Err(malformed("logical counter at limit"));
        }
        if op.op.bytes.len() > self.limits.max_payload {
            return Err(malformed("payload too large"));
        }
        if op.deps.peers.len() > self.limits.max_deps {
            return Err(malformed("too many dependencies"));
        }
        if op
            .deps
            .peers
            .iter()
            .any(|(dep, dep_seq)| *dep == actor && *dep_seq >= seq)
        {
            return Err(malformed("dependency on own future operation"));
        }
        if op.timestamp.wall_time > now_ms.saturating_add(self.limits.max_drift_ms) {
            return Err(MergeError::BadTimestamp { actor, seq });
        }
        if !verifier.verify(actor, &op.signing_bytes(), &op.signature) {
            return Err(MergeError::BadSignature { actor, seq });
        }
        Ok(())
    }
}
