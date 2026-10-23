//! The CRDT executor. It validates incoming operations, holds those whose causes are missing, and
//! applies the rest in timestamp order.
//!
//! A batch is all-or-nothing for validation and quota failures: the merger works on copies of the
//! pending buffer and clock, and state is committed only when the whole batch is accepted. An
//! operation the state itself refuses is different. It is quarantined: logged so its author's
//! sequence numbers keep advancing, skipped by the state, and reported in [`MergeReport::rejected`].
//! Every replica makes the same decision for the same operation, so they stay in agreement.
//!
//! Waiting operations are indexed by the single cause they are blocked on, so an applied operation
//! wakes exactly the operations that were waiting for it. Draining a backlog costs
//! `O(log n)` per operation, not a rescan of the whole buffer.

use alloc::collections::{BTreeMap, BTreeSet};
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

/// A cause an operation is waiting for: `actor`'s clock must reach `seq`.
type Gate = (ActorId, u64);

/// Where a pending operation sits in the index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Wait {
    /// Inserted this batch and not yet checked against the clock.
    Fresh,
    /// Every cause is applied. It sits in `Pending::ready`.
    Ready,
    /// Blocked on one cause. It sits in `Pending::gates` under that cause.
    On(Gate),
}

#[derive(Clone, Debug)]
struct Held {
    op: StampedOperation,
    arrived_ms: u64,
    wait: Wait,
}

/// Causally incomplete operations, indexed by apply order and by `(actor, seq)` for deduplication.
/// Keeps per-author counts and a byte total so quotas are checked without rescanning.
#[derive(Clone, Debug, Default)]
struct Pending {
    by_key: BTreeMap<PendingKey, Held>,
    by_slot: BTreeMap<(ActorId, u64), PendingKey>,
    per_actor: BTreeMap<ActorId, usize>,
    bytes: usize,
    /// Operations with no unmet causes, in apply order.
    ready: BTreeSet<PendingKey>,
    /// Operations blocked on one cause each, keyed by that cause.
    gates: BTreeMap<Gate, Vec<PendingKey>>,
    /// Inserted this batch, waiting for [`Pending::classify_fresh`].
    fresh: Vec<PendingKey>,
}

/// The first cause `op` is still missing, or `None` if it is ready. Its own predecessor comes
/// before its explicit dependencies.
fn first_unmet(op: &StampedOperation, clock: &VectorClock) -> Option<Gate> {
    let have = clock.get(op.actor);
    if have.saturating_add(1) < op.seq {
        return Some((op.actor, op.seq - 1));
    }
    op.deps
        .peers
        .iter()
        .find(|(actor, seq)| clock.get(**actor) < **seq)
        .map(|(actor, seq)| (*actor, *seq))
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
        self.fresh.push(key);
        self.by_key.insert(
            key,
            Held {
                op,
                arrived_ms: now_ms,
                wait: Wait::Fresh,
            },
        );
    }

    fn remove(&mut self, key: &PendingKey) -> Option<StampedOperation> {
        let held = self.by_key.remove(key)?;
        match held.wait {
            Wait::Fresh => self.fresh.retain(|k| k != key),
            Wait::Ready => {
                self.ready.remove(key);
            }
            Wait::On(gate) => {
                if let Some(waiting) = self.gates.get_mut(&gate) {
                    waiting.retain(|k| k != key);
                    if waiting.is_empty() {
                        self.gates.remove(&gate);
                    }
                }
            }
        }
        let op = held.op;
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

    /// Files `key` under its first unmet cause, or in the ready set if it has none.
    fn classify(&mut self, key: PendingKey, clock: &VectorClock) {
        let Some(held) = self.by_key.get_mut(&key) else {
            return;
        };
        let wait = match first_unmet(&held.op, clock) {
            None => {
                self.ready.insert(key);
                Wait::Ready
            }
            Some(gate) => {
                self.gates.entry(gate).or_default().push(key);
                Wait::On(gate)
            }
        };
        held.wait = wait;
    }

    /// Puts every waiting operation back in the fresh list. Needed when the clock moved without
    /// going through this merger, which a snapshot install does.
    fn reclassify_all(&mut self) {
        let mut keys: Vec<PendingKey> = core::mem::take(&mut self.ready).into_iter().collect();
        for (_, waiting) in core::mem::take(&mut self.gates) {
            keys.extend(waiting);
        }
        for key in &keys {
            if let Some(held) = self.by_key.get_mut(key) {
                held.wait = Wait::Fresh;
            }
        }
        self.fresh.extend(keys);
    }

    /// Checks every operation inserted since the last call against the clock.
    fn classify_fresh(&mut self, clock: &VectorClock) {
        for key in core::mem::take(&mut self.fresh) {
            self.classify(key, clock);
        }
    }

    /// Re-checks every operation waiting on a cause that `actor`'s clock has now reached.
    fn release(&mut self, actor: ActorId, clock: &VectorClock) {
        let upto = clock.get(actor);
        let due: Vec<Gate> = self
            .gates
            .range((actor, 0)..=(actor, upto))
            .map(|(gate, _)| *gate)
            .collect();
        for gate in due {
            for key in self.gates.remove(&gate).unwrap_or_default() {
                self.classify(key, clock);
            }
        }
    }

    /// Removes and returns the earliest ready operation.
    fn pop_ready(&mut self) -> Option<PendingKey> {
        self.ready.pop_first()
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

/// Executes remote batches against a replica's state and log.
#[derive(Clone, Debug, Default)]
pub struct Merger {
    limits: MergeLimits,
    pending: Pending,
    floor: Option<Timestamp>,
    /// The log clock the pending index was last filed against.
    synced: Option<VectorClock>,
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

        // Waiting operations were filed against the clock as it stood after the last batch. If
        // the log's clock has moved since (a snapshot install does this), file them again.
        if self.synced.as_ref() != Some(&clock) {
            pending.reclassify_all();
        }
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
        pending.classify_fresh(&clock);

        // The state is cloned only when something will be applied, so duplicate-only batches are
        // cheap. Operations are applied in timestamp order among those that are ready. Applying
        // one may make others ready, and they join the ready set as their causes are released.
        let mut staged: Option<S> = None;
        let mut ready = Vec::new();
        while let Some(key) = pending.pop_ready() {
            let Some(op) = pending.remove(&key) else {
                continue;
            };
            // Only the next operation in an author's sequence may be applied. A stale entry is
            // dropped without being applied.
            if clock.get(op.actor).checked_add(1) != Some(op.seq) {
                continue;
            }
            clock.observe(op.actor, op.seq);
            pending.release(op.actor, &clock);
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

        pending.check_quotas(&self.limits)?;
        report.buffered = pending.len();
        for op in ready {
            log.insert(op);
        }
        if let Some(staged) = staged {
            *state = staged;
        }
        self.pending = pending;
        self.synced = Some(clock);
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
