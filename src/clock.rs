//! Logical time: Hybrid Logical Clocks for total ordering and vector clocks for causal history.

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::rc::Rc;
use core::cell::Cell;
use core::cmp::Ordering;

use crate::error::ClockError;
use crate::stubs::ActorId;

/// Default bound on how far ahead of local time a remote timestamp may be, in milliseconds.
pub const DEFAULT_MAX_DRIFT_MS: u64 = 60_000;

/// A totally ordered HLC timestamp. Ordering is `(wall_time, logical_counter, actor_id)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp {
    /// Physical time in milliseconds, as read from the injected [`ClockSource`].
    pub wall_time: u64,
    /// Orders events that share a `wall_time`.
    pub logical_counter: u32,
    /// The actor that issued the timestamp. Breaks every remaining tie.
    pub actor_id: ActorId,
}

/// Source of physical time. Injected so the crate stays `no_std`.
pub trait ClockSource {
    /// Current physical time in milliseconds.
    fn now_ms(&self) -> u64;
}

/// A manually driven clock for tests and simulations. Clones share one time value.
#[derive(Clone, Debug, Default)]
pub struct ManualClock {
    now: Rc<Cell<u64>>,
}

impl ManualClock {
    /// Creates a clock reading `start_ms`.
    pub fn new(start_ms: u64) -> Self {
        Self {
            now: Rc::new(Cell::new(start_ms)),
        }
    }

    /// Sets the current time.
    pub fn set(&self, ms: u64) {
        self.now.set(ms);
    }

    /// Moves time forward by `ms`.
    pub fn advance(&self, ms: u64) {
        self.now.set(self.now.get().saturating_add(ms));
    }
}

impl ClockSource for ManualClock {
    fn now_ms(&self) -> u64 {
        self.now.get()
    }
}

/// Wall-clock source backed by `std::time::SystemTime`.
#[cfg(feature = "std")]
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

#[cfg(feature = "std")]
impl ClockSource for SystemClock {
    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|d| u64::try_from(d.as_millis()).ok())
            .unwrap_or(0)
    }
}

/// Hybrid Logical Clock for one actor.
///
/// Every timestamp it issues is strictly greater than the last one it issued. Timestamps it
/// receives from remote peers advance it, which keeps causally later events ordered after their
/// causes even when physical clocks disagree.
pub struct HybridClock {
    actor: ActorId,
    source: Box<dyn ClockSource>,
    last_wall: u64,
    last_counter: u32,
    max_drift_ms: u64,
}

impl HybridClock {
    /// Creates a clock for `actor` reading time from `source`.
    pub fn new(actor: ActorId, source: Box<dyn ClockSource>) -> Self {
        Self {
            actor,
            source,
            last_wall: 0,
            last_counter: 0,
            max_drift_ms: DEFAULT_MAX_DRIFT_MS,
        }
    }

    /// Sets the maximum accepted lead of remote timestamps over local time.
    pub fn set_max_drift(&mut self, max_drift_ms: u64) {
        self.max_drift_ms = max_drift_ms;
    }

    /// Current physical time from the injected source.
    pub fn physical_now(&self) -> u64 {
        self.source.now_ms()
    }

    /// Issues a new local timestamp.
    pub fn now(&mut self) -> Result<Timestamp, ClockError> {
        let physical = self.source.now_ms();
        if physical > self.last_wall {
            self.last_wall = physical;
            self.last_counter = 0;
        } else {
            self.last_counter = self
                .last_counter
                .checked_add(1)
                .ok_or(ClockError::CounterOverflow)?;
        }
        Ok(Timestamp {
            wall_time: self.last_wall,
            logical_counter: self.last_counter,
            actor_id: self.actor,
        })
    }

    /// Folds a remote timestamp into the local clock.
    ///
    /// Rejects timestamps further ahead than the configured drift. Call it only after the
    /// timestamp's operation has been authenticated, so an unauthenticated peer cannot move this
    /// clock.
    pub fn update(&mut self, remote: &Timestamp) -> Result<(), ClockError> {
        let physical = self.source.now_ms();
        if remote.wall_time > physical.saturating_add(self.max_drift_ms) {
            return Err(ClockError::RemoteTooFarAhead {
                remote_wall: remote.wall_time,
                local_wall: physical,
            });
        }
        let wall = self.last_wall.max(remote.wall_time).max(physical);
        let counter = if wall == self.last_wall && wall == remote.wall_time {
            self.last_counter.max(remote.logical_counter).checked_add(1)
        } else if wall == self.last_wall {
            self.last_counter.checked_add(1)
        } else if wall == remote.wall_time {
            remote.logical_counter.checked_add(1)
        } else {
            Some(0)
        };
        self.last_counter = counter.ok_or(ClockError::CounterOverflow)?;
        self.last_wall = wall;
        Ok(())
    }
}

/// Causal relationship between two vector clocks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CausalOrder {
    /// `self` happened strictly before `other`.
    Before,
    /// `self` happened strictly after `other`.
    After,
    /// The clocks are identical.
    Equal,
    /// Neither happened before the other.
    Concurrent,
}

/// Per-actor count of operations seen, used to decide what a peer is missing.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VectorClock {
    /// Highest sequence number seen from each actor. Absent means zero.
    pub peers: BTreeMap<ActorId, u64>,
}

impl VectorClock {
    /// Creates an empty clock.
    pub fn new() -> Self {
        Self::default()
    }

    /// Highest sequence number seen from `actor`.
    pub fn get(&self, actor: ActorId) -> u64 {
        self.peers.get(&actor).copied().unwrap_or(0)
    }

    /// Raises the entry for `actor` to at least `seq`.
    pub fn observe(&mut self, actor: ActorId, seq: u64) {
        if seq == 0 {
            return;
        }
        let entry = self.peers.entry(actor).or_insert(0);
        if seq > *entry {
            *entry = seq;
        }
    }

    /// Increments the entry for `actor` and returns the new value.
    pub fn increment(&mut self, actor: ActorId) -> Result<u64, ClockError> {
        let entry = self.peers.entry(actor).or_insert(0);
        *entry = entry.checked_add(1).ok_or(ClockError::CounterOverflow)?;
        Ok(*entry)
    }

    /// Pointwise maximum with `other`.
    pub fn merge(&mut self, other: &VectorClock) {
        for (actor, seq) in &other.peers {
            self.observe(*actor, *seq);
        }
    }

    /// Whether every entry of `other` is covered by `self`.
    pub fn dominates(&self, other: &VectorClock) -> bool {
        other
            .peers
            .iter()
            .all(|(actor, seq)| self.get(*actor) >= *seq)
    }

    /// Compares causal history with `other`.
    pub fn compare(&self, other: &VectorClock) -> CausalOrder {
        match (self.dominates(other), other.dominates(self)) {
            (true, true) => CausalOrder::Equal,
            (true, false) => CausalOrder::After,
            (false, true) => CausalOrder::Before,
            (false, false) => CausalOrder::Concurrent,
        }
    }
}

impl PartialOrd for VectorClock {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.compare(other) {
            CausalOrder::Equal => Some(Ordering::Equal),
            CausalOrder::Before => Some(Ordering::Less),
            CausalOrder::After => Some(Ordering::Greater),
            CausalOrder::Concurrent => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clock(actor: u64, wall: u64) -> (HybridClock, ManualClock) {
        let source = ManualClock::new(wall);
        (
            HybridClock::new(ActorId(actor), Box::new(source.clone())),
            source,
        )
    }

    #[test]
    fn timestamps_are_strictly_increasing_under_skew() {
        let (mut hlc, source) = clock(1, 100);
        let mut last = hlc.now().unwrap();
        for step in 0..1_000u64 {
            // Physical time sometimes stalls or goes backwards.
            source.set(100 + (step % 7) * 3 - (step % 2));
            let next = hlc.now().unwrap();
            assert!(next > last, "{next:?} must follow {last:?}");
            last = next;
        }
    }

    #[test]
    fn remote_update_orders_later_than_remote() {
        let (mut hlc, _) = clock(1, 100);
        let remote = Timestamp {
            wall_time: 150,
            logical_counter: 4,
            actor_id: ActorId(2),
        };
        hlc.update(&remote).unwrap();
        let local = hlc.now().unwrap();
        assert!(local > remote);
        assert_eq!(local.wall_time, 150);
    }

    #[test]
    fn far_future_remote_is_rejected_and_clock_unchanged() {
        let (mut hlc, _) = clock(1, 100);
        let before = hlc.now().unwrap();
        let bogus = Timestamp {
            wall_time: 100 + DEFAULT_MAX_DRIFT_MS + 1,
            logical_counter: 0,
            actor_id: ActorId(2),
        };
        assert!(matches!(
            hlc.update(&bogus),
            Err(ClockError::RemoteTooFarAhead { .. })
        ));
        assert!(hlc.now().unwrap() > before);
        assert!(hlc.now().unwrap().wall_time < bogus.wall_time);
    }

    #[test]
    fn counter_overflow_is_reported() {
        let (mut hlc, _) = clock(1, 100);
        let remote = Timestamp {
            wall_time: 100,
            logical_counter: u32::MAX,
            actor_id: ActorId(2),
        };
        assert_eq!(hlc.update(&remote), Err(ClockError::CounterOverflow));
    }

    #[test]
    fn vector_clock_compare() {
        let mut a = VectorClock::new();
        let mut b = VectorClock::new();
        a.observe(ActorId(1), 2);
        b.observe(ActorId(1), 2);
        assert_eq!(a.compare(&b), CausalOrder::Equal);
        b.observe(ActorId(2), 1);
        assert_eq!(a.compare(&b), CausalOrder::Before);
        a.observe(ActorId(3), 1);
        assert_eq!(a.compare(&b), CausalOrder::Concurrent);
        assert_eq!(a.partial_cmp(&b), None);
    }

    #[test]
    fn vector_clock_merge_is_pointwise_max() {
        let mut a = VectorClock::new();
        let mut b = VectorClock::new();
        a.observe(ActorId(1), 5);
        b.observe(ActorId(1), 3);
        b.observe(ActorId(2), 7);
        a.merge(&b);
        assert_eq!(a.get(ActorId(1)), 5);
        assert_eq!(a.get(ActorId(2)), 7);
    }
}
