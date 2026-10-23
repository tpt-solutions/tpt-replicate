//! Per-peer connection state.
//!
//! Lifecycle: `Connecting` until the transport is up, then `Syncing` while the handshake runs,
//! then `UpToDate` once the peer's `Ack` arrives. A dead link goes to `Disconnected`. A link that
//! comes back up restarts the handshake.
//!
//! The offline queue is the log itself. A peer that is not `UpToDate` gets no live broadcasts.
//! The handshake delta replays everything it is missing, so nothing has to be buffered per peer.

use crate::clock::VectorClock;
use crate::log::ReplicatedLog;
use crate::stubs::ActorId;

/// Connection status of one peer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerState {
    /// The transport is not up yet.
    Connecting,
    /// The handshake is in progress.
    Syncing,
    /// The peer has acknowledged our clock. Live operations are sent immediately.
    UpToDate,
    /// The link failed or timed out. Operations accumulate in the log until reconnection.
    Disconnected,
}

/// What this replica knows about one peer.
#[derive(Clone, Debug)]
pub struct Peer {
    /// The peer's identity. Frames from this link must be signed by it.
    pub id: ActorId,
    /// Connection status.
    pub state: PeerState,
    /// Latest clock received from the peer. Used for delta calculation and the stable frontier.
    pub remote_clock: VectorClock,
    /// Physical time of the last frame received.
    pub last_seen_ms: u64,
    /// Physical time of the last ping sent.
    pub last_ping_ms: u64,
    /// Physical time of the last hello sent.
    pub last_hello_ms: u64,
}

impl Peer {
    /// Creates a peer in the `Connecting` state.
    pub fn new(id: ActorId) -> Self {
        Self {
            id,
            state: PeerState::Connecting,
            remote_clock: VectorClock::new(),
            last_seen_ms: 0,
            last_ping_ms: 0,
            last_hello_ms: 0,
        }
    }

    /// Marks the start of a handshake at `now`.
    pub fn connection_started(&mut self, now: u64) {
        self.state = PeerState::Syncing;
        self.last_seen_ms = now;
        self.last_ping_ms = now;
        self.last_hello_ms = now;
    }

    /// Marks the link as lost.
    pub fn connection_lost(&mut self) {
        self.state = PeerState::Disconnected;
    }

    /// Folds in a clock the peer reported in an `Ack`. Acks can arrive late, so this only raises
    /// the remote clock.
    pub fn absorb_clock(&mut self, clock: &VectorClock) {
        self.remote_clock.merge(clock);
    }

    /// Replaces the remote clock with what the peer reported in a `Hello`. A `Hello` describes the
    /// peer's state at the start of a handshake. If the peer restarted and lost data, its clock is
    /// lower than what we recorded before. Keeping the old, higher value would overstate what the
    /// peer has, which would let compaction drop operations it still needs.
    pub fn replace_clock(&mut self, clock: &VectorClock) {
        self.remote_clock = clock.clone();
    }

    /// Pulls every timer back to `now` if the physical clock moved backwards. Otherwise a timer set
    /// in the future would never expire.
    pub fn rebase_timers(&mut self, now: u64) {
        for timer in [
            &mut self.last_seen_ms,
            &mut self.last_ping_ms,
            &mut self.last_hello_ms,
        ] {
            if *timer > now {
                *timer = now;
            }
        }
    }

    /// Operations in `log` this peer has not acknowledged. This is the offline queue depth.
    pub fn pending_ops(&self, log: &ReplicatedLog) -> usize {
        log.ops()
            .iter()
            .filter(|op| op.seq > self.remote_clock.get(op.actor))
            .count()
    }
}
