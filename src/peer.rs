//! Per-peer connection state.
//!
//! Lifecycle: `Connecting` until the transport is up, then `Syncing` while the handshake runs,
//! then `UpToDate` once the peer's `Ack` arrives. A dead link goes to `Disconnected`. A link that
//! comes back up restarts the handshake.
//!
//! The offline queue is the log itself. A peer that is not `UpToDate` gets no live broadcasts.
//! The handshake delta replays everything it is missing, so nothing has to be buffered per peer.

use alloc::vec::Vec;

use crate::clock::VectorClock;

/// Most pings kept while waiting for pongs.
const MAX_OUTSTANDING_PINGS: usize = 8;
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
    /// Nonces of pings sent and not yet answered. A pong must match one of them.
    pub outstanding_pings: Vec<u64>,
    /// Nonces of recently answered pings. A repeat pong is a duplicated frame, not a forgery.
    pub answered_pings: Vec<u64>,
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
            outstanding_pings: Vec::new(),
            answered_pings: Vec::new(),
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

    /// Records a ping sent at `now`. Only the most recent few are kept, so a lost pong does not
    /// grow the list without bound.
    pub fn record_ping(&mut self, now: u64) {
        self.last_ping_ms = now;
        self.outstanding_pings.push(now);
        if self.outstanding_pings.len() > MAX_OUTSTANDING_PINGS {
            self.outstanding_pings.remove(0);
        }
    }

    /// Consumes the ping that `nonce` answers. Returns `false` if `nonce` answers no ping this replica
    /// sent, neither outstanding nor recently answered. A repeat of a recent answer returns `true`,
    /// because it is a duplicated frame.
    pub fn take_pong(&mut self, nonce: u64) -> bool {
        match self
            .outstanding_pings
            .iter()
            .position(|sent| *sent == nonce)
        {
            Some(index) => {
                self.outstanding_pings.remove(index);
                self.answered_pings.push(nonce);
                if self.answered_pings.len() > MAX_OUTSTANDING_PINGS {
                    self.answered_pings.remove(0);
                }
                true
            }
            None => self.answered_pings.contains(&nonce),
        }
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
