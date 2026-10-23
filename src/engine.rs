//! The replica: the type applications and transports talk to.
//!
//! A local operation is stamped, signed, applied, logged and broadcast. Remote frames go through
//! envelope authentication, decoding and the merge executor. Compaction runs only after a snapshot
//! has been persisted.
//!
//! A [`Replica`] is not `Send`. Its clock source may hold non-`Send` state, such as an `Rc`-backed
//! test clock. Run each replica on the thread that owns its clock.

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::vec;
use alloc::vec::Vec;

use crate::clock::{ClockSource, HybridClock, Timestamp, VectorClock};
use crate::codec::{BinaryCodec, Codec, MAX_DEPS, MAX_FRAME, MAX_OPS_PER_MESSAGE, MAX_PAYLOAD};
use crate::error::{
    ApplyError, ClockError, EngineError, FrameError, MergeError, RestoreError, SnapshotError,
    TransportError,
};
use crate::gc;
use crate::log::{ReplicatedLog, StampedOperation};
use crate::merge::{AllowAll, MergeLimits, MergeReport, Merger, OpPolicy, DEFAULT_PENDING_AGE_MS};
use crate::peer::{Peer, PeerState};
use crate::stubs::crypto::{Signer, Verifier};
use crate::stubs::{ActorId, Snapshot, SnapshotSink, State, UirNode};
use crate::sync::{calculate_delta, chunk_ops, Envelope, Message};
use crate::transport::Transport;

/// Most events kept while the application has not drained them. Older events are dropped first.
pub const MAX_QUEUED_EVENTS: usize = 1024;

/// Most frames read from one link per [`Replica::poll`]. Keeps one busy link from starving the rest.
const MAX_FRAMES_PER_POLL: usize = 1024;

/// Byte budget for one `Delta` frame before compression. Half the frame limit leaves room for the
/// envelope, the codec header and any expansion from compression.
const DELTA_BYTE_BUDGET: usize = MAX_FRAME / 2;

/// Tuning knobs for a replica. All intervals are in milliseconds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaConfig {
    /// Most operations per `Delta` frame. Clamped to the protocol maximum when applied.
    pub max_ops_per_frame: usize,
    /// How often to send a ping.
    pub ping_interval_ms: u64,
    /// How often to re-send a hello. This is anti-entropy: it repairs frames lost in transit.
    pub resync_interval_ms: u64,
    /// Silence longer than this declares a peer dead.
    pub peer_timeout_ms: u64,
    /// Silence longer than this declares a peer dead on a delay-tolerant link, such as a courier
    /// route. Such links are often quiet for hours between passes.
    pub delay_tolerant_timeout_ms: u64,
    /// Attempt compaction after this many operations.
    pub snapshot_every_ops: u64,
    /// Attempt compaction after this much time.
    pub snapshot_every_ms: u64,
    /// Bound on operations waiting for causes.
    pub max_pending: usize,
    /// Bound on operations waiting for causes from any single author.
    pub max_pending_per_actor: usize,
    /// Bound on payload bytes waiting for causes.
    pub max_pending_bytes: usize,
    /// How long an operation may wait for its causes before it is dropped.
    pub max_pending_age_ms: u64,
    /// Bound on how far ahead of local time remote timestamps may be.
    pub max_drift_ms: u64,
}

impl Default for ReplicaConfig {
    fn default() -> Self {
        Self {
            max_ops_per_frame: 128,
            ping_interval_ms: 1_000,
            resync_interval_ms: 5_000,
            peer_timeout_ms: 10_000,
            delay_tolerant_timeout_ms: 86_400_000,
            snapshot_every_ops: 1_000,
            snapshot_every_ms: 3_600_000,
            max_pending: 4_096,
            max_pending_per_actor: 1_024,
            max_pending_bytes: 16 << 20,
            max_pending_age_ms: DEFAULT_PENDING_AGE_MS,
            max_drift_ms: crate::clock::DEFAULT_MAX_DRIFT_MS,
        }
    }
}

/// Something worth telling the application about. Drain with [`Replica::drain_events`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// A link came up and the handshake started.
    PeerConnected(ActorId),
    /// A link went down.
    PeerDisconnected(ActorId),
    /// A received frame was dropped. The link stays up.
    FrameRejected { peer: ActorId, error: FrameError },
    /// The state refused an operation. It is logged, so the author's sequence numbers keep
    /// advancing, but its effect is not in the state. Every replica makes the same decision.
    OpRejected {
        actor: ActorId,
        seq: u64,
        error: ApplyError,
    },
    /// An outbound frame was not sent because it was too large or the buffer was full. The link
    /// stays up, and the next anti-entropy exchange sends the missing operations again.
    OutboundDropped {
        peer: ActorId,
        error: TransportError,
    },
    /// A committed batch could not be folded into the local clock. The operations are in the log.
    ClockFoldFailed(ClockError),
    /// The peer's clock is behind compaction. It needs a snapshot before it can catch up.
    PeerNeedsSnapshot(ActorId),
    /// The log and state were compacted up to `cutoff`.
    Compacted { cutoff: Timestamp, removed: usize },
    /// The snapshot sink rejected a snapshot, so compaction was skipped.
    SnapshotFailed(SnapshotError),
}

/// A replica's operation log, with what it needs to resume. Produced by [`Replica::export_log`]
/// and consumed by [`Replica::restore`]. The state is not included, so the application persists it
/// alongside this image.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogImage {
    /// Every retained entry, in timestamp order.
    pub ops: Vec<StampedOperation>,
    /// Highest sequence number per actor removed by compaction.
    pub compacted: VectorClock,
    /// The compaction floor, if compaction has run.
    pub floor: Option<Timestamp>,
    /// [`State::state_hash`] at export time. Restoring checks the supplied state against it.
    pub state_hash: u64,
}

struct Link {
    transport: Box<dyn Transport>,
    peer: Peer,
}

enum Failure {
    Frame(FrameError),
    Link,
}

impl From<FrameError> for Failure {
    fn from(error: FrameError) -> Self {
        Failure::Frame(error)
    }
}

fn push_event(events: &mut Vec<Event>, event: Event) {
    if events.len() >= MAX_QUEUED_EVENTS {
        events.remove(0);
    }
    events.push(event);
}

/// One replica of a shared state.
pub struct Replica<S: State> {
    actor: ActorId,
    config: ReplicaConfig,
    clock: HybridClock,
    signer: Box<dyn Signer>,
    verifier: Box<dyn Verifier>,
    policy: Box<dyn OpPolicy>,
    codec: Box<dyn Codec>,
    state: S,
    log: ReplicatedLog,
    merger: Merger,
    peers: BTreeMap<ActorId, Link>,
    sink: Option<Box<dyn SnapshotSink>>,
    ops_since_snapshot: u64,
    last_snapshot_ms: u64,
    events: Vec<Event>,
}

impl<S: State> Replica<S> {
    /// Creates a replica with an empty log, the default config and the binary codec.
    pub fn new(
        actor: ActorId,
        state: S,
        signer: Box<dyn Signer>,
        verifier: Box<dyn Verifier>,
        clock: Box<dyn ClockSource>,
    ) -> Self {
        let now = clock.now_ms();
        Self {
            actor,
            config: ReplicaConfig::default(),
            clock: HybridClock::new(actor, clock),
            signer,
            verifier,
            policy: Box::new(AllowAll),
            codec: Box::new(BinaryCodec),
            state,
            log: ReplicatedLog::new(),
            merger: Merger::new(MergeLimits::default()),
            peers: BTreeMap::new(),
            sink: None,
            ops_since_snapshot: 0,
            last_snapshot_ms: now,
            events: Vec::new(),
        }
    }

    /// Rebuilds a replica from an exported log and the state it was exported with.
    ///
    /// The clock is advanced past every timestamp in the image, so local operations issued after
    /// the restore order after everything already logged. The sequence number resumes from the
    /// log. Peers and any pending buffer are not restored. Peers rebuild them through the
    /// handshake.
    pub fn restore(
        actor: ActorId,
        state: S,
        image: LogImage,
        signer: Box<dyn Signer>,
        verifier: Box<dyn Verifier>,
        clock: Box<dyn ClockSource>,
    ) -> Result<Self, RestoreError> {
        if state.state_hash() != image.state_hash {
            return Err(RestoreError::StateMismatch);
        }
        let mut replica = Self::new(actor, state, signer, verifier, clock);
        if let Some(floor) = image.floor {
            replica.clock.update(&floor).map_err(RestoreError::Clock)?;
            replica.merger.set_floor(floor);
        }
        for op in &image.ops {
            replica
                .clock
                .update(&op.timestamp)
                .map_err(RestoreError::Clock)?;
        }
        replica.log = ReplicatedLog::from_parts(image.ops, image.compacted);
        Ok(replica)
    }

    /// Copies out the log, the compaction state and a fingerprint of the state. Persist the state
    /// with this image. [`Replica::restore`] needs both.
    pub fn export_log(&self) -> LogImage {
        LogImage {
            ops: self.log.ops().to_vec(),
            compacted: self.log.compacted().clone(),
            floor: self.merger.floor(),
            state_hash: self.state.state_hash(),
        }
    }

    /// Applies a config. Call before adding peers or issuing operations.
    pub fn with_config(mut self, mut config: ReplicaConfig) -> Self {
        config.max_ops_per_frame = config.max_ops_per_frame.clamp(1, MAX_OPS_PER_MESSAGE);
        self.clock.set_max_drift(config.max_drift_ms);
        self.merger.set_limits(MergeLimits {
            max_pending: config.max_pending,
            max_pending_per_actor: config.max_pending_per_actor,
            max_pending_bytes: config.max_pending_bytes,
            max_pending_age_ms: config.max_pending_age_ms,
            max_drift_ms: config.max_drift_ms,
            ..MergeLimits::default()
        });
        self.config = config;
        self
    }

    /// Replaces the operation policy. Every replica in a group must use the same policy, or they
    /// will disagree about which operations apply.
    pub fn with_policy(mut self, policy: Box<dyn OpPolicy>) -> Self {
        self.policy = policy;
        self
    }

    /// Replaces the codec used for message bodies. Both ends must use the same codec.
    pub fn with_codec(mut self, codec: Box<dyn Codec>) -> Self {
        self.codec = codec;
        self
    }

    /// Enables compaction. Without a sink the log grows without bound.
    pub fn with_snapshot_sink(mut self, sink: Box<dyn SnapshotSink>) -> Self {
        self.sink = Some(sink);
        self
    }

    /// This replica's identity.
    pub fn actor(&self) -> ActorId {
        self.actor
    }

    /// The current state.
    pub fn state(&self) -> &S {
        &self.state
    }

    /// The operation log.
    pub fn log(&self) -> &ReplicatedLog {
        &self.log
    }

    /// Connection status of `peer`, if it has been added.
    pub fn peer_state(&self, peer: ActorId) -> Option<PeerState> {
        self.peers.get(&peer).map(|link| link.peer.state)
    }

    /// Operations this replica holds that `peer` has not acknowledged. This is the offline queue.
    pub fn pending_for(&self, peer: ActorId) -> Option<usize> {
        self.peers
            .get(&peer)
            .map(|link| link.peer.pending_ops(&self.log))
    }

    /// Takes all queued events. The queue holds at most [`MAX_QUEUED_EVENTS`]. The oldest are
    /// dropped first.
    pub fn drain_events(&mut self) -> Vec<Event> {
        core::mem::take(&mut self.events)
    }

    /// Registers a peer, or replaces its transport. The link is brought up on the next poll.
    pub fn add_peer(&mut self, peer: ActorId, transport: Box<dyn Transport>) {
        self.peers.insert(
            peer,
            Link {
                transport,
                peer: Peer::new(peer),
            },
        );
    }

    /// Forgets a peer and returns whether it was known. Its acknowledgements stop holding back
    /// compaction, so call this when a peer has left for good.
    pub fn remove_peer(&mut self, peer: ActorId) -> bool {
        let Some(link) = self.peers.remove(&peer) else {
            return false;
        };
        if matches!(link.peer.state, PeerState::Syncing | PeerState::UpToDate) {
            push_event(&mut self.events, Event::PeerDisconnected(peer));
        }
        true
    }

    /// Stamps, signs, applies and logs a local operation, then broadcasts it to every peer that is
    /// up to date.
    ///
    /// Fails with [`EngineError::OperationTooLarge`] when the payload or dependency set is bigger
    /// than remote peers accept. Such an operation would make every peer reject the batch that
    /// carries it.
    pub fn local_op(&mut self, op: UirNode) -> Result<StampedOperation, EngineError> {
        if op.bytes.len() > MAX_PAYLOAD || self.log.clock().peers.len() > MAX_DEPS {
            return Err(EngineError::OperationTooLarge);
        }
        let timestamp = self.clock.now().map_err(EngineError::Clock)?;
        let seq = self
            .log
            .clock()
            .get(self.actor)
            .checked_add(1)
            .ok_or(EngineError::SequenceExhausted)?;
        self.policy
            .check(self.actor, &op)
            .and_then(|()| self.state.apply(&timestamp, &op))
            .map_err(EngineError::Apply)?;
        let mut stamped = StampedOperation {
            timestamp,
            actor: self.actor,
            seq,
            deps: self.log.clock().clone(),
            op,
            signature: Vec::new(),
        };
        stamped.signature = self.signer.sign(&stamped.signing_bytes());
        self.log.insert(stamped.clone());
        self.ops_since_snapshot += 1;
        self.broadcast(&stamped);
        Ok(stamped)
    }

    /// Imports operations obtained outside the transport layer, such as from a courier or a
    /// file. They go through the same validation and causal buffering as network input.
    pub fn import_ops(&mut self, ops: Vec<StampedOperation>) -> Result<MergeReport, FrameError> {
        self.apply_delta(ops).map_err(FrameError::Merge)
    }

    /// Advances every link: connects, reads frames, sends heartbeats and anti-entropy hellos,
    /// detects dead peers, and compacts when due.
    pub fn poll(&mut self) {
        let now = self.clock.physical_now();
        let ids: Vec<ActorId> = self.peers.keys().copied().collect();
        for id in ids {
            if let Some(mut link) = self.peers.remove(&id) {
                self.poll_link(&mut link, now);
                self.peers.insert(id, link);
            }
        }
        self.maybe_snapshot(now);
    }

    fn poll_link(&mut self, link: &mut Link, now: u64) {
        link.peer.rebase_timers(now);
        if !link.transport.is_connected() {
            self.lose_link(link);
            return;
        }
        if matches!(
            link.peer.state,
            PeerState::Connecting | PeerState::Disconnected
        ) {
            link.peer.connection_started(now);
            push_event(&mut self.events, Event::PeerConnected(link.peer.id));
            if self.send_hello(link, now).is_err() {
                self.lose_link(link);
                return;
            }
        }
        for _ in 0..MAX_FRAMES_PER_POLL {
            match link.transport.poll_recv() {
                Ok(Some(frame)) => match self.handle_frame(link, &frame, now) {
                    Ok(()) => {}
                    Err(Failure::Frame(error)) => {
                        push_event(
                            &mut self.events,
                            Event::FrameRejected {
                                peer: link.peer.id,
                                error,
                            },
                        );
                    }
                    Err(Failure::Link) => {
                        self.lose_link(link);
                        return;
                    }
                },
                Ok(None) => break,
                Err(_) => {
                    self.lose_link(link);
                    return;
                }
            }
        }
        let timeout = if link.transport.is_delay_tolerant() {
            self.config.delay_tolerant_timeout_ms
        } else {
            self.config.peer_timeout_ms
        };
        if now.saturating_sub(link.peer.last_seen_ms) > timeout {
            self.lose_link(link);
            return;
        }
        if now.saturating_sub(link.peer.last_hello_ms) >= self.config.resync_interval_ms
            && self.send_hello(link, now).is_err()
        {
            self.lose_link(link);
            return;
        }
        if now.saturating_sub(link.peer.last_ping_ms) >= self.config.ping_interval_ms {
            link.peer.last_ping_ms = now;
            if self.transmit(link, &Message::Ping(now)).is_err() {
                self.lose_link(link);
            }
        }
    }

    fn lose_link(&mut self, link: &mut Link) {
        let was_connected = matches!(link.peer.state, PeerState::Syncing | PeerState::UpToDate);
        link.peer.connection_lost();
        if was_connected {
            push_event(&mut self.events, Event::PeerDisconnected(link.peer.id));
        }
    }

    fn handle_frame(&mut self, link: &mut Link, frame: &[u8], now: u64) -> Result<(), Failure> {
        let envelope = Envelope::decode(frame).map_err(FrameError::Codec)?;
        if envelope.sender != link.peer.id
            || !self
                .verifier
                .verify(envelope.sender, &envelope.body, &envelope.sig)
        {
            return Err(FrameError::Auth.into());
        }
        // Only authenticated frames count as proof of life. Otherwise anyone who can reach the
        // link could keep a dead peer looking alive.
        link.peer.last_seen_ms = now;
        let message = self
            .codec
            .decode(&envelope.body)
            .map_err(FrameError::Codec)?;
        match message {
            Message::Hello(clock) => {
                link.peer.replace_clock(&clock);
                self.send_delta(link, &clock)?;
                let ack = Message::Ack(self.log.clock().clone());
                self.transmit(link, &ack)
            }
            Message::Ack(clock) => {
                link.peer.absorb_clock(&clock);
                link.peer.state = PeerState::UpToDate;
                self.send_delta(link, &clock)
            }
            Message::Delta(ops) => {
                let report = self.apply_delta(ops).map_err(FrameError::Merge)?;
                let grew = report.applied + report.stale + report.rejected.len();
                if grew == 0 {
                    return Ok(());
                }
                // Acknowledge growth so the sender can refresh our clock. That keeps its offline
                // queue depth and its stable frontier accurate.
                let ack = Message::Ack(self.log.clock().clone());
                self.transmit(link, &ack)
            }
            Message::Ping(nonce) => self.transmit(link, &Message::Pong(nonce)),
            Message::Pong(nonce) => {
                if nonce == link.peer.last_ping_ms {
                    Ok(())
                } else {
                    Err(FrameError::UnexpectedPong.into())
                }
            }
        }
    }

    /// Merges a batch and records what the state refused. Once the batch is committed, nothing
    /// here may fail the frame. The operations are already in the log, so the caller must still
    /// acknowledge them.
    fn apply_delta(&mut self, ops: Vec<StampedOperation>) -> Result<MergeReport, MergeError> {
        let stamps: Vec<Timestamp> = ops.iter().map(|op| op.timestamp).collect();
        let now = self.clock.physical_now();
        let report = self.merger.apply_remote_ops(
            self.verifier.as_ref(),
            self.policy.as_ref(),
            now,
            &mut self.state,
            &mut self.log,
            ops,
        )?;
        // Validation bounds the counters and the drift check matches the clock's, so folding should
        // not fail. If it does, the error is reported and the replica carries on.
        for stamp in &stamps {
            if let Err(error) = self.clock.update(stamp) {
                push_event(&mut self.events, Event::ClockFoldFailed(error));
            }
        }
        self.ops_since_snapshot += (report.applied + report.stale + report.rejected.len()) as u64;
        for rejection in &report.rejected {
            push_event(
                &mut self.events,
                Event::OpRejected {
                    actor: rejection.actor,
                    seq: rejection.seq,
                    error: rejection.error.clone(),
                },
            );
        }
        Ok(report)
    }

    fn send_hello(&mut self, link: &mut Link, now: u64) -> Result<(), Failure> {
        link.peer.last_hello_ms = now;
        let hello = Message::Hello(self.log.clock().clone());
        self.transmit(link, &hello)
    }

    fn send_delta(&mut self, link: &mut Link, peer_clock: &VectorClock) -> Result<(), Failure> {
        if !self.log.can_serve(peer_clock) {
            push_event(&mut self.events, Event::PeerNeedsSnapshot(link.peer.id));
            return Ok(());
        }
        let ops = calculate_delta(&self.log, peer_clock);
        for chunk in chunk_ops(&ops, self.config.max_ops_per_frame, DELTA_BYTE_BUDGET) {
            self.transmit(link, &Message::Delta(chunk))?;
        }
        Ok(())
    }

    fn broadcast(&mut self, op: &StampedOperation) {
        let frame = self.seal(&Message::Delta(vec![op.clone()]));
        for link in self.peers.values_mut() {
            if link.peer.state != PeerState::UpToDate {
                continue;
            }
            match link.transport.send(&frame) {
                Ok(()) => {}
                Err(error @ (TransportError::FrameTooLarge | TransportError::Backpressure)) => {
                    push_event(
                        &mut self.events,
                        Event::OutboundDropped {
                            peer: link.peer.id,
                            error,
                        },
                    );
                }
                Err(_) => {
                    link.peer.connection_lost();
                    push_event(&mut self.events, Event::PeerDisconnected(link.peer.id));
                }
            }
        }
    }

    /// Sends one message on `link`. A frame that cannot be sent right now is dropped and reported.
    /// The link stays up, and anti-entropy sends the operations again. Only a dead link fails.
    fn transmit(&mut self, link: &mut Link, message: &Message) -> Result<(), Failure> {
        let frame = self.seal(message);
        match link.transport.send(&frame) {
            Ok(()) => Ok(()),
            Err(error @ (TransportError::FrameTooLarge | TransportError::Backpressure)) => {
                push_event(
                    &mut self.events,
                    Event::OutboundDropped {
                        peer: link.peer.id,
                        error,
                    },
                );
                Ok(())
            }
            Err(_) => Err(Failure::Link),
        }
    }

    fn seal(&self, message: &Message) -> Vec<u8> {
        let body = self.codec.encode(message);
        Envelope::seal(self.signer.as_ref(), self.actor, body).encode()
    }

    /// The highest clock every peer has acknowledged, capped at our own clock.
    fn stable_frontier(&self) -> VectorClock {
        let mut frontier = self.log.clock().clone();
        for link in self.peers.values() {
            for (actor, seq) in frontier.peers.iter_mut() {
                *seq = (*seq).min(link.peer.remote_clock.get(*actor));
            }
        }
        frontier
    }

    fn maybe_snapshot(&mut self, now: u64) {
        if self.sink.is_none() {
            return;
        }
        // With no peers, nothing has been acknowledged by anyone. Compacting would discard
        // operations that a peer joining later still needs.
        if self.peers.is_empty() {
            return;
        }
        let due = self.ops_since_snapshot >= self.config.snapshot_every_ops
            || now.saturating_sub(self.last_snapshot_ms) >= self.config.snapshot_every_ms;
        if !due {
            return;
        }
        let Some(cutoff) = gc::cutoff(&self.log, &self.stable_frontier()) else {
            return;
        };
        if self.merger.floor().is_some_and(|floor| cutoff <= floor) {
            return;
        }
        let snapshot = Snapshot {
            cutoff,
            state_hash: self.state.state_hash(),
            op_count: self.log.len() as u64,
        };
        let persisted = match self.sink.as_mut() {
            Some(sink) => sink.persist(&snapshot),
            None => return,
        };
        if let Err(error) = persisted {
            push_event(&mut self.events, Event::SnapshotFailed(error));
            return;
        }
        let removed = self.log.compact(&cutoff);
        self.state.compact(&cutoff);
        self.merger.set_floor(cutoff);
        self.ops_since_snapshot = 0;
        self.last_snapshot_ms = now;
        push_event(&mut self.events, Event::Compacted { cutoff, removed });
    }
}
