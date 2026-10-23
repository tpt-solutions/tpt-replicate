# tpt-replicate guide

This guide covers the concepts, the wire protocol, the traits you implement, what to do when the
stubs are replaced, and how to read the events the engine reports.

## Concepts

**Replicas and operations.** Each replica has an `ActorId`. It applies local operations to its
state at once, and sends them to peers. An operation is a `UirNode`: opaque bytes that the state's
`apply` understands. Every operation is stamped with an HLC `Timestamp` and a per-author sequence
number, signed, and logged.

**Causality.** An operation carries the author's vector clock at creation time (`deps`). A replica
applies it only after the author's previous operation and everything in `deps` are applied. Ready
operations are applied in timestamp order. Operations that are not ready wait in the pending
buffer.

**Convergence.** The state must be a CRDT: `apply` for the same `(timestamp, op)` is idempotent,
and two replicas that have applied the same set of operations, in any order, reach equal
`state_hash` values. The bundled `LwwMap` is a last-writer-wins map with tombstones.

**Compaction.** Once every peer has acknowledged a prefix of the log, and a snapshot sink has
persisted a snapshot, the prefix can be dropped. Tombstones older than the cutoff go with it.
Compaction runs only with a snapshot sink, and only when at least one peer is known.

**Snapshots.** A replica that is behind compaction cannot catch up from the log. The sender sends
its encoded state, and the receiver installs it. Operations the receiver has that the snapshot does
not cover are kept and re-applied. The retained operations follow in an ordinary delta.

**Quarantine.** An operation that the state or the `OpPolicy` refuses is still logged, so its
author's sequence numbers keep advancing. It is reported as `Event::OpRejected` and never blocks
later operations from the same author.

## Wire protocol

Every frame is an `Envelope`: the sender's id, the encoded message body, and a signature over the
body. A frame whose sender does not match its link, or whose signature does not verify, is dropped
with `FrameError::Auth`.

```
  A (dialer)                                   B (listener)
     |-------- Hello(clock_A) ------------------->|   B replies with what A lacks
     |<------- Delta(ops B has, A lacks) ---------|
     |<------- Ack(clock_B) ----------------------|
     |-------- Delta(ops A has, B lacks) -------->|   sent when A sees B's Ack
     |-------- Ack(clock_A) --------------------->|
     |  ... Ping / Pong for liveness ...          |
     |  ... Hello again every resync_interval_ms (anti-entropy) ...
```

When `B` is behind A's compaction floor, A sends a `Snapshot` in place of the delta:

```
  A                                            B (behind compaction)
     |<------- Hello(clock_B) --------------------|  B's clock is below A's compaction floor
     |-------- SnapshotBegin(len, hash, clocks) ->|  B reserves space for the state
     |-------- SnapshotChunk(bytes) x N ---------->|  B verifies the hash once all bytes arrive, installs the state
     |-------- Delta(retained ops after compacted)->|
     |<------- Ack(clock_B) ----------------------|
```

After a `Delta` that grows the log, the receiver sends an `Ack`, so the sender refreshes its view of
the receiver. That view drives the offline queue depth (`Replica::pending_for`) and the stable
frontier used for compaction.

Live operations go only to peers that are `UpToDate`. Operations received from one peer are not
forwarded live to others. They reach other peers at the next resync.

## Traits you implement

| Trait | Purpose | Example |
|-------|---------|---------|
| `CrdtCompatible` + `State` | Your operations and state, including snapshot encoding. | `examples/custom_crdt.rs` |
| `Signer` / `Verifier` | Message authentication. The bundled one is **not secure**. | `examples/custom_signer.rs` |
| `Transport` | A frame link: send, poll, is-connected, and whether it is delay tolerant. | `transport::ReconnectingTcp` |
| `SnapshotSink` | Durable storage for snapshots, required before compaction. | `stubs::MemorySnapshotSink` (tests only) |
| `OpPolicy` | Authorization and application-level validation, consulted per operation. | `merge::AllowAll` (default) |
| `Codec` | Message body encoding. Both ends must agree. | `codec::BinaryCodec`, `codec::Compressed` |
| `ClockSource` | The physical time source. | `clock::ManualClock`, `clock::SystemClock` |

### State recipe

1. `apply(&mut self, stamp, op)`: decode `op`, return `ApplyError` for bad input, and **leave the
   state unchanged on error**. Applying the same `(stamp, op)` twice must do nothing.
2. `state_hash(&self)`: hash only what is observable. Tombstones are not observable, so they must
   not change the hash.
3. `compact(&mut self, cutoff)`: drop metadata that no operation at or before `cutoff` can need.
4. `encode_state` / `decode_state`: the full state, tombstones included. `decode_state` must return
   `SnapshotError::Malformed` for bad input, not panic.

### Policy recipe

`OpPolicy::check(author, op)` runs before the state sees an operation, for local and remote
operations alike. Every replica in a group must use the same policy, or they will disagree about
what applies. Return `ApplyError::Denied` for authorization failures and `ApplyError::Invalid(reason)`
for validation failures.

## Configuring a replica

```rust
use tpt_replicate::prelude::*;

let replica = Replica::builder(ActorId(1), LwwMap::new())
    .keys(my_signer, my_verifier)   // required
    .clock(Box::new(SystemClock))   // required without the `std` feature
    .config(ReplicaConfig { peer_timeout_ms: 15_000, ..ReplicaConfig::default() })
    .policy(Box::new(my_policy))
    .snapshot_sink(Box::new(my_store))
    .build()?;
```

Settings that are easy to get wrong:

- **`max_pending`, `max_pending_per_actor`, `max_pending_bytes`, `max_pending_age_ms`**: bound what
  the replica holds while it waits for causes. Dropped operations are repaired by the next resync.
- **`delay_tolerant_timeout_ms`**: applies to links whose `is_delay_tolerant()` is true, such as
  `FileTransport`. Keep it well above the courier interval.
- **`snapshot_every_ops` / `snapshot_every_ms`**: how often compaction is attempted. Without a
  snapshot sink, the log grows without bound.
- **`max_drift_ms`**: how far ahead of local time a remote timestamp may be. Clocks that disagree by
  more than this will see operations refused as `BadTimestamp`.

## Replacing the stubs

The `stubs` module stands in for the sibling TPT crates. Replacing a stub is a local change, because
the engine only sees the traits.

1. **Signing (`tpt-crypto`)**: implement `Signer` and `Verifier` over the real scheme. Signatures
   are opaque byte vectors. The verifier must reject unknown actors.
2. **Operations and state (`tpt-operations`, `tpt-uir`)**: implement `CrdtCompatible` and `State`
   for the real operation types. Keep `UirNode` as the wire payload, or change the payload type
   behind `apply`.
3. **Primitives (`tpt-primitives`)**: `ActorId` is a `u64` wrapper. If the real type is wider,
   change it in one place and fix the call sites.
4. **Snapshots (`tpt-version`)**: implement `SnapshotSink` over the real store.
5. **WebRTC (`tpt-webrtc`)**: implement `DataChannel` and use `WebRtcTransport`.

Then re-run the tests. `tests/convergence.rs` and `tests/snapshot.rs` drive the engine end to end
with the bundled stubs, so they catch changes in the behaviour the engine relies on.

## Reading the events

Drain events with `Replica::drain_events()` after each poll. The queue keeps the newest 1,024. To see every event as it happens, register a callback with `Replica::with_event_observer`.

| Event | Meaning | What to do |
|-------|---------|------------|
| `PeerConnected(peer)` | A handshake started on a link. | Nothing. |
| `PeerDisconnected(peer)` | A link was lost or timed out. | Expect reconnection. Operations queue in the log meanwhile. |
| `FrameRejected { error }` | A frame was dropped. The link stays up. | `Auth`: check keys. `Codec`: check codec agreement. `Merge(..)`: a peer sent bad operations. `UnexpectedPong`: a stale or forged pong. |
| `OpRejected { actor, seq, error }` | The state or policy refused an operation. | Check the policy and the operation format. The refusal is the same on every replica. |
| `OutboundDropped { peer, error }` | A frame was too large or the outbound buffer was full. | Anti-entropy resends the operations. Frequent drops mean the buffer or frame budget is too small. |
| `ClockFoldFailed(error)` | A committed batch could not be folded into the clock. | Rare. The operations are in the log. Check clock skew. |
| `PeerNeedsSnapshot(peer)` | A peer was behind compaction. This replica sent a snapshot. | Expected after long offline periods. |
| `SnapshotInstalled { floor }` | This replica installed a peer's snapshot. | Expected after joining or long offline periods. |
| `Compacted { cutoff, removed }` | The log was compacted. | Nothing. |
| `SnapshotFailed(error)` | The sink refused a snapshot, so compaction was skipped. | Check storage. The log keeps growing until this succeeds. |

## Restarting from a checkpoint

`Replica::checkpoint()` returns the state and the log as one blob. Store it with a
`CheckpointStore` (`MemoryStore`, or `FileStore` with `std`), and rebuild the replica with
`Replica::recover(bytes, actor, signer, verifier, clock)`. The sequence numbers and the clock resume
from the log, so peers see no gap and no reuse. Pending operations and peer links are not saved, and
the handshake restores them.

Save a checkpoint after a batch of work, not after every operation. A checkpoint of a large log is
large, so checkpoint after compaction where you can.

## Replicated types (`crdt` feature)

`crdt::PnCounter` and `crdt::OrSet` are ready-made states. Build a replica over either one with
`Replica::builder`. Update through `PnCounter::add(delta)`, and `OrSet::add(element)` and
`OrSet::remove(element, state)`. `OrSet::remove` removes only the adds the caller has seen, so a
concurrent add survives. Both types tolerate a repeated operation, which snapshot installation can
cause.

## Troubleshooting

**A peer never reaches `UpToDate`.** Check for `FrameRejected` events with `Auth`. This usually
means the two sides use different keys, or a frame's sender does not match the link it arrived on.

**The log grows without bound.** No snapshot sink is configured, or `SnapshotFailed` is firing. Or a
peer that never acknowledges is holding back the stable frontier. Check `Replica::pending_for` for
each peer, and `remove_peer` one that has left for good.

**Operations from one author stop applying.** Look for `OpRejected` from that author. A refused
operation is quarantined and does not block later ones, so the later ones should still apply. If
they do not, check `pending_for` and the pending quotas.

**Reconnection is slow.** `ReconnectingTcp` backs off up to 30 seconds. Reconnect runs from
`is_connected`, so the engine's poll loop must keep running.

**A large operation is refused locally.** `local_op` returns `EngineError::OperationTooLarge` when
the payload exceeds 1 MiB or the dependency set exceeds 4,096 entries. Those are the limits remote
peers accept.

**Clocks disagree.** Remote timestamps further ahead than `max_drift_ms` are refused as
`BadTimestamp`. A replica whose physical clock moved backwards still times out dead peers, because
timers rebase to the new time.

**Tests are flaky under a `ManualClock`.** Anti-entropy and liveness both depend on the clock.
Advance it past `resync_interval_ms` before expecting relayed operations to arrive.
