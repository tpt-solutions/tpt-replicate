# tpt-replicate

Pure-Rust, offline-first replication engine for the TPT ecosystem. It provides:

- **Hybrid Logical Clocks** (`clock`): total ordering of events across skewed clocks, with a bound on accepted remote drift and detection of counter overflow.
- **A causal operation log** (`log`): per-actor sequence numbers, dependency vectors, watermarks and compaction.
- **A deterministic merge executor** (`merge`): authenticates operations, buffers ones whose causes are missing, applies the rest in timestamp order, and is all-or-nothing per batch.
- **Delta sync** (`sync`, `engine`): a Hello, Delta, Ack handshake that sends only what a peer lacks, in bounded batches.
- **Transport-agnostic peers** (`transport`, `peer`): heartbeats, dead-peer detection, automatic resume after reconnection, and anti-entropy re-sync.

The core is `no_std` + `alloc` with no dependencies.

## Dependency audit

`cargo tree -e normal --no-default-features` lists only the crate itself. The only dev-dependency is `proptest`, which is used by the tests and is not part of the library.

## Features

| Feature  | Adds |
|----------|------|
| *(none)* | The `no_std` core: clocks, log, merge, sync, peers, engine, in-memory transport, in-memory snapshot sink. |
| `std`    | `SystemClock`, `FileTransport` (delay-tolerant link through a directory), `TcpTransport`. |
| `webrtc` | `WebRtcTransport`, an adapter from a message-oriented data channel to `Transport`. |

## Quick start

```rust
use tpt_replicate::clock::ManualClock;
use tpt_replicate::stubs::crypto::{KeyedHashSigner, KeyedHashVerifier};
use tpt_replicate::stubs::lww::{LwwMap, LwwOp};
use tpt_replicate::transport::{Faults, MemoryTransport};
use tpt_replicate::{ActorId, Replica};

let clock = ManualClock::new(1_000);
let new_replica = |id: u64| {
    Replica::new(
        ActorId(id),
        LwwMap::new(),
        Box::new(KeyedHashSigner::new(ActorId(id), 42)),
        Box::new(KeyedHashVerifier::new(42)),
        Box::new(clock.clone()),
    )
};
let (mut a, mut b) = (new_replica(1), new_replica(2));

let (ta, tb) = MemoryTransport::pair(7, Faults::default());
a.add_peer(b.actor(), Box::new(ta));
b.add_peer(a.actor(), Box::new(tb));

let op = LwwOp::Set { key: b"cell".to_vec(), value: b"42".to_vec() };
a.local_op(op.to_node()).unwrap();
for _ in 0..3 {
    a.poll();
    b.poll();
}
assert_eq!(b.state().get(b"cell"), Some(&b"42"[..]));
```

Examples live in `examples/`: `cad_vertex` (concurrent edits and tie-breaking), `spreadsheet_lww` (deletes against an offline collaborator), and `rover_dtn` (`cargo run --example rover_dtn --features std`).

## Design

- **Ordering.** Timestamps are `(wall_time, logical_counter, actor_id)`. Concurrent writes to one key resolve to the greatest timestamp on every replica.
- **Causality.** An operation is applied only after the next operation from its author and every operation in its dependency vector are applied. Authors use per-actor contiguous sequence numbers.
- **Atomicity.** A batch is validated first (signature, timestamp drift, structure, pending quotas), then applied to copies of the state and buffer. Nothing is committed unless the whole batch succeeds.
- **Quarantine.** An operation the state or the `OpPolicy` refuses is logged so its author's sequence numbers keep moving, skipped by the state, and reported as `Event::OpRejected`. It does not roll back the batch, so one bad operation cannot stall its author. Every replica must use the same policy.
- **Liveness.** Only authenticated frames count as proof that a peer is alive. Links that are expected to go quiet (`Transport::is_delay_tolerant`) get a much longer timeout.
- **Deletes.** Deletes are timestamped tombstones. They are removed only by compaction, after a snapshot is persisted and every known peer has acknowledged them.
- **Offline queue.** The log is the queue. A peer that is not up to date gets no live broadcasts, and the handshake replays what it is missing.

## Limitations

- **Signatures are a stand-in.** `stubs::crypto` uses a keyed 64-bit hash. It detects accidental and simple tampering and does not resist forgery. Replace it with `tpt-crypto` before deploying across untrusted networks.
- **Compaction horizon.** Compaction is capped at the lowest watermark of any actor the replica has seen. A peer that has produced nothing yet can still hold back compaction, and an actor that has never been seen can still produce an operation older than the compaction floor. Such operations are logged but not applied, and that peer needs a snapshot to rejoin. Snapshot transfer is not implemented.
- **Idle peers pin compaction.** The horizon moves only as fast as the least recently active actor. Compaction is delayed, not unsafe, while an idle peer stays idle.
- **Stubs.** The `stubs` module stands in for `tpt-operations`, `tpt-primitives`, `tpt-uir`, `tpt-version` and `tpt-webrtc`. Their trait shapes are chosen to be replaceable, but the real APIs are unconfirmed.

## Verifying

```powershell
powershell -ExecutionPolicy Bypass -File scripts/ci.ps1
```

The script checks formatting, builds with and without features, runs all tests, runs clippy and rustdoc with warnings as errors, builds for the `thumbv7em-none-eabihf` bare-metal target, and confirms the dependency list is empty.
