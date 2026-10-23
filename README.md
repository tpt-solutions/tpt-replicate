# tpt-replicate

Pure-Rust, offline-first replication engine for the TPT ecosystem. It provides:

- **Hybrid Logical Clocks** (`clock`): total ordering of events across skewed clocks, with a bound on accepted remote drift and detection of counter overflow.
- **A causal operation log** (`log`): per-actor sequence numbers, dependency vectors, watermarks and compaction.
- **A deterministic merge executor** (`merge`): authenticates operations, buffers ones whose causes are missing, applies the rest in timestamp order, and quarantines operations the state refuses.
- **Delta sync** (`sync`, `engine`): a Hello, Delta, Ack handshake that sends only what a peer lacks, in bounded batches, with snapshot transfer for peers behind compaction.
- **Transport-agnostic peers** (`transport`, `peer`): heartbeats, dead-peer detection, automatic resume after reconnection, anti-entropy re-sync, and TCP links that redial with backoff.

The core is `no_std` + `alloc` with no dependencies.

## Install

Until the crate is published, depend on it from git:

```toml
[dependencies]
tpt-replicate = { git = "https://github.com/tpt-solutions/tpt-replicate" }
```

Enable the `std` feature for `SystemClock`, file and TCP transports:

```toml
tpt-replicate = { git = "https://github.com/tpt-solutions/tpt-replicate", features = ["std"] }
```

## Features

| Feature  | Adds |
|----------|------|
| *(none)* | The `no_std` core: clocks, log, merge, sync, peers, engine, in-memory transport, in-memory snapshot sink. |
| `std`    | `SystemClock`, `FileTransport` (delay-tolerant link through a directory), `TcpTransport`, `TcpAcceptor`, `ReconnectingTcp`. |
| `webrtc` | `WebRtcTransport`, an adapter from a message-oriented data channel to `Transport`. |

## Quick start

```rust
use tpt_replicate::prelude::*;

let clock = ManualClock::new(1_000);
let mut a = Replica::builder(ActorId(1), LwwMap::new())
    .shared_secret(42) // a stand-in key for demos; use `keys(...)` in production
    .clock(Box::new(clock.clone()))
    .build()
    .unwrap();
let mut b = Replica::builder(ActorId(2), LwwMap::new())
    .shared_secret(42)
    .clock(Box::new(clock))
    .build()
    .unwrap();

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

## Examples

Run any of them with `cargo run --example <name>`. Examples marked `std` also need `--features std`.

| Example | Shows |
|---------|-------|
| `quickstart` | The smallest program: two replicas over an in-memory link. |
| `cad_vertex` | Concurrent edits and tie-breaking. |
| `spreadsheet_lww` | Deletes against an offline collaborator. |
| `three_peer_partition` | A triangle of replicas, a cut link, and convergence after repair. |
| `custom_crdt` | A replicated register with its own operations and snapshot format. |
| `custom_signer` | Plugging in a counting signer and an allow-list verifier. |
| `rover_dtn` (`std`) | Delay-tolerant sync through a pair of directories. |
| `tcp_chat` (`std`) | A two-person chat over TCP, with listener, dialer and reconnection. |

## Documentation

- [`docs/guide.md`](docs/guide.md): concepts, the wire protocol, trait recipes, migrating from the stubs, and troubleshooting.
- [`CHANGELOG.md`](CHANGELOG.md): what changed.

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
- **Snapshots are trusted from peers.** A snapshot replaces the receiver's state. Its hash is checked, but its contents are not validated against the operation policy. Only snapshot from peers you trust.
- **Snapshot size.** A snapshot streams its encoded state in chunks, and a receiver refuses any state over 256 MiB (`engine::MAX_SNAPSHOT_BYTES`). A peer that cannot receive the whole state in one session can be interrupted, and the next handshake starts it again from the beginning.
- **Relay is by anti-entropy.** Operations received from one peer are not forwarded live to others. They reach other peers at the next resync, which is every `resync_interval_ms`.
- **Idle peers pin compaction.** The horizon moves only as fast as the least recently active actor. Compaction is delayed, not unsafe, while an idle peer stays idle.
- **Stubs.** The `stubs` module stands in for `tpt-operations`, `tpt-primitives`, `tpt-uir`, `tpt-version` and `tpt-webrtc`. Their trait shapes are chosen to be replaceable, but the real APIs are unconfirmed.

## Verifying

```powershell
powershell -ExecutionPolicy Bypass -File scripts/ci.ps1
```

```sh
sh scripts/ci.sh
```

Both scripts check formatting, build with and without features, run all tests, run clippy and rustdoc with warnings as errors, build for the `thumbv7em-none-eabihf` bare-metal target, and confirm the dependency list is empty.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md). Report security issues as described in [`SECURITY.md`](SECURITY.md).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option. Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this work by you shall be dual licensed as above, without any additional terms or conditions.
