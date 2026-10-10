# tpt-replicate TODO

Decisions: single crate, `#![no_std]` + `alloc` (optional `std` feature), local stubs for sibling TPT crates behind traits, per-actor sequence numbers for causality, pluggable Codec/Signer/GC traits, LWW-map + tombstone stub CRDT for tests, proptest + randomized fuzz tests.

## Phase 0: Setup
- [ ] `git init`, `.gitignore`
- [ ] `cargo init --lib` named `tpt-replicate`; edition 2021
- [ ] `#![no_std]` + `extern crate alloc`; `std` feature (default off for core, on for tests/TCP)
- [ ] Verify no C-FFI deps (`cargo tree` audit); document in README
- [ ] CI-style script: `cargo build --no-default-features`, `cargo test`, `cargo clippy`, `cargo fmt --check`, build for a bare-metal target (e.g. `thumbv7em-none-eabihf`) to prove no_std
- [ ] Module skeleton: `clock`, `log`, `sync`, `merge`, `peer`, `error`

## Phase 0b: Local stubs for sibling crates (`stubs` module / feature)
- [ ] `ActorId` (tpt-primitives stub) with `Ord`, serialization
- [ ] `UirNode` (tpt-uir stub) serializable op payload
- [ ] `CrdtCompatible` + `State` traits (tpt-operations stub): apply, deterministic order, state hash
- [ ] `Signer` / `Verifier` traits (tpt-crypto stub) + a dummy/pure-Rust impl
- [ ] `SnapshotSink` trait (tpt-version stub)
- [ ] Stub LWW-map state with `Set` / `Delete` (tombstone) ops for tests

## Phase 1: Clocks & Log (Weeks 1-3)
- [ ] `Timestamp { wall_time, logical_counter, actor_id }` with total `Ord` (wall, counter, actor)
- [ ] `HybridClock`: `now()`, `update(remote)`; injectable time source (no_std friendly)
- [ ] Clock-skew / max-drift rejection of far-future remote timestamps
- [ ] Counter overflow handling
- [ ] `VectorClock { peers: BTreeMap<ActorId, u64> }`: increment, merge, `compare` -> Before/After/Equal/Concurrent
- [ ] Add per-actor `seq` to `StampedOperation` (+ signature field)
- [ ] `ReplicatedLog`: append local op, insert remote op, dedupe (idempotent), ordered by timestamp
- [ ] Watermarks per actor; derive VectorClock from log
- [ ] Unit tests: ordering, uniqueness, monotonicity under skew
- [ ] Multi-thread uniqueness test (std): **Milestone** causally ordered, unique timestamps across threads

## Phase 2: Merge Engine (Weeks 4-6)
- [ ] Causal pending buffer / priority queue keyed by HLC
- [ ] Readiness check: `seq == clock[actor] + 1` (+ optional explicit deps)
- [ ] `apply_remote_ops(state, log, incoming) -> Result<(), MergeError>`
- [ ] `MergeError` enum (bad signature, bad timestamp, gap/overflow, apply failure, duplicate)
- [ ] Atomicity: failed batch must not corrupt state/log
- [ ] Verify signature before accepting ops
- [ ] Idempotent re-application of duplicates
- [ ] Tombstone handling: Delete retained in log; late/offline peer cannot resurrect entities
- [ ] Bound on pending-buffer size (DoS protection)
- [ ] **Milestone**: shuffled op list reconstructs identical final state

## Phase 3: Sync Protocol (Weeks 7-9)
- [ ] `calculate_delta(local_log, peer_clock) -> Vec<StampedOperation>`
- [ ] Wire messages: Hello(VectorClock), Delta(batch), Ack(watermark), Ping/Pong
- [ ] `Codec` trait + default pure-Rust binary encoding (postcard-style)
- [ ] Optional pure-Rust compression of batches
- [ ] Batch signing/verification (tamper detection)
- [ ] Batch size limits and chunking for large deltas
- [ ] Bi-directional sync handshake (both sides exchange clocks)
- [ ] Tests: two peers sync missing state; **Milestone** efficient delta only
- [ ] Network-partition test: A drops, B/C continue, A reconnects, no data loss

## Phase 4: Transport & Integration (Weeks 10-12)
- [ ] `Transport` trait (send/recv bytes, poll, no_std-compatible)
- [ ] Peer module: states Connecting / Syncing / UpToDate / Disconnected
- [ ] Heartbeats, keepalive timeouts, dead-connection cleanup (injectable clock)
- [ ] Offline-first outbound queue; auto-resume on reconnect
- [ ] In-memory transport (tests, with loss/reorder/dup injection)
- [ ] File-based transport (delay-tolerant networking)
- [ ] TCP transport (`std` feature)
- [ ] WebRTC data-channel adapter against stubbed tpt-webrtc trait (feature-gated)
- [ ] Engine: local op -> apply -> stamp -> sign -> append -> broadcast -> ack
- [ ] Snapshot trigger (hourly / every N ops) -> `SnapshotSink` (tpt-version stub)
- [ ] Tombstone GC: stable frontier (min vector clock acked by all peers) or snapshot-confirmed cutoff
- [ ] Log compaction after snapshot
- [ ] Swap stubs for real tpt-* crates when available (tracked, blocked)
- [ ] README, rustdoc, examples (CAD vertex move, spreadsheet LWW, rover/orbiter DTN)
- [ ] Publish v1.0.0 (version, changelog, license, `cargo publish --dry-run`)

## Testing & Quality
- [ ] proptest: 5 virtual peers, 1,000 random ops, shuffled delivery, equal state hash
- [ ] Concurrent same-key edits resolve deterministically via HLC
- [ ] Delete-then-offline-peer-reconnect: no resurrection
- [ ] Fuzz-style randomized tests: malformed, truncated, maliciously timestamped batches fail gracefully, state unchanged
- [ ] Duplicate / replayed / reordered delivery tests
- [ ] no_std build verified on a bare-metal target
- [ ] Clippy clean, `#![forbid(unsafe_code)]`

## Open Items
- [ ] Confirm real APIs of tpt-operations / tpt-primitives / tpt-crypto / tpt-uir / tpt-version / tpt-webrtc once available
