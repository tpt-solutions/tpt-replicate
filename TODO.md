# tpt-replicate TODO

Decisions: single crate, `#![no_std]` + `alloc` (optional `std` feature), local stubs for sibling TPT crates behind traits, per-actor sequence numbers for causality, pluggable Codec/Signer/GC traits, LWW-map + tombstone stub CRDT for tests, proptest + randomized fuzz tests.

## Phase 0: Setup
- [x] `git init`, `.gitignore`
- [x] `cargo init --lib` named `tpt-replicate`; edition 2021
- [x] `#![no_std]` + `extern crate alloc`; `std` feature (default off for core, on for tests/TCP)
- [x] Verify no C-FFI deps (`cargo tree` audit); document in README
- [x] CI-style script: `cargo build --no-default-features`, `cargo test`, `cargo clippy`, `cargo fmt --check`, build for a bare-metal target (e.g. `thumbv7em-none-eabihf`) to prove no_std (`scripts/ci.ps1`)
- [x] Module skeleton: `clock`, `log`, `sync`, `merge`, `peer`, `error`

## Phase 0b: Local stubs for sibling crates (`stubs` module / feature)
- [x] `ActorId` (tpt-primitives stub) with `Ord`, serialization
- [x] `UirNode` (tpt-uir stub) serializable op payload
- [x] `CrdtCompatible` + `State` traits (tpt-operations stub): apply, deterministic order, state hash
- [x] `Signer` / `Verifier` traits (tpt-crypto stub) + a dummy/pure-Rust impl (**keyed hash, not secure**)
- [x] `SnapshotSink` trait (tpt-version stub)
- [x] Stub LWW-map state with `Set` / `Delete` (tombstone) ops for tests

## Phase 1: Clocks & Log (Weeks 1-3)
- [x] `Timestamp { wall_time, logical_counter, actor_id }` with total `Ord` (wall, counter, actor)
- [x] `HybridClock`: `now()`, `update(remote)`; injectable time source (no_std friendly)
- [x] Clock-skew / max-drift rejection of far-future remote timestamps
- [x] Counter overflow handling
- [x] `VectorClock { peers: BTreeMap<ActorId, u64> }`: increment, merge, `compare` -> Before/After/Equal/Concurrent
- [x] Add per-actor `seq` to `StampedOperation` (+ signature field)
- [x] `ReplicatedLog`: append local op, insert remote op, dedupe (idempotent), ordered by timestamp
- [x] Watermarks per actor; derive VectorClock from log
- [x] Unit tests: ordering, uniqueness, monotonicity under skew
- [x] Multi-thread uniqueness test (std): **Milestone** causally ordered, unique timestamps across threads (per-thread monotonic and globally unique; each thread owns its clock, since HLC holds a non-`Send` source)

## Phase 2: Merge Engine (Weeks 4-6)
- [x] Causal pending buffer / priority queue keyed by HLC
- [x] Readiness check: `seq == clock[actor] + 1` (+ explicit deps: each op carries its author's vector clock)
- [x] `apply_remote_ops(state, log, incoming) -> Result<(), MergeError>` (as `Merger::apply_remote_ops`, which also takes the verifier and local time)
- [x] `MergeError` enum (bad signature, bad timestamp, malformed, conflict, pending overflow, apply failure)
- [x] Atomicity: failed batch must not corrupt state/log
- [x] Verify signature before accepting ops
- [x] Idempotent re-application of duplicates
- [x] Tombstone handling: Delete retained in log; late/offline peer cannot resurrect entities
- [x] Bound on pending-buffer size (DoS protection)
- [x] **Milestone**: shuffled op list reconstructs identical final state

## Phase 3: Sync Protocol (Weeks 7-9)
- [x] `calculate_delta(local_log, peer_clock) -> Vec<StampedOperation>`
- [x] Wire messages: Hello(VectorClock), Delta(batch), Ack(clock), Ping/Pong
- [x] `Codec` trait + default pure-Rust binary encoding (varints, bounded decoding)
- [x] Optional pure-Rust compression of batches (PackBits, `Compressed<C>`)
- [x] Batch signing/verification (tamper detection): signed envelope per message, plus per-op signatures
- [x] Batch size limits and chunking for large deltas
- [x] Bi-directional sync handshake (both sides exchange clocks)
- [x] Tests: two peers sync missing state; **Milestone** efficient delta only
- [x] Network-partition test: A drops, B/C continue, A reconnects, no data loss

## Phase 4: Transport & Integration (Weeks 10-12)
- [x] `Transport` trait (send/recv bytes, poll, no_std-compatible)
- [x] Peer module: states Connecting / Syncing / UpToDate / Disconnected
- [x] Heartbeats, keepalive timeouts, dead-connection cleanup (injectable clock)
- [x] Offline-first outbound queue; auto-resume on reconnect (the log is the queue; the handshake replays it, and `Replica::pending_for` reports depth)
- [x] In-memory transport (tests, with loss/reorder/dup injection)
- [x] File-based transport (delay-tolerant networking)
- [x] TCP transport (`std` feature)
- [x] WebRTC data-channel adapter against stubbed tpt-webrtc trait (feature-gated)
- [x] Engine: local op -> apply -> stamp -> sign -> append -> broadcast -> ack
- [x] Snapshot trigger (hourly / every N ops) -> `SnapshotSink` (tpt-version stub)
- [x] Tombstone GC: stable frontier (min vector clock acked by all peers) or snapshot-confirmed cutoff
- [x] Log compaction after snapshot
- [ ] Swap stubs for real tpt-* crates when available (tracked, blocked)
- [x] README, rustdoc, examples (CAD vertex move, spreadsheet LWW, rover/orbiter DTN)
- [ ] Publish v1.0.0 (version, changelog, license, `cargo publish --dry-run`) — version, changelog, license and dry run are done. Not published: the publish itself is outward-facing and needs your go-ahead.

## Testing & Quality
- [x] proptest: 5 virtual peers, 1,000 random ops, shuffled delivery, equal state hash
- [x] Concurrent same-key edits resolve deterministically via HLC
- [x] Delete-then-offline-peer-reconnect: no resurrection
- [x] Fuzz-style randomized tests: malformed, truncated, maliciously timestamped batches fail gracefully, state unchanged
- [x] Duplicate / replayed / reordered delivery tests
- [x] no_std build verified on a bare-metal target (builds; the bare-metal target is cross-compiled, not run)
- [x] Clippy clean, `#![forbid(unsafe_code)]`

## Open Items
- [ ] Confirm real APIs of tpt-operations / tpt-primitives / tpt-crypto / tpt-uir / tpt-version / tpt-webrtc once available
- [x] Choose a license: MIT OR Apache-2.0 (`LICENSE-MIT`, `LICENSE-APACHE`, Cargo metadata)
- [x] Snapshot transfer for peers behind the compaction floor (see the next item)

## Notes from implementation
- `StampedOperation` carries a `deps: VectorClock` (the author's clock at creation). The spec did not list it. It gives true cross-actor causal delivery and is signed.
- The wire `Ack` carries the sender's full clock rather than a single watermark.
- Receivers acknowledge deltas that grow their log. Without this, live traffic never refreshed the sender's view of the peer, so the offline-queue depth and the stable frontier went stale.
- The state hash covers live entries only, so compacting tombstones does not change it.
- Compaction is capped at the lowest watermark of any known actor. An idle actor therefore holds back compaction, which delays tombstone GC but does not make it unsafe.
- Operations from an actor never seen before, older than the compaction floor, are logged but not applied. This is the known gap for peers that were offline longer than the GC window; snapshot transfer would close it.
- Per-op `(actor, seq)` deduplication is by contiguous clock, so an equivocating author's second op for an already-applied sequence number is silently ignored. Conflicts are only detected for operations still pending.

## Phase 5: Review follow-ups (from platform review)

### A. Bug fixes
- [x] Poisoned op wedges its batch: quarantine per op (record as rejected, advance author seq, emit `Event::OpRejected`) (`src/merge.rs`, `src/engine.rs`)
- [x] TCP `send` busy-spins on `WouldBlock` and can deadlock: bounded outbound buffer, flush in poll, connect timeout (`src/transport.rs`)
- [x] Oversized frames cause a reconnect loop: chunk by encoded bytes, treat `FrameTooLarge` as a local error, validate `max_ops_per_frame <= MAX_OPS_PER_MESSAGE` (`src/engine.rs`, `src/sync.rs`)
- [x] Partial failure after merge commit: bound `logical_counter` in `Merger::validate`, make HLC fold non-fatal, always ack after a commit (`src/engine.rs`)
- [x] Pending buffer can be pinned: per-actor cap, age expiry, byte budget, check readiness before the overflow check (`src/merge.rs`)
- [x] Restart safety: `Replica::restore`, `export_log` so seq and HLC resume (the state is persisted by the application; `restore` checks it against `state_hash`)
- [x] GC safety: `remove_peer`, no compaction with zero peers, a `Hello` replaces `remote_clock` when a peer restarts (`src/peer.rs`)
- [x] DTN link flapping: per-transport timeout (`Transport::is_delay_tolerant`), fsync on write, next sequence number checked against existing names on each send. Not persisted across restarts: it is read from the outbox directory on open.
- [ ] Minor: partly done. Done: cap events queue (`MAX_QUEUED_EVENTS`) and TCP rx buffer, cap `FileTransport` frame size, monotonic liveness timer, validate Pong, document `!Send`. **Not done:** cache the `FileTransport` dir listing.
- [x] Performance: skip state clone for duplicate-only batches; the ready queue is now an index keyed by each operation's blocking cause (merge of 1000 reversed ops: ~4.8 ms to ~1.7 ms). **Not done:** `ReplicatedLog::insert` is still O(n) in the worst case (Vec insert).
- [x] Regression test for each fix above (`tests/regressions.rs`, `tests/transport_limits.rs`). They were written against the fixed code and not run against the pre-fix code.

### B. Missing features
- [x] Snapshot transfer (`State::encode_state` / `decode_state`, `Snapshot` wire message). Chunked over several frames (`SnapshotBegin` and `SnapshotChunk`), and states above 256 MiB are refused
- [x] TCP listener and auto-reconnect with backoff (`TcpAcceptor`, `ReconnectingTcp`). The application maps accepted connections to peers
- [x] `Authorizer` trait (capability hook, default allow-all) — implemented as `OpPolicy` (`AllowAll` default), which also covers the next item
- [x] App-level op validation hook — `OpPolicy::check` returns `ApplyError::Invalid(reason)`; refusals are quarantined like state refusals

### C. Innovation (optional)
- [x] `crdt` feature: `PnCounter` and `OrSet` (done). **Not done:** RGA list/text, MV-Register
- [x] Deterministic network simulator (`sim`): seeded writes, partitions, late joiners, loss, duplication and reordering. Latency is not modelled; the random poll order stands in for it
- [ ] Anti-entropy efficiency: range digests or compressed vector clocks (not started)
- [x] `Replica::stats()` and an event sink (`with_event_observer`). Tracing spans are not wired in
- [x] Persistence: `Replica::checkpoint` / `recover` with `CheckpointStore`; `MemoryStore` and `FileStore` (atomic save)
- [ ] Replay protection (session nonce in `Envelope`): deferred. Operations are idempotent, so a replayed frame costs bandwidth but cannot change state; the wire change is not justified without a threat model

### D. Usability and adoption
- [x] `prelude` module and `Replica::builder()` with defaults; ~10-line quickstart
- [x] `examples/quickstart.rs` and README as doctest
- [x] Examples: `tcp_chat`, `custom_crdt`, `custom_signer`, `three_peer_partition`
- [ ] Demo CLI example (`serve` / `connect` / `set` / `get`) and a `cargo-generate` app template. **Not done:** `tcp_chat` covers serve and connect only; no template
- [x] `docs/` guide: concepts, protocol diagram, trait recipes, stub-to-real migration, troubleshooting (`docs/guide.md`)
- [ ] `rust-toolchain.toml` and devcontainer (done). **Not done:** Dockerfile and 3-peer docker-compose demo (not runnable here, so not written)
- [ ] README badges and install instructions (install done; badges need the repository URL, which is not known yet)

### E. Automation and release
- [x] Commit the untracked source tree (commit `ca3e882` on branch `phase-5-review-fixes`)
- [x] `.github/workflows/ci.yml`: OS matrix, stable + MSRV 1.70, thumbv7em, fmt, clippy, doc, run examples
- [x] `scripts/ci.sh` twin of `scripts/ci.ps1` (with `rustup target add`)
- [ ] Scheduled workflow: heavy proptest, `cargo audit`, coverage, `cargo deny` (done). **Not done:** `cargo-semver-checks` (needs a published baseline). `cargo deny` was not run locally
- [x] Dependabot, release workflow (dry run on tags; no automatic publish), `[package.metadata.docs.rs]`
- [x] Criterion benches (merge, delta, codec)
- [x] `CONTRIBUTING.md`, `SECURITY.md`, issue / PR templates
- [ ] Choose license (suggest `MIT OR Apache-2.0`), add `LICENSE-*` and Cargo metadata
