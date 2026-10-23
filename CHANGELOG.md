# Changelog

## 1.0.0 (unreleased)

First full implementation of the design in `spec.txt` and `TODO.md`.

### Added

- Hybrid Logical Clocks with injected time sources, bounded remote drift, and counter-overflow detection. `VectorClock` with causal comparison.
- `ReplicatedLog`: timestamp-ordered entries, per-actor clocks and watermarks, and compaction that tracks what it removed.
- `Merger`: validation of signatures, timestamps, and structure. Causal buffering with a bounded pending buffer. All-or-nothing batch application. Idempotent deduplication. Conflict detection for reused `(actor, seq)`.
- Delta sync: `calculate_delta`, chunking, a Hello/Delta/Ack handshake, anti-entropy re-sync, and pings with dead-peer detection.
- `Codec` trait, `BinaryCodec` (LEB128 varints, bounded decoding), and `Compressed` (PackBits).
- `Envelope`: sender-signed message bodies, verified before decoding.
- `Replica` engine: local operations (stamp, sign, apply, log, broadcast), remote frames, snapshot-gated compaction with tombstone GC.
- Transports: `MemoryTransport` with loss, duplication and reordering injection; `FileTransport` and `TcpTransport` (`std`); `WebRtcTransport` (`webrtc`).
- `stubs` module: `ActorId`, `UirNode`, `CrdtCompatible`, `State`, `Signer`/`Verifier` (not cryptographically secure), `SnapshotSink`, `DataChannel`, and an LWW map.
- Examples: `cad_vertex`, `spreadsheet_lww`, `rover_dtn`.
- `scripts/ci.ps1`: formatting, builds, tests, clippy, rustdoc, bare-metal build, and dependency audit.

### Changed

- Operations the state refuses are quarantined rather than failing the whole batch. The refusal is reported as `Event::OpRejected` and `MergeReport::rejected`.
- Delta frames are chunked by encoded size as well as operation count. Oversized and back-pressured frames are reported as `Event::OutboundDropped` instead of dropping the link.
- A `Hello` replaces the peer's recorded clock, so a restarted peer's lower clock is honoured.
- Peer liveness counts only authenticated frames. Timers are rebased if the physical clock moves backwards.
- `local_op` refuses operations that remote peers would reject (`EngineError::OperationTooLarge`).
- Compaction does not run while a replica has no peers.
- `TcpTransport::send` queues frames and never spins. A full outbound buffer returns `TransportError::Backpressure`. `connect` has a timeout.
- `FileTransport` fsyncs writes, skips names already in use, and discards oversized files.

### Added

- `OpPolicy` hook (default `AllowAll`) for authorization and application-level validation. Refusals use `ApplyError::Denied` or `ApplyError::Invalid`.
- Pending-buffer quotas: per-author count, total payload bytes, and age expiry. Ready operations are drained before quotas are checked.
- `Replica::export_log` and `Replica::restore` (with `LogImage`), so a replica can resume its sequence numbers and clock. The state is persisted separately.
- `Replica::remove_peer`, `Transport::is_delay_tolerant`, and `Event::ClockFoldFailed`.
- `FrameError::UnexpectedPong`: a `Pong` must answer a ping this replica sent.
- Snapshot transfer, chunked over several frames. A peer behind the compaction floor receives the sender's encoded state (`State::encode_state` / `decode_state`, now required methods). The receiver checks the state hash, keeps its own operations that the snapshot does not cover, and takes the retained operations from the delta that follows. Events: `PeerNeedsSnapshot`, `SnapshotInstalled`.
- `TcpAcceptor` (accepts connections without blocking) and `ReconnectingTcp` (dials with exponential backoff from 100 ms to 30 s, driven by the engine's polling).
- `Replica::builder(actor, state)` and `ReplicaBuilder`. `BuildError` reports a missing clock or keys, so no key is chosen silently.
- `prelude` module, `Replica::stats()` (`ReplicaStats`), and `LwwMap::iter()`.
- Examples: `quickstart`, `tcp_chat`, `custom_crdt`, `custom_signer`, `three_peer_partition`.
- Documentation: `docs/guide.md`, `CONTRIBUTING.md`, `SECURITY.md`, issue and pull-request templates. The README quick start is compiled as a doctest.
- Continuous integration: `.github/workflows/ci.yml` (three OS, MSRV 1.70, bare-metal, dependency check), `scheduled.yml` (heavy property tests, audit, coverage), `release.yml` (publish dry run), Dependabot, and `scripts/ci.sh`.
- Dual license: MIT OR Apache-2.0.

- Checkpoints: `Replica::checkpoint()` encodes the state and log into one blob, and `Replica::recover()` rebuilds a replica from it. Damaged input returns `RecoverError`, never a panic. `CheckpointStore` with `MemoryStore` and, under `std`, `FileStore` (atomic save: temporary file, sync, rename).
- `crdt` feature: `PnCounter` (up and down counter) and `OrSet` (add-wins set). Both tolerate repeated delivery, which snapshot installation can cause.
- `Replica::with_event_observer`: a callback that sees every event as it is raised. Polling with `drain_events` still works.
- `deny.toml` for `cargo deny`, run weekly by the scheduled workflow.

### Not yet

- Real `tpt-*` crate integration (blocked on their APIs).
- Crates.io publication (waiting on the first release).
- Deterministic network simulator, range-digest anti-entropy, replay protection, an event sink, and the `cargo-generate` template.
