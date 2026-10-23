# Contributing

Thanks for looking at tpt-replicate. It is a replication engine, so correctness matters more than
speed of change. The notes below say what a change needs before it can be merged.

## Ground rules

- The core stays `no_std` + `alloc` with no runtime dependencies. `cargo tree -e normal
  --no-default-features` must list only this crate. Dev-dependencies (for tests) are fine.
- `#![forbid(unsafe_code)]` stays on. Do not add `unsafe`.
- Anything that decodes bytes from a peer must return an error on bad input, never panic. Add a
  test with truncated or hostile input.
- Behaviour that affects convergence (merge order, compaction, snapshot installation, the wire
  format) needs a test that shows the convergence, not only that the code runs.

## Before you open a pull request

Run the same checks CI runs:

```sh
sh scripts/ci.sh          # or: powershell -ExecutionPolicy Bypass -File scripts/ci.ps1
```

That covers formatting, builds with and without features, all tests, clippy and rustdoc with
warnings as errors, the bare-metal build, and the dependency check.

## Tests

- `tests/convergence.rs`, `tests/semantics.rs`, `tests/snapshot.rs`: scenario tests on the engine.
- `tests/regressions.rs`: one test per fixed bug. When you fix a bug, add a test here that would
  fail without the fix.
- `tests/fuzz.rs`, `tests/log_invariants.rs`: randomized and property-based checks.
- `tests/transport_std.rs`, `tests/transport_limits.rs`: std transports (`--features std`).

Tests that depend on time use `ManualClock`. Advance it past the resync interval before expecting
relayed operations to arrive. Tests that use real sockets must not rely on exact timing.

## Commits and pull requests

- Keep a pull request to one change. Describe the behaviour change, and call out anything that
  changes the wire format or a public trait.
- Update `CHANGELOG.md` under the unreleased version for user-visible changes.
- Update `docs/guide.md` when you change the protocol, an event, or a configuration default.

## Licensing

The project is dual licensed under MIT or Apache-2.0. By submitting a contribution you agree that it
is licensed the same way, as stated in the Apache-2.0 license (section 5).
