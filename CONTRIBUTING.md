# Contributing

Thanks for looking at tpt-replicate. Contributions are handled through issues only. Pull requests
are not accepted. Open an issue to report a bug, ask a question, or propose a change, and describe
the behaviour you expect. Do not send code changes as a pull request; they will not be merged.

Security problems are not issues. Report them as described in [`SECURITY.md`](SECURITY.md).

## Rules for any change

The maintainers apply these when they act on an issue. They also describe what a useful report
should respect.

## Ground rules

- The core stays `no_std` + `alloc` with no runtime dependencies. `cargo tree -e normal
  --no-default-features` must list only this crate. Dev-dependencies (for tests) are fine.
- `#![forbid(unsafe_code)]` stays on. Do not add `unsafe`.
- Anything that decodes bytes from a peer must return an error on bad input, never panic. Add a
  test with truncated or hostile input.
- Behaviour that affects convergence (merge order, compaction, snapshot installation, the wire
  format) needs a test that shows the convergence, not only that the code runs.

## Checks the maintainers run

```sh
sh scripts/ci.sh          # or: powershell -ExecutionPolicy Bypass -File scripts/ci.ps1
```

That covers formatting, builds with and without features, all tests, clippy and rustdoc with
warnings as errors, the bare-metal build, and the dependency check.

## Tests

- `tests/convergence.rs`, `tests/semantics.rs`, `tests/snapshot.rs`: scenario tests on the engine.
- `tests/regressions.rs`: one test per fixed bug.
- `tests/fuzz.rs`, `tests/log_invariants.rs`: randomized and property-based checks.
- `tests/transport_std.rs`, `tests/transport_limits.rs`: std transports (`--features std`).

Tests that depend on time use `ManualClock`. Advance it past the resync interval before expecting
relayed operations to arrive. Tests that use real sockets must not rely on exact timing.

## Licensing

The project is dual licensed under MIT or Apache-2.0. By opening an issue you agree that any
material you submit is licensed the same way, as stated in the Apache-2.0 license (section 5).
