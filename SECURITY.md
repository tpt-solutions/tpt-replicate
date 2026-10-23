# Security policy

## Supported versions

Only the latest release receives security fixes. Before 1.0.0 is published, only the `master`
branch is supported.

## Reporting a vulnerability

Please do not open a public issue for a security problem. Report it privately through GitHub's
"Report a vulnerability" button on the repository's Security tab, or by email to the maintainer.
Include:

- the version or commit,
- the smallest input or sequence of frames that shows the problem, and
- what you expected and what happened.

You should get an acknowledgement within a week. Fixes are released with a note in the changelog.

## What is in scope

- Frames or snapshots that make a replica panic, allocate without bound, or never terminate.
- Operations accepted from a peer that the sender could not have authored (authentication bypass).
- Replicas that diverge permanently when every peer is honest.
- Compaction or snapshot paths that drop operations a peer still needs.

## Known limitations

These are documented in the README and are not treated as vulnerabilities in this crate by
themselves:

- The bundled `stubs::crypto` signer is a keyed 64-bit hash. It is **not** a secure signature
  scheme and must be replaced (for example by `tpt-crypto`) before use across untrusted networks.
- Snapshots are trusted from authenticated peers. A snapshot is checked against its hash, but its
  operations are not re-checked against the operation policy.
