//! # tpt-replicate
//!
//! Pure-Rust, offline-first replication engine for the TPT ecosystem. It provides Hybrid
//! Logical Clocks, a causally ordered replicated operation log, a deterministic merge executor,
//! a delta-sync protocol and a transport-agnostic peer runtime.
//!
//! The core is `no_std` + `alloc`. The `std` feature adds `SystemClock`, `FileTransport` (for
//! delay-tolerant links), `TcpTransport`, `TcpAcceptor` and `ReconnectingTcp`. The `webrtc`
//! feature adds a data-channel adapter.
//!
//! Start with [`Replica::builder`] or [`prelude`]. The guide in `docs/guide.md` covers concepts
//! and the wire protocol.
//!
//! Sibling TPT crates (`tpt-operations`, `tpt-primitives`, `tpt-crypto`, `tpt-uir`,
//! `tpt-version`, `tpt-webrtc`) are not published yet. [`stubs`] supplies local stand-ins that
//! match the trait shapes the engine needs. Swapping them out is tracked in `TODO.md`.
#![no_std]
#![forbid(unsafe_code)]

extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

pub mod builder;
pub mod checkpoint;
pub mod clock;
pub mod codec;
#[cfg(feature = "crdt")]
pub mod crdt;
#[cfg(feature = "ed25519")]
pub mod ed25519;
pub mod engine;
pub mod error;
pub mod gc;
pub mod log;
pub mod merge;
pub mod peer;
pub mod prelude;
pub mod sim;
pub mod stubs;
pub mod sync;
pub mod transport;
#[cfg(feature = "version")]
pub mod version;
#[cfg(feature = "webrtc-app")]
pub mod webrtc_app;

pub use builder::{BuildError, ReplicaBuilder};
#[cfg(feature = "std")]
pub use checkpoint::FileStore;
pub use checkpoint::{CheckpointStore, MemoryStore};

// The README's code blocks are compiled as doctests, so the quick start cannot go stale.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

pub use clock::{CausalOrder, HybridClock, Timestamp, VectorClock};
pub use engine::{Event, LogImage, Replica, ReplicaConfig, ReplicaStats};
pub use log::{ReplicatedLog, StampedOperation};
pub use merge::{AllowAll, MergeLimits, MergeReport, Merger, OpPolicy};
pub use peer::PeerState;
pub use stubs::ActorId;
