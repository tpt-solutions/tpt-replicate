//! # tpt-replicate
//!
//! Pure-Rust, offline-first replication engine for the TPT ecosystem. It provides Hybrid
//! Logical Clocks, a causally ordered replicated operation log, a deterministic merge executor,
//! a delta-sync protocol and a transport-agnostic peer runtime.
//!
//! The core is `no_std` + `alloc`. The `std` feature adds `SystemClock`, `FileTransport` (for
//! delay-tolerant links) and `TcpTransport`. The `webrtc` feature adds a data-channel adapter.
//!
//! Sibling TPT crates (`tpt-operations`, `tpt-primitives`, `tpt-crypto`, `tpt-uir`,
//! `tpt-version`, `tpt-webrtc`) are not published yet. [`stubs`] supplies local stand-ins that
//! match the trait shapes the engine needs. Swapping them out is tracked in `TODO.md`.
#![no_std]
#![forbid(unsafe_code)]

extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

pub mod clock;
pub mod codec;
pub mod engine;
pub mod error;
pub mod gc;
pub mod log;
pub mod merge;
pub mod peer;
pub mod stubs;
pub mod sync;
pub mod transport;

pub use clock::{CausalOrder, HybridClock, Timestamp, VectorClock};
pub use engine::{Event, LogImage, Replica, ReplicaConfig};
pub use log::{ReplicatedLog, StampedOperation};
pub use merge::{AllowAll, MergeLimits, MergeReport, Merger, OpPolicy};
pub use peer::PeerState;
pub use stubs::ActorId;
