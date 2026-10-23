//! A builder for [`Replica`], so a replica can be set up in a few lines.

use alloc::boxed::Box;

use crate::clock::ClockSource;
use crate::engine::{Replica, ReplicaConfig};
use crate::merge::OpPolicy;
use crate::stubs::crypto::{KeyedHashSigner, KeyedHashVerifier, Signer, Verifier};
use crate::stubs::{ActorId, SnapshotSink, State};

/// Why [`ReplicaBuilder::build`] could not produce a replica.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BuildError {
    /// No clock was given, and the `std` feature (which provides `SystemClock`) is off.
    MissingClock,
    /// No signing keys were given. Call [`ReplicaBuilder::keys`] or
    /// [`ReplicaBuilder::shared_secret`].
    MissingKeys,
}

/// Collects the parts of a replica. Created with [`Replica::builder`].
///
/// Only the clock and the keys have no default. The clock defaults to `SystemClock` when the `std`
/// feature is on. The keys must be given explicitly, so that a replica is never created with a key
/// nobody chose.
pub struct ReplicaBuilder<S: State> {
    actor: ActorId,
    state: S,
    keys: Option<(Box<dyn Signer>, Box<dyn Verifier>)>,
    clock: Option<Box<dyn ClockSource>>,
    config: ReplicaConfig,
    policy: Option<Box<dyn OpPolicy>>,
    snapshot_sink: Option<Box<dyn SnapshotSink>>,
}

impl<S: State> ReplicaBuilder<S> {
    pub(crate) fn new(actor: ActorId, state: S) -> Self {
        Self {
            actor,
            state,
            keys: None,
            clock: None,
            config: ReplicaConfig::default(),
            policy: None,
            snapshot_sink: None,
        }
    }

    /// Uses the given signer and verifier.
    pub fn keys(mut self, signer: Box<dyn Signer>, verifier: Box<dyn Verifier>) -> Self {
        self.keys = Some((signer, verifier));
        self
    }

    /// Uses the stub keyed-hash signer with `secret`. **Not secure.** It exists for tests and
    /// examples. Use [`ReplicaBuilder::keys`] with real keys in deployments.
    pub fn shared_secret(self, secret: u64) -> Self {
        let signer = Box::new(KeyedHashSigner::new(self.actor, secret));
        let verifier = Box::new(KeyedHashVerifier::new(secret));
        self.keys(signer, verifier)
    }

    /// Uses the given time source.
    pub fn clock(mut self, clock: Box<dyn ClockSource>) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Uses the given tuning. Defaults to [`ReplicaConfig::default`].
    pub fn config(mut self, config: ReplicaConfig) -> Self {
        self.config = config;
        self
    }

    /// Uses the given operation policy. Defaults to allowing every operation.
    pub fn policy(mut self, policy: Box<dyn OpPolicy>) -> Self {
        self.policy = Some(policy);
        self
    }

    /// Enables compaction, with snapshots persisted to `sink`.
    pub fn snapshot_sink(mut self, sink: Box<dyn SnapshotSink>) -> Self {
        self.snapshot_sink = Some(sink);
        self
    }

    /// Builds the replica.
    pub fn build(self) -> Result<Replica<S>, BuildError> {
        let (signer, verifier) = self.keys.ok_or(BuildError::MissingKeys)?;
        let clock = match self.clock {
            Some(clock) => clock,
            None => default_clock().ok_or(BuildError::MissingClock)?,
        };
        let mut replica =
            Replica::new(self.actor, self.state, signer, verifier, clock).with_config(self.config);
        if let Some(policy) = self.policy {
            replica = replica.with_policy(policy);
        }
        if let Some(sink) = self.snapshot_sink {
            replica = replica.with_snapshot_sink(sink);
        }
        Ok(replica)
    }
}

#[cfg(feature = "std")]
fn default_clock() -> Option<Box<dyn ClockSource>> {
    Some(Box::new(crate::clock::SystemClock))
}

#[cfg(not(feature = "std"))]
fn default_clock() -> Option<Box<dyn ClockSource>> {
    None
}
