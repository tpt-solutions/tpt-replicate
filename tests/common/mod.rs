#![allow(dead_code)]

use tpt_replicate::clock::{ManualClock, Timestamp, VectorClock};
use tpt_replicate::log::StampedOperation;
use tpt_replicate::stubs::crypto::{KeyedHashSigner, KeyedHashVerifier, Signer};
use tpt_replicate::stubs::lww::{LwwMap, LwwOp};
use tpt_replicate::stubs::{ActorId, UirNode};
use tpt_replicate::transport::{Faults, MemoryTransport};
use tpt_replicate::Replica;

pub const SECRET: u64 = 0xFEED;

pub fn replica(actor: u64, clock: &ManualClock) -> Replica<LwwMap> {
    Replica::new(
        ActorId(actor),
        LwwMap::new(),
        Box::new(KeyedHashSigner::new(ActorId(actor), SECRET)),
        Box::new(KeyedHashVerifier::new(SECRET)),
        Box::new(clock.clone()),
    )
}

pub fn set(key: &str, value: &str) -> UirNode {
    LwwOp::Set {
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
    }
    .to_node()
}

pub fn delete(key: &str) -> UirNode {
    LwwOp::Delete {
        key: key.as_bytes().to_vec(),
    }
    .to_node()
}

/// Connects two replicas with an in-memory link. Returns a handle that can partition it.
pub fn link(
    a: &mut Replica<LwwMap>,
    b: &mut Replica<LwwMap>,
    seed: u64,
    faults: Faults,
) -> MemoryTransport {
    let (ta, tb) = MemoryTransport::pair(seed, faults);
    let control = ta.clone();
    a.add_peer(b.actor(), Box::new(ta));
    b.add_peer(a.actor(), Box::new(tb));
    control
}

/// Runs `rounds` full polling passes over every replica.
pub fn settle(replicas: &mut [&mut Replica<LwwMap>], rounds: usize) {
    for _ in 0..rounds {
        for replica in replicas.iter_mut() {
            replica.poll();
        }
    }
}

/// Builds an operation with a valid signature for `actor`, whatever its contents. Used to test
/// how the merge engine treats hostile but authenticated input.
pub fn forge(
    actor: u64,
    seq: u64,
    wall: u64,
    deps: VectorClock,
    payload: &[u8],
) -> StampedOperation {
    let mut op = StampedOperation {
        timestamp: Timestamp {
            wall_time: wall,
            logical_counter: 0,
            actor_id: ActorId(actor),
        },
        actor: ActorId(actor),
        seq,
        deps,
        op: UirNode {
            bytes: payload.to_vec(),
        },
        signature: Vec::new(),
    };
    op.signature = KeyedHashSigner::new(ActorId(actor), SECRET).sign(&op.signing_bytes());
    op
}
