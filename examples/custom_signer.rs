//! Plugging in your own signing and verification. The engine only sees the `Signer` and `Verifier`
//! traits, so a real key store, hardware token or signature scheme can replace the stub.
//!
//! This example wraps the stub signer to count signatures, and wraps the verifier with an allow
//! list. Frames from actors that are not on the list are dropped before any operation is decoded.
//!
//! Run with `cargo run --example custom_signer`.

use std::cell::Cell;
use std::collections::BTreeSet;

use tpt_replicate::clock::ManualClock;
use tpt_replicate::prelude::*;
use tpt_replicate::stubs::crypto::{Signature, Verifier};

/// Counts how many messages this replica has signed.
struct CountingSigner {
    inner: KeyedHashSigner,
    signed: Cell<u64>,
}

impl Signer for CountingSigner {
    fn sign(&self, msg: &[u8]) -> Signature {
        self.signed.set(self.signed.get() + 1);
        self.inner.sign(msg)
    }
}

/// Accepts signatures only from actors on the allow list.
struct AllowList {
    inner: KeyedHashVerifier,
    allowed: BTreeSet<ActorId>,
}

impl Verifier for AllowList {
    fn verify(&self, actor: ActorId, msg: &[u8], sig: &[u8]) -> bool {
        self.allowed.contains(&actor) && self.inner.verify(actor, msg, sig)
    }
}

fn main() {
    const SECRET: u64 = 99;
    let clock = ManualClock::new(1_000);
    let trusted: BTreeSet<ActorId> = [ActorId(1), ActorId(2)].into_iter().collect();

    let mut ada = Replica::builder(ActorId(1), LwwMap::new())
        .keys(
            Box::new(CountingSigner {
                inner: KeyedHashSigner::new(ActorId(1), SECRET),
                signed: Cell::new(0),
            }),
            Box::new(AllowList {
                inner: KeyedHashVerifier::new(SECRET),
                allowed: trusted.clone(),
            }),
        )
        .clock(Box::new(clock.clone()))
        .build()
        .expect("replica");
    let mut bo = Replica::builder(ActorId(2), LwwMap::new())
        .keys(
            Box::new(KeyedHashSigner::new(ActorId(2), SECRET)),
            Box::new(AllowList {
                inner: KeyedHashVerifier::new(SECRET),
                allowed: trusted,
            }),
        )
        .clock(Box::new(clock))
        .build()
        .expect("replica");

    let (to_bo, to_ada) = MemoryTransport::pair(4, Faults::default());
    ada.add_peer(bo.actor(), Box::new(to_bo));
    bo.add_peer(ada.actor(), Box::new(to_ada));

    let op = LwwOp::Set {
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    };
    ada.local_op(op.to_node()).expect("write");
    for _ in 0..4 {
        ada.poll();
        bo.poll();
    }
    assert_eq!(bo.state().get(b"k"), Some(&b"v"[..]));
    println!("bo received the write; the verifier accepted it");
}
