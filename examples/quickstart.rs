//! The smallest useful program: two replicas share a key-value map over an in-memory link.
//!
//! Run with `cargo run --example quickstart`.

use tpt_replicate::prelude::*;

fn main() {
    // A manual clock keeps the example deterministic. Use `SystemClock` in a real program.
    let clock = ManualClock::new(1_000);
    // `shared_secret` is a stand-in signer for demos. Use `keys(...)` with real keys in production.
    let mut ada = Replica::builder(ActorId(1), LwwMap::new())
        .shared_secret(7)
        .clock(Box::new(clock.clone()))
        .build()
        .expect("replica");
    let mut bo = Replica::builder(ActorId(2), LwwMap::new())
        .shared_secret(7)
        .clock(Box::new(clock))
        .build()
        .expect("replica");

    let (to_bo, to_ada) = MemoryTransport::pair(1, Faults::default());
    ada.add_peer(bo.actor(), Box::new(to_bo));
    bo.add_peer(ada.actor(), Box::new(to_ada));

    let set = LwwOp::Set {
        key: b"greeting".to_vec(),
        value: b"hello from ada".to_vec(),
    };
    ada.local_op(set.to_node()).expect("local write");

    for _ in 0..4 {
        ada.poll();
        bo.poll();
    }
    let greeting = bo.state().get(b"greeting").map(String::from_utf8_lossy);
    println!("bo sees: {greeting:?}");
    assert_eq!(bo.state().get(b"greeting"), Some(&b"hello from ada"[..]));
}
