//! Three replicas in a triangle. Cy is cut off from Ada, the other two keep working, and the link
//! is restored. Everything converges, and nothing is lost.
//!
//! Run with `cargo run --example three_peer_partition`.

use tpt_replicate::clock::ManualClock;
use tpt_replicate::prelude::*;

fn node(actor: u64, clock: &ManualClock) -> Replica<LwwMap> {
    Replica::builder(ActorId(actor), LwwMap::new())
        .shared_secret(11)
        .clock(Box::new(clock.clone()))
        .build()
        .expect("replica")
}

/// Connects two replicas and returns the handle that controls the link.
fn connect(a: &mut Replica<LwwMap>, b: &mut Replica<LwwMap>, seed: u64) -> MemoryTransport {
    let (to_b, to_a) = MemoryTransport::pair(seed, Faults::default());
    let control = to_b.clone();
    a.add_peer(b.actor(), Box::new(to_b));
    b.add_peer(a.actor(), Box::new(to_a));
    control
}

fn set(replica: &mut Replica<LwwMap>, key: &str, value: &str) {
    let op = LwwOp::Set {
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
    };
    replica.local_op(op.to_node()).expect("write");
}

fn round(replicas: &mut [&mut Replica<LwwMap>], clock: &ManualClock) {
    for _ in 0..4 {
        for replica in replicas.iter_mut() {
            replica.poll();
        }
    }
    clock.advance(10);
}

fn main() {
    let clock = ManualClock::new(1_000);
    let (mut ada, mut bo, mut cy) = (node(1, &clock), node(2, &clock), node(3, &clock));
    connect(&mut ada, &mut bo, 1);
    connect(&mut bo, &mut cy, 2);
    let ada_cy = connect(&mut ada, &mut cy, 3);

    set(&mut ada, "shared", "before the split");
    round(&mut [&mut ada, &mut bo, &mut cy], &clock);

    // Cy loses its link to Ada. Cy is still reachable through Bo.
    ada_cy.set_up(false);
    set(&mut ada, "ada-note", "written while cy was cut off");
    set(&mut cy, "cy-note", "written while ada was cut off");
    round(&mut [&mut ada, &mut bo, &mut cy], &clock);

    // Ada and Cy have not heard from each other since the cut. Restoring the link and running
    // a few rounds lets their handshakes exchange what each side missed.
    ada_cy.set_up(true);
    for _ in 0..3 {
        round(&mut [&mut ada, &mut bo, &mut cy], &clock);
    }

    for (name, replica) in [("ada", &ada), ("bo", &bo), ("cy", &cy)] {
        println!("{name}: {} live keys", replica.state().iter().count());
    }
    assert_eq!(ada.state().state_hash(), bo.state().state_hash());
    assert_eq!(bo.state().state_hash(), cy.state().state_hash());
    assert!(ada.state().get(b"cy-note").is_some());
}
