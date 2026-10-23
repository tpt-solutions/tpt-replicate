//! Spreadsheet cells as last-writer-wins registers. Shows concurrent edits to one cell, and a
//! delete that an offline collaborator's older edit cannot undo.
//!
//! Run with `cargo run --example spreadsheet_lww`.

use tpt_replicate::clock::ManualClock;
use tpt_replicate::stubs::crypto::{KeyedHashSigner, KeyedHashVerifier};
use tpt_replicate::stubs::lww::{LwwMap, LwwOp};
use tpt_replicate::stubs::State;
use tpt_replicate::transport::{Faults, MemoryTransport};
use tpt_replicate::{ActorId, Replica};

const SECRET: u64 = 0x5EE7;

fn user(actor: u64, clock: &ManualClock) -> Replica<LwwMap> {
    Replica::new(
        ActorId(actor),
        LwwMap::new(),
        Box::new(KeyedHashSigner::new(ActorId(actor), SECRET)),
        Box::new(KeyedHashVerifier::new(SECRET)),
        Box::new(clock.clone()),
    )
}

fn link(a: &mut Replica<LwwMap>, b: &mut Replica<LwwMap>, seed: u64) -> MemoryTransport {
    let (ta, tb) = MemoryTransport::pair(seed, Faults::default());
    let control = ta.clone();
    a.add_peer(b.actor(), Box::new(ta));
    b.add_peer(a.actor(), Box::new(tb));
    control
}

fn write(user: &mut Replica<LwwMap>, cell: &str, value: &str) {
    let op = LwwOp::Set {
        key: cell.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
    };
    user.local_op(op.to_node()).expect("write");
}

fn clear(user: &mut Replica<LwwMap>, cell: &str) {
    let op = LwwOp::Delete {
        key: cell.as_bytes().to_vec(),
    };
    user.local_op(op.to_node()).expect("clear");
}

fn show(label: &str, user: &Replica<LwwMap>) {
    let a1 = user.state().get(b"A1").map_or("<empty>".into(), |v| {
        String::from_utf8_lossy(v).into_owned()
    });
    let b2 = user.state().get(b"B2").map_or("<empty>".into(), |v| {
        String::from_utf8_lossy(v).into_owned()
    });
    println!("{label}: A1={a1} B2={b2}");
}

fn settle(users: &mut [&mut Replica<LwwMap>]) {
    for _ in 0..4 {
        for user in users.iter_mut() {
            user.poll();
        }
    }
}

fn main() {
    let clock = ManualClock::new(1_000);
    let mut ada = user(1, &clock);
    let mut bo = user(2, &clock);
    let mut cy = user(3, &clock);

    let _ada_bo = link(&mut ada, &mut bo, 1);
    let ada_cy = link(&mut ada, &mut cy, 2);
    let bo_cy = link(&mut bo, &mut cy, 3);
    settle(&mut [&mut ada, &mut bo, &mut cy]);

    write(&mut ada, "B2", "100");
    settle(&mut [&mut ada, &mut bo, &mut cy]);

    // Cy goes offline and types into B2 at t=1500.
    ada_cy.set_up(false);
    bo_cy.set_up(false);
    clock.set(1_500);
    write(&mut cy, "B2", "7");

    // Ada and Bo, online together, clear B2 at t=2000 and both write A1.
    clock.set(2_000);
    clear(&mut bo, "B2");
    write(&mut ada, "A1", "=SUM(A2:A9)");
    clock.set(2_001);
    write(&mut bo, "A1", "42");
    settle(&mut [&mut ada, &mut bo, &mut cy]);
    show("ada before Cy reconnects", &ada);

    // Cy comes back with its old edit. The later clear wins, so B2 stays empty.
    ada_cy.set_up(true);
    bo_cy.set_up(true);
    settle(&mut [&mut ada, &mut bo, &mut cy]);
    show("ada after Cy reconnects", &ada);
    show("bo  after Cy reconnects", &bo);
    show("cy  after Cy reconnects", &cy);

    assert_eq!(ada.state().get(b"A1"), Some(&b"42"[..]));
    assert_eq!(ada.state().get(b"B2"), None);
    assert_eq!(ada.state().state_hash(), cy.state().state_hash());
}
