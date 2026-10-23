//! Three designers edit one CAD vertex. Two of them move it at the same instant while offline from
//! each other. Hybrid logical clocks pick the same winner on every replica.
//!
//! Run with `cargo run --example cad_vertex`.

use tpt_replicate::clock::ManualClock;
use tpt_replicate::stubs::crypto::{KeyedHashSigner, KeyedHashVerifier};
use tpt_replicate::stubs::lww::{LwwMap, LwwOp};
use tpt_replicate::transport::{Faults, MemoryTransport};
use tpt_replicate::{ActorId, Replica};

const SECRET: u64 = 0xCAD;
const VERTEX: &[u8] = b"part-7/vertex-3";

fn designer(actor: u64, clock: &ManualClock) -> Replica<LwwMap> {
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

fn move_vertex(designer: &mut Replica<LwwMap>, x: i32, y: i32) {
    let op = LwwOp::Set {
        key: VERTEX.to_vec(),
        value: format!("{x},{y}").into_bytes(),
    };
    designer.local_op(op.to_node()).expect("local op");
}

fn position(designer: &Replica<LwwMap>) -> String {
    let bytes = designer.state().get(VERTEX).unwrap_or(b"<none>");
    String::from_utf8_lossy(bytes).into_owned()
}

fn settle(designers: &mut [&mut Replica<LwwMap>]) {
    for _ in 0..4 {
        for designer in designers.iter_mut() {
            designer.poll();
        }
    }
}

fn main() {
    let clock = ManualClock::new(1_000);
    let mut ana = designer(1, &clock);
    let mut ben = designer(2, &clock);
    let mut cho = designer(3, &clock);

    let _ana_ben = link(&mut ana, &mut ben, 1);
    let ana_cho = link(&mut ana, &mut cho, 2);
    let ben_cho = link(&mut ben, &mut cho, 3);
    settle(&mut [&mut ana, &mut ben, &mut cho]);

    // Cho drops off the network and moves the vertex.
    ana_cho.set_up(false);
    ben_cho.set_up(false);
    move_vertex(&mut cho, 40, 40);
    settle(&mut [&mut ana, &mut ben, &mut cho]);

    // Ana and Ben move the vertex at the same wall-clock millisecond, after Cho's edit.
    clock.advance(5);
    move_vertex(&mut ana, 10, 10);
    move_vertex(&mut ben, 25, 5);
    settle(&mut [&mut ana, &mut ben, &mut cho]);
    println!(
        "after concurrent edits, ana={} ben={} cho={} (cho offline)",
        position(&ana),
        position(&ben),
        position(&cho)
    );

    // Cho reconnects. Its edit is older than both concurrent edits, so it does not win.
    ana_cho.set_up(true);
    ben_cho.set_up(true);
    settle(&mut [&mut ana, &mut ben, &mut cho]);
    println!(
        "after reconnect,        ana={} ben={} cho={}",
        position(&ana),
        position(&ben),
        position(&cho)
    );

    // A later edit from Cho wins everywhere.
    clock.advance(10);
    move_vertex(&mut cho, 9, 9);
    settle(&mut [&mut ana, &mut ben, &mut cho]);
    println!(
        "after a later edit,     ana={} ben={} cho={}",
        position(&ana),
        position(&ben),
        position(&cho)
    );

    assert_eq!(position(&ana), position(&ben));
    assert_eq!(position(&ben), position(&cho));
}
