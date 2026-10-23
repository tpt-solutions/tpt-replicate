//! A rover and an orbiter sync through a delay-tolerant link. The link is a pair of directories.
//! Frames sit on disk until the other side reads them, so the rover can keep working while no
//! contact is available.
//!
//! Run with `cargo run --example rover_dtn --features std`.

use std::fs;
use std::path::PathBuf;

use tpt_replicate::clock::ManualClock;
use tpt_replicate::stubs::crypto::{KeyedHashSigner, KeyedHashVerifier};
use tpt_replicate::stubs::lww::{LwwMap, LwwOp};
use tpt_replicate::stubs::State;
use tpt_replicate::transport::FileTransport;
use tpt_replicate::{ActorId, Replica};

const SECRET: u64 = 0x0A7;

fn node(actor: u64, clock: &ManualClock) -> Replica<LwwMap> {
    Replica::new(
        ActorId(actor),
        LwwMap::new(),
        Box::new(KeyedHashSigner::new(ActorId(actor), SECRET)),
        Box::new(KeyedHashVerifier::new(SECRET)),
        Box::new(clock.clone()),
    )
}

fn set(replica: &mut Replica<LwwMap>, key: &str, value: &str) {
    let op = LwwOp::Set {
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
    };
    replica.local_op(op.to_node()).expect("local op");
}

fn main() {
    let root: PathBuf = std::env::temp_dir().join("tpt-replicate-rover-dtn");
    let _ = fs::remove_dir_all(&root);
    let rover_to_orbiter = root.join("rover-to-orbiter");
    let orbiter_to_rover = root.join("orbiter-to-rover");

    let clock = ManualClock::new(10_000);
    let mut rover = node(10, &clock);
    let mut orbiter = node(20, &clock);
    rover.add_peer(
        orbiter.actor(),
        Box::new(FileTransport::new(&rover_to_orbiter, &orbiter_to_rover).expect("rover link")),
    );
    orbiter.add_peer(
        rover.actor(),
        Box::new(FileTransport::new(&orbiter_to_rover, &rover_to_orbiter).expect("orbiter link")),
    );

    // The rover logs survey results with no contact. Nothing reaches the orbiter yet.
    for sample in 0..5 {
        set(
            &mut rover,
            &format!("sample/{sample}"),
            &format!("{}.{}", 3 + sample, sample),
        );
    }
    rover.poll();
    println!(
        "rover queued {} operations for the orbiter",
        rover.pending_for(orbiter.actor()).unwrap_or(0)
    );

    // Contact window: the orbiter reads what the rover left on the link and replies.
    for _ in 0..3 {
        orbiter.poll();
        rover.poll();
    }
    println!("orbiter now holds {} operations", orbiter.log().len());

    // The orbiter updates a shared setting and passes it back on the next window.
    set(&mut orbiter, "mode", "science");
    for _ in 0..3 {
        rover.poll();
        orbiter.poll();
    }
    println!(
        "rover mode = {:?}",
        rover.state().get(b"mode").map(String::from_utf8_lossy)
    );

    assert_eq!(rover.state().state_hash(), orbiter.state().state_hash());
    assert_eq!(rover.state().get(b"sample/4"), Some(&b"7.4"[..]));
    let _ = fs::remove_dir_all(&root);
}
