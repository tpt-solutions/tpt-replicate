//! Checkpoints: a replica survives a restart with its state, log and clock intact.

mod common;

use common::*;
use tpt_replicate::checkpoint::{CheckpointStore, MemoryStore};
use tpt_replicate::clock::ManualClock;
use tpt_replicate::error::RecoverError;

use tpt_replicate::stubs::crypto::{KeyedHashSigner, KeyedHashVerifier};
use tpt_replicate::stubs::lww::LwwMap;
use tpt_replicate::stubs::{ActorId, State};
use tpt_replicate::transport::Faults;
use tpt_replicate::Replica;

fn recover(bytes: &[u8], clock: &ManualClock) -> Result<Replica<LwwMap>, RecoverError> {
    Replica::recover(
        bytes,
        ActorId(1),
        Box::new(KeyedHashSigner::new(ActorId(1), SECRET)),
        Box::new(KeyedHashVerifier::new(SECRET)),
        Box::new(clock.clone()),
    )
}

#[test]
fn checkpoint_restores_state_log_and_sequence_numbers() {
    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock);
    let mut b = replica(2, &clock);
    link(&mut a, &mut b, 3, Faults::default());
    for i in 0..3 {
        clock.advance(1);
        a.local_op(set(&format!("k{i}"), "v")).unwrap();
    }
    settle(&mut [&mut a, &mut b], 4);
    clock.advance(1);
    b.local_op(set("from-b", "kept")).unwrap();
    settle(&mut [&mut a, &mut b], 4);

    let mut store = MemoryStore::default();
    store.save(&a.checkpoint()).unwrap();
    let restored = recover(&store.load().unwrap().unwrap(), &clock).unwrap();

    assert_eq!(restored.state().state_hash(), a.state().state_hash());
    assert_eq!(restored.log().len(), a.log().len());
    assert_eq!(restored.state().get(b"from-b"), Some(&b"kept"[..]));
    // The sequence continues where the checkpoint left off, so peers do not see a gap or a reuse.
    let mut restored = restored;
    let next = restored.local_op(set("after", "restart")).unwrap();
    assert_eq!(next.seq, 4);
}

#[test]
fn damaged_checkpoints_are_refused_without_panicking() {
    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock);
    a.local_op(set("k", "v")).unwrap();
    let bytes = a.checkpoint();

    assert!(recover(&[], &clock).is_err());
    assert!(recover(&bytes[..bytes.len() / 2], &clock).is_err());
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(recover(&trailing, &clock).is_err());

    // Flip every byte in turn. Each result must be an error or a valid replica, never a panic.
    for i in 0..bytes.len() {
        let mut damaged = bytes.clone();
        damaged[i] ^= 0x55;
        let _ = recover(&damaged, &clock);
    }
}

#[cfg(feature = "std")]
#[test]
fn file_store_survives_a_process_restart() {
    use tpt_replicate::checkpoint::FileStore;

    let root = std::env::temp_dir().join(format!("tpt-replicate-ckpt-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("replica.ckpt");

    let clock = ManualClock::new(1_000);
    let mut store = FileStore::new(&path);
    assert_eq!(store.load().unwrap(), None);

    let mut a = replica(1, &clock);
    a.local_op(set("durable", "yes")).unwrap();
    store.save(&a.checkpoint()).unwrap();
    // A second save replaces the first.
    a.local_op(set("later", "also")).unwrap();
    store.save(&a.checkpoint()).unwrap();

    // A fresh store value, as if after a restart.
    let reopened = FileStore::new(&path);
    let restored = recover(&reopened.load().unwrap().unwrap(), &clock).unwrap();
    assert_eq!(restored.state().get(b"durable"), Some(&b"yes"[..]));
    assert_eq!(restored.state().get(b"later"), Some(&b"also"[..]));
    let _ = std::fs::remove_dir_all(&root);
}
