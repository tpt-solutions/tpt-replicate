//! Replicas authenticated with real Ed25519 keys (tpt-crypto), behind the `ed25519` feature.

#![cfg(feature = "ed25519")]

mod common;

use common::*;
use tpt_replicate::clock::ManualClock;
use tpt_replicate::ed25519::{Ed25519Directory, Ed25519Signer};
use tpt_replicate::error::FrameError;
use tpt_replicate::stubs::lww::LwwMap;
use tpt_replicate::stubs::{ActorId, State};
use tpt_replicate::transport::Faults;
use tpt_replicate::{Event, Replica};

fn signer(actor: u64) -> Ed25519Signer {
    let mut seed = [0u8; 32];
    seed[0] = actor as u8;
    seed[31] = 0x5A;
    Ed25519Signer::from_seed(seed)
}

/// A replica that trusts exactly the keys in `directory`.
fn replica(actor: u64, clock: &ManualClock, directory: Ed25519Directory) -> Replica<LwwMap> {
    Replica::builder(ActorId(actor), LwwMap::new())
        .keys(Box::new(signer(actor)), Box::new(directory))
        .clock(Box::new(clock.clone()))
        .build()
        .expect("replica")
}

fn full_directory(actors: &[u64]) -> Ed25519Directory {
    let mut directory = Ed25519Directory::new();
    for &actor in actors {
        assert!(directory.register(ActorId(actor), &signer(actor).public_key()));
    }
    directory
}

#[test]
fn replicas_with_registered_keys_converge() {
    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock, full_directory(&[1, 2]));
    let mut b = replica(2, &clock, full_directory(&[1, 2]));
    link(&mut a, &mut b, 4, Faults::default());
    a.local_op(set("k", "signed")).unwrap();
    b.local_op(set("other", "also signed")).unwrap();
    settle(&mut [&mut a, &mut b], 6);
    assert_eq!(a.state().get(b"other"), Some(&b"also signed"[..]));
    assert_eq!(b.state().get(b"k"), Some(&b"signed"[..]));
    assert_eq!(a.state().state_hash(), b.state().state_hash());
}

#[test]
fn frames_from_an_unregistered_author_are_refused() {
    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock, full_directory(&[1]));
    let mut b = replica(2, &clock, full_directory(&[1, 2]));
    link(&mut a, &mut b, 5, Faults::default());
    b.local_op(set("from-b", "x")).unwrap();
    settle(&mut [&mut a, &mut b], 6);
    assert_eq!(a.state().get(b"from-b"), None);
    assert!(a.drain_events().iter().any(|e| matches!(
        e,
        Event::FrameRejected {
            error: FrameError::Auth,
            ..
        }
    )));
}

#[test]
fn frames_signed_with_the_wrong_key_are_refused() {
    let clock = ManualClock::new(1_000);
    // a registers actor 2 under actor 3's public key, so b's real signatures do not verify.
    let mut wrong = Ed25519Directory::new();
    assert!(wrong.register(ActorId(2), &signer(3).public_key()));
    let mut a = replica(1, &clock, wrong);
    let mut b = replica(2, &clock, full_directory(&[1, 2]));
    link(&mut a, &mut b, 6, Faults::default());
    b.local_op(set("forged", "x")).unwrap();
    settle(&mut [&mut a, &mut b], 6);
    assert_eq!(a.state().get(b"forged"), None);
    assert!(a.drain_events().iter().any(|e| matches!(
        e,
        Event::FrameRejected {
            error: FrameError::Auth,
            ..
        }
    )));
}

#[test]
fn an_invalid_public_key_is_not_registered() {
    let mut directory = Ed25519Directory::new();
    // All-ones is not a canonical encoding of any Edwards point.
    assert!(!directory.register(ActorId(9), &[0xFF; 32]));
    assert!(directory.register(ActorId(9), &signer(9).public_key()));
}
