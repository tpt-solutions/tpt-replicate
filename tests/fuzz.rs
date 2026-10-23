//! Fuzz-style tests. Malformed, truncated and tampered input must fail gracefully and leave the
//! replica unchanged.

mod common;

use common::*;
use proptest::prelude::*;
use tpt_replicate::clock::{ManualClock, VectorClock};
use tpt_replicate::codec::{BinaryCodec, Codec, Compressed};
use tpt_replicate::stubs::crypto::KeyedHashSigner;
use tpt_replicate::stubs::State;
use tpt_replicate::sync::{Envelope, Message};
use tpt_replicate::transport::{MemoryTransport, Transport};
use tpt_replicate::{ActorId, Event};

fn sealed_delta(actor: u64, ops: Vec<tpt_replicate::StampedOperation>) -> Vec<u8> {
    let body = BinaryCodec.encode(&Message::Delta(ops));
    Envelope::seal(
        &KeyedHashSigner::new(ActorId(actor), SECRET),
        ActorId(actor),
        body,
    )
    .encode()
}

proptest! {
    #[test]
    fn decoders_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
        let _ = BinaryCodec.decode(&bytes);
        let _ = Compressed::new(BinaryCodec).decode(&bytes);
        let _ = Envelope::decode(&bytes);
    }

    #[test]
    fn every_truncation_of_a_delta_fails_to_decode(count in 1usize..6) {
        let clock = ManualClock::new(1_000);
        let mut author = replica(1, &clock);
        for i in 0..count {
            author.local_op(set(&format!("k{i}"), "value")).unwrap();
        }
        let body = BinaryCodec.encode(&Message::Delta(author.log().ops().to_vec()));
        for cut in 0..body.len() {
            prop_assert!(BinaryCodec.decode(&body[..cut]).is_err(), "prefix of length {} decoded", cut);
        }
        prop_assert!(BinaryCodec.decode(&body).is_ok());
    }

    #[test]
    fn tampered_frames_are_rejected_and_state_is_untouched(index in any::<usize>(), flip in 1u8..=255) {
        let clock = ManualClock::new(1_000);
        let mut author = replica(1, &clock);
        author.local_op(set("x", "1")).unwrap();
        author.local_op(set("y", "2")).unwrap();
        let mut frame = sealed_delta(1, author.log().ops().to_vec());
        let at = index % frame.len();
        frame[at] ^= flip;

        let mut victim = replica(2, &clock);
        let (ours, theirs) = MemoryTransport::pair(3, Default::default());
        victim.add_peer(ActorId(1), Box::new(ours));
        let mut sender = theirs;
        sender.send(&frame).unwrap();
        victim.poll();

        prop_assert!(victim.log().is_empty());
        prop_assert_eq!(victim.state().state_hash(), tpt_replicate::stubs::lww::LwwMap::new().state_hash());
        let rejected = victim
            .drain_events()
            .iter()
            .any(|event| matches!(event, Event::FrameRejected { .. }));
        prop_assert!(rejected);
    }
}

#[test]
fn random_garbage_frames_never_change_state() {
    let clock = ManualClock::new(1_000);
    let mut victim = replica(2, &clock);
    let (ours, theirs) = MemoryTransport::pair(17, Default::default());
    victim.add_peer(ActorId(1), Box::new(ours));
    let mut sender = theirs;
    let mut rng = 0x9E37_79B9_7F4A_7C15u64;
    for _ in 0..300 {
        let mut frame = Vec::new();
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let len = (rng % 200) as usize;
        for _ in 0..len {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            frame.push(rng as u8);
        }
        sender.send(&frame).unwrap();
        victim.poll();
    }
    assert!(victim.log().is_empty());
    assert_eq!(
        victim.state().state_hash(),
        tpt_replicate::stubs::lww::LwwMap::new().state_hash()
    );
}

#[test]
fn signer_for_wrong_actor_is_rejected() {
    let clock = ManualClock::new(1_000);
    let mut author = replica(1, &clock);
    author.local_op(set("x", "1")).unwrap();
    // Sealed as actor 1, but the envelope claims actor 3.
    let body = BinaryCodec.encode(&Message::Ack(VectorClock::new()));
    let forged =
        Envelope::seal(&KeyedHashSigner::new(ActorId(1), SECRET), ActorId(3), body).encode();

    let mut victim = replica(2, &clock);
    let (ours, theirs) = MemoryTransport::pair(5, Default::default());
    victim.add_peer(ActorId(3), Box::new(ours));
    let mut sender = theirs;
    sender.send(&forged).unwrap();
    victim.poll();
    assert!(victim.drain_events().iter().any(|event| matches!(
        event,
        Event::FrameRejected {
            error: tpt_replicate::error::FrameError::Auth,
            ..
        }
    )));
}
