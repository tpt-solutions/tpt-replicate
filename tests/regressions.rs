//! One regression test per Phase 5 bug fix. Each guards against a failure that existed before the
//! fix.

mod common;

use common::*;
use tpt_replicate::clock::{ManualClock, VectorClock};
use tpt_replicate::codec::{BinaryCodec, Codec, MAX_PAYLOAD};
use tpt_replicate::engine::MAX_QUEUED_EVENTS;
use tpt_replicate::error::{ApplyError, EngineError, FrameError, MergeError, RestoreError};
use tpt_replicate::merge::OpPolicy;
use tpt_replicate::peer::{Peer, PeerState};
use tpt_replicate::stubs::crypto::{KeyedHashSigner, KeyedHashVerifier, Signer};
use tpt_replicate::stubs::lww::{LwwMap, LwwOp};
use tpt_replicate::stubs::{ActorId, MemorySnapshotSink, State, UirNode};
use tpt_replicate::sync::{Envelope, Message};
use tpt_replicate::transport::{Faults, MemoryTransport, Transport};
use tpt_replicate::{Event, Replica, ReplicaConfig};

/// Frame bytes a real peer would send, sealed as `sender` with the shared test key.
fn sealed(sender: u64, message: &Message) -> Vec<u8> {
    let signer = KeyedHashSigner::new(ActorId(sender), SECRET);
    let body = BinaryCodec.encode(message);
    Envelope::seal(&signer, ActorId(sender), body).encode()
}

fn lww_set(key: &str, value: &str) -> Vec<u8> {
    set(key, value).bytes
}

// Poisoned op wedges its batch -----------------------------------------------------------------

#[test]
fn op_refused_by_state_does_not_block_later_ops_from_same_author() {
    let clock = ManualClock::new(1_000);
    let mut b = replica(2, &clock);
    // Tag 0xEE is unknown to the LWW state, so the state refuses it.
    b.import_ops(vec![forge(1, 1, 1_000, VectorClock::new(), &[0xEE])])
        .unwrap();

    // Before the fix, seq 1 stayed in the pending buffer and seq 2 waited for it forever.
    let later = forge(1, 2, 1_001, VectorClock::new(), &lww_set("x", "ok"));
    let report = b.import_ops(vec![later]).unwrap();
    assert_eq!(report.applied, 1);
    assert_eq!(b.state().get(b"x"), Some(&b"ok"[..]));
    assert_eq!(b.log().clock().get(ActorId(1)), 2);
}

// Partial failure after merge commit -----------------------------------------------------------

#[test]
fn logical_counter_at_limit_is_refused_before_commit() {
    let clock = ManualClock::new(1_000);
    let mut b = replica(2, &clock);
    let mut op = forge(1, 1, 1_000, VectorClock::new(), &lww_set("x", "1"));
    op.timestamp.logical_counter = u32::MAX;
    op.signature = KeyedHashSigner::new(ActorId(1), SECRET).sign(&op.signing_bytes());
    assert_eq!(
        b.import_ops(vec![op]),
        Err(FrameError::Merge(MergeError::Malformed {
            actor: ActorId(1),
            seq: 1,
            reason: "logical counter at limit",
        }))
    );
    assert!(b.log().is_empty());
}

// Pending buffer can be pinned -----------------------------------------------------------------

#[test]
fn ready_ops_do_not_count_against_pending_quota() {
    let clock = ManualClock::new(1_000);
    let config = ReplicaConfig {
        max_pending: 1,
        ..ReplicaConfig::default()
    };
    let mut b = replica(2, &clock).with_config(config);
    // Both ops are ready in order. Before the fix, the batch was refused for holding two entries
    // at once, even though the first one leaves the buffer straight away.
    let batch = vec![
        forge(1, 1, 1_000, VectorClock::new(), &lww_set("x", "1")),
        forge(1, 2, 1_001, VectorClock::new(), &lww_set("y", "2")),
    ];
    let report = b.import_ops(batch).unwrap();
    assert_eq!(report.applied, 2);
    assert_eq!(report.buffered, 0);
}

#[test]
fn per_author_quota_stops_one_author_crowding_out_others() {
    let clock = ManualClock::new(1_000);
    let config = ReplicaConfig {
        max_pending_per_actor: 2,
        ..ReplicaConfig::default()
    };
    let mut b = replica(3, &clock).with_config(config);
    let blocked: Vec<_> = (2..=4)
        .map(|seq| forge(1, seq, 1_000, VectorClock::new(), &lww_set("x", "1")))
        .collect();
    assert_eq!(
        b.import_ops(blocked),
        Err(FrameError::Merge(MergeError::PendingActorOverflow {
            actor: ActorId(1),
            limit: 2,
        }))
    );
}

#[test]
fn pending_byte_budget_is_enforced() {
    let clock = ManualClock::new(1_000);
    let config = ReplicaConfig {
        max_pending_bytes: 10,
        ..ReplicaConfig::default()
    };
    let mut b = replica(2, &clock).with_config(config);
    let blocked = forge(1, 2, 1_000, VectorClock::new(), &[0; 20]);
    assert_eq!(
        b.import_ops(vec![blocked]),
        Err(FrameError::Merge(MergeError::PendingBytesExceeded {
            limit: 10,
        }))
    );
}

#[test]
fn held_ops_expire_and_are_repaired_later() {
    let clock = ManualClock::new(1_000);
    let config = ReplicaConfig {
        max_pending_age_ms: 100,
        ..ReplicaConfig::default()
    };
    let mut b = replica(2, &clock).with_config(config);
    // seq 2 arrives first and waits for seq 1.
    b.import_ops(vec![forge(
        1,
        2,
        1_000,
        VectorClock::new(),
        &lww_set("y", "2"),
    )])
    .unwrap();
    clock.set(2_000);
    // Expiry runs at the start of the next batch, so seq 2 is gone before seq 1 arrives. The
    // author's next anti-entropy hello brings it back.
    let report = b
        .import_ops(vec![forge(
            1,
            1,
            2_000,
            VectorClock::new(),
            &lww_set("x", "1"),
        )])
        .unwrap();
    assert_eq!(report.applied, 1);
    assert_eq!(report.buffered, 0);
    assert_eq!(b.log().clock().get(ActorId(1)), 1);
}

// Oversized frames cause a reconnect loop ------------------------------------------------------

#[test]
fn large_payloads_sync_without_oversized_frames() {
    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock);
    let mut b = replica(2, &clock);
    // 20 ops of 600 KB is 12 MB, more than one delta may carry. Before chunking by bytes, a full
    // chunk of 128 such ops exceeded the frame limit and the link was torn down again and again.
    for i in 0..20u8 {
        let mut value = vec![b'x'; 600 * 1024];
        value[0] = i;
        let op = LwwOp::Set {
            key: vec![i],
            value,
        };
        a.local_op(op.to_node()).unwrap();
    }
    link(&mut a, &mut b, 11, Faults::default());
    settle(&mut [&mut a, &mut b], 12);
    assert_eq!(b.log().len(), 20);
    assert_eq!(a.state().state_hash(), b.state().state_hash());
    let events = b.drain_events();
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::FrameRejected { .. })));
}

#[test]
fn local_op_too_large_for_peers_is_refused_before_it_is_logged() {
    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock);
    let huge = UirNode {
        bytes: vec![0; MAX_PAYLOAD + 1],
    };
    assert_eq!(a.local_op(huge), Err(EngineError::OperationTooLarge));
    assert!(a.log().is_empty());
    // The refused operation did not use up a sequence number.
    a.local_op(set("k", "v")).unwrap();
    assert_eq!(a.log().clock().get(ActorId(1)), 1);
}

// Tombstone GC safety --------------------------------------------------------------------------

#[test]
fn compaction_does_not_run_without_peers() {
    let clock = ManualClock::new(1_000);
    let config = ReplicaConfig {
        snapshot_every_ops: 1,
        ..ReplicaConfig::default()
    };
    let mut a = replica(1, &clock)
        .with_config(config)
        .with_snapshot_sink(Box::new(MemorySnapshotSink::default()));
    for i in 0..3 {
        a.local_op(set(&format!("k{i}"), "v")).unwrap();
    }
    a.poll();
    assert_eq!(a.log().len(), 3);
    assert!(!a
        .drain_events()
        .iter()
        .any(|e| matches!(e, Event::Compacted { .. })));
}

#[test]
fn remove_peer_reports_and_forgets_the_link() {
    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock);
    let mut b = replica(2, &clock);
    link(&mut a, &mut b, 3, Faults::default());
    settle(&mut [&mut a, &mut b], 4);
    assert!(a.remove_peer(b.actor()));
    assert!(!a.remove_peer(b.actor()));
    assert_eq!(a.peer_state(b.actor()), None);
    assert!(a
        .drain_events()
        .contains(&Event::PeerDisconnected(b.actor())));
}

#[test]
fn hello_replaces_recorded_peer_clock() {
    // A restarted peer reports a lower clock. Keeping the old, higher value would overstate what it
    // has, and compaction could then drop operations it still needs.
    let mut high = VectorClock::new();
    high.observe(ActorId(1), 9);
    let mut low = VectorClock::new();
    low.observe(ActorId(1), 2);

    let mut peer = Peer::new(ActorId(2));
    peer.absorb_clock(&high);
    peer.absorb_clock(&low);
    assert_eq!(
        peer.remote_clock.get(ActorId(1)),
        9,
        "acks only raise the clock"
    );
    peer.replace_clock(&low);
    assert_eq!(peer.remote_clock.get(ActorId(1)), 2);
}

// Restart safety -------------------------------------------------------------------------------

#[test]
fn restored_replica_resumes_sequence_numbers_and_rejects_wrong_state() {
    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock);
    for i in 0..3 {
        a.local_op(set(&format!("k{i}"), "v")).unwrap();
    }
    let image = a.export_log();
    let signer = || Box::new(KeyedHashSigner::new(ActorId(1), SECRET));
    let verifier = || Box::new(KeyedHashVerifier::new(SECRET));

    let mut restored = Replica::restore(
        ActorId(1),
        a.state().clone(),
        image.clone(),
        signer(),
        verifier(),
        Box::new(clock.clone()),
    )
    .unwrap();
    let next = restored.local_op(set("after", "restart")).unwrap();
    assert_eq!(next.seq, 4, "sequence resumes after the last logged op");
    assert!(next.timestamp > image.ops.last().unwrap().timestamp);

    let wrong = Replica::restore(
        ActorId(1),
        LwwMap::new(),
        image,
        signer(),
        verifier(),
        Box::new(clock.clone()),
    );
    assert!(matches!(wrong, Err(RestoreError::StateMismatch)));
}

// Clock and liveness ---------------------------------------------------------------------------

#[test]
fn garbage_frames_do_not_keep_a_dead_peer_alive() {
    let clock = ManualClock::new(10_000);
    let config = ReplicaConfig {
        peer_timeout_ms: 1_000,
        ..ReplicaConfig::default()
    };
    let mut a = replica(1, &clock).with_config(config);
    let (ta, mut tb) = MemoryTransport::pair(5, Faults::default());
    a.add_peer(ActorId(2), Box::new(ta));
    a.poll();
    for _ in 0..10 {
        clock.advance(200);
        tb.send(&[0xAB; 32]).unwrap();
        a.poll();
    }
    assert!(a
        .drain_events()
        .contains(&Event::PeerDisconnected(ActorId(2))));
}

#[test]
fn authenticated_frames_keep_a_peer_alive() {
    let clock = ManualClock::new(10_000);
    let config = ReplicaConfig {
        peer_timeout_ms: 1_000,
        ..ReplicaConfig::default()
    };
    let mut a = replica(1, &clock).with_config(config);
    let (ta, mut tb) = MemoryTransport::pair(5, Faults::default());
    a.add_peer(ActorId(2), Box::new(ta));
    a.poll();
    for nonce in 0..10u64 {
        clock.advance(200);
        tb.send(&sealed(2, &Message::Ping(nonce))).unwrap();
        a.poll();
    }
    assert!(!a
        .drain_events()
        .contains(&Event::PeerDisconnected(ActorId(2))));
    assert_ne!(a.peer_state(ActorId(2)), Some(PeerState::Disconnected));
}

#[test]
fn clock_moving_backwards_still_times_out_dead_peers() {
    let clock = ManualClock::new(100_000);
    let config = ReplicaConfig {
        peer_timeout_ms: 1_000,
        ..ReplicaConfig::default()
    };
    let mut a = replica(1, &clock).with_config(config);
    let (ta, _tb) = MemoryTransport::pair(5, Faults::default());
    a.add_peer(ActorId(2), Box::new(ta));
    a.poll();
    // Physical time jumps back, for example after a clock correction. Without the rebase, timers
    // set in the future would never expire.
    clock.set(100);
    for _ in 0..10 {
        clock.advance(200);
        a.poll();
    }
    assert!(a
        .drain_events()
        .contains(&Event::PeerDisconnected(ActorId(2))));
}

#[test]
fn pong_without_matching_ping_is_rejected() {
    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock);
    let (ta, mut tb) = MemoryTransport::pair(5, Faults::default());
    a.add_peer(ActorId(2), Box::new(ta));
    a.poll();
    tb.send(&sealed(2, &Message::Pong(424_242))).unwrap();
    a.poll();
    assert!(a.drain_events().contains(&Event::FrameRejected {
        peer: ActorId(2),
        error: FrameError::UnexpectedPong,
    }));
}

#[test]
fn event_queue_is_bounded() {
    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock);
    let (ta, mut tb) = MemoryTransport::pair(5, Faults::default());
    a.add_peer(ActorId(2), Box::new(ta));
    for _ in 0..(MAX_QUEUED_EVENTS + 50) {
        tb.send(&[0xAB; 32]).unwrap();
        a.poll();
    }
    assert!(a.drain_events().len() <= MAX_QUEUED_EVENTS);
}

// Authorizer and app-level validation hook -----------------------------------------------------

/// Denies one author from writing a specific key, and refuses any payload over 64 bytes.
struct Rules;

impl OpPolicy for Rules {
    fn check(&self, author: ActorId, op: &UirNode) -> Result<(), ApplyError> {
        if author == ActorId(1) && op.bytes == set("secret", "x").bytes {
            return Err(ApplyError::Denied);
        }
        if op.bytes.len() > 64 {
            return Err(ApplyError::Invalid("payload over 64 bytes"));
        }
        Ok(())
    }
}

#[test]
fn policy_quarantines_remote_ops_and_refuses_local_ones() {
    let clock = ManualClock::new(1_000);
    let mut b = replica(2, &clock).with_policy(Box::new(Rules));
    let denied = forge(1, 1, 1_000, VectorClock::new(), &set("secret", "x").bytes);
    let too_big = forge(1, 2, 1_001, VectorClock::new(), &[b'a'; 100]);
    let fine = forge(1, 3, 1_002, VectorClock::new(), &lww_set("x", "1"));
    let report = b.import_ops(vec![denied, too_big, fine]).unwrap();
    assert_eq!(
        report.applied, 1,
        "the later op from the same author still applies"
    );
    assert_eq!(report.rejected.len(), 2);
    assert_eq!(report.rejected[0].error, ApplyError::Denied);
    assert_eq!(
        report.rejected[1].error,
        ApplyError::Invalid("payload over 64 bytes")
    );
    assert_eq!(b.state().get(b"secret"), None);

    let mut a = replica(1, &clock).with_policy(Box::new(Rules));
    assert_eq!(
        a.local_op(set("secret", "x")),
        Err(EngineError::Apply(ApplyError::Denied))
    );
    assert!(a.log().is_empty(), "a refused local op is never logged");
}

#[test]
fn stats_reports_peer_health_and_pending_depth() {
    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock);
    let mut b = replica(2, &clock);
    assert_eq!(a.stats().peers, 0);
    link(&mut a, &mut b, 8, Faults::default());
    settle(&mut [&mut a, &mut b], 4);
    a.local_op(set("k", "v")).unwrap();
    let stats = a.stats();
    assert_eq!(stats.log_len, 1);
    assert_eq!(stats.peers, 1);
    assert_eq!(stats.peers_up_to_date, 1);
    assert_eq!(stats.pending, 0);

    // An operation from a gap in the sequence waits in the pending buffer.
    b.import_ops(vec![forge(
        3,
        2,
        1_000,
        VectorClock::new(),
        &lww_set("x", "1"),
    )])
    .unwrap();
    assert_eq!(b.stats().pending, 1);
}

#[test]
fn event_observer_sees_every_event_and_the_queue_still_works() {
    use std::cell::RefCell;
    use std::rc::Rc;

    let clock = ManualClock::new(1_000);
    let seen = Rc::new(RefCell::new(Vec::new()));
    let sink = seen.clone();
    let mut a = replica(1, &clock).with_event_observer(Box::new(move |event: &Event| {
        sink.borrow_mut().push(event.clone());
    }));
    let mut b = replica(2, &clock);
    link(&mut a, &mut b, 9, Faults::default());
    settle(&mut [&mut a, &mut b], 4);

    assert!(seen.borrow().contains(&Event::PeerConnected(b.actor())));
    let queued = a.drain_events();
    assert!(queued.contains(&Event::PeerConnected(b.actor())));
    assert_eq!(queued.len(), seen.borrow().len());
}
