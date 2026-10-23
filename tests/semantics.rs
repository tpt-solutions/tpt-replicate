//! Scenario tests for the replica: handshake, live broadcast, LWW conflicts, deletes under
//! partition, malformed and hostile batches, compaction, and back-pressure.

mod common;

use common::*;
use tpt_replicate::clock::{ManualClock, VectorClock};
use tpt_replicate::error::{ApplyError, FrameError, MergeError};
use tpt_replicate::stubs::lww::LwwMap;
use tpt_replicate::stubs::{MemorySnapshotSink, State};
use tpt_replicate::sync::calculate_delta;
use tpt_replicate::transport::Faults;
use tpt_replicate::{ActorId, Event, PeerState, ReplicaConfig};

fn empty_hash() -> u64 {
    LwwMap::new().state_hash()
}

#[test]
fn handshake_transfers_full_history() {
    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock);
    let mut b = replica(2, &clock);
    for i in 0..20 {
        a.local_op(set(&format!("k{i}"), "v")).unwrap();
    }
    link(&mut a, &mut b, 7, Faults::default());
    settle(&mut [&mut a, &mut b], 6);
    assert_eq!(b.log().len(), 20);
    assert_eq!(a.state().state_hash(), b.state().state_hash());
    assert_eq!(a.peer_state(b.actor()), Some(PeerState::UpToDate));
    assert_eq!(b.peer_state(a.actor()), Some(PeerState::UpToDate));
}

#[test]
fn live_operations_reach_up_to_date_peer() {
    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock);
    let mut b = replica(2, &clock);
    link(&mut a, &mut b, 1, Faults::default());
    settle(&mut [&mut a, &mut b], 4);
    a.local_op(set("x", "1")).unwrap();
    assert_eq!(a.pending_for(b.actor()), Some(1));
    b.poll();
    assert_eq!(b.state().get(b"x"), Some(&b"1"[..]));
    // B acknowledges the growth, so A's queue depth drops to zero.
    a.poll();
    assert_eq!(a.pending_for(b.actor()), Some(0));
}

#[test]
fn concurrent_same_key_writes_resolve_by_timestamp() {
    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock);
    let mut b = replica(2, &clock);
    link(&mut a, &mut b, 2, Faults::default());
    settle(&mut [&mut a, &mut b], 4);

    // Same wall millisecond and counter: the larger actor id wins.
    a.local_op(set("cell", "from-a")).unwrap();
    b.local_op(set("cell", "from-b")).unwrap();
    settle(&mut [&mut a, &mut b], 4);
    assert_eq!(a.state().get(b"cell"), Some(&b"from-b"[..]));
    assert_eq!(b.state().get(b"cell"), Some(&b"from-b"[..]));

    // A later write beats any earlier one, whatever the actor.
    clock.advance(1);
    a.local_op(set("cell", "later")).unwrap();
    settle(&mut [&mut a, &mut b], 4);
    assert_eq!(b.state().get(b"cell"), Some(&b"later"[..]));
    assert_eq!(a.state().state_hash(), b.state().state_hash());
}

#[test]
fn delete_is_not_resurrected_by_offline_peer() {
    let clock = ManualClock::new(100);
    let mut a = replica(1, &clock);
    let mut b = replica(2, &clock);
    let mut c = replica(3, &clock);
    link(&mut a, &mut b, 1, Faults::default());
    let a_c = link(&mut a, &mut c, 2, Faults::default());
    let b_c = link(&mut b, &mut c, 3, Faults::default());
    settle(&mut [&mut a, &mut b, &mut c], 4);

    a.local_op(set("k", "v0")).unwrap();
    settle(&mut [&mut a, &mut b, &mut c], 4);

    // C goes offline and writes at t=110. Its write is not yet known to anyone.
    a_c.set_up(false);
    b_c.set_up(false);
    clock.set(110);
    c.local_op(set("k", "offline")).unwrap();

    // A and B delete at t=200, after C's write.
    clock.set(200);
    b.local_op(delete("k")).unwrap();
    settle(&mut [&mut a, &mut b, &mut c], 4);
    assert_eq!(a.state().get(b"k"), None);

    a_c.set_up(true);
    b_c.set_up(true);
    settle(&mut [&mut a, &mut b, &mut c], 6);
    assert_eq!(a.state().get(b"k"), None);
    assert_eq!(b.state().get(b"k"), None);
    assert_eq!(c.state().get(b"k"), None);
    assert_eq!(a.state().state_hash(), c.state().state_hash());
}

#[test]
fn partition_heals_without_data_loss() {
    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock);
    let mut b = replica(2, &clock);
    let mut c = replica(3, &clock);
    let a_b = link(&mut a, &mut b, 1, Faults::default());
    let a_c = link(&mut a, &mut c, 2, Faults::default());
    link(&mut b, &mut c, 3, Faults::default());
    settle(&mut [&mut a, &mut b, &mut c], 4);

    a_b.set_up(false);
    a_c.set_up(false);
    for i in 0..5 {
        a.local_op(set(&format!("a{i}"), "x")).unwrap();
        b.local_op(set(&format!("b{i}"), "y")).unwrap();
        c.local_op(set(&format!("c{i}"), "z")).unwrap();
        clock.advance(1);
        settle(&mut [&mut a, &mut b, &mut c], 1);
    }
    assert_eq!(a.log().len(), 5);
    assert_eq!(b.log().len(), 10);

    a_b.set_up(true);
    a_c.set_up(true);
    settle(&mut [&mut a, &mut b, &mut c], 8);
    assert_eq!(a.log().len(), 15);
    assert_eq!(b.log().len(), 15);
    assert_eq!(c.log().len(), 15);
    assert_eq!(a.state().state_hash(), b.state().state_hash());
    assert_eq!(b.state().state_hash(), c.state().state_hash());
}

#[test]
fn lossy_duplicating_reordering_link_still_converges() {
    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock);
    let mut b = replica(2, &clock);
    let mut c = replica(3, &clock);
    let faults = Faults {
        loss_permille: 200,
        duplicate_permille: 200,
        reorder: true,
    };
    link(&mut a, &mut b, 11, faults);
    link(&mut a, &mut c, 12, faults);
    link(&mut b, &mut c, 13, faults);

    for round in 0..120 {
        clock.advance(1_000);
        if round < 10 {
            a.local_op(set(&format!("a{round}"), "x")).unwrap();
            b.local_op(set(&format!("b{round}"), "y")).unwrap();
            c.local_op(set(&format!("c{round}"), "z")).unwrap();
        }
        settle(&mut [&mut a, &mut b, &mut c], 1);
    }
    assert_eq!(a.log().len(), 30);
    assert_eq!(b.log().len(), 30);
    assert_eq!(c.log().len(), 30);
    assert_eq!(a.state().state_hash(), c.state().state_hash());
    assert_eq!(b.state().state_hash(), c.state().state_hash());
}

#[test]
fn delta_contains_only_what_peer_lacks() {
    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock);
    let mut b = replica(2, &clock);
    for i in 0..10 {
        a.local_op(set(&format!("k{i}"), "v")).unwrap();
    }
    b.import_ops(a.log().ops()[..6].to_vec()).unwrap();
    let delta = calculate_delta(a.log(), b.log().clock());
    assert_eq!(delta.len(), 4);
    assert_eq!(delta.first().map(|op| op.seq), Some(7));
}

#[test]
fn duplicate_and_reversed_batches_are_idempotent() {
    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock);
    let mut b = replica(2, &clock);
    for i in 0..5 {
        a.local_op(set(&format!("k{i}"), "v")).unwrap();
    }
    let reversed: Vec<_> = a.log().ops().iter().rev().cloned().collect();
    let report = b.import_ops(reversed.clone()).unwrap();
    assert_eq!(report.applied, 5);
    let again = b.import_ops(reversed).unwrap();
    assert_eq!(again.duplicates, 5);
    assert_eq!(again.applied, 0);
    assert_eq!(b.log().len(), 5);
    assert_eq!(a.state().state_hash(), b.state().state_hash());
}

#[test]
fn tampered_operation_rejects_whole_batch() {
    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock);
    let mut b = replica(2, &clock);
    a.local_op(set("x", "1")).unwrap();
    a.local_op(set("y", "2")).unwrap();
    let mut ops = a.log().ops().to_vec();
    ops[1].op.bytes[0] ^= 0xff;
    let err = b.import_ops(ops).unwrap_err();
    assert_eq!(
        err,
        FrameError::Merge(MergeError::BadSignature {
            actor: ActorId(1),
            seq: 2
        })
    );
    assert!(b.log().is_empty());
    assert_eq!(b.state().state_hash(), empty_hash());
}

#[test]
fn far_future_timestamp_is_rejected() {
    let clock = ManualClock::new(1_000);
    let mut b = replica(2, &clock);
    let far = 1_000 + tpt_replicate::clock::DEFAULT_MAX_DRIFT_MS + 1;
    let op = forge(1, 1, far, VectorClock::new(), &[0]);
    let err = b.import_ops(vec![op]).unwrap_err();
    assert_eq!(
        err,
        FrameError::Merge(MergeError::BadTimestamp {
            actor: ActorId(1),
            seq: 1
        })
    );
    assert!(b.log().is_empty());
}

#[test]
fn state_refused_op_is_quarantined_without_blocking_its_batch() {
    let clock = ManualClock::new(1_000);
    let mut b = replica(2, &clock);
    let good = forge(
        1,
        1,
        1_000,
        VectorClock::new(),
        set("x", "1").bytes.as_slice(),
    );
    let bad = forge(1, 2, 1_001, VectorClock::new(), &[0xEE]);
    let report = b.import_ops(vec![good, bad]).unwrap();
    assert_eq!(report.applied, 1);
    assert_eq!(report.rejected.len(), 1);
    assert_eq!(report.rejected[0].actor, ActorId(1));
    assert_eq!(report.rejected[0].seq, 2);
    assert_eq!(report.rejected[0].error, ApplyError::UnknownOpTag(0xEE));

    // The refused op takes its sequence number in the log, so later operations from the same author
    // are not stuck behind it. Its effect is not in the state.
    assert_eq!(b.log().len(), 2);
    assert_eq!(b.state().get(b"x"), Some(&b"1"[..]));
    assert!(b.drain_events().contains(&Event::OpRejected {
        actor: ActorId(1),
        seq: 2,
        error: ApplyError::UnknownOpTag(0xEE),
    }));
    assert_ne!(b.state().state_hash(), empty_hash());
}

#[test]
fn conflicting_operations_for_same_slot_are_rejected() {
    let clock = ManualClock::new(1_000);
    let mut b = replica(2, &clock);
    let mut deps = VectorClock::new();
    deps.observe(ActorId(1), 1);
    let first = forge(1, 2, 1_001, deps.clone(), &set("x", "A").bytes);
    let second = forge(1, 2, 1_001, deps, &set("x", "B").bytes);
    let err = b.import_ops(vec![first, second]).unwrap_err();
    assert_eq!(
        err,
        FrameError::Merge(MergeError::Conflict {
            actor: ActorId(1),
            seq: 2
        })
    );
}

#[test]
fn self_dependency_on_future_operation_is_malformed() {
    let clock = ManualClock::new(1_000);
    let mut b = replica(2, &clock);
    let mut deps = VectorClock::new();
    deps.observe(ActorId(1), 5);
    let op = forge(1, 2, 1_000, deps, &[0]);
    assert!(matches!(
        b.import_ops(vec![op]),
        Err(FrameError::Merge(MergeError::Malformed {
            actor: ActorId(1),
            seq: 2,
            ..
        }))
    ));
}

#[test]
fn pending_buffer_is_bounded() {
    let clock = ManualClock::new(1_000);
    let config = ReplicaConfig {
        max_pending: 2,
        ..ReplicaConfig::default()
    };
    let mut b = replica(2, &clock).with_config(config);
    let ops: Vec<_> = (2..=4)
        .map(|seq| forge(1, seq, 1_000, VectorClock::new(), &[0]))
        .collect();
    let err = b.import_ops(ops).unwrap_err();
    assert_eq!(
        err,
        FrameError::Merge(MergeError::PendingOverflow { limit: 2 })
    );
    assert!(b.log().is_empty());
}

#[test]
fn compaction_keeps_state_and_convergence() {
    let clock = ManualClock::new(1_000);
    let config = ReplicaConfig {
        snapshot_every_ops: 1,
        ..ReplicaConfig::default()
    };
    let mut a = replica(1, &clock)
        .with_config(config.clone())
        .with_snapshot_sink(Box::new(MemorySnapshotSink::default()));
    let mut b = replica(2, &clock)
        .with_config(config)
        .with_snapshot_sink(Box::new(MemorySnapshotSink::default()));
    link(&mut a, &mut b, 5, Faults::default());
    settle(&mut [&mut a, &mut b], 4);

    for i in 0..10 {
        clock.advance(1);
        a.local_op(set(&format!("k{i}"), "v")).unwrap();
        b.local_op(delete(&format!("k{}", i / 2))).unwrap();
        settle(&mut [&mut a, &mut b], 2);
    }
    settle(&mut [&mut a, &mut b], 4);
    let before = a.state().state_hash();
    let events: Vec<Event> = a
        .drain_events()
        .into_iter()
        .chain(b.drain_events())
        .collect();
    assert!(events.iter().any(|e| matches!(e, Event::Compacted { .. })));
    assert!(
        a.log().len() < 10,
        "log should shrink, has {}",
        a.log().len()
    );
    assert_eq!(a.state().state_hash(), before);
    assert_eq!(a.state().state_hash(), b.state().state_hash());

    clock.advance(1);
    b.local_op(set("after", "compaction")).unwrap();
    settle(&mut [&mut a, &mut b], 4);
    assert_eq!(a.state().get(b"after"), Some(&b"compaction"[..]));
    assert_eq!(a.state().state_hash(), b.state().state_hash());
}

#[test]
fn silent_peer_is_declared_dead_and_recovers() {
    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock);
    let mut b = replica(2, &clock);
    let a_b = link(&mut a, &mut b, 9, Faults::default());
    settle(&mut [&mut a, &mut b], 3);
    a_b.set_up(false);
    settle(&mut [&mut a], 1);
    assert_eq!(a.peer_state(b.actor()), Some(PeerState::Disconnected));
    a_b.set_up(true);
    settle(&mut [&mut a, &mut b], 4);
    assert_eq!(a.peer_state(b.actor()), Some(PeerState::UpToDate));
    let events: Vec<Event> = a.drain_events();
    assert!(events.contains(&Event::PeerDisconnected(b.actor())));
}
