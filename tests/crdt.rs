//! PnCounter and OrSet: convergence under concurrency, idempotent re-delivery, and snapshots.

#![cfg(feature = "crdt")]

mod common;

use common::*;
use tpt_replicate::clock::{ManualClock, Timestamp};
use tpt_replicate::crdt::{OrSet, PnCounter};
use tpt_replicate::stubs::{ActorId, CrdtCompatible, State};
use tpt_replicate::transport::Faults;
use tpt_replicate::{Replica, ReplicaConfig};

fn counter(actor: u64, clock: &ManualClock) -> Replica<PnCounter> {
    Replica::builder(ActorId(actor), PnCounter::default())
        .shared_secret(SECRET)
        .clock(Box::new(clock.clone()))
        .build()
        .expect("replica")
}

fn set_replica(actor: u64, clock: &ManualClock) -> Replica<OrSet> {
    Replica::builder(ActorId(actor), OrSet::default())
        .shared_secret(SECRET)
        .clock(Box::new(clock.clone()))
        .build()
        .expect("replica")
}

fn settle_all<S: State>(replicas: &mut [&mut Replica<S>], rounds: usize, clock: &ManualClock) {
    for _ in 0..rounds {
        for replica in replicas.iter_mut() {
            replica.poll();
        }
        clock.advance(10);
    }
    // One anti-entropy period, so operations that were not relayed live still arrive.
    clock.advance(10_000);
    for _ in 0..4 {
        for replica in replicas.iter_mut() {
            replica.poll();
        }
    }
}

#[test]
fn counter_converges_with_increments_and_decrements_on_each_side() {
    let clock = ManualClock::new(1_000);
    let (mut a, mut b, mut c) = (counter(1, &clock), counter(2, &clock), counter(3, &clock));
    let (x, y) = (
        tpt_replicate::transport::MemoryTransport::pair(1, Faults::default()),
        tpt_replicate::transport::MemoryTransport::pair(2, Faults::default()),
    );
    a.add_peer(b.actor(), Box::new(x.0));
    b.add_peer(a.actor(), Box::new(x.1));
    b.add_peer(c.actor(), Box::new(y.0));
    c.add_peer(b.actor(), Box::new(y.1));

    a.local_op(PnCounter::add(5)).unwrap();
    clock.advance(1);
    b.local_op(PnCounter::add(-2)).unwrap();
    clock.advance(1);
    c.local_op(PnCounter::add(10)).unwrap();
    clock.advance(1);
    c.local_op(PnCounter::add(-1)).unwrap();
    settle_all(&mut [&mut a, &mut b, &mut c], 6, &clock);

    assert_eq!(a.state().value(), 12);
    assert_eq!(b.state().value(), 12);
    assert_eq!(c.state().value(), 12);
    assert_eq!(a.state().state_hash(), c.state().state_hash());
}

#[test]
fn counter_ignores_a_redelivered_operation() {
    let clock = ManualClock::new(1_000);
    let mut a = counter(1, &clock);
    a.local_op(PnCounter::add(7)).unwrap();
    let op = a.log().ops()[0].clone();
    // Applying the same stamped operation again must not count it twice.
    let mut state = a.state().clone();
    state.apply(&op.timestamp, &op.op).unwrap();
    assert_eq!(state.value(), 7);
    // The engine's own dedupe agrees.
    let mut b = counter(2, &clock);
    b.import_ops(vec![op.clone()]).unwrap();
    let report = b.import_ops(vec![op]).unwrap();
    assert_eq!(report.duplicates, 1);
    assert_eq!(b.state().value(), 7);
}

#[test]
fn counter_snapshot_round_trips() {
    let clock = ManualClock::new(1_000);
    let mut a = counter(1, &clock);
    a.local_op(PnCounter::add(3)).unwrap();
    a.local_op(PnCounter::add(-8)).unwrap();
    let bytes = a.state().encode_state();
    let decoded = PnCounter::decode_state(&bytes).unwrap();
    assert_eq!(decoded.value(), -5);
    assert_eq!(decoded.state_hash(), a.state().state_hash());
    assert!(PnCounter::decode_state(&bytes[..bytes.len() - 1]).is_err());
}

#[test]
fn add_wins_over_a_concurrent_remove() {
    let clock = ManualClock::new(1_000);
    let (mut a, mut b) = (set_replica(1, &clock), set_replica(2, &clock));
    let link = (tpt_replicate::transport::MemoryTransport::pair(
        3,
        Faults::default(),
    ),);
    let (ta, tb) = link.0;
    let control = ta.clone();
    a.add_peer(b.actor(), Box::new(ta));
    b.add_peer(a.actor(), Box::new(tb));

    a.local_op(OrSet::add(b"milk")).unwrap();
    settle_all(&mut [&mut a, &mut b], 4, &clock);
    assert!(b.state().contains(b"milk"));

    // Cut the link. b removes what it saw. a adds milk again, which is a new tag.
    control.set_up(false);
    let remove = OrSet::remove(b"milk", b.state());
    clock.advance(1);
    b.local_op(remove).unwrap();
    clock.advance(1);
    a.local_op(OrSet::add(b"milk")).unwrap();
    control.set_up(true);
    settle_all(&mut [&mut a, &mut b], 6, &clock);

    assert!(a.state().contains(b"milk"));
    assert!(b.state().contains(b"milk"));
    assert_eq!(a.state().state_hash(), b.state().state_hash());
}

#[test]
fn remove_of_seen_add_takes_effect() {
    let clock = ManualClock::new(1_000);
    let mut a = set_replica(1, &clock);
    a.local_op(OrSet::add(b"x")).unwrap();
    clock.advance(1);
    let remove = OrSet::remove(b"x", a.state());
    a.local_op(remove).unwrap();
    assert!(!a.state().contains(b"x"));
    assert!(a.state().iter().next().is_none());
}

#[test]
fn set_snapshot_round_trips_and_rejects_garbage() {
    let clock = ManualClock::new(1_000);
    let mut a = set_replica(1, &clock);
    a.local_op(OrSet::add(b"one")).unwrap();
    clock.advance(1);
    a.local_op(OrSet::add(b"two")).unwrap();
    let bytes = a.state().encode_state();
    let decoded = OrSet::decode_state(&bytes).unwrap();
    assert_eq!(decoded.state_hash(), a.state().state_hash());
    assert!(decoded.contains(b"two"));
    assert!(OrSet::decode_state(&[9, 9, 9]).is_err());
    let _ = (
        Timestamp {
            wall_time: 0,
            logical_counter: 0,
            actor_id: ActorId(0),
        },
        ReplicaConfig::default(),
    );
}

#[test]
fn malformed_operations_are_refused() {
    let mut set = OrSet::default();
    let stamp = Timestamp {
        wall_time: 1,
        logical_counter: 0,
        actor_id: ActorId(1),
    };
    assert!(set
        .apply(&stamp, &tpt_replicate::stubs::UirNode { bytes: vec![] })
        .is_err());
    assert!(set
        .apply(
            &stamp,
            &tpt_replicate::stubs::UirNode {
                bytes: vec![1, 0, 0, 0, 9]
            }
        )
        .is_err());
    let mut counter = PnCounter::default();
    assert!(counter
        .apply(&stamp, &tpt_replicate::stubs::UirNode { bytes: vec![1, 2] })
        .is_err());
}
