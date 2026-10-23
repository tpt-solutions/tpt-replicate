//! Snapshots committed to a tpt-version repository, behind the `version` feature.

#![cfg(feature = "version")]

mod common;

use common::*;
use tpt_replicate::clock::{ManualClock, Timestamp};
use tpt_replicate::stubs::lww::LwwMap;
use tpt_replicate::stubs::{ActorId, CrdtCompatible, Snapshot, SnapshotSink, State};
use tpt_replicate::version::VersionSink;
use tpt_version::{ActorId as CommitAuthor, Ed25519Signer, LogicalClock, MemoryStore};

fn author() -> CommitAuthor {
    CommitAuthor::from_bytes([9; 16])
}

fn sink(store: MemoryStore) -> VersionSink<MemoryStore, LogicalClock> {
    VersionSink::open(store, LogicalClock::starting_at(1), "main", author()).expect("open")
}

fn snapshot_of(map: &LwwMap, wall: u64) -> Snapshot {
    Snapshot {
        cutoff: Timestamp {
            wall_time: wall,
            logical_counter: 2,
            actor_id: ActorId(4),
        },
        state_hash: map.state_hash(),
        op_count: 5,
    }
}

#[test]
fn a_persisted_snapshot_reads_back_exactly() {
    let mut map = LwwMap::new();
    map.apply(
        &Timestamp {
            wall_time: 10,
            logical_counter: 0,
            actor_id: ActorId(1),
        },
        &set("k", "v"),
    )
    .unwrap();
    let mut versions = sink(MemoryStore::default());
    assert!(versions.latest().unwrap().is_none());

    let state = map.encode_state();
    versions.persist(&snapshot_of(&map, 10), &state).unwrap();
    let record = versions.latest().unwrap().expect("a record");
    assert_eq!(record.cutoff, snapshot_of(&map, 10).cutoff);
    assert_eq!(record.state_hash, map.state_hash());
    assert_eq!(record.op_count, 5);
    assert_eq!(record.state, state);
    assert_eq!(LwwMap::decode_state(&record.state).unwrap(), map);
}

#[test]
fn each_snapshot_is_a_commit_and_the_latest_one_wins() {
    let map = LwwMap::new();
    let mut versions = sink(MemoryStore::default());
    versions
        .persist(&snapshot_of(&map, 10), &map.encode_state())
        .unwrap();
    versions
        .persist(&snapshot_of(&map, 20), &map.encode_state())
        .unwrap();

    let history = versions.repository().log("main").unwrap();
    assert_eq!(history.len(), 2);
    assert!(history[0].message.contains("cutoff 20."));
    assert_eq!(versions.latest().unwrap().unwrap().cutoff.wall_time, 20);
}

#[test]
fn a_fresh_open_on_the_same_store_sees_the_snapshot() {
    // Restart: the store is what persists. Reopening it on the same data finds the snapshot.
    let map = LwwMap::new();
    let mut first = sink(MemoryStore::default());
    first
        .persist(&snapshot_of(&map, 30), &map.encode_state())
        .unwrap();
    // The store is the durable part. Keep a copy of it, then drop the sink.
    let store = first.repository().store().clone();
    drop(first);
    let reopened = sink(store);
    assert_eq!(reopened.latest().unwrap().unwrap().cutoff.wall_time, 30);
}

#[test]
fn signed_commits_carry_a_signature() {
    let map = LwwMap::new();
    let mut versions =
        sink(MemoryStore::default()).with_signer(Box::new(Ed25519Signer::from_seed([3; 32])));
    versions
        .persist(&snapshot_of(&map, 40), &map.encode_state())
        .unwrap();
    let head = versions.repository().log("main").unwrap();
    assert!(head[0].signature.is_some());
}

#[test]
fn compaction_through_a_replica_commits_snapshots() {
    use tpt_replicate::{Event, ReplicaConfig};
    let clock = ManualClock::new(1_000);
    let config = ReplicaConfig {
        snapshot_every_ops: 1,
        ..ReplicaConfig::default()
    };
    let mut a = replica(1, &clock)
        .with_config(config.clone())
        .with_snapshot_sink(Box::new(sink(MemoryStore::default())));
    let mut b = replica(2, &clock).with_config(config);
    link(
        &mut a,
        &mut b,
        31,
        tpt_replicate::transport::Faults::default(),
    );
    for i in 0..4 {
        clock.advance(1);
        a.local_op(set(&format!("k{i}"), "v")).unwrap();
    }
    settle(&mut [&mut a, &mut b], 8);
    let events = a.drain_events();
    assert!(events.iter().any(|e| matches!(e, Event::Compacted { .. })));
    assert!(!events.iter().any(|e| matches!(e, Event::SnapshotFailed(_))));
}
