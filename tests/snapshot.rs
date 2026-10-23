//! Snapshot transfer: a replica that is behind compaction installs the sender's state instead of
//! waiting for operations that no longer exist.

mod common;

use common::*;
use tpt_replicate::clock::{ManualClock, Timestamp};
use tpt_replicate::error::SnapshotError;
use tpt_replicate::stubs::lww::LwwMap;
use tpt_replicate::stubs::lww::LwwOp;
use tpt_replicate::stubs::{ActorId, CrdtCompatible, MemorySnapshotSink, State};
use tpt_replicate::transport::Faults;
use tpt_replicate::Replica;
use tpt_replicate::{Event, ReplicaConfig};

fn compacting() -> ReplicaConfig {
    ReplicaConfig {
        snapshot_every_ops: 1,
        ..ReplicaConfig::default()
    }
}

fn stamp(wall: u64) -> Timestamp {
    Timestamp {
        wall_time: wall,
        logical_counter: 0,
        actor_id: ActorId(1),
    }
}

#[test]
fn state_encoding_keeps_tombstones_and_rejects_garbage() {
    let mut map = LwwMap::new();
    map.apply(&stamp(10), &set("live", "yes")).unwrap();
    map.apply(&stamp(11), &set("gone", "x")).unwrap();
    map.apply(&stamp(12), &delete("gone")).unwrap();

    let decoded = LwwMap::decode_state(&map.encode_state()).unwrap();
    assert_eq!(decoded.state_hash(), map.state_hash());
    assert_eq!(decoded.get(b"live"), Some(&b"yes"[..]));
    // The tombstone survives, so a late write still cannot resurrect the key.
    assert_eq!(decoded.tombstones(), 1);
    assert_eq!(decoded, map);

    assert_eq!(
        LwwMap::decode_state(&[0xFF, 0xFF, 0xFF]),
        Err(SnapshotError::Malformed)
    );
    let mut trailing = map.encode_state();
    trailing.push(0);
    assert_eq!(
        LwwMap::decode_state(&trailing),
        Err(SnapshotError::Malformed)
    );
}

#[test]
fn late_joiner_installs_snapshot_instead_of_deltas() {
    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock)
        .with_config(compacting())
        .with_snapshot_sink(Box::new(MemorySnapshotSink::default()));
    let mut b = replica(2, &clock)
        .with_config(compacting())
        .with_snapshot_sink(Box::new(MemorySnapshotSink::default()));
    link(&mut a, &mut b, 3, Faults::default());
    for i in 0..10 {
        clock.advance(1);
        a.local_op(set(&format!("k{i}"), "v")).unwrap();
    }
    settle(&mut [&mut a, &mut b], 8);
    assert!(a.log().len() < 10, "compaction should have trimmed the log");

    // C arrives after the history it would need has been compacted away.
    let mut c = replica(3, &clock);
    link(&mut a, &mut c, 4, Faults::default());
    settle(&mut [&mut a, &mut b, &mut c], 8);
    assert_eq!(c.state().state_hash(), a.state().state_hash());
    let events = c.drain_events();
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::SnapshotInstalled { .. })));
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::FrameRejected { .. })));

    // Live traffic flows both ways afterwards.
    clock.advance(1);
    c.local_op(set("from-c", "hello")).unwrap();
    settle(&mut [&mut a, &mut b, &mut c], 8);
    // Remote operations are not relayed live. b learns about them from the next anti-entropy
    // exchange, which waits for the resync interval.
    clock.advance(10_000);
    settle(&mut [&mut a, &mut b, &mut c], 8);
    assert_eq!(a.state().get(b"from-c"), Some(&b"hello"[..]));
    assert_eq!(b.state().state_hash(), a.state().state_hash());
}

#[test]
fn joiner_keeps_its_own_unsynced_operation() {
    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock)
        .with_config(compacting())
        .with_snapshot_sink(Box::new(MemorySnapshotSink::default()));
    let mut b = replica(2, &clock)
        .with_config(compacting())
        .with_snapshot_sink(Box::new(MemorySnapshotSink::default()));
    link(&mut a, &mut b, 5, Faults::default());
    for i in 0..6 {
        clock.advance(1);
        a.local_op(set(&format!("k{i}"), "v")).unwrap();
    }
    settle(&mut [&mut a, &mut b], 8);
    assert!(a.log().len() < 6);

    // C writes while offline. The snapshot does not know about that write.
    let mut c = replica(3, &clock);
    clock.advance(1);
    c.local_op(set("offline", "mine")).unwrap();
    link(&mut a, &mut c, 6, Faults::default());
    settle(&mut [&mut a, &mut b, &mut c], 8);
    clock.advance(10_000);
    settle(&mut [&mut a, &mut b, &mut c], 8);

    assert_eq!(c.state().get(b"offline"), Some(&b"mine"[..]));
    assert_eq!(a.state().get(b"offline"), Some(&b"mine"[..]));
    assert_eq!(a.state().state_hash(), c.state().state_hash());
    assert_eq!(a.state().state_hash(), b.state().state_hash());
}

/// A snapshot-sending peer, scripted by hand: the test writes frames straight onto the link.
fn scripted_link(
    clock: &ManualClock,
) -> (Replica<LwwMap>, tpt_replicate::transport::MemoryTransport) {
    use tpt_replicate::transport::MemoryTransport;
    let mut c = replica(3, clock);
    let (ta, tb) = MemoryTransport::pair(9, Faults::default());
    c.add_peer(ActorId(1), Box::new(ta));
    c.poll();
    (c, tb)
}

fn sealed_frame(message: &tpt_replicate::sync::Message) -> Vec<u8> {
    use tpt_replicate::codec::{BinaryCodec, Codec};
    use tpt_replicate::stubs::crypto::KeyedHashSigner;
    use tpt_replicate::sync::Envelope;
    Envelope::seal(
        &KeyedHashSigner::new(ActorId(1), SECRET),
        ActorId(1),
        BinaryCodec.encode(message),
    )
    .encode()
}

#[test]
fn snapshot_with_wrong_hash_is_refused() {
    use tpt_replicate::sync::{Message, SnapshotHeader};
    use tpt_replicate::transport::Transport;
    let clock = ManualClock::new(1_000);
    let (mut c, mut tb) = scripted_link(&clock);

    let state = LwwMap::new().encode_state();
    let header = SnapshotHeader {
        state_len: state.len(),
        state_hash: 0xDEAD_BEEF,
        clock: Default::default(),
        compacted: Default::default(),
        floor: None,
    };
    tb.send(&sealed_frame(&Message::SnapshotBegin(Box::new(header))))
        .unwrap();
    tb.send(&sealed_frame(&Message::SnapshotChunk(state)))
        .unwrap();
    c.poll();
    assert!(c.drain_events().iter().any(|e| matches!(
        e,
        Event::FrameRejected {
            error: tpt_replicate::error::FrameError::BadSnapshot(SnapshotError::HashMismatch),
            ..
        }
    )));
    assert!(c.log().is_empty());
}

#[test]
fn chunk_without_a_header_is_refused() {
    use tpt_replicate::sync::Message;
    use tpt_replicate::transport::Transport;
    let clock = ManualClock::new(1_000);
    let (mut c, mut tb) = scripted_link(&clock);
    tb.send(&sealed_frame(&Message::SnapshotChunk(vec![1, 2, 3])))
        .unwrap();
    c.poll();
    assert!(c.drain_events().iter().any(|e| matches!(
        e,
        Event::FrameRejected {
            error: tpt_replicate::error::FrameError::BadSnapshot(SnapshotError::Malformed),
            ..
        }
    )));
}

#[test]
fn chunks_past_the_declared_length_are_refused() {
    use tpt_replicate::sync::{Message, SnapshotHeader};
    use tpt_replicate::transport::Transport;
    let clock = ManualClock::new(1_000);
    let (mut c, mut tb) = scripted_link(&clock);
    let header = SnapshotHeader {
        state_len: 4,
        state_hash: 0,
        clock: Default::default(),
        compacted: Default::default(),
        floor: None,
    };
    tb.send(&sealed_frame(&Message::SnapshotBegin(Box::new(header))))
        .unwrap();
    tb.send(&sealed_frame(&Message::SnapshotChunk(vec![0; 9])))
        .unwrap();
    c.poll();
    assert!(c.drain_events().iter().any(|e| matches!(
        e,
        Event::FrameRejected {
            error: tpt_replicate::error::FrameError::BadSnapshot(SnapshotError::Malformed),
            ..
        }
    )));
}

#[test]
fn large_state_arrives_in_chunks_and_installs() {
    // The retained state is larger than one delta budget, so the snapshot must be chunked.
    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock)
        .with_config(compacting())
        .with_snapshot_sink(Box::new(MemorySnapshotSink::default()));
    let mut b = replica(2, &clock)
        .with_config(compacting())
        .with_snapshot_sink(Box::new(MemorySnapshotSink::default()));
    link(&mut a, &mut b, 12, Faults::default());
    for i in 0..20u8 {
        clock.advance(1);
        let value = vec![i; 600 * 1024];
        a.local_op(
            LwwOp::Set {
                key: vec![i],
                value,
            }
            .to_node(),
        )
        .unwrap();
    }
    settle(&mut [&mut a, &mut b], 8);
    assert!(a.log().len() < 20, "compaction should have trimmed the log");
    assert!(
        a.state().encode_state().len() > 8 << 20,
        "state must exceed one chunk for this test to mean anything"
    );

    let mut c = replica(3, &clock);
    link(&mut a, &mut c, 13, Faults::default());
    settle(&mut [&mut a, &mut b, &mut c], 8);
    clock.advance(10_000);
    settle(&mut [&mut a, &mut b, &mut c], 8);

    assert_eq!(c.state().state_hash(), a.state().state_hash());
    let events = c.drain_events();
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::SnapshotInstalled { .. })));
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::FrameRejected { .. })));
}

#[test]
fn snapshot_covering_our_own_operation_does_not_reuse_its_sequence_number() {
    // Regression: installing a snapshot that covers this replica's own operation removed that
    // operation from the log, and the next local write reused its sequence number. Two different
    // operations then carried the same (author, seq), which peers read as equivocation.
    use tpt_replicate::sync::{Message, SnapshotHeader};
    use tpt_replicate::transport::{MemoryTransport, Transport};

    let clock = ManualClock::new(1_000);
    let mut c = replica(3, &clock);
    c.local_op(set("own", "1")).unwrap();
    let own = c.log().ops()[0].clone();
    assert_eq!(own.seq, 1);

    // A snapshot from peer 1 whose state holds our own write, and whose clock covers it.
    let mut peer_state = LwwMap::new();
    peer_state.apply(&own.timestamp, &own.op).unwrap();
    let state = peer_state.encode_state();
    let header = SnapshotHeader {
        state_len: state.len(),
        state_hash: peer_state.state_hash(),
        clock: {
            let mut clock = tpt_replicate::clock::VectorClock::new();
            clock.observe(ActorId(3), 1);
            clock.observe(ActorId(1), 4);
            clock
        },
        compacted: Default::default(),
        floor: None,
    };
    let (ta, mut tb) = MemoryTransport::pair(21, Faults::default());
    c.add_peer(ActorId(1), Box::new(ta));
    c.poll();
    tb.send(&sealed_frame(&Message::SnapshotBegin(Box::new(header))))
        .unwrap();
    tb.send(&sealed_frame(&Message::SnapshotChunk(state)))
        .unwrap();
    c.poll();
    assert!(c
        .drain_events()
        .iter()
        .any(|e| matches!(e, Event::SnapshotInstalled { .. })));

    let next = c.local_op(set("after", "install")).unwrap();
    assert_eq!(next.seq, 2, "the sequence number must not go back");
}
