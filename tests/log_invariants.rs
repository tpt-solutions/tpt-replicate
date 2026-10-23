//! Log invariants: the incrementally maintained clock matches the retained entries, watermarks
//! track the latest timestamp per actor, and compaction only removes what it is told to.

mod common;

use common::*;
use tpt_replicate::clock::{ManualClock, VectorClock};
use tpt_replicate::{ActorId, ReplicatedLog};

#[test]
fn derived_clock_matches_incremental_clock_through_out_of_order_inserts_and_compaction() {
    let clock = ManualClock::new(1_000);
    let mut author = replica(1, &clock);
    let mut other = replica(2, &clock);
    for i in 0..6 {
        clock.advance(1);
        author.local_op(set(&format!("a{i}"), "x")).unwrap();
        other.local_op(set(&format!("b{i}"), "y")).unwrap();
    }

    let mut log = ReplicatedLog::new();
    // Insert both authors' operations interleaved in reverse, to exercise sorted insertion.
    let mut all: Vec<_> = author
        .log()
        .ops()
        .iter()
        .chain(other.log().ops())
        .cloned()
        .collect();
    all.sort_by_key(|op| std::cmp::Reverse(op.timestamp));
    for op in all {
        log.insert(op);
    }
    assert_eq!(log.len(), 12);
    assert_eq!(&log.derive_clock(), log.clock());
    assert!(log
        .ops()
        .windows(2)
        .all(|pair| pair[0].timestamp <= pair[1].timestamp));
    assert_eq!(
        log.watermarks()[&ActorId(1)],
        author.log().ops().last().unwrap().timestamp
    );

    let cutoff = log.ops()[5].timestamp;
    let removed = log.compact(&cutoff);
    assert_eq!(removed, 6);
    assert_eq!(&log.derive_clock(), log.clock());
    assert!(log.compacted().peers.values().all(|seq| *seq > 0));
    assert!(log.can_serve(log.clock()));
    assert!(!log.can_serve(&VectorClock::new()));
}
