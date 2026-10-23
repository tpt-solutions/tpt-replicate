//! Property tests for deterministic convergence. Five virtual peers generate random operations and
//! gossip. Every observer then receives the full history in a shuffled order, and must reach the
//! same state as the ideal, timestamp-ordered application.

mod common;

use std::collections::BTreeMap;

use common::*;
use proptest::prelude::*;
use tpt_replicate::clock::ManualClock;
use tpt_replicate::log::StampedOperation;
use tpt_replicate::stubs::lww::LwwMap;
use tpt_replicate::stubs::{ActorId, CrdtCompatible, State};
use tpt_replicate::sync::calculate_delta;

/// Deterministic xorshift generator, so every failing seed reproduces.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]

    #[test]
    fn shuffled_delivery_converges_to_ideal_state(seed in any::<u64>()) {
        let ops = 1_000;
        let clock = ManualClock::new(1_000);
        let mut replicas: Vec<_> = (1..=5).map(|i| replica(i, &clock)).collect();
        let mut rng = Rng(seed | 1);

        for _ in 0..ops {
            clock.advance(rng.below(4));
            let i = rng.below(5) as usize;
            if rng.below(5) == 0 {
                let j = rng.below(5) as usize;
                if i != j {
                    let delta = calculate_delta(replicas[j].log(), replicas[i].log().clock());
                    replicas[i].import_ops(delta).expect("gossip import");
                }
            } else {
                let key = format!("k{}", rng.below(16));
                let node = if rng.below(4) == 0 {
                    delete(&key)
                } else {
                    set(&key, &format!("v{}", rng.below(1_000)))
                };
                replicas[i].local_op(node).expect("local op");
            }
        }

        // The union of every log is the complete causal history.
        let mut union: BTreeMap<(ActorId, u64), StampedOperation> = BTreeMap::new();
        for replica in &replicas {
            for op in replica.log().ops() {
                union.insert((op.actor, op.seq), op.clone());
            }
        }
        let mut ordered: Vec<StampedOperation> = union.values().cloned().collect();
        ordered.sort_by_key(|op| op.timestamp);

        let mut ideal = LwwMap::new();
        for op in &ordered {
            ideal.apply(&op.timestamp, &op.op).expect("ideal apply");
        }

        for observer_index in 0..5u64 {
            let mut shuffle = Rng(seed.wrapping_add(observer_index * 7_919) | 1);
            let mut shuffled = ordered.clone();
            for k in (1..shuffled.len()).rev() {
                let j = shuffle.below(k as u64 + 1) as usize;
                shuffled.swap(k, j);
            }

            let mut observer = replica(100 + observer_index, &clock);
            let mut rest = shuffled;
            while !rest.is_empty() {
                let take = (1 + shuffle.below(16) as usize).min(rest.len());
                let chunk: Vec<_> = rest.drain(..take).collect();
                observer.import_ops(chunk).expect("import");
            }

            prop_assert_eq!(observer.log().len(), ordered.len());
            prop_assert_eq!(observer.state().state_hash(), ideal.state_hash());
        }
    }
}
