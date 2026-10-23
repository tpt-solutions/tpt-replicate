//! Deterministic simulation for convergence testing.
//!
//! [`run`] builds a fully connected cluster of replicas, then plays a random script of local writes,
//! link partitions, clock advances and polls, all drawn from one seed. The links can lose,
//! duplicate and reorder frames. After the script it heals every link, lets anti-entropy run, and
//! reports whether all replicas agree.
//!
//! The same seed always gives the same run, so a failing seed can be replayed exactly.

use alloc::vec::Vec;

use crate::clock::ManualClock;
use crate::engine::{Event, ReplicaConfig};
use crate::stubs::crypto::{KeyedHashSigner, KeyedHashVerifier};
use crate::stubs::lww::{LwwMap, LwwOp};
use crate::stubs::{ActorId, MemorySnapshotSink, State};
use crate::transport::{Faults, MemoryTransport};
use crate::Replica;

const SIM_SECRET: u64 = 0x51_4D;

/// What to simulate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimConfig {
    /// Number of replicas. Values below 2 are raised to 2.
    pub replicas: usize,
    /// Number of random steps before the heal phase.
    pub steps: usize,
    /// Seeds every random choice.
    pub seed: u64,
    /// Loss, duplication and reordering on every link.
    pub faults: Faults,
    /// Whether replicas compact, with an in-memory snapshot sink. Compaction makes late peers
    /// install snapshots rather than replaying the log.
    pub compaction: bool,
    /// Connects the last replica only halfway through the script. It then has to catch up, and
    /// with compaction on, that means installing a snapshot.
    pub late_join: bool,
}

/// The outcome of one run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimReport {
    /// Whether every replica holds the same state after healing.
    pub converged: bool,
    /// Each replica's state hash after healing, in replica order.
    pub state_hashes: Vec<u64>,
    /// Local writes issued during the script.
    pub local_writes: usize,
    /// Link partitions started during the script.
    pub partitions: usize,
    /// Frames the engine rejected, with the reason for each.
    pub rejections: Vec<crate::error::FrameError>,
    /// Snapshots installed across all replicas.
    pub snapshots_installed: usize,
}

/// A small deterministic generator. Not for anything but the simulation.
struct Script(u64);

impl Script {
    fn next(&mut self) -> u64 {
        // SplitMix64. Good enough for choosing actions, and it has no zero state.
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

fn replica(actor: u64, clock: &ManualClock, compaction: bool) -> Replica<LwwMap> {
    let config = ReplicaConfig {
        snapshot_every_ops: if compaction { 6 } else { u64::MAX },
        ..ReplicaConfig::default()
    };
    let mut builder = Replica::builder(ActorId(actor), LwwMap::new())
        .keys(
            alloc::boxed::Box::new(KeyedHashSigner::new(ActorId(actor), SIM_SECRET)),
            alloc::boxed::Box::new(KeyedHashVerifier::new(SIM_SECRET)),
        )
        .clock(alloc::boxed::Box::new(clock.clone()))
        .config(config);
    if compaction {
        builder = builder.snapshot_sink(alloc::boxed::Box::new(MemorySnapshotSink::default()));
    }
    builder.build().expect("keys and clock are given")
}

/// Runs one simulation.
pub fn run(config: &SimConfig) -> SimReport {
    let clock = ManualClock::new(1_000);
    let mut script = Script(config.seed);
    let count = config.replicas.max(2);
    let mut replicas: Vec<Replica<LwwMap>> = (0..count)
        .map(|i| replica(i as u64 + 1, &clock, config.compaction))
        .collect();
    let ids: Vec<ActorId> = replicas.iter().map(Replica::actor).collect();

    // One link per pair. Each keeps a handle that can take it down. With `late_join`, the last
    // replica is not connected until the middle of the script.
    let late = if config.late_join { count - 1 } else { count };
    let mut controls: Vec<MemoryTransport> = Vec::new();
    let mut up: Vec<bool> = Vec::new();
    for i in 0..late {
        for j in (i + 1)..late {
            let seed = config
                .seed
                .wrapping_mul(31)
                .wrapping_add((i * count + j) as u64);
            connect(
                &mut replicas,
                &ids,
                &mut controls,
                i,
                j,
                seed,
                config.faults,
            );
            up.push(true);
        }
    }

    let mut local_writes = 0;
    let mut partitions = 0;
    for step in 0..config.steps {
        if config.late_join && step == config.steps / 2 {
            for i in 0..late {
                let seed = config.seed.wrapping_mul(37).wrapping_add(i as u64);
                connect(
                    &mut replicas,
                    &ids,
                    &mut controls,
                    i,
                    late,
                    seed,
                    config.faults,
                );
                up.push(true);
            }
        }
        match script.below(100) {
            0..=59 => {
                let who = script.below(count as u64) as usize;
                let key = alloc::format!("k{}", script.below(8));
                let op = if script.below(4) == 0 {
                    LwwOp::Delete {
                        key: key.into_bytes(),
                    }
                } else {
                    LwwOp::Set {
                        key: key.into_bytes(),
                        value: alloc::format!("{step}").into_bytes(),
                    }
                };
                replicas[who].local_op(op.to_node()).expect("local write");
                local_writes += 1;
            }
            60..=69 if !controls.is_empty() => {
                let link = script.below(controls.len() as u64) as usize;
                up[link] = !up[link];
                controls[link].set_up(up[link]);
                if !up[link] {
                    partitions += 1;
                }
            }
            _ => {}
        }
        clock.advance(script.below(200));
        // A random poll order stands in for uneven scheduling between replicas.
        let mut order: Vec<usize> = (0..count).collect();
        for i in (1..count).rev() {
            order.swap(i, script.below(i as u64 + 1) as usize);
        }
        for i in order {
            replicas[i].poll();
        }
    }

    // Heal: every link back up, then enough anti-entropy periods for everything to arrive.
    for control in &controls {
        control.set_up(true);
    }
    for _ in 0..40 {
        clock.advance(5_001);
        for replica in replicas.iter_mut() {
            replica.poll();
        }
    }

    let state_hashes: Vec<u64> = replicas.iter().map(|r| r.state().state_hash()).collect();
    let mut rejections = Vec::new();
    let mut snapshots_installed = 0;
    for replica in replicas.iter_mut() {
        for event in replica.drain_events() {
            match event {
                Event::FrameRejected { error, .. } => rejections.push(error),
                Event::SnapshotInstalled { .. } => snapshots_installed += 1,
                _ => {}
            }
        }
    }
    SimReport {
        converged: state_hashes.windows(2).all(|pair| pair[0] == pair[1]),
        state_hashes,
        local_writes,
        partitions,
        rejections,
        snapshots_installed,
    }
}

/// Connects replicas `i` and `j` with a link, recording the handle that controls it.
fn connect(
    replicas: &mut [Replica<LwwMap>],
    ids: &[ActorId],
    controls: &mut Vec<MemoryTransport>,
    i: usize,
    j: usize,
    seed: u64,
    faults: Faults,
) {
    let (a, b) = MemoryTransport::pair(seed | 1, faults);
    controls.push(a.clone());
    replicas[i].add_peer(ids[j], alloc::boxed::Box::new(a));
    replicas[j].add_peer(ids[i], alloc::boxed::Box::new(b));
}
