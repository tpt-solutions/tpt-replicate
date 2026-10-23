//! Cost of working out which operations a peer is missing.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use tpt_replicate::clock::{ManualClock, VectorClock};
use tpt_replicate::prelude::*;
use tpt_replicate::sync::calculate_delta;

fn bench_delta(c: &mut Criterion) {
    let clock = ManualClock::new(1_000);
    let mut replica = Replica::builder(ActorId(1), LwwMap::new())
        .shared_secret(5)
        .clock(Box::new(clock.clone()))
        .build()
        .expect("replica");
    for i in 0..10_000u32 {
        clock.advance(1);
        let op = LwwOp::Set {
            key: format!("k{i}").into_bytes(),
            value: b"v".to_vec(),
        };
        replica.local_op(op.to_node()).expect("write");
    }
    // A peer that has seen the first half of the log.
    let mut peer_clock = VectorClock::new();
    peer_clock.observe(ActorId(1), 5_000);
    let log = replica.log();

    let mut group = c.benchmark_group("delta");
    group.throughput(Throughput::Elements(10_000));
    group.bench_function("delta for a peer half-way through a 10k log", |b| {
        b.iter(|| calculate_delta(log, &peer_clock).len());
    });
    group.finish();
}

criterion_group!(benches, bench_delta);
criterion_main!(benches);
