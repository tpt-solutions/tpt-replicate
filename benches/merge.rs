//! Cost of merging a batch of remote operations into a replica.

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use tpt_replicate::clock::ManualClock;
use tpt_replicate::prelude::*;

const SECRET: u64 = 5;

fn replica(actor: u64, clock: &ManualClock) -> Replica<LwwMap> {
    Replica::builder(ActorId(actor), LwwMap::new())
        .shared_secret(SECRET)
        .clock(Box::new(clock.clone()))
        .build()
        .expect("replica")
}

fn bench_merge(c: &mut Criterion) {
    let clock = ManualClock::new(1_000);
    let mut source = replica(1, &clock);
    for i in 0..1_000u32 {
        clock.advance(1);
        let op = LwwOp::Set {
            key: format!("k{i}").into_bytes(),
            value: b"value".to_vec(),
        };
        source.local_op(op.to_node()).expect("write");
    }
    let ops = source.export_log().ops;

    let mut group = c.benchmark_group("merge");
    group.throughput(Throughput::Elements(ops.len() as u64));
    group.bench_function("import 1000 ordered ops into a fresh replica", |b| {
        b.iter_batched(
            || (replica(2, &clock), ops.clone()),
            |(mut target, batch)| target.import_ops(batch).expect("merge"),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("import 1000 duplicate ops", |b| {
        let mut target = replica(2, &clock);
        target.import_ops(ops.clone()).expect("merge");
        b.iter_batched(
            || ops.clone(),
            |batch| target.import_ops(batch).expect("merge"),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(benches, bench_merge);
criterion_main!(benches);
