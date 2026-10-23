//! Cost of encoding and decoding a batch of operations, plain and compressed.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use tpt_replicate::clock::ManualClock;
use tpt_replicate::codec::{BinaryCodec, Codec, Compressed};
use tpt_replicate::prelude::*;
use tpt_replicate::sync::Message;

fn bench_codec(c: &mut Criterion) {
    let clock = ManualClock::new(1_000);
    let mut replica = Replica::builder(ActorId(1), LwwMap::new())
        .shared_secret(5)
        .clock(Box::new(clock.clone()))
        .build()
        .expect("replica");
    for i in 0..128u32 {
        clock.advance(1);
        let op = LwwOp::Set {
            key: format!("key-{i:05}").into_bytes(),
            value: vec![b'x'; 64],
        };
        replica.local_op(op.to_node()).expect("write");
    }
    let message = Message::Delta(replica.export_log().ops);

    let plain = BinaryCodec;
    let packed = Compressed::new(BinaryCodec);
    let encoded = plain.encode(&message);
    let compressed = packed.encode(&message);

    let mut group = c.benchmark_group("codec");
    group.throughput(Throughput::Elements(128));
    group.bench_function("encode 128-op delta", |b| b.iter(|| plain.encode(&message)));
    group.bench_function("decode 128-op delta", |b| {
        b.iter(|| plain.decode(&encoded).expect("decode"))
    });
    group.bench_function("encode 128-op delta, compressed", |b| {
        b.iter(|| packed.encode(&message))
    });
    group.bench_function("decode 128-op delta, compressed", |b| {
        b.iter(|| packed.decode(&compressed).expect("decode"))
    });
    group.finish();
}

criterion_group!(benches, bench_codec);
criterion_main!(benches);
