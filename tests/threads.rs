//! Multi-thread uniqueness milestone: timestamps issued concurrently from several threads are
//! unique and strictly increasing within each thread.

#![cfg(feature = "std")]

use std::collections::HashSet;
use std::thread;

use tpt_replicate::clock::{HybridClock, SystemClock, Timestamp};
use tpt_replicate::stubs::ActorId;

#[test]
fn concurrent_threads_issue_unique_monotonic_timestamps() {
    const THREADS: u64 = 8;
    const PER_THREAD: usize = 5_000;

    let handles: Vec<_> = (1..=THREADS)
        .map(|actor| {
            thread::spawn(move || {
                // HybridClock holds a non-Send clock source, so each thread builds its own.
                let mut clock = HybridClock::new(ActorId(actor), Box::new(SystemClock));
                let mut stamps = Vec::with_capacity(PER_THREAD);
                for _ in 0..PER_THREAD {
                    stamps.push(clock.now().expect("counter must not overflow"));
                }
                stamps
            })
        })
        .collect();

    let mut all: Vec<Timestamp> = Vec::new();
    for handle in handles {
        let stamps = handle.join().expect("thread panicked");
        assert!(
            stamps.windows(2).all(|pair| pair[0] < pair[1]),
            "per-thread timestamps must be strictly increasing"
        );
        all.extend(stamps);
    }

    let unique: HashSet<Timestamp> = all.iter().copied().collect();
    assert_eq!(
        unique.len(),
        all.len(),
        "timestamps must be globally unique"
    );
    assert_eq!(all.len(), THREADS as usize * PER_THREAD);
}
