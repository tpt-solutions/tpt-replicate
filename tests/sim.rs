//! Seeded simulation: many random histories, each of which must converge once healed.

use tpt_replicate::error::FrameError;
use tpt_replicate::sim::{run, SimConfig};
use tpt_replicate::transport::Faults;

struct Case {
    name: &'static str,
    faults: Faults,
    compaction: bool,
}

fn check(case: Case, seeds: std::ops::Range<u64>) -> usize {
    let mut failures = Vec::new();
    let mut installs = 0;
    let mut partitions = 0;
    let mut rejected = 0;
    let mut unexpected = Vec::new();
    let mut reasons: std::collections::BTreeMap<String, usize> = Default::default();
    for seed in seeds {
        let report = run(&SimConfig {
            replicas: 4,
            steps: 250,
            seed,
            faults: case.faults,
            compaction: case.compaction,
            late_join: true,
        });
        installs += report.snapshots_installed;
        partitions += report.partitions;
        rejected += report.rejections.len();
        for error in &report.rejections {
            let kind: String = format!("{error:?}").chars().take(60).collect();
            *reasons.entry(kind).or_insert(0usize) += 1;
        }
        unexpected.extend(
            report
                .rejections
                .iter()
                .filter(|error| !matches!(error, FrameError::BadSnapshot(_)))
                .cloned(),
        );
        if !report.converged {
            failures.push((seed, report.state_hashes));
        }
    }
    eprintln!(
        "{}: partitions={partitions} snapshots_installed={installs} rejected_frames={rejected}",
        case.name
    );
    eprintln!("{}: reasons={reasons:?}", case.name);
    // Only snapshot-stream rejections are expected: reordering and loss cut chunk sequences short.
    assert!(
        unexpected.is_empty(),
        "{}: unexpected rejections {unexpected:?}",
        case.name
    );
    assert!(
        failures.is_empty(),
        "{}: diverged for (seed, hashes): {failures:?}",
        case.name
    );
    installs
}

const FAULTY: Faults = Faults {
    loss_permille: 80,
    duplicate_permille: 40,
    reorder: true,
};

#[test]
fn clean_links_converge() {
    check(
        Case {
            name: "clean",
            faults: Faults::default(),
            compaction: false,
        },
        0..20,
    );
}

#[test]
fn lossy_duplicating_reordering_links_converge() {
    check(
        Case {
            name: "faulty",
            faults: FAULTY,
            compaction: false,
        },
        0..20,
    );
}

#[test]
fn late_joiner_catches_up_through_snapshots() {
    let installs = check(
        Case {
            name: "compacting",
            faults: Faults::default(),
            compaction: true,
        },
        0..20,
    );
    assert!(
        installs > 0,
        "the scenario must exercise snapshot installation"
    );
}

#[test]
fn late_joiner_catches_up_on_faulty_links() {
    let installs = check(
        Case {
            name: "compacting+faulty",
            faults: FAULTY,
            compaction: true,
        },
        0..20,
    );
    assert!(
        installs > 0,
        "the scenario must exercise snapshot installation"
    );
}

/// Many more histories than the default run. The scheduled workflow runs it with
/// `--include-ignored`.
#[test]
#[ignore = "slow: run with --include-ignored (the scheduled workflow does)"]
fn many_seeds_on_faulty_compacting_links() {
    let installs = check(
        Case {
            name: "heavy",
            faults: FAULTY,
            compaction: true,
        },
        0..300,
    );
    assert!(installs > 0);
}
