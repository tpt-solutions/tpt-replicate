//! Writing your own replicated state. This one is a last-writer-wins register holding a `u64`.
//!
//! Three things are needed: an operation format (`apply`), a state (`state_hash`, `compact`) and a
//! snapshot format (`encode_state` and `decode_state`). Snapshots let a replica that was offline
//! for a long time catch up without the operations it missed.
//!
//! Run with `cargo run --example custom_crdt`.

use tpt_replicate::clock::{ManualClock, Timestamp};
use tpt_replicate::error::{ApplyError, SnapshotError};
use tpt_replicate::prelude::*;
use tpt_replicate::stubs::{CrdtCompatible, State, UirNode};

/// The register. `None` until the first write.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Register {
    latest: Option<(Timestamp, u64)>,
}

impl CrdtCompatible for Register {
    /// Operations are eight big-endian bytes. The greatest timestamp wins, so repeating an
    /// operation changes nothing and the order of application does not matter.
    fn apply(&mut self, stamp: &Timestamp, op: &UirNode) -> Result<(), ApplyError> {
        let bytes: [u8; 8] = op
            .bytes
            .as_slice()
            .try_into()
            .map_err(|_| ApplyError::MalformedOp)?;
        let value = u64::from_be_bytes(bytes);
        match self.latest {
            Some((current, _)) if *stamp <= current => {}
            _ => self.latest = Some((*stamp, value)),
        }
        Ok(())
    }
}

impl State for Register {
    fn state_hash(&self) -> u64 {
        // Only the value is observable, so the hash covers only the value.
        self.latest.map_or(0, |(_, value)| value.wrapping_add(1))
    }

    /// A register keeps no tombstones, so there is nothing to drop.
    fn compact(&mut self, _cutoff: &Timestamp) {}

    fn encode_state(&self) -> Vec<u8> {
        match self.latest {
            None => vec![0],
            Some((stamp, value)) => {
                let mut out = vec![1];
                out.extend_from_slice(&stamp.wall_time.to_be_bytes());
                out.extend_from_slice(&stamp.logical_counter.to_be_bytes());
                out.extend_from_slice(&stamp.actor_id.0.to_be_bytes());
                out.extend_from_slice(&value.to_be_bytes());
                out
            }
        }
    }

    fn decode_state(bytes: &[u8]) -> Result<Self, SnapshotError> {
        match bytes {
            [0] => Ok(Self::default()),
            [1, rest @ ..] if rest.len() == 28 => {
                let wall = u64::from_be_bytes(rest[0..8].try_into().unwrap());
                let counter = u32::from_be_bytes(rest[8..12].try_into().unwrap());
                let actor = u64::from_be_bytes(rest[12..20].try_into().unwrap());
                let value = u64::from_be_bytes(rest[20..28].try_into().unwrap());
                let stamp = Timestamp {
                    wall_time: wall,
                    logical_counter: counter,
                    actor_id: ActorId(actor),
                };
                Ok(Self {
                    latest: Some((stamp, value)),
                })
            }
            _ => Err(SnapshotError::Malformed),
        }
    }
}

fn write(value: u64) -> UirNode {
    UirNode {
        bytes: value.to_be_bytes().to_vec(),
    }
}

fn main() {
    let clock = ManualClock::new(1_000);
    let mut left = Replica::builder(ActorId(1), Register::default())
        .shared_secret(3)
        .clock(Box::new(clock.clone()))
        .build()
        .expect("replica");
    let mut right = Replica::builder(ActorId(2), Register::default())
        .shared_secret(3)
        .clock(Box::new(clock.clone()))
        .build()
        .expect("replica");
    let (to_right, to_left) = MemoryTransport::pair(2, Faults::default());
    left.add_peer(right.actor(), Box::new(to_right));
    right.add_peer(left.actor(), Box::new(to_left));

    left.local_op(write(41)).expect("write");
    clock.advance(5);
    right.local_op(write(42)).expect("write");
    for _ in 0..4 {
        left.poll();
        right.poll();
    }

    // The later write (42) wins on both sides.
    println!("left = {:?}", left.state().latest.map(|(_, v)| v));
    println!("right = {:?}", right.state().latest.map(|(_, v)| v));
    assert_eq!(left.state(), right.state());
}
