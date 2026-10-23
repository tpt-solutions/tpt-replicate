//! Replicated types beyond the LWW map, behind the `crdt` feature.
//!
//! Both types are operation-based, and both are safe to apply more than once. The engine delivers
//! each operation once, but a snapshot followed by a delta can re-deliver operations the snapshot
//! already contains. Each type is built so that a repeat changes nothing.
//!
//! - [`PnCounter`]: a counter that goes up and down. Each author's operations are applied once,
//!   in order, so the state keeps the author's last applied timestamp and skips anything not newer.
//! - [`OrSet`]: a set where an add beats a concurrent remove. Each add is tagged with its
//!   timestamp. A remove carries the tags it saw, so it removes only those.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::vec::Vec;

use crate::clock::Timestamp;
use crate::error::{ApplyError, SnapshotError};
use crate::stubs::{ActorId, CrdtCompatible, State, UirNode};

/// Stable 64-bit FNV-1a over a byte sequence. Used for state hashes.
fn fnv(hash: &mut u64, bytes: &[u8]) {
    for &byte in bytes {
        *hash ^= u64::from(byte);
        *hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

fn put_timestamp(out: &mut Vec<u8>, stamp: &Timestamp) {
    out.extend_from_slice(&stamp.wall_time.to_be_bytes());
    out.extend_from_slice(&stamp.logical_counter.to_be_bytes());
    out.extend_from_slice(&stamp.actor_id.0.to_be_bytes());
}

/// Reads a timestamp from the front of `bytes`, returning it with the remainder.
fn take_timestamp(bytes: &[u8]) -> Option<(Timestamp, &[u8])> {
    if bytes.len() < 20 {
        return None;
    }
    let wall = u64::from_be_bytes(bytes[0..8].try_into().ok()?);
    let counter = u32::from_be_bytes(bytes[8..12].try_into().ok()?);
    let actor = u64::from_be_bytes(bytes[12..20].try_into().ok()?);
    Some((
        Timestamp {
            wall_time: wall,
            logical_counter: counter,
            actor_id: ActorId(actor),
        },
        &bytes[20..],
    ))
}

/// A counter that supports increments and decrements.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PnCounter {
    /// Per author: the last timestamp applied, and that author's net total.
    authors: BTreeMap<ActorId, (Timestamp, i64)>,
}

impl PnCounter {
    /// An operation that adds `delta` to the counter. Negative values decrement.
    pub fn add(delta: i64) -> UirNode {
        UirNode {
            bytes: delta.to_be_bytes().to_vec(),
        }
    }

    /// The counter's value.
    pub fn value(&self) -> i64 {
        self.authors
            .values()
            .fold(0i64, |sum, (_, total)| sum.wrapping_add(*total))
    }
}

impl CrdtCompatible for PnCounter {
    fn apply(&mut self, stamp: &Timestamp, op: &UirNode) -> Result<(), ApplyError> {
        let bytes: [u8; 8] = op
            .bytes
            .as_slice()
            .try_into()
            .map_err(|_| ApplyError::MalformedOp)?;
        let delta = i64::from_be_bytes(bytes);
        match self.authors.get_mut(&stamp.actor_id) {
            Some((last, _)) if *stamp <= *last => {}
            Some((last, total)) => {
                *last = *stamp;
                *total = total.wrapping_add(delta);
            }
            None => {
                self.authors.insert(stamp.actor_id, (*stamp, delta));
            }
        }
        Ok(())
    }
}

impl State for PnCounter {
    fn state_hash(&self) -> u64 {
        let mut hash = FNV_OFFSET;
        for (actor, (_, total)) in &self.authors {
            fnv(&mut hash, &actor.0.to_be_bytes());
            fnv(&mut hash, &total.to_be_bytes());
        }
        hash
    }

    /// Nothing to drop: the per-author stamps are needed for deduplication.
    fn compact(&mut self, _cutoff: &Timestamp) {}

    fn encode_state(&self) -> Vec<u8> {
        let mut out = (self.authors.len() as u64).to_be_bytes().to_vec();
        for (actor, (stamp, total)) in &self.authors {
            out.extend_from_slice(&actor.0.to_be_bytes());
            put_timestamp(&mut out, stamp);
            out.extend_from_slice(&total.to_be_bytes());
        }
        out
    }

    fn decode_state(bytes: &[u8]) -> Result<Self, SnapshotError> {
        let malformed = SnapshotError::Malformed;
        let (count, mut rest) = take_u64(bytes).ok_or(malformed.clone())?;
        let mut authors = BTreeMap::new();
        for _ in 0..count {
            let (actor, after) = take_u64(rest).ok_or(malformed.clone())?;
            let (stamp, after) = take_timestamp(after).ok_or(malformed.clone())?;
            let (total, after) = take_u64(after).ok_or(malformed.clone())?;
            if stamp.actor_id != ActorId(actor)
                || authors
                    .insert(ActorId(actor), (stamp, total as i64))
                    .is_some()
            {
                return Err(malformed);
            }
            rest = after;
        }
        if !rest.is_empty() {
            return Err(malformed);
        }
        Ok(Self { authors })
    }
}

fn take_u64(bytes: &[u8]) -> Option<(u64, &[u8])> {
    let head: [u8; 8] = bytes.get(0..8)?.try_into().ok()?;
    Some((u64::from_be_bytes(head), &bytes[8..]))
}

const TAG_ADD: u8 = 0;
const TAG_REMOVE: u8 = 1;

/// A set with add-wins semantics. Elements are byte strings.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OrSet {
    /// Each element with the tags of the adds that currently hold it.
    elements: BTreeMap<Vec<u8>, BTreeSet<Timestamp>>,
}

impl OrSet {
    /// An operation that adds `element`. Its timestamp becomes the element's tag.
    pub fn add(element: &[u8]) -> UirNode {
        let mut bytes = alloc::vec![TAG_ADD];
        bytes.extend_from_slice(element);
        UirNode { bytes }
    }

    /// An operation that removes `element` as it is in `state`. Only the adds seen here are removed,
    /// so an add made concurrently survives.
    pub fn remove(element: &[u8], state: &Self) -> UirNode {
        let tags: Vec<Timestamp> = state
            .elements
            .get(element)
            .map(|tags| tags.iter().copied().collect())
            .unwrap_or_default();
        let mut bytes = alloc::vec![TAG_REMOVE];
        bytes.extend_from_slice(&(element.len() as u32).to_be_bytes());
        bytes.extend_from_slice(element);
        bytes.extend_from_slice(&(tags.len() as u32).to_be_bytes());
        for tag in &tags {
            put_timestamp(&mut bytes, tag);
        }
        UirNode { bytes }
    }

    /// Whether `element` is in the set.
    pub fn contains(&self, element: &[u8]) -> bool {
        self.elements.contains_key(element)
    }

    /// The elements, in byte order.
    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.elements.keys().map(Vec::as_slice)
    }

    fn tags_of(&self, element: &[u8]) -> Option<&BTreeSet<Timestamp>> {
        self.elements.get(element)
    }
}

impl CrdtCompatible for OrSet {
    fn apply(&mut self, stamp: &Timestamp, op: &UirNode) -> Result<(), ApplyError> {
        let (tag, body) = op.bytes.split_first().ok_or(ApplyError::MalformedOp)?;
        match *tag {
            TAG_ADD => {
                self.elements
                    .entry(body.to_vec())
                    .or_default()
                    .insert(*stamp);
                Ok(())
            }
            TAG_REMOVE => {
                let (len, rest) = take_u32(body).ok_or(ApplyError::MalformedOp)?;
                let len = len as usize;
                if rest.len() < len {
                    return Err(ApplyError::MalformedOp);
                }
                let (element, rest) = rest.split_at(len);
                let (count, mut rest) = take_u32(rest).ok_or(ApplyError::MalformedOp)?;
                let mut observed = Vec::new();
                for _ in 0..count {
                    let (tag, after) = take_timestamp(rest).ok_or(ApplyError::MalformedOp)?;
                    observed.push(tag);
                    rest = after;
                }
                if !rest.is_empty() {
                    return Err(ApplyError::MalformedOp);
                }
                if let Some(tags) = self.elements.get_mut(element) {
                    for tag in &observed {
                        tags.remove(tag);
                    }
                    if tags.is_empty() {
                        self.elements.remove(element);
                    }
                }
                Ok(())
            }
            other => Err(ApplyError::UnknownOpTag(other)),
        }
    }
}

fn take_u32(bytes: &[u8]) -> Option<(u32, &[u8])> {
    let head: [u8; 4] = bytes.get(0..4)?.try_into().ok()?;
    Some((u32::from_be_bytes(head), &bytes[4..]))
}

impl State for OrSet {
    fn state_hash(&self) -> u64 {
        let mut hash = FNV_OFFSET;
        for (element, tags) in &self.elements {
            fnv(&mut hash, &(element.len() as u64).to_be_bytes());
            fnv(&mut hash, element);
            for tag in tags {
                let mut buf = Vec::with_capacity(20);
                put_timestamp(&mut buf, tag);
                fnv(&mut hash, &buf);
            }
        }
        hash
    }

    /// Removed tags are dropped at the time of removal, so there is nothing left to compact.
    fn compact(&mut self, _cutoff: &Timestamp) {}

    fn encode_state(&self) -> Vec<u8> {
        let mut out = (self.elements.len() as u32).to_be_bytes().to_vec();
        for (element, tags) in &self.elements {
            out.extend_from_slice(&(element.len() as u32).to_be_bytes());
            out.extend_from_slice(element);
            out.extend_from_slice(&(tags.len() as u32).to_be_bytes());
            for tag in tags {
                put_timestamp(&mut out, tag);
            }
        }
        out
    }

    fn decode_state(bytes: &[u8]) -> Result<Self, SnapshotError> {
        let malformed = SnapshotError::Malformed;
        let (count, mut rest) = take_u32(bytes).ok_or(malformed.clone())?;
        let mut elements = BTreeMap::new();
        for _ in 0..count {
            let (len, after) = take_u32(rest).ok_or(malformed.clone())?;
            let len = len as usize;
            if after.len() < len {
                return Err(malformed);
            }
            let (element, after) = after.split_at(len);
            let (tag_count, mut after) = take_u32(after).ok_or(malformed.clone())?;
            let mut tags = BTreeSet::new();
            for _ in 0..tag_count {
                let (tag, next) = take_timestamp(after).ok_or(malformed.clone())?;
                tags.insert(tag);
                after = next;
            }
            if tags.is_empty() || elements.insert(element.to_vec(), tags).is_some() {
                return Err(malformed);
            }
            rest = after;
        }
        if !rest.is_empty() {
            return Err(malformed);
        }
        Ok(Self { elements })
    }
}

impl OrSet {
    /// Tags currently holding `element`, for callers that want to inspect them.
    pub fn tags(&self, element: &[u8]) -> Vec<Timestamp> {
        self.tags_of(element)
            .map(|tags| tags.iter().copied().collect())
            .unwrap_or_default()
    }
}
