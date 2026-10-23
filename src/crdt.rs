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
//! - [`Rga`]: a text or list sequence. Each element is named by its insert's timestamp, and
//!   concurrent inserts at one position are ordered by timestamp. Deletes leave tombstones.

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

const TAG_INSERT: u8 = 0;
const TAG_DELETE: u8 = 1;

/// One element of an [`Rga`] sequence.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Element {
    /// The timestamp of the insert that created the element. It names the element everywhere.
    id: Timestamp,
    value: Vec<u8>,
    /// Deleted elements stay as tombstones. A concurrent insert may still refer to them.
    deleted: bool,
}

/// A replicated sequence of byte-string elements (RGA).
///
/// Each element is identified by the timestamp of its insert. An insert names the element it goes
/// after. Concurrent inserts after the same element are ordered by timestamp, newest first, so every
/// replica places them the same way. A delete marks a tombstone.
///
/// Tombstones are never removed. A concurrent insert may name a deleted element, so compaction
/// cannot tell which tombstones are still needed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Rga {
    elements: Vec<Element>,
}

impl Rga {
    /// An operation that inserts `value` after the element `after`, or at the head if `None`.
    pub fn insert(after: Option<Timestamp>, value: &[u8]) -> UirNode {
        let mut bytes = alloc::vec![TAG_INSERT];
        match after {
            Some(id) => {
                bytes.push(1);
                put_timestamp(&mut bytes, &id);
            }
            None => bytes.push(0),
        }
        bytes.extend_from_slice(value);
        UirNode { bytes }
    }

    /// An operation that deletes the element `target`.
    pub fn delete(target: Timestamp) -> UirNode {
        let mut bytes = alloc::vec![TAG_DELETE];
        put_timestamp(&mut bytes, &target);
        UirNode { bytes }
    }

    /// The live elements in order, with their ids.
    pub fn iter(&self) -> impl Iterator<Item = (Timestamp, &[u8])> {
        self.elements
            .iter()
            .filter(|e| !e.deleted)
            .map(|e| (e.id, e.value.as_slice()))
    }

    /// Number of live elements.
    pub fn len(&self) -> usize {
        self.elements.iter().filter(|e| !e.deleted).count()
    }

    /// Whether there are no live elements.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The id of the live element at `index`.
    pub fn id_at(&self, index: usize) -> Option<Timestamp> {
        self.iter().nth(index).map(|(id, _)| id)
    }

    /// The id to insert after, to place a new element at `index`. `None` means the head.
    pub fn after_index(&self, index: usize) -> Option<Timestamp> {
        if index == 0 {
            None
        } else {
            self.id_at(index - 1)
        }
    }

    /// Position in `elements` of the element with `id`, deleted or not.
    fn position(&self, id: &Timestamp) -> Option<usize> {
        self.elements.iter().position(|e| e.id == *id)
    }
}

impl CrdtCompatible for Rga {
    fn apply(&mut self, stamp: &Timestamp, op: &UirNode) -> Result<(), ApplyError> {
        let (tag, body) = op.bytes.split_first().ok_or(ApplyError::MalformedOp)?;
        match *tag {
            TAG_INSERT => {
                let (flag, rest) = body.split_first().ok_or(ApplyError::MalformedOp)?;
                let (after, value) = match *flag {
                    0 => (None, rest),
                    1 => {
                        let (id, rest) = take_timestamp(rest).ok_or(ApplyError::MalformedOp)?;
                        (Some(id), rest)
                    }
                    _ => return Err(ApplyError::MalformedOp),
                };
                // Applying the same insert twice changes nothing.
                if self.position(stamp).is_some() {
                    return Ok(());
                }
                // Start just after the element named, or at the head.
                let start = match after {
                    None => 0,
                    Some(id) => {
                        self.position(&id)
                            .ok_or(ApplyError::Invalid("unknown reference"))?
                            + 1
                    }
                };
                // Skip past elements that are newer than this one, so that concurrent inserts
                // come out in the same order everywhere.
                let mut at = start;
                while at < self.elements.len() && self.elements[at].id > *stamp {
                    at += 1;
                }
                self.elements.insert(
                    at,
                    Element {
                        id: *stamp,
                        value: value.to_vec(),
                        deleted: false,
                    },
                );
                Ok(())
            }
            TAG_DELETE => {
                let (target, rest) = take_timestamp(body).ok_or(ApplyError::MalformedOp)?;
                if !rest.is_empty() {
                    return Err(ApplyError::MalformedOp);
                }
                let index = self
                    .position(&target)
                    .ok_or(ApplyError::Invalid("unknown element"))?;
                self.elements[index].deleted = true;
                Ok(())
            }
            other => Err(ApplyError::UnknownOpTag(other)),
        }
    }
}

impl State for Rga {
    /// Hashes the live elements in order. Tombstones are not observable.
    fn state_hash(&self) -> u64 {
        let mut hash = FNV_OFFSET;
        for (_, value) in self.iter() {
            fnv(&mut hash, &(value.len() as u64).to_be_bytes());
            fnv(&mut hash, value);
        }
        hash
    }

    /// Tombstones stay, because a concurrent insert may still name one. See [`Rga`].
    fn compact(&mut self, _cutoff: &Timestamp) {}

    fn encode_state(&self) -> Vec<u8> {
        let mut out = (self.elements.len() as u32).to_be_bytes().to_vec();
        for element in &self.elements {
            put_timestamp(&mut out, &element.id);
            out.push(u8::from(element.deleted));
            out.extend_from_slice(&(element.value.len() as u32).to_be_bytes());
            out.extend_from_slice(&element.value);
        }
        out
    }

    fn decode_state(bytes: &[u8]) -> Result<Self, SnapshotError> {
        let malformed = SnapshotError::Malformed;
        let (count, mut rest) = take_u32(bytes).ok_or(malformed.clone())?;
        let mut elements: Vec<Element> = Vec::new();
        for _ in 0..count {
            let (id, after) = take_timestamp(rest).ok_or(malformed.clone())?;
            let (&flag, after) = after.split_first().ok_or(malformed.clone())?;
            let deleted = match flag {
                0 => false,
                1 => true,
                _ => return Err(malformed),
            };
            let (len, after) = take_u32(after).ok_or(malformed.clone())?;
            let len = len as usize;
            if after.len() < len {
                return Err(malformed);
            }
            let (value, after) = after.split_at(len);
            if elements.iter().any(|e| e.id == id) {
                return Err(malformed);
            }
            elements.push(Element {
                id,
                value: value.to_vec(),
                deleted,
            });
            rest = after;
        }
        if !rest.is_empty() {
            return Err(malformed);
        }
        Ok(Self { elements })
    }
}

const TAG_WRITE: u8 = 0;

/// A register that keeps every value written concurrently (MV-register).
///
/// A write names the values its author had seen, and replaces exactly those. So a write that
/// follows another replaces it, while two concurrent writes both survive. Readers see every
/// surviving value, and can resolve the conflict by writing again, which replaces them all.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MvRegister {
    /// Each surviving write, keyed by its timestamp.
    values: BTreeMap<Timestamp, Vec<u8>>,
}

impl MvRegister {
    /// An operation that writes `value`, replacing every value currently in `state`.
    pub fn write(value: &[u8], state: &Self) -> UirNode {
        let mut bytes = alloc::vec![TAG_WRITE];
        bytes.extend_from_slice(&(state.values.len() as u32).to_be_bytes());
        for tag in state.values.keys() {
            put_timestamp(&mut bytes, tag);
        }
        bytes.extend_from_slice(value);
        UirNode { bytes }
    }

    /// Every surviving value, in timestamp order. One value means no conflict.
    pub fn values(&self) -> impl Iterator<Item = &[u8]> {
        self.values.values().map(Vec::as_slice)
    }

    /// The value, if exactly one survives.
    pub fn get(&self) -> Option<&[u8]> {
        match self.values.len() {
            1 => self.values.values().next().map(Vec::as_slice),
            _ => None,
        }
    }
}

impl CrdtCompatible for MvRegister {
    fn apply(&mut self, stamp: &Timestamp, op: &UirNode) -> Result<(), ApplyError> {
        let (tag, body) = op.bytes.split_first().ok_or(ApplyError::MalformedOp)?;
        if *tag != TAG_WRITE {
            return Err(ApplyError::UnknownOpTag(*tag));
        }
        let (count, mut rest) = take_u32(body).ok_or(ApplyError::MalformedOp)?;
        let mut observed = Vec::new();
        for _ in 0..count {
            let (id, after) = take_timestamp(rest).ok_or(ApplyError::MalformedOp)?;
            observed.push(id);
            rest = after;
        }
        for id in &observed {
            self.values.remove(id);
        }
        self.values.insert(*stamp, rest.to_vec());
        Ok(())
    }
}

impl State for MvRegister {
    fn state_hash(&self) -> u64 {
        let mut hash = FNV_OFFSET;
        for (tag, value) in &self.values {
            let mut buf = Vec::with_capacity(20);
            put_timestamp(&mut buf, tag);
            fnv(&mut hash, &buf);
            fnv(&mut hash, &(value.len() as u64).to_be_bytes());
            fnv(&mut hash, value);
        }
        hash
    }

    /// Nothing to drop: a surviving value is a live value.
    fn compact(&mut self, _cutoff: &Timestamp) {}

    fn encode_state(&self) -> Vec<u8> {
        let mut out = (self.values.len() as u32).to_be_bytes().to_vec();
        for (tag, value) in &self.values {
            put_timestamp(&mut out, tag);
            out.extend_from_slice(&(value.len() as u32).to_be_bytes());
            out.extend_from_slice(value);
        }
        out
    }

    fn decode_state(bytes: &[u8]) -> Result<Self, SnapshotError> {
        let malformed = SnapshotError::Malformed;
        let (count, mut rest) = take_u32(bytes).ok_or(malformed.clone())?;
        let mut values = BTreeMap::new();
        for _ in 0..count {
            let (tag, after) = take_timestamp(rest).ok_or(malformed.clone())?;
            let (len, after) = take_u32(after).ok_or(malformed.clone())?;
            let len = len as usize;
            if after.len() < len {
                return Err(malformed);
            }
            let (value, after) = after.split_at(len);
            if values.insert(tag, value.to_vec()).is_some() {
                return Err(malformed);
            }
            rest = after;
        }
        if !rest.is_empty() {
            return Err(malformed);
        }
        Ok(Self { values })
    }
}
