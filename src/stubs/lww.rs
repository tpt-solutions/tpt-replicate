//! Last-writer-wins map: the stub CRDT used by tests and examples.
//!
//! Each key keeps the greatest [`Timestamp`] it has seen. Deletes are tombstones that carry a
//! timestamp, so a late write from an offline peer cannot resurrect a deleted key. Because the
//! winner is decided by timestamp alone, application order does not change the result.

use alloc::collections::BTreeMap;
use alloc::vec::Vec;

use crate::clock::Timestamp;
use crate::codec::{Reader, Writer, MAX_PAYLOAD};
use crate::error::{ApplyError, CodecError};
use crate::stubs::{CrdtCompatible, State, UirNode};

const TAG_SET: u8 = 0;
const TAG_DELETE: u8 = 1;

/// A map operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LwwOp {
    /// Writes `value` under `key`.
    Set { key: Vec<u8>, value: Vec<u8> },
    /// Removes `key`. Leaves a tombstone.
    Delete { key: Vec<u8> },
}

impl LwwOp {
    /// Serialises the operation into a [`UirNode`] payload.
    pub fn to_node(&self) -> UirNode {
        let mut w = Writer::new();
        match self {
            LwwOp::Set { key, value } => {
                w.u8(TAG_SET);
                w.bytes(key);
                w.bytes(value);
            }
            LwwOp::Delete { key } => {
                w.u8(TAG_DELETE);
                w.bytes(key);
            }
        }
        UirNode { bytes: w.finish() }
    }

    /// Parses a payload produced by [`LwwOp::to_node`].
    pub fn from_node(node: &UirNode) -> Result<Self, ApplyError> {
        decode_op(&node.bytes)
    }
}

fn malformed(_: CodecError) -> ApplyError {
    ApplyError::MalformedOp
}

fn decode_op(bytes: &[u8]) -> Result<LwwOp, ApplyError> {
    let mut r = Reader::new(bytes);
    let tag = r.u8().map_err(malformed)?;
    let op = match tag {
        TAG_SET => {
            let key = r.bytes(MAX_PAYLOAD).map_err(malformed)?;
            let value = r.bytes(MAX_PAYLOAD).map_err(malformed)?;
            LwwOp::Set { key, value }
        }
        TAG_DELETE => LwwOp::Delete {
            key: r.bytes(MAX_PAYLOAD).map_err(malformed)?,
        },
        other => return Err(ApplyError::UnknownOpTag(other)),
    };
    r.finish().map_err(malformed)?;
    Ok(op)
}

/// One key's winning write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// Timestamp of the winning operation.
    pub stamp: Timestamp,
    /// The value, or `None` for a tombstone.
    pub value: Option<Vec<u8>>,
}

/// Last-writer-wins map over byte-string keys.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LwwMap {
    entries: BTreeMap<Vec<u8>, Entry>,
}

impl LwwMap {
    /// Creates an empty map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the live value for `key`.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.entries
            .get(key)
            .and_then(|entry| entry.value.as_deref())
    }

    /// Returns the raw entry for `key`, including tombstones.
    pub fn entry(&self, key: &[u8]) -> Option<&Entry> {
        self.entries.get(key)
    }

    /// Number of entries, including tombstones.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the map holds no entries at all.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Number of tombstones currently retained.
    pub fn tombstones(&self) -> usize {
        self.entries
            .values()
            .filter(|entry| entry.value.is_none())
            .count()
    }
}

impl CrdtCompatible for LwwMap {
    fn apply(&mut self, stamp: &Timestamp, op: &UirNode) -> Result<(), ApplyError> {
        let (key, value) = match LwwOp::from_node(op)? {
            LwwOp::Set { key, value } => (key, Some(value)),
            LwwOp::Delete { key } => (key, None),
        };
        if let Some(existing) = self.entries.get(&key) {
            if existing.stamp >= *stamp {
                return Ok(());
            }
        }
        self.entries.insert(
            key,
            Entry {
                stamp: *stamp,
                value,
            },
        );
        Ok(())
    }
}

impl State for LwwMap {
    /// Hashes live entries only. Tombstones are not observable, so compacting them must not
    /// change the hash.
    fn state_hash(&self) -> u64 {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        let mut feed = |bytes: &[u8]| {
            for &byte in bytes {
                h ^= u64::from(byte);
                h = h.wrapping_mul(0x0100_0000_01b3);
            }
        };
        for (key, entry) in &self.entries {
            let Some(value) = &entry.value else { continue };
            feed(&(key.len() as u64).to_le_bytes());
            feed(key.as_slice());
            feed(&entry.stamp.wall_time.to_le_bytes());
            feed(&entry.stamp.logical_counter.to_le_bytes());
            feed(&entry.stamp.actor_id.0.to_le_bytes());
            feed(&(value.len() as u64).to_le_bytes());
            feed(value.as_slice());
        }
        h
    }

    fn compact(&mut self, cutoff: &Timestamp) {
        self.entries
            .retain(|_, entry| entry.value.is_some() || entry.stamp > *cutoff);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stubs::ActorId;

    fn stamp(wall: u64, counter: u32, actor: u64) -> Timestamp {
        Timestamp {
            wall_time: wall,
            logical_counter: counter,
            actor_id: ActorId(actor),
        }
    }

    #[test]
    fn op_roundtrip() {
        let op = LwwOp::Set {
            key: b"a".to_vec(),
            value: b"1".to_vec(),
        };
        assert_eq!(LwwOp::from_node(&op.to_node()), Ok(op));
        let del = LwwOp::Delete { key: b"a".to_vec() };
        assert_eq!(LwwOp::from_node(&del.to_node()), Ok(del));
    }

    #[test]
    fn unknown_tag_and_trailing_bytes_are_rejected() {
        assert_eq!(
            LwwOp::from_node(&UirNode {
                bytes: alloc::vec![9]
            }),
            Err(ApplyError::UnknownOpTag(9))
        );
        let mut node = LwwOp::Delete { key: b"k".to_vec() }.to_node();
        node.bytes.push(0);
        assert_eq!(LwwOp::from_node(&node), Err(ApplyError::MalformedOp));
    }

    #[test]
    fn greatest_timestamp_wins_in_any_order() {
        let old = LwwOp::Set {
            key: b"k".to_vec(),
            value: b"old".to_vec(),
        }
        .to_node();
        let new = LwwOp::Set {
            key: b"k".to_vec(),
            value: b"new".to_vec(),
        }
        .to_node();
        let mut a = LwwMap::new();
        a.apply(&stamp(2, 0, 1), &new).unwrap();
        a.apply(&stamp(1, 0, 1), &old).unwrap();
        let mut b = LwwMap::new();
        b.apply(&stamp(1, 0, 1), &old).unwrap();
        b.apply(&stamp(2, 0, 1), &new).unwrap();
        assert_eq!(a.get(b"k"), Some(&b"new"[..]));
        assert_eq!(a, b);
    }

    #[test]
    fn tombstone_blocks_older_write_and_compaction_keeps_newer_entries() {
        let mut m = LwwMap::new();
        let del = LwwOp::Delete { key: b"k".to_vec() }.to_node();
        let set = LwwOp::Set {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        }
        .to_node();
        m.apply(&stamp(5, 0, 1), &del).unwrap();
        m.apply(&stamp(3, 0, 2), &set).unwrap();
        assert_eq!(m.get(b"k"), None);
        assert_eq!(m.tombstones(), 1);
        m.compact(&stamp(5, 0, 1));
        assert!(m.is_empty());
    }

    #[test]
    fn state_hash_ignores_compacted_tombstones() {
        let mut with = LwwMap::new();
        let mut without = LwwMap::new();
        let del = LwwOp::Delete {
            key: b"gone".to_vec(),
        }
        .to_node();
        with.apply(&stamp(1, 0, 1), &del).unwrap();
        with.compact(&stamp(0, 0, 0));
        assert_eq!(with.state_hash(), without.state_hash());
        without.compact(&stamp(9, 0, 0));
        assert_eq!(with.state_hash(), without.state_hash());
    }
}
