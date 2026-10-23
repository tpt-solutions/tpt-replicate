//! Snapshots as commits in `tpt-version`, behind the `version` feature.
//!
//! Each snapshot is one commit. Its tree holds one entity, the snapshot record: the cutoff, the
//! state hash, the operation count, and the encoded state. A commit's history is the snapshot
//! history, so it can be audited and, once [`VersionSink::with_signer`] is used, signed.
//!
//! The entity trait comes from `tpt-version`, which describes its own `Entity` as a stand-in for
//! `tpt-model`. The mapping here follows that trait. If the real trait changes, only this module
//! moves.

use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::ToString;

use alloc::vec::Vec;

use tpt_version::{
    ActorId as CommitAuthor, CasStore, Clock, Entity, EntityId, ObjectError, RefStore, Repository,
    Scalar, Signer as CommitSigner, UirNode,
};

use crate::clock::Timestamp;
use crate::error::SnapshotError;
use crate::stubs::{Snapshot, SnapshotSink};

/// The fixed entity id of the snapshot record. Each snapshot replaces the previous one in the tree.
const SNAPSHOT_ENTITY: [u8; 16] = *b"tpt-replicate.sn";

/// Layout of the header field: wall time, logical counter, actor, state hash, operation count.
const HEADER_LEN: usize = 8 + 4 + 8 + 8 + 8;

/// A snapshot as read back from the repository.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotRecord {
    /// Every operation at or before this timestamp is in the state.
    pub cutoff: Timestamp,
    /// The state hash at the snapshot point.
    pub state_hash: u64,
    /// The number of log entries at the snapshot point.
    pub op_count: u64,
    /// The encoded state, from `State::encode_state`.
    pub state: Vec<u8>,
}

/// The snapshot record stored as the commit's one entity.
struct SnapshotEntity(SnapshotRecord);

impl Entity for SnapshotEntity {
    fn id(&self) -> EntityId {
        EntityId::from_bytes(SNAPSHOT_ENTITY)
    }

    fn to_uir(&self) -> UirNode {
        let record = &self.0;
        let mut header = Vec::with_capacity(HEADER_LEN);
        header.extend_from_slice(&record.cutoff.wall_time.to_be_bytes());
        header.extend_from_slice(&record.cutoff.logical_counter.to_be_bytes());
        header.extend_from_slice(&record.cutoff.actor_id.0.to_be_bytes());
        header.extend_from_slice(&record.state_hash.to_be_bytes());
        header.extend_from_slice(&record.op_count.to_be_bytes());
        let mut fields = BTreeMap::new();
        fields.insert("header".to_string(), UirNode::Scalar(Scalar::Bytes(header)));
        fields.insert(
            "state".to_string(),
            UirNode::Scalar(Scalar::Bytes(record.state.clone())),
        );
        UirNode::Map(fields)
    }

    fn from_uir(node: &UirNode) -> Result<Self, ObjectError> {
        let UirNode::Map(fields) = node else {
            return Err(ObjectError::InvalidValue("snapshot record is not a map"));
        };
        let bytes = |name: &str| match fields.get(name) {
            Some(UirNode::Scalar(Scalar::Bytes(bytes))) => Ok(bytes.as_slice()),
            _ => Err(ObjectError::InvalidValue("snapshot record field")),
        };
        let header = bytes("header")?;
        if header.len() != HEADER_LEN {
            return Err(ObjectError::InvalidValue("snapshot header length"));
        }
        let word = |at: usize, len: usize| -> &[u8] { &header[at..at + len] };
        let wall = u64::from_be_bytes(word(0, 8).try_into().map_err(|_| ObjectError::Truncated)?);
        let counter =
            u32::from_be_bytes(word(8, 4).try_into().map_err(|_| ObjectError::Truncated)?);
        let actor = u64::from_be_bytes(word(12, 8).try_into().map_err(|_| ObjectError::Truncated)?);
        let state_hash =
            u64::from_be_bytes(word(20, 8).try_into().map_err(|_| ObjectError::Truncated)?);
        let op_count =
            u64::from_be_bytes(word(28, 8).try_into().map_err(|_| ObjectError::Truncated)?);
        Ok(Self(SnapshotRecord {
            cutoff: Timestamp {
                wall_time: wall,
                logical_counter: counter,
                actor_id: crate::stubs::ActorId(actor),
            },
            state_hash,
            op_count,
            state: bytes("state")?.to_vec(),
        }))
    }
}

/// A [`SnapshotSink`] that commits each snapshot to a `tpt-version` repository.
///
/// The commit author is `tpt-version`'s [`CommitAuthor`], a 16-byte identity. Commits are
/// unsigned until [`VersionSink::with_signer`] is given a signer.
pub struct VersionSink<S, C> {
    repo: Repository<S, C>,
    author: CommitAuthor,
}

impl<S, C> VersionSink<S, C>
where
    S: CasStore + RefStore + 'static,
    C: Clock + 'static,
{
    /// Opens the repository on `branch`. Snapshots are committed to that branch.
    pub fn open(
        store: S,
        clock: C,
        branch: &str,
        author: CommitAuthor,
    ) -> Result<Self, SnapshotError> {
        let repo = Repository::open(store, clock, branch).map_err(storage)?;
        Ok(Self { repo, author })
    }

    /// Signs every commit with `signer`, so each snapshot can be verified later.
    pub fn with_signer(self, signer: alloc::boxed::Box<dyn CommitSigner>) -> Self {
        Self {
            repo: self.repo.with_signer(signer),
            author: self.author,
        }
    }

    /// The most recent snapshot, or `None` if none has been committed yet.
    pub fn latest(&self) -> Result<Option<SnapshotRecord>, SnapshotError> {
        let entity = self
            .repo
            .entity::<SnapshotEntity>(&EntityId::from_bytes(SNAPSHOT_ENTITY))
            .map_err(storage)?;
        Ok(entity.map(|entity| entity.0))
    }

    /// The underlying repository, for reading history with `log`, `head` and similar.
    pub fn repository(&self) -> &Repository<S, C> {
        &self.repo
    }
}

impl<S, C> SnapshotSink for VersionSink<S, C>
where
    S: CasStore + RefStore + 'static,
    C: Clock + 'static,
{
    fn persist(&mut self, snapshot: &Snapshot, state: &[u8]) -> Result<(), SnapshotError> {
        let record = SnapshotRecord {
            cutoff: snapshot.cutoff,
            state_hash: snapshot.state_hash,
            op_count: snapshot.op_count,
            state: state.to_vec(),
        };
        self.repo
            .put_entity(&SnapshotEntity(record))
            .map_err(storage)?;
        let message = format!(
            "snapshot cutoff {}.{}.{} ops {}",
            snapshot.cutoff.wall_time,
            snapshot.cutoff.logical_counter,
            snapshot.cutoff.actor_id.0,
            snapshot.op_count
        );
        self.repo
            .commit(&message, &self.author)
            .map(|_| ())
            .map_err(storage)
    }
}

fn storage(error: impl core::fmt::Debug) -> SnapshotError {
    SnapshotError::Storage(format!("{error:?}"))
}
