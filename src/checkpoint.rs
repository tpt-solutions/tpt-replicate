//! Durable checkpoints: one byte blob holds a replica's state and log, so a restart can resume from
//! it without the application tracking the state separately.
//!
//! A checkpoint is written by [`Replica::checkpoint`] and read by [`Replica::recover`]. Where the
//! bytes go is up to a [`CheckpointStore`]. Pending operations and peer links are not included.
//! Peers re-send anything missing through the handshake.

use alloc::vec::Vec;

use crate::clock::ClockSource;
use crate::codec::{Reader, Writer, MAX_FRAME};
use crate::engine::{LogImage, Replica};
use crate::error::RecoverError;
use crate::error::{CodecError, SnapshotError};
use crate::stubs::crypto::{Signer, Verifier};
use crate::stubs::{ActorId, State};

impl LogImage {
    /// Encodes the image to bytes. [`LogImage::decode`] reverses it.
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.varint(self.ops.len() as u64);
        for op in &self.ops {
            w.signed_op(op);
        }
        w.clock(&self.compacted);
        match &self.floor {
            Some(floor) => {
                w.u8(1);
                w.timestamp(floor);
            }
            None => w.u8(0),
        }
        w.varint(self.state_hash);
        w.finish()
    }

    /// Decodes bytes produced by [`LogImage::encode`]. Returns an error for anything else.
    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut r = Reader::new(bytes);
        let count = r.count(MAX_FRAME)?;
        let mut ops = Vec::with_capacity(count);
        for _ in 0..count {
            ops.push(r.signed_op()?);
        }
        let compacted = r.clock()?;
        let floor = match r.u8()? {
            0 => None,
            1 => Some(r.timestamp()?),
            other => return Err(CodecError::InvalidTag(other)),
        };
        let state_hash = r.varint()?;
        r.finish()?;
        Ok(Self {
            ops,
            compacted,
            floor,
            state_hash,
        })
    }
}

impl<S: State> Replica<S> {
    /// Encodes the state and log into one blob. Store it with a [`CheckpointStore`].
    pub fn checkpoint(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.bytes(&self.state().encode_state());
        w.bytes(&self.export_log().encode());
        w.finish()
    }

    /// Rebuilds a replica from a blob made by [`Replica::checkpoint`].
    ///
    /// Malformed or tampered bytes return an error. They never panic. The state is checked against
    /// the hash in the log, and the restore rules in [`Replica::restore`] apply.
    pub fn recover(
        bytes: &[u8],
        actor: ActorId,
        signer: alloc::boxed::Box<dyn Signer>,
        verifier: alloc::boxed::Box<dyn Verifier>,
        clock: alloc::boxed::Box<dyn ClockSource>,
    ) -> Result<Self, RecoverError> {
        let malformed = |_: CodecError| RecoverError::Malformed;
        let mut r = Reader::new(bytes);
        let state_bytes = r.bytes(MAX_FRAME).map_err(malformed)?;
        let image_bytes = r.bytes(MAX_FRAME).map_err(malformed)?;
        r.finish().map_err(malformed)?;
        let state = S::decode_state(&state_bytes).map_err(RecoverError::State)?;
        let image = LogImage::decode(&image_bytes).map_err(malformed)?;
        Self::restore(actor, state, image, signer, verifier, clock).map_err(RecoverError::Restore)
    }
}

/// Somewhere checkpoint bytes are kept durably.
pub trait CheckpointStore {
    /// Replaces the stored checkpoint. A failed save must leave the previous checkpoint intact.
    fn save(&mut self, bytes: &[u8]) -> Result<(), SnapshotError>;
    /// Returns the stored checkpoint, or `None` if there is none yet.
    fn load(&self) -> Result<Option<Vec<u8>>, SnapshotError>;
}

/// An in-memory [`CheckpointStore`], for tests and for replicas that restart inside one process.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MemoryStore {
    bytes: Option<Vec<u8>>,
}

impl CheckpointStore for MemoryStore {
    fn save(&mut self, bytes: &[u8]) -> Result<(), SnapshotError> {
        self.bytes = Some(bytes.to_vec());
        Ok(())
    }

    fn load(&self) -> Result<Option<Vec<u8>>, SnapshotError> {
        Ok(self.bytes.clone())
    }
}

/// A [`CheckpointStore`] backed by one file. Saves write a temporary file, sync it to disk, and
/// rename it over the old checkpoint. A crash mid-save leaves the previous checkpoint in place.
#[cfg(feature = "std")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileStore {
    path: std::path::PathBuf,
}

#[cfg(feature = "std")]
impl FileStore {
    /// Stores checkpoints at `path`. The file is created on the first save.
    pub fn new(path: impl Into<std::path::PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

#[cfg(feature = "std")]
impl CheckpointStore for FileStore {
    fn save(&mut self, bytes: &[u8]) -> Result<(), SnapshotError> {
        use std::io::Write;
        let io = |error: std::io::Error| {
            SnapshotError::Storage(alloc::string::ToString::to_string(&error))
        };
        let tmp = self.path.with_extension("tmp");
        let mut file = std::fs::File::create(&tmp).map_err(io)?;
        file.write_all(bytes).map_err(io)?;
        file.sync_all().map_err(io)?;
        drop(file);
        std::fs::rename(&tmp, &self.path).map_err(io)
    }

    fn load(&self) -> Result<Option<Vec<u8>>, SnapshotError> {
        match std::fs::read(&self.path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(SnapshotError::Storage(alloc::string::ToString::to_string(
                &error,
            ))),
        }
    }
}
