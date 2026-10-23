//! Error types. Each module reports its own failures. The engine maps them into
//! [`EngineError`] and [`FrameError`].

use alloc::string::String;

use crate::stubs::ActorId;

/// Wire decoding failures. Decoding never allocates more than its input can justify.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CodecError {
    /// The input ended before a field was complete.
    Truncated,
    /// A varint does not fit in 64 bits.
    VarintOverflow,
    /// An unknown message or envelope tag.
    InvalidTag(u8),
    /// A length or count exceeded its limit.
    TooLarge,
    /// Bytes remained after a complete message.
    TrailingBytes,
}

/// Logical clock failures.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClockError {
    /// The logical counter would wrap within one millisecond.
    CounterOverflow,
    /// A remote timestamp is further ahead of local time than the allowed drift.
    RemoteTooFarAhead { remote_wall: u64, local_wall: u64 },
}

/// A state operation could not be applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApplyError {
    /// The payload does not decode as a valid operation.
    MalformedOp,
    /// The payload uses an operation tag this state does not know.
    UnknownOpTag(u8),
    /// The replica's [`OpPolicy`](crate::merge::OpPolicy) does not let this author do this.
    Denied,
    /// The application's validation hook refused the operation for the given reason.
    Invalid(&'static str),
}

/// A remote batch was rejected. A rejected batch leaves state, log and pending buffer untouched.
/// Operations the state refuses are not batch failures; see [`crate::merge::Rejection`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MergeError {
    /// The signature does not verify for the claimed author.
    BadSignature { actor: ActorId, seq: u64 },
    /// The timestamp is further ahead of local time than the allowed drift.
    BadTimestamp { actor: ActorId, seq: u64 },
    /// The operation is structurally invalid.
    Malformed {
        actor: ActorId,
        seq: u64,
        reason: &'static str,
    },
    /// Two different operations claim the same `(actor, seq)`.
    Conflict { actor: ActorId, seq: u64 },
    /// The causal pending buffer is full.
    PendingOverflow { limit: usize },
    /// One author holds too many operations waiting for causes.
    PendingActorOverflow { actor: ActorId, limit: usize },
    /// Operations waiting for causes hold too many payload bytes.
    PendingBytesExceeded { limit: usize },
}

/// Transport failures.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransportError {
    /// The link is down.
    Disconnected,
    /// A frame exceeds the maximum frame size.
    FrameTooLarge,
    /// The outbound buffer is full. The frame was not queued, and the link is still up.
    Backpressure,
    /// An I/O error from a std-backed transport.
    Io(String),
}

/// Snapshot persistence failures.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotError {
    /// The snapshot store rejected the write.
    Storage(String),
    /// The snapshot bytes do not decode.
    Malformed,
    /// The decoded state does not hash to the value the snapshot claimed.
    HashMismatch,
}

/// A received frame was dropped. The link stays up.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrameError {
    /// The envelope or message body did not decode.
    Codec(CodecError),
    /// The envelope sender does not match the link, or the envelope signature is invalid.
    Auth,
    /// The merge engine rejected the batch.
    Merge(MergeError),
    /// A timestamp could not be folded into the local clock.
    Clock(ClockError),
    /// A `Pong` did not answer a ping this replica sent.
    UnexpectedPong,
    /// A snapshot could not be installed.
    BadSnapshot(SnapshotError),
}

/// Restoring a replica from an exported log failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RestoreError {
    /// The state supplied does not match the state the log was exported with.
    StateMismatch,
    /// A timestamp in the log is further ahead of the local clock than the allowed drift.
    Clock(ClockError),
}

/// Local operation failures.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EngineError {
    /// The local clock could not issue a timestamp.
    Clock(ClockError),
    /// The local state rejected the operation.
    Apply(ApplyError),
    /// This actor has used up its sequence-number space.
    SequenceExhausted,
    /// The payload or dependency set is larger than remote peers accept. Sending it would make
    /// every peer reject the batch that carries it, so it is refused here.
    OperationTooLarge,
}
