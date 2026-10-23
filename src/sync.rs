//! Delta sync: working out what a peer is missing, batching it, and wrapping frames in signed
//! envelopes.

use alloc::vec::Vec;

use crate::clock::VectorClock;
use crate::codec::{Reader, Writer, MAX_FRAME, MAX_SIGNATURE};
use crate::error::CodecError;
use crate::log::{ReplicatedLog, StampedOperation};
use crate::stubs::crypto::Signer;
use crate::stubs::ActorId;

/// A sync protocol message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// Sender's clock. The receiver replies with the delta the sender lacks, then an [`Message::Ack`].
    Hello(VectorClock),
    /// A batch of operations.
    Delta(Vec<StampedOperation>),
    /// Sender's clock after it has finished sending its delta. The receiver marks the peer up to
    /// date and sends back whatever the sender lacks.
    Ack(VectorClock),
    /// Liveness probe carrying a nonce.
    Ping(u64),
    /// Reply to [`Message::Ping`] with the same nonce.
    Pong(u64),
}

/// Returns every operation in `local_log` that a peer with `peer_clock` has not seen, in
/// timestamp order.
pub fn calculate_delta(
    local_log: &ReplicatedLog,
    peer_clock: &VectorClock,
) -> Vec<StampedOperation> {
    local_log
        .ops()
        .iter()
        .filter(|op| op.seq > peer_clock.get(op.actor))
        .cloned()
        .collect()
}

/// Splits `ops` into batches of at most `max_per_chunk` operations and roughly `max_bytes` of
/// encoded data. A single operation larger than `max_bytes` still gets a chunk of its own.
pub fn chunk_ops(
    ops: &[StampedOperation],
    max_per_chunk: usize,
    max_bytes: usize,
) -> Vec<Vec<StampedOperation>> {
    let max_per_chunk = max_per_chunk.max(1);
    let mut chunks = Vec::new();
    let mut current: Vec<StampedOperation> = Vec::new();
    let mut current_bytes = 0;
    for op in ops {
        // Sizes are estimated from the signed fields plus a small allowance for framing.
        let size = op.signing_bytes().len() + op.signature.len() + 16;
        if !current.is_empty()
            && (current.len() == max_per_chunk || current_bytes + size > max_bytes)
        {
            chunks.push(core::mem::take(&mut current));
            current_bytes = 0;
        }
        current.push(op.clone());
        current_bytes += size;
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

/// A message body with its sender and a signature over the body. This is the batch-level tamper
/// check, on top of the per-operation signatures.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope {
    /// The replica that sent the body.
    pub sender: ActorId,
    /// Encoded [`Message`].
    pub body: Vec<u8>,
    /// Signature over `body`.
    pub sig: Vec<u8>,
}

impl Envelope {
    /// Signs `body` as `sender`.
    pub fn seal(signer: &dyn Signer, sender: ActorId, body: Vec<u8>) -> Self {
        let sig = signer.sign(&body);
        Self { sender, body, sig }
    }

    /// Encodes the envelope for a transport frame.
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.varint(self.sender.0);
        w.bytes(&self.body);
        w.bytes(&self.sig);
        w.finish()
    }

    /// Decodes a transport frame. Does not verify the signature.
    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut r = Reader::new(bytes);
        let sender = ActorId(r.varint()?);
        let body = r.bytes(MAX_FRAME)?;
        let sig = r.bytes(MAX_SIGNATURE)?;
        r.finish()?;
        Ok(Self { sender, body, sig })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::Timestamp;
    use crate::stubs::crypto::KeyedHashSigner;
    use crate::stubs::UirNode;

    fn op(actor: u64, seq: u64) -> StampedOperation {
        StampedOperation {
            timestamp: Timestamp {
                wall_time: seq,
                logical_counter: 0,
                actor_id: ActorId(actor),
            },
            actor: ActorId(actor),
            seq,
            deps: VectorClock::new(),
            op: UirNode {
                bytes: alloc::vec![seq as u8],
            },
            signature: Vec::new(),
        }
    }

    #[test]
    fn chunking_respects_limit() {
        let ops: Vec<_> = (1..=10).map(|s| op(1, s)).collect();
        let chunks = chunk_ops(&ops, 4, usize::MAX);
        assert_eq!(chunks.iter().map(Vec::len).collect::<Vec<_>>(), [4, 4, 2]);
        assert_eq!(chunk_ops(&ops, 0, usize::MAX).len(), 10);
    }

    #[test]
    fn chunking_respects_byte_budget() {
        let mut ops: Vec<_> = (1..=3).map(|s| op(1, s)).collect();
        ops[0].op.bytes = alloc::vec![0; 1000];
        let one = ops[0].signing_bytes().len() + 16;
        // The large first op fills the budget alone. The two small ones share the next chunk.
        let chunks = chunk_ops(&ops, 128, one);
        assert_eq!(chunks.iter().map(Vec::len).collect::<Vec<_>>(), [1, 2]);
        // An op bigger than the whole budget still goes out, alone.
        assert_eq!(chunk_ops(&ops, 128, 1).len(), 3);
    }

    #[test]
    fn envelope_roundtrip_and_tamper_detection() {
        let signer = KeyedHashSigner::new(ActorId(7), 99);
        let env = Envelope::seal(&signer, ActorId(7), alloc::vec![1, 2, 3]);
        let decoded = Envelope::decode(&env.encode()).unwrap();
        assert_eq!(decoded, env);
        let verifier = crate::stubs::crypto::KeyedHashVerifier::new(99);
        use crate::stubs::crypto::Verifier;
        assert!(verifier.verify(ActorId(7), &decoded.body, &decoded.sig));
        assert!(!verifier.verify(ActorId(8), &decoded.body, &decoded.sig));
    }
}
