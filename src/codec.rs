//! Wire encoding. Contains the fixed binary layout used for signing and envelopes, the pluggable
//! [`Codec`] trait for message bodies, and a compressing wrapper.

use alloc::vec::Vec;

use crate::clock::{Timestamp, VectorClock};
use crate::error::CodecError;
use crate::log::StampedOperation;
use crate::stubs::{ActorId, UirNode};
use crate::sync::{Message, SnapshotHeader};
use alloc::boxed::Box;

/// Largest accepted operation payload, in bytes.
pub const MAX_PAYLOAD: usize = 1 << 20;
/// Largest accepted signature, in bytes.
pub const MAX_SIGNATURE: usize = 1024;
/// Largest accepted number of entries in one vector clock.
pub const MAX_DEPS: usize = 4096;
/// Largest accepted number of operations in one message.
pub const MAX_OPS_PER_MESSAGE: usize = 4096;
/// Largest accepted frame or decompressed body, in bytes.
pub const MAX_FRAME: usize = 1 << 24;

const TAG_HELLO: u8 = 0;
const TAG_DELTA: u8 = 1;
const TAG_ACK: u8 = 2;
const TAG_PING: u8 = 3;
const TAG_PONG: u8 = 4;
const TAG_SNAPSHOT_BEGIN: u8 = 5;
const TAG_SNAPSHOT_CHUNK: u8 = 6;

/// Encodes and decodes message bodies. Implementations must round-trip exactly and must return
/// an error, not panic, on malformed input.
pub trait Codec {
    /// Serialises a message body.
    fn encode(&self, msg: &Message) -> Vec<u8>;
    /// Parses a message body.
    fn decode(&self, bytes: &[u8]) -> Result<Message, CodecError>;
}

/// Default pure-Rust binary codec: LEB128 varints and length-prefixed byte strings.
#[derive(Clone, Copy, Debug, Default)]
pub struct BinaryCodec;

impl Codec for BinaryCodec {
    fn encode(&self, msg: &Message) -> Vec<u8> {
        let mut w = Writer::new();
        match msg {
            Message::Hello(clock) => {
                w.u8(TAG_HELLO);
                w.clock(clock);
            }
            Message::Delta(ops) => {
                w.u8(TAG_DELTA);
                w.varint(ops.len() as u64);
                for op in ops {
                    w.signed_op(op);
                }
            }
            Message::Ack(clock) => {
                w.u8(TAG_ACK);
                w.clock(clock);
            }
            Message::Ping(nonce) => {
                w.u8(TAG_PING);
                w.varint(*nonce);
            }
            Message::Pong(nonce) => {
                w.u8(TAG_PONG);
                w.varint(*nonce);
            }
            Message::SnapshotChunk(bytes) => {
                w.u8(TAG_SNAPSHOT_CHUNK);
                w.bytes(bytes);
            }
            Message::SnapshotBegin(snapshot) => {
                w.u8(TAG_SNAPSHOT_BEGIN);
                w.varint(snapshot.state_len as u64);
                w.varint(snapshot.state_hash);
                w.clock(&snapshot.clock);
                w.clock(&snapshot.compacted);
                match &snapshot.floor {
                    Some(floor) => {
                        w.u8(1);
                        w.timestamp(floor);
                    }
                    None => w.u8(0),
                }
            }
        }
        w.finish()
    }

    fn decode(&self, bytes: &[u8]) -> Result<Message, CodecError> {
        let mut r = Reader::new(bytes);
        let message = match r.u8()? {
            TAG_HELLO => Message::Hello(r.clock()?),
            TAG_DELTA => {
                let count = r.count(MAX_OPS_PER_MESSAGE)?;
                let mut ops = Vec::with_capacity(count);
                for _ in 0..count {
                    ops.push(r.signed_op()?);
                }
                Message::Delta(ops)
            }
            TAG_ACK => Message::Ack(r.clock()?),
            TAG_PING => Message::Ping(r.varint()?),
            TAG_PONG => Message::Pong(r.varint()?),
            TAG_SNAPSHOT_CHUNK => Message::SnapshotChunk(r.bytes(MAX_FRAME)?),
            TAG_SNAPSHOT_BEGIN => {
                let state_len = usize::try_from(r.varint()?).map_err(|_| CodecError::TooLarge)?;
                let state_hash = r.varint()?;
                let clock = r.clock()?;
                let compacted = r.clock()?;
                let floor = match r.u8()? {
                    0 => None,
                    1 => Some(r.timestamp()?),
                    other => return Err(CodecError::InvalidTag(other)),
                };
                Message::SnapshotBegin(Box::new(SnapshotHeader {
                    state_len,
                    state_hash,
                    clock,
                    compacted,
                    floor,
                }))
            }
            other => return Err(CodecError::InvalidTag(other)),
        };
        r.finish()?;
        Ok(message)
    }
}

/// Wraps another codec and compresses its output with PackBits run-length encoding.
#[derive(Clone, Debug, Default)]
pub struct Compressed<C: Codec> {
    inner: C,
}

impl<C: Codec> Compressed<C> {
    /// Wraps `inner`.
    pub fn new(inner: C) -> Self {
        Self { inner }
    }
}

impl<C: Codec> Codec for Compressed<C> {
    fn encode(&self, msg: &Message) -> Vec<u8> {
        compress(&self.inner.encode(msg))
    }

    fn decode(&self, bytes: &[u8]) -> Result<Message, CodecError> {
        self.inner.decode(&decompress(bytes)?)
    }
}

/// PackBits: a control byte `0..=127` copies the next `n+1` bytes, `129..=255` repeats the next
/// byte `257-n` times, and `128` is a no-op.
fn compress(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < input.len() {
        let mut run = 1;
        while i + run < input.len() && input[i + run] == input[i] && run < 128 {
            run += 1;
        }
        if run >= 2 {
            out.push((257 - run) as u8);
            out.push(input[i]);
            i += run;
            continue;
        }
        let start = i;
        let mut len = 0;
        while i < input.len() && len < 128 && !(i + 1 < input.len() && input[i + 1] == input[i]) {
            i += 1;
            len += 1;
        }
        out.push((len - 1) as u8);
        out.extend_from_slice(&input[start..i]);
    }
    out
}

fn decompress(input: &[u8]) -> Result<Vec<u8>, CodecError> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < input.len() {
        let control = input[i];
        i += 1;
        match control {
            0..=127 => {
                let end = i
                    .checked_add(usize::from(control) + 1)
                    .filter(|end| *end <= input.len())
                    .ok_or(CodecError::Truncated)?;
                out.extend_from_slice(&input[i..end]);
                i = end;
            }
            128 => {}
            _ => {
                let byte = *input.get(i).ok_or(CodecError::Truncated)?;
                i += 1;
                out.resize(out.len() + (257 - usize::from(control)), byte);
            }
        }
        if out.len() > MAX_FRAME {
            return Err(CodecError::TooLarge);
        }
    }
    Ok(out)
}

/// Append-only encoder for the crate's fixed binary layout.
pub(crate) struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    pub(crate) fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub(crate) fn finish(self) -> Vec<u8> {
        self.buf
    }

    pub(crate) fn u8(&mut self, value: u8) {
        self.buf.push(value);
    }

    pub(crate) fn varint(&mut self, mut value: u64) {
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                self.buf.push(byte);
                return;
            }
            self.buf.push(byte | 0x80);
        }
    }

    pub(crate) fn bytes(&mut self, bytes: &[u8]) {
        self.varint(bytes.len() as u64);
        self.buf.extend_from_slice(bytes);
    }

    pub(crate) fn timestamp(&mut self, ts: &Timestamp) {
        self.varint(ts.wall_time);
        self.varint(u64::from(ts.logical_counter));
        self.varint(ts.actor_id.0);
    }

    pub(crate) fn clock(&mut self, clock: &VectorClock) {
        self.varint(clock.peers.len() as u64);
        for (actor, seq) in &clock.peers {
            self.varint(actor.0);
            self.varint(*seq);
        }
    }

    /// Writes every signed field except the signature.
    pub(crate) fn unsigned_op(&mut self, op: &StampedOperation) {
        self.timestamp(&op.timestamp);
        self.varint(op.actor.0);
        self.varint(op.seq);
        self.clock(&op.deps);
        self.bytes(&op.op.bytes);
    }

    pub(crate) fn signed_op(&mut self, op: &StampedOperation) {
        self.unsigned_op(op);
        self.bytes(&op.signature);
    }
}

/// Bounds-checked decoder for the crate's fixed binary layout.
pub(crate) struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub(crate) fn u8(&mut self) -> Result<u8, CodecError> {
        let byte = *self.buf.get(self.pos).ok_or(CodecError::Truncated)?;
        self.pos += 1;
        Ok(byte)
    }

    pub(crate) fn varint(&mut self) -> Result<u64, CodecError> {
        let mut value = 0u64;
        let mut shift = 0u32;
        loop {
            let byte = self.u8()?;
            if shift >= 64 {
                return Err(CodecError::VarintOverflow);
            }
            let part = u64::from(byte & 0x7f);
            if shift == 63 && part > 1 {
                return Err(CodecError::VarintOverflow);
            }
            value |= part << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
        }
    }

    fn u32(&mut self) -> Result<u32, CodecError> {
        u32::try_from(self.varint()?).map_err(|_| CodecError::VarintOverflow)
    }

    /// Reads an element count. Rejects counts above `max` or above the bytes that remain, since
    /// every element occupies at least one byte.
    pub(crate) fn count(&mut self, max: usize) -> Result<usize, CodecError> {
        let n = usize::try_from(self.varint()?).map_err(|_| CodecError::TooLarge)?;
        if n > max || n > self.remaining() {
            return Err(CodecError::TooLarge);
        }
        Ok(n)
    }

    pub(crate) fn bytes(&mut self, max: usize) -> Result<Vec<u8>, CodecError> {
        let n = self.count(max)?;
        let buf = self.buf;
        let slice = &buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice.to_vec())
    }

    pub(crate) fn timestamp(&mut self) -> Result<Timestamp, CodecError> {
        Ok(Timestamp {
            wall_time: self.varint()?,
            logical_counter: self.u32()?,
            actor_id: ActorId(self.varint()?),
        })
    }

    pub(crate) fn clock(&mut self) -> Result<VectorClock, CodecError> {
        let n = self.count(MAX_DEPS)?;
        let mut clock = VectorClock::new();
        for _ in 0..n {
            let actor = ActorId(self.varint()?);
            let seq = self.varint()?;
            clock.observe(actor, seq);
        }
        Ok(clock)
    }

    fn unsigned_op(&mut self) -> Result<StampedOperation, CodecError> {
        let timestamp = self.timestamp()?;
        let actor = ActorId(self.varint()?);
        let seq = self.varint()?;
        let deps = self.clock()?;
        let op = UirNode {
            bytes: self.bytes(MAX_PAYLOAD)?,
        };
        Ok(StampedOperation {
            timestamp,
            actor,
            seq,
            deps,
            op,
            signature: Vec::new(),
        })
    }

    pub(crate) fn signed_op(&mut self) -> Result<StampedOperation, CodecError> {
        let mut op = self.unsigned_op()?;
        op.signature = self.bytes(MAX_SIGNATURE)?;
        Ok(op)
    }

    pub(crate) fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub(crate) fn finish(&self) -> Result<(), CodecError> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err(CodecError::TrailingBytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packbits_roundtrip_edge_cases() {
        let cases: [&[u8]; 5] = [
            b"",
            b"a",
            b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            b"abcdefg",
            b"aabbccddeeffgg",
        ];
        for case in cases {
            assert_eq!(decompress(&compress(case)).unwrap(), case);
        }
    }

    #[test]
    fn decompress_rejects_truncated_input() {
        assert_eq!(decompress(&[5, 1, 2]), Err(CodecError::Truncated));
        assert_eq!(decompress(&[255]), Err(CodecError::Truncated));
    }

    #[test]
    fn varint_overflow_is_rejected() {
        let bytes = [0xff; 11];
        assert_eq!(
            Reader::new(&bytes).varint(),
            Err(CodecError::VarintOverflow)
        );
    }

    #[test]
    fn message_roundtrip_and_trailing_bytes() {
        let codec = BinaryCodec;
        let mut clock = VectorClock::new();
        clock.observe(ActorId(3), 9);
        let msgs = [
            Message::Hello(clock.clone()),
            Message::Ack(clock),
            Message::Ping(42),
            Message::Pong(u64::MAX),
            Message::Delta(Vec::new()),
        ];
        for msg in msgs {
            let bytes = codec.encode(&msg);
            assert_eq!(codec.decode(&bytes), Ok(msg.clone()));
            let compressed = Compressed::new(BinaryCodec);
            assert_eq!(compressed.decode(&compressed.encode(&msg)), Ok(msg));
            let mut extra = bytes.clone();
            extra.push(0);
            assert_eq!(codec.decode(&extra), Err(CodecError::TrailingBytes));
        }
    }
}
