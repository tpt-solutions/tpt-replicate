//! Stand-in for tpt-crypto.
//!
//! **NOT CRYPTOGRAPHICALLY SECURE.** The "signature" is a keyed 64-bit non-cryptographic hash.
//! It exercises the sign and verify plumbing and detects accidental or simple tampering in
//! tests. It does not resist forgery. Replace it with real signatures before any untrusted
//! deployment.

use alloc::vec::Vec;

use crate::stubs::ActorId;

/// Opaque signature bytes.
pub type Signature = Vec<u8>;

/// Signs messages on behalf of one actor.
pub trait Signer {
    /// Signs `msg`.
    fn sign(&self, msg: &[u8]) -> Signature;
}

/// Verifies signatures from any actor.
pub trait Verifier {
    /// Returns `true` if `sig` is a valid signature of `msg` by `actor`.
    fn verify(&self, actor: ActorId, msg: &[u8], sig: &[u8]) -> bool;
}

fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn derive_key(secret: u64, actor: ActorId) -> u64 {
    mix(secret ^ mix(actor.0))
}

fn mac(key: u64, msg: &[u8]) -> Signature {
    let mut h = mix(key) ^ 0xcbf2_9ce4_8422_2325;
    for &byte in msg {
        h ^= u64::from(byte);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    mix(h).to_le_bytes().to_vec()
}

/// Test-only signer. Derives a per-actor key from a shared secret.
#[derive(Clone, Debug)]
pub struct KeyedHashSigner {
    key: u64,
}

impl KeyedHashSigner {
    /// Creates the signer for `actor` under `secret`.
    pub fn new(actor: ActorId, secret: u64) -> Self {
        Self {
            key: derive_key(secret, actor),
        }
    }
}

impl Signer for KeyedHashSigner {
    fn sign(&self, msg: &[u8]) -> Signature {
        mac(self.key, msg)
    }
}

/// Test-only verifier matching [`KeyedHashSigner`].
#[derive(Clone, Debug)]
pub struct KeyedHashVerifier {
    secret: u64,
}

impl KeyedHashVerifier {
    /// Creates a verifier for the shared `secret`.
    pub fn new(secret: u64) -> Self {
        Self { secret }
    }
}

impl Verifier for KeyedHashVerifier {
    fn verify(&self, actor: ActorId, msg: &[u8], sig: &[u8]) -> bool {
        mac(derive_key(self.secret, actor), msg).as_slice() == sig
    }
}
