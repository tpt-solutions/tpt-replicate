//! Ed25519 signing and verification backed by `tpt-crypto`, behind the `ed25519` feature.
//!
//! [`Ed25519Signer`] signs as one replica. [`Ed25519Directory`] maps each actor to its public key and
//! verifies frames from them. A frame from an actor with no registered key is refused, which is the
//! safe default. The stub in [`crate::stubs::crypto`] is for tests only.

use alloc::collections::BTreeMap;
use alloc::vec::Vec;

use tpt_crypto_sig::ed25519::{Signature, SigningKey, VerifyingKey};

use crate::stubs::crypto::{Signer, Verifier};
use crate::stubs::ActorId;

/// Signs messages with an Ed25519 key.
pub struct Ed25519Signer {
    key: SigningKey,
}

impl Ed25519Signer {
    /// Builds a signer from a 32-byte seed. Keep the seed secret and generate it from a good random
    /// source.
    pub fn from_seed(seed: [u8; 32]) -> Self {
        Self {
            key: SigningKey::from_seed(seed),
        }
    }

    /// The matching public key, to register with the other replicas' directories.
    pub fn public_key(&self) -> [u8; 32] {
        self.key.verifying_key().to_bytes()
    }
}

impl Signer for Ed25519Signer {
    fn sign(&self, msg: &[u8]) -> Vec<u8> {
        self.key.sign(msg).to_bytes().to_vec()
    }
}

/// The public keys of known actors. Verifies a frame only if its author is registered and the
/// signature matches that author's key.
#[derive(Clone, Debug, Default)]
pub struct Ed25519Directory {
    keys: BTreeMap<ActorId, VerifyingKey>,
}

impl Ed25519Directory {
    /// Creates an empty directory.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `actor`'s public key. Returns `false` if the bytes are not a valid public key.
    pub fn register(&mut self, actor: ActorId, public_key: &[u8; 32]) -> bool {
        match VerifyingKey::from_bytes(public_key) {
            Ok(key) => {
                self.keys.insert(actor, key);
                true
            }
            Err(_) => false,
        }
    }
}

impl Verifier for Ed25519Directory {
    fn verify(&self, actor: ActorId, msg: &[u8], sig: &[u8]) -> bool {
        let Some(key) = self.keys.get(&actor) else {
            return false;
        };
        let Ok(bytes) = <[u8; 64]>::try_from(sig) else {
            return false;
        };
        key.verify(msg, &Signature::from_bytes(bytes)).is_ok()
    }
}
