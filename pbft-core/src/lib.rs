use std::{collections::HashMap, ops::Deref};

use crate::config::NodeId;
use ed25519_dalek::{Signer, Verifier};
use serde::{Deserialize, Serialize};

pub mod api;
pub mod broadcast;
pub mod config;
pub mod error;
pub(crate) mod message_store;
pub mod pbft;
pub mod pbft_executor;
pub(crate) mod pbft_state;
pub mod state_machine;

pub use crate::config::Config;
pub use pbft::Pbft;

pub use pbft_state::ReplicaState;

pub use state_machine::InMemoryKVStore;

pub const NULL_DIGEST: MessageDigest = MessageDigest([0; 16]);

#[cfg(test)]
mod pbft_tests;

#[cfg(test)]
pub mod test_util;

pub mod dev;

pub type Result<T> = std::result::Result<T, error::Error>;

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub enum Operation {
    Noop, // Used by the protocol
    Set { key: String, value: String },
    Get { key: String },
}

impl Operation {
    pub fn matches_result(&self, result: &OperationResult) -> bool {
        match (self, result) {
            (Operation::Noop, OperationResult::Noop) => true,
            (Operation::Set { key, value }, OperationResult::Set { key: k, value: v }) => {
                key == k && value == v
            }
            (Operation::Get { key }, OperationResult::Get { key: k, value: _ }) => key == k,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum OperationResult {
    Set { key: String, value: String },
    Get { key: String, value: Option<String> },
    Noop,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct OperationResultSequenced {
    pub result: OperationResult,
    /// Sequence number of the operation assigned by the pbft leader when
    /// initially accepting the operation
    pub sequence_number: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct ClientRequestId(pub uuid::Uuid);

impl ClientRequestId {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }
}

impl Default for ClientRequestId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ClientRequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct ClientRequest {
    pub request_id: ClientRequestId,
    pub operation: Operation,
}

impl ClientRequest {
    pub fn digest(&self) -> MessageDigest {
        let serialized = serde_json::to_string(&self).unwrap();
        let digest = md5::compute(serialized);
        MessageDigest(digest.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct AcceptedRequest {
    pub sequence: u64,
    pub request: ClientRequest,
    pub result: Option<OperationResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationAck {
    pub client_request: ClientRequest,
    pub leader_id: u64,
    pub sequence_number: u64,
    pub status: OperationStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperationStatus {
    Accepted,
    AlreadyHandled(Option<OperationResult>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientResponse {
    pub request: ClientRequest,
    pub result: OperationResultSequenced,
}

pub trait ReplicaId {
    fn replica_id(&self) -> NodeId;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProtocolMessage {
    // TODO: Not all of those need to be signed, so maybe it does not make sense
    // to make them as such
    PrePrepare(SignedPrePrepare),
    Prepare(SignedPrepare),
    Commit(SignedCommit),
    Checkpoint(SignedCheckpoint),
    ViewChange(SignedViewChange),
    NewView(SignedNewView),
}

impl ProtocolMessage {
    pub fn new_preare(
        meta: MessageMeta,
        replica_id: NodeId,
        keypair: &ed25519_dalek::Keypair,
    ) -> Result<Self> {
        let prepare = Prepare {
            replica_id,
            metadata: meta,
        };
        Ok(ProtocolMessage::Prepare(SignedPrepare::new(
            prepare, keypair,
        )?))
    }

    pub fn new_commit(
        meta: MessageMeta,
        replica_id: NodeId,
        keypair: &ed25519_dalek::Keypair,
    ) -> Result<Self> {
        let commit = Commit {
            replica_id,
            metadata: meta,
        };
        Ok(ProtocolMessage::Commit(SignedCommit::new(commit, keypair)?))
    }

    pub fn message_type_str(&self) -> &'static str {
        match self {
            ProtocolMessage::PrePrepare(_) => "PrePrepare",
            ProtocolMessage::Prepare(_) => "Prepare",
            ProtocolMessage::Commit(_) => "Commit",
            ProtocolMessage::Checkpoint(_) => "Checkpoint",
            ProtocolMessage::ViewChange(_) => "ViewChange",
            ProtocolMessage::NewView(_) => "NewView",
        }
    }

    pub fn in_view(&self) -> Option<u64> {
        match self {
            ProtocolMessage::PrePrepare(m) => Some(m.metadata.view),
            ProtocolMessage::Prepare(m) => Some(m.metadata.view),
            ProtocolMessage::Commit(m) => Some(m.metadata.view),
            ProtocolMessage::Checkpoint(_m) => None,
            ProtocolMessage::ViewChange(_m) => None,
            ProtocolMessage::NewView(_m) => None,
        }
    }

    pub fn sequence(&self) -> Option<u64> {
        match self {
            ProtocolMessage::PrePrepare(m) => Some(m.metadata.sequence),
            ProtocolMessage::Prepare(m) => Some(m.metadata.sequence),
            ProtocolMessage::Commit(m) => Some(m.metadata.sequence),
            ProtocolMessage::Checkpoint(m) => Some(m.sequence),
            ProtocolMessage::ViewChange(_m) => None,
            ProtocolMessage::NewView(_m) => None,
        }
    }

    pub fn replica_id(&self) -> Option<NodeId> {
        match self {
            ProtocolMessage::PrePrepare(_m) => None,
            ProtocolMessage::Prepare(m) => Some(m.replica_id),
            ProtocolMessage::Commit(m) => Some(m.replica_id),
            ProtocolMessage::Checkpoint(m) => Some(m.replica_id),
            ProtocolMessage::ViewChange(m) => Some(m.replica_id),
            ProtocolMessage::NewView(_m) => None,
        }
    }

    pub fn verify_signature(&self, replica_id: NodeId) -> Result<bool> {
        if let Some(msg_replica_id) = self.replica_id() {
            if msg_replica_id != replica_id {
                return Err(error::Error::InvalidMessageSignatureReplicaIdMismatch {
                    expected: replica_id.0,
                    actual: msg_replica_id.0,
                });
            }
        }

        // TODO: Macro to extract the inner?
        match self {
            ProtocolMessage::PrePrepare(m) => m.verify(),
            ProtocolMessage::Prepare(m) => m.verify(),
            ProtocolMessage::Commit(m) => m.verify(),
            ProtocolMessage::Checkpoint(m) => m.verify(),
            ProtocolMessage::ViewChange(m) => m.verify(),
            ProtocolMessage::NewView(m) => m.verify(),
        }
    }

    pub fn is_new_view(&self) -> bool {
        matches!(self, ProtocolMessage::NewView(_))
    }
}

// TODO: This could be a macro, or proc macro
impl ReplicaId for Prepare {
    fn replica_id(&self) -> NodeId {
        self.replica_id
    }
}
impl ReplicaId for Commit {
    fn replica_id(&self) -> NodeId {
        self.replica_id
    }
}
impl ReplicaId for Checkpoint {
    fn replica_id(&self) -> NodeId {
        self.replica_id
    }
}
impl ReplicaId for ViewChange {
    fn replica_id(&self) -> NodeId {
        self.replica_id
    }
}

impl From<SignedPrepare> for ProtocolMessage {
    fn from(p: SignedPrepare) -> Self {
        ProtocolMessage::Prepare(p)
    }
}
impl From<SignedCommit> for ProtocolMessage {
    fn from(p: SignedCommit) -> Self {
        ProtocolMessage::Commit(p)
    }
}
impl From<SignedCheckpoint> for ProtocolMessage {
    fn from(p: SignedCheckpoint) -> Self {
        ProtocolMessage::Checkpoint(p)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageDigest(pub [u8; 16]);

impl std::fmt::Display for MessageDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageMeta {
    pub view: u64,
    pub sequence: u64,
    pub digest: MessageDigest,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrePrepare {
    pub metadata: MessageMeta,
}

impl PrePrepare {
    pub fn is_null(&self) -> bool {
        self.metadata.digest == NULL_DIGEST
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Prepare {
    pub replica_id: NodeId,
    pub metadata: MessageMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Commit {
    pub replica_id: NodeId,
    pub metadata: MessageMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CheckpointDigest(pub [u8; 16]);

impl std::fmt::Display for CheckpointDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub replica_id: NodeId,
    pub sequence: u64,
    pub digest: CheckpointDigest,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ViewChange {
    pub replica_id: NodeId,
    pub view: u64,

    // Last stable checkpoint for given replica. It is an Option in case we do
    // not have any checkpoints yet.
    pub last_stable_checkpoint: Option<ViewChangeCheckpoint>,

    // Proof for each prepared message (by sequence), containing at least 2f+1
    // Prepare messages from different replicas for a given message.
    // Each proof contains the pre-prepare message and the prepare messages by
    // public key of different replicas.
    pub prepared_proofs: HashMap<u64, PreparedProof>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ViewChangeCheckpoint {
    pub sequence: u64,
    pub digest: CheckpointDigest,
    // Map public key to checkpoint signed checkpoint message by the given
    // replica.
    pub checkpoint_proofs: HashMap<String, SignedCheckpoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PreparedProof {
    pub pre_prepare: SignedPrePrepare,
    pub prepares: HashMap<String, SignedPrepare>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewView {
    pub view: u64,
    // Proof of View Change messages received from different replicas
    pub view_change_messages: HashMap<String, SignedViewChange>,
    // Pre-prepare messages for those that were prepared in previous view, but
    // were not included in the last stable checkpoint.
    pub pre_prepares: Vec<SignedPrePrepare>,
}

impl NewView {
    pub fn latest_sequence(&self) -> u64 {
        self.pre_prepares
            .iter()
            .map(|pp| pp.metadata.sequence)
            .max()
            // We expect the vaule to be there since we always have at least one
            // PrePrepare message with NULL digest.
            .unwrap()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedMessage<T> {
    pub message: T,
    pub signature: Vec<u8>,
    pub pub_key: [u8; 32],
}

impl<T: Serialize> SignedMessage<T> {
    pub fn new(message: T, keypair: &ed25519_dalek::Keypair) -> Result<Self> {
        let serialized = serde_json::to_string(&message).map_err(
            crate::error::Error::serde_json_error("failed to serialize message"),
        )?;
        let signature = keypair.sign(serialized.as_bytes()).to_bytes().to_vec();
        Ok(Self {
            message,
            signature,
            pub_key: keypair.public.to_bytes(),
        })
    }

    pub fn verify(&self) -> Result<bool> {
        let serialized = serde_json::to_string(&self.message).map_err(
            crate::error::Error::serde_json_error("failed to serialize message"),
        )?;

        let pub_key = ed25519_dalek::PublicKey::from_bytes(&self.pub_key).map_err(
            crate::error::Error::ed25519_error("failed to parse public key from bytes"),
        )?;

        let signature = &ed25519_dalek::Signature::from_bytes(&self.signature).map_err(
            crate::error::Error::ed25519_error("failed to parse signature from bytes"),
        )?;

        Ok(pub_key.verify(serialized.as_bytes(), signature).is_ok())
    }

    pub fn pub_key_hex(&self) -> String {
        hex::encode(self.pub_key)
    }
}

impl<T: Serialize + ReplicaId> SignedMessage<T> {
    pub fn verify_replica_signature(&self, replica_id: NodeId) -> Result<bool> {
        if self.replica_id() != replica_id {
            return Err(error::Error::InvalidMessageSignatureReplicaIdMismatch {
                expected: replica_id.0,
                actual: self.replica_id().0,
            });
        }
        self.verify()
    }
}

impl<T> Deref for SignedMessage<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.message
    }
}

pub type SignedPrePrepare = SignedMessage<PrePrepare>;
pub type SignedPrepare = SignedMessage<Prepare>;
pub type SignedCommit = SignedMessage<Commit>;
pub type SignedCheckpoint = SignedMessage<Checkpoint>;
pub type SignedViewChange = SignedMessage<ViewChange>;
pub type SignedNewView = SignedMessage<NewView>;

pub trait SignMessage<T: Serialize> {
    fn sign(self, keypair: &ed25519_dalek::Keypair) -> Result<SignedMessage<T>>;
}

impl<T: Serialize> SignMessage<T> for T {
    fn sign(self, keypair: &ed25519_dalek::Keypair) -> Result<SignedMessage<T>> {
        SignedMessage::new(self, keypair)
    }
}
