use serde::{Deserialize, Serialize};

use crate::{pbft_state::ReplicaState, ClientRequest, ProtocolMessage, SignedPrePrepare};

pub const REPLICA_ID_HEADER: &str = "pbft-replica-id";
pub const REPLICA_SIGNATURE_HEADER: &str = "pbft-signature-id";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientRequestBroadcast {
    pub request: ClientRequest,
    pub sequence_number: u64,
    pub pre_prepare: SignedPrePrepare,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolMessageBroadcast {
    pub message: ProtocolMessage,
}

impl ProtocolMessageBroadcast {
    pub fn new(message: ProtocolMessage) -> Self {
        Self { message }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PbftNodeState {
    pub replica_state: ReplicaState,
    pub view: u64,
    pub last_applied: u64,
    pub last_stable_checkpoint_sequence: Option<u64>,
    // TODO: extend if needed
    // TODO: make it possible to dump log messages too - with sequence, view etc.
}
