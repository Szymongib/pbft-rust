use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use tracing::info;

use crate::{
    config::NodeId,
    message_store::MessageStore,
    pbft_executor::{quorum_size, view_leader},
    Checkpoint, CheckpointDigest, MessageDigest, MessageMeta, SignedCheckpoint, SignedCommit,
    SignedPrePrepare, SignedPrepare, SignedViewChange,
};

/// ConsensusLog is a map of a combination of view and sequence numbers. It
/// determines a state of a consensus for a specific request.
type ConsensusLog = BTreeMap<ConsensusLogIdx, RequestConsensusState>;
/// ViewChangeLog is a map of view number to a list of view change messages
type ViewChangeLog = BTreeMap<u64, Vec<SignedViewChange>>;
/// ConsensusLog is a map of sequence number to a checkpoint consensus state.
/// It keeps track of Checkpoint messages for specific sequence numbers.
type CheckpointLog = BTreeMap<u64, CheckpointConsensusState>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaState {
    Replica,
    // Only the leader needs to keep track of the latest sequence number, other
    // replicas realy on watermarks.
    Leader { sequence: u64 },
    ViewChange,
}

pub struct PbftState {
    pub(crate) replica_state: ReplicaState,
    pub(crate) view: u64,

    pub(crate) high_watermark: u64,
    pub(crate) low_watermark: u64,
    /// watermark_k defines the range between low and high watermarks
    pub(crate) watermark_k: u64,

    pub(crate) last_applied_seq: u64,

    pub(crate) message_store: MessageStore,
    pub(crate) consensus_log: ConsensusLog,
    pub(crate) checkpoint_log: CheckpointLog,

    pub(crate) view_change_log: ViewChangeLog,

    pub(crate) timer: Option<ViewChangeTimer>,

    // We are going to store checkpoints in JSON format so that we can easily
    // take digest of them. In a real system, they would also not live in memory
    // but rather be stored on disk. Also for that reason we separate
    // checkpoints and their digests.
    pub(crate) checkpoints: BTreeMap<u64, String>,
    pub(crate) checkpoint_digests: BTreeMap<u64, CheckpointDigest>,
}

impl PbftState {
    pub fn new(nodes_count: u64, self_id: NodeId, watermark_k: u64) -> Self {
        // TODO: Initialize those and store in persistant storage

        let view = 1;
        let replica_state = if view_leader(nodes_count, view) == self_id.0 {
            ReplicaState::Leader { sequence: 0 }
        } else {
            ReplicaState::Replica
        };
        info!(state=?replica_state, "initializing PBFT State");

        Self {
            replica_state,
            view,
            high_watermark: watermark_k,
            low_watermark: 0,
            watermark_k,
            last_applied_seq: 0,
            message_store: MessageStore::new(),
            consensus_log: BTreeMap::new(),
            checkpoint_log: BTreeMap::new(),
            view_change_log: BTreeMap::new(),

            timer: None,

            checkpoints: BTreeMap::new(),
            checkpoint_digests: BTreeMap::new(),
        }
    }

    pub fn set_watermarks(&mut self, low: u64) {
        if self.low_watermark > low {
            return;
        }
        self.low_watermark = low;
        self.high_watermark = low + self.watermark_k;
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct ConsensusLogIdx {
    pub view: u64,
    pub sequence: u64,
}

impl PartialOrd for ConsensusLogIdx {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.sequence.cmp(&other.sequence))
    }
}

impl Ord for ConsensusLogIdx {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.sequence.cmp(&other.sequence) {
            std::cmp::Ordering::Equal => self.view.cmp(&other.view),
            o => o,
        }
    }
}

impl From<&MessageMeta> for ConsensusLogIdx {
    fn from(msg: &MessageMeta) -> Self {
        Self {
            view: msg.view,
            sequence: msg.sequence,
        }
    }
}

pub struct RequestConsensusState {
    pub digest: MessageDigest,
    pub pre_prepare: Option<SignedPrePrepare>,
    pub prepare: Vec<SignedPrepare>,
    pub commit: Vec<SignedCommit>,

    // Those will be flipped to true when the state is reached initially, so
    // that the replica does not broadcast the same message multiple times.
    pub reported_prepared: bool,
    pub reported_committed_local: bool,
}

impl RequestConsensusState {
    pub fn new(meta: &MessageMeta) -> Self {
        Self {
            digest: meta.digest.clone(),
            pre_prepare: None,
            prepare: Vec::new(),
            commit: Vec::new(),

            reported_prepared: false,
            reported_committed_local: false,
        }
    }

    pub fn is_prepared(&self, replicas_count: usize) -> bool {
        self.prepare.len() > quorum_size(replicas_count) && self.pre_prepare.is_some()
    }

    pub fn is_committed_local(&self, replicas_count: usize) -> bool {
        self.is_prepared(replicas_count) && self.commit.len() >= quorum_size(replicas_count)
    }
}

pub struct CheckpointConsensusState {
    pub digest: CheckpointDigest,
    pub sequence: u64,
    // Use HashMap to store proofs by digest so that malicious peers can't
    // lead to digeset mismatch.
    pub proofs: HashMap<CheckpointDigest, Vec<SignedCheckpoint>>,
}

impl CheckpointConsensusState {
    pub fn new(data: &Checkpoint) -> Self {
        Self {
            digest: data.digest.clone(),
            sequence: data.sequence,
            proofs: HashMap::new(),
        }
    }

    pub fn is_stable(&self, nodes_count: usize) -> bool {
        let proofs_count = self.proofs.get(&self.digest).map(|v| v.len()).unwrap_or(0);

        proofs_count >= quorum_size(nodes_count)
    }
}

pub struct ViewChangeTimer {
    pub trigger_digest: MessageDigest,
    pub task: tokio::task::JoinHandle<()>,
}
