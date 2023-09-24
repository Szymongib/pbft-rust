use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex, RwLock},
};

use tracing::{debug, error, info, warn};

use crate::{
    api::{ClientRequestBroadcast, ProtocolMessageBroadcast},
    broadcast::PbftBroadcaster,
    config::NodeId,
    error::Error,
    pbft_state::{
        CheckpointConsensusState, ConsensusLogIdx, PbftState, ReplicaState, RequestConsensusState,
        ViewChangeTimer,
    },
    state_machine::StateMachie,
    AcceptedRequest, Checkpoint, CheckpointDigest, ClientRequest, ClientResponse, Commit, Config,
    MessageDigest, MessageMeta, NewView, OperationResultSequenced, PrePrepare, Prepare,
    PreparedProof, ProtocolMessage, Result, SignMessage, SignedCheckpoint, SignedCommit,
    SignedNewView, SignedPrePrepare, SignedPrepare, SignedViewChange, ViewChange,
    ViewChangeCheckpoint, NULL_DIGEST,
};

pub enum ClientRequestResult {
    NotLeader(NotLeader),
    Accepted(u64),
    AlreadyAccepted(HandledRequest),
}

pub struct NotLeader {
    pub leader_id: NodeId,
}

pub struct HandledRequest {
    pub request: AcceptedRequest,
    pub last_applied_sequence: u64,
    pub leader_id: u64,
}

#[derive(Debug, Clone)]
pub enum Event {
    RequestBroadcast(NodeId, ClientRequestBroadcast),
    ProtocolMessage(NodeId, ProtocolMessage),
    ViewChangeTimerExpired(MessageDigest),
}

impl Event {
    pub fn event_info(&self) -> String {
        match self {
            Event::RequestBroadcast(sender, req) => format!(
                "RequestBroadcast: Sender: {}, Sequence: {})",
                sender.0, req.sequence_number
            ),
            Event::ProtocolMessage(sender, cm) => format!(
                "ProtocolMessage: Sender: {}, Msg: {})",
                sender.0,
                cm.message_type_str()
            ),
            Event::ViewChangeTimerExpired(digest) => format!("ViewChangeTimerExpired: {}", digest),
        }
    }
}

impl From<Event> for EventOccurance {
    fn from(event: Event) -> Self {
        Self { event, attempt: 1 }
    }
}

#[derive(Debug, Clone)]
pub struct EventOccurance {
    pub event: Event,
    pub attempt: u32,
}

pub type EventQueue = tokio::sync::mpsc::Sender<EventOccurance>;

pub struct PbftExecutor {
    event_tx: EventQueue,
    // HACK: We wrap event_rx in Mutex<Option<...>> so that we we can swap it
    // and take the full ownership of the receiver without mutable referencing
    // the Executor itself.
    event_rx: Arc<Mutex<Option<tokio::sync::mpsc::Receiver<EventOccurance>>>>,

    // backup_queue is used if the main queue is full.
    // For real system, this could be backed by some persistant storage to ofload
    // the memory. Here we are going to simply use unbouded channel.
    backup_queue: tokio::sync::mpsc::UnboundedSender<EventOccurance>,
    backup_rx: Arc<Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<EventOccurance>>>>,

    node_id: NodeId,
    config: Config,
    pbft_state: Arc<Mutex<PbftState>>,
    state_machine: Arc<RwLock<dyn StateMachie>>,

    keypair: Arc<ed25519_dalek::Keypair>,

    broadcaster: Arc<dyn PbftBroadcaster>,
}

impl PbftExecutor {
    pub fn new(
        config: Config,
        keypair: Arc<ed25519_dalek::Keypair>,
        state_machine: Arc<RwLock<dyn StateMachie>>,
        broadcaster: Arc<dyn PbftBroadcaster>,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(10000);
        let state = PbftState::new(
            config.node_config.nodes.len() as u64,
            config.node_config.self_id,
            config.checkpoint_frequency * 2,
        );

        let (backup_tx, backup_rx) = tokio::sync::mpsc::unbounded_channel();

        Self {
            event_tx: tx.clone(),
            event_rx: Arc::new(Mutex::new(Some(rx))),
            node_id: config.node_config.self_id,
            backup_queue: backup_tx.clone(),
            backup_rx: Arc::new(Mutex::new(Some(backup_rx))),
            config,
            pbft_state: Arc::new(Mutex::new(state)),
            state_machine,
            keypair,
            broadcaster,
        }
    }

    pub fn handle_client_request(&self, request: ClientRequest) -> Result<ClientRequestResult> {
        let digest = request.digest();
        info!(digest = digest.to_string(), "handling client request");

        let mut state = self.pbft_state.lock().unwrap();

        // Check if request was already handled.
        // Return handled request which will contain the response if already
        // processed and latest sequence number.
        match state.message_store.get_by_id(&request.request_id) {
            Some(req) => {
                debug!(
                    request_id = request.request_id.to_string(),
                    "request already accepted"
                );
                return Ok(ClientRequestResult::AlreadyAccepted(HandledRequest {
                    request: req.clone(),
                    last_applied_sequence: state.last_applied_seq,
                    leader_id: self.view_leader(state.view),
                }));
            }
            None => {
                debug!(
                    request_id = request.request_id.to_string(),
                    "accepting new request"
                )
            }
        }

        match state.replica_state {
            ReplicaState::Replica | ReplicaState::ViewChange => {
                debug!(
                    digest = digest.to_string(),
                    "received client request on non-leader replica, starting view change timer"
                );
                // Start view change timer, if not already started
                if state.timer.is_none() {
                    self.start_veiw_change_timer(&mut state, digest.clone());
                }

                Ok(ClientRequestResult::NotLeader(NotLeader {
                    leader_id: NodeId(self.view_leader(state.view)),
                }))
            }
            ReplicaState::Leader { sequence } => {
                let message_sequence = sequence + 1;
                state.replica_state = ReplicaState::Leader {
                    sequence: message_sequence,
                };

                let pre_prepare = PrePrepare {
                    metadata: MessageMeta {
                        view: state.view,
                        sequence: message_sequence,
                        digest: request.digest(),
                    },
                }
                .sign(&self.keypair)?;

                info!(
                    digest = digest.to_string(),
                    sequence = message_sequence,
                    request_id = request.request_id.to_string(),
                    "assigned client request sequence",
                );

                let prepare = self.process_pre_prepare_and_request(
                    &mut state,
                    pre_prepare.clone(),
                    request.clone(),
                    message_sequence,
                )?;

                let prepare_cm: ProtocolMessage = prepare.into();

                let event =
                    Event::ProtocolMessage(self.config.node_config.self_id, prepare_cm.clone());
                self.queue_event(event.into());

                // Broadcast the request with PrePrepare
                self.broadcaster
                    .broadcast_operation(ClientRequestBroadcast {
                        request,
                        sequence_number: message_sequence,
                        pre_prepare,
                    });
                // Broadcast leader's own Prepare
                self.broadcaster
                    .broadcast_consensus_message(ProtocolMessageBroadcast::new(prepare_cm));

                Ok(ClientRequestResult::Accepted(message_sequence))
            }
        }
    }

    fn start_veiw_change_timer(&self, state: &mut PbftState, digest: MessageDigest) {
        let tx = self.event_tx.clone();
        let digest_m = digest.clone();
        let vc_timoeut = self.config.view_change_timeout;
        let timer_task = tokio::spawn(async move {
            tokio::time::sleep(vc_timoeut).await;
            match tx
                .send(Event::ViewChangeTimerExpired(digest_m).into())
                .await
            {
                Ok(_) => {}
                Err(_) => {
                    warn!(
                        "failed to send view change trigger -- pbft module might have been stopped"
                    )
                }
            }
        });
        state.timer = Some(ViewChangeTimer {
            trigger_digest: digest,
            task: timer_task,
        });
    }

    pub fn queue_protocol_message(&self, sender_id: u64, msg: ProtocolMessage) {
        // TODO: Teoretically we could return error here so that the sender retries
        self.queue_event(Event::ProtocolMessage(NodeId(sender_id), msg).into())
    }

    pub fn queue_request_broadcast(&self, sender_id: u64, msg: ClientRequestBroadcast) {
        self.queue_event(Event::RequestBroadcast(NodeId(sender_id), msg).into())
    }

    fn queue_event(&self, event: EventOccurance) {
        if let Err(err) = self.event_tx.try_send(event) {
            match err {
                tokio::sync::mpsc::error::TrySendError::Full(prepare_event) => {
                    self.push_to_backup_queue(prepare_event)
                }
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    warn!("failed to send protocol message event, queue closed -- pbft module might have been stopped")
                }
            }
        }
    }

    fn queue_delayed(&self, event: EventOccurance, delay: std::time::Duration) {
        let tx = self.event_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            if tx.send(event).await.is_err() {
                warn!("failed to send delayed event, queue closed -- pbft module might have been stopped")
            }
        });
    }

    fn push_to_backup_queue(&self, event: EventOccurance) {
        match self.backup_queue.send(event) {
            Ok(_) => {}
            Err(err) => {
                error!(error = ?err, "failed to push event to backup queue, system might be shutting down");
            }
        }
    }

    // NOTE: the drawback of this approach is that we are limmited to single
    // therad, however we lock the state on every event anyway, so making it
    // concurent would not bring much benefit.
    pub async fn run(&self, mut rx_cancel: tokio::sync::broadcast::Receiver<()>) {
        // HACK: hack to take full ownership of the receiver without needing a
        // mutable reference to the executor itself.
        let event_rx = self.event_rx.lock().unwrap().take();
        if event_rx.is_none() {
            error!("event channel is not present, cannot start executor event loop");
            return;
        }
        let mut event_rx = event_rx.unwrap();

        // We could make executor distribute event across different worker
        // threads, however, I doubt there would be much benefit from that since
        // handling of every event takes lock on the state so we might as well
        // just run a single one.
        loop {
            tokio::select! {
                _ = rx_cancel.recv() => {
                    info!("received cancel signal");
                    return;
                }
                event = event_rx.recv() => {
                    if event.is_none() {
                        warn!("event channel closed, stopping executor event loop");
                        return;
                    }
                    let event_occ = event.unwrap();
                    let event = event_occ.event;

                    info!(event = event.event_info(), "executor processing event");

                    match event {
                        Event::RequestBroadcast(sender_id, client_request_broadcast) => {
                            self.request_broadcast_event(sender_id, client_request_broadcast)
                        }
                        Event::ProtocolMessage(sender_id, consensus_message) => {
                            self.protocol_message_event(sender_id, consensus_message, event_occ.attempt)
                        }
                        Event::ViewChangeTimerExpired(message_digest) => {
                            self.view_change_timer_event(message_digest);
                        }
                    };
                }
            }
        }
    }

    // For real implementation we could watch saturation of the main queue and if
    // it has space to process events read from the backup queue and push the
    // events there.
    pub async fn run_backup_queue_watcher(
        &self,
        mut rx_cancel: tokio::sync::broadcast::Receiver<()>,
    ) {
        let backup_event_rx = self.backup_rx.lock().unwrap().take();
        if backup_event_rx.is_none() {
            error!("backup event channel is not present, cannot start executor event loop");
            return;
        }
        let mut backup_event_rx = backup_event_rx.unwrap();

        loop {
            tokio::select! {
                _ = rx_cancel.recv() => {
                    info!("received cancel signal");
                    return;
                }
                event = backup_event_rx.recv() => {
                    if event.is_none() {
                        warn!("backup channel closed, stopping executor event loop");
                        return;
                    }

                    match self.event_tx.send(event.unwrap()).await {
                        Ok(_) => {}
                        Err(err) => {
                            error!(error = ?err, "failed to send event from backup queue to main queue, system might be shutting down");
                        }
                    }
                }
            }
        }
    }

    fn view_change_timer_event(&self, digest: MessageDigest) {
        let mut state = self.pbft_state.lock().unwrap();

        if state.timer.is_none() {
            // Timer was already stopped.
            // Message might have been applied when we were waiting
            // for the lock therefore we do not start the view change.
            return;
        }

        let timer = state.timer.take().unwrap();
        if digest != timer.trigger_digest {
            warn!("received view change trigger for different digest than expected");
            // cleanup
            state.timer = Some(timer);
            return;
        }

        // Make sure we cleanup the timer, however it should not be
        // necessary.
        self.reset_timer(&mut state.timer);

        info!(
            digest = timer.trigger_digest.to_string(),
            "message was not applied in time, starting view change",
        );

        match self.initiate_view_change(&mut state) {
            Ok(vc) => self
                .broadcaster
                .broadcast_consensus_message(ProtocolMessageBroadcast {
                    message: ProtocolMessage::ViewChange(vc),
                }),
            Err(e) => {
                error!(error=?e, "failed to initiate view change, reverting to replica state");
                state.replica_state = ReplicaState::Replica;
            }
        }
    }

    fn request_broadcast_event(
        &self,
        sender_id: NodeId,
        client_request_broadcast: ClientRequestBroadcast,
    ) {
        match self.handle_request_broadcast(sender_id, client_request_broadcast) {
            Ok(prepare) => {
                let prepare_cm = ProtocolMessage::Prepare(prepare);
                let event =
                    Event::ProtocolMessage(self.config.node_config.self_id, prepare_cm.clone());
                self.queue_event(event.into());

                self.broadcaster
                    .broadcast_consensus_message(ProtocolMessageBroadcast::new(prepare_cm));
            }
            Err(err) => {
                error!(err = ?err, "failed to process request broadcast event")
            }
        }
    }

    fn handle_request_broadcast(
        &self,
        sender_id: NodeId,
        client_request_broadcast: ClientRequestBroadcast,
    ) -> Result<SignedPrepare> {
        let mut state = self.pbft_state.lock().unwrap();

        if sender_id.0 != self.view_leader(state.view) {
            error!("received request broadcast from non-leader replica");
            return Err(Error::PrePrepareMessageNotFromLeader {
                leader_id: self.view_leader(state.view),
                sender_id: sender_id.0,
            });
        }

        self.process_pre_prepare_and_request(
            &mut state,
            client_request_broadcast.pre_prepare,
            client_request_broadcast.request,
            client_request_broadcast.sequence_number,
        )
    }

    fn protocol_message_event(
        &self,
        sender_id: NodeId,
        protocol_message: ProtocolMessage,
        attempt: u32,
    ) {
        match self.process_protocol_message(protocol_message.clone(), sender_id) {
            Ok((p_messages, responses)) => {
                for p_msg in p_messages.iter() {
                    // Do not queue NewView message for yourself to process
                    if !p_msg.is_new_view() {
                        let event =
                            Event::ProtocolMessage(self.config.node_config.self_id, p_msg.clone());
                        self.queue_event(event.into());
                    }

                    self.broadcaster
                        .broadcast_consensus_message(ProtocolMessageBroadcast::new(p_msg.clone()));
                }

                if !responses.is_empty() {
                    self.broadcaster.send_client_responses(responses);
                }
            }
            Err(err) => {
                error!(err = ?err, "failed to process protocol message event!");
                // We might have failed to process the message due to some
                // ordering issues, such as receiving Prepare from the new view
                // before the actual NewView message. In such cases we requeue
                // the message, so that we can process it after awaiting ones.
                // We also delay requeuing the message in case other messages
                // did not arrive yet.
                if err.is_executor_retrieable_error()
                    && attempt < self.config.executor_config.max_requeue_attempts_on_failure
                {
                    warn!(
                        attempt = attempt + 1,
                        "protocol message failed with retriable error, requeuing"
                    );
                    let event = Event::ProtocolMessage(sender_id, protocol_message);
                    self.queue_delayed(
                        EventOccurance {
                            event,
                            attempt: attempt + 1,
                        },
                        std::time::Duration::from_millis(200) * attempt,
                    );
                }
            }
        }
    }

    fn process_protocol_message(
        &self,
        message: ProtocolMessage,
        sender_id: NodeId,
    ) -> Result<(Vec<ProtocolMessage>, Vec<ClientResponse>)> {
        if let Some(replica_id) = message.replica_id() {
            if sender_id != replica_id {
                return Err(Error::InvalidReplicaID { replica_id });
            }
        }

        let mut state = self.pbft_state.lock().unwrap();

        self.ensure_can_handle_message(&state, &message)?;

        match message {
            ProtocolMessage::PrePrepare(pre_prepare) => {
                // PrePrepare is handled separately with client request
                Err(Error::PrePrepareMessageWithoutClientRequest {
                    view: state.view,
                    sequence: pre_prepare.metadata.sequence,
                })
            }
            ProtocolMessage::Prepare(prepare) => {
                let cm = self
                    .process_prepare(&mut state, prepare)?
                    .map(|cm| vec![cm])
                    .unwrap_or_default();
                Ok((cm, vec![]))
            }
            ProtocolMessage::Commit(commit) => self.process_commit(&mut state, commit),
            ProtocolMessage::Checkpoint(checkpoint) => {
                self.process_checkpoint_message(&mut state, checkpoint);
                Ok((vec![], vec![]))
            }
            ProtocolMessage::ViewChange(view_change) => {
                self.process_view_change_message(&mut state, view_change)
            }
            ProtocolMessage::NewView(new_view) => {
                self.process_new_view_message(&mut state, new_view)
            }
        }
    }

    fn process_pre_prepare_and_request(
        &self,
        state: &mut PbftState,
        pre_prepare: SignedPrePrepare,
        request: ClientRequest,
        sequence: u64,
    ) -> Result<SignedPrepare> {
        if pre_prepare.metadata.digest != request.digest() {
            return Err(Error::InvalidDigest {
                expected: pre_prepare.metadata.digest.clone(),
                actual: request.digest(),
            });
        }
        if pre_prepare.metadata.sequence != sequence {
            return Err(Error::RequestSequenceDoesNotMatchMetadata {
                protocol_msg_seq: pre_prepare.metadata.sequence,
                request_seq: sequence,
            });
        }

        let prepare = self.store_pre_prepare(state, pre_prepare)?;

        // We also insert client message to the log
        state.message_store.insert_client_request(sequence, request);

        Ok(prepare)
    }

    fn store_pre_prepare(
        &self,
        state: &mut PbftState,
        pre_prepare: SignedPrePrepare,
    ) -> Result<SignedPrepare> {
        let idx = ConsensusLogIdx::from(&pre_prepare.metadata);

        // Since consensus messages may arrive out of order (eg. prepare before pre-prepare)
        // we might already have an entry in the log for this view and sequence number.
        let request_cons_state = if let Some(request_cons_state) = state.consensus_log.get_mut(&idx)
        {
            // If we already accepted PrePrepare for this view and sequence number
            // with different digest, we should reject the message.
            if let Some(existing_pre_prep) = &request_cons_state.pre_prepare {
                if existing_pre_prep.metadata.digest != pre_prepare.metadata.digest {
                    return Err(Error::PrePrepareForViewAndSequenceAlreadyAccepted {
                        view: pre_prepare.metadata.view,
                        sequence: pre_prepare.metadata.sequence,
                    });
                }
            }

            // In case we already have a log entry for this view and sequence number
            // we expect it's digest to match the digest of the pre-prepare message.
            // If that is not the case, we are going to discard the whole entry and
            // replace it with new one based on the pre-prepare message.
            if request_cons_state.digest != pre_prepare.metadata.digest {
                // Return None so that entry will be replaced with new one
                None
            } else {
                // Return existing entry so that we can store the pre-prepare message there.
                Some(request_cons_state)
            }
        } else {
            None
        };

        if let Some(request_cons_state) = request_cons_state {
            debug!(
                sequence = pre_prepare.metadata.sequence,
                "storing pre-prepare message to existing consensus log entry",
            );
            request_cons_state.pre_prepare.replace(pre_prepare.clone());
        } else {
            debug!(
                sequence = pre_prepare.metadata.sequence,
                "inserting new pre-prepare message to consensus log with new entry",
            );
            // Insert new entry into the log
            let mut log_msg = RequestConsensusState::new(&pre_prepare.metadata);
            log_msg.pre_prepare = Some(pre_prepare.clone());
            state.consensus_log.insert(idx, log_msg);
        };

        // Create Prepare message
        let prepare = self.prepare_msg(pre_prepare.metadata.clone())?;
        Ok(prepare)
    }

    fn prepare_msg(&self, message_meta: MessageMeta) -> Result<SignedPrepare> {
        Prepare {
            metadata: MessageMeta { ..message_meta },
            replica_id: self.node_id,
        }
        .sign(&self.keypair)
    }

    fn process_prepare(
        &self,
        state: &mut PbftState,
        prepare: SignedPrepare,
    ) -> Result<Option<ProtocolMessage>> {
        let message_meta = prepare.metadata.clone();

        let log = &mut state.consensus_log;

        let idx = ConsensusLogIdx::from(&message_meta);

        // Since we are not guaranteed to receive pre-prepare prior to receiving first prepare messages,
        // we might need to create a new entry in the log based on the prepare message.
        let entry = log
            .entry(idx)
            .or_insert(RequestConsensusState::new(&message_meta));

        // If we received message for a given view and sequence number with different digest,
        // we reject it.
        if message_meta.digest != entry.digest {
            return Err(Error::PrepareForViewAndSequenceDoesNotMatchDigest {
                view: message_meta.view,
                sequence: message_meta.sequence,
                expected: entry.digest.clone(),
                actual: message_meta.digest,
            });
        }

        // If we already have Prepare message from this replica, we do not add it again.
        if !entry
            .prepare
            .iter()
            .any(|m| m.replica_id == prepare.replica_id)
        {
            debug!(
                replica_id = prepare.replica_id.0,
                sequence = message_meta.sequence,
                "storing new prepare message",
            );
            entry.prepare.push(prepare.clone());
        }

        debug!(prepares = entry.prepare.len(), "prepare message stored");

        // Check if replica is prepared
        if entry.is_prepared(self.config.node_config.nodes.len())
            && state.message_store.has_message(message_meta.sequence)
        {
            if !entry.reported_prepared {
                entry.reported_prepared = true;
                debug!(
                    replica_id = prepare.replica_id.0,
                    sequence = message_meta.sequence,
                    "replica is prepared",
                );
                let commit = self.commit_msg(message_meta)?;
                return Ok(Some(ProtocolMessage::Commit(commit)));
            } else {
                return Ok(None);
            }
        }

        Ok(None)
    }

    fn commit_msg(&self, message_meta: MessageMeta) -> Result<SignedCommit> {
        Commit {
            metadata: MessageMeta { ..message_meta },
            replica_id: self.node_id,
        }
        .sign(&self.keypair)
    }

    fn process_commit(
        &self,
        state: &mut PbftState,
        commit: SignedCommit,
    ) -> Result<(Vec<ProtocolMessage>, Vec<ClientResponse>)> {
        let message_meta = commit.metadata.clone();

        let idx = ConsensusLogIdx::from(&message_meta);

        let entry = state
            .consensus_log
            .entry(idx)
            .or_insert(RequestConsensusState::new(&message_meta));

        // If we received message for a given view and sequence number with different digest,
        // we reject it.
        if message_meta.digest != entry.digest {
            return Err(Error::CommitForViewAndSequenceDoesNotMatchDigest {
                view: message_meta.view,
                sequence: message_meta.sequence,
                expected: entry.digest.clone(),
                actual: message_meta.digest,
            });
        }

        // If we already have Commit message from this replica, we do not add it again.
        if !entry
            .commit
            .iter()
            .any(|m| m.replica_id == commit.replica_id)
        {
            debug!(
                replica_id = commit.replica_id.0,
                sequence = message_meta.sequence,
                "storing new commit message"
            );
            entry.commit.push(commit.clone());
        }

        let result = if entry.is_committed_local(self.config.node_config.nodes.len()) {
            if !entry.reported_committed_local {
                entry.reported_committed_local = true;
                // We will try to apply all messages that we can
                let (checkpoints, responses) = self.apply_messages(state)?;
                (
                    checkpoints.into_iter().map(|m| m.into()).collect(),
                    responses,
                )
            } else {
                (vec![], vec![])
            }
        } else {
            (vec![], vec![])
        };

        Ok(result)
    }

    // TODO: I could consider making it an event to unwind this a bit
    fn apply_messages(
        &self,
        state: &mut PbftState,
    ) -> Result<(Vec<SignedCheckpoint>, Vec<ClientResponse>)> {
        // Start from state.last_applied
        let last_applied = &mut state.last_applied_seq;

        // Because we are using a BTreeMap, we can iterate over it in order of
        // sequence number.
        let start = state
            .consensus_log
            .iter()
            .position(|(idx, _)| idx.sequence > *last_applied);
        if start.is_none() {
            return Ok((vec![], vec![]));
        }

        debug!(
            last_applied = *last_applied,
            start = start.unwrap(),
            "attempting to apply messages"
        );

        let mut responses = Vec::new();
        let mut checkpoints = Vec::new();

        // We skip already applied log entries and since we use BTreeMap, we will
        // apply messages in order.
        // We are going to apply all messages since last_applied that are committed
        // but were not applied yet.
        for (idx, entry) in state.consensus_log.iter().skip(start.unwrap()) {
            // Entry is not committed locally, we should not apply it, and we
            // cannot proceed further.
            if !entry.is_committed_local(self.config.node_config.nodes.len()) {
                // It is possible to have entries with the same sequence in
                // different views due to the View Change protocol, this may
                // result in entry never being committed, hence we cannot simply
                // break the loop here.
                continue;
            }
            if idx.sequence > *last_applied + 1 {
                break;
            }

            let mut state_machine = self.state_machine.write().unwrap();

            match state.message_store.get_by_seq_mut(idx.sequence) {
                Some(store_msg) => {
                    // Make sure that the digest matches - this should always
                    // be the case, hence we simply assert.
                    assert!(entry.digest == store_msg.digest());

                    info!(sequence = idx.sequence, "applying message");
                    let result = state_machine.apply_operation(store_msg.operation());
                    // Sequence numbers increment strictly by 1.
                    *last_applied += 1;
                    assert!(*last_applied == idx.sequence);

                    // Stop View Change timer if this message started it
                    if let Some(timer) = &state.timer {
                        // The message that started the timer was applied,
                        // so we can stop the timer.
                        if timer.trigger_digest == store_msg.digest() {
                            info!(
                                    digest = hex::encode(timer.trigger_digest.0),
                                    "message that started view change timer was applied, stopping timer",
                                );
                            self.reset_timer(&mut state.timer);
                        }
                    }

                    store_msg.set_opreation_result(result.clone());

                    let result = OperationResultSequenced {
                        result,
                        sequence_number: idx.sequence,
                    };
                    if let crate::message_store::StoredMessage::AcceptedRequest(request) = store_msg
                    {
                        responses.push(ClientResponse {
                            request: request.request.clone(),
                            result,
                        });
                    }
                }
                None => {
                    // If we cannot apply the operation because we are missing the request,
                    // we stop applying messages
                    // TODO: we could queue some request to other replicas to fetch messages here
                    return Ok((checkpoints, responses));
                }
            };

            if *last_applied % self.config.checkpoint_frequency == 0 {
                let checkpoint = state_machine.checkpoint(idx.sequence)?;
                let digest = md5::compute(checkpoint.as_bytes());

                let checkpoint_digest = CheckpointDigest(digest.0);
                state.checkpoints.insert(idx.sequence, checkpoint.clone());
                state
                    .checkpoint_digests
                    .insert(idx.sequence, checkpoint_digest.clone());

                checkpoints.push(
                    Checkpoint {
                        replica_id: self.config.node_config.self_id,
                        sequence: idx.sequence,
                        digest: checkpoint_digest,
                    }
                    .sign(&self.keypair)?,
                )
            }
        }
        Ok((checkpoints, responses))
    }

    fn reset_timer(&self, timer: &mut Option<ViewChangeTimer>) {
        if let Some(timer) = &timer {
            timer.task.abort();
        }
        *timer = None;
    }

    fn process_checkpoint_message(&self, state: &mut PbftState, checkpoint: SignedCheckpoint) {
        let log = &mut state.checkpoint_log;

        let entry = log
            .entry(checkpoint.sequence)
            .or_insert(CheckpointConsensusState::new(&checkpoint));

        let digest_proofs = entry
            .proofs
            .entry(checkpoint.digest.clone())
            .or_insert(vec![]);

        for proof in digest_proofs.iter() {
            if proof.replica_id == checkpoint.replica_id {
                return;
            }
        }
        digest_proofs.push(checkpoint.clone());
        debug!(
            replica_id = checkpoint.replica_id.0,
            seq = checkpoint.sequence,
            proofs = digest_proofs.len(),
            "checkpoint proof stored"
        );

        if entry.is_stable(self.config.node_config.nodes.len()) {
            debug!(
                seq = checkpoint.sequence,
                "checkpoint reached -- updating watermarks and discarding messages"
            );
            state.set_watermarks(checkpoint.sequence);
            self.discard_messages(state, checkpoint.sequence);
        }
    }

    fn discard_messages(&self, state: &mut PbftState, check_seq: u64) {
        let mut messages_to_discard = vec![];
        let mut idx_to_remove = vec![];

        for (idx, req_state) in state.consensus_log.iter() {
            if idx.sequence < check_seq {
                debug!(
                    seq = idx.sequence,
                    digest = hex::encode(req_state.digest.0),
                    "marking message to discard"
                );
                idx_to_remove.push(*idx);
                messages_to_discard.push(req_state.pre_prepare.as_ref().unwrap().metadata.sequence);
            } else {
                break;
            }
        }

        for idx in idx_to_remove {
            debug!(
                view = idx.view,
                seq = idx.sequence,
                "removing consensus state"
            );
            state.consensus_log.remove(&idx);
        }
    }

    fn initiate_view_change(&self, state: &mut PbftState) -> Result<SignedViewChange> {
        state.replica_state = ReplicaState::ViewChange;

        // Go through the BTreeMap in reverse order and find the last stable
        // checkpoint.
        let checkpoint = self.get_last_stable_checkpoint(state);
        let checkpoint_seq = checkpoint.as_ref().map(|c| c.sequence).unwrap_or(0);

        let prepared = state
            .consensus_log
            .iter()
            .rev()
            .take_while(|(idx, cs_state)| {
                idx.sequence > checkpoint_seq
                    && cs_state.is_prepared(self.config.node_config.nodes.len())
            })
            .map(|(idx, entry)| {
                let proof = PreparedProof {
                    // We only take "prepared" entries, so we can unwrap here as
                    // pre-prepare message is required for the entry to be prepared.
                    pre_prepare: entry.pre_prepare.as_ref().unwrap().clone(),
                    prepares: entry
                        .prepare
                        .iter()
                        .map(|p| (p.pub_key_hex(), p.clone()))
                        .collect(),
                };
                (idx.sequence, proof)
            })
            .collect();

        let vc = ViewChange {
            replica_id: self.config.node_config.self_id,
            view: state.view + 1,
            last_stable_checkpoint: checkpoint,
            prepared_proofs: prepared,
        }
        .sign(&self.keypair)?;

        Ok(vc)
    }

    fn process_view_change_message(
        &self,
        state: &mut PbftState,
        view_change: SignedViewChange,
    ) -> Result<(Vec<ProtocolMessage>, Vec<ClientResponse>)> {
        let log = &mut state.view_change_log;

        if state.view >= view_change.view {
            warn!(
                view = view_change.view,
                replica_view = state.view,
                "ignoring view change message for lower or equal view"
            );
            return Ok((vec![], vec![]));
        }
        self.verify_view_change(&view_change)?;

        let entry = log.entry(view_change.view).or_insert(vec![]);

        // If we already have ViewChange message from this replica, we do not add it again.
        if !entry.iter().any(|m| m.replica_id == view_change.replica_id) {
            debug!(
                replica_id = view_change.replica_id.0,
                view = view_change.view,
                "storing view change message",
            );
            entry.push(view_change.clone());
        }

        // If the node is suppose to be a leader for the next view, we check if
        // there is enough ViewChange messages to start the new view.
        #[allow(clippy::collapsible_if)]
        if view_leader(self.config.node_config.nodes.len() as u64, view_change.view)
            == self.config.node_config.self_id.0
        {
            if entry.len() >= quorum_size(self.config.node_config.nodes.len()) {
                info!(
                    view = view_change.view,
                    "starting new view with {} ViewChange messages",
                    entry.len()
                );
                let new_view = self.compose_new_view_message(view_change.view, entry)?;

                let mut cm_messages = self.transition_view(
                    state,
                    ReplicaState::Leader {
                        sequence: new_view.latest_sequence(),
                    },
                    &new_view,
                )?;

                cm_messages.push(ProtocolMessage::NewView(new_view));

                return Ok((cm_messages, vec![]));
            }
        }

        Ok((vec![], vec![]))
    }

    fn process_new_view_message(
        &self,
        state: &mut PbftState,
        new_view: SignedNewView,
    ) -> Result<(Vec<ProtocolMessage>, Vec<ClientResponse>)> {
        self.verify_new_view(&new_view)?;

        info!(view = new_view.view, "received new view message");
        let prepares = self.transition_view(state, ReplicaState::Replica, &new_view)?;

        Ok((prepares, vec![]))
    }

    fn compose_new_view_message(
        &self,
        view: u64,
        vc_messages: &[SignedViewChange],
    ) -> Result<SignedNewView> {
        let min_s = vc_messages
            .iter()
            .map(|vc| {
                vc.last_stable_checkpoint
                    .as_ref()
                    .map(|c| c.sequence)
                    .unwrap_or(0)
            })
            .max()
            .unwrap();

        let prepared_after_checkpoint: BTreeMap<u64, SignedPrePrepare> = vc_messages
            .iter()
            .flat_map(|vc| {
                vc.prepared_proofs.iter().filter_map(|(s, proof)| {
                    // Those earlier or equal to min-s we skip
                    if s <= &min_s {
                        None
                    } else {
                        Some((
                            proof.pre_prepare.metadata.sequence,
                            proof.pre_prepare.clone(),
                        ))
                    }
                })
            })
            .collect();

        let mut pre_prepares = vec![];

        // Create PrePrepare messages for all those that prepared after the
        // last stable checkpoint in a previous view.
        for (_s, pp) in prepared_after_checkpoint.iter() {
            pre_prepares.push(
                PrePrepare {
                    metadata: MessageMeta {
                        view,
                        ..pp.metadata.clone()
                    },
                }
                .sign(&self.keypair)?,
            );
        }
        // If there are no new PrePrepares, we create one with NULL digest
        if pre_prepares.is_empty() {
            pre_prepares.push(
                PrePrepare {
                    metadata: MessageMeta {
                        view,
                        sequence: min_s + 1,
                        digest: NULL_DIGEST,
                    },
                }
                .sign(&self.keypair)?,
            );
        }

        let vc_proof = vc_messages
            .iter()
            .map(|vc| (vc.pub_key_hex(), vc.clone()))
            .collect();

        let new_view = NewView {
            view,
            view_change_messages: vc_proof,
            pre_prepares,
        };

        new_view.sign(&self.keypair)
    }

    // TODO: This part should be transactional in some way, so that
    // if transition fails we do not end up in some partial failure
    // state.
    fn transition_view(
        &self,
        state: &mut PbftState,
        role: ReplicaState,
        new_view: &NewView,
    ) -> Result<Vec<ProtocolMessage>> {
        info!(view = new_view.view, "transitioning to new view");

        state.view = new_view.view;
        state.replica_state = role;
        self.reset_timer(&mut state.timer);

        // TODO: if we do not have messages for given sequence nunmbers, we would have to
        // request them from other replicas.
        // For a naive implementation this could be an async call that creates
        // new task, which results in queuing new event to the event loop when completed
        let prepares: Result<Vec<ProtocolMessage>> = new_view
            .pre_prepares
            .iter()
            .map(|pp| {
                // If the digest is NULL, there is no client request, so we
                // insert NULL message to the log.
                if pp.is_null() {
                    state.message_store.insert_null(pp.metadata.sequence);
                }
                self.store_pre_prepare(state, pp.clone())
                    .map(ProtocolMessage::Prepare)
            })
            .collect();

        prepares
    }

    fn verify_view_change(&self, view_change: &SignedViewChange) -> Result<()> {
        let trusted_pub_keys = self.config.node_config.trusted_pub_keys();

        if let Some(checkpoint) = &view_change.last_stable_checkpoint {
            self.verify_checkpoint_proof(checkpoint, &trusted_pub_keys)?;
        }

        self.verify_prepared_proofs(&view_change.prepared_proofs, &trusted_pub_keys)?;

        Ok(())
    }

    fn verify_checkpoint_proof(
        &self,
        checkpoint: &ViewChangeCheckpoint,
        trusted_pub_keys: &HashMap<&str, NodeId>,
    ) -> Result<()> {
        for (pub_key, cp_msg) in &checkpoint.checkpoint_proofs {
            match trusted_pub_keys.get(pub_key.as_str()) {
                Some(replica_id) => {
                    if !cp_msg.verify_replica_signature(*replica_id)? {
                        return Err(Error::InvalidViewChange(format!(
                            "Invalid signature for checkpoint proof: {}",
                            pub_key
                        )));
                    }
                }
                None => {
                    return Err(Error::InvalidViewChange(format!(
                        "Checkpoint proof contains unknown replica key: {}",
                        pub_key
                    )));
                }
            }

            if cp_msg.sequence != checkpoint.sequence {
                return Err(Error::InvalidViewChange(format!(
                    "Proof sequence number does not match: {}",
                    cp_msg.sequence
                )));
            }

            if cp_msg.digest != checkpoint.digest {
                return Err(Error::InvalidViewChange(format!(
                    "Proof digest does not match: {}",
                    hex::encode(cp_msg.digest.0)
                )));
            }
        }

        Ok(())
    }

    fn verify_prepared_proofs(
        &self,
        prepared_proofs: &HashMap<u64, PreparedProof>,
        trusted_pub_keys: &HashMap<&str, NodeId>,
    ) -> Result<()> {
        for (seq, proof) in prepared_proofs {
            let digest = proof.pre_prepare.metadata.digest.clone();
            if proof.pre_prepare.metadata.sequence != *seq {
                return Err(Error::InvalidViewChange(format!(
                    "PrePrepare sequence number does not match proof: {}",
                    proof.pre_prepare.metadata.sequence
                )));
            }

            for (pub_key, prepare) in proof.prepares.iter() {
                match trusted_pub_keys.get(pub_key.as_str()) {
                    Some(replica_id) => {
                        if !prepare.verify_replica_signature(*replica_id)? {
                            return Err(Error::InvalidViewChange(format!(
                                "Invalid signature for prepare proof: {}",
                                pub_key
                            )));
                        }
                    }
                    None => {
                        return Err(Error::InvalidViewChange(format!(
                            "Prepare proof contains unknown replica key: {}",
                            pub_key
                        )));
                    }
                }

                if prepare.metadata.sequence != *seq {
                    return Err(Error::InvalidViewChange(format!(
                        "Prepare sequence number does not match proof: {}",
                        prepare.metadata.sequence
                    )));
                }

                if prepare.metadata.digest != digest {
                    return Err(Error::InvalidViewChange(format!(
                        "Prepare proof does not match PrePrepare digest: {}",
                        prepare.metadata.digest
                    )));
                }
            }
        }
        Ok(())
    }

    fn verify_new_view(&self, new_view: &NewView) -> Result<()> {
        if new_view.view_change_messages.len() < quorum_size(self.config.node_config.nodes.len()) {
            return Err(Error::InvalidViewChange(format!(
                "Not enough ViewChange messages, got: {}, expected at least: {}",
                new_view.view_change_messages.len(),
                quorum_size(self.config.node_config.nodes.len())
            )));
        }

        let trusted_pub_keys = self.config.node_config.trusted_pub_keys();

        // TODO: we could ignore invalid view changes and just check if we have
        // 2f + 1 valid ones
        for (key, vc) in &new_view.view_change_messages {
            if let Some(replica_id) = trusted_pub_keys.get(key.as_str()) {
                if vc.replica_id != *replica_id || vc.message.replica_id != *replica_id {
                    return Err(Error::InvalidViewChange(
                        "Public key does not match replica id".into(),
                    ));
                }
                if let Err(e) = vc.verify() {
                    return Err(Error::InvalidViewChange(format!(
                        "View Change signature verification failed: {:?}",
                        e
                    )));
                }
            } else {
                return Err(Error::InvalidViewChange(format!(
                    "Public key not found in trusted keys: {}",
                    key
                )));
            }
        }

        Ok(())
    }

    pub fn get_state(&self) -> crate::api::PbftNodeState {
        let state = self.pbft_state.lock().unwrap();

        let checkpoint = self.get_last_stable_checkpoint(&state);
        let checkpoint_seq = checkpoint.as_ref().map(|c| c.sequence);

        crate::api::PbftNodeState {
            view: state.view,
            last_applied: state.last_applied_seq,
            replica_state: state.replica_state,
            last_stable_checkpoint_sequence: checkpoint_seq,
        }
    }

    fn get_last_stable_checkpoint(&self, state: &PbftState) -> Option<ViewChangeCheckpoint> {
        // Go through the BTreeMap in reverse order and find the last stable
        // checkpoint.
        state
            .checkpoint_log
            .iter()
            .rev()
            .find(|c| c.1.is_stable(self.config.node_config.nodes.len()))
            .map(|c| {
                let proofs =
                    c.1.proofs
                        .get(&c.1.digest)
                        // Since the checkpoint is stable it should be safe to unwrap
                        // here, and we should have enough proofs.
                        .unwrap()
                        .iter()
                        .map(|p| (p.pub_key_hex(), p.clone()))
                        .collect();

                ViewChangeCheckpoint {
                    sequence: *c.0,
                    digest: c.1.digest.clone(),
                    checkpoint_proofs: proofs,
                }
            })
    }

    pub fn leader_id(&self) -> u64 {
        view_leader(
            self.config.node_config.nodes.len() as u64,
            self.get_state().view,
        )
    }

    fn ensure_can_handle_message(&self, state: &PbftState, msg: &ProtocolMessage) -> Result<()> {
        // Not all messages have view number hence non all of them will be
        // "in any view".
        if let Some(in_view) = msg.in_view() {
            if in_view != state.view {
                return Err(Error::MessageReplicaViewsMissmatch {
                    msg_view: in_view,
                    replica_view: state.view,
                });
            }
        }
        // Check if the message is within the watermarks range
        if let Some(sequence) = msg.sequence() {
            if sequence < state.low_watermark || sequence > state.high_watermark {
                return Err(Error::MessageSequenceOutsideWatermarksRange {
                    sequence,
                    low: state.low_watermark,
                    high: state.high_watermark,
                });
            }
        }

        let can_handle = match msg {
            ProtocolMessage::PrePrepare(_) => {
                panic!("PrePrepare message should be handled separatelly together with the client request")
            }
            ProtocolMessage::Prepare(_) => {
                state.replica_state == ReplicaState::Replica
                    || matches!(state.replica_state, ReplicaState::Leader { .. })
            }
            ProtocolMessage::Commit(_) => {
                state.replica_state == ReplicaState::Replica
                    || matches!(state.replica_state, ReplicaState::Leader { .. })
            }
            ProtocolMessage::Checkpoint(_) => true,
            ProtocolMessage::ViewChange(_) => true,
            ProtocolMessage::NewView(_) => true,
        };
        if !can_handle {
            return Err(Error::ReplicaInStateCannotHandleMessage {
                state: state.replica_state,
                message_type: msg.message_type_str(),
            });
        }
        Ok(())
    }

    // We assume node ids go up sequentially from 0 to N-1
    fn view_leader(&self, view: u64) -> u64 {
        view_leader(self.config.node_config.nodes.len() as u64, view)
    }
}

pub fn quorum_size(nodes_count: usize) -> usize {
    (nodes_count / 3) * 2 + 1
}

pub(crate) fn view_leader(nodes_count: u64, view: u64) -> u64 {
    // This is kind of ugly hack, but I want to start with replica 0 as a leader
    // Perhaps replicas should be 1-indexed...
    (view - 1) % nodes_count
}
