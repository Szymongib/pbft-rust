use std::collections::{BTreeMap, HashMap};

use crate::{
    AcceptedRequest, ClientRequest, ClientRequestId, MessageDigest, Operation, OperationResult,
    NULL_DIGEST,
};

static NOOP_OPERATION: Operation = Operation::Noop;

type MessageLog = BTreeMap<u64, StoredMessage>;

pub enum StoredMessage {
    AcceptedRequest(AcceptedRequest),
    Null { sequence: u64 },
}

impl StoredMessage {
    pub fn digest(&self) -> MessageDigest {
        match self {
            StoredMessage::AcceptedRequest(req) => req.request.digest(),
            StoredMessage::Null { sequence: _ } => NULL_DIGEST,
        }
    }

    pub fn operation(&self) -> &Operation {
        match self {
            StoredMessage::AcceptedRequest(req) => &req.request.operation,
            StoredMessage::Null { sequence: _ } => &NOOP_OPERATION,
        }
    }

    pub fn set_opreation_result(&mut self, result: OperationResult) {
        match self {
            StoredMessage::AcceptedRequest(req) => req.result = Some(result),
            StoredMessage::Null { sequence: _ } => (),
        }
    }
}

pub struct MessageStore {
    message_log: MessageLog,

    // by_request_id is used to check if a request has already been processed.
    // It references the ClientRequest in message_log.
    by_request_id: HashMap<ClientRequestId, u64>,
}

impl MessageStore {
    pub fn new() -> Self {
        Self {
            message_log: BTreeMap::new(),
            by_request_id: HashMap::new(),
        }
    }

    pub fn insert_client_request(&mut self, sequence: u64, request: ClientRequest) {
        let req_id = request.request_id.clone();
        self.inner_insert(
            sequence,
            StoredMessage::AcceptedRequest(AcceptedRequest {
                sequence,
                request,
                result: None,
            }),
        );
        self.by_request_id.insert(req_id, sequence); // Store the sequence instead of reference
    }

    pub fn insert_null(&mut self, sequence: u64) {
        self.inner_insert(sequence, StoredMessage::Null { sequence });
    }

    fn inner_insert(&mut self, sequence: u64, msg: StoredMessage) {
        if let Some(old_req) = self.message_log.insert(sequence, msg) {
            self.message_log.insert(sequence, old_req); // Put back the old request

            // TODO: not sure how to best handle this, it should not happen but
            // panic is probably not good...
            panic!(
                "Request with sequence already exists! Sequence: {}",
                sequence
            );
        }
    }

    pub fn get_by_seq_mut(&mut self, seq: u64) -> Option<&mut StoredMessage> {
        self.message_log.get_mut(&seq)
    }

    pub fn has_message(&self, seq: u64) -> bool {
        self.message_log.contains_key(&seq)
    }

    pub fn get_by_id(&self, request_id: &ClientRequestId) -> Option<&AcceptedRequest> {
        if let Some(&sequence) = self.by_request_id.get(request_id) {
            self.message_log.get(&sequence).and_then(|r| {
                if let StoredMessage::AcceptedRequest(req) = r {
                    Some(req)
                } else {
                    None
                }
            })
        } else {
            None
        }
    }
}
