#[cfg(test)]
mod tests {
    use crate::{
        broadcast::PbftBroadcaster,
        config::NodeId,
        dev,
        pbft_executor::{ClientRequestResult, PbftExecutor},
        state_machine, ClientRequestId, Operation,
    };
    use std::sync::{Arc, RwLock};

    pub struct NoopBroadcast;

    impl PbftBroadcaster for NoopBroadcast {
        fn broadcast_consensus_message(&self, _msg: crate::api::ConsensusMessageBroadcast) -> () {}
        fn broadcast_operation(&self, _msg: crate::api::ClientRequestBroadcast) -> () {}
        fn send_client_responses(&self, _responses: Vec<crate::ClientResponse>) -> () {}
    }

    /// expect_msg! macro is used to extract values from the enum, or panic if
    /// the enum variant is different than expected.
    /// ```rust
    /// let msg = ConsensusMessage::Prepare(PrePrepare {
    ///     metadata: MessageMeta {
    ///         view: 0,
    ///         seq_num: 0,
    ///         digest: MessageDigest([0; 16]),
    /// ]);
    /// let pre_prepare = expect_msg!(msg, ConsensusMessage::PrePrepare);
    /// assert_eq!(pre_prepare.metadata.view, 0);
    /// ```
    #[macro_export]
    macro_rules! expect_msg {
        ( $msg:expr, $variant:path ) => {
            match $msg {
                $variant(x) => x,
                _ => {
                    panic!("Invalid message type, expected {}", stringify!($variant));
                }
            }
        };
    }

    fn init_replica(id: u64) -> (PbftExecutor, Arc<RwLock<state_machine::InMemoryKVStore>>) {
        let config = dev::dev_config(NodeId(id), 12000);
        let keypair = Arc::new(config.node_config.get_keypair().unwrap());
        let state_machine = Arc::new(RwLock::new(state_machine::InMemoryKVStore::new()));
        (
            PbftExecutor::new(
                config,
                keypair,
                state_machine.clone(),
                Arc::new(NoopBroadcast {}),
            ),
            state_machine,
        )
    }

    #[tokio::test]
    async fn pbft_executor_client_request_test() {
        // Since we initialize view with value 0, the first replica will be the leader
        let (pbft_replicas, _state_machines) =
            (0..5).map(init_replica).unzip::<_, _, Vec<_>, Vec<_>>();

        let client_request = crate::ClientRequest {
            request_id: ClientRequestId::new(),
            operation: Operation::Set {
                key: "key1".to_string(),
                value: "value1".to_string(),
            },
        };

        println!("Leader should handle client request and emmit pre-prepare and prepare messages");
        let cr_result = pbft_replicas[0]
            .handle_client_request(client_request.clone())
            .expect("failed to handle client request");
        let sequence = expect_msg!(cr_result, ClientRequestResult::Accepted);
        assert_eq!(sequence, 1);

        println!("Node should not process same message twice");
        let cr_result = pbft_replicas[0]
            .handle_client_request(client_request.clone())
            .expect("failed to handle client request");
        let handled_req = expect_msg!(cr_result, ClientRequestResult::AlreadyAccepted);
        assert_eq!(handled_req.request.sequence, 1);

        println!("Backup should return NotLeader when asked to handle client request");
        let cr_result = pbft_replicas[1]
            .handle_client_request(client_request.clone())
            .expect("failed to handle client request");
        let not_leader = expect_msg!(cr_result, ClientRequestResult::NotLeader);
        assert_eq!(not_leader.leader_id.0, 0);
    }

    // TODO: Would be good to add some more tests for handling different messages
    // however KV API tests already handle a lot of scenarios.
}
