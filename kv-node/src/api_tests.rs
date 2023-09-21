pub mod tests {
    use std::time::Duration;

    use pbft_core::{
        api::PbftNodeState, dev::DEV_VIEW_CHANGE_TIMEOUT, ClientRequestId, ReplicaState,
    };
    use tokio::task::JoinHandle;
    use tracing::info;

    use crate::{
        api::{KvResponse, KvSetRequest, REQUEST_ID_HEADER},
        dev,
    };

    #[tokio::test]
    async fn test_client_flow() {
        // pbft_core::test_util::init_tracing(tracing::Level::INFO);

        let node_handles = setup_dev_cluster(20000).await;
        let client = reqwest::Client::new();

        let req_id1 = ClientRequestId::new();
        let request = KvSetRequest {
            key: "test_key".to_string(),
            value: "test_value".to_string(),
        };

        let set_resp = send_success_set_request(
            &client,
            &request,
            Some(&req_id1),
            "http://localhost:20000/api/v1/kv",
        )
        .await;
        assert_eq!(set_resp.key, request.key);
        assert_eq!(set_resp.value.unwrap(), request.value);
        assert_eq!(set_resp.request_id, req_id1);
        assert_eq!(set_resp.operation_sequence, 1);

        // Repeat request with the same ID and assert that it has the same
        // sequence.
        let set_resp = send_success_set_request(
            &client,
            &request,
            Some(&req_id1),
            "http://localhost:20000/api/v1/kv",
        )
        .await;
        assert_eq!(set_resp.operation_sequence, 1);

        // Send request to non-leader replica
        let req_id2 = ClientRequestId::new();
        let request = KvSetRequest {
            key: "test_key_2".to_string(),
            value: "test_value_2".to_string(),
        };

        let set_resp = send_success_set_request(
            &client,
            &request,
            Some(&req_id2),
            "http://localhost:20003/api/v1/kv",
        )
        .await;
        assert_eq!(set_resp.operation_sequence, 2);

        // Generate request ID if not specified
        let request = KvSetRequest {
            key: "test_key_3".to_string(),
            value: "test_value_3".to_string(),
        };
        let set_resp =
            send_success_set_request(&client, &request, None, "http://localhost:20000/api/v1/kv")
                .await;
        assert_eq!(set_resp.operation_sequence, 3);

        // Get value
        let get_resp = send_success_get_request(
            &client,
            "test_key_3",
            None,
            "http://localhost:20004/api/v1/kv",
        )
        .await;
        assert_eq!(get_resp.operation_sequence, 4);
        assert_eq!(get_resp.key, "test_key_3");
        assert_eq!(get_resp.value.unwrap(), "test_value_3");

        // Get non-existing value
        let get_resp = send_success_get_request(
            &client,
            "not-a-key",
            None,
            "http://localhost:20002/api/v1/kv",
        )
        .await;
        assert_eq!(get_resp.operation_sequence, 5);
        assert!(get_resp.value.is_none());

        for (handle, tx_cancel) in node_handles {
            tx_cancel.send(()).unwrap();
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_view_change_basic() {
        let mut node_handles = setup_dev_cluster(30000).await;
        let client = reqwest::Client::new();

        let req_id1 = ClientRequestId::new();
        let request = KvSetRequest {
            key: "test_key".to_string(),
            value: "test_value".to_string(),
        };

        let set_resp = send_success_set_request(
            &client,
            &request,
            Some(&req_id1),
            "http://localhost:30000/api/v1/kv",
        )
        .await;

        assert_eq!(set_resp.key, request.key);
        assert_eq!(set_resp.value.unwrap(), request.value);
        assert_eq!(set_resp.request_id, req_id1);
        assert_eq!(set_resp.operation_sequence, 1);

        // Stop the leader
        let leader_node = node_handles.remove(0);
        leader_node.1.send(()).unwrap();
        leader_node.0.await.unwrap();
        info!("Leader stopped");

        // Assert that leader is down
        let _resp = client
            .get("http://localhost:30000/api/v1/health")
            .send()
            .await
            .expect_err("expected node 0 to be down");

        // Send request to any live replica expecting error
        let req_id2 = ClientRequestId::new();
        let request = KvSetRequest {
            key: "test_key2".to_string(),
            value: "test_value2".to_string(),
        };

        let resp = client
            .post("http://localhost:30001/api/v1/kv")
            .json(&request)
            .header(REQUEST_ID_HEADER, req_id2.to_string())
            .send()
            .await
            .expect("failed to send request");
        assert!(
            resp.status().is_server_error(),
            "expected request to fail when leader goes down"
        );

        // Wait for view change time + 1 second
        let vc_wait = DEV_VIEW_CHANGE_TIMEOUT + Duration::from_secs(1);
        println!("Waiting for view change to happen: {:?}", vc_wait);
        tokio::time::sleep(vc_wait).await;
        println!("Expected view change to happen");

        // Repeat the request to any replica other than the dead on
        // we expect that view change occured and the request is processed
        let set_resp = send_success_set_request(
            &client,
            &request,
            Some(&req_id2),
            "http://localhost:30004/api/v1/kv",
        )
        .await;
        assert_eq!(
            set_resp.operation_sequence, 2,
            "expected view change to happen and request to succeed"
        );

        // Assert next replica became the leader
        assert_replica_state(
            &client,
            "http://localhost:30001",
            ReplicaState::Leader { sequence: 2 },
            2,
            2,
            None,
        )
        .await;

        // Get value
        let get_resp = send_success_get_request(
            &client,
            "test_key2",
            None,
            "http://localhost:30004/api/v1/kv",
        )
        .await;
        assert_eq!(get_resp.operation_sequence, 3);
        assert_eq!(get_resp.key, "test_key2");
        assert_eq!(get_resp.value.unwrap(), "test_value2");

        println!("Waiting for nodes to stop...");
        for (handle, tx) in node_handles {
            tx.send(()).unwrap();
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_view_change_with_checkpoint() {
        let mut node_handles = setup_dev_cluster(40000).await;
        let client = reqwest::Client::new();

        // Send several messages to reach the checkpoint
        for i in 0..10 {
            let req_id = ClientRequestId::new();
            let request = KvSetRequest {
                key: format!("test_key_{}", i),
                value: format!("test_val_{}", i),
            };
            let set_resp = send_success_set_request(
                &client,
                &request,
                Some(&req_id),
                "http://localhost:40000/api/v1/kv",
            )
            .await;
            assert_eq!(
                set_resp.operation_sequence,
                i + 1,
                "expected request to succeed"
            );
        }

        assert_replica_state(
            &client,
            "http://localhost:40000",
            ReplicaState::Leader { sequence: 10 },
            1,
            10,
            Some(10),
        )
        .await;
        assert_replica_state(
            &client,
            "http://localhost:40001",
            ReplicaState::Replica,
            1,
            10,
            Some(10),
        )
        .await;

        // Stop the leader
        let leader_node = node_handles.remove(0);
        leader_node.1.send(()).unwrap();
        leader_node.0.await.unwrap();
        info!("Leader stopped");

        // Assert that leader is down
        let _resp = client
            .get("http://localhost:40000/api/v1/health")
            .send()
            .await
            .expect_err("expected node 0 to be down");

        // Initiate view change
        let req_id10 = ClientRequestId::new();
        let request = KvSetRequest {
            key: "test_key_10".to_string(),
            value: "test_val_10".to_string(),
        };

        let resp = client
            .post("http://localhost:40002/api/v1/kv")
            .json(&request)
            .header(REQUEST_ID_HEADER, req_id10.to_string())
            .send()
            .await
            .expect("failed to send request");
        assert!(
            resp.status().is_server_error(),
            "expected request to fail when leader goes down"
        );

        // Wait for view change time + 1 second
        let vc_wait = DEV_VIEW_CHANGE_TIMEOUT + Duration::from_secs(1);
        println!("Waiting for view change to happen: {:?}", vc_wait);
        tokio::time::sleep(vc_wait).await;
        println!("Expected view change to happen");

        // Send request after view change - assert sequence number incremented
        // indicating NULL message was processed
        let set_resp = send_success_set_request(
            &client,
            &request,
            Some(&req_id10),
            "http://localhost:40003/api/v1/kv",
        )
        .await;
        // Latest sequence from prev requests + 1 for NULL message + 1
        // for this request
        assert_eq!(
            set_resp.operation_sequence, 12,
            "expected view change to happen and request to succeed"
        );

        // Assert next replica became the leader
        assert_replica_state(
            &client,
            "http://localhost:40001",
            ReplicaState::Leader { sequence: 12 },
            2,
            12,
            Some(10),
        )
        .await;

        println!("Waiting for nodes to stop...");
        for (handle, tx) in node_handles {
            tx.send(()).unwrap();
            handle.await.unwrap();
        }
    }

    async fn send_success_set_request(
        client: &reqwest::Client,
        req: &KvSetRequest,
        req_id: Option<&ClientRequestId>,
        url: &str,
    ) -> KvResponse {
        let request_builder = client.post(url);
        let request_builder = if let Some(req_id) = req_id {
            request_builder.header(REQUEST_ID_HEADER, req_id.to_string())
        } else {
            request_builder
        };

        request_builder
            .json(req)
            .send()
            .await
            .expect("failed to send request")
            .json::<KvResponse>()
            .await
            .expect("failed to parse response")
    }

    async fn send_success_get_request(
        client: &reqwest::Client,
        key: &str,
        req_id: Option<&ClientRequestId>,
        url: &str,
    ) -> KvResponse {
        let request_builder = client.get(url);
        let request_builder = if let Some(req_id) = req_id {
            request_builder.header(REQUEST_ID_HEADER, req_id.to_string())
        } else {
            request_builder
        };

        request_builder
            .query(&[("key", key)])
            .send()
            .await
            .expect("failed to send request")
            .json::<KvResponse>()
            .await
            .expect("failed to parse response")
    }

    async fn get_replica_state(client: &reqwest::Client, node_url: &str) -> PbftNodeState {
        client
            .get(format!("{}/api/v1/pbft/state", node_url))
            .send()
            .await
            .expect("failed to send request")
            .json::<PbftNodeState>()
            .await
            .expect("failed to parse response")
    }

    async fn assert_replica_state(
        client: &reqwest::Client,
        node_url: &str,
        expected_state: ReplicaState,
        expected_view: u64,
        expected_last_applied: u64,
        expected_last_checkpoint: Option<u64>,
    ) {
        let state = get_replica_state(client, node_url).await;
        assert_eq!(state.replica_state, expected_state);
        assert_eq!(state.view, expected_view);
        assert_eq!(state.last_applied, expected_last_applied);
        assert_eq!(
            state.last_stable_checkpoint_sequence,
            expected_last_checkpoint
        );
    }

    async fn setup_dev_cluster(
        starting_port: u16,
    ) -> Vec<(JoinHandle<()>, tokio::sync::broadcast::Sender<()>)> {
        let replicas_conf = vec![
            dev::dev_config(0, starting_port),
            dev::dev_config(1, starting_port),
            dev::dev_config(2, starting_port),
            dev::dev_config(3, starting_port),
            dev::dev_config(4, starting_port),
        ];

        let mut node_handles = vec![];
        for r in replicas_conf.iter() {
            let (tx_cancel, rx_cancel) = tokio::sync::broadcast::channel(1);
            let replica_conf = r.clone();
            let handle = tokio::spawn(async move {
                crate::node::start_kv_node(replica_conf.clone(), rx_cancel)
                    .await
                    .expect("failed to start kv node");
            });
            node_handles.push((handle, tx_cancel));
        }

        for r in replicas_conf.iter() {
            tokio::time::timeout(Duration::from_secs(10), poll_node_health(&r.node_url()))
                .await
                .expect("failed while waiting for node health");
        }

        node_handles
    }

    pub async fn poll_node_health(node_url: &str) {
        let client = reqwest::Client::new();
        loop {
            match client
                .get(format!("{}/api/v1/health", node_url))
                .send()
                .await
            {
                Ok(resp) => {
                    if resp.status().is_success() {
                        return;
                    }
                }
                Err(err) => {
                    tracing::error!(error = ?err, "failed to poll node health");
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
