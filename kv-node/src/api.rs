use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use axum::{
    async_trait,
    body::Body,
    extract::{FromRequest, Query},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use futures::{stream::FuturesUnordered, TryFutureExt};
use pbft_core::{
    api::ClientRequestBroadcast, api::ConsensusMessageBroadcast, ClientRequest, ClientRequestId,
    ClientResponse, OperationAck, OperationResultSequenced,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tracing::{debug, error, info};

use crate::{
    config::AppConfig,
    kv::{KeyRequest, Value},
};

pub const REQUEST_ID_HEADER: &str = "request-id";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Request error: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("Failed to broadcast request to any replica")]
    FailedToBroadcast,
}

trait VerifySignrature {
    fn verify_signature(
        &self,
        peer_id: u64,
        signature: &str,
        msg: &[u8],
    ) -> Result<(), axum::response::Response>;
}

pub struct ApiServer {
    addr: SocketAddr,
    ctx: HandlerContext,
}

pub struct APIContext {
    pbft_module: Arc<pbft_core::Pbft>,

    kv_store: Arc<RwLock<pbft_core::state_machine::InMemoryKVStore>>,

    client: reqwest::Client,
    pbft_leader_url: Mutex<String>,
    nodes_urls: HashMap<u64, String>,

    resp_channels: Mutex<HashMap<ClientRequestId, tokio::sync::broadcast::Sender<ClientResponse>>>,
    /// replica_responses maps received operation result responses to the
    /// replica IDs that send the specific response. Since we hash the whole
    /// result together with sequence number, we ensure that not matching
    /// responses for the same sequence are not counted together.
    replica_responses: Mutex<HashMap<OperationResultSequenced, HashSet<u64>>>,
}

type HandlerContext = Arc<APIContext>;

impl VerifySignrature for HandlerContext {
    fn verify_signature(
        &self,
        peer_id: u64,
        signature: &str,
        msg: &[u8],
    ) -> Result<(), axum::response::Response> {
        self.pbft_module
            .verify_request_signature(peer_id, signature, msg)
            .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()).into_response())
    }
}

impl ApiServer {
    pub async fn new(
        config: AppConfig,
        pbft_module: Arc<pbft_core::Pbft>,
        kv_store: Arc<RwLock<pbft_core::state_machine::InMemoryKVStore>>,
    ) -> Self {
        let addr = SocketAddr::from((config.listen_addr, config.port));

        let api_context = Arc::new(APIContext {
            pbft_module,
            kv_store,
            client: reqwest::Client::new(),
            pbft_leader_url: Mutex::new(config.node_url()),
            nodes_urls: config
                .pbft_config
                .node_config
                .nodes
                .iter()
                .map(|node| (node.id.0, node.addr.clone()))
                .collect(),
            resp_channels: Mutex::new(HashMap::new()),
            replica_responses: Mutex::new(HashMap::new()),
        });

        Self {
            addr,
            ctx: api_context,
        }
    }

    pub async fn run(&mut self, mut rx_shutdown: tokio::sync::broadcast::Receiver<()>) {
        let kv_router = Router::new()
            .route("/", post(handle_kv_set))
            .route("/", get(handle_kv_get))
            .route("/local", get(handle_kv_get_local));

        let consensus_client_router =
            Router::new().route("/response", post(handle_client_consensus_response));

        // TODO: Paths could probably be better...
        let consensus_ext_router = Router::new()
            // Request operation to execute
            .route("/operation", post(handle_consensus_operation_execute));

        let consensus_int_router = Router::new()
            // Client Request + PrePrepare broadcasted by the leader
            .route("/execute", post(handle_consensus_pre_prepare))
            // Any other consensus message -- Prepare, Commit, ViewChange, NewView
            .route("/message", post(handle_consensus_message))
            // Debuging endpoint to dump current pBFT state
            .route("/state", get(handle_state_dump));

        // Combine routers
        let app = Router::new()
            .route("/api/v1/health", get(health_handler))
            .nest("/api/v1/kv", kv_router)
            .nest("/api/v1/client", consensus_client_router)
            .nest("/api/v1/consensus", consensus_ext_router)
            .nest("/api/v1/pbft", consensus_int_router)
            .with_state(self.ctx.clone());

        let server = axum::Server::bind(&self.addr);

        // Disable keep alive for tests so that we do not wait long on graceful
        // shutdown.
        #[cfg(test)]
        let server = server
            .http1_keepalive(false)
            .tcp_keepalive(Some(Duration::from_secs(1)));

        let server = server
            .serve(app.into_make_service_with_connect_info::<SocketAddr>())
            .with_graceful_shutdown(async move {
                rx_shutdown
                    .recv()
                    .await
                    .expect("failed to await for cancel receive");
                info!("gracefully shutting down server...");
            });

        info!(addr = ?self.addr, "server starting to listen");

        server.await.expect("failed to await for server");

        info!(addr = ?self.addr, "Server shut down!",);
    }
}

pub struct JsonAuthenticated<T: DeserializeOwned> {
    pub sender_id: u64,
    pub data: T,
}

pub struct JsonAuthenticatedExt<T: DeserializeOwned>(pub JsonAuthenticated<T>);

#[async_trait]
impl<S, T: DeserializeOwned> FromRequest<S, Body> for JsonAuthenticatedExt<T>
where
    S: VerifySignrature + Send + Sync,
{
    type Rejection = axum::response::Response;

    async fn from_request(
        req: axum::http::Request<Body>,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let (parts, body) = req.into_parts();

        let signature = get_replica_signature(&parts.headers)?;
        let peer_id = get_sender_replica_id(&parts.headers)?;

        // this wont work if the body is an long running stream -- it is fine
        let bytes = hyper::body::to_bytes(body)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response())?;

        state.verify_signature(peer_id, &signature, &bytes)?;

        Ok(JsonAuthenticatedExt(JsonAuthenticated {
            sender_id: peer_id,
            data: serde_json::from_slice(&bytes).unwrap(),
        }))
    }
}

async fn health_handler(_ctx: axum::extract::State<HandlerContext>) -> &'static str {
    "Ok"
}

// KV router handlers

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvSetRequest {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvResponse {
    pub request_id: ClientRequestId,
    pub operation_sequence: u64,
    pub key: String,
    pub value: Option<String>,
}

async fn handle_kv_set(
    ctx: axum::extract::State<HandlerContext>,
    headers: HeaderMap,
    Json(request): Json<KvSetRequest>,
) -> impl axum::response::IntoResponse {
    let operation = pbft_core::Operation::Set {
        key: request.key,
        value: request.value,
    };

    handle_kv_operation(ctx, headers, operation).await
}

async fn handle_kv_operation(
    ctx: axum::extract::State<HandlerContext>,
    headers: HeaderMap,
    operation: pbft_core::Operation,
) -> impl axum::response::IntoResponse {
    let request_id = get_request_id(&headers)?;
    let request_id = ClientRequestId(request_id);

    info!(
        request_id = request_id.to_string(),
        operation = ?operation,
        "handling kv operation",
    );

    // Create channel and put tx part into shared memory. It is identified by
    // combination of client id and client request sequence number.
    let mut resp_rx = {
        let mut channels = ctx.resp_channels.lock().unwrap();
        let chan = channels.entry(request_id.clone()).or_insert({
            let (tx, _) = tokio::sync::broadcast::channel(1000);
            tx
        });
        chan.subscribe()
    };

    // Send request to consensus layer
    let client_req = ClientRequest {
        request_id: request_id.clone(),
        operation: operation.clone(),
    };
    let ack = match send_consensus_request(&ctx, &client_req).await {
        Ok(ack) => ack,
        Err(err) => {
            error!(error = ?err, "failed to send request to consensus layer");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to send request to consensus layer",
            )
                .into_response());
        }
    };
    match ack.status {
        pbft_core::OperationStatus::Accepted => {
            debug!(
                sequence = ack.sequence_number,
                "request accepted by consensus layer"
            );
        }
        pbft_core::OperationStatus::AlreadyHandled(res) => {
            // If we already have the result, we can return it immediately
            // otherwise we assume that we did not receive resoponses from
            // replicas yet, hence we are going to wait for the channel.
            if let Some(res) = res {
                return result_to_response(request_id, ack.sequence_number, operation, res)
                    .map_err(|e| e.into_response())
                    .await;
            }
        }
    }

    tokio::select! {
        resp = resp_rx.recv() => {
            match resp {
                Ok(resp) => {
                    result_to_response(
                        resp.request.request_id,
                        resp.result.sequence_number,
                        operation,
                        resp.result.result,
                    ).map_err(|e| e.into_response()).await
                },
                Err(err) => {
                    error!(error = ?err, "failed to receive response");
                    Err((StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response())
                }
            }
        },
        _ = tokio::time::sleep(std::time::Duration::from_secs(20)) => {
            error!("timeout while waiting for response");
            Err((StatusCode::GATEWAY_TIMEOUT, "timeout while waiting for request confirmation").into_response())
        }
    }
}

async fn result_to_response(
    request_id: ClientRequestId,
    sequence: u64,
    operation: pbft_core::Operation,
    result: pbft_core::OperationResult,
) -> Result<Json<KvResponse>, (StatusCode, String)> {
    if !operation.matches_result(&result) {
        return Err((
            StatusCode::BAD_REQUEST,
            "operation does not match result - request ID might have been reused for different operation".to_string(),
        ));
    }

    match result {
        pbft_core::OperationResult::Set { key, value } => Ok(Json(KvResponse {
            request_id,
            operation_sequence: sequence,
            key,
            value: Some(value),
        })),
        pbft_core::OperationResult::Get { key, value } => Ok(Json(KvResponse {
            request_id,
            operation_sequence: sequence,
            key,
            value,
        })),
        pbft_core::OperationResult::Noop => Err((
            StatusCode::BAD_REQUEST,
            "requested result to noop operation".to_string(),
        )),
    }
}

async fn send_consensus_request(
    ctx: &HandlerContext,
    msg: &ClientRequest,
) -> Result<OperationAck, Error> {
    let url = ctx.pbft_leader_url.lock().unwrap().clone();
    let resp = ctx
        .client
        .post(format!("{}/api/v1/consensus/operation", url))
        .json(msg)
        .timeout(Duration::from_secs(5))
        .send()
        .await;

    let resp = if let Err(err) = resp {
        error!(error = ?err, "failed to send request to last known leader, broadcasting to all");
        broadcast_request_to_all(ctx, msg).await?
    } else {
        let resp = resp.unwrap();
        if !resp.status().is_success() {
            error!(status = ?resp.status(), "failed to send request to last known leader, broadcasting to all");
            broadcast_request_to_all(ctx, msg).await?
        } else {
            resp
        }
    };

    let operation_ack = resp.json::<OperationAck>().await?;
    // TODO: We could update only when it changes using some RwLock
    if let Some(leader_url) = ctx.nodes_urls.get(&operation_ack.leader_id) {
        debug!(
            leader_id = operation_ack.leader_id,
            leader_url, "updating last known leader url"
        );
        *ctx.pbft_leader_url.lock().unwrap() = leader_url.clone();
    }

    Ok(operation_ack)
}

async fn broadcast_request_to_all(
    ctx: &HandlerContext,
    msg: &ClientRequest,
) -> Result<reqwest::Response, Error> {
    let futs = FuturesUnordered::new();

    for (_, url) in ctx.nodes_urls.iter() {
        let fut = ctx
            .client
            .post(format!("{}/api/v1/consensus/operation", url))
            .json(msg)
            .timeout(Duration::from_secs(5))
            .send();
        futs.push(fut);
    }

    for fut in futs {
        match fut.await {
            Ok(resp) => {
                if resp.status().is_success() {
                    return Ok(resp);
                }
                error!(status = ?resp.status(), "failed to send request to replica");
            }
            Err(err) => {
                error!(error = ?err, "failed to send request to replica");
            }
        }
    }

    Err(Error::FailedToBroadcast)
}

async fn handle_kv_get(
    ctx: axum::extract::State<HandlerContext>,
    headers: HeaderMap,
    Query(key_request): Query<KeyRequest>,
) -> impl axum::response::IntoResponse {
    let operation = pbft_core::Operation::Get {
        key: key_request.key,
    };

    handle_kv_operation(ctx, headers, operation).await
}

async fn handle_kv_get_local(
    ctx: axum::extract::State<HandlerContext>,
    Query(key_request): Query<KeyRequest>,
) -> impl axum::response::IntoResponse {
    let val = ctx.kv_store.read().unwrap().get(&key_request.key);

    Json(Value {
        key: key_request.key,
        value: val,
    })
}

// Consensus client router handlers

async fn handle_client_consensus_response(
    ctx: axum::extract::State<HandlerContext>,
    JsonAuthenticatedExt(client_response): JsonAuthenticatedExt<ClientResponse>,
) -> Result<StatusCode, StatusCode> {
    let replica_id = client_response.sender_id;
    let client_response = client_response.data;
    let req_id = client_response.request.request_id.clone();

    let seq = client_response.result.sequence_number;

    debug!(
        sequence = seq,
        sender_id = replica_id,
        "received client response"
    );

    let mut replica_responses = ctx.replica_responses.lock().unwrap();
    let replica_responses = replica_responses
        .entry(client_response.result.clone())
        .or_insert(HashSet::new());
    replica_responses.insert(replica_id);

    if replica_responses.len() >= ctx.pbft_module.quorum_size() {
        let channels = ctx.resp_channels.lock().unwrap();
        match channels.get(&req_id) {
            Some(chan) => {
                // TODO: This could potentially block if the channel is full
                let _ = chan.send(client_response).map_err(|_| {
                    debug!(
                        sequence = seq,
                        sender_id = replica_id,
                        "failed to send response to channel, likely noone is listening",
                    )
                });
            }
            None => {
                // Noone is waiting for this response
                debug!(
                    sequence = seq,
                    sender_id = replica_id,
                    "no channel found for client request key that we received response for",
                );
            }
        }
    }
    Ok(StatusCode::OK)
}

// Consensus external router handlers

async fn handle_consensus_operation_execute(
    ctx: axum::extract::State<HandlerContext>,
    Json(client_request): Json<ClientRequest>,
) -> impl axum::response::IntoResponse {
    match ctx.pbft_module.handle_client_request(client_request).await {
        Ok(ack) => Ok(Json(ack)),
        Err(err) => {
            error!(error = ?err, "failed to handle client request");
            Err((StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response())
        }
    }
}

// Consensus internal router handlers

async fn handle_consensus_pre_prepare(
    ctx: axum::extract::State<HandlerContext>,
    JsonAuthenticatedExt(client_request): JsonAuthenticatedExt<ClientRequestBroadcast>,
) -> impl axum::response::IntoResponse {
    match ctx
        .pbft_module
        .handle_client_request_broadcast(client_request.sender_id, client_request.data)
    {
        Ok(_) => Ok(StatusCode::OK),
        Err(err) => {
            error!(error = ?err, "failed to handle client request broadcast");
            Err((StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response())
        }
    }
}

async fn handle_consensus_message(
    ctx: axum::extract::State<HandlerContext>,
    JsonAuthenticatedExt(client_request): JsonAuthenticatedExt<ConsensusMessageBroadcast>,
) -> impl axum::response::IntoResponse {
    match ctx
        .pbft_module
        .handle_consensus_message(client_request.sender_id, client_request.data.message)
    {
        Ok(_) => Ok(StatusCode::OK),
        Err(err) => {
            error!(error = ?err, "failed to handle consensus request");
            Err((StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response())
        }
    }
}

async fn handle_state_dump(
    ctx: axum::extract::State<HandlerContext>,
) -> impl axum::response::IntoResponse {
    let state = ctx.pbft_module.get_state();

    Json(state)
}

fn get_sender_replica_id(headers_map: &HeaderMap) -> Result<u64, axum::response::Response> {
    headers_map
        .get(pbft_core::api::REPLICA_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .ok_or((StatusCode::BAD_REQUEST, "Missing replica ID header").into_response())
}

fn get_replica_signature(headers_map: &HeaderMap) -> Result<String, axum::response::Response> {
    headers_map
        .get(pbft_core::api::REPLICA_SIGNATURE_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_string())
        .ok_or((StatusCode::BAD_REQUEST, "Missing replica signature header").into_response())
}

fn get_request_id(headers_map: &HeaderMap) -> Result<uuid::Uuid, axum::response::Response> {
    headers_map
        .get(REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(uuid::Uuid::parse_str)
        // If client does not specify request ID, we assume it is a new request
        // and generate a new ID.
        .unwrap_or(Ok(uuid::Uuid::new_v4()))
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()).into_response())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ed25519_dalek::Signer;
    use pbft_core::{
        api::{ConsensusMessageBroadcast, REPLICA_ID_HEADER, REPLICA_SIGNATURE_HEADER},
        config::{key_pair_from_priv_hex, NodeId},
        dev::DEV_PRIVATE_KEYS,
        ProtocolMessage, SignMessage,
    };

    use crate::{api_tests::tests::poll_node_health, dev, node::start_kv_node};

    #[tokio::test]
    async fn test_authentication() {
        // pbft_core::test_util::init_tracing(tracing::Level::INFO);

        let keypair1 = key_pair_from_priv_hex(DEV_PRIVATE_KEYS[1].as_bytes()).unwrap();

        let replicas_conf = vec![
            dev::dev_config(0, 8888),
            dev::dev_config(1, 8888),
            dev::dev_config(2, 8888),
        ];

        let node_url = replicas_conf[0].node_url();

        let (tx_cancel, rx_cancel) = tokio::sync::broadcast::channel(1);

        let node_handle = tokio::spawn(start_kv_node(replicas_conf[0].clone(), rx_cancel));

        tokio::time::timeout(Duration::from_secs(10), poll_node_health(&node_url))
            .await
            .expect("failed while waiting for node health");

        // Pretend that we are another replica sending consensus message
        let endpoint = format!("{}/api/v1/pbft/message", node_url);
        let msg = ConsensusMessageBroadcast {
            message: ProtocolMessage::Prepare(
                pbft_core::Prepare {
                    replica_id: NodeId(1),
                    metadata: pbft_core::MessageMeta {
                        view: 1,
                        sequence: 1,
                        digest: pbft_core::MessageDigest([0; 16]),
                    },
                }
                .sign(&keypair1)
                .unwrap(),
            ),
        };
        let body = serde_json::to_string(&msg).unwrap();

        // No headers
        let client = reqwest::Client::new();
        let resp = client
            .post(&endpoint)
            .body(body.clone())
            .send()
            .await
            .expect("failed to send request");
        assert_eq!(resp.status(), 400);
        let resp_body = resp.text().await.unwrap();
        assert!(resp_body.contains("Missing replica signature header"));

        // Missing ID header
        let signature = keypair1.sign(body.as_bytes());
        let signature_hex = hex::encode(signature.to_bytes());

        let client = reqwest::Client::new();
        let resp = client
            .post(&endpoint)
            .header(REPLICA_SIGNATURE_HEADER, signature_hex.to_string())
            .body(body.clone())
            .send()
            .await
            .expect("failed to send request");
        assert_eq!(resp.status(), 400);
        let resp_body = resp.text().await.unwrap();
        assert!(resp_body.contains("Missing replica ID header"));

        // Replica ID does not match signature (keypair = 1; replica ID = 2)
        let client = reqwest::Client::new();
        let resp = client
            .post(&endpoint)
            .header(REPLICA_ID_HEADER, 2)
            .header(REPLICA_SIGNATURE_HEADER, signature_hex.to_string())
            .body(body.clone())
            .send()
            .await
            .expect("failed to send request");
        assert_eq!(resp.status(), 400);
        let resp_body = resp.text().await.unwrap();
        assert!(resp_body.contains("Invalid signature"));

        // Signature does not match body
        let msg2 = ConsensusMessageBroadcast {
            message: ProtocolMessage::Prepare(
                pbft_core::Prepare {
                    replica_id: NodeId(1),
                    metadata: pbft_core::MessageMeta {
                        view: 1,
                        sequence: 2,
                        digest: pbft_core::MessageDigest([0; 16]),
                    },
                }
                .sign(&keypair1)
                .unwrap(),
            ),
        };
        let body2 = serde_json::to_string(&msg2).unwrap();

        let client = reqwest::Client::new();
        let resp = client
            .post(&endpoint)
            .header(REPLICA_ID_HEADER, 1)
            .header(REPLICA_SIGNATURE_HEADER, signature_hex.to_string())
            .body(body2.clone())
            .send()
            .await
            .expect("failed to send request");
        assert_eq!(resp.status(), 400);
        let resp_body = resp.text().await.unwrap();
        assert!(resp_body.contains("Invalid signature"));

        // Everything should pass
        let client = reqwest::Client::new();
        let resp = client
            .post(&endpoint)
            .header(REPLICA_ID_HEADER, 1)
            .header(REPLICA_SIGNATURE_HEADER, signature_hex.to_string())
            .body(body.clone())
            .send()
            .await
            .expect("failed to send request");
        assert_eq!(resp.status(), 200);

        // Stop the node and wait for it to finish
        tx_cancel.send(()).unwrap();
        tokio::join!(node_handle).0.unwrap().unwrap();
    }
}
