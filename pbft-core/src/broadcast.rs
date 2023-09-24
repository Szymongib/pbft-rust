use std::{pin::Pin, sync::Arc, time::Duration};

use ed25519_dalek::Signer;
use futures::{stream::FuturesUnordered, Future, StreamExt};
use serde::Serialize;
use tracing::{debug, error};

use crate::{
    api::{
        ClientRequestBroadcast, ProtocolMessageBroadcast, REPLICA_ID_HEADER,
        REPLICA_SIGNATURE_HEADER,
    },
    config::{NodeConfig, NodeId},
    ClientRequest, ClientResponse, OperationAck,
};

#[derive(Debug, thiserror::Error)]
pub enum BroadcastError {
    #[error("Failed to send message to {url}, error: {error}")]
    SendError { url: String, error: reqwest::Error },

    #[error("Failed to parse response: {context}, error: {error}")]
    ResponseError {
        context: &'static str,
        error: reqwest::Error,
    },

    #[error("Serde error, context {context}, error: {error}")]
    SerdeError {
        context: &'static str,
        error: serde_json::Error,
    },

    #[error("Unexpected status code when sending message to {url}, status: {status_code}")]
    UnexpectedStatusError {
        url: String,
        status_code: reqwest::StatusCode,
    },

    #[error("Retries limit exceeded when sending message to {url}, attempts: {attempts}, last error: {last_error}")]
    RetriesLimitExceededError {
        url: String,
        attempts: i32,
        last_error: Box<BroadcastError>,
    },
}

impl BroadcastError {
    pub fn send_error(url: String) -> impl FnOnce(reqwest::Error) -> Self + 'static {
        move |error| Self::SendError { url, error }
    }

    pub fn response_error(context: &'static str) -> impl FnOnce(reqwest::Error) -> Self + 'static {
        move |error| Self::ResponseError { context, error }
    }

    pub fn serde_error(context: &'static str) -> impl FnOnce(serde_json::Error) -> Self + '_ {
        move |error| Self::SerdeError { context, error }
    }
}

pub trait PbftBroadcaster: Send + Sync {
    fn broadcast_consensus_message(&self, msg: ProtocolMessageBroadcast);
    fn broadcast_operation(&self, msg: ClientRequestBroadcast);
    fn send_client_responses(&self, responses: Vec<ClientResponse>);
}

type Result<T> = std::result::Result<T, BroadcastError>;

pub struct Broadcaster {
    node_self_id: NodeId,
    nodes: Vec<NodeConfig>,
    response_urls: Vec<String>,
    keypair: Arc<ed25519_dalek::Keypair>,

    client: reqwest::Client,
}

impl Broadcaster {
    pub fn new(
        node_id: NodeId,
        nodes: Vec<NodeConfig>,
        keypair: Arc<ed25519_dalek::Keypair>,
        response_urls: Vec<String>,
    ) -> Self {
        Self {
            node_self_id: node_id,
            nodes,
            keypair,
            response_urls,

            client: reqwest::Client::new(),
        }
    }

    pub async fn forward_to_node(
        &self,
        request: ClientRequest,
        node_id: NodeId,
    ) -> Result<OperationAck> {
        let node = &self.nodes[node_id.0 as usize];

        let url = format!("{}/api/v1/consensus/operation", node.addr);
        let resp = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(BroadcastError::send_error(url.clone()))?;

        if resp.status().is_success() {
            let ack = resp
                .json::<OperationAck>()
                .await
                .map_err(BroadcastError::response_error(
                    "failed to parse JSON response when forwarding client request",
                ))?;
            Ok(ack)
        } else {
            Err(BroadcastError::UnexpectedStatusError {
                url,
                status_code: resp.status(),
            })
        }
    }

    fn broadcast<M, F>(&self, msg: M, path: &'static str, ignore_self: bool, broadcast_fn: F)
    where
        M: Clone + Serialize + Send + Sync,
        F: Fn(
            reqwest::Client,
            M,
            String,
            NodeId,
            Arc<ed25519_dalek::Keypair>,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>,
    {
        let mut futures = FuturesUnordered::new();
        for peer in self.nodes.iter() {
            // Do not send message to yourself
            if peer.id == self.node_self_id && ignore_self {
                continue;
            }
            let url = format!("{}/{}", peer.addr, path);
            let msg = msg.clone();
            let node_id = self.node_self_id;

            futures.push(broadcast_fn(
                self.client.clone(),
                msg,
                url,
                node_id,
                self.keypair.clone(),
            ))
        }

        tokio::spawn(async move {
            while let Some(out) = futures.next().await {
                match out {
                    Ok(_) => {}
                    Err(err) => {
                        error!(error = ?err, "failed to send consensus message to peer in all attempts");
                    }
                }
            }
        });
    }

    pub async fn send_msg<T: Serialize>(
        client: &reqwest::Client,
        self_id: NodeId,
        keypair: &ed25519_dalek::Keypair,
        msg: &T,
        url: &str,
    ) -> Result<()> {
        let body = serde_json::to_vec(msg).map_err(BroadcastError::serde_error(
            "failed to serialize request body",
        ))?;

        let signature = keypair.sign(&body).to_bytes().to_vec();
        let signature_hex = hex::encode(signature);

        let res = client
            .post(url)
            .header(REPLICA_ID_HEADER, self_id.0)
            .header(REPLICA_SIGNATURE_HEADER, signature_hex.to_string())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(BroadcastError::send_error(url.to_string()))?;

        if res.status().is_success() {
            Ok(())
        } else {
            Err(BroadcastError::UnexpectedStatusError {
                url: url.to_string(),
                status_code: res.status(),
            })
        }
    }

    async fn send_with_retires<T: Serialize + std::fmt::Debug>(
        client: reqwest::Client,
        self_id: NodeId,
        keypair: &ed25519_dalek::Keypair,
        msg: &T,
        url: &str,
    ) -> Result<()> {
        let mut attempt = 1;
        let mut backoff = Duration::from_millis(200);

        debug!(
            self_id = self_id.0,
            url = url,
            msg = ?msg,
            "attempting to send message",
        );

        while let Err(err) = Broadcaster::send_msg(&client, self_id, keypair, msg, url).await {
            error!(
                error = ?err,
                self_id = self_id.0,
                url = url,
                attempt,
                "replica failed to sent message"
            );

            if attempt > 5 {
                return Err(BroadcastError::RetriesLimitExceededError {
                    url: url.to_string(),
                    attempts: attempt,
                    last_error: Box::new(err),
                });
            }

            attempt += 1;
            tokio::time::sleep(backoff).await;
            backoff *= 2;
        }
        Ok(())
    }
}

impl PbftBroadcaster for Broadcaster {
    fn send_client_responses(&self, responses: Vec<ClientResponse>) {
        let mut futs = FuturesUnordered::new();
        for resp in responses {
            for url in &self.response_urls {
                let client = self.client.clone();
                let self_id = self.node_self_id;
                let keypair = self.keypair.clone();
                let resp = resp.clone();
                let url = url.clone();

                futs.push(Box::pin(async move {
                    Broadcaster::send_with_retires(client, self_id, &keypair, &resp, url.as_str())
                        .await
                }));
            }
        }

        tokio::spawn(async move {
            while let Some(out) = futs.next().await {
                match out {
                    Ok(_) => {}
                    Err(err) => {
                        error!(error = ?err, "failed to send response to client in all attempts");
                    }
                }
            }
        });
    }

    fn broadcast_consensus_message(&self, msg: ProtocolMessageBroadcast) {
        self.broadcast(
            msg,
            "api/v1/pbft/message",
            true,
            |client, msg, url, self_id, keypair| {
                Box::pin(async move {
                    Broadcaster::send_with_retires(client, self_id, &keypair, &msg, &url).await
                })
            },
        )
    }

    fn broadcast_operation(&self, msg: ClientRequestBroadcast) {
        self.broadcast(
            msg,
            "api/v1/pbft/execute",
            true,
            |client, msg, url, self_id, keypair| {
                Box::pin(async move {
                    Broadcaster::send_with_retires(client, self_id, &keypair, &msg, &url).await
                })
            },
        )
    }
}
