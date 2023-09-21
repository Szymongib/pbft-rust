use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::error::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub node_config: PbftNodeConfig,
    pub checkpoint_frequency: u64,
    #[serde(default = "default_view_change_timeout")]
    pub view_change_timeout: Duration,
    pub response_urls: Vec<String>,
    pub executor_config: ExecutorConfig,
}

fn default_view_change_timeout() -> Duration {
    Duration::from_secs(10)
}

#[derive(Debug, Clone, Serialize, Deserialize, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub u64);

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PbftNodeConfig {
    pub self_id: NodeId,
    pub private_key_path: PathBuf,
    pub nodes: Vec<NodeConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub id: NodeId,
    pub addr: String,
    pub public_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorConfig {
    #[serde(default = "GenericDefault::<5>::value")]
    pub max_requeue_attempts_on_failure: u32,
}

impl PbftNodeConfig {
    pub fn get_keypair(&self) -> Result<ed25519_dalek::Keypair, crate::error::Error> {
        let keypair = load_keypair(self.private_key_path.as_path())?;

        // Verify that the public key configured for this peer matches the one
        // derived from the private key.
        let node_id = self.self_id;
        let configured_pub_key = hex::decode(&self.nodes[node_id.0 as usize].public_key)
            .map_err(Error::hex_error("failed to decode public key from config"))?;

        let expected_pub = ed25519_dalek::PublicKey::from_bytes(&configured_pub_key)
            .expect("failed to parse public key from bytes");

        if keypair.public != expected_pub {
            return Err(crate::error::Error::ReplicaPrivKeyDoesNotMatchPubKey {
                actual: hex::encode(keypair.public.as_bytes()),
                expected: hex::encode(expected_pub.as_bytes()),
            });
        }
        Ok(keypair)
    }

    pub fn trusted_pub_keys(&self) -> HashMap<&str, NodeId> {
        self.nodes
            .iter()
            .map(|node| (node.public_key.as_str(), node.id))
            .collect()
    }
}

pub fn load_keypair(path: &Path) -> Result<ed25519_dalek::Keypair, crate::error::Error> {
    let private_key_hex = std::fs::read(path).map_err(Error::io_error(&format!(
        "failed to read private key from file: {:?}",
        path
    )))?;
    key_pair_from_priv_hex(private_key_hex.as_slice())
}

pub fn key_pair_from_priv_hex(
    private_key_hex: &[u8],
) -> Result<ed25519_dalek::Keypair, crate::error::Error> {
    let private_key = hex::decode(private_key_hex).map_err(Error::hex_error(
        "failed to decode private key from hex-encoded file contents",
    ))?;

    let secret_key = ed25519_dalek::SecretKey::from_bytes(&private_key).map_err(
        crate::error::Error::ed25519_error("failed to parse private key from file contents"),
    )?;

    let pk: ed25519_dalek::PublicKey = (&secret_key).into();
    let keypair = ed25519_dalek::Keypair {
        secret: secret_key,
        public: pk,
    };

    Ok(keypair)
}

pub fn pub_key_from_priv_hex(priv_key_hex: &str) -> Result<String, crate::error::Error> {
    let keypair = key_pair_from_priv_hex(priv_key_hex.as_bytes())?;
    let pub_key_hex = hex::encode(keypair.public.as_bytes());
    Ok(pub_key_hex)
}

struct GenericDefault<const U: u32>;

impl<const U: u32> GenericDefault<U> {
    fn value() -> u32 {
        U
    }
}
