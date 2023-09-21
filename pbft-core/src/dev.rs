use std::time::Duration;

use crate::{
    config::{pub_key_from_priv_hex, ExecutorConfig, NodeConfig, NodeId, PbftNodeConfig},
    Config,
};

pub const DEV_VIEW_CHANGE_TIMEOUT: Duration = Duration::from_secs(5);

pub static DEV_PRIVATE_KEYS: [&str; 5] = [
    "f27c6cd411d662857649213ebb4c85152a0dc07a832b1c7c7536513981e5310e",
    "591d6957eccbf0f4b3dbe03d9dff47cba25564035787f9b4ce04436428ae144f",
    "4beb12a2dd43aac5765556c71ff61c8ec22524bc9d9d0542de61159a83bb37b6",
    "9bfc32b4bd787a9011ba167c025eea1617232aaa5cfdc50d8e4ae72a7881ac33",
    "cc55911ebe495c4b98aad7792a7102f2b8ea48eb97381f1599dedec67def749b",
];

pub fn dev_config(self_id: NodeId, start_port: u16) -> Config {
    if self_id.0 >= DEV_PRIVATE_KEYS.len() as u64 {
        panic!(
            "Index out of bounds. Only {} dev replicas are supported",
            DEV_PRIVATE_KEYS.len()
        );
    }

    // Create temp file with private key
    let private_key_path = std::env::temp_dir().join(format!("private_key_replica_{}", self_id.0));

    let cfg = Config {
        view_change_timeout: DEV_VIEW_CHANGE_TIMEOUT,
        node_config: PbftNodeConfig {
            self_id,
            private_key_path: private_key_path.clone(),
            nodes: vec![
                NodeConfig {
                    id: NodeId(0),
                    addr: format!("http://localhost:{}", start_port),
                    public_key: pub_key_from_priv_hex(DEV_PRIVATE_KEYS[0])
                        .expect("failed to derive public key from private key"),
                },
                NodeConfig {
                    id: NodeId(1),
                    addr: format!("http://localhost:{}", start_port + 1),
                    public_key: pub_key_from_priv_hex(DEV_PRIVATE_KEYS[1])
                        .expect("failed to derive public key from private key"),
                },
                NodeConfig {
                    id: NodeId(2),
                    addr: format!("http://localhost:{}", start_port + 2),
                    public_key: pub_key_from_priv_hex(DEV_PRIVATE_KEYS[2])
                        .expect("failed to derive public key from private key"),
                },
                NodeConfig {
                    id: NodeId(3),
                    addr: format!("http://localhost:{}", start_port + 3),
                    public_key: pub_key_from_priv_hex(DEV_PRIVATE_KEYS[3])
                        .expect("failed to derive public key from private key"),
                },
                NodeConfig {
                    id: NodeId(4),
                    addr: format!("http://localhost:{}", start_port + 4),
                    public_key: pub_key_from_priv_hex(DEV_PRIVATE_KEYS[4])
                        .expect("failed to derive public key from private key"),
                },
            ],
        },
        checkpoint_frequency: 10,
        response_urls: vec![
            format!("http://localhost:{}/api/v1/client/response", start_port),
            format!("http://localhost:{}/api/v1/client/response", start_port + 1),
            format!("http://localhost:{}/api/v1/client/response", start_port + 2),
            format!("http://localhost:{}/api/v1/client/response", start_port + 3),
            format!("http://localhost:{}/api/v1/client/response", start_port + 4),
        ],
        executor_config: ExecutorConfig {
            max_requeue_attempts_on_failure: 5,
        },
    };

    std::fs::write(private_key_path, DEV_PRIVATE_KEYS[self_id.0 as usize])
        .expect("failed to write private key to file");

    cfg
}
