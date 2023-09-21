use serde::{self, Deserialize};
use std::{
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
};

#[derive(Deserialize, Debug, Clone)]
pub struct AppConfig {
    #[serde(default = "default_addr")]
    pub listen_addr: IpAddr,
    #[serde(default = "default_port")]
    pub port: u16,

    pub pbft_config: pbft_core::Config,
}

fn default_port() -> u16 {
    3000
}

fn default_addr() -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0))
}

impl AppConfig {
    pub fn new(config_file: Option<PathBuf>) -> std::result::Result<Self, config::ConfigError> {
        let mut config_builder = config::Config::builder();

        if let Some(config_file) = config_file {
            config_builder = config_builder.add_source(config::File::from(config_file));
        }

        let config = config_builder
            .add_source(
                config::Environment::with_prefix("KV_STORE")
                    .separator(".")
                    .prefix_separator("_"),
            )
            .build()?;

        config.try_deserialize()
    }

    pub fn node_url(&self) -> String {
        format!("http://{}:{}", self.listen_addr, self.port)
    }
}
