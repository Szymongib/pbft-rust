use std::net::Ipv4Addr;

use pbft_core::config::NodeId;

use crate::config::AppConfig;

pub fn dev_config(replica_self_id: u64, start_port: u16) -> AppConfig {
    let pbft_config = pbft_core::dev::dev_config(NodeId(replica_self_id), start_port);

    AppConfig {
        listen_addr: std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        port: start_port + replica_self_id as u16,
        pbft_config,
    }
}
