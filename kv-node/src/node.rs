use std::{
    sync::{Arc, RwLock},
    vec,
};

use tracing::{debug, info};

use crate::{api, config::AppConfig};

pub async fn start_kv_node(
    config: AppConfig,
    mut rx_cancel: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), pbft_core::error::Error> {
    let kv_store = pbft_core::InMemoryKVStore::new();
    let kv_store = Arc::new(RwLock::new(kv_store));

    let (inner_tx_cancel, inner_rx_cancel) = tokio::sync::broadcast::channel(10);

    let pbft_module = pbft_core::Pbft::new(config.pbft_config.clone(), kv_store.clone())?;
    let pbft_module = Arc::new(pbft_module);

    let pbft_module_m = pbft_module.clone();
    let pbft_executor_rx_cancel = inner_tx_cancel.subscribe();
    let pbft_backup_queue_rx_cancel = inner_tx_cancel.subscribe();
    info!("starting pbft module...");
    let pbft_handle = tokio::spawn(async move {
        pbft_module_m
            .start(pbft_executor_rx_cancel, pbft_backup_queue_rx_cancel)
            .await
    });

    let mut api_server = api::ApiServer::new(config.clone(), pbft_module, kv_store).await;
    info!("starting api server...");
    let api_handle = tokio::spawn(async move { api_server.run(inner_rx_cancel).await });

    // This is not ideal in case api_handle or pbft_handle crashes
    rx_cancel.recv().await.unwrap();
    info!("received cancel signal");
    inner_tx_cancel.send(()).unwrap();

    for h in vec![api_handle, pbft_handle] {
        debug!("waiting for task to finish...");
        h.await.expect("failed to join task");
    }

    info!(
        node_id = config.pbft_config.node_config.self_id.0,
        "Node stopped..."
    );

    Ok(())
}
