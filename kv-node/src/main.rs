mod api;
mod config;
mod dev;
mod kv;
pub(crate) mod node;

#[cfg(test)]
mod api_tests;

#[tokio::main]
async fn main() {
    let subscriber = tracing_subscriber::fmt::SubscriberBuilder::default()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                // Use INFO level as default
                .add_directive(tracing::Level::INFO.into()),
        )
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("failed to initialize logger");

    let config = if let Some(dev_replica) = std::env::var_os("PBFT_DEV") {
        let dev_replica = dev_replica
            .to_str()
            .expect("failed to convert dev replica to string")
            .parse::<u64>()
            .expect("failed to parse dev replica as u64");

        dev::dev_config(dev_replica, 10000)
    } else {
        config::AppConfig::new(None).unwrap()
    };

    let (tx_cancel, rx_cancel) = tokio::sync::broadcast::channel(1);

    let mut signals = signal_hook::iterator::Signals::new([
        signal_hook::consts::SIGINT,
        signal_hook::consts::SIGTERM,
    ])
    .expect("failed to register signal handler");
    tokio::spawn(async move {
        for sig in signals.forever() {
            match tx_cancel.send(()) {
                Ok(_) => {}
                Err(e) => {
                    tracing::error!(error = ?e, "failed to send cancel signal");
                }
            }
            if sig == signal_hook::consts::SIGINT || sig == signal_hook::consts::SIGTERM {
                tracing::info!(signal = sig, "Received signal. Shutting down.");
                break;
            }
        }
    });

    node::start_kv_node(config, rx_cancel)
        .await
        .expect("failed to start kv node");
}
