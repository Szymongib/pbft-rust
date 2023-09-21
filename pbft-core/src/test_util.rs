use std::path::PathBuf;

use std::sync::Once;

lazy_static::lazy_static! {
        static ref TRACING_SUBSCRIBER_INIT: Once = Once::new();
}

/// Generates private key and stores it in a temp file. Returns path to the
/// file and public key as hex string.
pub fn generate_priv_key(replica_id: u64) -> (PathBuf, String) {
    let mut csrng = rand::rngs::OsRng {};
    let keypair = ed25519_dalek::Keypair::generate(&mut csrng);

    let private_key_path = std::env::temp_dir().join(format!("private_key_replica_{}", replica_id));
    std::fs::write(&private_key_path, hex::encode(keypair.secret.as_bytes()))
        .expect("failed to save private key to file");

    (private_key_path, hex::encode(keypair.public.as_bytes()))
}

pub fn init_tracing(level: tracing::Level) {
    TRACING_SUBSCRIBER_INIT.call_once(|| {
        let subscriber = tracing_subscriber::fmt::SubscriberBuilder::default()
            .with_env_filter(
                tracing_subscriber::EnvFilter::from_default_env()
                    // Use INFO level as default
                    .add_directive(level.into()),
            )
            .finish();
        tracing::subscriber::set_global_default(subscriber).expect("failed to initialize logger");
    })
}
