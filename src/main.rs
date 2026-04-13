//! Forge — the production line subsystem of the Onsager AI factory.
//!
//! Forge drives artifacts through their lifecycle: register, decide, dispatch,
//! gate, advance, route. See `specs/forge-v0.1.md` for the full specification.

// Public API surface for subsystem integration — will be used as Stiglab,
// Synodic, and consumer sinks are connected.
#![allow(dead_code)]

mod artifact_store;
mod config;
mod core_loop;
mod kernel;
mod router;

use anyhow::Result;
use tracing_subscriber::EnvFilter;

use crate::config::ForgeConfig;
use crate::core_loop::ForgeLoop;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("forge=info,onsager=info")),
        )
        .init();

    let config = ForgeConfig::from_env()?;

    tracing::info!(
        "Forge starting — database: {}",
        config.database_url_masked()
    );

    let forge = ForgeLoop::new(&config).await?;
    forge.run().await
}
