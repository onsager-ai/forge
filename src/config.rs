//! Forge configuration, loaded from environment variables.

use anyhow::{Context, Result};
use std::time::Duration;

/// Top-level configuration for the Forge process.
#[derive(Debug, Clone)]
pub struct ForgeConfig {
    /// PostgreSQL connection URL.
    pub database_url: String,
    /// How often the scheduling kernel ticks when idle.
    pub idle_tick_interval: Duration,
    /// How often idle ticks emit factory events (reduced frequency).
    pub idle_event_interval: Duration,
    /// Maximum concurrent in-flight shaping requests.
    pub max_in_flight: usize,
    /// Base URL for Stiglab (e.g., "http://localhost:3000").
    pub stiglab_url: String,
    /// Base URL for Synodic (e.g., "http://localhost:3001").
    pub synodic_url: String,
    /// Timeout for shaping requests to Stiglab, in seconds.
    pub shaping_timeout_secs: u64,
}

impl ForgeConfig {
    /// Load configuration from environment variables.
    pub fn from_env() -> Result<Self> {
        let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;

        let idle_tick_ms: u64 = std::env::var("FORGE_IDLE_TICK_MS")
            .unwrap_or_else(|_| "1000".into())
            .parse()
            .context("FORGE_IDLE_TICK_MS must be a number")?;

        let idle_event_ms: u64 = std::env::var("FORGE_IDLE_EVENT_MS")
            .unwrap_or_else(|_| "30000".into())
            .parse()
            .context("FORGE_IDLE_EVENT_MS must be a number")?;

        let max_in_flight: usize = std::env::var("FORGE_MAX_IN_FLIGHT")
            .unwrap_or_else(|_| "10".into())
            .parse()
            .context("FORGE_MAX_IN_FLIGHT must be a number")?;

        let stiglab_url =
            std::env::var("STIGLAB_URL").unwrap_or_else(|_| "http://localhost:3000".into());

        let synodic_url =
            std::env::var("SYNODIC_URL").unwrap_or_else(|_| "http://localhost:3001".into());

        let shaping_timeout_secs: u64 = std::env::var("FORGE_SHAPING_TIMEOUT_SECS")
            .unwrap_or_else(|_| "300".into())
            .parse()
            .context("FORGE_SHAPING_TIMEOUT_SECS must be a number")?;

        Ok(Self {
            database_url,
            idle_tick_interval: Duration::from_millis(idle_tick_ms),
            idle_event_interval: Duration::from_millis(idle_event_ms),
            max_in_flight,
            stiglab_url,
            synodic_url,
            shaping_timeout_secs,
        })
    }

    /// Return the database URL with the password masked for logging.
    pub fn database_url_masked(&self) -> String {
        if let Some(at_pos) = self.database_url.rfind('@') {
            if let Some(colon_pos) = self.database_url[..at_pos].rfind(':') {
                let mut masked = self.database_url.clone();
                masked.replace_range(colon_pos + 1..at_pos, "***");
                return masked;
            }
        }
        self.database_url.clone()
    }
}
