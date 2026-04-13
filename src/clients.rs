//! HTTP clients for Stiglab and Synodic subsystems.

use anyhow::Result;
use onsager::protocol::{GateRequest, GateVerdict, ShapingRequest, ShapingResult};
use std::time::Duration;

/// HTTP client for the Stiglab shaping service.
pub struct StiglabClient {
    client: reqwest::Client,
    base_url: String,
}

impl StiglabClient {
    pub fn new(base_url: &str, timeout: Duration) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .expect("failed to build HTTP client"),
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    /// Dispatch a shaping request to Stiglab and await the result.
    pub async fn dispatch_shaping(&self, req: &ShapingRequest) -> Result<ShapingResult> {
        let url = format!("{}/api/shaping", self.base_url);
        let resp = self.client.post(&url).json(req).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Stiglab returned {status}: {body}");
        }
        let result = resp.json::<ShapingResult>().await?;
        Ok(result)
    }
}

/// HTTP client for the Synodic governance service.
pub struct SynodicClient {
    client: reqwest::Client,
    base_url: String,
}

impl SynodicClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("failed to build HTTP client"),
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    /// Send a gate request to Synodic and await the verdict.
    pub async fn gate(&self, req: &GateRequest) -> Result<GateVerdict> {
        let url = format!("{}/gate", self.base_url);
        let resp = self.client.post(&url).json(req).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Synodic returned {status}: {body}");
        }
        let verdict = resp.json::<GateVerdict>().await?;
        Ok(verdict)
    }
}
