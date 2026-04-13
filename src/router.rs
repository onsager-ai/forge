//! Consumer routing — dispatches released artifacts to their declared consumers.
//!
//! See `specs/forge-v0.1.md §2` (responsibility #6: Route) and forge invariant
//! #10 (at-least-once delivery to consumers).

use anyhow::Result;
use async_trait::async_trait;
use onsager::ArtifactId;

/// A consumer sink that receives released artifacts.
///
/// Sinks must be idempotent — Forge guarantees at-least-once delivery,
/// which means duplicates are possible.
#[async_trait]
pub trait ConsumerSink: Send + Sync {
    /// Route a released artifact to this sink.
    async fn route(&self, artifact_id: &ArtifactId, version: u32) -> Result<()>;

    /// Human-readable sink name for logging and events.
    fn name(&self) -> &str;
}

/// A no-op sink for development and testing.
pub struct LogSink;

#[async_trait]
impl ConsumerSink for LogSink {
    async fn route(&self, artifact_id: &ArtifactId, version: u32) -> Result<()> {
        tracing::info!(
            artifact_id = artifact_id.as_str(),
            version,
            "artifact routed to log sink"
        );
        Ok(())
    }

    fn name(&self) -> &str {
        "log"
    }
}
