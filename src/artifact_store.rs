//! Artifact store — Forge's read/write access to artifact state.
//!
//! Forge is the single writer to artifact state (artifact-model §8).
//! This module provides the database operations for register, advance,
//! version creation, and world state queries.

use anyhow::Result;
use chrono::{DateTime, Utc};
use onsager::{ArtifactId, ArtifactState, Kind};
use sqlx::PgPool;
use std::collections::HashSet;

// ---------------------------------------------------------------------------
// World state — the scheduling kernel's view of the factory
// ---------------------------------------------------------------------------

/// A lightweight projection of an artifact for scheduling decisions.
#[derive(Debug, Clone)]
pub struct SchedulableArtifact {
    pub artifact_id: ArtifactId,
    pub kind: Kind,
    pub owner: String,
    pub state: ArtifactState,
    pub current_version: u32,
    pub priority: i32,
    pub deadline: Option<DateTime<Utc>>,
    pub shaping_intent: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

/// The world state visible to the scheduling kernel.
#[derive(Debug, Clone)]
pub struct WorldState {
    pub artifacts: Vec<SchedulableArtifact>,
    pub in_flight: HashSet<ArtifactId>,
}

// ---------------------------------------------------------------------------
// Artifact store
// ---------------------------------------------------------------------------

/// Forge's interface to the artifact tables.
///
/// All write operations happen in the same transaction as the corresponding
/// factory event (outbox pattern, forge invariant #3).
#[derive(Clone)]
pub struct ArtifactStore {
    pool: PgPool,
}

impl ArtifactStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Register a new artifact. Assigns the ID and sets state to `draft`.
    pub async fn register(
        &self,
        artifact_id: &ArtifactId,
        kind: &str,
        name: &str,
        owner: &str,
        created_by: &str,
        consumers: &serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO artifacts (artifact_id, kind, name, owner, created_by, consumers, state)
            VALUES ($1, $2, $3, $4, $5, $6, 'draft')
            "#,
        )
        .bind(artifact_id.as_str())
        .bind(kind)
        .bind(name)
        .bind(owner)
        .bind(created_by)
        .bind(consumers)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Advance an artifact to a new state. Returns an error if the transition
    /// is invalid (enforces the state machine from artifact-model §4.5).
    pub async fn advance_state(
        &self,
        artifact_id: &ArtifactId,
        new_state: ArtifactState,
    ) -> Result<ArtifactState> {
        let row: (String,) =
            sqlx::query_as("SELECT state FROM artifacts WHERE artifact_id = $1 FOR UPDATE")
                .bind(artifact_id.as_str())
                .fetch_one(&self.pool)
                .await?;

        let current: ArtifactState = serde_json::from_value(serde_json::Value::String(row.0))?;

        anyhow::ensure!(
            current.can_transition_to(new_state),
            "invalid state transition: {current} -> {new_state}"
        );

        sqlx::query("UPDATE artifacts SET state = $1 WHERE artifact_id = $2")
            .bind(new_state.to_string())
            .bind(artifact_id.as_str())
            .execute(&self.pool)
            .await?;

        Ok(current)
    }

    /// Create a new version for an artifact. Increments current_version.
    pub async fn create_version(
        &self,
        artifact_id: &ArtifactId,
        version: u32,
        content_ref_uri: &str,
        content_ref_checksum: Option<&str>,
        change_summary: &str,
        session_id: &str,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            r#"
            INSERT INTO artifact_versions
                (artifact_id, version, content_ref_uri, content_ref_checksum, change_summary, created_by_session)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(artifact_id.as_str())
        .bind(version as i32)
        .bind(content_ref_uri)
        .bind(content_ref_checksum)
        .bind(change_summary)
        .bind(session_id)
        .execute(&mut *tx)
        .await?;

        sqlx::query("UPDATE artifacts SET current_version = $1 WHERE artifact_id = $2")
            .bind(version as i32)
            .bind(artifact_id.as_str())
            .execute(&mut *tx)
            .await?;

        // Record vertical lineage
        sqlx::query(
            r#"
            INSERT INTO vertical_lineage (artifact_id, version, session_id)
            VALUES ($1, $2, $3)
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(artifact_id.as_str())
        .bind(version as i32)
        .bind(session_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    /// Load the world state for the scheduling kernel.
    pub async fn load_world_state(&self, in_flight: &HashSet<ArtifactId>) -> Result<WorldState> {
        // Query artifacts that are in schedulable states.
        let rows: Vec<ArtifactRow> = sqlx::query_as(
            r#"
            SELECT artifact_id, kind, owner, state, current_version, created_at
            FROM artifacts
            WHERE state IN ('draft', 'in_progress', 'under_review')
            ORDER BY created_at ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let artifacts = rows
            .into_iter()
            .map(|r| SchedulableArtifact {
                artifact_id: ArtifactId::new(&r.artifact_id),
                kind: serde_json::from_value(serde_json::Value::String(r.kind))
                    .unwrap_or(Kind::Code),
                owner: r.owner,
                state: serde_json::from_value(serde_json::Value::String(r.state))
                    .unwrap_or(ArtifactState::Draft),
                current_version: r.current_version as u32,
                priority: 0, // TODO: load from artifact metadata or scheduling config
                deadline: None,
                shaping_intent: serde_json::json!({}),
                created_at: r.created_at,
            })
            .collect();

        Ok(WorldState {
            artifacts,
            in_flight: in_flight.clone(),
        })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[derive(sqlx::FromRow)]
struct ArtifactRow {
    artifact_id: String,
    kind: String,
    owner: String,
    state: String,
    current_version: i32,
    created_at: DateTime<Utc>,
}
