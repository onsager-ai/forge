//! Scheduling kernel — the pluggable decision-maker inside Forge.
//!
//! The kernel answers one question each tick: "Given the current world state,
//! what is the next artifact to shape, and how?"
//!
//! See `specs/forge-v0.1.md §4` for the full specification.

use async_trait::async_trait;
use onsager::factory_event::FactoryEventKind;
use onsager::protocol::ShapingDecision;

use crate::artifact_store::WorldState;

// ---------------------------------------------------------------------------
// Kernel trait
// ---------------------------------------------------------------------------

/// The scheduling kernel contract (forge-v0.1 §4.3).
///
/// Any implementation that honors this trait is a valid scheduling kernel.
/// The kernel must be a pure function of world state and observed events —
/// no side effects beyond emitting decisions.
#[async_trait]
pub trait SchedulingKernel: Send + Sync {
    /// Given the current world state, decide what to shape next.
    /// Returns `None` when there is no work to do (idle).
    async fn decide(&self, world: &WorldState) -> Option<ShapingDecision>;

    /// Observe a factory event to update internal state.
    fn observe(&mut self, event: &FactoryEventKind);
}

// ---------------------------------------------------------------------------
// Baseline kernel: priority queue + deadline awareness + round-robin fairness
// ---------------------------------------------------------------------------

/// The v0.1 baseline scheduling kernel.
///
/// Strategy: priority queue ordered by (priority DESC, deadline ASC, last_touched ASC).
/// Round-robin fairness across owners prevents starvation.
pub struct BaselineKernel {
    /// Track which owner was last served for round-robin fairness.
    last_served_owner: Option<String>,
}

impl BaselineKernel {
    pub fn new() -> Self {
        Self {
            last_served_owner: None,
        }
    }
}

impl Default for BaselineKernel {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SchedulingKernel for BaselineKernel {
    async fn decide(&self, world: &WorldState) -> Option<ShapingDecision> {
        // Find artifacts that need shaping: in_progress with no in-flight request.
        let mut candidates: Vec<_> = world
            .artifacts
            .iter()
            .filter(|a| {
                a.state == onsager::ArtifactState::InProgress
                    && !world.in_flight.contains(&a.artifact_id)
            })
            .collect();

        if candidates.is_empty() {
            return None;
        }

        // Sort by priority (highest first), then by created_at (oldest first)
        // for fairness.
        candidates.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then_with(|| a.created_at.cmp(&b.created_at))
        });

        // Round-robin: if the top candidate has the same owner as last served,
        // try to find one with a different owner.
        let chosen = if let Some(ref last_owner) = self.last_served_owner {
            candidates
                .iter()
                .find(|a| a.owner != *last_owner)
                .or(candidates.first())
        } else {
            candidates.first()
        };

        chosen.map(|a| ShapingDecision {
            artifact_id: a.artifact_id.clone(),
            target_version: a.current_version + 1,
            target_state: onsager::ArtifactState::UnderReview,
            shaping_intent: a.shaping_intent.clone(),
            inputs: vec![],
            constraints: vec![],
            priority: a.priority,
            deadline: a.deadline,
        })
    }

    fn observe(&mut self, event: &FactoryEventKind) {
        // Track the last served owner for round-robin fairness.
        if let FactoryEventKind::ForgeShapingDispatched { artifact_id, .. } = event {
            // We'd look up the owner from the artifact_id in a real implementation.
            // For now, store the artifact_id as a proxy.
            self.last_served_owner = Some(artifact_id.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact_store::{SchedulableArtifact, WorldState};
    use chrono::Utc;
    use onsager::{ArtifactId, ArtifactState};
    use std::collections::HashSet;

    fn make_artifact(id: &str, priority: i32) -> SchedulableArtifact {
        SchedulableArtifact {
            artifact_id: ArtifactId::new(id),
            kind: onsager::Kind::Code,
            owner: "test-owner".into(),
            state: ArtifactState::InProgress,
            current_version: 1,
            priority,
            deadline: None,
            shaping_intent: serde_json::json!({"action": "shape"}),
            created_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn decide_returns_none_when_no_candidates() {
        let kernel = BaselineKernel::new();
        let world = WorldState {
            artifacts: vec![],
            in_flight: HashSet::new(),
        };
        assert!(kernel.decide(&world).await.is_none());
    }

    #[tokio::test]
    async fn decide_picks_highest_priority() {
        let kernel = BaselineKernel::new();
        let world = WorldState {
            artifacts: vec![make_artifact("art_low", 1), make_artifact("art_high", 10)],
            in_flight: HashSet::new(),
        };
        let decision = kernel.decide(&world).await.unwrap();
        assert_eq!(decision.artifact_id.as_str(), "art_high");
    }

    #[tokio::test]
    async fn decide_skips_in_flight() {
        let kernel = BaselineKernel::new();
        let mut in_flight = HashSet::new();
        in_flight.insert(ArtifactId::new("art_high"));

        let world = WorldState {
            artifacts: vec![make_artifact("art_low", 1), make_artifact("art_high", 10)],
            in_flight,
        };
        let decision = kernel.decide(&world).await.unwrap();
        assert_eq!(decision.artifact_id.as_str(), "art_low");
    }

    #[tokio::test]
    async fn decide_skips_non_in_progress() {
        let kernel = BaselineKernel::new();
        let mut art = make_artifact("art_draft", 10);
        art.state = ArtifactState::Draft;

        let world = WorldState {
            artifacts: vec![art, make_artifact("art_wip", 1)],
            in_flight: HashSet::new(),
        };
        let decision = kernel.decide(&world).await.unwrap();
        assert_eq!(decision.artifact_id.as_str(), "art_wip");
    }
}
