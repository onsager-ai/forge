//! Forge core loop — the main run loop that drives the factory.
//!
//! ```text
//! loop:
//!     decision = scheduling_kernel.decide(world_state)
//!     verdict  = synodic.gate(decision)            // pre-dispatch gate
//!     result   = stiglab.dispatch(decision)         // imperative dispatch
//!     verdict  = synodic.gate(result, target_state) // state-transition gate
//!     if verdict.allow:
//!         forge.advance(artifact, result, target_state)
//!         if target_state == RELEASED:
//!             forge.route(artifact, consumers)
//!     emit_factory_events()
//! ```
//!
//! See `specs/forge-v0.1.md §3` for the full specification.

use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

use anyhow::Result;
use onsager::factory_event::{
    FactoryEventKind, ForgeProcessState, GatePoint, ShapingOutcome, VerdictSummary,
};
use onsager::{ArtifactId, EventMetadata, EventStore};

use crate::artifact_store::ArtifactStore;
use crate::config::ForgeConfig;
use crate::kernel::{BaselineKernel, SchedulingKernel};

/// Forge process state (forge-v0.1 §8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessState {
    Running,
    Paused,
    Draining,
    Stopped,
}

impl ProcessState {
    fn to_factory_state(self) -> ForgeProcessState {
        match self {
            ProcessState::Running => ForgeProcessState::Running,
            ProcessState::Paused => ForgeProcessState::Paused,
            ProcessState::Draining => ForgeProcessState::Draining,
            ProcessState::Stopped => ForgeProcessState::Stopped,
        }
    }
}

/// The main Forge run loop.
pub struct ForgeLoop {
    config: ForgeConfig,
    event_store: EventStore,
    artifact_store: ArtifactStore,
    kernel: Arc<Mutex<BaselineKernel>>,
    state: Arc<Mutex<ProcessState>>,
    in_flight: Arc<Mutex<HashSet<ArtifactId>>>,
}

impl ForgeLoop {
    /// Create a new ForgeLoop, connecting to the database.
    pub async fn new(config: &ForgeConfig) -> Result<Self> {
        let event_store = EventStore::connect(&config.database_url).await?;
        let artifact_store = ArtifactStore::new(event_store.pool().clone());

        Ok(Self {
            config: config.clone(),
            event_store,
            artifact_store,
            kernel: Arc::new(Mutex::new(BaselineKernel::new())),
            state: Arc::new(Mutex::new(ProcessState::Running)),
            in_flight: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    /// Run the core loop. Returns when the process reaches `Stopped` state.
    pub async fn run(&self) -> Result<()> {
        self.emit_state_change(ForgeProcessState::Stopped, ForgeProcessState::Running)
            .await?;

        tracing::info!("Forge core loop started");

        let mut idle_event_countdown = self.config.idle_event_interval;

        loop {
            let state = *self.state.lock().await;

            match state {
                ProcessState::Stopped => {
                    tracing::info!("Forge stopped");
                    break;
                }
                ProcessState::Draining => {
                    let in_flight = self.in_flight.lock().await;
                    if in_flight.is_empty() {
                        let mut s = self.state.lock().await;
                        *s = ProcessState::Stopped;
                        self.emit_state_change(
                            ForgeProcessState::Draining,
                            ForgeProcessState::Stopped,
                        )
                        .await?;
                        continue;
                    }
                    tracing::debug!(
                        in_flight = in_flight.len(),
                        "draining, waiting for in-flight requests"
                    );
                    tokio::time::sleep(self.config.idle_tick_interval).await;
                    continue;
                }
                ProcessState::Paused => {
                    // Paused: no new decisions, but continue processing.
                    tokio::time::sleep(self.config.idle_tick_interval).await;
                    continue;
                }
                ProcessState::Running => {
                    // Check capacity.
                    let in_flight_count = self.in_flight.lock().await.len();
                    if in_flight_count >= self.config.max_in_flight {
                        tracing::debug!(
                            in_flight = in_flight_count,
                            max = self.config.max_in_flight,
                            "at capacity, waiting"
                        );
                        tokio::time::sleep(self.config.idle_tick_interval).await;
                        continue;
                    }

                    // Load world state and ask kernel for a decision.
                    let in_flight = self.in_flight.lock().await.clone();
                    let world = self.artifact_store.load_world_state(&in_flight).await?;

                    let decision = self.kernel.lock().await.decide(&world).await;

                    match decision {
                        None => {
                            // Idle tick — emit at reduced frequency.
                            idle_event_countdown = idle_event_countdown
                                .checked_sub(self.config.idle_tick_interval)
                                .unwrap_or_default();

                            if idle_event_countdown.is_zero() {
                                self.emit_event(FactoryEventKind::ForgeIdleTick).await?;
                                idle_event_countdown = self.config.idle_event_interval;
                            }

                            tokio::time::sleep(self.config.idle_tick_interval).await;
                        }
                        Some(decision) => {
                            idle_event_countdown = self.config.idle_event_interval;

                            tracing::info!(
                                artifact_id = decision.artifact_id.as_str(),
                                target_version = decision.target_version,
                                priority = decision.priority,
                                "scheduling kernel decided"
                            );

                            // Emit decision event.
                            self.emit_event(FactoryEventKind::ForgeDecisionMade {
                                artifact_id: decision.artifact_id.clone(),
                                target_version: decision.target_version,
                                priority: decision.priority,
                            })
                            .await?;

                            // TODO: Pre-dispatch gate (Forge → Synodic).
                            // For v0.1 baseline, we emit the gate event but
                            // auto-allow until Synodic is connected.
                            self.emit_event(FactoryEventKind::ForgeGateRequested {
                                artifact_id: decision.artifact_id.clone(),
                                gate_point: GatePoint::PreDispatch,
                            })
                            .await?;

                            self.emit_event(FactoryEventKind::ForgeGateVerdict {
                                artifact_id: decision.artifact_id.clone(),
                                gate_point: GatePoint::PreDispatch,
                                verdict: VerdictSummary::Allow,
                            })
                            .await?;

                            // Track in-flight.
                            self.in_flight
                                .lock()
                                .await
                                .insert(decision.artifact_id.clone());

                            let request_id = uuid::Uuid::new_v4().to_string();

                            // Emit dispatch event.
                            self.emit_event(FactoryEventKind::ForgeShapingDispatched {
                                request_id: request_id.clone(),
                                artifact_id: decision.artifact_id.clone(),
                                target_version: decision.target_version,
                            })
                            .await?;

                            // Observe the dispatch in the kernel.
                            self.kernel.lock().await.observe(
                                &FactoryEventKind::ForgeShapingDispatched {
                                    request_id,
                                    artifact_id: decision.artifact_id.clone(),
                                    target_version: decision.target_version,
                                },
                            );

                            // TODO: Actual Stiglab dispatch over the network.
                            // For now, log and remove from in-flight
                            // (Stiglab integration comes next).
                            tracing::info!(
                                artifact_id = decision.artifact_id.as_str(),
                                "dispatched to Stiglab (stub — awaiting Stiglab integration)"
                            );

                            self.emit_event(FactoryEventKind::ForgeShapingReturned {
                                request_id: uuid::Uuid::new_v4().to_string(),
                                artifact_id: decision.artifact_id.clone(),
                                outcome: ShapingOutcome::Completed,
                            })
                            .await?;

                            self.in_flight.lock().await.remove(&decision.artifact_id);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    // -- Helpers -------------------------------------------------------------

    async fn emit_event(&self, event: FactoryEventKind) -> Result<()> {
        let data = serde_json::to_value(&event)?;
        let metadata = EventMetadata {
            actor: "forge".into(),
            ..Default::default()
        };

        self.event_store
            .append_ext(
                &event.stream_id(),
                "forge",
                event.event_type(),
                data,
                &metadata,
                None,
            )
            .await?;

        Ok(())
    }

    async fn emit_state_change(
        &self,
        from: ForgeProcessState,
        to: ForgeProcessState,
    ) -> Result<()> {
        self.emit_event(FactoryEventKind::ForgeStateChanged {
            from_state: from,
            to_state: to,
        })
        .await
    }
}
