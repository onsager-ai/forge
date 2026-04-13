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
use std::time::Duration;
use tokio::sync::Mutex;

use anyhow::Result;
use onsager::factory_event::{
    FactoryEventKind, ForgeProcessState, GatePoint, ShapingOutcome, VerdictSummary,
};
use onsager::{ArtifactId, EventMetadata, EventStore};

use crate::artifact_store::ArtifactStore;
use crate::clients::{StiglabClient, SynodicClient};
use crate::config::ForgeConfig;
use crate::kernel::{BaselineKernel, SchedulingKernel};
use crate::router::{ConsumerSink, LogSink};

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
    stiglab: StiglabClient,
    synodic: SynodicClient,
    sinks: Vec<Arc<dyn ConsumerSink>>,
}

impl ForgeLoop {
    /// Create a new ForgeLoop, connecting to the database.
    pub async fn new(config: &ForgeConfig) -> Result<Self> {
        let event_store = EventStore::connect(&config.database_url).await?;
        let artifact_store = ArtifactStore::new(event_store.pool().clone());

        let stiglab = StiglabClient::new(
            &config.stiglab_url,
            Duration::from_secs(config.shaping_timeout_secs),
        );
        let synodic = SynodicClient::new(&config.synodic_url);

        Ok(Self {
            config: config.clone(),
            event_store,
            artifact_store,
            kernel: Arc::new(Mutex::new(BaselineKernel::new())),
            state: Arc::new(Mutex::new(ProcessState::Running)),
            in_flight: Arc::new(Mutex::new(HashSet::new())),
            stiglab,
            synodic,
            sinks: vec![Arc::new(LogSink)],
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

                            // Pre-dispatch gate (Forge -> Synodic)
                            let gate_req = onsager::protocol::GateRequest {
                                context: onsager::protocol::GateContext {
                                    gate_point: GatePoint::PreDispatch,
                                    artifact_id: decision.artifact_id.clone(),
                                    artifact_kind: onsager::Kind::Code, // TODO: look up from artifact
                                    current_state: onsager::ArtifactState::InProgress,
                                    target_state: Some(decision.target_state),
                                    extra: None,
                                },
                                proposed_action: onsager::protocol::ProposedAction {
                                    description: format!(
                                        "Shape {} v{}",
                                        decision.artifact_id, decision.target_version
                                    ),
                                    payload: decision.shaping_intent.clone(),
                                },
                            };

                            self.emit_event(FactoryEventKind::ForgeGateRequested {
                                artifact_id: decision.artifact_id.clone(),
                                gate_point: GatePoint::PreDispatch,
                            })
                            .await?;

                            let verdict = match self.synodic.gate(&gate_req).await {
                                Ok(v) => v,
                                Err(e) => {
                                    tracing::warn!(error = %e, "synodic gate call failed, auto-allowing");
                                    onsager::GateVerdict::Allow
                                }
                            };

                            let verdict_summary = match &verdict {
                                onsager::GateVerdict::Allow => VerdictSummary::Allow,
                                onsager::GateVerdict::Deny { .. } => VerdictSummary::Deny,
                                onsager::GateVerdict::Modify { .. } => VerdictSummary::Modify,
                                onsager::GateVerdict::Escalate { .. } => VerdictSummary::Escalate,
                            };

                            self.emit_event(FactoryEventKind::ForgeGateVerdict {
                                artifact_id: decision.artifact_id.clone(),
                                gate_point: GatePoint::PreDispatch,
                                verdict: verdict_summary,
                            })
                            .await?;

                            match &verdict {
                                onsager::GateVerdict::Deny { reason } => {
                                    tracing::warn!(
                                        artifact_id = decision.artifact_id.as_str(),
                                        reason = %reason,
                                        "pre-dispatch gate denied"
                                    );
                                    continue;
                                }
                                onsager::GateVerdict::Allow => {} // proceed
                                _ => {
                                    tracing::warn!(
                                        artifact_id = decision.artifact_id.as_str(),
                                        "gate returned modify/escalate, treating as deny for MVP"
                                    );
                                    continue;
                                }
                            }

                            // Track in-flight
                            self.in_flight
                                .lock()
                                .await
                                .insert(decision.artifact_id.clone());

                            let request_id = uuid::Uuid::new_v4().to_string();

                            self.emit_event(FactoryEventKind::ForgeShapingDispatched {
                                request_id: request_id.clone(),
                                artifact_id: decision.artifact_id.clone(),
                                target_version: decision.target_version,
                            })
                            .await?;

                            // Build ShapingRequest
                            let shaping_req = onsager::ShapingRequest {
                                request_id: request_id.clone(),
                                artifact_id: decision.artifact_id.clone(),
                                target_version: decision.target_version,
                                shaping_intent: decision.shaping_intent.clone(),
                                inputs: decision.inputs.clone(),
                                constraints: decision.constraints.clone(),
                                deadline: decision.deadline,
                            };

                            // Observe dispatch in kernel
                            self.kernel.lock().await.observe(
                                &FactoryEventKind::ForgeShapingDispatched {
                                    request_id: request_id.clone(),
                                    artifact_id: decision.artifact_id.clone(),
                                    target_version: decision.target_version,
                                },
                            );

                            // Dispatch to Stiglab
                            let shaping_result = self.stiglab.dispatch_shaping(&shaping_req).await;

                            match shaping_result {
                                Ok(result) => {
                                    tracing::info!(
                                        artifact_id = decision.artifact_id.as_str(),
                                        outcome = ?result.outcome,
                                        duration_ms = result.duration_ms,
                                        "shaping completed"
                                    );

                                    self.emit_event(FactoryEventKind::ForgeShapingReturned {
                                        request_id: result.request_id.clone(),
                                        artifact_id: decision.artifact_id.clone(),
                                        outcome: result.outcome,
                                    })
                                    .await?;

                                    if result.outcome == ShapingOutcome::Completed {
                                        // Create version
                                        if let Some(ref content_ref) = result.content_ref {
                                            self.artifact_store
                                                .create_version(
                                                    &decision.artifact_id,
                                                    decision.target_version,
                                                    &content_ref.uri,
                                                    content_ref.checksum.as_deref(),
                                                    &result.change_summary,
                                                    &result.session_id,
                                                )
                                                .await?;

                                            self.emit_event(
                                                FactoryEventKind::ArtifactVersionCreated {
                                                    artifact_id: decision.artifact_id.clone(),
                                                    version: decision.target_version,
                                                    content_ref_uri: content_ref.uri.clone(),
                                                    change_summary: result.change_summary.clone(),
                                                    session_id: result.session_id.clone(),
                                                },
                                            )
                                            .await?;
                                        }

                                        // Advance state
                                        let from_state = self
                                            .artifact_store
                                            .advance_state(
                                                &decision.artifact_id,
                                                decision.target_state,
                                            )
                                            .await?;

                                        self.emit_event(FactoryEventKind::ArtifactStateChanged {
                                            artifact_id: decision.artifact_id.clone(),
                                            from_state,
                                            to_state: decision.target_state,
                                        })
                                        .await?;

                                        // Route released artifacts to consumers.
                                        if decision.target_state == onsager::ArtifactState::Released
                                        {
                                            self.route_artifact(
                                                &decision.artifact_id,
                                                decision.target_version,
                                            )
                                            .await?;
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::error!(
                                        artifact_id = decision.artifact_id.as_str(),
                                        error = %e,
                                        "stiglab dispatch failed"
                                    );
                                    self.emit_event(FactoryEventKind::ForgeShapingReturned {
                                        request_id,
                                        artifact_id: decision.artifact_id.clone(),
                                        outcome: ShapingOutcome::Failed,
                                    })
                                    .await?;
                                }
                            }

                            self.in_flight.lock().await.remove(&decision.artifact_id);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    // -- Helpers -------------------------------------------------------------

    /// Gate the consumer routing step with Synodic, then dispatch to all sinks.
    async fn route_artifact(&self, artifact_id: &ArtifactId, version: u32) -> Result<()> {
        let gate_req = onsager::protocol::GateRequest {
            context: onsager::protocol::GateContext {
                gate_point: GatePoint::ConsumerRouting,
                artifact_id: artifact_id.clone(),
                artifact_kind: onsager::Kind::Code, // TODO: look up from artifact
                current_state: onsager::ArtifactState::Released,
                target_state: None,
                extra: None,
            },
            proposed_action: onsager::protocol::ProposedAction {
                description: format!("Route artifact {} v{} to consumers", artifact_id, version),
                payload: serde_json::Value::Null,
            },
        };

        self.emit_event(FactoryEventKind::ForgeGateRequested {
            artifact_id: artifact_id.clone(),
            gate_point: GatePoint::ConsumerRouting,
        })
        .await?;

        let verdict = match self.synodic.gate(&gate_req).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "synodic consumer-routing gate failed, auto-allowing");
                onsager::GateVerdict::Allow
            }
        };

        let verdict_summary = match &verdict {
            onsager::GateVerdict::Allow => VerdictSummary::Allow,
            onsager::GateVerdict::Deny { .. } => VerdictSummary::Deny,
            onsager::GateVerdict::Modify { .. } => VerdictSummary::Modify,
            onsager::GateVerdict::Escalate { .. } => VerdictSummary::Escalate,
        };

        self.emit_event(FactoryEventKind::ForgeGateVerdict {
            artifact_id: artifact_id.clone(),
            gate_point: GatePoint::ConsumerRouting,
            verdict: verdict_summary,
        })
        .await?;

        if !matches!(verdict, onsager::GateVerdict::Allow) {
            tracing::warn!(
                artifact_id = artifact_id.as_str(),
                "consumer routing gate denied, skipping sinks"
            );
            return Ok(());
        }

        // Dispatch to each registered sink (at-least-once; sinks must be idempotent).
        for sink in &self.sinks {
            match sink.route(artifact_id, version).await {
                Ok(()) => {
                    self.emit_event(FactoryEventKind::ArtifactRouted {
                        artifact_id: artifact_id.clone(),
                        consumer_id: sink.name().to_string(),
                        sink: sink.name().to_string(),
                    })
                    .await?;
                }
                Err(e) => {
                    tracing::error!(
                        artifact_id = artifact_id.as_str(),
                        sink = sink.name(),
                        error = %e,
                        "consumer sink routing failed"
                    );
                }
            }
        }

        Ok(())
    }

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
