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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use anyhow::Result;
use onsager::factory_event::{
    FactoryEventKind, ForgeProcessState, GatePoint, ShapingOutcome, VerdictSummary,
};
use onsager::protocol::EscalationContext;
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
    /// Artifacts parked waiting for an escalation verdict.
    /// Key: escalation_id (from Synodic); Value: artifact_id.
    escalated: Arc<Mutex<HashMap<String, ArtifactId>>>,
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
            escalated: Arc::new(Mutex::new(HashMap::new())),
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

        // Spawn background: escalation resolver watches for synodic.escalation_resolved.
        tokio::spawn(run_escalation_resolver(
            self.event_store.clone(),
            Arc::clone(&self.escalated),
        ));

        // Spawn background: Ising observer forwards insights to the kernel.
        tokio::spawn(run_ising_observer(
            self.event_store.clone(),
            Arc::clone(&self.kernel),
        ));

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
                    // Exclude both in-flight and escalated artifacts from scheduling.
                    let mut excluded = self.in_flight.lock().await.clone();
                    for art_id in self.escalated.lock().await.values() {
                        excluded.insert(art_id.clone());
                    }
                    let world = self.artifact_store.load_world_state(&excluded).await?;

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
                                onsager::GateVerdict::Allow => {} // proceed
                                onsager::GateVerdict::Deny { reason } => {
                                    tracing::warn!(
                                        artifact_id = decision.artifact_id.as_str(),
                                        reason = %reason,
                                        "pre-dispatch gate denied"
                                    );
                                    continue;
                                }
                                onsager::GateVerdict::Modify { .. } => {
                                    tracing::warn!(
                                        artifact_id = decision.artifact_id.as_str(),
                                        "pre-dispatch gate returned modify, treating as deny for v0.1"
                                    );
                                    continue;
                                }
                                onsager::GateVerdict::Escalate { context } => {
                                    self.park_escalation(&decision.artifact_id, context).await?;
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

                                        // State-transition gate (Forge -> Synodic)
                                        let st_gate_req = onsager::protocol::GateRequest {
                                            context: onsager::protocol::GateContext {
                                                gate_point: GatePoint::StateTransition,
                                                artifact_id: decision.artifact_id.clone(),
                                                artifact_kind: onsager::Kind::Code, // TODO: look up from artifact
                                                current_state: onsager::ArtifactState::InProgress,
                                                target_state: Some(decision.target_state),
                                                extra: None,
                                            },
                                            proposed_action: onsager::protocol::ProposedAction {
                                                description: format!(
                                                    "Advance {} to {:?}",
                                                    decision.artifact_id, decision.target_state
                                                ),
                                                payload: serde_json::Value::Null,
                                            },
                                        };

                                        self.emit_event(FactoryEventKind::ForgeGateRequested {
                                            artifact_id: decision.artifact_id.clone(),
                                            gate_point: GatePoint::StateTransition,
                                        })
                                        .await?;

                                        let st_verdict =
                                            match self.synodic.gate(&st_gate_req).await {
                                                Ok(v) => v,
                                                Err(e) => {
                                                    tracing::warn!(error = %e, "synodic state-transition gate failed, auto-allowing");
                                                    onsager::GateVerdict::Allow
                                                }
                                            };

                                        let st_verdict_summary = match &st_verdict {
                                            onsager::GateVerdict::Allow => VerdictSummary::Allow,
                                            onsager::GateVerdict::Deny { .. } => {
                                                VerdictSummary::Deny
                                            }
                                            onsager::GateVerdict::Modify { .. } => {
                                                VerdictSummary::Modify
                                            }
                                            onsager::GateVerdict::Escalate { .. } => {
                                                VerdictSummary::Escalate
                                            }
                                        };

                                        self.emit_event(FactoryEventKind::ForgeGateVerdict {
                                            artifact_id: decision.artifact_id.clone(),
                                            gate_point: GatePoint::StateTransition,
                                            verdict: st_verdict_summary,
                                        })
                                        .await?;

                                        match &st_verdict {
                                            onsager::GateVerdict::Allow => {} // proceed
                                            onsager::GateVerdict::Deny { reason } => {
                                                tracing::warn!(
                                                    artifact_id = decision.artifact_id.as_str(),
                                                    reason = %reason,
                                                    "state-transition gate denied"
                                                );
                                                self.in_flight
                                                    .lock()
                                                    .await
                                                    .remove(&decision.artifact_id);
                                                continue;
                                            }
                                            onsager::GateVerdict::Modify { .. } => {
                                                tracing::warn!(
                                                    artifact_id = decision.artifact_id.as_str(),
                                                    "state-transition gate returned modify, treating as deny for v0.1"
                                                );
                                                self.in_flight
                                                    .lock()
                                                    .await
                                                    .remove(&decision.artifact_id);
                                                continue;
                                            }
                                            onsager::GateVerdict::Escalate { context } => {
                                                self.park_escalation(
                                                    &decision.artifact_id,
                                                    context,
                                                )
                                                .await?;
                                                continue;
                                            }
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

    /// Park an artifact waiting for an escalation to be resolved.
    ///
    /// Removes the artifact from `in_flight` and records it in `escalated`
    /// under the escalation ID returned by Synodic. The background escalation
    /// resolver will unpark it when `synodic.escalation_resolved` arrives.
    async fn park_escalation(
        &self,
        artifact_id: &ArtifactId,
        ctx: &EscalationContext,
    ) -> Result<()> {
        self.in_flight.lock().await.remove(artifact_id);
        self.escalated
            .lock()
            .await
            .insert(ctx.escalation_id.clone(), artifact_id.clone());
        tracing::info!(
            artifact_id = artifact_id.as_str(),
            escalation_id = %ctx.escalation_id,
            reason = %ctx.reason,
            target = %ctx.target,
            "artifact parked for escalation"
        );
        Ok(())
    }

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

// ---------------------------------------------------------------------------
// Background tasks
// ---------------------------------------------------------------------------

/// Watch the event spine for `synodic.escalation_resolved` events and unpark
/// the corresponding artifact from the escalated map.
///
/// When an escalation is resolved, Synodic appends an extension event with
/// `stream_id = escalation_id`. This resolver removes the artifact from the
/// parked set so the scheduling kernel will consider it again on the next tick.
async fn run_escalation_resolver(
    event_store: EventStore,
    escalated: Arc<Mutex<HashMap<String, ArtifactId>>>,
) {
    loop {
        let mut rx = match event_store.subscribe_bounded(256).await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(error = %e, "escalation resolver: pg_notify subscribe failed; retrying in 5s");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        tracing::info!("escalation resolver: subscribed to pg_notify");

        while let Some(notification) = rx.recv().await {
            if notification.event_type != "synodic.escalation_resolved" {
                continue;
            }
            // stream_id on events_ext is set to the escalation_id by Synodic.
            let escalation_id = &notification.stream_id;
            let removed = escalated.lock().await.remove(escalation_id);
            match removed {
                Some(artifact_id) => {
                    tracing::info!(
                        escalation_id = %escalation_id,
                        artifact_id = artifact_id.as_str(),
                        "escalation resolved; artifact unparked"
                    );
                }
                None => {
                    tracing::debug!(
                        escalation_id = %escalation_id,
                        "received escalation_resolved for unknown escalation (already unparked?)"
                    );
                }
            }
        }

        tracing::warn!("escalation resolver: pg_notify channel closed; reconnecting");
    }
}

/// Watch the event spine for `ising.insight_detected` events and forward them
/// to the scheduling kernel as advisory observations.
///
/// In v0.1 this logs the event type; full forwarding requires a DB fetch to
/// reconstruct the `FactoryEventKind` payload from the spine record.
async fn run_ising_observer(
    event_store: EventStore,
    kernel: Arc<Mutex<BaselineKernel>>,
) {
    loop {
        let mut rx = match event_store.subscribe_bounded(256).await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(error = %e, "ising observer: pg_notify subscribe failed; retrying in 5s");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        tracing::info!("ising observer: subscribed to pg_notify");

        while let Some(notification) = rx.recv().await {
            if !notification.event_type.starts_with("ising.insight") {
                continue;
            }
            tracing::info!(
                event_type = %notification.event_type,
                stream_id = %notification.stream_id,
                "ising insight received (advisory only)"
            );
            // v0.1: log only. Full advisory forwarding requires fetching the
            // full event record and calling kernel.lock().await.observe(&kind).
            let _ = kernel.lock().await; // ensure kernel is accessible for future use
        }

        tracing::warn!("ising observer: pg_notify channel closed; reconnecting");
    }
}
