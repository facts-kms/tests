use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use std::{env, fs};

use anyhow::{Context, Result, bail};
use fact_sdk::decision::{DecisionInput, DecisionValue, create_decision_with_runtime};
use fact_sdk::discussion::{
    create_comment_with_runtime, join_deliberation_with_runtime, leave_deliberation_with_runtime,
    open_deliberation_for_revision_with_runtime,
};
use fact_sdk::environment::{LedgerEntry, UserEnvironment};
use fact_sdk::identity::{
    CreateIdentityInput, create_identity_grant_with_runtime, create_identity_with_runtime,
    export_identity, import_identity, rotate_identity_key_with_runtime,
};
use fact_sdk::invitation::{
    create_invitation_with_runtime, update_invitation_lifecycle_with_runtime,
};
use fact_sdk::lifecycle::{archive_proposition_with_runtime, withdraw_proposition_with_runtime};
use fact_sdk::proposition::{
    ContentSelection, DerivedRevisionInput, ListPropositionsFilter, PropositionListItem,
    ReconciliationConflictInput, ReconciliationInput, accept_proposition_with_runtime,
    create_derived_revision_with_runtime, create_proposition_with_runtime,
    create_reconciliation_proposition_with_runtime, list_propositions, list_revisions,
    pending_propositions, read_proposition_content, read_proposition_content_with_selection,
    reject_proposition_with_runtime, update_proposition_content_with_runtime,
};
use fact_sdk::runtime::DeterministicRuntime;
use fact_sdk::settlement::{
    SettlementInput, SettlementProducerType, create_settlement_with_runtime,
};
use fact_sdk::state::rebuild_state;
use fact_sdk::sync::{export_bundle, export_object, import_bundle};
use fact_sdk::workflow::{BootstrapLedgerInput, create_ledger_with_runtime};
use fact_sim_core::{
    Clock, ConflictRepairManifestReport, CoordinatorDisposition, DeterministicRandomSource,
    FailureClassification, ObjectCounts, RandomSource, RunManifest, SimClock,
};
use fact_sim_dsl::{Assertion, Scenario, Step};
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub mod conflict_repair;
pub mod multi_actor;
pub mod scale;
pub mod sync_scale;

const SCHEDULER_VERSION: &str = "seeded-branch-interleave-v0";
static RUN_WORKSPACE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn default_fact_binary_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .join("cli")
        .join("target")
        .join("debug")
        .join("fact")
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StepReceipt {
    pub step_index: usize,
    pub operation: String,
    pub object_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CliReceipt {
    pub command: Vec<String>,
    pub status: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub parsed_json: Option<serde_json::Value>,
    pub duration_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScenarioRun {
    pub scenario_name: String,
    pub receipts: Vec<StepReceipt>,
    pub cli_receipts: Vec<CliReceipt>,
    pub characters: Vec<CharacterRunState>,
    pub final_state: SdkStateSnapshot,
    pub logical_digest: LogicalDigest,
    pub retry_report: Vec<ScenarioRetryRecord>,
    pub coordinator_disposition_report: Vec<ScenarioCoordinatorDispositionRecord>,
    pub semantic_correction_report: Vec<ScenarioSemanticCorrectionRecord>,
    pub invitation_race_report: Vec<ScenarioInvitationRaceRecord>,
    pub repair_report: ScenarioRepairReport,
    pub manifest: RunManifest,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScenarioRetryRecord {
    pub scenario: String,
    pub name: String,
    pub object_id: Uuid,
    pub first_disposition: CoordinatorDisposition,
    pub retry_disposition: CoordinatorDisposition,
    pub classification: FailureClassification,
    pub missing_dependency_kind: String,
    pub retryable_unchanged: bool,
    pub original_payload_hash: String,
    pub retried_payload_hash: String,
    pub original_signed_object_hash: String,
    pub retried_signed_object_hash: String,
    pub duplicate_count: usize,
    pub converged: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScenarioCoordinatorDispositionRecord {
    pub scenario: String,
    pub name: String,
    pub coordinator: String,
    pub coordinator_actor_id: Uuid,
    pub object_id: Uuid,
    pub disposition: CoordinatorDisposition,
    pub classification: Option<FailureClassification>,
    pub reason: Option<String>,
    pub statement_payload_hash: String,
    pub statement_signature: String,
    pub canonical_unchanged: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScenarioSemanticCorrectionRecord {
    pub scenario: String,
    pub name: String,
    pub proposition_id: Uuid,
    pub previous_revision_id: Uuid,
    pub corrective_revision_id: Uuid,
    pub previous_payload_hash: String,
    pub preserved_payload_hash: String,
    pub previous_signed_object_hash: String,
    pub preserved_signed_object_hash: String,
    pub corrective_payload_hash: String,
    pub corrective_signed_object_hash: String,
    pub preserved_history: bool,
    pub requires_new_signed_object: bool,
    pub effective_changed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScenarioInvitationRaceRecord {
    pub scenario: String,
    pub invitation_id: Uuid,
    pub join_change_id: Uuid,
    pub lifecycle_id: Uuid,
    pub lifecycle_operation: String,
    pub conflict_type: String,
    pub classification: FailureClassification,
    pub concurrent: bool,
    pub enrollment_occurs: bool,
    pub new_invitation_required: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScenarioRepairReport {
    pub projection_repairs: usize,
    pub partial_sync_repairs: usize,
    pub semantic_corrections: usize,
    pub repaired_replicas_converged: bool,
    pub canonical_history_preserved: bool,
    pub repairs: Vec<ScenarioRepairRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScenarioRepairRecord {
    pub scenario: String,
    pub repair_type: String,
    pub object_id: Option<Uuid>,
    pub detected: bool,
    pub retry_unchanged: bool,
    pub converged: bool,
    pub canonical_history_preserved: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CharacterRunState {
    pub name: String,
    pub actor_id: Uuid,
    pub current_key_id: Uuid,
    pub historical_key_ids: Vec<Uuid>,
    pub environments: Vec<String>,
    pub environment_databases: BTreeMap<String, BTreeMap<String, String>>,
    pub ledger_capabilities: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalDigest {
    pub proposition_count: usize,
    pub revision_count: usize,
    pub pending_action_count: usize,
    pub effective_summaries: Vec<String>,
    pub revision_statuses: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SdkStateSnapshot {
    pub propositions: Vec<PropositionState>,
    pub revisions: Vec<RevisionState>,
    pub pending: Vec<PendingState>,
    pub object_counts_by_type: BTreeMap<String, usize>,
}

impl SdkStateSnapshot {
    fn logical_digest(&self) -> LogicalDigest {
        LogicalDigest {
            proposition_count: self.propositions.len(),
            revision_count: self.revisions.len(),
            pending_action_count: self.pending.len(),
            effective_summaries: self
                .propositions
                .iter()
                .map(|item| item.summary.clone())
                .collect(),
            revision_statuses: self
                .revisions
                .iter()
                .map(|item| format!("{}:{}", item.summary, item.status))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PropositionState {
    pub proposition_id: Uuid,
    pub reference: String,
    pub status: String,
    pub effective_status: String,
    pub summary: String,
    pub effective_revision_id: Option<Uuid>,
    pub deliberation_id: Option<Uuid>,
    pub latest_revision_id: Option<Uuid>,
    pub latest_revision_status: String,
    pub pending_revision_id: Option<Uuid>,
    pub pending_deliberation_id: Option<Uuid>,
    pub current_actor_pending: bool,
    pub has_pending_revision: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RevisionState {
    pub revision_id: Uuid,
    pub reference: String,
    pub status: String,
    pub effective: bool,
    pub latest: bool,
    pub tip: bool,
    pub summary: String,
    pub current_actor_pending: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingState {
    pub proposition_id: Uuid,
    pub reference: String,
    pub summary: String,
    pub pending_revision_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EffectiveProjection {
    status: String,
    reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReconciliationProjection {
    affected_proposition_id: Uuid,
    common_ancestor_revision_id: Uuid,
    resolution_mode: String,
    selected_revision_id: Option<Uuid>,
    result_revision_id: Option<Uuid>,
    conflict_revision_ids: BTreeSet<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FailureReport {
    pub scenario_name: String,
    pub seed: u64,
    pub step_number: usize,
    pub operation: String,
    pub expected_result: String,
    pub actual_result: String,
    pub symbolic_references: BTreeMap<String, String>,
    pub protocol_ids: BTreeMap<String, String>,
}

#[derive(Debug)]
struct SdkScenarioContext {
    scenario_name: String,
    seed: u64,
    clock: SimClock,
    runtime: DeterministicRuntime,
    random: DeterministicRandomSource,
    workspace: PathBuf,
    environment: UserEnvironment,
    characters: BTreeMap<String, Character>,
    ledgers: BTreeMap<String, SdkLedger>,
    propositions: BTreeMap<String, SdkProposition>,
    revisions: BTreeMap<String, Uuid>,
    deliberations: BTreeMap<String, Uuid>,
    decisions: BTreeMap<String, Uuid>,
    settlements: BTreeMap<String, Uuid>,
    invitations: BTreeMap<String, Uuid>,
    participant_changes: BTreeMap<String, Uuid>,
    invitation_lifecycles: BTreeMap<String, Uuid>,
    last_pre_rebuild_state: Option<SdkStateSnapshot>,
    last_pre_rebuild_hashes: Option<BTreeMap<PathBuf, Vec<String>>>,
    last_rebuild_equivalent: Option<bool>,
    last_canonical_history_unchanged: Option<bool>,
    projection_corruption_detected: bool,
    retry_records: BTreeMap<String, ScenarioRetryRecord>,
    coordinator_disposition_records: BTreeMap<String, ScenarioCoordinatorDispositionRecord>,
    semantic_correction_records: BTreeMap<String, ScenarioSemanticCorrectionRecord>,
    invitation_race_records: BTreeMap<String, ScenarioInvitationRaceRecord>,
    repair_report: ScenarioRepairReport,
    conflict_counts_by_class: BTreeMap<String, usize>,
    assertion_results: BTreeMap<String, usize>,
    cli_receipts: Vec<CliReceipt>,
}

#[derive(Debug, Clone)]
pub struct Character {
    name: String,
    actor_id: Uuid,
    current_key_id: Uuid,
    identity_entry: LedgerEntry,
    current_seed: [u8; 32],
    historical_keys: Vec<CharacterKey>,
    environments: BTreeMap<String, CharacterEnvironment>,
    identity_bundle: Vec<u8>,
}

#[derive(Debug, Clone)]
struct CharacterKey {
    key_id: Uuid,
}

#[derive(Debug, Clone, Default)]
struct CharacterEnvironment {
    name: String,
    ledger_entries: BTreeMap<String, LedgerEntry>,
    imported_ledgers: BTreeMap<String, usize>,
    capabilities: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Clone)]
struct SdkLedger {
    entry: LedgerEntry,
    seed: [u8; 32],
}

#[derive(Debug, Clone)]
struct CharacterLedgerEntry {
    entry: LedgerEntry,
    seed: [u8; 32],
}

#[derive(Debug, Clone)]
struct SdkProposition {
    proposition_id: Uuid,
    ledger: String,
    environment: String,
    latest_revision_symbol: String,
}

pub fn run_scenario(scenario: &Scenario) -> Result<ScenarioRun> {
    let mut context = SdkScenarioContext::new(scenario)?;

    for actor in &scenario.actors {
        context
            .create_character(&actor.name, &[])
            .map_err(|error| {
                context.failure(
                    0,
                    "create_character",
                    "character created",
                    error.to_string(),
                )
            })?;
    }
    for character in &scenario.characters {
        context
            .create_character(&character.name, &character.environments)
            .map_err(|error| {
                context.failure(
                    0,
                    "create_character",
                    "character created",
                    error.to_string(),
                )
            })?;
    }
    for ledger in &scenario.ledgers {
        context
            .create_ledger(&ledger.name, ledger.owner.as_deref())
            .map_err(|error| {
                context.failure(0, "create_ledger", "ledger created", error.to_string())
            })?;
    }

    let mut receipts = Vec::new();
    for (step_index, step) in scenario.steps.iter().enumerate() {
        receipts.push(
            context
                .execute_step(step_index + 1, step)
                .map_err(|error| {
                    context.failure(
                        step_index + 1,
                        operation_name(step),
                        "step completed",
                        format!("{error:#}"),
                    )
                })?,
        );
    }

    for assertion in &scenario.assertions {
        context
            .execute_assertion(scenario.steps.len() + 1, assertion)
            .map_err(|error| {
                context.failure(
                    scenario.steps.len() + 1,
                    "final_assertion",
                    "assertion passed",
                    format!("{error:#}"),
                )
            })?;
    }

    let final_state = context.snapshot_any()?;
    let characters = context.character_states();
    let manifest = context.manifest(&scenario.name, &final_state)?;
    Ok(ScenarioRun {
        scenario_name: scenario.name.clone(),
        receipts,
        cli_receipts: context.cli_receipts,
        characters,
        logical_digest: final_state.logical_digest(),
        final_state,
        retry_report: context.retry_records.into_values().collect(),
        coordinator_disposition_report: context
            .coordinator_disposition_records
            .into_values()
            .collect(),
        semantic_correction_report: context.semantic_correction_records.into_values().collect(),
        invitation_race_report: context.invitation_race_records.into_values().collect(),
        repair_report: context.repair_report,
        manifest,
    })
}

impl SdkScenarioContext {
    fn new(scenario: &Scenario) -> Result<Self> {
        let workspace = run_workspace(&scenario.name, scenario.seed)?;
        let fact_home = workspace.join("fact-home");
        let environment = UserEnvironment {
            catalog: fact_home.join("catalog.toml"),
            identity_dir: fact_home.join("identities"),
            ledger_dir: fact_home.join("ledgers"),
            active_file: fact_home.join("active"),
            remote_file: fact_home.join("remotes.toml"),
        };
        environment.ensure_dirs()?;
        Ok(Self {
            scenario_name: scenario.name.clone(),
            seed: scenario.seed,
            clock: SimClock::new(scenario.clock.start),
            runtime: DeterministicRuntime::new(
                format!("{}:{}", scenario.name, scenario.seed),
                scenario.clock.start,
            ),
            random: DeterministicRandomSource::from_seed(scenario.seed),
            workspace,
            environment,
            characters: BTreeMap::new(),
            ledgers: BTreeMap::new(),
            propositions: BTreeMap::new(),
            revisions: BTreeMap::new(),
            deliberations: BTreeMap::new(),
            decisions: BTreeMap::new(),
            settlements: BTreeMap::new(),
            invitations: BTreeMap::new(),
            participant_changes: BTreeMap::new(),
            invitation_lifecycles: BTreeMap::new(),
            last_pre_rebuild_state: None,
            last_pre_rebuild_hashes: None,
            last_rebuild_equivalent: None,
            last_canonical_history_unchanged: None,
            projection_corruption_detected: false,
            retry_records: BTreeMap::new(),
            coordinator_disposition_records: BTreeMap::new(),
            semantic_correction_records: BTreeMap::new(),
            invitation_race_records: BTreeMap::new(),
            repair_report: ScenarioRepairReport::default(),
            conflict_counts_by_class: BTreeMap::new(),
            assertion_results: BTreeMap::new(),
            cli_receipts: Vec::new(),
        })
    }

    fn create_character(&mut self, name: &str, environments: &[String]) -> Result<()> {
        if self.characters.contains_key(name) {
            bail!("character `{name}` already exists");
        }
        let database = self
            .environment
            .ledger_dir
            .join(format!("identity-{name}.sqlite"));
        let seed = self.next_seed();
        let store = fact_store::Store::open(&database)?;
        self.sync_runtime_clock()?;
        let output = create_identity_with_runtime(
            &store,
            CreateIdentityInput {
                namespace: format!("local.identity.{name}"),
                seed,
                actor_type: "human".into(),
            },
            &self.runtime,
        )?;
        let actor_id = output.actor_id;
        let key_id = output.key_id;
        let seed_file = self
            .environment
            .identity_dir
            .join(format!("{}.seed", output.actor_id));
        self.environment.write_seed(&seed_file, &seed)?;
        let identity_entry = LedgerEntry {
            name: format!("identity-{name}"),
            ledger_id: output.ledger_id.to_string(),
            database,
            actor_id: output.actor_id.to_string(),
            key_id: output.key_id.to_string(),
            seed_file,
            read_only: false,
        };
        let identity_bundle = output.bundle;
        let mut character_environments = BTreeMap::new();
        let declared_environments = if environments.is_empty() {
            vec![default_environment_name(name)]
        } else {
            environments.to_vec()
        };
        for environment_name in declared_environments {
            if character_environments.contains_key(&environment_name) {
                bail!(
                    "character `{name}` declares environment `{environment_name}` more than once"
                );
            }
            character_environments.insert(
                environment_name.clone(),
                CharacterEnvironment {
                    name: environment_name,
                    ..CharacterEnvironment::default()
                },
            );
        }
        self.characters.insert(
            name.to_string(),
            Character {
                name: name.to_string(),
                actor_id,
                current_key_id: key_id,
                identity_entry,
                current_seed: seed,
                historical_keys: vec![CharacterKey { key_id }],
                environments: character_environments,
                identity_bundle,
            },
        );
        Ok(())
    }

    fn create_ledger(&mut self, name: &str, owner: Option<&str>) -> Result<Uuid> {
        if let Some(owner) = owner {
            self.require_character(owner)?;
        }
        if self.ledgers.contains_key(name) {
            bail!("ledger `{name}` already exists");
        }
        let database = self.environment.ledger_dir.join(format!("{name}.sqlite"));
        let seed = self.next_seed();
        let nonce = self.next_nonce();
        let store = fact_store::Store::open(&database)?;
        self.sync_runtime_clock()?;
        let output = create_ledger_with_runtime(
            &store,
            BootstrapLedgerInput {
                namespace: format!("local.{name}"),
                created_at: sdk_timestamp(self.clock.now()),
                seed,
                nonce,
            },
            &self.runtime,
        )?;
        let seed_file = self
            .environment
            .identity_dir
            .join(format!("{}.seed", output.actor_id));
        self.environment.write_seed(&seed_file, &seed)?;
        let entry = LedgerEntry {
            name: name.to_string(),
            ledger_id: output.ledger_id.clone(),
            database,
            actor_id: output.actor_id.clone(),
            key_id: output.key_id,
            seed_file,
            read_only: false,
        };
        let mut catalog = self.environment.load()?;
        catalog.insert(name.to_string(), entry.clone());
        self.environment.save(&catalog)?;
        self.environment.set_active(name)?;
        let ledger_id = Uuid::parse_str(&output.ledger_id)?;
        self.ledgers
            .insert(name.to_string(), SdkLedger { entry, seed });
        if let Some(owner) = owner {
            self.grant(
                name,
                owner,
                &[
                    "admin".to_string(),
                    "accept".to_string(),
                    "archive".to_string(),
                    "comment".to_string(),
                    "deliberate".to_string(),
                    "invite".to_string(),
                    "propose".to_string(),
                    "reject".to_string(),
                    "withdraw".to_string(),
                ],
                true,
            )?;
        }
        Ok(ledger_id)
    }

    fn execute_step(&mut self, step_number: usize, step: &Step) -> Result<StepReceipt> {
        let (operation, object_id) = match step {
            Step::Propose { propose } => (
                "propose",
                Some(self.propose(
                    &propose.actor,
                    propose.replica.as_deref(),
                    &propose.ledger,
                    &propose.symbol,
                    &propose.markdown,
                )?),
            ),
            Step::Accept { accept } => (
                "accept",
                Some(self.accept(
                    &accept.actor,
                    accept.replica.as_deref(),
                    &accept.proposition,
                )?),
            ),
            Step::Revise { revise } => (
                "revise",
                Some(self.revise(
                    &revise.actor,
                    revise.replica.as_deref(),
                    &revise.proposition,
                    revise.symbol.as_deref(),
                    &revise.markdown,
                )?),
            ),
            Step::Derive { derive } => (
                "derive",
                Some(self.derive_revision(
                    &derive.actor,
                    derive.replica.as_deref(),
                    &derive.proposition,
                    &derive.common_ancestor,
                    &derive.derived_from,
                    &derive.symbol,
                    &derive.markdown,
                )?),
            ),
            Step::Reject { reject } => (
                "reject",
                Some(self.reject(
                    &reject.actor,
                    reject.replica.as_deref(),
                    &reject.proposition,
                )?),
            ),
            Step::Decide { decide } => (
                "decide",
                Some(self.decide(
                    &decide.actor,
                    decide.replica.as_deref(),
                    &decide.proposition,
                    decide.deliberation.as_deref(),
                    &decide.value,
                    decide.symbol.as_deref(),
                    &decide.supersedes,
                )?),
            ),
            Step::OpenDeliberation { open_deliberation } => (
                "open_deliberation",
                Some(self.open_deliberation(
                    &open_deliberation.actor,
                    open_deliberation.replica.as_deref(),
                    &open_deliberation.proposition,
                    &open_deliberation.revision,
                    &open_deliberation.symbol,
                )?),
            ),
            Step::Settle { settle } => (
                "settle",
                Some(self.settle(
                    &settle.actor,
                    settle.replica.as_deref(),
                    &settle.proposition,
                    &settle.revision,
                    &settle.deliberation,
                    settle.symbol.as_deref(),
                )?),
            ),
            Step::Reconcile { reconcile } => (
                "reconcile",
                Some(self.reconcile(
                    &reconcile.actor,
                    reconcile.replica.as_deref(),
                    &reconcile.symbol,
                    &reconcile.affected_proposition,
                    &reconcile.common_ancestor,
                    &reconcile.resolution_mode,
                    reconcile.selected_revision.as_deref(),
                    reconcile.result_revision.as_deref(),
                    &reconcile.resolved_tips,
                    &reconcile.conflicts,
                    &reconcile.markdown,
                )?),
            ),
            Step::SemanticCorrection {
                semantic_correction,
            } => (
                "semantic_correction",
                Some(self.semantic_correction(
                    &semantic_correction.actor,
                    semantic_correction.replica.as_deref(),
                    &semantic_correction.proposition,
                    &semantic_correction.previous_revision,
                    &semantic_correction.symbol,
                    &semantic_correction.markdown,
                )?),
            ),
            Step::RetryMissingDependency {
                retry_missing_dependency,
            } => (
                "retry_missing_dependency",
                Some(self.retry_missing_dependency(
                    &retry_missing_dependency.from,
                    &retry_missing_dependency.to,
                    retry_missing_dependency.ledger.as_deref(),
                    &retry_missing_dependency.object,
                    retry_missing_dependency.missing_dependency_kind.as_deref(),
                    retry_missing_dependency.symbol.as_deref(),
                )?),
            ),
            Step::InitReplica { init_replica } => {
                self.init_replica(
                    &init_replica.actor,
                    &init_replica.replica,
                    &init_replica.ledger,
                )?;
                ("init_replica", None)
            }
            Step::RecordDisposition { record_disposition } => (
                "record_disposition",
                Some(self.record_disposition(
                    &record_disposition.coordinator,
                    &record_disposition.object,
                    &record_disposition.disposition,
                    record_disposition.classification.as_deref(),
                    record_disposition.reason.as_deref(),
                    &record_disposition.symbol,
                )?),
            ),
            Step::Assert { assert } => {
                for assertion in assert {
                    self.execute_assertion(step_number, assertion)?;
                }
                ("assert", None)
            }
            Step::RebuildProjections { .. } => {
                self.rebuild_and_compare()?;
                ("rebuild_projections", None)
            }
            Step::CorruptProjections { .. } => {
                self.corrupt_projections()?;
                ("corrupt_projections", None)
            }
            Step::CliCheck { cli_check } => {
                self.run_cli_checks(
                    &cli_check.fact_binary_env,
                    cli_check.expected_pending_actions,
                )?;
                ("cli_check", None)
            }
            Step::Grant { grant } => {
                self.grant(
                    &grant.ledger,
                    &grant.actor,
                    &grant.capabilities,
                    grant.propagate,
                )?;
                ("grant", None)
            }
            Step::RotateKey { rotate_key } => (
                "rotate_key",
                Some(self.rotate_character_key(&rotate_key.actor)?),
            ),
            Step::Sync { sync } => {
                self.sync_environments(&sync.from, &sync.to, sync.ledger.as_deref())?;
                ("sync", None)
            }
            Step::Parallel { parallel } => {
                self.execute_parallel(step_number, parallel)?;
                ("parallel", None)
            }
            Step::Comment { comment } => (
                "comment",
                Some(self.comment(
                    &comment.actor,
                    comment.replica.as_deref(),
                    &comment.proposition,
                    &comment.message,
                )?),
            ),
            Step::Invite { invite } => (
                "invite",
                Some(
                    self.invite(
                        &invite.actor,
                        invite.replica.as_deref(),
                        &invite.proposition,
                        invite
                            .participant
                            .as_deref()
                            .context("invite step requires participant")?,
                        invite.symbol.as_deref(),
                    )?,
                ),
            ),
            Step::Join { join } => (
                "join",
                Some(self.join(
                    &join.actor,
                    join.replica.as_deref(),
                    &join.proposition,
                    join.invitation.as_deref().or(join.participant.as_deref()),
                    join.symbol.as_deref(),
                )?),
            ),
            Step::InvitationLifecycle {
                invitation_lifecycle,
            } => (
                "invitation_lifecycle",
                Some(self.invitation_lifecycle(
                    &invitation_lifecycle.actor,
                    invitation_lifecycle.replica.as_deref(),
                    &invitation_lifecycle.invitation,
                    &invitation_lifecycle.operation,
                    invitation_lifecycle.reason.as_deref(),
                    invitation_lifecycle.symbol.as_deref(),
                )?),
            ),
            Step::Leave { leave } => (
                "leave",
                Some(self.leave(&leave.actor, leave.replica.as_deref(), &leave.proposition)?),
            ),
            Step::Archive { archive } => (
                "archive",
                Some(self.archive_or_withdraw(
                    archive.actor.as_deref(),
                    archive.replica.as_deref(),
                    &archive.proposition,
                    true,
                )?),
            ),
            Step::Withdraw { withdraw } => (
                "withdraw",
                Some(self.archive_or_withdraw(
                    withdraw.actor.as_deref(),
                    withdraw.replica.as_deref(),
                    &withdraw.proposition,
                    false,
                )?),
            ),
            Step::AdvanceTime { advance_time } => {
                self.clock.advance(advance_time.duration())?;
                self.sync_runtime_clock()?;
                ("advance_time", None)
            }
        };
        Ok(StepReceipt {
            step_index: step_number,
            operation: operation.to_string(),
            object_id,
        })
    }

    fn execute_parallel(
        &mut self,
        step_number: usize,
        parallel: &fact_sim_dsl::ParallelStep,
    ) -> Result<()> {
        let branch_count = parallel.branches.len();
        if branch_count == 0 {
            bail!("parallel step requires at least one branch");
        }
        let mut branch_order = (0..branch_count).collect::<Vec<_>>();
        branch_order.sort_by_key(|index| self.schedule_key(step_number, *index));
        let max_steps = parallel
            .branches
            .iter()
            .map(|branch| branch.steps.len())
            .max()
            .unwrap_or_default();
        for offset in 0..max_steps {
            for branch_index in &branch_order {
                let branch = &parallel.branches[*branch_index];
                let Some(step) = branch.steps.get(offset) else {
                    continue;
                };
                let scheduled = step_with_default_replica(step.clone(), branch.replica.as_deref());
                self.execute_step(step_number, &scheduled)
                    .with_context(|| {
                        format!(
                            "parallel branch {} step {} failed",
                            branch_index,
                            offset + 1
                        )
                    })?;
            }
        }
        Ok(())
    }

    fn schedule_key(&self, step_number: usize, branch_index: usize) -> u64 {
        let mut value = self.seed
            ^ ((step_number as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15))
            ^ ((branch_index as u64).wrapping_mul(0xbf58_476d_1ce4_e5b9));
        value ^= value >> 30;
        value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value ^= value >> 27;
        value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    fn propose(
        &mut self,
        actor: &str,
        replica: Option<&str>,
        ledger: &str,
        symbol: &str,
        markdown: &str,
    ) -> Result<Uuid> {
        let environment = self.character_environment_name(actor, replica)?;
        let character_entry = self.character_entry(actor, ledger, replica)?;
        self.sync_runtime_clock()?;
        let result = create_proposition_with_runtime(
            &character_entry.entry,
            &character_entry.seed,
            markdown.as_bytes(),
            None,
            &self.runtime,
        )?;
        self.revisions
            .insert(symbol.to_string(), result.revision_id);
        self.deliberations
            .insert(symbol.to_string(), result.deliberation_id);
        self.propositions.insert(
            symbol.to_string(),
            SdkProposition {
                proposition_id: result.proposition_id,
                ledger: ledger.to_string(),
                environment,
                latest_revision_symbol: symbol.to_string(),
            },
        );
        Ok(result.proposition_id)
    }

    fn accept(&mut self, actor: &str, replica: Option<&str>, proposition: &str) -> Result<Uuid> {
        let proposition_state = self.proposition(proposition)?.clone();
        let environment = self.character_environment_name(actor, replica)?;
        let character_entry = self.character_entry(actor, &proposition_state.ledger, replica)?;
        self.sync_runtime_clock()?;
        let result = accept_proposition_with_runtime(
            &character_entry.entry,
            &character_entry.seed,
            Some(&proposition_state.proposition_id.to_string()),
            &self.runtime,
        )?;
        self.propositions
            .get_mut(proposition)
            .context("proposition disappeared")?
            .environment = environment;
        Ok(result.decision_id.unwrap_or(result.revision_id))
    }

    fn reject(&mut self, actor: &str, replica: Option<&str>, proposition: &str) -> Result<Uuid> {
        let proposition_state = self.proposition(proposition)?.clone();
        let environment = self.character_environment_name(actor, replica)?;
        let character_entry = self.character_entry(actor, &proposition_state.ledger, replica)?;
        self.sync_runtime_clock()?;
        let result = reject_proposition_with_runtime(
            &character_entry.entry,
            &character_entry.seed,
            Some(&proposition_state.proposition_id.to_string()),
            &self.runtime,
        )?;
        self.propositions
            .get_mut(proposition)
            .context("proposition disappeared")?
            .environment = environment;
        Ok(result.decision_id.unwrap_or(result.revision_id))
    }

    #[allow(clippy::too_many_arguments)]
    fn decide(
        &mut self,
        actor: &str,
        replica: Option<&str>,
        proposition: &str,
        deliberation: Option<&str>,
        value: &str,
        symbol: Option<&str>,
        supersedes: &[String],
    ) -> Result<Uuid> {
        let proposition_state = self.proposition(proposition)?.clone();
        let participant_actor_id = self.character(actor)?.actor_id;
        let environment = self.character_environment_name(actor, replica)?;
        let character_entry = self.character_entry(actor, &proposition_state.ledger, replica)?;
        let sdk_state = self.state_for_symbol(&proposition_state)?;
        let deliberation_id = if let Some(deliberation) = deliberation {
            self.deliberation_id(deliberation)?
        } else {
            sdk_state
                .pending_deliberation_id
                .or(sdk_state.deliberation_id)
                .context("proposition has no deliberation for decision")?
        };
        let value = match value {
            "accepted" => DecisionValue::Accepted,
            "rejected" => DecisionValue::Rejected,
            other => bail!("decision value `{other}` must be `accepted` or `rejected`"),
        };
        let supersedes_decision_ids = supersedes
            .iter()
            .map(|decision| self.decision_id(decision))
            .collect::<Result<Vec<_>>>()?;
        self.sync_runtime_clock()?;
        let result = create_decision_with_runtime(
            &character_entry.entry,
            &character_entry.seed,
            DecisionInput {
                deliberation_id,
                participant_actor_id,
                value,
                supersedes_decision_ids,
                authorization_ref: None,
            },
            &self.runtime,
        )?;
        let decision_id = Uuid::parse_str(&result.object_id)?;
        let store = fact_store::Store::open(&character_entry.entry.database)?;
        rebuild_state(&store)?;
        self.propositions
            .get_mut(proposition)
            .context("proposition disappeared")?
            .environment = environment;
        if let Some(symbol) = symbol {
            self.decisions.insert(symbol.to_string(), decision_id);
        }
        Ok(decision_id)
    }

    fn open_deliberation(
        &mut self,
        actor: &str,
        replica: Option<&str>,
        proposition: &str,
        revision: &str,
        symbol: &str,
    ) -> Result<Uuid> {
        let proposition_state = self.proposition(proposition)?.clone();
        let environment = self.character_environment_name(actor, replica)?;
        let character_entry = self.character_entry(actor, &proposition_state.ledger, replica)?;
        let revision_id = self
            .revision_id(revision)
            .with_context(|| format!("unknown revision `{revision}`"))?;
        self.sync_runtime_clock()?;
        let result = open_deliberation_for_revision_with_runtime(
            &character_entry.entry,
            &character_entry.seed,
            revision_id,
            &self.runtime,
        )?;
        self.propositions
            .get_mut(proposition)
            .context("proposition disappeared")?
            .environment = environment;
        self.deliberations
            .insert(symbol.to_string(), result.object_id);
        Ok(result.object_id)
    }

    fn settle(
        &mut self,
        actor: &str,
        replica: Option<&str>,
        proposition: &str,
        revision: &str,
        deliberation: &str,
        symbol: Option<&str>,
    ) -> Result<Uuid> {
        let proposition_state = self.proposition(proposition)?.clone();
        let environment = self.character_environment_name(actor, replica)?;
        let character_entry = self.character_entry(actor, &proposition_state.ledger, replica)?;
        let revision_id = self
            .revision_id(revision)
            .with_context(|| format!("unknown revision `{revision}`"))?;
        let deliberation_id = self.deliberation_id(deliberation)?;
        self.sync_runtime_clock()?;
        let result = create_settlement_with_runtime(
            &character_entry.entry,
            &character_entry.seed,
            SettlementInput {
                deliberation_id,
                revision_id,
                producer_type: SettlementProducerType::Participant,
                producer_id: None,
            },
            &self.runtime,
        )?;
        let settlement_id = Uuid::parse_str(&result.object_id)?;
        self.propositions
            .get_mut(proposition)
            .context("proposition disappeared")?
            .environment = environment;
        if let Some(symbol) = symbol {
            self.settlements.insert(symbol.to_string(), settlement_id);
        }
        Ok(settlement_id)
    }

    #[allow(clippy::too_many_arguments)]
    fn reconcile(
        &mut self,
        actor: &str,
        replica: Option<&str>,
        symbol: &str,
        affected_proposition: &str,
        common_ancestor: &str,
        resolution_mode: &str,
        selected_revision: Option<&str>,
        result_revision: Option<&str>,
        resolved_tips: &[String],
        conflicts: &[fact_sim_dsl::ReconciliationConflictStep],
        markdown: &str,
    ) -> Result<Uuid> {
        let affected = self.proposition(affected_proposition)?.clone();
        let environment = self.character_environment_name(actor, replica)?;
        let character_entry = self.character_entry(actor, &affected.ledger, replica)?;
        let conflict_inputs = conflicts
            .iter()
            .map(|conflict| {
                Ok(ReconciliationConflictInput {
                    revision_id: self
                        .revision_id(&conflict.revision)
                        .with_context(|| format!("unknown revision `{}`", conflict.revision))?,
                    deliberation_id: self.deliberation_id(&conflict.deliberation)?,
                    settlement_id: self.settlement_id(&conflict.settlement)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        self.sync_runtime_clock()?;
        let result = create_reconciliation_proposition_with_runtime(
            &character_entry.entry,
            &character_entry.seed,
            ReconciliationInput {
                affected_proposition_id: affected.proposition_id,
                common_ancestor_revision_id: self
                    .revision_id(common_ancestor)
                    .with_context(|| format!("unknown common ancestor `{common_ancestor}`"))?,
                conflicts: conflict_inputs,
                detecting_actor_id: self.character(actor)?.actor_id,
                resolution_mode: resolution_mode.to_string(),
                resolved_tip_ids: resolved_tips
                    .iter()
                    .map(|tip| {
                        self.revision_id(tip)
                            .with_context(|| format!("unknown resolved tip `{tip}`"))
                    })
                    .collect::<Result<Vec<_>>>()?,
                selected_revision_id: selected_revision
                    .map(|revision| {
                        self.revision_id(revision)
                            .with_context(|| format!("unknown selected revision `{revision}`"))
                    })
                    .transpose()?,
                result_revision_id: result_revision
                    .map(|revision| {
                        self.revision_id(revision)
                            .with_context(|| format!("unknown result revision `{revision}`"))
                    })
                    .transpose()?,
                markdown: Some(markdown.as_bytes().to_vec()),
            },
            &self.runtime,
        )?;
        self.revisions
            .insert(symbol.to_string(), result.revision_id);
        self.deliberations
            .insert(symbol.to_string(), result.deliberation_id);
        self.propositions.insert(
            symbol.to_string(),
            SdkProposition {
                proposition_id: result.proposition_id,
                ledger: affected.ledger,
                environment,
                latest_revision_symbol: symbol.to_string(),
            },
        );
        Ok(result.proposition_id)
    }

    fn revise(
        &mut self,
        actor: &str,
        replica: Option<&str>,
        proposition: &str,
        symbol: Option<&str>,
        markdown: &str,
    ) -> Result<Uuid> {
        let proposition_state = self.proposition(proposition)?.clone();
        let environment = self.character_environment_name(actor, replica)?;
        let character_entry = self.character_entry(actor, &proposition_state.ledger, replica)?;
        self.sync_runtime_clock()?;
        let result = update_proposition_content_with_runtime(
            &character_entry.entry,
            &character_entry.seed,
            &proposition_state.proposition_id.to_string(),
            markdown.as_bytes(),
            &self.runtime,
        )?;
        let revision_symbol = symbol.unwrap_or("latest").to_string();
        self.revisions
            .insert(revision_symbol.clone(), result.revision_id);
        self.deliberations
            .insert(revision_symbol.clone(), result.deliberation_id);
        self.propositions
            .get_mut(proposition)
            .context("proposition disappeared")?
            .latest_revision_symbol = revision_symbol;
        self.propositions
            .get_mut(proposition)
            .context("proposition disappeared")?
            .environment = environment;
        Ok(result.revision_id)
    }

    #[allow(clippy::too_many_arguments)]
    fn derive_revision(
        &mut self,
        actor: &str,
        replica: Option<&str>,
        proposition: &str,
        common_ancestor: &str,
        derived_from: &[String],
        symbol: &str,
        markdown: &str,
    ) -> Result<Uuid> {
        let proposition_state = self.proposition(proposition)?.clone();
        let environment = self.character_environment_name(actor, replica)?;
        let character_entry = self.character_entry(actor, &proposition_state.ledger, replica)?;
        self.sync_runtime_clock()?;
        let result = create_derived_revision_with_runtime(
            &character_entry.entry,
            &character_entry.seed,
            DerivedRevisionInput {
                proposition_id: proposition_state.proposition_id,
                parent_revision_id: self
                    .revision_id(common_ancestor)
                    .with_context(|| format!("unknown common ancestor `{common_ancestor}`"))?,
                contributing_revision_ids: derived_from
                    .iter()
                    .map(|revision| {
                        self.revision_id(revision)
                            .with_context(|| format!("unknown derived source `{revision}`"))
                    })
                    .collect::<Result<Vec<_>>>()?,
                markdown: markdown.as_bytes().to_vec(),
            },
            &self.runtime,
        )?;
        self.revisions
            .insert(symbol.to_string(), result.revision_id);
        self.deliberations
            .insert(symbol.to_string(), result.deliberation_id);
        self.propositions
            .get_mut(proposition)
            .context("proposition disappeared")?
            .latest_revision_symbol = symbol.to_string();
        self.propositions
            .get_mut(proposition)
            .context("proposition disappeared")?
            .environment = environment;
        Ok(result.revision_id)
    }

    fn semantic_correction(
        &mut self,
        actor: &str,
        replica: Option<&str>,
        proposition: &str,
        previous_revision: &str,
        symbol: &str,
        markdown: &str,
    ) -> Result<Uuid> {
        let proposition_state = self.proposition(proposition)?.clone();
        let previous_revision_id = self
            .revision_id(previous_revision)
            .with_context(|| format!("unknown previous revision `{previous_revision}`"))?;
        let environment = self.character_environment_name(actor, replica)?;
        let character_entry = self.character_entry(actor, &proposition_state.ledger, replica)?;
        let ledger_id = Uuid::parse_str(&character_entry.entry.ledger_id)?;
        let store = fact_store::Store::open(&character_entry.entry.database)?;
        let previous = export_object(&store, ledger_id, previous_revision_id)?;
        let previous_payload_hash = payload_hash(&previous.bytes)?;
        let previous_signed_object_hash = signed_object_hash(&previous.bytes);

        self.sync_runtime_clock()?;
        let result = update_proposition_content_with_runtime(
            &character_entry.entry,
            &character_entry.seed,
            &proposition_state.proposition_id.to_string(),
            markdown.as_bytes(),
            &self.runtime,
        )?;
        self.revisions
            .insert(symbol.to_string(), result.revision_id);
        self.deliberations
            .insert(symbol.to_string(), result.deliberation_id);
        self.propositions
            .get_mut(proposition)
            .context("proposition disappeared")?
            .latest_revision_symbol = symbol.to_string();
        self.propositions
            .get_mut(proposition)
            .context("proposition disappeared")?
            .environment = environment;

        self.accept(actor, replica, proposition)?;

        let preserved = export_object(&store, ledger_id, previous_revision_id)?;
        let corrective = export_object(&store, ledger_id, result.revision_id)?;
        let preserved_payload_hash = payload_hash(&preserved.bytes)?;
        let preserved_signed_object_hash = signed_object_hash(&preserved.bytes);
        let corrective_payload_hash = payload_hash(&corrective.bytes)?;
        let corrective_signed_object_hash = signed_object_hash(&corrective.bytes);
        let preserved_history = previous_payload_hash == preserved_payload_hash
            && previous_signed_object_hash == preserved_signed_object_hash;
        let requires_new_signed_object = previous_revision_id != result.revision_id
            && previous_payload_hash != corrective_payload_hash
            && previous_signed_object_hash != corrective_signed_object_hash;
        let effective_changed = self
            .state_for(proposition)?
            .effective_revision_id
            .is_some_and(|effective| effective == result.revision_id);

        self.semantic_correction_records.insert(
            symbol.to_string(),
            ScenarioSemanticCorrectionRecord {
                scenario: self.scenario_name.clone(),
                name: symbol.to_string(),
                proposition_id: proposition_state.proposition_id,
                previous_revision_id,
                corrective_revision_id: result.revision_id,
                previous_payload_hash,
                preserved_payload_hash,
                previous_signed_object_hash,
                preserved_signed_object_hash,
                corrective_payload_hash,
                corrective_signed_object_hash,
                preserved_history,
                requires_new_signed_object,
                effective_changed,
            },
        );
        self.repair_report.semantic_corrections += 1;
        self.repair_report.canonical_history_preserved = preserved_history;
        self.repair_report.repairs.push(ScenarioRepairRecord {
            scenario: self.scenario_name.clone(),
            repair_type: "semantic-correction".to_string(),
            object_id: Some(result.revision_id),
            detected: true,
            retry_unchanged: false,
            converged: true,
            canonical_history_preserved: preserved_history,
        });
        Ok(result.revision_id)
    }

    fn comment(
        &mut self,
        actor: &str,
        replica: Option<&str>,
        proposition: &str,
        message: &str,
    ) -> Result<Uuid> {
        let proposition_state = self.proposition(proposition)?.clone();
        let environment = self.character_environment_name(actor, replica)?;
        let character_entry = self.character_entry(actor, &proposition_state.ledger, replica)?;
        self.sync_runtime_clock()?;
        let result = create_comment_with_runtime(
            &character_entry.entry,
            &character_entry.seed,
            &proposition_state.proposition_id.to_string(),
            message.as_bytes(),
            &self.runtime,
        )?;
        self.propositions
            .get_mut(proposition)
            .context("proposition disappeared")?
            .environment = environment;
        Ok(result.comment_id)
    }

    fn invite(
        &mut self,
        actor: &str,
        replica: Option<&str>,
        proposition: &str,
        participant: &str,
        symbol: Option<&str>,
    ) -> Result<Uuid> {
        let proposition_state = self.proposition(proposition)?.clone();
        let environment = self.character_environment_name(actor, replica)?;
        let (participant_id, participant_bundle) = {
            let character = self.character(participant)?;
            (character.actor_id, character.identity_bundle.clone())
        };
        let character_entry = self.character_entry(actor, &proposition_state.ledger, replica)?;
        import_identity(&character_entry.entry, &participant_bundle).with_context(|| {
            format!("import participant `{participant}` into replica `{environment}`")
        })?;
        self.sync_runtime_clock()?;
        let result = create_invitation_with_runtime(
            &character_entry.entry,
            &character_entry.seed,
            &proposition_state.proposition_id.to_string(),
            &participant_id.to_string(),
            &self.runtime,
        )?;
        let invitation_symbol = symbol
            .map(str::to_string)
            .unwrap_or_else(|| format!("{proposition}:{participant}:invitation"));
        self.invitations
            .insert(invitation_symbol, result.invitation_id);
        self.propositions
            .get_mut(proposition)
            .context("proposition disappeared")?
            .environment = environment;
        Ok(result.invitation_id)
    }

    fn join(
        &mut self,
        actor: &str,
        replica: Option<&str>,
        proposition: &str,
        invitation: Option<&str>,
        symbol: Option<&str>,
    ) -> Result<Uuid> {
        let proposition_state = self.proposition(proposition)?.clone();
        let environment = self.character_environment_name(actor, replica)?;
        let character_entry = self.character_entry(actor, &proposition_state.ledger, replica)?;
        let invitation_id = self.invitation_id(invitation)?;
        self.sync_runtime_clock()?;
        let result = join_deliberation_with_runtime(
            &character_entry.entry,
            &character_entry.seed,
            &proposition_state.proposition_id.to_string(),
            &invitation_id.to_string(),
            &self.runtime,
        )?;
        self.propositions
            .get_mut(proposition)
            .context("proposition disappeared")?
            .environment = environment;
        let participant_symbol = symbol
            .map(str::to_string)
            .unwrap_or_else(|| format!("{proposition}:{actor}:join"));
        self.participant_changes
            .insert(participant_symbol, result.change_id);
        Ok(result.change_id)
    }

    fn invitation_lifecycle(
        &mut self,
        actor: &str,
        replica: Option<&str>,
        invitation: &str,
        operation: &str,
        reason: Option<&str>,
        symbol: Option<&str>,
    ) -> Result<Uuid> {
        if !matches!(operation, "decline" | "revoke" | "supersede") {
            bail!("unsupported invitation lifecycle operation `{operation}`");
        }
        let invitation_id = self.invitation_id(Some(invitation))?;
        let ledger_name = self.single_ledger_name()?;
        let character_entry = self.character_entry(actor, &ledger_name, replica)?;
        self.sync_runtime_clock()?;
        let result = update_invitation_lifecycle_with_runtime(
            &character_entry.entry,
            &character_entry.seed,
            &invitation_id.to_string(),
            operation,
            reason.unwrap_or(operation),
            &self.runtime,
        )?;
        let lifecycle_symbol = symbol
            .map(str::to_string)
            .unwrap_or_else(|| format!("{invitation}:{operation}"));
        self.invitation_lifecycles
            .insert(lifecycle_symbol, result.lifecycle_id);
        Ok(result.lifecycle_id)
    }

    fn leave(&mut self, actor: &str, replica: Option<&str>, proposition: &str) -> Result<Uuid> {
        let proposition_state = self.proposition(proposition)?.clone();
        let environment = self.character_environment_name(actor, replica)?;
        let character_entry = self.character_entry(actor, &proposition_state.ledger, replica)?;
        self.sync_runtime_clock()?;
        let result = leave_deliberation_with_runtime(
            &character_entry.entry,
            &character_entry.seed,
            &proposition_state.proposition_id.to_string(),
            &self.runtime,
        )?;
        self.propositions
            .get_mut(proposition)
            .context("proposition disappeared")?
            .environment = environment;
        Ok(result.change_id)
    }

    fn archive_or_withdraw(
        &mut self,
        actor: Option<&str>,
        replica: Option<&str>,
        proposition: &str,
        archive: bool,
    ) -> Result<Uuid> {
        let proposition_state = self.proposition(proposition)?.clone();
        let actor = actor.map(str::to_string).unwrap_or_else(|| {
            self.environment_owner(&proposition_state.environment)
                .unwrap()
        });
        let environment = self.character_environment_name(&actor, replica)?;
        let character_entry = self.character_entry(&actor, &proposition_state.ledger, replica)?;
        self.sync_runtime_clock()?;
        let result = if archive {
            archive_proposition_with_runtime(
                &character_entry.entry,
                &character_entry.seed,
                &proposition_state.proposition_id.to_string(),
                "scenario archive",
                &self.runtime,
            )?
        } else {
            withdraw_proposition_with_runtime(
                &character_entry.entry,
                &character_entry.seed,
                &proposition_state.proposition_id.to_string(),
                "scenario withdraw",
                &self.runtime,
            )?
        };
        self.propositions
            .get_mut(proposition)
            .context("proposition disappeared")?
            .environment = environment;
        Ok(result.lifecycle_id)
    }

    fn execute_assertion(&mut self, step_number: usize, assertion: &Assertion) -> Result<()> {
        let assertion_name = assertion_name(assertion);
        let result = match assertion {
            Assertion::Status { status } => {
                let actual = self.state_for(&status.proposition)?.status;
                self.assert_eq(
                    step_number,
                    "status",
                    &status.equals,
                    &actual,
                    &status.proposition,
                )
            }
            Assertion::EffectiveRevision { effective_revision } => {
                let actual = self
                    .state_for(&effective_revision.proposition)?
                    .effective_revision_id
                    .map(|id| self.revision_symbol_for(id))
                    .unwrap_or_else(|| "none".to_string());
                self.assert_eq(
                    step_number,
                    "effective_revision",
                    &effective_revision.equals,
                    &actual,
                    &effective_revision.proposition,
                )
            }
            Assertion::LatestRevision { latest_revision } => {
                let actual = self
                    .state_for(&latest_revision.proposition)?
                    .latest_revision_id
                    .map(|id| self.revision_symbol_for(id))
                    .unwrap_or_else(|| "none".to_string());
                self.assert_eq(
                    step_number,
                    "latest_revision",
                    &latest_revision.equals,
                    &actual,
                    &latest_revision.proposition,
                )
            }
            Assertion::RevisionStatus { revision_status } => {
                let revision_id = self
                    .revision_id(&revision_status.revision)
                    .with_context(|| format!("unknown revision `{}`", revision_status.revision))?;
                let actual = self.revision_state(revision_id)?.status;
                self.assert_eq(
                    step_number,
                    "revision_status",
                    &revision_status.equals,
                    &actual,
                    &revision_status.revision,
                )
            }
            Assertion::PendingActionCount {
                pending_action_count,
            } => {
                let ledger_name = self.single_ledger_name()?;
                let character_entry =
                    self.character_entry(&pending_action_count.actor, &ledger_name, None)?;
                let actual = pending_propositions(&character_entry.entry)?
                    .len()
                    .to_string();
                self.assert_eq(
                    step_number,
                    "pending_action_count",
                    &pending_action_count.equals.to_string(),
                    &actual,
                    &pending_action_count.actor,
                )
            }
            Assertion::ObjectCount { object_count } => {
                let actual = self
                    .snapshot_any()?
                    .object_counts_by_type
                    .values()
                    .sum::<usize>();
                self.assert_eq(
                    step_number,
                    "object_count",
                    &object_count.equals.to_string(),
                    &actual.to_string(),
                    "canonical_objects",
                )
            }
            Assertion::LatestContent { latest_content } => {
                let proposition = self.proposition(&latest_content.proposition)?;
                let entry = self
                    .environment_ledger_entry(&proposition.environment, &proposition.ledger)?
                    .unwrap_or_else(|| {
                        self.ledger(&proposition.ledger)
                            .expect("ledger exists")
                            .entry
                            .clone()
                    });
                let content = read_proposition_content_with_selection(
                    &entry,
                    &proposition.proposition_id.to_string(),
                    ContentSelection::Latest,
                )?;
                let actual = String::from_utf8_lossy(&content.content).to_string();
                if actual.contains(&latest_content.contains) {
                    Ok(())
                } else {
                    Err(self.failure(
                        step_number,
                        "latest_content",
                        format!("content contains `{}`", latest_content.contains),
                        actual,
                    ))
                }
            }
            Assertion::ProjectionRebuildEquivalent {
                projection_rebuild_equivalent,
            } => {
                let actual = self
                    .last_rebuild_equivalent
                    .context("projection rebuild has not been run")?
                    .to_string();
                self.assert_eq(
                    step_number,
                    "projection_rebuild_equivalent",
                    &projection_rebuild_equivalent.equals.to_string(),
                    &actual,
                    "projection_rebuild",
                )
            }
            Assertion::CanonicalHistoryUnchanged {
                canonical_history_unchanged,
            } => {
                let actual = self
                    .last_canonical_history_unchanged
                    .context("projection repair has not been run")?
                    .to_string();
                self.assert_eq(
                    step_number,
                    "canonical_history_unchanged",
                    &canonical_history_unchanged.equals.to_string(),
                    &actual,
                    "canonical_history",
                )
            }
            Assertion::Conflict { conflict } => self.assert_conflict(step_number, conflict),
            Assertion::DecisionConflict { decision_conflict } => {
                self.assert_decision_conflict(step_number, decision_conflict)
            }
            Assertion::DeliberationConflict {
                deliberation_conflict,
            } => self.assert_deliberation_conflict(step_number, deliberation_conflict),
            Assertion::DerivedRevision { derived_revision } => {
                self.assert_derived_revision(step_number, derived_revision)
            }
            Assertion::Reconciliation { reconciliation } => {
                self.assert_reconciliation(step_number, reconciliation)
            }
            Assertion::ReconciliationConflict {
                reconciliation_conflict,
            } => self.assert_reconciliation_conflict(step_number, reconciliation_conflict),
            Assertion::SemanticCorrection {
                semantic_correction,
            } => self.assert_semantic_correction(step_number, semantic_correction),
            Assertion::Retry { retry } => self.assert_retry(step_number, retry),
            Assertion::CoordinatorDisposition {
                coordinator_disposition,
            } => self.assert_coordinator_disposition(step_number, coordinator_disposition),
            Assertion::InvitationRace { invitation_race } => {
                self.assert_invitation_race(step_number, invitation_race)
            }
        };
        if result.is_ok() {
            *self
                .assertion_results
                .entry(assertion_name.to_string())
                .or_default() += 1;
            if let Assertion::Conflict { conflict } = assertion {
                *self
                    .conflict_counts_by_class
                    .entry(conflict.conflict_type.clone())
                    .or_default() += 1;
            }
            if let Assertion::DecisionConflict { decision_conflict } = assertion
                && decision_conflict.equals
            {
                *self
                    .conflict_counts_by_class
                    .entry("decision-conflict".to_string())
                    .or_default() += 1;
            }
            if let Assertion::DeliberationConflict {
                deliberation_conflict,
            } = assertion
                && deliberation_conflict.equals
            {
                *self
                    .conflict_counts_by_class
                    .entry("incompatible-parallel-deliberations".to_string())
                    .or_default() += 1;
            }
            if let Assertion::ReconciliationConflict {
                reconciliation_conflict,
            } = assertion
                && reconciliation_conflict.equals
            {
                *self
                    .conflict_counts_by_class
                    .entry("conflicting-reconciliation-outcomes".to_string())
                    .or_default() += 1;
            }
            if let Assertion::InvitationRace { invitation_race } = assertion
                && invitation_race.equals
            {
                *self
                    .conflict_counts_by_class
                    .entry("invitation-race".to_string())
                    .or_default() += 1;
            }
        }
        result
    }

    fn assert_conflict(
        &self,
        step_number: usize,
        conflict: &fact_sim_dsl::ConflictAssertion,
    ) -> Result<()> {
        let proposition_state = self.state_for(&conflict.proposition)?;
        self.assert_eq(
            step_number,
            "conflict_status",
            "conflict",
            &proposition_state.effective_status,
            &conflict.proposition,
        )?;
        let ancestor_id = self
            .revision_id(&conflict.last_undisputed_ancestor)
            .with_context(|| {
                format!(
                    "unknown last undisputed ancestor `{}`",
                    conflict.last_undisputed_ancestor
                )
            })?;
        let effective_id = proposition_state
            .effective_revision_id
            .context("conflict proposition has no effective ancestor")?;
        if effective_id != ancestor_id {
            return Err(self.failure(
                step_number,
                "conflict_last_undisputed_ancestor",
                format!(
                    "{}: {}",
                    conflict.proposition, conflict.last_undisputed_ancestor
                ),
                format!(
                    "{}: {}",
                    conflict.proposition,
                    self.revision_symbol_for(effective_id)
                ),
            ));
        }
        if conflict.conflict_type != "accepted-sibling-revisions" {
            bail!(
                "unsupported conflict assertion type `{}`",
                conflict.conflict_type
            );
        }
        if conflict.branch_tips.len() < 2 {
            bail!("accepted-sibling-revisions assertion requires at least two branch tips");
        }
        let mut tip_ids = Vec::new();
        for tip in &conflict.branch_tips {
            tip_ids.push(
                self.revision_id(tip)
                    .with_context(|| format!("unknown conflict branch tip `{tip}`"))?,
            );
        }
        let mut parents = BTreeMap::<Uuid, Vec<Uuid>>::new();
        for tip_id in &tip_ids {
            let parent_id = self
                .revision_parent(*tip_id)?
                .with_context(|| format!("conflict branch tip `{tip_id}` has no parent"))?;
            parents.entry(parent_id).or_default().push(*tip_id);
            let state = self.revision_state(*tip_id)?;
            if state.effective {
                return Err(self.failure(
                    step_number,
                    "conflict_no_arbitrary_winner",
                    "no branch tip is effective",
                    format!("{} is effective", self.revision_symbol_for(*tip_id)),
                ));
            }
        }
        if parents.len() != 1 || !parents.contains_key(&ancestor_id) {
            return Err(self.failure(
                step_number,
                "conflict_branch_tips",
                format!(
                    "all branch tips parent {}",
                    conflict.last_undisputed_ancestor
                ),
                format!(
                    "branch parent set {:?}",
                    parents.keys().map(Uuid::to_string).collect::<Vec<_>>()
                ),
            ));
        }
        if conflict.reconciliation_required
            && proposition_state
                .latest_revision_id
                .is_some_and(|latest_id| latest_id != ancestor_id)
        {
            return Err(self.failure(
                step_number,
                "conflict_reconciliation_required",
                "no latest winner before reconciliation",
                format!(
                    "latest revision is {:?}",
                    proposition_state
                        .latest_revision_id
                        .map(|id| self.revision_symbol_for(id))
                ),
            ));
        }
        if conflict.no_arbitrary_winner && tip_ids.contains(&effective_id) {
            return Err(self.failure(
                step_number,
                "conflict_no_arbitrary_winner",
                "effective state remains on ancestor",
                format!(
                    "effective state selected {}",
                    self.revision_symbol_for(effective_id)
                ),
            ));
        }
        Ok(())
    }

    fn assert_decision_conflict(
        &self,
        step_number: usize,
        assertion: &fact_sim_dsl::DecisionConflictAssertion,
    ) -> Result<()> {
        let proposition = self.proposition(&assertion.proposition)?;
        let state = self.state_for_symbol(proposition)?;
        let deliberation_id = state
            .pending_deliberation_id
            .or(state.deliberation_id)
            .context("proposition has no deliberation for decision conflict assertion")?;
        let entry = self
            .environment_ledger_entry(&proposition.environment, &proposition.ledger)?
            .unwrap_or_else(|| {
                self.ledger(&proposition.ledger)
                    .expect("ledger exists")
                    .entry
                    .clone()
            });
        let (consensus, applicable_decision_count) =
            self.consensus_for_deliberation(&entry, deliberation_id)?;
        let actual_conflict = consensus == "conflict";
        self.assert_eq(
            step_number,
            "decision_conflict",
            &assertion.equals.to_string(),
            &actual_conflict.to_string(),
            &assertion.proposition,
        )?;
        if let Some(expected) = assertion.applicable_decision_count {
            self.assert_eq(
                step_number,
                "decision_applicable_count",
                &expected.to_string(),
                &applicable_decision_count.to_string(),
                &assertion.proposition,
            )?;
        }
        if assertion.superseded_by.is_none()
            && let Some(participant) = &assertion.participant
        {
            let participant_id = self.character(participant)?.actor_id;
            for decision in &assertion.decision_tips {
                let decision_id = self.decision_id(decision)?;
                self.assert_decision_participant(
                    step_number,
                    &entry,
                    decision_id,
                    participant_id,
                    deliberation_id,
                )?;
            }
        }
        if let Some(superseding) = &assertion.superseded_by {
            let superseding_id = self.decision_id(superseding)?;
            let expected = assertion
                .decision_tips
                .iter()
                .map(|decision| self.decision_id(decision))
                .collect::<Result<BTreeSet<_>>>()?;
            let actual = self.decision_supersedes(superseding_id)?;
            if actual != expected {
                return Err(self.failure(
                    step_number,
                    "decision_supersedes",
                    format!(
                        "supersedes {:?}",
                        expected.iter().map(Uuid::to_string).collect::<Vec<_>>()
                    ),
                    format!(
                        "supersedes {:?}",
                        actual.iter().map(Uuid::to_string).collect::<Vec<_>>()
                    ),
                ));
            }
        }
        Ok(())
    }

    fn assert_deliberation_conflict(
        &self,
        step_number: usize,
        assertion: &fact_sim_dsl::DeliberationConflictAssertion,
    ) -> Result<()> {
        let proposition = self.proposition(&assertion.proposition)?;
        let revision_id = self
            .revision_id(&assertion.revision)
            .with_context(|| format!("unknown revision `{}`", assertion.revision))?;
        let entry = self
            .environment_ledger_entry(&proposition.environment, &proposition.ledger)?
            .unwrap_or_else(|| {
                self.ledger(&proposition.ledger)
                    .expect("ledger exists")
                    .entry
                    .clone()
            });
        let effective = self.effective_projection(&entry, proposition.proposition_id)?;
        let actual_conflict = effective.status == "conflict";
        self.assert_eq(
            step_number,
            "deliberation_conflict",
            &assertion.equals.to_string(),
            &actual_conflict.to_string(),
            &assertion.proposition,
        )?;
        if let Some(reason) = &assertion.reason {
            self.assert_eq(
                step_number,
                "deliberation_conflict_reason",
                reason,
                &effective.reason,
                &assertion.proposition,
            )?;
        }
        for deliberation in &assertion.deliberations {
            let deliberation_id = self.deliberation_id(deliberation)?;
            let (projected_revision, consensus) =
                self.deliberation_projection(&entry, deliberation_id)?;
            if projected_revision != revision_id {
                return Err(self.failure(
                    step_number,
                    "deliberation_conflict_revision",
                    format!(
                        "deliberation {deliberation} evaluates {}",
                        assertion.revision
                    ),
                    format!("deliberation {deliberation} evaluates {projected_revision}"),
                ));
            }
            if assertion.equals && !matches!(consensus.as_str(), "accepted" | "rejected") {
                return Err(self.failure(
                    step_number,
                    "deliberation_conflict_settlement",
                    format!("deliberation {deliberation} settled accepted or rejected"),
                    format!("deliberation {deliberation} consensus is {consensus}"),
                ));
            }
        }
        Ok(())
    }

    fn assert_derived_revision(
        &self,
        step_number: usize,
        assertion: &fact_sim_dsl::DerivedRevisionAssertion,
    ) -> Result<()> {
        let revision_id = self
            .revision_id(&assertion.revision)
            .with_context(|| format!("unknown derived revision `{}`", assertion.revision))?;
        let expected_parent = self
            .revision_id(&assertion.parent)
            .with_context(|| format!("unknown derived revision parent `{}`", assertion.parent))?;
        let expected_sources = assertion
            .derived_from
            .iter()
            .map(|source| {
                self.revision_id(source)
                    .with_context(|| format!("unknown derived source `{source}`"))
            })
            .collect::<Result<BTreeSet<_>>>()?;
        let payload = self
            .payload_for_object(revision_id)?
            .with_context(|| format!("derived revision `{}` is not stored", assertion.revision))?;
        let value: serde_json::Value = serde_json::from_slice(&payload)?;
        let body = value
            .get("body")
            .and_then(serde_json::Value::as_object)
            .context("derived revision payload is missing body")?;
        let parent = body
            .get("parent_revision_id")
            .and_then(serde_json::Value::as_str)
            .context("derived revision is missing parent_revision_id")?
            .parse::<Uuid>()?;
        if parent != expected_parent {
            return Err(self.failure(
                step_number,
                "derived_revision_parent",
                expected_parent.to_string(),
                parent.to_string(),
            ));
        }
        let actual_sources = body
            .get("relationships")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flat_map(|relationships| relationships.iter())
            .filter(|relationship| {
                relationship
                    .get("relationship")
                    .and_then(serde_json::Value::as_str)
                    == Some("protocol:derived-from")
            })
            .flat_map(|relationship| {
                relationship
                    .get("targets")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flat_map(|targets| targets.iter())
            })
            .map(|target| {
                target
                    .as_str()
                    .context("derived-from target is not a string")?
                    .parse::<Uuid>()
                    .map_err(Into::into)
            })
            .collect::<Result<BTreeSet<_>>>()?;
        if actual_sources != expected_sources {
            return Err(self.failure(
                step_number,
                "derived_revision_sources",
                format!(
                    "{:?}",
                    expected_sources
                        .iter()
                        .map(Uuid::to_string)
                        .collect::<Vec<_>>()
                ),
                format!(
                    "{:?}",
                    actual_sources
                        .iter()
                        .map(Uuid::to_string)
                        .collect::<Vec<_>>()
                ),
            ));
        }
        Ok(())
    }

    fn assert_reconciliation(
        &self,
        step_number: usize,
        assertion: &fact_sim_dsl::ReconciliationAssertion,
    ) -> Result<()> {
        let proposition = self.proposition(&assertion.proposition)?;
        let revision_id = self.revision_id(&assertion.proposition).with_context(|| {
            format!(
                "unknown reconciliation revision `{}`",
                assertion.proposition
            )
        })?;
        let entry = self
            .environment_ledger_entry(&proposition.environment, &proposition.ledger)?
            .unwrap_or_else(|| {
                self.ledger(&proposition.ledger)
                    .expect("ledger exists")
                    .entry
                    .clone()
            });
        let projection = self.reconciliation_projection(&entry, revision_id)?;
        self.assert_eq(
            step_number,
            "reconciliation_mode",
            &assertion.resolution_mode,
            &projection.resolution_mode,
            &assertion.proposition,
        )?;
        self.assert_eq(
            step_number,
            "reconciliation_affected_proposition",
            &self
                .proposition(&assertion.affected_proposition)?
                .proposition_id
                .to_string(),
            &projection.affected_proposition_id.to_string(),
            &assertion.proposition,
        )?;
        self.assert_eq(
            step_number,
            "reconciliation_common_ancestor",
            &self
                .revision_id(&assertion.common_ancestor)
                .with_context(|| {
                    format!("unknown common ancestor `{}`", assertion.common_ancestor)
                })?
                .to_string(),
            &projection.common_ancestor_revision_id.to_string(),
            &assertion.proposition,
        )?;
        let selected = assertion
            .selected_revision
            .as_deref()
            .map(|revision| {
                self.revision_id(revision)
                    .with_context(|| format!("unknown selected revision `{revision}`"))
            })
            .transpose()?;
        if selected != projection.selected_revision_id {
            return Err(self.failure(
                step_number,
                "reconciliation_selected_revision",
                format!("{selected:?}"),
                format!("{:?}", projection.selected_revision_id),
            ));
        }
        let result = assertion
            .result_revision
            .as_deref()
            .map(|revision| {
                self.revision_id(revision)
                    .with_context(|| format!("unknown result revision `{revision}`"))
            })
            .transpose()?;
        if result != projection.result_revision_id {
            return Err(self.failure(
                step_number,
                "reconciliation_result_revision",
                format!("{result:?}"),
                format!("{:?}", projection.result_revision_id),
            ));
        }
        for conflict in &assertion.conflicts {
            let revision_id = self
                .revision_id(conflict)
                .with_context(|| format!("unknown reconciliation conflict `{conflict}`"))?;
            if !projection.conflict_revision_ids.contains(&revision_id) {
                return Err(self.failure(
                    step_number,
                    "reconciliation_conflicts",
                    format!("manifest contains conflict revision {conflict}"),
                    format!(
                        "manifest conflict revisions {:?}",
                        projection
                            .conflict_revision_ids
                            .iter()
                            .map(Uuid::to_string)
                            .collect::<Vec<_>>()
                    ),
                ));
            }
        }
        Ok(())
    }

    fn assert_reconciliation_conflict(
        &self,
        step_number: usize,
        assertion: &fact_sim_dsl::ReconciliationConflictAssertion,
    ) -> Result<()> {
        let proposition = self.proposition(&assertion.proposition)?;
        let entry = self
            .environment_ledger_entry(&proposition.environment, &proposition.ledger)?
            .unwrap_or_else(|| {
                self.ledger(&proposition.ledger)
                    .expect("ledger exists")
                    .entry
                    .clone()
            });
        let effective = self.effective_projection(&entry, proposition.proposition_id)?;
        let actual_conflict = effective.status == "conflict";
        self.assert_eq(
            step_number,
            "reconciliation_conflict",
            &assertion.equals.to_string(),
            &actual_conflict.to_string(),
            &assertion.proposition,
        )?;
        if let Some(reason) = &assertion.reason {
            self.assert_eq(
                step_number,
                "reconciliation_conflict_reason",
                reason,
                &effective.reason,
                &assertion.proposition,
            )?;
        }
        Ok(())
    }

    fn assert_retry(
        &self,
        step_number: usize,
        assertion: &fact_sim_dsl::RetryAssertion,
    ) -> Result<()> {
        let record = self
            .retry_records
            .get(&assertion.retry)
            .with_context(|| format!("retry record `{}` is not declared", assertion.retry))?;
        if let Some(expected) = &assertion.first_disposition {
            self.assert_eq(
                step_number,
                "retry_first_disposition",
                expected,
                record.first_disposition.as_str(),
                &assertion.retry,
            )?;
        }
        if let Some(expected) = &assertion.retry_disposition {
            self.assert_eq(
                step_number,
                "retry_disposition",
                expected,
                record.retry_disposition.as_str(),
                &assertion.retry,
            )?;
        }
        if let Some(expected) = &assertion.classification {
            self.assert_eq(
                step_number,
                "retry_classification",
                expected,
                failure_classification_as_str(&record.classification),
                &assertion.retry,
            )?;
        }
        if let Some(expected) = &assertion.missing_dependency_kind {
            self.assert_eq(
                step_number,
                "retry_missing_dependency_kind",
                expected,
                &record.missing_dependency_kind,
                &assertion.retry,
            )?;
        }
        if let Some(expected) = assertion.retryable_unchanged {
            self.assert_eq(
                step_number,
                "retryable_unchanged",
                &expected.to_string(),
                &record.retryable_unchanged.to_string(),
                &assertion.retry,
            )?;
            if expected
                && (record.original_payload_hash != record.retried_payload_hash
                    || record.original_signed_object_hash != record.retried_signed_object_hash)
            {
                return Err(self.failure(
                    step_number,
                    "retry_same_signed_object",
                    "payload and signed object hashes unchanged",
                    "retry changed payload or signed object hash",
                ));
            }
        }
        if let Some(expected) = assertion.duplicate_count {
            self.assert_eq(
                step_number,
                "retry_duplicate_count",
                &expected.to_string(),
                &record.duplicate_count.to_string(),
                &assertion.retry,
            )?;
        }
        if let Some(expected) = assertion.converged {
            self.assert_eq(
                step_number,
                "retry_converged",
                &expected.to_string(),
                &record.converged.to_string(),
                &assertion.retry,
            )?;
        }
        Ok(())
    }

    fn assert_semantic_correction(
        &self,
        step_number: usize,
        assertion: &fact_sim_dsl::SemanticCorrectionAssertion,
    ) -> Result<()> {
        let record = self
            .semantic_correction_records
            .get(&assertion.correction)
            .with_context(|| {
                format!(
                    "semantic correction record `{}` is not declared",
                    assertion.correction
                )
            })?;
        let previous_revision_id = self
            .revision_id(&assertion.previous_revision)
            .with_context(|| {
                format!(
                    "unknown previous revision `{}`",
                    assertion.previous_revision
                )
            })?;
        let corrective_revision_id = self
            .revision_id(&assertion.corrective_revision)
            .with_context(|| {
                format!(
                    "unknown corrective revision `{}`",
                    assertion.corrective_revision
                )
            })?;
        let effective_revision_id = self
            .revision_id(&assertion.effective_revision)
            .with_context(|| {
                format!(
                    "unknown effective revision `{}`",
                    assertion.effective_revision
                )
            })?;
        if record.previous_revision_id != previous_revision_id {
            return Err(self.failure(
                step_number,
                "semantic_correction_previous_revision",
                previous_revision_id.to_string(),
                record.previous_revision_id.to_string(),
            ));
        }
        if record.corrective_revision_id != corrective_revision_id {
            return Err(self.failure(
                step_number,
                "semantic_correction_corrective_revision",
                corrective_revision_id.to_string(),
                record.corrective_revision_id.to_string(),
            ));
        }
        if record.corrective_revision_id != effective_revision_id || !record.effective_changed {
            return Err(self.failure(
                step_number,
                "semantic_correction_effective_revision",
                effective_revision_id.to_string(),
                record.corrective_revision_id.to_string(),
            ));
        }
        if let Some(expected) = assertion.preserved_history {
            self.assert_eq(
                step_number,
                "semantic_correction_preserved_history",
                &expected.to_string(),
                &record.preserved_history.to_string(),
                &assertion.correction,
            )?;
        }
        if let Some(expected) = assertion.requires_new_signed_object {
            self.assert_eq(
                step_number,
                "semantic_correction_requires_new_signed_object",
                &expected.to_string(),
                &record.requires_new_signed_object.to_string(),
                &assertion.correction,
            )?;
        }
        Ok(())
    }

    fn assert_coordinator_disposition(
        &self,
        step_number: usize,
        assertion: &fact_sim_dsl::CoordinatorDispositionAssertion,
    ) -> Result<()> {
        if assertion.records.is_empty() {
            bail!("coordinator disposition assertion requires at least one record");
        }
        let expected_object = self.object_id_for_symbol(&assertion.object)?;
        let records = assertion
            .records
            .iter()
            .map(|name| {
                self.coordinator_disposition_records
                    .get(name)
                    .with_context(|| format!("coordinator disposition `{name}` is not declared"))
            })
            .collect::<Result<Vec<_>>>()?;
        if let Some(true) = assertion.same_object {
            for record in &records {
                if record.object_id != expected_object {
                    return Err(self.failure(
                        step_number,
                        "coordinator_disposition_same_object",
                        expected_object.to_string(),
                        record.object_id.to_string(),
                    ));
                }
            }
        }
        if !assertion.dispositions.is_empty() {
            let actual = records
                .iter()
                .map(|record| record.disposition.as_str().to_string())
                .collect::<BTreeSet<_>>();
            let expected = assertion
                .dispositions
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            if actual != expected {
                return Err(self.failure(
                    step_number,
                    "coordinator_disposition_set",
                    format!("{expected:?}"),
                    format!("{actual:?}"),
                ));
            }
        }
        for excluded in &assertion.not_dispositions {
            if records
                .iter()
                .any(|record| record.disposition.as_str() == excluded)
            {
                return Err(self.failure(
                    step_number,
                    "coordinator_disposition_excluded",
                    format!("no record has disposition `{excluded}`"),
                    format!("record had disposition `{excluded}`"),
                ));
            }
        }
        if let Some(true) = assertion.different_coordinators {
            let coordinators = records
                .iter()
                .map(|record| record.coordinator_actor_id)
                .collect::<BTreeSet<_>>();
            if coordinators.len() != records.len() {
                return Err(self.failure(
                    step_number,
                    "coordinator_disposition_different_coordinators",
                    "all records signed by different coordinators",
                    format!(
                        "{} unique coordinators for {} records",
                        coordinators.len(),
                        records.len()
                    ),
                ));
            }
        }
        if let Some(expected) = assertion.canonical_unchanged {
            let actual = records.iter().all(|record| record.canonical_unchanged);
            self.assert_eq(
                step_number,
                "coordinator_disposition_canonical_unchanged",
                &expected.to_string(),
                &actual.to_string(),
                &assertion.object,
            )?;
        }
        for record in records {
            if record.statement_payload_hash.is_empty() || record.statement_signature.is_empty() {
                return Err(self.failure(
                    step_number,
                    "coordinator_disposition_signed_statement",
                    "statement hash and signature are present",
                    "missing statement hash or signature",
                ));
            }
        }
        Ok(())
    }

    fn assert_invitation_race(
        &mut self,
        step_number: usize,
        assertion: &fact_sim_dsl::InvitationRaceAssertion,
    ) -> Result<()> {
        let invitation_id = self.invitation_id(Some(&assertion.invitation))?;
        let join_change_id = self.participant_change_id(&assertion.join)?;
        let lifecycle_id = self.invitation_lifecycle_id(&assertion.lifecycle)?;
        let join_dependencies = self.object_dependencies(join_change_id)?;
        let lifecycle_dependencies = self.object_dependencies(lifecycle_id)?;
        let concurrent = !join_dependencies.contains(&lifecycle_id)
            && !lifecycle_dependencies.contains(&join_change_id);
        let lifecycle_operation = self.invitation_lifecycle_operation(lifecycle_id)?;
        if lifecycle_operation != assertion.operation {
            return Err(self.failure(
                step_number,
                "invitation_race_operation",
                assertion.operation.clone(),
                lifecycle_operation,
            ));
        }
        let actual_race = concurrent
            && matches!(
                assertion.operation.as_str(),
                "decline" | "revoke" | "supersede"
            );
        self.assert_eq(
            step_number,
            "invitation_race",
            &assertion.equals.to_string(),
            &actual_race.to_string(),
            &assertion.invitation,
        )?;
        let enrollment_occurs = !actual_race;
        if let Some(expected) = assertion.enrollment_occurs {
            self.assert_eq(
                step_number,
                "invitation_race_enrollment",
                &expected.to_string(),
                &enrollment_occurs.to_string(),
                &assertion.invitation,
            )?;
        }
        let new_invitation_required = actual_race;
        if let Some(expected) = assertion.new_invitation_required {
            self.assert_eq(
                step_number,
                "invitation_race_new_invitation_required",
                &expected.to_string(),
                &new_invitation_required.to_string(),
                &assertion.invitation,
            )?;
        }
        let classification = FailureClassification::RequiresNewSignedObject;
        if let Some(expected) = &assertion.classification {
            self.assert_eq(
                step_number,
                "invitation_race_classification",
                expected,
                failure_classification_as_str(&classification),
                &assertion.invitation,
            )?;
        }
        self.invitation_race_records.insert(
            format!(
                "{}:{}:{}",
                assertion.invitation, assertion.join, assertion.lifecycle
            ),
            ScenarioInvitationRaceRecord {
                scenario: self.scenario_name.clone(),
                invitation_id,
                join_change_id,
                lifecycle_id,
                lifecycle_operation: assertion.operation.clone(),
                conflict_type: "invitation-race".to_string(),
                classification,
                concurrent,
                enrollment_occurs,
                new_invitation_required,
            },
        );
        Ok(())
    }

    fn rebuild_and_compare(&mut self) -> Result<()> {
        let before = match self.last_pre_rebuild_state.clone() {
            Some(state) => state,
            None => self.snapshot_any()?,
        };
        let before_hashes = match self.last_pre_rebuild_hashes.clone() {
            Some(hashes) => hashes,
            None => self.canonical_hashes_by_database()?,
        };
        for entry in self.snapshot_entries() {
            let store = fact_store::Store::open(&entry.database)?;
            rebuild_state(&store)?;
        }
        let after = self.snapshot_any()?;
        let after_hashes = self.canonical_hashes_by_database()?;
        let rebuilt_equivalent = before == after;
        let canonical_history_unchanged = before_hashes == after_hashes;
        self.last_rebuild_equivalent = Some(rebuilt_equivalent);
        self.last_canonical_history_unchanged = Some(canonical_history_unchanged);
        self.last_pre_rebuild_state = Some(before);
        self.last_pre_rebuild_hashes = Some(before_hashes);
        if self.projection_corruption_detected {
            self.repair_report.projection_repairs += 1;
            self.repair_report.repaired_replicas_converged = rebuilt_equivalent;
            self.repair_report.canonical_history_preserved = canonical_history_unchanged;
            self.repair_report.repairs.push(ScenarioRepairRecord {
                scenario: self.scenario_name.clone(),
                repair_type: "projection".to_string(),
                object_id: None,
                detected: true,
                retry_unchanged: false,
                converged: rebuilt_equivalent,
                canonical_history_preserved: canonical_history_unchanged,
            });
            self.projection_corruption_detected = false;
        }
        Ok(())
    }

    fn corrupt_projections(&mut self) -> Result<()> {
        self.last_pre_rebuild_state = Some(self.snapshot_any()?);
        self.last_pre_rebuild_hashes = Some(self.canonical_hashes_by_database()?);
        self.last_rebuild_equivalent = None;
        self.last_canonical_history_unchanged = None;
        self.projection_corruption_detected = true;
        for entry in self.snapshot_entries() {
            let connection = rusqlite::Connection::open(&entry.database)?;
            for table in [
                "projection_object",
                "projection_actor",
                "projection_key",
                "projection_binding",
                "projection_authority",
                "projection_revision",
                "projection_deliberation",
                "projection_standing_change",
                "projection_participant",
                "projection_decision",
                "projection_lifecycle",
                "projection_pending",
                "projection_reconciliation",
                "projection_roster",
                "projection_effective",
                "projection_consensus",
            ] {
                let table = projection_table_name(&connection, table)?;
                connection.execute(&format!("DELETE FROM {table}"), [])?;
            }
        }
        Ok(())
    }

    fn canonical_hashes_by_database(&self) -> Result<BTreeMap<PathBuf, Vec<String>>> {
        let mut hashes = BTreeMap::new();
        for entry in self.snapshot_entries() {
            hashes.insert(
                entry.database.clone(),
                protocol_hashes_for_database(&entry.database)?,
            );
        }
        Ok(hashes)
    }

    fn consensus_for_deliberation(
        &self,
        entry: &LedgerEntry,
        deliberation_id: Uuid,
    ) -> Result<(String, usize)> {
        let connection = rusqlite::Connection::open(&entry.database)?;
        let table = projection_table_name(&connection, "projection_consensus")?;
        connection
            .query_row(
                &format!("SELECT consensus, applicable_decision_count FROM {table} WHERE deliberation_id = ?"),
                [deliberation_id.as_bytes().as_slice()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as usize)),
            )
            .with_context(|| format!("missing consensus projection for deliberation `{deliberation_id}`"))
    }

    fn deliberation_projection(
        &self,
        entry: &LedgerEntry,
        deliberation_id: Uuid,
    ) -> Result<(Uuid, String)> {
        let connection = rusqlite::Connection::open(&entry.database)?;
        let table = projection_table_name(&connection, "projection_consensus")?;
        let (revision, consensus): (Vec<u8>, String) = connection
            .query_row(
                &format!("SELECT revision_id, consensus FROM {table} WHERE deliberation_id = ?"),
                [deliberation_id.as_bytes().as_slice()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .with_context(|| {
                format!("missing consensus projection for deliberation `{deliberation_id}`")
            })?;
        Ok((Uuid::from_slice(&revision)?, consensus))
    }

    fn effective_projection(
        &self,
        entry: &LedgerEntry,
        proposition_id: Uuid,
    ) -> Result<EffectiveProjection> {
        let connection = rusqlite::Connection::open(&entry.database)?;
        let table = projection_table_name(&connection, "projection_effective")?;
        let (status, reason): (String, String) = connection
            .query_row(
                &format!("SELECT status, reason FROM {table} WHERE proposition_id = ?"),
                [proposition_id.as_bytes().as_slice()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .with_context(|| {
                format!("missing effective projection for proposition `{proposition_id}`")
            })?;
        Ok(EffectiveProjection { status, reason })
    }

    #[allow(clippy::type_complexity)]
    fn reconciliation_projection(
        &self,
        entry: &LedgerEntry,
        revision_id: Uuid,
    ) -> Result<ReconciliationProjection> {
        let connection = rusqlite::Connection::open(&entry.database)?;
        let (
            affected,
            common_ancestor,
            resolution_mode,
            selected,
            result,
            payload,
        ): (
            Vec<u8>,
            Vec<u8>,
            String,
            Option<Vec<u8>>,
            Option<Vec<u8>>,
            Vec<u8>,
        ) = connection
            .query_row(
                &format!(
                    "SELECT affected_proposition_id, common_ancestor_revision_id, resolution_mode, selected_revision_id, result_revision_id, payload FROM {} WHERE revision_id = ?",
                    projection_table_name(&connection, "projection_reconciliation")?
                ),
                [revision_id.as_bytes().as_slice()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .with_context(|| {
                format!("missing reconciliation projection for revision `{revision_id}`")
            })?;
        let payload: serde_json::Value = serde_json::from_slice(&payload)?;
        let conflict_revision_ids = payload
            .get("body")
            .and_then(|body| body.get("reconciliation_manifest"))
            .and_then(|manifest| manifest.get("conflicts"))
            .and_then(serde_json::Value::as_array)
            .context("reconciliation projection payload is missing manifest conflicts")?
            .iter()
            .map(|conflict| {
                conflict
                    .get("revision_id")
                    .and_then(serde_json::Value::as_str)
                    .context("reconciliation conflict is missing revision_id")?
                    .parse::<Uuid>()
                    .map_err(Into::into)
            })
            .collect::<Result<BTreeSet<_>>>()?;
        Ok(ReconciliationProjection {
            affected_proposition_id: Uuid::from_slice(&affected)?,
            common_ancestor_revision_id: Uuid::from_slice(&common_ancestor)?,
            resolution_mode,
            selected_revision_id: selected.as_deref().map(Uuid::from_slice).transpose()?,
            result_revision_id: result.as_deref().map(Uuid::from_slice).transpose()?,
            conflict_revision_ids,
        })
    }

    fn assert_decision_participant(
        &self,
        step_number: usize,
        entry: &LedgerEntry,
        decision_id: Uuid,
        participant_id: Uuid,
        deliberation_id: Uuid,
    ) -> Result<()> {
        let connection = rusqlite::Connection::open(&entry.database)?;
        let table = projection_table_name(&connection, "projection_decision")?;
        let actual: Option<(Vec<u8>, Vec<u8>)> = connection
            .query_row(
                &format!("SELECT participant_actor_id, deliberation_id FROM {table} WHERE decision_id = ?"),
                [decision_id.as_bytes().as_slice()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((actual_participant, actual_deliberation)) = actual else {
            return Err(self.failure(
                step_number,
                "decision_tip",
                format!("decision {decision_id} is projected"),
                "decision is missing from projection",
            ));
        };
        if actual_participant.as_slice() != participant_id.as_bytes().as_slice()
            || actual_deliberation.as_slice() != deliberation_id.as_bytes().as_slice()
        {
            return Err(self.failure(
                step_number,
                "decision_tip",
                format!("decision {decision_id} belongs to participant {participant_id}"),
                format!("decision {decision_id} is projected in another scope"),
            ));
        }
        Ok(())
    }

    fn decision_supersedes(&self, decision_id: Uuid) -> Result<BTreeSet<Uuid>> {
        for entry in self.snapshot_entries() {
            let store = fact_store::Store::open(&entry.database)?;
            let Some(payload) = store.get_payload(decision_id.as_bytes())? else {
                continue;
            };
            let value: serde_json::Value = serde_json::from_slice(&payload)?;
            if value["object_type"].as_str() != Some("decision") {
                continue;
            }
            return value["body"]["supersedes_decision_ids"]
                .as_array()
                .context("decision is missing supersedes_decision_ids")?
                .iter()
                .map(|id| {
                    id.as_str()
                        .context("superseded decision id is not a string")?
                        .parse()
                        .map_err(Into::into)
                })
                .collect();
        }
        bail!("decision `{decision_id}` is not present in known replica stores")
    }

    fn object_dependencies(&self, object_id: Uuid) -> Result<BTreeSet<Uuid>> {
        let mut dependencies = BTreeSet::new();
        for entry in self.snapshot_entries() {
            let connection = rusqlite::Connection::open(&entry.database)?;
            let mut statement = connection
                .prepare("SELECT dependency_id FROM object_dependency WHERE object_id = ?")?;
            let rows = statement.query_map([object_id.as_bytes().as_slice()], |row| {
                row.get::<_, Vec<u8>>(0)
            })?;
            for row in rows {
                dependencies.insert(Uuid::from_slice(&row?)?);
            }
        }
        Ok(dependencies)
    }

    fn invitation_lifecycle_operation(&self, lifecycle_id: Uuid) -> Result<String> {
        for entry in self.snapshot_entries() {
            let store = fact_store::Store::open(&entry.database)?;
            let Some(payload) = store.get_payload(lifecycle_id.as_bytes())? else {
                continue;
            };
            let value: serde_json::Value = serde_json::from_slice(&payload)?;
            if value["object_type"].as_str() != Some("invitation_lifecycle") {
                continue;
            }
            return value["body"]["operation"]
                .as_str()
                .map(str::to_string)
                .context("invitation lifecycle is missing operation");
        }
        bail!("invitation lifecycle `{lifecycle_id}` is not present in known replica stores")
    }

    fn run_cli_checks(
        &mut self,
        fact_binary_env: &str,
        expected_pending_actions: usize,
    ) -> Result<()> {
        let fact_binary = env::var_os(fact_binary_env)
            .map(PathBuf::from)
            .unwrap_or_else(default_fact_binary_path);
        if !fact_binary.exists() {
            bail!(
                "fact binary `{}` does not exist; set {fact_binary_env}",
                fact_binary.display()
            );
        }
        let ledger = self.single_ledger()?.clone();
        let active_entry = self.prepare_cli_active_entry(&ledger)?;
        let proposition = self.single_proposition()?.clone();
        let reference = proposition.proposition_id.to_string();
        let sdk_state = self.state_for_symbol(&proposition)?;
        let expected_effective_revision = sdk_state
            .effective_revision_id
            .as_ref()
            .map(|id| id.to_string());
        let effective_content = read_proposition_content(&active_entry, &reference)?.content;
        let effective_text = String::from_utf8_lossy(&effective_content).to_string();

        let list_human = self.run_fact_command(&fact_binary, &["list"])?;
        if !list_human.stdout.contains(&sdk_state.summary) {
            bail!(
                "fact list did not contain SDK summary `{}`",
                sdk_state.summary
            );
        }
        let list_json = self.run_fact_command(&fact_binary, &["--json", "list"])?;
        let list_items = list_json
            .parsed_json
            .as_ref()
            .and_then(|value| value.as_array())
            .context("fact list --json did not return an array")?;
        let list_item = list_items
            .iter()
            .find(|item| item["proposition_id"].as_str() == Some(reference.as_str()))
            .context("fact list --json did not include generated proposition")?;
        if list_item["summary"].as_str() != Some(sdk_state.summary.as_str())
            || list_item["status"].as_str() != Some(sdk_state.status.as_str())
            || list_item["revision_id"].as_str() != expected_effective_revision.as_deref()
        {
            bail!("fact list JSON disagrees with SDK state");
        }

        let revisions_human = self.run_fact_command(&fact_binary, &["revisions", &reference])?;
        let expected_revisions = list_revisions(&active_entry, &reference)?;
        for revision in &expected_revisions {
            let revision_reference = revision["reference"]
                .as_str()
                .context("SDK revision listing omitted reference")?;
            let revision_status = revision["status"]
                .as_str()
                .context("SDK revision listing omitted status")?;
            if !revisions_human.stdout.contains(revision_reference)
                || !revisions_human.stdout.contains(revision_status)
            {
                bail!(
                    "fact revisions human output omitted revision {} status {}",
                    revision_reference,
                    revision_status
                );
            }
        }
        let revisions_json =
            self.run_fact_command(&fact_binary, &["--json", "revisions", &reference])?;
        let revisions = revisions_json
            .parsed_json
            .as_ref()
            .and_then(|value| value.as_array())
            .context("fact revisions --json did not return an array")?;
        if revisions.len() != expected_revisions.len() {
            bail!(
                "fact revisions --json returned {} revisions, expected {}",
                revisions.len(),
                expected_revisions.len()
            );
        }
        for revision in expected_revisions {
            let revision_id = revision["object_id"]
                .as_str()
                .context("SDK revision listing omitted object_id")?;
            let item = revisions
                .iter()
                .find(|item| item["object_id"].as_str() == Some(revision_id))
                .with_context(|| format!("fact revisions --json omitted {revision_id}"))?;
            if item["status"] != revision["status"] || item["effective"] != revision["effective"] {
                bail!("fact revisions --json disagrees with SDK revision state");
            }
        }

        let echo = self.run_fact_command(&fact_binary, &["echo", &reference])?;
        if echo.stdout != effective_text {
            bail!("fact echo disagrees with SDK effective content");
        }

        let search_human =
            self.run_fact_command(&fact_binary, &["search", "--effective", "Deployment"])?;
        if !search_human.stdout.contains(&reference[..12])
            && !search_human.stdout.contains(&sdk_state.summary)
        {
            bail!("fact search did not include the sampled effective proposition");
        }
        let search_json = self.run_fact_command(
            &fact_binary,
            &["--json", "search", "--effective", "Deployment"],
        )?;
        let search_items = search_json
            .parsed_json
            .as_ref()
            .and_then(|value| value.as_array())
            .context("fact search --json did not return an array")?;
        if search_items.is_empty() {
            bail!("fact search --json returned no sampled effective results");
        }

        let history_human = self.run_fact_command(&fact_binary, &["history", &reference])?;
        if !history_human.stdout.contains(&reference[..12]) {
            bail!("fact history did not include the sampled proposition reference");
        }
        let history_json =
            self.run_fact_command(&fact_binary, &["--json", "history", &reference])?;
        let history_items = history_json
            .parsed_json
            .as_ref()
            .and_then(|value| value.as_array())
            .context("fact history --json did not return an array")?;
        if history_items.is_empty() {
            bail!("fact history --json returned no sampled history items");
        }

        self.run_isolated_cli_write_sync_samples(&fact_binary)?;

        let pending_human = self.run_fact_command(&fact_binary, &["pending"])?;
        if expected_pending_actions == 0 {
            if !pending_human.stdout.contains("no pending actions") {
                bail!("fact pending human output disagrees with expected empty pending set");
            }
        } else if !pending_human.stdout.contains(&sdk_state.summary) {
            bail!(
                "fact pending human output did not include SDK summary `{}`",
                sdk_state.summary
            );
        }

        let pending_json = self.run_fact_command(&fact_binary, &["--json", "pending"])?;
        let pending = pending_json
            .parsed_json
            .as_ref()
            .and_then(|value| value.as_array())
            .context("fact pending --json did not return an array")?;
        if pending.len() != expected_pending_actions {
            bail!(
                "fact pending returned {} actions, expected {expected_pending_actions}",
                pending.len()
            );
        }
        if expected_pending_actions > 0
            && !pending
                .iter()
                .any(|item| item["proposition_id"].as_str() == Some(reference.as_str()))
        {
            bail!("fact pending --json did not include generated proposition");
        }
        let status_json = self.run_fact_command(&fact_binary, &["--json", "status"])?;
        if status_json
            .parsed_json
            .as_ref()
            .and_then(|value| value["pending_actions"].as_u64())
            != Some(expected_pending_actions as u64)
        {
            bail!("fact status --json disagrees about pending action count");
        }
        Ok(())
    }

    fn prepare_cli_active_entry(&mut self, ledger: &SdkLedger) -> Result<LedgerEntry> {
        if self.characters.len() != 1 {
            return Ok(ledger.entry.clone());
        }
        let actor = self
            .characters
            .keys()
            .next()
            .expect("one character exists")
            .clone();
        let ledger_name = self.single_ledger_name()?;
        let mut entry = self.character_entry(&actor, &ledger_name, None)?.entry;
        entry.name = ledger.entry.name.clone();
        let mut catalog = self.environment.load()?;
        catalog.insert(ledger.entry.name.clone(), entry.clone());
        self.environment.save(&catalog)?;
        self.environment.set_active(&ledger.entry.name)?;
        Ok(entry)
    }

    fn prepare_cli_push_target_database(&self, source_database: &Path) -> Result<PathBuf> {
        let target = self.workspace.join("cli-sample-push-target.sqlite");
        fs::copy(source_database, &target).with_context(|| {
            format!(
                "copy {} to {} for sampled CLI push",
                source_database.display(),
                target.display()
            )
        })?;
        let connection = rusqlite::Connection::open(&target)?;
        connection.execute("PRAGMA foreign_keys = OFF", [])?;
        let (object_id, content_hash) = connection
            .query_row(
                "SELECT object_id, content_hash FROM protocol_object WHERE object_type='revision' ORDER BY content_hash LIMIT 1",
                [],
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .context("select sampled CLI revision object for push target")?;
        delete_blob_match_if_table_exists(
            &connection,
            "object_dependency",
            "object_id",
            &object_id,
        )?;
        delete_blob_match_if_table_exists(
            &connection,
            "protocol_revision",
            "object_id",
            &object_id,
        )?;
        let projected_object = projection_table_name(&connection, "projection_object")?;
        let projected_revision = projection_table_name(&connection, "projection_revision")?;
        delete_blob_match_if_table_exists(&connection, &projected_object, "object_id", &object_id)?;
        delete_blob_match_if_table_exists(
            &connection,
            &projected_revision,
            "object_id",
            &object_id,
        )?;
        delete_blob_match_if_table_exists(
            &connection,
            "protocol_object",
            "content_hash",
            &content_hash,
        )?;
        Ok(target)
    }

    fn run_isolated_cli_write_sync_samples(&mut self, fact_binary: &Path) -> Result<()> {
        let fact_home = self.workspace.join(format!(
            "cli-workflow-fact-home-{}",
            self.cli_receipts.len()
        ));
        fs::create_dir_all(&fact_home)?;
        self.run_fact_command_in_home(fact_binary, &["init"], &fact_home)?;
        let cli_proposed = self.run_fact_command_in_home(
            fact_binary,
            &[
                "--json",
                "propose",
                "--message",
                "# CLI sampled proposition\n\nCreated by the benchmark CLI sample.",
                "--decision",
                "accept",
            ],
            &fact_home,
        )?;
        let cli_proposed_json = cli_proposed
            .parsed_json
            .as_ref()
            .context("fact propose --json did not return JSON")?;
        let cli_proposition = cli_proposed_json["proposition_id"]
            .as_str()
            .context("fact propose --json omitted proposition_id")?
            .to_string();
        let cli_revised = self.run_fact_command_in_home(
            fact_binary,
            &[
                "--json",
                "revise",
                &cli_proposition,
                "--message",
                "# CLI sampled proposition\n\nRevised by the benchmark CLI sample.",
            ],
            &fact_home,
        )?;
        let cli_revised_json = cli_revised
            .parsed_json
            .as_ref()
            .context("fact revise --json did not return JSON")?;
        let cli_revision = cli_revised_json["revision_id"]
            .as_str()
            .context("fact revise --json omitted revision_id")?
            .to_string();
        let cli_accepted = self.run_fact_command_in_home(
            fact_binary,
            &["--json", "accept", &cli_proposition],
            &fact_home,
        )?;
        if cli_accepted
            .parsed_json
            .as_ref()
            .and_then(|value| value["revision_id"].as_str())
            != Some(cli_revision.as_str())
        {
            bail!("fact accept --json did not accept the sampled CLI revision");
        }

        let cli_status =
            self.run_fact_command_in_home(fact_binary, &["--json", "status"], &fact_home)?;
        let cli_ledger = cli_status
            .parsed_json
            .as_ref()
            .and_then(|value| value["ledger_id"].as_str())
            .context("fact status --json omitted ledger_id")?
            .to_string();
        let cli_database = fact_home.join("ledgers").join("default.sqlite");
        let cli_database_file = cli_database.to_string_lossy().into_owned();
        let cli_push_target = self.prepare_cli_push_target_database(&cli_database)?;
        let cli_known_hashes = self.workspace.join("cli-sample-known-hashes.txt");
        fs::write(
            &cli_known_hashes,
            protocol_hashes_for_database(&cli_push_target)?.join("\n"),
        )?;
        let cli_known_hashes_file = cli_known_hashes.to_string_lossy().into_owned();
        let cli_bundle = self.workspace.join("cli-sample-pull.factbndl");
        let cli_bundle_file = cli_bundle.to_string_lossy().into_owned();
        let cli_pull = self.run_fact_command_in_home(
            fact_binary,
            &[
                "--json",
                "pull",
                &cli_database_file,
                &cli_ledger,
                &cli_bundle_file,
                "--known-hashes",
                &cli_known_hashes_file,
            ],
            &fact_home,
        )?;
        if cli_pull
            .parsed_json
            .as_ref()
            .and_then(|value| value["pulled"].as_u64())
            .unwrap_or_default()
            == 0
            || !cli_bundle.exists()
        {
            bail!("fact pull did not create a non-empty sampled CLI bundle");
        }
        let cli_push_target_file = cli_push_target.to_string_lossy().into_owned();
        let cli_push = self.run_fact_command_in_home(
            fact_binary,
            &["--json", "push", &cli_push_target_file, &cli_bundle_file],
            &fact_home,
        )?;
        if cli_push
            .parsed_json
            .as_ref()
            .and_then(|value| value["pushed"].as_u64())
            .unwrap_or_default()
            == 0
        {
            bail!("fact push did not import sampled CLI bundle objects");
        }
        Ok(())
    }

    fn run_fact_command(&mut self, fact_binary: &Path, args: &[&str]) -> Result<CliReceipt> {
        let fact_home = self.fact_home();
        self.run_fact_command_in_home(fact_binary, args, &fact_home)
    }

    fn run_fact_command_in_home(
        &mut self,
        fact_binary: &Path,
        args: &[&str],
        fact_home: &Path,
    ) -> Result<CliReceipt> {
        let started = Instant::now();
        let output = Command::new(fact_binary)
            .args(args)
            .env("FACT_HOME", fact_home)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .with_context(|| format!("failed to run fact {}", args.join(" ")))?;
        let receipt = CliReceipt {
            command: args.iter().map(|arg| (*arg).to_string()).collect(),
            status: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            parsed_json: serde_json::from_slice(&output.stdout).ok(),
            duration_ms: started.elapsed().as_millis(),
        };
        if !output.status.success() {
            bail!(
                "fact {} exited with {:?}: {}",
                args.join(" "),
                receipt.status,
                receipt.stderr
            );
        }
        self.cli_receipts.push(receipt.clone());
        Ok(receipt)
    }

    fn snapshot_any(&self) -> Result<SdkStateSnapshot> {
        let mut propositions = BTreeMap::<Uuid, PropositionState>::new();
        let mut revisions = BTreeMap::<Uuid, RevisionState>::new();
        let mut pending = BTreeMap::<Uuid, PendingState>::new();
        let mut object_counts_by_type = BTreeMap::<String, usize>::new();
        let mut seen_objects = std::collections::BTreeSet::<Uuid>::new();
        for entry in self.snapshot_entries() {
            let snapshot = snapshot_for_entry(&entry)?;
            for proposition in snapshot.propositions {
                propositions.insert(proposition.proposition_id, proposition);
            }
            for revision in snapshot.revisions {
                revisions.insert(revision.revision_id, revision);
            }
            for item in snapshot.pending {
                pending.insert(item.proposition_id, item);
            }
            let ledger_id = Uuid::parse_str(&entry.ledger_id)?;
            let store = fact_store::Store::open(&entry.database)?;
            for (id, _, object_type) in store.list_objects(ledger_id.as_bytes())? {
                if seen_objects.insert(id) {
                    *object_counts_by_type.entry(object_type).or_default() += 1;
                }
            }
        }
        Ok(SdkStateSnapshot {
            propositions: propositions.into_values().collect(),
            revisions: revisions.into_values().collect(),
            pending: pending.into_values().collect(),
            object_counts_by_type,
        })
    }

    fn snapshot(&self, ledger: &SdkLedger) -> Result<SdkStateSnapshot> {
        snapshot_for_entry(&ledger.entry)
    }

    fn snapshot_entries(&self) -> Vec<LedgerEntry> {
        let mut entries = BTreeMap::<PathBuf, LedgerEntry>::new();
        for ledger in self.ledgers.values() {
            entries.insert(ledger.entry.database.clone(), ledger.entry.clone());
        }
        for character in self.characters.values() {
            for environment in character.environments.values() {
                for entry in environment.ledger_entries.values() {
                    entries.insert(entry.database.clone(), entry.clone());
                }
            }
        }
        entries.into_values().collect()
    }

    fn state_for(&self, proposition: &str) -> Result<PropositionState> {
        let proposition = self.proposition(proposition)?;
        self.state_for_symbol(proposition)
    }

    fn state_for_symbol(&self, proposition: &SdkProposition) -> Result<PropositionState> {
        self.snapshot_for_proposition(proposition)?
            .propositions
            .into_iter()
            .find(|state| state.proposition_id == proposition.proposition_id)
            .context("proposition state not found")
    }

    fn snapshot_for_proposition(&self, proposition: &SdkProposition) -> Result<SdkStateSnapshot> {
        if let Some(entry) =
            self.environment_ledger_entry(&proposition.environment, &proposition.ledger)?
        {
            snapshot_for_entry(&entry)
        } else {
            self.snapshot(self.ledger(&proposition.ledger)?)
        }
    }

    fn revision_state(&self, revision_id: Uuid) -> Result<RevisionState> {
        self.snapshot_any()?
            .revisions
            .into_iter()
            .find(|state| state.revision_id == revision_id)
            .context("revision state not found")
    }

    fn revision_parent(&self, revision_id: Uuid) -> Result<Option<Uuid>> {
        for entry in self.snapshot_entries() {
            let store = fact_store::Store::open(&entry.database)?;
            let Some(payload) = store.get_payload(revision_id.as_bytes())? else {
                continue;
            };
            let value: serde_json::Value = serde_json::from_slice(&payload)?;
            if value["object_type"].as_str() != Some("revision") {
                continue;
            }
            return value["body"]["parent_revision_id"]
                .as_str()
                .map(Uuid::parse_str)
                .transpose()
                .map_err(Into::into);
        }
        bail!("revision `{revision_id}` is not present in known replica stores")
    }

    fn assert_eq(
        &self,
        step_number: usize,
        operation: &str,
        expected: &str,
        actual: &str,
        symbol: &str,
    ) -> Result<()> {
        if expected == actual {
            Ok(())
        } else {
            Err(self.failure(
                step_number,
                operation,
                format!("{symbol}: {expected}"),
                format!("{symbol}: {actual}"),
            ))
        }
    }

    fn failure(
        &self,
        step_number: usize,
        operation: &str,
        expected_result: impl Into<String>,
        actual_result: impl Into<String>,
    ) -> anyhow::Error {
        anyhow::anyhow!(
            "{}",
            serde_json::to_string_pretty(&FailureReport {
                scenario_name: self.scenario_name.clone(),
                seed: self.seed,
                step_number,
                operation: operation.to_string(),
                expected_result: expected_result.into(),
                actual_result: actual_result.into(),
                symbolic_references: self.symbolic_references(),
                protocol_ids: self.protocol_ids(),
            })
            .unwrap_or_else(|_| "failed to render failure report".to_string())
        )
    }

    fn symbolic_references(&self) -> BTreeMap<String, String> {
        let mut references = BTreeMap::new();
        for name in self.characters.keys() {
            references.insert(name.clone(), "character".to_string());
        }
        for name in self.ledgers.keys() {
            references.insert(name.clone(), "ledger".to_string());
        }
        for (name, proposition) in &self.propositions {
            references.insert(name.clone(), proposition.proposition_id.to_string());
        }
        for (name, id) in &self.revisions {
            references.insert(name.clone(), id.to_string());
        }
        for (name, id) in &self.deliberations {
            references.insert(name.clone(), id.to_string());
        }
        for (name, id) in &self.decisions {
            references.insert(name.clone(), id.to_string());
        }
        for (name, id) in &self.settlements {
            references.insert(name.clone(), id.to_string());
        }
        for (name, id) in &self.invitations {
            references.insert(name.clone(), id.to_string());
        }
        for (name, id) in &self.participant_changes {
            references.insert(name.clone(), id.to_string());
        }
        for (name, id) in &self.invitation_lifecycles {
            references.insert(name.clone(), id.to_string());
        }
        references
    }

    fn protocol_ids(&self) -> BTreeMap<String, String> {
        let mut ids = BTreeMap::new();
        for (name, character) in &self.characters {
            ids.insert(format!("actor.{name}"), character.actor_id.to_string());
            ids.insert(
                format!("key.{name}.current"),
                character.current_key_id.to_string(),
            );
        }
        for (name, ledger) in &self.ledgers {
            ids.insert(format!("ledger.{name}"), ledger.entry.ledger_id.clone());
        }
        for (name, proposition) in &self.propositions {
            ids.insert(
                format!("proposition.{name}"),
                proposition.proposition_id.to_string(),
            );
        }
        for (name, id) in &self.revisions {
            ids.insert(format!("revision.{name}"), id.to_string());
        }
        for (name, id) in &self.deliberations {
            ids.insert(format!("deliberation.{name}"), id.to_string());
        }
        for (name, id) in &self.decisions {
            ids.insert(format!("decision.{name}"), id.to_string());
        }
        for (name, id) in &self.settlements {
            ids.insert(format!("settlement.{name}"), id.to_string());
        }
        for (name, id) in &self.invitations {
            ids.insert(format!("invitation.{name}"), id.to_string());
        }
        for (name, id) in &self.participant_changes {
            ids.insert(format!("participant_change.{name}"), id.to_string());
        }
        for (name, id) in &self.invitation_lifecycles {
            ids.insert(format!("invitation_lifecycle.{name}"), id.to_string());
        }
        ids
    }

    fn character_states(&self) -> Vec<CharacterRunState> {
        self.characters
            .values()
            .map(|character| {
                let mut ledger_capabilities = BTreeMap::new();
                for environment in character.environments.values() {
                    for (ledger, capabilities) in &environment.capabilities {
                        ledger_capabilities
                            .entry(ledger.clone())
                            .or_insert_with(|| capabilities.clone());
                    }
                }
                CharacterRunState {
                    name: character.name.clone(),
                    actor_id: character.actor_id,
                    current_key_id: character.current_key_id,
                    historical_key_ids: character
                        .historical_keys
                        .iter()
                        .map(|key| key.key_id)
                        .collect(),
                    environments: character
                        .environments
                        .values()
                        .map(|environment| environment.name.clone())
                        .collect(),
                    environment_databases: character
                        .environments
                        .iter()
                        .map(|(name, environment)| {
                            (
                                name.clone(),
                                environment
                                    .ledger_entries
                                    .iter()
                                    .map(|(ledger, entry)| {
                                        (ledger.clone(), entry.database.display().to_string())
                                    })
                                    .collect(),
                            )
                        })
                        .collect(),
                    ledger_capabilities,
                }
            })
            .collect()
    }

    fn manifest(&self, scenario_name: &str, state: &SdkStateSnapshot) -> Result<RunManifest> {
        let ledger_ids = self
            .ledgers
            .values()
            .map(|ledger| Uuid::parse_str(&ledger.entry.ledger_id))
            .collect::<Result<Vec<_>, _>>()?;
        let object_counts_by_type = ObjectCounts {
            actors: state
                .object_counts_by_type
                .get("actor")
                .copied()
                .unwrap_or(0),
            ledgers: state
                .object_counts_by_type
                .get("genesis")
                .copied()
                .unwrap_or(0),
            propositions: state
                .object_counts_by_type
                .get("proposition")
                .copied()
                .unwrap_or(0),
            revisions: state
                .object_counts_by_type
                .get("revision")
                .copied()
                .unwrap_or(0),
            decisions: state
                .object_counts_by_type
                .get("decision")
                .copied()
                .unwrap_or(0),
            comments: state
                .object_counts_by_type
                .get("deliberation_comment")
                .copied()
                .unwrap_or(0),
        };
        Ok(RunManifest {
            generator_version: env!("CARGO_PKG_VERSION").to_string(),
            facts_sdk_version: option_env!("FACTS_SDK_VERSION")
                .unwrap_or("path-dependency")
                .to_string(),
            scheduler_version: SCHEDULER_VERSION.to_string(),
            seed: self.seed,
            started_at: self.clock.now(),
            scenario_count: 1,
            object_count: state.object_counts_by_type.values().sum(),
            object_counts_by_type,
            ledger_ids,
            character_ids: self
                .characters
                .iter()
                .map(|(name, character)| (name.clone(), character.actor_id))
                .collect(),
            character_key_ids: self
                .characters
                .iter()
                .map(|(name, character)| (name.clone(), character.current_key_id))
                .collect(),
            character_environments: self
                .characters
                .iter()
                .map(|(name, character)| {
                    (
                        name.clone(),
                        character.environments.keys().cloned().collect::<Vec<_>>(),
                    )
                })
                .collect(),
            output_database: if self.ledgers.len() == 1 {
                Some(self.single_ledger()?.entry.database.display().to_string())
            } else {
                None
            },
            project_git_commit: option_env!("FACT_SIM_GIT_COMMIT").map(str::to_string),
            facts_git_commit: option_env!("FACTS_GIT_COMMIT").map(str::to_string),
            rust_version: option_env!("RUSTC_VERSION").map(str::to_string),
            scenario_corpus_version: scenario_name.to_string(),
            configuration_digest: Some(format!("seed:{}:start:{}", self.seed, self.clock.now())),
            final_commitment_root: None,
            conflict_repair_report: self.conflict_repair_manifest_report()?,
        })
    }

    fn conflict_repair_manifest_report(&self) -> Result<ConflictRepairManifestReport> {
        Ok(ConflictRepairManifestReport {
            conflict_counts_by_class: self.conflict_counts_by_class.clone(),
            reconciliation_counts_by_mode: self.reconciliation_counts_by_mode()?,
            retry_counts_by_disposition: self.retry_counts_by_disposition(),
            repair_counts: self.repair_counts(),
            fixture_locations: self.fixture_locations(),
            assertion_results: self.assertion_results.clone(),
        })
    }

    fn retry_counts_by_disposition(&self) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for record in self.retry_records.values() {
            *counts
                .entry(record.first_disposition.as_str().to_string())
                .or_default() += 1;
            *counts
                .entry(record.retry_disposition.as_str().to_string())
                .or_default() += 1;
        }
        counts
    }

    fn reconciliation_counts_by_mode(&self) -> Result<BTreeMap<String, usize>> {
        let mut seen = BTreeSet::<(Vec<u8>, String)>::new();
        for entry in self.snapshot_entries() {
            let connection = rusqlite::Connection::open(&entry.database)?;
            let table = projection_table_name(&connection, "projection_reconciliation")?;
            let mut statement =
                connection.prepare(&format!("SELECT revision_id, resolution_mode FROM {table}"))?;
            let rows = statement.query_map([], |row| {
                Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, String>(1)?))
            })?;
            for row in rows {
                seen.insert(row?);
            }
        }
        let mut counts = BTreeMap::new();
        for (_, mode) in seen {
            *counts.entry(mode).or_default() += 1;
        }
        Ok(counts)
    }

    fn repair_counts(&self) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        if self.repair_report.projection_repairs > 0 {
            counts.insert(
                "projection".to_string(),
                self.repair_report.projection_repairs,
            );
        }
        if self.repair_report.partial_sync_repairs > 0 {
            counts.insert(
                "partial-sync".to_string(),
                self.repair_report.partial_sync_repairs,
            );
        }
        if self.repair_report.semantic_corrections > 0 {
            counts.insert(
                "semantic-correction".to_string(),
                self.repair_report.semantic_corrections,
            );
        }
        counts
    }

    fn fixture_locations(&self) -> BTreeMap<String, String> {
        let mut locations = BTreeMap::new();
        locations.insert(
            "workspace".to_string(),
            self.workspace.display().to_string(),
        );
        for entry in self.snapshot_entries() {
            locations.insert(
                format!("database:{}", entry.name),
                entry.database.display().to_string(),
            );
        }
        locations
    }

    fn next_seed(&mut self) -> [u8; 32] {
        let mut seed = [0_u8; 32];
        for chunk in seed.chunks_mut(8) {
            chunk.copy_from_slice(&self.random.next_u64().to_le_bytes());
        }
        seed
    }

    fn next_nonce(&mut self) -> [u8; 16] {
        let mut nonce = [0_u8; 16];
        for chunk in nonce.chunks_mut(8) {
            chunk.copy_from_slice(&self.random.next_u64().to_le_bytes());
        }
        nonce
    }

    fn sync_runtime_clock(&self) -> Result<()> {
        self.runtime.set_time(self.clock.now())?;
        Ok(())
    }

    fn require_character(&self, name: &str) -> Result<()> {
        if self.characters.contains_key(name) {
            Ok(())
        } else {
            bail!("character `{name}` is not declared")
        }
    }

    fn grant(
        &mut self,
        ledger: &str,
        actor: &str,
        capabilities: &[String],
        propagate: bool,
    ) -> Result<()> {
        if capabilities.is_empty() {
            bail!("grant for `{actor}` on `{ledger}` requires at least one capability");
        }
        let ledger_state = self.ledger(ledger)?.clone();
        let (actor_id, identity_bundle) = {
            let character = self.character(actor)?;
            (character.actor_id, character.identity_bundle.clone())
        };
        import_identity(&ledger_state.entry, &identity_bundle)
            .with_context(|| format!("import identity for `{actor}` into ledger `{ledger}`"))?;
        self.sync_runtime_clock()?;
        create_identity_grant_with_runtime(
            &ledger_state.entry,
            &ledger_state.seed,
            &actor_id.to_string(),
            capabilities,
            &self.runtime,
        )
        .with_context(|| {
            format!(
                "grant capabilities [{}] to character `{actor}` on ledger `{ledger}`",
                capabilities.join(",")
            )
        })?;
        let character = self.character_mut(actor)?;
        for environment in character.environments.values_mut() {
            environment
                .capabilities
                .insert(ledger.to_string(), capabilities.to_vec());
        }
        if propagate {
            self.propagate_primary_to_existing_replicas(ledger)?;
        }
        Ok(())
    }

    fn init_replica(&mut self, actor: &str, replica: &str, ledger: &str) -> Result<()> {
        self.character_entry(actor, ledger, Some(replica))
            .with_context(|| format!("initialize replica `{replica}` for `{actor}`"))?;
        Ok(())
    }

    fn character_entry(
        &mut self,
        actor: &str,
        ledger: &str,
        replica: Option<&str>,
    ) -> Result<CharacterLedgerEntry> {
        self.import_character_identity(ledger, actor, replica)?;
        let environment_name = self.character_environment_name(actor, replica)?;
        let character = self.character(actor)?;
        let environment = character
            .environments
            .get(&environment_name)
            .with_context(|| {
                format!("character `{actor}` has no environment `{environment_name}`")
            })?;
        let entry = environment
            .ledger_entries
            .get(ledger)
            .with_context(|| {
                format!(
                    "character `{actor}` has no ledger context for `{ledger}` in `{environment_name}`"
                )
            })?
            .clone();
        Ok(CharacterLedgerEntry {
            entry,
            seed: character.current_seed,
        })
    }

    fn sync_environments(&mut self, from: &str, to: &str, ledger: Option<&str>) -> Result<usize> {
        let ledger_name = ledger
            .map(str::to_string)
            .map(Ok)
            .unwrap_or_else(|| self.single_ledger_name())?;
        let source = self
            .environment_ledger_entry(from, &ledger_name)?
            .with_context(|| {
                format!("source environment `{from}` has no replica for ledger `{ledger_name}`")
            })?;
        let target = self.ensure_environment_ledger_entry(to, &ledger_name)?;
        self.import_missing_objects(&source, &target)
            .with_context(|| format!("sync `{from}` to `{to}` for ledger `{ledger_name}`"))
    }

    fn retry_missing_dependency(
        &mut self,
        from: &str,
        to: &str,
        ledger: Option<&str>,
        object: &str,
        missing_dependency_kind: Option<&str>,
        symbol: Option<&str>,
    ) -> Result<Uuid> {
        let ledger_name = ledger
            .map(str::to_string)
            .map(Ok)
            .unwrap_or_else(|| self.single_ledger_name())?;
        let source = self
            .environment_ledger_entry(from, &ledger_name)?
            .with_context(|| {
                format!("source environment `{from}` has no replica for ledger `{ledger_name}`")
            })?;
        let target = self.ensure_environment_ledger_entry(to, &ledger_name)?;
        let object_id = self.object_id_for_symbol(object)?;
        let source_store = fact_store::Store::open(&source.database)?;
        let target_store = fact_store::Store::open(&target.database)?;
        let ledger_id = Uuid::parse_str(&source.ledger_id)?;
        let exported = export_object(&source_store, ledger_id, object_id)?;
        let original_payload_hash = payload_hash(&exported.bytes)?;
        let original_signed_object_hash = signed_object_hash(&exported.bytes);

        match import_bundle(&target_store, std::slice::from_ref(&exported.bytes)) {
            Err(error)
                if error.to_string().contains("missing")
                    || error.to_string().contains("dependency") => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("partial retry import for `{object}` into `{to}`"));
            }
            Ok(_) => {
                bail!("object `{object}` imported without its dependency closure");
            }
        }

        self.import_missing_objects(&source, &target)
            .with_context(|| format!("deliver dependency closure for `{object}`"))?;
        let retried = export_object(&target_store, ledger_id, object_id)?;
        let retried_payload_hash = payload_hash(&retried.bytes)?;
        let retried_signed_object_hash = signed_object_hash(&retried.bytes);
        let retryable_unchanged = original_payload_hash == retried_payload_hash
            && original_signed_object_hash == retried_signed_object_hash;
        let duplicate_count = protocol_object_id_count(&target.database, object_id)?;
        let converged = protocol_hashes_for_database(&source.database)?
            == protocol_hashes_for_database(&target.database)?;
        let record_name = symbol.unwrap_or(object).to_string();
        self.retry_records.insert(
            record_name.clone(),
            ScenarioRetryRecord {
                scenario: self.scenario_name.clone(),
                name: record_name,
                object_id,
                first_disposition: CoordinatorDisposition::RejectedMissingDependency,
                retry_disposition: CoordinatorDisposition::Accepted,
                classification: FailureClassification::RetryableUnchanged,
                missing_dependency_kind: missing_dependency_kind
                    .unwrap_or("dependency-closure")
                    .to_string(),
                retryable_unchanged,
                original_payload_hash,
                retried_payload_hash,
                original_signed_object_hash,
                retried_signed_object_hash,
                duplicate_count,
                converged,
            },
        );
        self.repair_report.partial_sync_repairs += 1;
        self.repair_report.repaired_replicas_converged = converged;
        self.repair_report.canonical_history_preserved = true;
        self.repair_report.repairs.push(ScenarioRepairRecord {
            scenario: self.scenario_name.clone(),
            repair_type: "partial-sync".to_string(),
            object_id: Some(object_id),
            detected: true,
            retry_unchanged: retryable_unchanged,
            converged,
            canonical_history_preserved: true,
        });
        for proposition in self.propositions.values_mut() {
            if self
                .revisions
                .get(&proposition.latest_revision_symbol)
                .copied()
                == Some(object_id)
            {
                proposition.environment = to.to_string();
            }
        }
        Ok(object_id)
    }

    fn record_disposition(
        &mut self,
        coordinator: &str,
        object: &str,
        disposition: &str,
        classification: Option<&str>,
        reason: Option<&str>,
        symbol: &str,
    ) -> Result<Uuid> {
        let object_id = self.object_id_for_symbol(object)?;
        let disposition = coordinator_disposition_from_str(disposition)?;
        let classification = classification
            .map(failure_classification_from_str)
            .transpose()?;
        let (coordinator_actor_id, seed) = {
            let character = self.character(coordinator)?;
            (character.actor_id, character.current_seed)
        };
        let before = self.canonical_hashes_by_database()?;
        let statement = serde_json::json!({
            "scenario": self.scenario_name,
            "coordinator": coordinator,
            "coordinator_actor_id": coordinator_actor_id,
            "object_id": object_id,
            "disposition": disposition.as_str(),
            "classification": classification.as_ref().map(failure_classification_as_str),
            "reason": reason,
            "observed_at": sdk_timestamp(self.clock.now()),
        });
        let statement_bytes = serde_json::to_vec(&statement)?;
        let key = fact_crypto::SigningKey::from_seed(&seed)?;
        let signature = key.sign(&statement_bytes);
        let after = self.canonical_hashes_by_database()?;
        self.coordinator_disposition_records.insert(
            symbol.to_string(),
            ScenarioCoordinatorDispositionRecord {
                scenario: self.scenario_name.clone(),
                name: symbol.to_string(),
                coordinator: coordinator.to_string(),
                coordinator_actor_id,
                object_id,
                disposition,
                classification,
                reason: reason.map(str::to_string),
                statement_payload_hash: fact_core::Hash::digest(&statement_bytes).hex(),
                statement_signature: hex_bytes(&signature),
                canonical_unchanged: before == after,
            },
        );
        Ok(object_id)
    }

    fn ensure_environment_ledger_entry(
        &mut self,
        environment: &str,
        ledger: &str,
    ) -> Result<LedgerEntry> {
        if let Some(entry) = self.environment_ledger_entry(environment, ledger)? {
            return Ok(entry);
        }
        let actor = self.environment_owner(environment)?;
        self.import_character_identity(ledger, &actor, Some(environment))?;
        self.environment_ledger_entry(environment, ledger)?
            .with_context(|| format!("environment `{environment}` replica was not created"))
    }

    fn environment_ledger_entry(
        &self,
        environment: &str,
        ledger: &str,
    ) -> Result<Option<LedgerEntry>> {
        for character in self.characters.values() {
            if let Some(character_environment) = character.environments.get(environment) {
                return Ok(character_environment.ledger_entries.get(ledger).cloned());
            }
        }
        bail!("environment `{environment}` is not declared")
    }

    fn environment_owner(&self, environment: &str) -> Result<String> {
        self.characters
            .iter()
            .find_map(|(name, character)| {
                character
                    .environments
                    .contains_key(environment)
                    .then(|| name.clone())
            })
            .with_context(|| format!("environment `{environment}` is not declared"))
    }

    fn import_character_identity(
        &mut self,
        ledger: &str,
        actor: &str,
        replica: Option<&str>,
    ) -> Result<()> {
        self.require_character(actor)?;
        let ledger_state = self.ledger(ledger)?.clone();
        let environment_name = self.character_environment_name(actor, replica)?;
        let (identity_bundle, actor_id, key_id, seed_file) = {
            let character = self.character(actor)?;
            (
                character.identity_bundle.clone(),
                character.actor_id,
                character.current_key_id,
                character.identity_entry.seed_file.clone(),
            )
        };
        let already_created = self
            .character(actor)?
            .environments
            .get(&environment_name)
            .and_then(|environment| environment.ledger_entries.get(ledger))
            .is_some();
        if already_created {
            return Ok(());
        }
        let database = self.environment.ledger_dir.join(format!(
            "{}__{}.sqlite",
            safe_file_component(ledger),
            safe_file_component(&environment_name)
        ));
        let target_entry = LedgerEntry {
            name: format!("{actor}@{ledger}:{environment_name}"),
            ledger_id: ledger_state.entry.ledger_id.clone(),
            database,
            actor_id: actor_id.to_string(),
            key_id: key_id.to_string(),
            seed_file,
            read_only: false,
        };
        if let Some(parent) = target_entry.database.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::copy(&ledger_state.entry.database, &target_entry.database).with_context(|| {
            format!(
                "initialize replica `{environment_name}` for character `{actor}` on ledger `{ledger}`"
            )
        })?;
        import_identity(&target_entry, &identity_bundle).with_context(|| {
            format!("import identity for `{actor}` into replica `{environment_name}`")
        })?;
        let entry = LedgerEntry {
            name: format!("{actor}@{ledger}:{environment_name}"),
            ledger_id: ledger_state.entry.ledger_id,
            database: target_entry.database,
            actor_id: actor_id.to_string(),
            key_id: key_id.to_string(),
            seed_file: target_entry.seed_file,
            read_only: false,
        };
        let character = self.character_mut(actor)?;
        let environment = character
            .environments
            .get_mut(&environment_name)
            .with_context(|| {
                format!("character `{actor}` has no environment `{environment_name}`")
            })?;
        environment.ledger_entries.insert(ledger.to_string(), entry);
        environment.imported_ledgers.insert(ledger.to_string(), 1);
        Ok(())
    }

    fn propagate_primary_to_existing_replicas(&self, ledger: &str) -> Result<()> {
        let ledger_state = self.ledger(ledger)?.clone();
        for character in self.characters.values() {
            for environment in character.environments.values() {
                if let Some(entry) = environment.ledger_entries.get(ledger) {
                    self.import_missing_objects(&ledger_state.entry, entry)?;
                }
            }
        }
        Ok(())
    }

    fn import_missing_objects(&self, source: &LedgerEntry, target: &LedgerEntry) -> Result<usize> {
        let source_store = fact_store::Store::open(&source.database)?;
        let target_store = fact_store::Store::open(&target.database)?;
        let ledger_id = Uuid::parse_str(&source.ledger_id)?;
        let objects = export_bundle(&source_store, ledger_id)?;
        let mut missing = Vec::new();
        for object in objects {
            let cose = fact_crypto::decode_sign1(&object)?;
            let value: serde_json::Value = serde_json::from_slice(&cose.payload)?;
            let Some(id) = value["id"]
                .as_str()
                .and_then(|value| Uuid::parse_str(value).ok())
            else {
                continue;
            };
            if target_store.get_cose_by_id_any(id.as_bytes())?.is_none() {
                missing.push(object);
            }
        }
        if missing.is_empty() {
            Ok(0)
        } else {
            Ok(target_store.insert_authorized_bundle(&missing)?.len())
        }
    }

    fn rotate_character_key(&mut self, actor: &str) -> Result<Uuid> {
        self.require_character(actor)?;
        let (entry, seed) = {
            let character = self.character(actor)?;
            (character.identity_entry.clone(), character.current_seed)
        };
        self.sync_runtime_clock()?;
        let rotation = rotate_identity_key_with_runtime(&entry, &seed, &self.runtime)
            .with_context(|| format!("rotate signing key for character `{actor}`"))?;
        let seed_file = self
            .environment
            .identity_dir
            .join(format!("{}.seed", rotation.key_id));
        self.environment
            .write_seed(&seed_file, &rotation.new_seed)?;
        let character = self.character_mut(actor)?;
        character.current_key_id = rotation.key_id;
        character.current_seed = rotation.new_seed;
        character.identity_entry.key_id = rotation.key_id.to_string();
        character.identity_entry.seed_file = seed_file;
        character.identity_bundle = export_identity(&character.identity_entry)?.bundle;
        let identity_bundle = character.identity_bundle.clone();
        let key_id = character.current_key_id.to_string();
        let seed_file = character.identity_entry.seed_file.clone();
        for environment in character.environments.values_mut() {
            for entry in environment.ledger_entries.values_mut() {
                entry.key_id = key_id.clone();
                entry.seed_file = seed_file.clone();
                import_identity(entry, &identity_bundle).with_context(|| {
                    format!(
                        "import rotated identity for `{actor}` into environment `{}`",
                        environment.name
                    )
                })?;
            }
        }
        Ok(rotation.key_id)
    }

    fn character_environment_name(&self, actor: &str, replica: Option<&str>) -> Result<String> {
        let character = self.character(actor)?;
        if let Some(replica) = replica {
            if character.environments.contains_key(replica) {
                return Ok(replica.to_string());
            }
            bail!("character `{actor}` has no environment `{replica}`");
        }
        if character.environments.len() == 1 {
            Ok(character
                .environments
                .keys()
                .next()
                .expect("one environment exists")
                .clone())
        } else {
            bail!("character `{actor}` has multiple environments; specify replica")
        }
    }

    fn character(&self, name: &str) -> Result<&Character> {
        self.characters
            .get(name)
            .with_context(|| format!("character `{name}` is not declared"))
    }

    fn character_mut(&mut self, name: &str) -> Result<&mut Character> {
        self.characters
            .get_mut(name)
            .with_context(|| format!("character `{name}` is not declared"))
    }

    fn ledger(&self, ledger: &str) -> Result<&SdkLedger> {
        self.ledgers
            .get(ledger)
            .with_context(|| format!("ledger `{ledger}` is not declared"))
    }

    fn proposition(&self, proposition: &str) -> Result<&SdkProposition> {
        self.propositions
            .get(proposition)
            .with_context(|| format!("proposition `{proposition}` is not declared"))
    }

    fn revision_id(&self, revision: &str) -> Option<Uuid> {
        if revision == "latest" {
            self.propositions
                .values()
                .next()
                .and_then(|proposition| self.revisions.get(&proposition.latest_revision_symbol))
                .copied()
        } else {
            self.revisions.get(revision).copied()
        }
    }

    fn object_id_for_symbol(&self, object: &str) -> Result<Uuid> {
        self.revisions
            .get(object)
            .copied()
            .or_else(|| self.decisions.get(object).copied())
            .or_else(|| self.deliberations.get(object).copied())
            .or_else(|| self.settlements.get(object).copied())
            .or_else(|| self.invitations.get(object).copied())
            .or_else(|| self.participant_changes.get(object).copied())
            .or_else(|| self.invitation_lifecycles.get(object).copied())
            .or_else(|| {
                self.propositions
                    .get(object)
                    .and_then(|proposition| self.revisions.get(&proposition.latest_revision_symbol))
                    .copied()
            })
            .or_else(|| Uuid::parse_str(object).ok())
            .with_context(|| format!("object `{object}` is not declared"))
    }

    fn participant_change_id(&self, participant_change: &str) -> Result<Uuid> {
        self.participant_changes
            .get(participant_change)
            .copied()
            .or_else(|| Uuid::parse_str(participant_change).ok())
            .with_context(|| format!("participant change `{participant_change}` is not declared"))
    }

    fn invitation_lifecycle_id(&self, lifecycle: &str) -> Result<Uuid> {
        self.invitation_lifecycles
            .get(lifecycle)
            .copied()
            .or_else(|| Uuid::parse_str(lifecycle).ok())
            .with_context(|| format!("invitation lifecycle `{lifecycle}` is not declared"))
    }

    fn invitation_id(&self, invitation: Option<&str>) -> Result<Uuid> {
        if let Some(invitation) = invitation {
            return self
                .invitations
                .get(invitation)
                .copied()
                .or_else(|| Uuid::parse_str(invitation).ok())
                .with_context(|| format!("invitation `{invitation}` is not declared"));
        }
        match self
            .invitations
            .values()
            .copied()
            .collect::<Vec<_>>()
            .as_slice()
        {
            [invitation] => Ok(*invitation),
            [] => bail!("no invitation is available"),
            _ => bail!("multiple invitations are available; specify `invitation`"),
        }
    }

    fn decision_id(&self, decision: &str) -> Result<Uuid> {
        self.decisions
            .get(decision)
            .copied()
            .or_else(|| Uuid::parse_str(decision).ok())
            .with_context(|| format!("decision `{decision}` is not declared"))
    }

    fn deliberation_id(&self, deliberation: &str) -> Result<Uuid> {
        self.deliberations
            .get(deliberation)
            .copied()
            .or_else(|| Uuid::parse_str(deliberation).ok())
            .with_context(|| format!("deliberation `{deliberation}` is not declared"))
    }

    fn settlement_id(&self, settlement: &str) -> Result<Uuid> {
        self.settlements
            .get(settlement)
            .copied()
            .or_else(|| Uuid::parse_str(settlement).ok())
            .with_context(|| format!("settlement `{settlement}` is not declared"))
    }

    fn payload_for_object(&self, object_id: Uuid) -> Result<Option<Vec<u8>>> {
        for entry in self.snapshot_entries() {
            let store = fact_store::Store::open(&entry.database)?;
            if let Some(payload) = store.get_payload(object_id.as_bytes())? {
                return Ok(Some(payload));
            }
        }
        Ok(None)
    }

    fn revision_symbol_for(&self, revision_id: Uuid) -> String {
        self.revisions
            .iter()
            .find_map(|(symbol, id)| (*id == revision_id).then(|| symbol.clone()))
            .unwrap_or_else(|| revision_id.to_string())
    }

    fn single_ledger(&self) -> Result<&SdkLedger> {
        if self.ledgers.len() == 1 {
            Ok(self.ledgers.values().next().expect("one ledger exists"))
        } else {
            bail!("vertical slice requires exactly one ledger")
        }
    }

    fn single_ledger_name(&self) -> Result<String> {
        if self.ledgers.len() == 1 {
            Ok(self
                .ledgers
                .keys()
                .next()
                .expect("one ledger exists")
                .clone())
        } else {
            bail!("operation requires exactly one ledger")
        }
    }

    fn single_proposition(&self) -> Result<&SdkProposition> {
        if self.propositions.len() == 1 {
            Ok(self
                .propositions
                .values()
                .next()
                .expect("one proposition exists"))
        } else {
            bail!("vertical slice requires exactly one proposition")
        }
    }

    fn fact_home(&self) -> PathBuf {
        self.workspace.join("fact-home")
    }
}

fn operation_name(step: &Step) -> &'static str {
    match step {
        Step::Propose { .. } => "propose",
        Step::Revise { .. } => "revise",
        Step::Derive { .. } => "derive",
        Step::Accept { .. } => "accept",
        Step::Reject { .. } => "reject",
        Step::Decide { .. } => "decide",
        Step::OpenDeliberation { .. } => "open_deliberation",
        Step::Settle { .. } => "settle",
        Step::Reconcile { .. } => "reconcile",
        Step::SemanticCorrection { .. } => "semantic_correction",
        Step::RetryMissingDependency { .. } => "retry_missing_dependency",
        Step::InitReplica { .. } => "init_replica",
        Step::RecordDisposition { .. } => "record_disposition",
        Step::Assert { .. } => "assert",
        Step::RebuildProjections { .. } => "rebuild_projections",
        Step::CorruptProjections { .. } => "corrupt_projections",
        Step::CliCheck { .. } => "cli_check",
        Step::Grant { .. } => "grant",
        Step::RotateKey { .. } => "rotate_key",
        Step::Sync { .. } => "sync",
        Step::Parallel { .. } => "parallel",
        Step::Comment { .. } => "comment",
        Step::Invite { .. } => "invite",
        Step::Join { .. } => "join",
        Step::InvitationLifecycle { .. } => "invitation_lifecycle",
        Step::Leave { .. } => "leave",
        Step::Archive { .. } => "archive",
        Step::Withdraw { .. } => "withdraw",
        Step::AdvanceTime { .. } => "advance_time",
    }
}

fn assertion_name(assertion: &Assertion) -> &'static str {
    match assertion {
        Assertion::Status { .. } => "status",
        Assertion::EffectiveRevision { .. } => "effective_revision",
        Assertion::LatestRevision { .. } => "latest_revision",
        Assertion::RevisionStatus { .. } => "revision_status",
        Assertion::PendingActionCount { .. } => "pending_action_count",
        Assertion::ObjectCount { .. } => "object_count",
        Assertion::LatestContent { .. } => "latest_content",
        Assertion::ProjectionRebuildEquivalent { .. } => "projection_rebuild_equivalent",
        Assertion::CanonicalHistoryUnchanged { .. } => "canonical_history_unchanged",
        Assertion::Conflict { .. } => "conflict",
        Assertion::DecisionConflict { .. } => "decision_conflict",
        Assertion::DeliberationConflict { .. } => "deliberation_conflict",
        Assertion::DerivedRevision { .. } => "derived_revision",
        Assertion::Reconciliation { .. } => "reconciliation",
        Assertion::ReconciliationConflict { .. } => "reconciliation_conflict",
        Assertion::SemanticCorrection { .. } => "semantic_correction",
        Assertion::Retry { .. } => "retry",
        Assertion::CoordinatorDisposition { .. } => "coordinator_disposition",
        Assertion::InvitationRace { .. } => "invitation_race",
    }
}

fn step_with_default_replica(mut step: Step, replica: Option<&str>) -> Step {
    let Some(replica) = replica else {
        return step;
    };
    match &mut step {
        Step::Propose { propose } => {
            if propose.replica.is_none() {
                propose.replica = Some(replica.to_string());
            }
        }
        Step::Revise { revise } => {
            if revise.replica.is_none() {
                revise.replica = Some(replica.to_string());
            }
        }
        Step::Derive { derive } => {
            if derive.replica.is_none() {
                derive.replica = Some(replica.to_string());
            }
        }
        Step::Accept { accept } | Step::Reject { reject: accept } => {
            if accept.replica.is_none() {
                accept.replica = Some(replica.to_string());
            }
        }
        Step::Decide { decide } => {
            if decide.replica.is_none() {
                decide.replica = Some(replica.to_string());
            }
        }
        Step::OpenDeliberation { open_deliberation } => {
            if open_deliberation.replica.is_none() {
                open_deliberation.replica = Some(replica.to_string());
            }
        }
        Step::Settle { settle } => {
            if settle.replica.is_none() {
                settle.replica = Some(replica.to_string());
            }
        }
        Step::Reconcile { reconcile } => {
            if reconcile.replica.is_none() {
                reconcile.replica = Some(replica.to_string());
            }
        }
        Step::SemanticCorrection {
            semantic_correction,
        } => {
            if semantic_correction.replica.is_none() {
                semantic_correction.replica = Some(replica.to_string());
            }
        }
        Step::Comment { comment } => {
            if comment.replica.is_none() {
                comment.replica = Some(replica.to_string());
            }
        }
        Step::Invite { invite } | Step::Join { join: invite } | Step::Leave { leave: invite } => {
            if invite.replica.is_none() {
                invite.replica = Some(replica.to_string());
            }
        }
        Step::InvitationLifecycle {
            invitation_lifecycle,
        } => {
            if invitation_lifecycle.replica.is_none() {
                invitation_lifecycle.replica = Some(replica.to_string());
            }
        }
        Step::Archive { archive } | Step::Withdraw { withdraw: archive } => {
            if archive.replica.is_none() {
                archive.replica = Some(replica.to_string());
            }
        }
        Step::Parallel { .. }
        | Step::Assert { .. }
        | Step::RebuildProjections { .. }
        | Step::CorruptProjections { .. }
        | Step::CliCheck { .. }
        | Step::Grant { .. }
        | Step::InitReplica { .. }
        | Step::RotateKey { .. }
        | Step::Sync { .. }
        | Step::RetryMissingDependency { .. }
        | Step::RecordDisposition { .. }
        | Step::AdvanceTime { .. } => {}
    }
    step
}

impl From<PropositionListItem> for PropositionState {
    fn from(item: PropositionListItem) -> Self {
        Self {
            proposition_id: item.proposition_id,
            reference: item.reference,
            status: item.status,
            effective_status: item.effective_status,
            summary: item.summary,
            effective_revision_id: item.revision_id,
            deliberation_id: item.deliberation_id,
            latest_revision_id: item.latest_revision_id,
            latest_revision_status: item.latest_revision_status,
            pending_revision_id: item.pending_revision_id,
            pending_deliberation_id: item.pending_deliberation_id,
            current_actor_pending: item.current_actor_pending,
            has_pending_revision: item.has_pending_revision,
        }
    }
}

impl From<PropositionListItem> for PendingState {
    fn from(item: PropositionListItem) -> Self {
        Self {
            proposition_id: item.proposition_id,
            reference: item.reference,
            summary: item.summary,
            pending_revision_id: item.pending_revision_id,
        }
    }
}

impl TryFrom<serde_json::Value> for RevisionState {
    type Error = anyhow::Error;

    fn try_from(value: serde_json::Value) -> Result<Self> {
        Ok(Self {
            revision_id: parse_json_uuid(&value, "object_id")?,
            reference: value["reference"].as_str().unwrap_or_default().to_string(),
            status: value["status"].as_str().unwrap_or_default().to_string(),
            effective: value["effective"].as_bool().unwrap_or(false),
            latest: value["latest"].as_bool().unwrap_or(false),
            tip: value["tip"].as_bool().unwrap_or(false),
            summary: value["summary"].as_str().unwrap_or_default().to_string(),
            current_actor_pending: value["current_actor_pending"].as_bool().unwrap_or(false),
        })
    }
}

fn parse_json_uuid(value: &serde_json::Value, field: &str) -> Result<Uuid> {
    value[field]
        .as_str()
        .with_context(|| format!("revision JSON field `{field}` missing"))?
        .parse()
        .with_context(|| format!("revision JSON field `{field}` is not a UUID"))
}

pub fn snapshot_for_entry(entry: &LedgerEntry) -> Result<SdkStateSnapshot> {
    let propositions = list_propositions(
        entry,
        ListPropositionsFilter {
            status: None,
            all: true,
        },
    )?
    .into_iter()
    .map(PropositionState::from)
    .collect::<Vec<_>>();
    let mut revisions = Vec::new();
    for proposition in &propositions {
        let values = list_revisions(entry, &proposition.proposition_id.to_string())?;
        for value in values {
            revisions.push(RevisionState::try_from(value)?);
        }
    }
    revisions.sort_by_key(|revision| revision.revision_id);
    let pending = pending_propositions(entry)?
        .into_iter()
        .map(PendingState::from)
        .collect::<Vec<_>>();
    let ledger_id = Uuid::parse_str(&entry.ledger_id)?;
    let store = fact_store::Store::open(&entry.database)?;
    let object_counts_by_type = store.list_objects(ledger_id.as_bytes())?.into_iter().fold(
        BTreeMap::<String, usize>::new(),
        |mut counts, (_, _, object_type)| {
            *counts.entry(object_type).or_default() += 1;
            counts
        },
    );
    Ok(SdkStateSnapshot {
        propositions,
        revisions,
        pending,
        object_counts_by_type,
    })
}

pub fn protocol_hashes_for_entry(entry: &LedgerEntry) -> Result<Vec<String>> {
    let ledger_id = Uuid::parse_str(&entry.ledger_id)?;
    let store = fact_store::Store::open(&entry.database)?;
    let mut hashes = store
        .list_objects(ledger_id.as_bytes())?
        .into_iter()
        .map(|(_, hash, _)| hash.hex())
        .collect::<Vec<_>>();
    hashes.sort();
    Ok(hashes)
}

pub fn protocol_hashes_for_database(database: &Path) -> Result<Vec<String>> {
    let conn = rusqlite::Connection::open(database)?;
    let mut statement =
        conn.prepare("SELECT lower(hex(content_hash)) FROM protocol_object ORDER BY content_hash")?;
    let hashes = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(hashes)
}

fn protocol_object_id_count(database: &Path, object_id: Uuid) -> Result<usize> {
    let conn = rusqlite::Connection::open(database)?;
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM protocol_object WHERE object_id = ?",
        [object_id.as_bytes().as_slice()],
        |row| row.get(0),
    )?;
    Ok(count as usize)
}

fn projection_table_name(connection: &rusqlite::Connection, legacy: &str) -> Result<String> {
    if table_exists(connection, legacy)? {
        return Ok(legacy.to_string());
    }
    if let Some(projected) = legacy.strip_prefix("projection_") {
        let projected = format!("projected_{projected}");
        if table_exists(connection, &projected)? {
            return Ok(projected);
        }
    }
    Ok(legacy.to_string())
}

fn delete_blob_match_if_table_exists(
    connection: &rusqlite::Connection,
    table: &str,
    column: &str,
    value: &[u8],
) -> Result<()> {
    if table_exists(connection, table)? {
        connection.execute(&format!("DELETE FROM {table} WHERE {column}=?1"), [value])?;
    }
    Ok(())
}

fn table_exists(connection: &rusqlite::Connection, table: &str) -> Result<bool> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
        [table],
        |row| row.get::<_, i64>(0),
    )? != 0)
}

fn payload_hash(cose_bytes: &[u8]) -> Result<String> {
    Ok(fact_core::Hash::digest(&fact_crypto::decode_sign1(cose_bytes)?.payload).hex())
}

fn signed_object_hash(cose_bytes: &[u8]) -> String {
    fact_core::Hash::digest(cose_bytes).hex()
}

fn coordinator_disposition_from_str(value: &str) -> Result<CoordinatorDisposition> {
    match value {
        "accepted" => Ok(CoordinatorDisposition::Accepted),
        "rejected-protocol-invalid" => Ok(CoordinatorDisposition::RejectedProtocolInvalid),
        "rejected-unauthorized" => Ok(CoordinatorDisposition::RejectedUnauthorized),
        "rejected-policy" => Ok(CoordinatorDisposition::RejectedPolicy),
        "rejected-missing-dependency" => Ok(CoordinatorDisposition::RejectedMissingDependency),
        "deferred" => Ok(CoordinatorDisposition::Deferred),
        "quarantined" => Ok(CoordinatorDisposition::Quarantined),
        "removed-local" => Ok(CoordinatorDisposition::RemovedLocal),
        "unknown" => Ok(CoordinatorDisposition::Unknown),
        other => bail!("unknown coordinator disposition `{other}`"),
    }
}

fn failure_classification_from_str(value: &str) -> Result<FailureClassification> {
    match value {
        "retryable-unchanged" => Ok(FailureClassification::RetryableUnchanged),
        "requires-new-signed-object" => Ok(FailureClassification::RequiresNewSignedObject),
        other => bail!("unknown failure classification `{other}`"),
    }
}

fn failure_classification_as_str(classification: &FailureClassification) -> &'static str {
    match classification {
        FailureClassification::RetryableUnchanged => "retryable-unchanged",
        FailureClassification::RequiresNewSignedObject => "requires-new-signed-object",
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn sdk_timestamp(value: time::OffsetDateTime) -> String {
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.000Z",
        value.year(),
        u8::from(value.month()),
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    )
}

fn default_environment_name(character: &str) -> String {
    format!("{character}.default")
}

fn safe_file_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn run_workspace(scenario_name: &str, seed: u64) -> Result<PathBuf> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_millis();
    let counter = RUN_WORKSPACE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let safe_name = scenario_name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    let workspace = PathBuf::from("target")
        .join("fact-sim-runs")
        .join(format!("{safe_name}-seed-{seed}-{timestamp}-{counter}"));
    std::fs::create_dir_all(&workspace)?;
    Ok(workspace)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn named_character_grant_signs_operations_as_selected_actor() {
        let scenario = Scenario::from_yaml_str(
            r#"
version: 1
name: named-character-grant
seed: 77
clock:
  start: 2026-01-05T09:00:00Z
characters:
  - name: alice
    environments:
      - operations_a
  - name: bob
    environments:
      - operations_b
ledgers:
  - name: operations
steps:
  - grant:
      ledger: operations
      actor: bob
      capabilities: [propose, accept, deliberate]
  - propose:
      actor: bob
      replica: operations_b
      ledger: operations
      as: bob_policy
      markdown: |
        # Bob policy

        Character-scoped proposal.
  - accept:
      actor: bob
      replica: operations_b
      proposition: bob_policy
  - assert:
      - status:
          proposition: bob_policy
          equals: accepted
"#,
        )
        .unwrap();

        let run = run_scenario(&scenario).unwrap();
        let bob = run
            .characters
            .iter()
            .find(|character| character.name == "bob")
            .unwrap();
        assert_eq!(
            run.manifest.character_ids.get("bob").copied(),
            Some(bob.actor_id)
        );
        assert_eq!(
            run.manifest
                .character_environments
                .get("bob")
                .map(Vec::as_slice),
            Some(&["operations_b".to_string()][..])
        );
        let proposition_id = run
            .final_state
            .propositions
            .iter()
            .find(|proposition| proposition.summary == "Bob policy")
            .unwrap()
            .proposition_id;
        let bob_database = bob
            .environment_databases
            .get("operations_b")
            .and_then(|ledgers| ledgers.get("operations"))
            .unwrap();
        let actor_id = object_actor_id(Path::new(bob_database), proposition_id).unwrap();
        assert_eq!(actor_id, bob.actor_id);
        assert_ne!(
            run.characters
                .iter()
                .find(|character| character.name == "alice")
                .unwrap()
                .actor_id,
            bob.actor_id
        );
    }

    #[test]
    fn rotate_key_preserves_character_actor_id_for_future_operations() {
        let scenario = Scenario::from_yaml_str(
            r#"
version: 1
name: named-character-rotation
seed: 78
clock:
  start: 2026-01-05T09:00:00Z
characters:
  - name: alice
    environments:
      - alice.laptop
ledgers:
  - name: operations
steps:
  - grant:
      ledger: operations
      actor: alice
      capabilities: [propose, accept, deliberate]
  - rotate_key:
      actor: alice
  - propose:
      actor: alice
      replica: alice.laptop
      ledger: operations
      as: rotated_policy
      markdown: |
        # Rotated policy

        Uses the current rotated key.
"#,
        )
        .unwrap();

        let run = run_scenario(&scenario).unwrap();
        let alice = run
            .characters
            .iter()
            .find(|character| character.name == "alice")
            .unwrap();
        assert_eq!(alice.historical_key_ids.len(), 1);
        assert_ne!(alice.historical_key_ids[0], alice.current_key_id);
        assert_eq!(
            run.manifest.character_key_ids.get("alice").copied(),
            Some(alice.current_key_id)
        );

        let proposition_id = run.final_state.propositions[0].proposition_id;
        let database = Path::new(
            alice
                .environment_databases
                .get("alice.laptop")
                .and_then(|ledgers| ledgers.get("operations"))
                .unwrap(),
        );
        assert_eq!(
            object_actor_id(database, proposition_id).unwrap(),
            alice.actor_id
        );
        assert_eq!(
            object_signing_key_id(database, proposition_id).unwrap(),
            alice.current_key_id
        );
    }

    #[test]
    fn character_environments_have_independent_replicas_that_sync() {
        let scenario = Scenario::from_yaml_str(
            r#"
version: 1
name: multi-environment-sync
seed: 80
clock:
  start: 2026-01-05T09:00:00Z
characters:
  - name: alice
    environments:
      - alice.laptop
      - alice.desktop
ledgers:
  - name: operations
steps:
  - grant:
      ledger: operations
      actor: alice
      capabilities: [propose, accept, deliberate]
  - propose:
      actor: alice
      replica: alice.laptop
      ledger: operations
      as: policy
      markdown: |
        # Environment policy

        Created on the laptop.
  - sync:
      from: alice.laptop
      to: alice.desktop
      ledger: operations
  - revise:
      actor: alice
      replica: alice.desktop
      proposition: policy
      as: policy_v2
      markdown: |
        # Environment policy

        Revised on the desktop.
  - sync:
      from: alice.desktop
      to: alice.laptop
      ledger: operations
  - accept:
      actor: alice
      replica: alice.laptop
      proposition: policy
  - sync:
      from: alice.laptop
      to: alice.desktop
      ledger: operations
  - sync:
      from: alice.laptop
      to: alice.desktop
      ledger: operations
  - assert:
      - status:
          proposition: policy
          equals: accepted
      - effective_revision:
          proposition: policy
          equals: policy_v2
"#,
        )
        .unwrap();

        let run = run_scenario(&scenario).unwrap();
        let alice = run
            .characters
            .iter()
            .find(|character| character.name == "alice")
            .unwrap();
        let laptop = alice
            .environment_databases
            .get("alice.laptop")
            .and_then(|ledgers| ledgers.get("operations"))
            .unwrap();
        let desktop = alice
            .environment_databases
            .get("alice.desktop")
            .and_then(|ledgers| ledgers.get("operations"))
            .unwrap();
        assert_ne!(laptop, desktop);
        assert_eq!(
            protocol_hashes_for_database(Path::new(laptop)).unwrap(),
            protocol_hashes_for_database(Path::new(desktop)).unwrap()
        );
        let proposition_id = run.final_state.propositions[0].proposition_id;
        assert_eq!(
            object_actor_id(Path::new(laptop), proposition_id).unwrap(),
            alice.actor_id
        );
        assert_eq!(
            object_actor_id(Path::new(desktop), proposition_id).unwrap(),
            alice.actor_id
        );
    }

    #[test]
    fn character_replicas_support_invitation_comment_and_lifecycle_steps() {
        let scenario = Scenario::from_yaml_str(
            r#"
version: 1
name: character-participation
seed: 81
clock:
  start: 2026-01-05T09:00:00Z
characters:
  - name: alice
    environments: [operations_a]
  - name: bob
    environments: [operations_b]
ledgers:
  - name: operations
steps:
  - grant:
      ledger: operations
      actor: alice
      capabilities: [propose, accept, deliberate, invite, comment, withdraw, archive]
  - grant:
      ledger: operations
      actor: bob
      capabilities: [comment, deliberate]
  - propose:
      actor: alice
      replica: operations_a
      ledger: operations
      as: policy
      markdown: |
        # Participation policy

        Bob should be able to join and comment.
  - invite:
      actor: alice
      replica: operations_a
      proposition: policy
      participant: bob
      as: bob_policy_invite
  - sync:
      from: operations_a
      to: operations_b
      ledger: operations
  - join:
      actor: bob
      replica: operations_b
      proposition: policy
      invitation: bob_policy_invite
  - comment:
      actor: bob
      replica: operations_b
      proposition: policy
      message: |
        # Bob comment

        Joined from his replica.
  - sync:
      from: operations_b
      to: operations_a
      ledger: operations
  - withdraw:
      actor: alice
      replica: operations_a
      proposition: policy
  - archive:
      actor: alice
      replica: operations_a
      proposition: policy
"#,
        )
        .unwrap();

        let run = run_scenario(&scenario).unwrap();
        assert_eq!(
            run.final_state
                .object_counts_by_type
                .get("participant_invitation")
                .copied(),
            Some(1)
        );
        assert_eq!(
            run.final_state
                .object_counts_by_type
                .get("deliberation_comment")
                .copied(),
            Some(1)
        );
        assert_eq!(
            run.final_state
                .object_counts_by_type
                .get("proposition_lifecycle")
                .copied(),
            Some(2)
        );
    }

    #[test]
    fn concurrent_revisions_are_expressed_as_replica_conflicts_not_identity_conflicts() {
        let scenario = Scenario::from_yaml_str(
            r#"
version: 1
name: character-conflict
seed: 82
clock:
  start: 2026-01-05T09:00:00Z
characters:
  - name: alice
    environments: [operations_a, operations_b]
ledgers:
  - name: operations
steps:
  - grant:
      ledger: operations
      actor: alice
      capabilities: [propose, accept, deliberate]
  - propose:
      actor: alice
      replica: operations_a
      ledger: operations
      as: policy
      markdown: |
        # Conflict policy

        Base version.
  - accept:
      actor: alice
      replica: operations_a
      proposition: policy
  - sync:
      from: operations_a
      to: operations_b
      ledger: operations
  - revise:
      actor: alice
      replica: operations_a
      proposition: policy
      as: policy_a
      markdown: |
        # Conflict policy

        Alice branch.
  - accept:
      actor: alice
      replica: operations_a
      proposition: policy
  - revise:
      actor: alice
      replica: operations_b
      proposition: policy
      as: policy_b
      markdown: |
        # Conflict policy

        Bob branch.
  - accept:
      actor: alice
      replica: operations_b
      proposition: policy
  - sync:
      from: operations_a
      to: operations_b
      ledger: operations
  - assert:
      - status:
          proposition: policy
          equals: conflict
"#,
        )
        .unwrap();

        let run = run_scenario(&scenario).unwrap();
        assert_eq!(run.final_state.propositions[0].effective_status, "conflict");
    }

    #[test]
    fn ledger_authority_does_not_leak_between_ledgers() {
        let scenario = Scenario::from_yaml_str(
            r#"
version: 1
name: multi-ledger-authority
seed: 79
clock:
  start: 2026-01-05T09:00:00Z
characters:
  - name: alice
ledgers:
  - name: operations
  - name: engineering
steps:
  - grant:
      ledger: operations
      actor: alice
      capabilities: [propose, deliberate]
  - grant:
      ledger: engineering
      actor: alice
      capabilities: [accept]
  - propose:
      actor: alice
      ledger: engineering
      as: leaked_policy
      markdown: |
        # Leaked policy

        This should not be authorized.
"#,
        )
        .unwrap();

        let error = run_scenario(&scenario).unwrap_err().to_string();
        assert!(error.contains("has no propose authority"));
        assert!(error.contains("engineering"));
    }

    #[test]
    fn scenario_validation_rejects_duplicate_characters_and_unknown_environments() {
        let duplicate = Scenario::from_yaml_str(
            r#"
version: 1
name: duplicate-character
clock:
  start: 2026-01-05T09:00:00Z
characters:
  - name: alice
  - name: alice
"#,
        );
        assert!(
            duplicate
                .unwrap_err()
                .to_string()
                .contains("duplicate character")
        );

        let unknown_environment = Scenario::from_yaml_str(
            r##"
version: 1
name: unknown-environment
clock:
  start: 2026-01-05T09:00:00Z
characters:
  - name: alice
    environments: [alice.laptop]
ledgers:
  - name: operations
steps:
  - propose:
      actor: alice
      replica: alice.desktop
      ledger: operations
      as: policy
      markdown: "# Policy"
"##,
        );
        assert!(
            unknown_environment
                .unwrap_err()
                .to_string()
                .contains("has no environment")
        );

        let unknown_character = Scenario::from_yaml_str(
            r#"
version: 1
name: unknown-character
clock:
  start: 2026-01-05T09:00:00Z
characters:
  - name: alice
ledgers:
  - name: operations
steps:
  - grant:
      ledger: operations
      actor: bob
      capabilities: [propose]
"#,
        );
        assert!(
            unknown_character
                .unwrap_err()
                .to_string()
                .contains("grant actor `bob` is not declared")
        );
    }

    #[test]
    fn deterministic_replay_returns_same_logical_digest() {
        let scenario = Scenario::from_yaml_str(include_str!(
            "../../../scenarios/smoke/pending-revision-acceptance.yaml"
        ))
        .unwrap();

        let first = run_scenario(&scenario).unwrap();
        let second = run_scenario(&scenario).unwrap();

        assert_eq!(first.logical_digest, second.logical_digest);
        assert_eq!(
            first.manifest.object_counts_by_type,
            second.manifest.object_counts_by_type
        );
        assert_eq!(
            protocol_hashes_for_database(Path::new(
                first.manifest.output_database.as_deref().unwrap()
            ))
            .unwrap(),
            protocol_hashes_for_database(Path::new(
                second.manifest.output_database.as_deref().unwrap()
            ))
            .unwrap()
        );
        assert_eq!(
            character_database_hashes(&first).unwrap(),
            character_database_hashes(&second).unwrap()
        );
    }

    #[test]
    fn parallel_schedule_replays_accepted_sibling_revision_conflict() {
        let scenario = Scenario::from_yaml_str(include_str!(
            "../../../scenarios/conflict/accepted-sibling-revisions.yaml"
        ))
        .unwrap();

        let run = run_scenario(&scenario).unwrap();
        let policy = run
            .final_state
            .propositions
            .iter()
            .find(|item| item.summary == "Conflict policy")
            .expect("policy proposition exists");

        assert_eq!(policy.effective_status, "conflict");
        assert_eq!(
            run.manifest
                .conflict_repair_report
                .conflict_counts_by_class
                .get("accepted-sibling-revisions"),
            Some(&1)
        );
        assert_eq!(
            run.manifest
                .conflict_repair_report
                .assertion_results
                .get("conflict"),
            Some(&1)
        );
        let effective = run
            .final_state
            .revisions
            .iter()
            .find(|item| Some(item.revision_id) == policy.effective_revision_id)
            .expect("effective revision exists");
        assert_eq!(effective.summary, "Conflict policy");
    }

    #[test]
    fn concurrent_participant_decisions_remain_pending_until_resolution() {
        let scenario = Scenario::from_yaml_str(include_str!(
            "../../../scenarios/conflict/concurrent-participant-decisions.yaml"
        ))
        .unwrap();

        let run = run_scenario(&scenario).unwrap();
        let policy = run
            .final_state
            .propositions
            .iter()
            .find(|item| item.summary == "Decision policy")
            .expect("decision policy proposition exists");

        assert_eq!(policy.effective_status, "pending");
        assert!(
            !run.manifest
                .conflict_repair_report
                .conflict_counts_by_class
                .contains_key("decision-conflict")
        );
        assert_eq!(
            run.manifest
                .conflict_repair_report
                .assertion_results
                .get("decision_conflict"),
            Some(&3)
        );
    }

    #[test]
    fn incompatible_parallel_deliberations_follow_projected_outcome() {
        let scenario = Scenario::from_yaml_str(include_str!(
            "../../../scenarios/conflict/incompatible-parallel-deliberations.yaml"
        ))
        .unwrap();

        let run = run_scenario(&scenario).unwrap();
        let policy = run
            .final_state
            .propositions
            .iter()
            .find(|item| item.summary == "Deliberation policy")
            .expect("deliberation policy proposition exists");

        assert_eq!(policy.effective_status, "rejected");
        assert!(
            !run.manifest
                .conflict_repair_report
                .conflict_counts_by_class
                .contains_key("incompatible-parallel-deliberations")
        );
    }

    #[test]
    fn compatible_parallel_deliberations_do_not_create_false_conflict() {
        let scenario = Scenario::from_yaml_str(include_str!(
            "../../../scenarios/conflict/compatible-parallel-deliberations.yaml"
        ))
        .unwrap();

        let run = run_scenario(&scenario).unwrap();
        let policy = run
            .final_state
            .propositions
            .iter()
            .find(|item| item.summary == "Compatible policy")
            .expect("compatible policy proposition exists");

        assert_eq!(policy.effective_status, "accepted");
        assert!(
            !run.manifest
                .conflict_repair_report
                .conflict_counts_by_class
                .contains_key("incompatible-parallel-deliberations")
        );
    }

    #[test]
    fn reconciliation_select_creates_reconciliation_projection() {
        let scenario = Scenario::from_yaml_str(include_str!(
            "../../../scenarios/reconciliation/select.yaml"
        ))
        .unwrap();

        let run = run_scenario(&scenario).unwrap();
        let policy = run
            .final_state
            .propositions
            .iter()
            .find(|item| item.summary == "Reconciliation policy")
            .expect("reconciled policy proposition exists");

        assert_eq!(policy.effective_status, "accepted");
        assert_eq!(
            run.manifest
                .conflict_repair_report
                .reconciliation_counts_by_mode
                .get("select"),
            Some(&1)
        );
        assert_eq!(
            run.manifest
                .conflict_repair_report
                .assertion_results
                .get("reconciliation"),
            Some(&1)
        );
        assert_eq!(
            run.manifest
                .conflict_repair_report
                .conflict_counts_by_class
                .get("accepted-sibling-revisions"),
            Some(&1)
        );
    }

    #[test]
    fn reconciliation_reject_all_keeps_common_ancestor_effective() {
        let scenario = Scenario::from_yaml_str(include_str!(
            "../../../scenarios/reconciliation/reject-all.yaml"
        ))
        .unwrap();

        let run = run_scenario(&scenario).unwrap();
        let policy = run
            .final_state
            .propositions
            .iter()
            .find(|item| item.summary == "Reject all policy")
            .expect("reconciled policy proposition exists");

        assert_eq!(policy.effective_status, "accepted");
        assert_eq!(
            run.manifest
                .conflict_repair_report
                .reconciliation_counts_by_mode
                .get("reject-all"),
            Some(&1)
        );
        assert_eq!(
            run.manifest
                .conflict_repair_report
                .assertion_results
                .get("reconciliation"),
            Some(&1)
        );
    }

    #[test]
    fn reconciliation_derive_advances_derived_revision_after_acceptance() {
        let scenario = Scenario::from_yaml_str(include_str!(
            "../../../scenarios/reconciliation/derive.yaml"
        ))
        .unwrap();

        let run = run_scenario(&scenario).unwrap();
        let policy = run
            .final_state
            .propositions
            .iter()
            .find(|item| item.summary == "Derived policy")
            .expect("reconciled policy proposition exists");

        assert_eq!(policy.effective_status, "accepted");
        assert_eq!(
            run.manifest
                .conflict_repair_report
                .reconciliation_counts_by_mode
                .get("derive"),
            Some(&1)
        );
        assert_eq!(
            run.manifest
                .conflict_repair_report
                .assertion_results
                .get("reconciliation"),
            Some(&1)
        );
    }

    #[test]
    fn conflicting_reconciliation_outcomes_leave_policy_contested() {
        let scenario = Scenario::from_yaml_str(include_str!(
            "../../../scenarios/reconciliation/conflicting-outcomes.yaml"
        ))
        .unwrap();

        let run = run_scenario(&scenario).unwrap();
        let policy = run
            .final_state
            .propositions
            .iter()
            .find(|item| item.summary == "Reconciliation conflict policy")
            .expect("contested policy proposition exists");

        assert_eq!(policy.effective_status, "conflict");
        assert_eq!(
            run.manifest
                .conflict_repair_report
                .conflict_counts_by_class
                .get("conflicting-reconciliation-outcomes"),
            Some(&1)
        );
        assert_eq!(
            run.manifest
                .conflict_repair_report
                .assertion_results
                .get("reconciliation_conflict"),
            Some(&1)
        );
    }

    #[test]
    fn projection_corruption_rebuild_preserves_canonical_history() {
        let scenario = Scenario::from_yaml_str(include_str!(
            "../../../scenarios/repair/projection-corruption-rebuild.yaml"
        ))
        .unwrap();

        let run = run_scenario(&scenario).unwrap();
        let policy = run
            .final_state
            .propositions
            .iter()
            .find(|item| item.summary == "Repair policy")
            .expect("repair policy proposition exists");

        assert_eq!(policy.effective_status, "accepted");
        assert_eq!(run.logical_digest.proposition_count, 1);
        assert_eq!(run.repair_report.projection_repairs, 1);
        assert!(run.repair_report.repaired_replicas_converged);
        assert!(run.repair_report.canonical_history_preserved);
        assert_eq!(run.repair_report.repairs.len(), 1);
        assert_eq!(run.repair_report.repairs[0].repair_type, "projection");
        assert_eq!(
            run.manifest
                .conflict_repair_report
                .repair_counts
                .get("projection"),
            Some(&1)
        );
        assert_eq!(
            run.manifest
                .conflict_repair_report
                .assertion_results
                .get("canonical_history_unchanged"),
            Some(&1)
        );
    }

    #[test]
    fn missing_dependency_retry_reuses_same_signed_object() {
        let scenario = Scenario::from_yaml_str(include_str!(
            "../../../scenarios/repair/missing-dependency-retry.yaml"
        ))
        .unwrap();

        let run = run_scenario(&scenario).unwrap();
        let policy = run
            .final_state
            .propositions
            .iter()
            .find(|item| item.summary == "Retry policy")
            .expect("retry policy proposition exists");

        assert_eq!(policy.effective_status, "accepted");
        assert_eq!(run.retry_report.len(), 1);
        let retry = &run.retry_report[0];
        assert_eq!(
            retry.first_disposition,
            CoordinatorDisposition::RejectedMissingDependency
        );
        assert_eq!(retry.retry_disposition, CoordinatorDisposition::Accepted);
        assert_eq!(
            retry.classification,
            FailureClassification::RetryableUnchanged
        );
        assert!(retry.retryable_unchanged);
        assert_eq!(retry.original_payload_hash, retry.retried_payload_hash);
        assert_eq!(
            retry.original_signed_object_hash,
            retry.retried_signed_object_hash
        );
        assert_eq!(retry.duplicate_count, 1);
        assert!(retry.converged);
        assert_eq!(run.repair_report.partial_sync_repairs, 1);
        assert_eq!(
            run.manifest
                .conflict_repair_report
                .retry_counts_by_disposition
                .get("rejected-missing-dependency"),
            Some(&1)
        );
        assert_eq!(
            run.manifest
                .conflict_repair_report
                .retry_counts_by_disposition
                .get("accepted"),
            Some(&1)
        );
        assert_eq!(
            run.manifest
                .conflict_repair_report
                .repair_counts
                .get("partial-sync"),
            Some(&1)
        );
        assert_eq!(
            run.manifest
                .conflict_repair_report
                .assertion_results
                .get("retry"),
            Some(&1)
        );
    }

    #[test]
    fn compensating_correction_creates_new_signed_successor() {
        let scenario = Scenario::from_yaml_str(include_str!(
            "../../../scenarios/repair/compensating-correction.yaml"
        ))
        .unwrap();

        let run = run_scenario(&scenario).unwrap();
        let policy = run
            .final_state
            .propositions
            .iter()
            .find(|item| item.summary == "Correction policy")
            .expect("correction policy proposition exists");

        assert_eq!(policy.effective_status, "accepted");
        assert_eq!(run.semantic_correction_report.len(), 1);
        let correction = &run.semantic_correction_report[0];
        assert!(correction.preserved_history);
        assert!(correction.requires_new_signed_object);
        assert!(correction.effective_changed);
        assert_ne!(
            correction.previous_revision_id,
            correction.corrective_revision_id
        );
        assert_eq!(
            correction.previous_payload_hash,
            correction.preserved_payload_hash
        );
        assert_eq!(
            correction.previous_signed_object_hash,
            correction.preserved_signed_object_hash
        );
        assert_ne!(
            correction.previous_signed_object_hash,
            correction.corrective_signed_object_hash
        );
        assert_eq!(run.repair_report.semantic_corrections, 1);
        assert_eq!(
            run.manifest
                .conflict_repair_report
                .repair_counts
                .get("semantic-correction"),
            Some(&1)
        );
        assert_eq!(
            run.manifest
                .conflict_repair_report
                .assertion_results
                .get("semantic_correction"),
            Some(&1)
        );
    }

    #[test]
    fn coordinator_policy_divergence_does_not_change_canonical_state() {
        let scenario = Scenario::from_yaml_str(include_str!(
            "../../../scenarios/repair/coordinator-policy-divergence.yaml"
        ))
        .unwrap();

        let run = run_scenario(&scenario).unwrap();
        let policy = run
            .final_state
            .propositions
            .iter()
            .find(|item| item.summary == "Divergence policy")
            .expect("divergence policy proposition exists");

        assert_eq!(policy.effective_status, "accepted");
        assert_eq!(run.coordinator_disposition_report.len(), 2);
        let dispositions = run
            .coordinator_disposition_report
            .iter()
            .map(|record| record.disposition.as_str().to_string())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            dispositions,
            BTreeSet::from(["accepted".to_string(), "rejected-policy".to_string()])
        );
        assert!(
            !run.coordinator_disposition_report
                .iter()
                .any(|record| record.disposition
                    == CoordinatorDisposition::RejectedProtocolInvalid)
        );
        assert!(
            run.coordinator_disposition_report
                .iter()
                .all(|record| record.canonical_unchanged)
        );
        assert!(
            run.coordinator_disposition_report
                .iter()
                .all(|record| !record.statement_payload_hash.is_empty()
                    && !record.statement_signature.is_empty())
        );
        assert_eq!(
            run.manifest
                .conflict_repair_report
                .assertion_results
                .get("coordinator_disposition"),
            Some(&1)
        );
    }

    #[test]
    fn concurrent_invitation_lifecycle_and_join_reports_invitation_race() {
        let scenario = Scenario::from_yaml_str(include_str!(
            "../../../scenarios/repair/invitation-race.yaml"
        ))
        .unwrap();

        let run = run_scenario(&scenario).unwrap();
        let policy = run
            .final_state
            .propositions
            .iter()
            .find(|item| item.summary == "Invitation race policy")
            .expect("invitation race policy proposition exists");

        assert_eq!(policy.effective_status, "pending");
        assert_eq!(run.invitation_race_report.len(), 1);
        let race = &run.invitation_race_report[0];
        assert_eq!(race.conflict_type, "invitation-race");
        assert_eq!(race.lifecycle_operation, "revoke");
        assert!(race.concurrent);
        assert!(!race.enrollment_occurs);
        assert!(race.new_invitation_required);
        assert_eq!(
            race.classification,
            FailureClassification::RequiresNewSignedObject
        );
        assert_eq!(
            run.manifest
                .conflict_repair_report
                .conflict_counts_by_class
                .get("invitation-race"),
            Some(&1)
        );
        assert_eq!(
            run.manifest
                .conflict_repair_report
                .assertion_results
                .get("invitation_race"),
            Some(&1)
        );
    }

    fn object_actor_id(database: &Path, object_id: Uuid) -> Result<Uuid> {
        object_uuid_field(database, object_id, "actor_id")
    }

    fn object_signing_key_id(database: &Path, object_id: Uuid) -> Result<Uuid> {
        object_uuid_field(database, object_id, "signing_key_id")
    }

    fn object_uuid_field(database: &Path, object_id: Uuid, field: &str) -> Result<Uuid> {
        let store = fact_store::Store::open(database)?;
        let payload = store
            .get_payload(object_id.as_bytes())?
            .with_context(|| format!("missing object {object_id}"))?;
        let value: serde_json::Value = serde_json::from_slice(&payload)?;
        value[field]
            .as_str()
            .with_context(|| format!("object {object_id} missing `{field}`"))?
            .parse()
            .with_context(|| format!("object {object_id} `{field}` is not a UUID"))
    }

    fn character_database_hashes(run: &ScenarioRun) -> Result<BTreeMap<String, Vec<String>>> {
        let mut hashes = BTreeMap::new();
        for character in &run.characters {
            for (environment, ledgers) in &character.environment_databases {
                for (ledger, database) in ledgers {
                    hashes.insert(
                        format!("{}:{environment}:{ledger}", character.name),
                        protocol_hashes_for_database(Path::new(database))?,
                    );
                }
            }
        }
        Ok(hashes)
    }
}
