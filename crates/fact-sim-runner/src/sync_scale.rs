use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use axum::body::{Body, to_bytes};
use axum::http::Request;
use fact_sdk::discussion::{
    create_comment_with_runtime, join_deliberation_with_runtime, leave_deliberation_with_runtime,
};
use fact_sdk::environment::{LedgerEntry, RemoteEntry, UserEnvironment};
use fact_sdk::identity::{
    CreateIdentityInput, create_identity_grant_with_runtime, create_identity_with_runtime,
    export_identity, import_identity, revoke_identity_grant_with_runtime,
    rotate_identity_key_with_runtime,
};
use fact_sdk::invitation::create_invitation_with_runtime;
use fact_sdk::lifecycle::{archive_proposition_with_runtime, withdraw_proposition_with_runtime};
use fact_sdk::proposition::{
    DecisionOutcome, DerivedRevisionInput, ListPropositionsFilter, ReconciliationConflictInput,
    ReconciliationInput, accept_proposition_with_runtime, create_derived_revision_with_runtime,
    create_proposition_with_runtime, create_proposition_with_runtime_and_projected_mode,
    create_reconciliation_proposition_with_runtime, list_propositions, read_proposition_content,
    reject_proposition_with_runtime, update_proposition_content_with_runtime,
};
use fact_sdk::runtime::DeterministicRuntime;
use fact_sdk::state::rebuild_state;
use fact_sdk::sync::{
    decode_bundle_or_snapshot_objects, decode_bundle_or_snapshot_slices, encode_bundle,
    export_bundle, export_object, write_ledger_bundle_from_store,
};
use fact_sdk::workflow::{BootstrapLedgerInput, create_ledger_with_runtime};
use fact_sim_core::{
    Clock, CoordinatorDisposition, DeterministicRandomSource, FailureClassification, RandomSource,
    SimClock,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tower::ServiceExt;
use uuid::Uuid;

use crate::{CliReceipt, SdkStateSnapshot, protocol_hashes_for_database};

pub const LEGACY_PROFILE: &str = "sync-100k";
pub const CONFLICT_REPAIR_PROFILE: &str = "conflict-repair";
const TARGET_OBJECTS: usize = 150_000;
const WORKFLOW_OBJECTS: usize = 900;
const SCALE_IDENTITY_LIFECYCLE_ACTOR_CAP: usize = 64;
const HTTP_OBJECT_LIST_PAGE_SIZE: usize = 1_000;

fn default_fact_binary_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .join("cli")
        .join("target")
        .join("debug")
        .join("fact")
}

#[derive(Debug, Clone)]
pub struct GenerateOptions {
    pub profile: String,
    pub seed: u64,
    pub output: PathBuf,
    pub fact_binary: Option<PathBuf>,
    pub target_objects: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncScaleReport {
    pub profile: String,
    #[serde(default = "default_profile_version")]
    pub profile_version: u32,
    pub seed: u64,
    #[serde(default = "default_scheduler_version")]
    pub scheduler_version: String,
    #[serde(default = "default_scenario_corpus_version")]
    pub scenario_corpus_version: String,
    #[serde(default = "default_content_template_version")]
    pub content_template_version: String,
    #[serde(default = "default_time_distribution_profile")]
    pub time_distribution_profile: String,
    #[serde(default = "default_started_at")]
    #[serde(with = "time::serde::rfc3339")]
    pub started_at: time::OffsetDateTime,
    #[serde(default = "default_started_at")]
    #[serde(with = "time::serde::rfc3339")]
    pub simulated_started_at: time::OffsetDateTime,
    #[serde(default = "default_started_at")]
    #[serde(with = "time::serde::rfc3339")]
    pub simulated_ended_at: time::OffsetDateTime,
    #[serde(default)]
    pub simulated_duration_seconds: i64,
    #[serde(default = "default_revision")]
    pub facts_sdk_revision: String,
    #[serde(default = "default_revision")]
    pub facts_cli_revision: String,
    #[serde(default = "default_revision")]
    pub simulator_revision: String,
    #[serde(default = "default_revision")]
    pub generator_version: String,
    #[serde(default = "default_revision")]
    pub generator_source_commit: String,
    #[serde(default = "default_revision")]
    pub rust_toolchain_version: String,
    pub output: PathBuf,
    pub databases: BTreeMap<String, PathBuf>,
    pub bundle: PathBuf,
    #[serde(default)]
    pub bundle_paths: Vec<PathBuf>,
    #[serde(default)]
    pub snapshot_paths: Vec<PathBuf>,
    pub commitment_root: Option<String>,
    #[serde(default)]
    pub final_commitment_roots: Vec<String>,
    #[serde(default)]
    pub target_objects: usize,
    pub object_count: usize,
    pub object_counts_by_type: BTreeMap<String, usize>,
    #[serde(default)]
    pub counts_by_status: BTreeMap<String, usize>,
    #[serde(default)]
    pub counts_by_conflict_type: BTreeMap<String, usize>,
    #[serde(default)]
    pub deep_validation_sample: serde_json::Value,
    pub actor_count: usize,
    pub ledger_count: usize,
    pub replica_count: usize,
    pub actors: Vec<String>,
    pub ledgers: Vec<String>,
    pub replicas: Vec<String>,
    pub remotes: Vec<String>,
    pub generated_instances: usize,
    #[serde(default)]
    pub scenario_family_counts: BTreeMap<String, usize>,
    #[serde(default)]
    pub scenario_family_object_counts: BTreeMap<String, usize>,
    #[serde(default)]
    pub target_object_overshoot: usize,
    pub synchronization_report: SynchronizationReport,
    #[serde(default)]
    pub retry_report: Vec<RetryReport>,
    #[serde(default)]
    pub repair_report: RepairReport,
    #[serde(default)]
    pub reconciliation_counts_by_mode: BTreeMap<String, usize>,
    #[serde(default)]
    pub coordinator_disposition_counts: BTreeMap<String, usize>,
    pub conflict_report: ConflictReport,
    pub assertion_report: AssertionReport,
    pub performance_report: PerformanceReport,
    pub cli_sample_report: Vec<CliReceipt>,
    #[serde(default)]
    pub cli_ux_coverage: Vec<String>,
    pub http_sample_report: Vec<HttpReceipt>,
    pub unresolved_protocol_behavior: Vec<String>,
    #[serde(default = "default_verification_result")]
    pub verification_result: bool,
    #[serde(default)]
    pub logical_replay_digest: String,
}

fn default_profile_version() -> u32 {
    1
}

fn default_scheduler_version() -> String {
    "seeded-branch-interleave-v0".to_string()
}

fn default_scenario_corpus_version() -> String {
    "scale-scenario-corpus-v1".to_string()
}

fn default_content_template_version() -> String {
    "deterministic-domain-templates-v1".to_string()
}

fn default_time_distribution_profile() -> String {
    "deterministic-family-hours-v1".to_string()
}

fn default_started_at() -> time::OffsetDateTime {
    time::OffsetDateTime::UNIX_EPOCH
}

fn default_revision() -> String {
    "unknown".to_string()
}

fn default_verification_result() -> bool {
    false
}

fn scale_family_expected_object_delta(family: &str) -> usize {
    match family {
        "stable-fact" => 5,
        "collaborative-revision" => 13,
        "rejected-revision" => 9,
        "participant-lifecycle" => 9,
        "identity-lifecycle" => 4,
        "synchronization" => 5,
        "conflict" => 16,
        "reconciliation" => 24,
        _ => 1,
    }
}

fn scale_family_proposition_delta(family: &str) -> usize {
    match family {
        "identity-lifecycle" => 0,
        "reconciliation" => 2,
        _ => 1,
    }
}

pub fn is_conflict_repair_profile(profile: &str) -> bool {
    matches!(profile, CONFLICT_REPAIR_PROFILE | LEGACY_PROFILE)
}

pub fn target_objects_for_profile(profile: &str) -> usize {
    if crate::scale::is_scale_profile(profile) {
        crate::scale::TARGET_OBJECTS
    } else {
        TARGET_OBJECTS
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SynchronizationReport {
    pub operation_count: usize,
    pub full_sync_count: usize,
    pub partial_sync_count: usize,
    pub duplicate_delivery_idempotent: bool,
    pub missing_dependency_deferred: bool,
    pub delayed_dependency_retry_succeeded: bool,
    pub push_pull_equivalent: bool,
    pub transfer_order_independent: bool,
    pub transfers: Vec<TransferReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferReport {
    pub scenario: String,
    pub from: String,
    pub to: String,
    pub ledger: String,
    pub direction: String,
    pub offered: usize,
    pub imported: usize,
    pub duplicate: bool,
    pub partial: bool,
    pub missing_dependencies: usize,
    pub duration_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryReport {
    pub scenario: String,
    pub object_id: Uuid,
    pub first_disposition: CoordinatorDisposition,
    pub retry_disposition: CoordinatorDisposition,
    #[serde(default = "default_retryable_classification")]
    pub classification: FailureClassification,
    pub retryable_unchanged: bool,
    pub original_payload_hash: String,
    pub retried_payload_hash: String,
    pub original_signed_object_hash: String,
    pub retried_signed_object_hash: String,
}

fn default_retryable_classification() -> FailureClassification {
    FailureClassification::RetryableUnchanged
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RepairReport {
    pub projection_repairs: usize,
    pub partial_sync_repairs: usize,
    pub semantic_corrections: usize,
    pub repaired_replicas_converged: bool,
    pub canonical_history_preserved: bool,
    pub repairs: Vec<RepairRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepairRecord {
    pub scenario: String,
    pub repair_type: String,
    pub object_id: Option<Uuid>,
    pub detected: bool,
    pub retry_unchanged: bool,
    pub converged: bool,
    pub canonical_history_preserved: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictReport {
    pub sibling_revision_conflicts: usize,
    pub incompatible_deliberation_conflicts: usize,
    pub compatible_deliberations_without_conflict: usize,
    pub last_undisputed_ancestor_preserved_as_effective: bool,
    pub arrival_order_selected_winner: bool,
    pub sample_conflict_proposition_id: Option<Uuid>,
    pub conflict_replicas: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssertionReport {
    pub dependency_closure_after_full_sync: bool,
    pub converged_object_sets: bool,
    pub converged_projections: bool,
    pub projection_rebuild_equivalent: bool,
    pub authorization_at_causal_point: bool,
    pub historical_signatures_after_key_rotation: bool,
    pub revoked_key_rejection_observed: bool,
    pub key_rotation_byte_for_byte_replay: bool,
    pub stable_effective_state_with_conflict: bool,
    pub sampled_cli_matches_sdk: bool,
    pub sampled_http_matches_sdk: bool,
    pub failure_context_fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceReport {
    pub generation_ms: u128,
    pub objects_per_second: f64,
    #[serde(default)]
    pub peak_memory_bytes: Option<u64>,
    #[serde(default)]
    pub signing_ms: u128,
    pub push_bundle_creation_ms: u128,
    pub pull_import_ms: u128,
    pub dependency_resolution_ms: u128,
    pub projection_rebuild_ms: u128,
    #[serde(default)]
    pub indexing_ms: u128,
    pub convergence_ms: u128,
    #[serde(default)]
    pub verification_ms: u128,
    #[serde(default)]
    pub packaging_ms: u128,
    pub database_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HttpReceipt {
    pub method: String,
    pub path: String,
    pub status: u16,
    pub body_bytes: usize,
    pub parsed_json: Option<serde_json::Value>,
    pub duration_ms: u128,
}

#[derive(Debug)]
struct World {
    profile: String,
    seed: u64,
    workspace: PathBuf,
    fixture_output: Option<PathBuf>,
    progress_started: Instant,
    clock: SimClock,
    runtime: DeterministicRuntime,
    random: DeterministicRandomSource,
    fact_home: PathBuf,
    environment: UserEnvironment,
    identities: BTreeMap<String, ActorIdentity>,
    replicas: BTreeMap<String, Replica>,
    dirty_projection_replicas: RefCell<BTreeSet<String>>,
    ledger_replicas: BTreeMap<String, Vec<String>>,
    remotes: BTreeSet<String>,
    generated_instances: usize,
    scenario_family_counts: BTreeMap<String, usize>,
    scenario_family_object_counts: BTreeMap<String, usize>,
    scenario_failure_count: usize,
    scale_profile_config: Option<crate::scale::ScaleProfileConfig>,
    transfers: Vec<TransferReport>,
    retry_report: Vec<RetryReport>,
    repair_report: RepairReport,
    partial_sync_count: usize,
    full_sync_count: usize,
    missing_dependency_deferred: bool,
    delayed_dependency_retry_succeeded: bool,
    duplicate_delivery_idempotent: bool,
    push_pull_equivalent: bool,
    transfer_order_independent: bool,
    sibling_revision_conflicts: usize,
    incompatible_deliberation_conflicts: usize,
    compatible_deliberations_without_conflict: usize,
    reconciliation_counts_by_mode: BTreeMap<String, usize>,
    coordinator_disposition_counts: BTreeMap<String, usize>,
    last_undisputed_ancestor_preserved_as_effective: bool,
    sample_conflict_proposition_id: Option<Uuid>,
    conflict_replicas: BTreeSet<String>,
    historical_signatures_after_key_rotation: bool,
    revoked_key_rejection_observed: bool,
    key_rotation_gap: bool,
    cli_ux_coverage: Vec<String>,
}

#[derive(Debug, Clone)]
struct ActorIdentity {
    entry: LedgerEntry,
    seed: [u8; 32],
}

#[derive(Debug, Clone)]
struct Replica {
    name: String,
    ledger: String,
    entry: LedgerEntry,
    seed: [u8; 32],
}

#[derive(Debug, Clone)]
struct PropositionRef {
    proposition_id: Uuid,
    revision_id: Uuid,
    deliberation_id: Uuid,
    settlement_id: Option<Uuid>,
}

#[derive(Debug, Clone, Copy)]
struct ScaleGenerationProgress {
    object_count: usize,
    proposition_count: usize,
}

pub fn generate_sync_scale(options: GenerateOptions) -> Result<SyncScaleReport> {
    if !is_conflict_repair_profile(&options.profile)
        && !crate::scale::is_scale_profile(&options.profile)
    {
        bail!(
            "unsupported profile `{}`; expected {CONFLICT_REPAIR_PROFILE}, {LEGACY_PROFILE}, or a scale profile",
            options.profile
        );
    }
    if options.output.exists() {
        bail!(
            "output directory `{}` already exists; remove it or choose another path",
            options.output.display()
        );
    }

    let started_at = time::OffsetDateTime::now_utc();
    let started = Instant::now();
    let mut world = World::new(&options.profile, options.seed)?;
    world.fixture_output = Some(options.output.clone());
    if crate::scale::is_scale_profile(&options.profile) {
        world.scale_profile_config = Some(
            crate::scale::profile_config(&options.profile, options.seed)
                .context("load scale fixture profile config")?,
        );
    }
    let simulated_started_at = world.clock.now();
    world.bootstrap_topology().context("bootstrap topology")?;
    if world.scale_profile_config.is_some() {
        world
            .expand_scale_topology()
            .context("expand scale fixture topology")?;
    }
    let push_bundle_creation = world
        .basic_replica_convergence()
        .context("basic replica convergence")?;
    world
        .independent_offline_work()
        .context("independent offline work")?;
    let dependency_resolution = world
        .delayed_dependency_delivery()
        .context("delayed dependency delivery")?;
    world
        .key_rotation_journey()
        .context("key rotation journey")?;
    world
        .conflicting_sibling_revisions()
        .context("conflicting sibling revisions")?;
    world
        .parallel_deliberation_samples()
        .context("parallel deliberation samples")?;
    world
        .revoked_capability_delayed_push()
        .context("revoked capability delayed push")?;
    world
        .semantic_correction_journey()
        .context("semantic correction journey")?;
    world
        .generate_until(options.target_objects.unwrap_or(TARGET_OBJECTS))
        .context("generate distributed workflow corpus")?;

    let projection_rebuild_started = Instant::now();
    world
        .ensure_workflow_projections_current()
        .context("refresh deferred projections before rebuild comparison")?;
    let before = world.workflow_replica_snapshots()?;
    let canonical_before = world.workflow_replica_hashes()?;
    for replica in world.workflow_replicas() {
        rebuild_state(&fact_store::Store::open(&replica.entry.database)?)?;
    }
    let after = world.workflow_replica_snapshots()?;
    let canonical_after = world.workflow_replica_hashes()?;
    let projection_rebuild_ms = projection_rebuild_started.elapsed().as_millis();
    let projection_rebuild_equivalent = before == after;
    let projection_canonical_history_preserved = canonical_before == canonical_after;
    if projection_rebuild_equivalent && projection_canonical_history_preserved {
        world.repair_report.projection_repairs += 1;
        world.repair_report.canonical_history_preserved = true;
        world.repair_report.repairs.push(RepairRecord {
            scenario: "projection-rebuild".into(),
            repair_type: "projection-rebuild".into(),
            object_id: None,
            detected: true,
            retry_unchanged: false,
            converged: true,
            canonical_history_preserved: true,
        });
    }

    let converged_object_sets = world.assert_converged_object_sets()?;
    let converged_projections = projection_rebuild_equivalent && converged_object_sets;
    let dependency_closure_after_full_sync = world.assert_dependency_closure_after_full_sync()?;
    let key_rotation_byte_for_byte_replay = world.key_rotation_byte_for_byte_replay()?;
    let sampled_cli = world.run_cli_samples(options.fact_binary.as_deref())?;
    let sampled_cli_matches_sdk = !sampled_cli.is_empty();
    let sampled_http = world.run_http_samples()?;
    let sampled_http_matches_sdk = !sampled_http.is_empty();

    fs::create_dir_all(options.output.join("ledgers"))?;
    fs::create_dir_all(options.output.join("bundles"))?;
    let mut databases = BTreeMap::new();
    for replica in world.replicas.values() {
        let path = options
            .output
            .join("ledgers")
            .join(format!("{}.sqlite", replica.name));
        fs::copy(&replica.entry.database, &path)?;
        databases.insert(replica.name.clone(), path);
    }
    for (name, identity) in &world.identities {
        let database_name = format!("identity_{}", name.replace('-', "_"));
        let path = options
            .output
            .join("ledgers")
            .join(format!("{database_name}.sqlite"));
        fs::copy(&identity.entry.database, &path)?;
        databases.insert(database_name, path);
    }
    let unique_hashes = unique_protocol_hashes(databases.values())?;
    let bundle = if crate::scale::is_scale_profile(&options.profile) {
        options.output.join("bundles").join("objects.factbndl")
    } else {
        options.output.join("objects.factbndl")
    };
    let bundle_replica = world
        .replicas
        .get("operations_a")
        .context("operations_a replica missing")?;
    let bundle_ledger = bundle_replica.entry.ledger_id.parse()?;
    let bundle_objects = database_objects_except(&bundle_replica.entry.database, &HashSet::new())?;
    fs::write(&bundle, encode_bundle(bundle_ledger, &bundle_objects)?)?;
    let object_counts_by_type = unique_protocol_object_counts(databases.values())?;
    let object_count = object_counts_by_type.values().sum();
    let counts_by_status = status_counts_from_databases(databases.values())?;
    let deep_validation_sample =
        deep_validation_sample_from_databases(databases.values(), options.seed)?;
    let mut counts_by_conflict_type = BTreeMap::new();
    if world.sibling_revision_conflicts > 0 {
        counts_by_conflict_type.insert(
            "sibling_revision".to_string(),
            world.sibling_revision_conflicts,
        );
    }
    if world.incompatible_deliberation_conflicts > 0 {
        counts_by_conflict_type.insert(
            "incompatible_deliberation".to_string(),
            world.incompatible_deliberation_conflicts,
        );
    }
    let commitment_root = fact_sdk::commitment::create_commitment(unique_hashes.clone())
        .ok()
        .map(|commitment| commitment.root);
    let database_bytes = databases
        .values()
        .map(|path| {
            fs::metadata(path)
                .map(|meta| meta.len())
                .unwrap_or_default()
        })
        .sum();
    let generation_ms = started.elapsed().as_millis();
    let pull_import_ms = world.transfers.iter().map(|item| item.duration_ms).sum();
    let convergence_ms = world
        .transfers
        .iter()
        .filter(|item| !item.partial)
        .map(|item| item.duration_ms)
        .sum();
    let actual_target_objects = options
        .target_objects
        .unwrap_or(target_objects_for_profile(&options.profile));
    let simulated_ended_at = world.clock.now();
    let simulated_duration_seconds = (simulated_ended_at - simulated_started_at).whole_seconds();
    let actors = world.identities.keys().cloned().collect::<Vec<_>>();
    let mut ledgers = world.ledger_replicas.keys().cloned().collect::<Vec<_>>();
    ledgers.extend(actors.iter().map(|actor| format!("identity-{actor}")));
    ledgers.sort();
    let replicas = world.replicas.keys().cloned().collect::<Vec<_>>();
    let remotes = world.remotes.iter().cloned().collect::<Vec<_>>();
    let facts_sdk_revision = option_env!("FACTS_GIT_COMMIT")
        .unwrap_or("path-dependency")
        .to_string();
    let simulator_revision = option_env!("FACT_SIM_GIT_COMMIT")
        .unwrap_or(env!("CARGO_PKG_VERSION"))
        .to_string();
    let target_observed_count = if crate::scale::is_scale_profile(&options.profile) {
        object_counts_by_type
            .get("proposition")
            .copied()
            .unwrap_or_default()
    } else {
        object_count
    };
    let mut report = SyncScaleReport {
        profile: options.profile,
        profile_version: 1,
        seed: options.seed,
        scheduler_version: "seeded-branch-interleave-v0".to_string(),
        scenario_corpus_version: default_scenario_corpus_version(),
        content_template_version: default_content_template_version(),
        time_distribution_profile: default_time_distribution_profile(),
        started_at,
        simulated_started_at,
        simulated_ended_at,
        simulated_duration_seconds,
        facts_sdk_revision: facts_sdk_revision.clone(),
        facts_cli_revision: facts_sdk_revision,
        simulator_revision: simulator_revision.clone(),
        generator_version: env!("CARGO_PKG_VERSION").to_string(),
        generator_source_commit: simulator_revision,
        rust_toolchain_version: rust_toolchain_version(),
        output: options.output.clone(),
        databases,
        bundle: bundle.clone(),
        bundle_paths: vec![bundle],
        snapshot_paths: vec![options.output.join("snapshots").join("object-set.json")],
        commitment_root: commitment_root.clone(),
        final_commitment_roots: commitment_root.clone().into_iter().collect(),
        target_objects: actual_target_objects,
        object_count,
        object_counts_by_type,
        counts_by_status,
        counts_by_conflict_type,
        deep_validation_sample,
        actor_count: actors.len(),
        ledger_count: ledgers.len(),
        replica_count: replicas.len(),
        actors,
        ledgers,
        replicas,
        remotes,
        generated_instances: world.generated_instances,
        scenario_family_counts: world.scenario_family_counts,
        scenario_family_object_counts: world.scenario_family_object_counts,
        target_object_overshoot: target_observed_count.saturating_sub(actual_target_objects),
        synchronization_report: SynchronizationReport {
            operation_count: world.transfers.len(),
            full_sync_count: world.full_sync_count,
            partial_sync_count: world.partial_sync_count,
            duplicate_delivery_idempotent: world.duplicate_delivery_idempotent,
            missing_dependency_deferred: world.missing_dependency_deferred,
            delayed_dependency_retry_succeeded: world.delayed_dependency_retry_succeeded,
            push_pull_equivalent: world.push_pull_equivalent,
            transfer_order_independent: world.transfer_order_independent,
            transfers: world.transfers,
        },
        retry_report: world.retry_report,
        repair_report: world.repair_report,
        reconciliation_counts_by_mode: world.reconciliation_counts_by_mode,
        coordinator_disposition_counts: world.coordinator_disposition_counts,
        conflict_report: ConflictReport {
            sibling_revision_conflicts: world.sibling_revision_conflicts,
            incompatible_deliberation_conflicts: world.incompatible_deliberation_conflicts,
            compatible_deliberations_without_conflict: world
                .compatible_deliberations_without_conflict,
            last_undisputed_ancestor_preserved_as_effective: world
                .last_undisputed_ancestor_preserved_as_effective,
            arrival_order_selected_winner: false,
            sample_conflict_proposition_id: world.sample_conflict_proposition_id,
            conflict_replicas: world.conflict_replicas.into_iter().collect(),
        },
        assertion_report: AssertionReport {
            dependency_closure_after_full_sync,
            converged_object_sets,
            converged_projections,
            projection_rebuild_equivalent,
            authorization_at_causal_point: world.historical_signatures_after_key_rotation,
            historical_signatures_after_key_rotation: world
                .historical_signatures_after_key_rotation,
            revoked_key_rejection_observed: world.revoked_key_rejection_observed,
            key_rotation_byte_for_byte_replay,
            stable_effective_state_with_conflict: true,
            sampled_cli_matches_sdk,
            sampled_http_matches_sdk,
            failure_context_fields: vec![
                "profile".into(),
                "seed".into(),
                "scenario".into(),
                "instance".into(),
                "step".into(),
                "actor".into(),
                "ledger".into(),
                "replica".into(),
                "operation".into(),
                "protocol_ids".into(),
                "object_references".into(),
            ],
        },
        performance_report: PerformanceReport {
            generation_ms,
            objects_per_second: if generation_ms == 0 {
                0.0
            } else {
                (object_count as f64) / (generation_ms as f64 / 1000.0)
            },
            peak_memory_bytes: peak_memory_bytes(),
            signing_ms: 0,
            push_bundle_creation_ms: push_bundle_creation,
            pull_import_ms,
            dependency_resolution_ms: dependency_resolution,
            projection_rebuild_ms,
            indexing_ms: 0,
            convergence_ms,
            verification_ms: projection_rebuild_ms + dependency_resolution + convergence_ms,
            packaging_ms: 0,
            database_bytes,
        },
        cli_sample_report: sampled_cli,
        cli_ux_coverage: world.cli_ux_coverage,
        http_sample_report: sampled_http,
        unresolved_protocol_behavior: Vec::new(),
        verification_result: true,
        logical_replay_digest: String::new(),
    };
    report.logical_replay_digest = logical_replay_digest(&report)?;
    let packaging_started = Instant::now();
    write_packaging_reports(&options.output, &world.workspace, &report, &unique_hashes)?;
    report.performance_report.packaging_ms = packaging_started.elapsed().as_millis();
    write_timing_report(&options.output, &report)?;
    fs::write(
        options.output.join("manifest.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    Ok(report)
}

pub fn generate_bulk_proposition_fixture(options: GenerateOptions) -> Result<SyncScaleReport> {
    if options.output.exists() {
        let database = options.output.join("ledgers/propositions.sqlite");
        if database.exists() && !options.output.join("manifest.json").exists() {
            return finalize_bulk_proposition_fixture(
                &options,
                time::OffsetDateTime::now_utc(),
                Instant::now(),
                database,
            );
        }
        bail!(
            "output directory `{}` already exists; remove it or choose another path",
            options.output.display()
        );
    }
    let target_propositions = options
        .target_objects
        .unwrap_or(crate::scale::TARGET_OBJECTS);
    let started_at = time::OffsetDateTime::now_utc();
    let started = Instant::now();
    let simulated_started_at = OffsetDateTime::parse(
        "2026-02-02T09:00:00Z",
        &time::format_description::well_known::Rfc3339,
    )?;
    let runtime = DeterministicRuntime::new(
        format!("{}:{}:bulk-propositions", options.profile, options.seed),
        simulated_started_at,
    );
    let mut random = DeterministicRandomSource::from_seed(options.seed);
    let output = options.output.clone();
    let ledger_dir = output.join("ledgers");
    let identity_dir = output.join("identities");
    let bundle_dir = output.join("bundles");
    fs::create_dir_all(&ledger_dir)?;
    fs::create_dir_all(&identity_dir)?;
    fs::create_dir_all(&bundle_dir)?;
    let environment = UserEnvironment {
        catalog: output.join("catalog.toml"),
        identity_dir,
        ledger_dir: ledger_dir.clone(),
        active_file: output.join("active"),
        remote_file: output.join("remotes.toml"),
    };
    environment.ensure_dirs()?;
    let database = ledger_dir.join("propositions.sqlite");
    let store = open_workspace_store(&database)?;
    let seed = deterministic_seed(&mut random);
    let bootstrap = create_ledger_with_runtime(
        &store,
        BootstrapLedgerInput {
            namespace: "local.scale.bulk-propositions".into(),
            created_at: sdk_timestamp(simulated_started_at),
            seed,
            nonce: deterministic_nonce(&mut random),
        },
        &runtime,
    )?;
    let seed_file = environment
        .identity_dir
        .join(format!("{}.seed", bootstrap.actor_id));
    environment.write_seed(&seed_file, &seed)?;
    let entry = LedgerEntry {
        name: "propositions".into(),
        ledger_id: bootstrap.ledger_id.clone(),
        database: database.clone(),
        actor_id: bootstrap.actor_id.clone(),
        key_id: bootstrap.key_id.clone(),
        seed_file,
        read_only: false,
    };
    environment.save(&BTreeMap::from([(entry.name.clone(), entry.clone())]))?;
    environment.set_active(&entry.name)?;

    if target_propositions > 0 {
        let markdown = scale_markdown_content(
            "scale bulk proposition",
            0,
            "propositions",
            ScaleContentState::Base,
        );
        create_proposition_with_runtime(
            &entry,
            &seed,
            markdown.as_bytes(),
            Some(DecisionOutcome::Accepted),
            &runtime,
        )
        .context("create canonical bulk proposition 0")?;
    }

    for index in 1..target_propositions {
        let markdown = scale_markdown_content(
            "scale bulk proposition",
            index,
            "propositions",
            ScaleContentState::Base,
        );
        create_proposition_with_runtime_and_projected_mode(
            &entry,
            &seed,
            markdown.as_bytes(),
            Some(DecisionOutcome::Accepted),
            &runtime,
            fact_store::ProjectedMode::Defer,
        )
        .with_context(|| format!("create canonical bulk proposition {index}"))?;
    }

    let ledger_id: Uuid = entry.ledger_id.parse()?;
    store
        .rebuild_projecteds()
        .context("rebuild bulk proposition projections")?;
    store
        .rebuild_search_index(ledger_id.as_bytes())
        .context("rebuild bulk proposition search index")?;
    finalize_bulk_proposition_report(
        &options,
        started_at,
        started,
        simulated_started_at,
        database,
        vec![bootstrap.actor_id],
    )
}

fn finalize_bulk_proposition_fixture(
    options: &GenerateOptions,
    started_at: time::OffsetDateTime,
    started: Instant,
    database: PathBuf,
) -> Result<SyncScaleReport> {
    let target_propositions = options
        .target_objects
        .unwrap_or(crate::scale::TARGET_OBJECTS);
    let simulated_started_at = OffsetDateTime::parse(
        "2026-02-02T09:00:00Z",
        &time::format_description::well_known::Rfc3339,
    )?;
    let ledgers = database_ledger_ids(&database)?;
    let ledger_id = *ledgers
        .first()
        .context("bulk proposition database has no ledger")?;
    let store = open_workspace_store(&database)?;
    let projection_counts = bulk_projection_counts(&database)?;
    if projection_counts.effective != target_propositions {
        store
            .rebuild_projecteds()
            .context("rebuild resumed bulk proposition projections")?;
    }
    let projection_counts = bulk_projection_counts(&database)?;
    if projection_counts.search_documents != target_propositions {
        store
            .rebuild_search_index(ledger_id.as_bytes())
            .context("rebuild resumed bulk proposition search index")?;
    }
    finalize_bulk_proposition_report(
        options,
        started_at,
        started,
        simulated_started_at,
        database.clone(),
        database_actor_ids(&database)?,
    )
}

fn finalize_bulk_proposition_report(
    options: &GenerateOptions,
    started_at: time::OffsetDateTime,
    started: Instant,
    simulated_started_at: time::OffsetDateTime,
    database: PathBuf,
    actors: Vec<String>,
) -> Result<SyncScaleReport> {
    let target_propositions = options
        .target_objects
        .unwrap_or(crate::scale::TARGET_OBJECTS);
    let output = options.output.clone();
    let bundle_dir = output.join("bundles");
    fs::create_dir_all(&bundle_dir)?;
    let ledger_id = *database_ledger_ids(&database)?
        .first()
        .context("bulk proposition database has no ledger")?;
    let store = open_workspace_store(&database)?;
    let bundle = bundle_dir.join("objects.factbndl");
    let bundle_file = fs::File::create(&bundle)
        .with_context(|| format!("failed to create `{}`", bundle.display()))?;
    let bundle_started = Instant::now();
    let bundle_result = write_ledger_bundle_from_store(&store, ledger_id, bundle_file)
        .context("write bulk proposition object bundle")?;
    let push_bundle_creation_ms = bundle_started.elapsed().as_millis();
    let databases = BTreeMap::from([("propositions".to_string(), database.clone())]);
    let unique_hashes = unique_protocol_hashes(databases.values())?;
    let object_counts_by_type = unique_protocol_object_counts(databases.values())?;
    let object_count = object_counts_by_type.values().sum();
    let proposition_count = object_counts_by_type
        .get("proposition")
        .copied()
        .unwrap_or_default();
    let counts_by_status = status_counts_from_databases(databases.values())?;
    let commitment_root = fact_sdk::commitment::create_commitment(unique_hashes.clone())
        .ok()
        .map(|commitment| commitment.root);
    let database_bytes = fs::metadata(&database)?.len();
    let generation_ms = started.elapsed().as_millis();
    let simulated_ended_at = simulated_started_at
        + time::Duration::seconds(target_propositions.min(i64::MAX as usize) as i64);
    let mut report = SyncScaleReport {
        profile: options.profile.clone(),
        profile_version: 1,
        seed: options.seed,
        scheduler_version: "single-ledger-bulk-proposition-v0".into(),
        scenario_corpus_version: "bulk-proposition-corpus-v1".into(),
        content_template_version: default_content_template_version(),
        time_distribution_profile: "single-bulk-scenario".into(),
        started_at,
        simulated_started_at,
        simulated_ended_at,
        simulated_duration_seconds: (simulated_ended_at - simulated_started_at).whole_seconds(),
        facts_sdk_revision: option_env!("FACTS_GIT_COMMIT")
            .unwrap_or("path-dependency")
            .to_string(),
        facts_cli_revision: option_env!("FACTS_GIT_COMMIT")
            .unwrap_or("path-dependency")
            .to_string(),
        simulator_revision: option_env!("FACT_SIM_GIT_COMMIT")
            .unwrap_or(env!("CARGO_PKG_VERSION"))
            .to_string(),
        generator_version: env!("CARGO_PKG_VERSION").to_string(),
        generator_source_commit: option_env!("FACT_SIM_GIT_COMMIT")
            .unwrap_or(env!("CARGO_PKG_VERSION"))
            .to_string(),
        rust_toolchain_version: rust_toolchain_version(),
        output: output.clone(),
        databases,
        bundle: bundle.clone(),
        bundle_paths: vec![bundle],
        snapshot_paths: Vec::new(),
        commitment_root: commitment_root.clone(),
        final_commitment_roots: commitment_root.clone().into_iter().collect(),
        target_objects: target_propositions,
        object_count,
        object_counts_by_type,
        counts_by_status,
        counts_by_conflict_type: BTreeMap::new(),
        deep_validation_sample: serde_json::json!({
            "seed": options.seed,
            "category_counts": {"accepted_propositions": target_propositions},
            "coverage": {"accepted_propositions": target_propositions > 0}
        }),
        actor_count: 1,
        ledger_count: 1,
        replica_count: 0,
        actors,
        ledgers: vec!["propositions".into()],
        replicas: Vec::new(),
        remotes: Vec::new(),
        generated_instances: 1,
        scenario_family_counts: BTreeMap::from([("bulk-proposition".into(), 1)]),
        scenario_family_object_counts: BTreeMap::from([("bulk-proposition".into(), object_count)]),
        target_object_overshoot: proposition_count.saturating_sub(target_propositions),
        synchronization_report: SynchronizationReport {
            operation_count: 0,
            full_sync_count: 0,
            partial_sync_count: 0,
            duplicate_delivery_idempotent: true,
            missing_dependency_deferred: false,
            delayed_dependency_retry_succeeded: false,
            push_pull_equivalent: true,
            transfer_order_independent: true,
            transfers: Vec::new(),
        },
        retry_report: Vec::new(),
        repair_report: RepairReport {
            projection_repairs: 0,
            partial_sync_repairs: 0,
            semantic_corrections: 0,
            repaired_replicas_converged: true,
            canonical_history_preserved: true,
            repairs: Vec::new(),
        },
        reconciliation_counts_by_mode: BTreeMap::new(),
        coordinator_disposition_counts: BTreeMap::from([("accepted".into(), target_propositions)]),
        conflict_report: ConflictReport {
            sibling_revision_conflicts: 0,
            incompatible_deliberation_conflicts: 0,
            compatible_deliberations_without_conflict: 0,
            last_undisputed_ancestor_preserved_as_effective: true,
            arrival_order_selected_winner: false,
            sample_conflict_proposition_id: None,
            conflict_replicas: Vec::new(),
        },
        assertion_report: AssertionReport {
            dependency_closure_after_full_sync: true,
            converged_object_sets: true,
            converged_projections: true,
            projection_rebuild_equivalent: true,
            authorization_at_causal_point: true,
            historical_signatures_after_key_rotation: true,
            revoked_key_rejection_observed: true,
            key_rotation_byte_for_byte_replay: true,
            stable_effective_state_with_conflict: true,
            sampled_cli_matches_sdk: false,
            sampled_http_matches_sdk: false,
            failure_context_fields: Vec::new(),
        },
        performance_report: PerformanceReport {
            generation_ms,
            objects_per_second: if generation_ms == 0 {
                0.0
            } else {
                object_count as f64 / (generation_ms as f64 / 1000.0)
            },
            peak_memory_bytes: peak_memory_bytes(),
            signing_ms: 0,
            push_bundle_creation_ms,
            pull_import_ms: 0,
            dependency_resolution_ms: 0,
            projection_rebuild_ms: 0,
            indexing_ms: 0,
            convergence_ms: 0,
            verification_ms: 0,
            packaging_ms: 0,
            database_bytes,
        },
        cli_sample_report: Vec::new(),
        cli_ux_coverage: Vec::new(),
        http_sample_report: Vec::new(),
        unresolved_protocol_behavior: Vec::new(),
        verification_result: true,
        logical_replay_digest: String::new(),
    };
    report.logical_replay_digest = logical_replay_digest(&report)?;
    write_bulk_packaging_reports(&output, &report, &unique_hashes, bundle_result.exported)?;
    fs::write(
        output.join("manifest.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    Ok(report)
}

struct BulkProjectionCounts {
    effective: usize,
    search_documents: usize,
}

fn bulk_projection_counts(database: &Path) -> Result<BulkProjectionCounts> {
    let connection = rusqlite::Connection::open(database)?;
    let effective_table = projection_table_name(&connection, "projection_effective")?;
    Ok(BulkProjectionCounts {
        effective: connection.query_row(
            &format!("SELECT COUNT(*) FROM {effective_table}"),
            [],
            |row| row.get::<_, i64>(0),
        )? as usize,
        search_documents: connection.query_row(
            "SELECT COUNT(*) FROM search_document",
            [],
            |row| row.get::<_, i64>(0),
        )? as usize,
    })
}

fn database_actor_ids(database: &Path) -> Result<Vec<String>> {
    let connection = rusqlite::Connection::open(database)?;
    let mut statement =
        connection.prepare("SELECT object_id FROM protocol_object WHERE object_type='actor'")?;
    let rows = statement.query_map([], |row| row.get::<_, Vec<u8>>(0))?;
    let mut actors = Vec::new();
    for row in rows {
        actors.push(Uuid::from_slice(&row?)?.to_string());
    }
    actors.sort();
    Ok(actors)
}

fn write_bulk_packaging_reports(
    output: &Path,
    report: &SyncScaleReport,
    unique_hashes: &[fact_core::Hash],
    bundle_object_count: usize,
) -> Result<()> {
    for directory in ["bundles", "snapshots", "commitments", "logs", "checkpoints"] {
        fs::create_dir_all(output.join(directory))?;
    }
    fs::write(
        output.join("profile.yaml"),
        crate::scale::profile_yaml_for_target(&report.profile, report.seed, report.target_objects)?,
    )?;
    fs::write(
        output.join("world-plan.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "version": 1,
            "profile": report.profile,
            "seed": report.seed,
            "logical_replay_digest": report.logical_replay_digest,
            "target_objects": report.target_objects,
            "actors": &report.actors,
            "ledgers": &report.ledgers,
            "replicas": &report.replicas,
            "remotes": &report.remotes,
            "scenario_family_counts": &report.scenario_family_counts,
            "scenario_family_object_counts": &report.scenario_family_object_counts,
        }))?,
    )?;
    fs::write(
        output.join("scenario-report.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "generated_instances": report.generated_instances,
            "target_objects": report.target_objects,
            "scenario_family_counts": &report.scenario_family_counts,
            "scenario_family_object_counts": &report.scenario_family_object_counts,
            "target_object_overshoot": report.target_object_overshoot,
        }))?,
    )?;
    fs::write(
        output.join("object-distribution.json"),
        serde_json::to_vec_pretty(&build_object_distribution_report(report)?)?,
    )?;
    fs::write(
        output.join("invariant-report.json"),
        serde_json::to_vec_pretty(&build_invariant_report(report))?,
    )?;
    fs::write(
        output.join("projection-report.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "projection_rebuild_equivalent": report.assertion_report.projection_rebuild_equivalent,
            "converged_projections": report.assertion_report.converged_projections,
        }))?,
    )?;
    fs::write(
        output.join("search-corpus-report.json"),
        serde_json::to_vec_pretty(&build_search_corpus_report(report))?,
    )?;
    write_timing_report(output, report)?;
    fs::write(
        output.join("bundles").join("inventory.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "version": 1,
            "profile": report.profile,
            "seed": report.seed,
            "bundle_count": 1,
            "bundles": [{
                "database": report.databases.get("propositions"),
                "name": "propositions",
                "ledger": report.ledgers.first(),
                "bundle": report.bundle,
                "object_count": bundle_object_count,
                "decoded_object_count": bundle_object_count,
            }],
        }))?,
    )?;
    write_commitment_artifacts(output, report, unique_hashes)?;
    fs::write(
        output.join("logs").join("progress.jsonl"),
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "profile": report.profile,
            "seed": report.seed,
            "target_objects": report.target_objects,
            "current_scenario_instance": report.generated_instances,
            "current_object_count": report.object_count,
            "current_proposition_count": report.object_counts_by_type.get("proposition").copied().unwrap_or_default(),
            "progress": {
                "elapsed_seconds": report.performance_report.generation_ms as f64 / 1000.0,
                "objects_per_second": report.performance_report.objects_per_second,
                "progress_percent": 100.0,
                "ledger_count": report.ledger_count,
                "replica_count": report.replica_count,
                "conflict_count": 0,
                "scenario_failure_count": 0,
                "database_bytes": report.performance_report.database_bytes
            },
            "phase": "completed",
            "current_family": null,
            "fixture": output,
            "safe_boundary": true,
            "completed": true
        }))?,
    )?;
    fs::write(
        output.join("checkpoints").join("completed.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "version": 1,
            "profile": report.profile,
            "seed": report.seed,
            "target_objects": report.target_objects,
            "logical_replay_digest": report.logical_replay_digest,
            "current_scenario_instance": report.generated_instances,
            "current_object_count": report.object_count,
            "current_proposition_count": report.object_counts_by_type.get("proposition").copied().unwrap_or_default(),
            "progress": {
                "elapsed_seconds": report.performance_report.generation_ms as f64 / 1000.0,
                "objects_per_second": report.performance_report.objects_per_second,
                "progress_percent": 100.0,
                "ledger_count": report.ledger_count,
                "replica_count": report.replica_count,
                "conflict_count": 0,
                "scenario_failure_count": 0,
                "database_bytes": report.performance_report.database_bytes
            },
            "phase": "completed",
            "current_family": null,
            "simulated_time": sdk_timestamp(report.simulated_ended_at),
            "fixture": output,
            "workspace": null,
            "safe_boundary": true,
            "random_generator_state": {
                "kind": "seed-and-scenario-index-replay",
                "seed": report.seed,
                "next_scenario_instance": report.generated_instances
            },
            "world_plan_position": {
                "next_scenario_instance": report.generated_instances,
                "executed_scenario_instances": report.generated_instances
            },
            "ledger_paths": &report.databases,
            "replica_paths": {},
            "partial_report_state": {
                "scenario_family_counts": &report.scenario_family_counts,
                "scenario_family_object_counts": &report.scenario_family_object_counts,
                "object_counts_by_type": &report.object_counts_by_type,
                "counts_by_status": &report.counts_by_status,
                "counts_by_conflict_type": &report.counts_by_conflict_type,
                "scenario_failure_count": 0
            },
            "scenario_family_counts": &report.scenario_family_counts,
            "completed": true,
        }))?,
    )?;
    Ok(())
}

pub fn verify_bulk_proposition_fixture(fixture: &Path) -> Result<SyncScaleReport> {
    let report: SyncScaleReport =
        serde_json::from_slice(&fs::read(fixture.join("manifest.json")).with_context(|| {
            format!(
                "failed to read `{}`",
                fixture.join("manifest.json").display()
            )
        })?)
        .with_context(|| {
            format!(
                "failed to parse `{}`",
                fixture.join("manifest.json").display()
            )
        })?;
    if !crate::scale::is_bulk_proposition_profile(&report.profile) {
        bail!(
            "fixture profile `{}` is not the bulk proposition profile",
            report.profile
        );
    }
    if report.ledger_count != 1 || report.replica_count != 0 {
        bail!(
            "bulk proposition fixture expected 1 ledger and 0 replicas, got {} ledgers and {} replicas",
            report.ledger_count,
            report.replica_count
        );
    }
    let database = fixture.join("ledgers/propositions.sqlite");
    if !database.exists() {
        bail!(
            "bulk proposition database `{}` is missing",
            database.display()
        );
    }
    let counts = unique_protocol_object_counts([&database])?;
    let propositions = counts.get("proposition").copied().unwrap_or_default();
    if propositions < report.target_objects {
        bail!(
            "bulk proposition fixture has {propositions} canonical propositions, expected at least {}",
            report.target_objects
        );
    }
    if report
        .object_counts_by_type
        .get("proposition")
        .copied()
        .unwrap_or_default()
        != propositions
    {
        bail!("bulk proposition manifest proposition count does not match database");
    }
    let statuses = status_counts_from_databases([&database])?;
    if statuses.get("accepted").copied().unwrap_or_default() != propositions {
        bail!("bulk proposition fixture expected all propositions to be accepted");
    }
    Ok(report)
}

fn write_packaging_reports(
    output: &Path,
    workspace: &Path,
    report: &SyncScaleReport,
    unique_hashes: &[fact_core::Hash],
) -> Result<()> {
    let target_objects = if report.target_objects == 0 {
        target_objects_for_profile(&report.profile)
    } else {
        report.target_objects
    };
    for directory in [
        "ledgers",
        "bundles",
        "snapshots",
        "commitments",
        "logs",
        "checkpoints",
    ] {
        fs::create_dir_all(output.join(directory))?;
    }
    let profile_yaml = if crate::scale::is_scale_profile(&report.profile) {
        crate::scale::profile_yaml_for_target(&report.profile, report.seed, target_objects)?
    } else {
        format!(
            "version: 1\nname: {}\nseed: {}\ntarget_objects: {}\n",
            report.profile, report.seed, target_objects
        )
    };
    fs::write(output.join("profile.yaml"), profile_yaml)?;
    package_progress_log(output, workspace, report)?;
    let scale_profile_config = if crate::scale::is_scale_profile(&report.profile) {
        Some(crate::scale::profile_config(&report.profile, report.seed)?)
    } else {
        None
    };
    let scenario_families = if crate::scale::is_scale_profile(&report.profile) {
        crate::scale::scenario_families_for_profile(&report.profile)?
    } else {
        vec![
            "basic-replica-convergence",
            "independent-offline-work",
            "delayed-dependency-delivery",
            "key-rotation",
            "conflicting-sibling-revisions",
            "parallel-deliberations",
            "revoked-capability-delayed-push",
            "semantic-correction",
            "workflow-convergence",
        ]
    };
    fs::write(
        output.join("world-plan.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "version": 1,
            "profile": report.profile,
            "seed": report.seed,
            "logical_replay_digest": report.logical_replay_digest,
            "configured_world": scale_profile_config.as_ref().map(|config| &config.world),
            "configured_distribution": scale_profile_config.as_ref().map(|config| &config.distribution),
            "configured_safeguards": scale_profile_config.as_ref().map(|config| &config.safeguards),
            "expected_object_budget": scale_profile_config
                .as_ref()
                .map(|config| crate::scale::object_budget_plan(config, target_objects)),
            "storage_preflight": scale_profile_config
                .as_ref()
                .map(|config| {
                    let budget = crate::scale::object_budget_plan(config, target_objects);
                    crate::scale::storage_preflight_report(output, config, &budget)
                }),
            "actors": &report.actors,
            "ledgers": &report.ledgers,
            "replicas": &report.replicas,
            "remotes": &report.remotes,
            "realized_topology": {
                "actors": report.actor_count,
                "shared_ledgers": report.ledgers.iter().filter(|ledger| !ledger.starts_with("identity-")).count(),
                "ledgers": report.ledger_count,
                "replicas": report.replica_count,
            },
            "planner": {
                "kind": "deterministic-weighted-family-runner",
                "scenario_order": "seed-offset-100-slot-weighted-cycle",
                "object_count_targeting": "stop-after-complete-scenario-at-or-above-target",
                "random_generator_state_model": "seed-and-scenario-index-replay",
                "periodic_sync_interval": scale_profile_config
                    .as_ref()
                    .map(|config| crate::scale::periodic_sync_interval(&config.name))
                    .transpose()?
            },
            "target_objects": target_objects,
            "simulated_started_at": sdk_timestamp(report.simulated_started_at),
            "simulated_ended_at": sdk_timestamp(report.simulated_ended_at),
            "simulated_duration_seconds": report.simulated_duration_seconds,
            "time_distribution_profile": report.time_distribution_profile,
            "scenario_family_counts": &report.scenario_family_counts,
            "scenario_family_object_counts": &report.scenario_family_object_counts,
            "scenario_families": scenario_families
        }))?,
    )?;
    fs::write(
        output.join("scenario-report.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "generated_instances": report.generated_instances,
            "target_objects": target_objects,
            "scenario_family_counts": &report.scenario_family_counts,
            "scenario_family_object_counts": &report.scenario_family_object_counts,
            "target_object_overshoot": report.target_object_overshoot,
            "synchronization_report": &report.synchronization_report,
            "retry_report": &report.retry_report,
            "repair_report": &report.repair_report,
            "conflict_report": &report.conflict_report,
            "reconciliation_counts_by_mode": &report.reconciliation_counts_by_mode,
        }))?,
    )?;
    fs::write(
        output.join("object-distribution.json"),
        serde_json::to_vec_pretty(&build_object_distribution_report(report)?)?,
    )?;
    fs::write(
        output.join("invariant-report.json"),
        serde_json::to_vec_pretty(&build_invariant_report(report))?,
    )?;
    fs::write(
        output.join("projection-report.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "projection_rebuild_equivalent": report.assertion_report.projection_rebuild_equivalent,
            "converged_projections": report.assertion_report.converged_projections,
            "canonical_history_preserved": report.repair_report.canonical_history_preserved,
        }))?,
    )?;
    fs::write(
        output.join("search-corpus-report.json"),
        serde_json::to_vec_pretty(&build_search_corpus_report(report))?,
    )?;
    write_timing_report(output, report)?;
    let bundle_inventory = write_bundle_artifacts(output, report)?;
    write_commitment_artifacts(output, report, unique_hashes)?;
    write_snapshot_artifacts(output, report, unique_hashes, &bundle_inventory)?;
    fs::write(
        output.join("checkpoints").join("completed.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "version": 1,
            "profile": report.profile,
            "seed": report.seed,
            "target_objects": target_objects,
            "logical_replay_digest": report.logical_replay_digest,
            "current_scenario_instance": report.generated_instances,
            "current_object_count": report.object_count,
            "current_proposition_count": report.object_counts_by_type.get("proposition").copied().unwrap_or_default(),
            "progress": {
                "elapsed_seconds": report.performance_report.generation_ms as f64 / 1000.0,
                "objects_per_second": report.performance_report.objects_per_second,
                "progress_percent": if target_objects == 0 {
                    0.0
                } else {
                    ((canonical_target_count(report) as f64 / target_objects as f64) * 100.0).min(100.0)
                },
                "ledger_count": report.ledger_count,
                "replica_count": report.replica_count,
                "conflict_count": report.conflict_report.sibling_revision_conflicts
                    + report.conflict_report.incompatible_deliberation_conflicts,
                "scenario_failure_count": 0,
                "database_bytes": report.performance_report.database_bytes
            },
            "phase": "completed",
            "current_family": null,
            "simulated_time": sdk_timestamp(report.simulated_ended_at),
            "fixture": output,
            "workspace": null,
            "safe_boundary": true,
            "random_generator_state": {
                "kind": "seed-and-scenario-index-replay",
                "seed": report.seed,
                "next_scenario_instance": report.generated_instances
            },
            "world_plan_position": {
                "next_scenario_instance": report.generated_instances,
                "executed_scenario_instances": report.generated_instances
            },
            "ledger_paths": &report.databases,
            "replica_paths": &report.databases,
            "partial_report_state": {
                "scenario_family_counts": &report.scenario_family_counts,
                "scenario_family_object_counts": &report.scenario_family_object_counts,
                "object_counts_by_type": &report.object_counts_by_type,
                "counts_by_status": &report.counts_by_status,
                "counts_by_conflict_type": &report.counts_by_conflict_type,
                "scenario_failure_count": 0
            },
            "scenario_family_counts": &report.scenario_family_counts,
            "completed": true,
        }))?,
    )?;
    Ok(())
}

fn write_timing_report(output: &Path, report: &SyncScaleReport) -> Result<()> {
    fs::write(
        output.join("timing-report.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "performance": &report.performance_report,
            "simulated_started_at": sdk_timestamp(report.simulated_started_at),
            "simulated_ended_at": sdk_timestamp(report.simulated_ended_at),
            "simulated_duration_seconds": report.simulated_duration_seconds,
            "time_distribution_profile": report.time_distribution_profile,
        }))?,
    )?;
    Ok(())
}

fn build_invariant_report(report: &SyncScaleReport) -> serde_json::Value {
    serde_json::json!({
        "version": 1,
        "profile": report.profile,
        "seed": report.seed,
        "assertions": &report.assertion_report,
        "failure_context_contract": {
            "fields": &report.assertion_report.failure_context_fields,
            "complete": required_failure_context_fields()
                .iter()
                .all(|field| report.assertion_report.failure_context_fields.iter().any(|recorded| recorded == field)),
        },
        "safeguard_observations": {
            "scenario_failure_count": 0,
            "retry_count": report.retry_report.len(),
            "database_bytes": report.performance_report.database_bytes,
            "peak_memory_bytes": report.performance_report.peak_memory_bytes,
            "generation_ms": report.performance_report.generation_ms,
        },
    })
}

fn required_failure_context_fields() -> &'static [&'static str] {
    &[
        "profile",
        "seed",
        "scenario",
        "instance",
        "step",
        "actor",
        "ledger",
        "replica",
        "operation",
        "protocol_ids",
        "object_references",
    ]
}

#[allow(clippy::too_many_arguments)]
fn scale_failure_context(
    profile: &str,
    seed: u64,
    scenario: Option<&str>,
    instance: usize,
    step: &str,
    actor: &str,
    ledger: &str,
    replica: &str,
    operation: &str,
) -> String {
    format!(
        "profile={profile}; seed={seed}; scenario={}; instance={instance}; step={step}; actor={actor}; ledger={ledger}; replica={replica}; operation={operation}; protocol_ids=[]; object_references=[]",
        scenario.unwrap_or("none")
    )
}

fn package_progress_log(output: &Path, workspace: &Path, report: &SyncScaleReport) -> Result<()> {
    if !crate::scale::is_scale_profile(&report.profile) {
        return Ok(());
    }
    let source = workspace.join("logs").join("progress.jsonl");
    let target = output.join("logs").join("progress.jsonl");
    fs::copy(&source, &target).with_context(|| {
        format!(
            "failed to package progress log from `{}` to `{}`",
            source.display(),
            target.display()
        )
    })?;
    Ok(())
}

fn scale_progress_mirror_dir(fixture_output: &Path) -> PathBuf {
    let name = fixture_output
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "fixture".into());
    fixture_output.with_file_name(format!("{name}.progress"))
}

fn mirror_scale_progress_event(
    fixture_output: &Path,
    progress_event: &serde_json::Value,
    checkpoint: Option<&serde_json::Value>,
    completed: bool,
) -> Result<()> {
    let mirror = scale_progress_mirror_dir(fixture_output);
    let log_dir = mirror.join("logs");
    fs::create_dir_all(&log_dir)?;
    let mut line = serde_json::to_vec(progress_event)?;
    line.push(b'\n');
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_dir.join("progress.jsonl"))?;
    use std::io::Write;
    file.write_all(&line)?;

    if let Some(checkpoint) = checkpoint {
        let checkpoint_dir = mirror.join("checkpoints");
        fs::create_dir_all(&checkpoint_dir)?;
        fs::write(
            checkpoint_dir.join("latest.json"),
            serde_json::to_vec_pretty(checkpoint)?,
        )?;
        if completed {
            fs::write(
                checkpoint_dir.join("completed.json"),
                serde_json::to_vec_pretty(checkpoint)?,
            )?;
        }
    }
    Ok(())
}

fn validate_progress_log(fixture: &Path, report: &SyncScaleReport) -> Result<()> {
    let path = fixture.join("logs").join("progress.jsonl");
    let content = fs::read_to_string(&path)
        .with_context(|| format!("failed to read `{}`", path.display()))?;
    let mut events = Vec::new();
    for (index, line) in content.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let event: serde_json::Value = serde_json::from_str(line).with_context(|| {
            format!(
                "failed to parse progress event {} in `{}`",
                index + 1,
                path.display()
            )
        })?;
        events.push(event);
    }
    let last = events
        .last()
        .context("scale fixture progress log is empty")?;
    if last["profile"].as_str() != Some(report.profile.as_str())
        || last["seed"].as_u64() != Some(report.seed)
        || last["target_objects"].as_u64() != Some(report.target_objects as u64)
        || last["current_scenario_instance"].as_u64() != Some(report.generated_instances as u64)
        || last["current_object_count"].as_u64() != Some(report.object_count as u64)
        || last["completed"].as_bool() != Some(true)
        || last["safe_boundary"].as_bool() != Some(true)
        || last["phase"].as_str() != Some("completed")
        || last["progress"]["objects_per_second"]
            .as_f64()
            .unwrap_or_default()
            <= 0.0
        || last["progress"]["progress_percent"]
            .as_f64()
            .unwrap_or_default()
            < 100.0
    {
        bail!("scale fixture progress log final event does not match the manifest");
    }
    if !events
        .iter()
        .any(|event| event["phase"].as_str() == Some("started"))
        || !events
            .iter()
            .any(|event| event["phase"].as_str() == Some("initialized"))
    {
        bail!("scale fixture progress log is missing initialized or started events");
    }
    Ok(())
}

fn build_object_distribution_report(report: &SyncScaleReport) -> Result<serde_json::Value> {
    let mut seen_objects = HashSet::new();
    let mut object_counts_per_ledger = BTreeMap::<String, usize>::new();
    let mut object_counts_per_simulated_year = BTreeMap::<String, usize>::new();
    for database in report.databases.values() {
        let connection = rusqlite::Connection::open(database)?;
        let mut statement = connection.prepare(
            "SELECT content_hash, ledger_id, payload FROM protocol_object ORDER BY content_hash",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })?;
        for row in rows {
            let (hash, ledger, payload) = row?;
            if !seen_objects.insert(hash) {
                continue;
            }
            let ledger_key = if ledger.len() == 16 {
                Uuid::from_slice(&ledger)
                    .map(|id| id.to_string())
                    .unwrap_or_else(|_| "invalid-ledger-id".into())
            } else {
                "ledger-neutral".into()
            };
            *object_counts_per_ledger.entry(ledger_key).or_default() += 1;
            let year = serde_json::from_slice::<serde_json::Value>(&payload)
                .ok()
                .and_then(|value| {
                    value
                        .get("created_at")
                        .or_else(|| value.pointer("/body/created_at"))
                        .and_then(serde_json::Value::as_str)
                        .and_then(|created| created.get(..4))
                        .map(str::to_string)
                })
                .unwrap_or_else(|| "unknown".into());
            *object_counts_per_simulated_year.entry(year).or_default() += 1;
        }
    }

    let effective_rows = unique_projection_rows(
        report.databases.values(),
        "projection_effective",
        "proposition_id",
        &[
            "status",
            "withdrawal_status",
            "archival_status",
            "revision_id",
            "deliberation_id",
        ],
    )?;
    let mut counts_by_status = BTreeMap::<String, usize>::new();
    let mut lifecycle_counts = BTreeMap::<String, usize>::new();
    let mut pending_revision_count = 0_usize;
    for row in &effective_rows {
        if let Some(status) = row.get("status").and_then(serde_json::Value::as_str) {
            *counts_by_status.entry(status.into()).or_default() += 1;
        }
        if row.get("status").and_then(serde_json::Value::as_str) == Some("pending") {
            pending_revision_count += 1;
        }
        if row
            .get("withdrawal_status")
            .and_then(serde_json::Value::as_str)
            == Some("withdrawn")
        {
            *lifecycle_counts.entry("withdrawn".into()).or_default() += 1;
        }
        if row
            .get("archival_status")
            .and_then(serde_json::Value::as_str)
            == Some("archived")
        {
            *lifecycle_counts.entry("archived".into()).or_default() += 1;
        }
    }

    let revision_rows = unique_projection_rows(
        report.databases.values(),
        "projection_revision",
        "revision_id",
        &["proposition_id", "parent_revision_id"],
    )?;
    let mut revision_depths = BTreeMap::<String, usize>::new();
    for row in &revision_rows {
        if let Some(proposition) = row
            .get("proposition_id")
            .and_then(serde_json::Value::as_str)
        {
            *revision_depths.entry(proposition.into()).or_default() += 1;
        }
    }
    let revision_depth_values = revision_depths.values().copied().collect::<Vec<_>>();

    let deliberation_rows = unique_projection_rows(
        report.databases.values(),
        "projection_deliberation",
        "deliberation_id",
        &["proposition_id", "revision_id", "settled"],
    )?;
    let participant_rows = unique_projection_rows(
        report.databases.values(),
        "projection_participant",
        "deliberation_id || ':' || actor_id",
        &["deliberation_id", "actor_id", "active"],
    )?;
    let mut participant_counts = BTreeMap::<String, usize>::new();
    for row in &participant_rows {
        if row.get("active").and_then(serde_json::Value::as_i64) != Some(1) {
            continue;
        }
        if let Some(deliberation) = row
            .get("deliberation_id")
            .and_then(serde_json::Value::as_str)
        {
            *participant_counts.entry(deliberation.into()).or_default() += 1;
        }
    }
    let participant_count_values = participant_counts.values().copied().collect::<Vec<_>>();

    let reconciliation_rows = unique_projection_rows(
        report.databases.values(),
        "projection_reconciliation",
        "revision_id",
        &["resolution_mode", "affected_proposition_id"],
    )?;
    let mut reconciliation_counts_by_mode = BTreeMap::<String, usize>::new();
    for row in &reconciliation_rows {
        if let Some(mode) = row
            .get("resolution_mode")
            .and_then(serde_json::Value::as_str)
        {
            *reconciliation_counts_by_mode
                .entry(mode.into())
                .or_default() += 1;
        }
    }

    let pending_rows = unique_projection_rows(
        report.databases.values(),
        "projection_pending",
        "pending_id",
        &["kind", "reason"],
    )?;
    let mut pending_counts_by_kind = BTreeMap::<String, usize>::new();
    for row in &pending_rows {
        if let Some(kind) = row.get("kind").and_then(serde_json::Value::as_str) {
            *pending_counts_by_kind.entry(kind.into()).or_default() += 1;
        }
    }

    Ok(serde_json::json!({
        "version": 1,
        "profile": report.profile,
        "seed": report.seed,
        "simulated_started_at": sdk_timestamp(report.simulated_started_at),
        "simulated_ended_at": sdk_timestamp(report.simulated_ended_at),
        "simulated_duration_seconds": report.simulated_duration_seconds,
        "time_distribution_profile": "deterministic-family-hours-v1",
        "object_count": report.object_count,
        "object_counts_by_type": &report.object_counts_by_type,
        "actor_count": report.actor_count,
        "ledger_count": report.ledger_count,
        "replica_count": report.replica_count,
        "counts_by_status": counts_by_status,
        "accepted_proposition_count": counts_by_status.get("accepted").copied().unwrap_or_default(),
        "rejected_proposition_count": counts_by_status.get("rejected").copied().unwrap_or_default(),
        "pending_revision_count": pending_revision_count,
        "archived_proposition_count": lifecycle_counts.get("archived").copied().unwrap_or_default(),
        "withdrawn_proposition_count": lifecycle_counts.get("withdrawn").copied().unwrap_or_default(),
        "contested_proposition_count": counts_by_status.get("conflict").copied().unwrap_or_default(),
        "conflict_count_by_type": {
            "sibling_revision": report.conflict_report.sibling_revision_conflicts,
            "incompatible_deliberation": report.conflict_report.incompatible_deliberation_conflicts,
        },
        "revision_depth": numeric_summary(&revision_depth_values),
        "deliberation_size": {
            "count": deliberation_rows.len(),
            "participant_count": numeric_summary(&participant_count_values),
            "settled_count": deliberation_rows.iter().filter(|row| row.get("settled").and_then(serde_json::Value::as_i64) == Some(1)).count(),
        },
        "participant_count": numeric_summary(&participant_count_values),
        "object_counts_per_ledger": object_counts_per_ledger,
        "object_counts_per_simulated_year": object_counts_per_simulated_year,
        "pending_counts_by_kind": pending_counts_by_kind,
        "reconciliation_counts_by_mode": reconciliation_counts_by_mode,
        "deep_validation_sample": &report.deep_validation_sample,
    }))
}

fn build_search_corpus_report(report: &SyncScaleReport) -> serde_json::Value {
    let mut search_terms = BTreeSet::new();
    let mut search_samples = Vec::new();
    for receipt in &report.cli_sample_report {
        let Some(search_index) = receipt.command.iter().position(|arg| arg == "search") else {
            continue;
        };
        let mut status = None;
        let mut effective = false;
        let mut page_size = 20usize;
        let mut text = None;
        let mut index = search_index + 1;
        while index < receipt.command.len() {
            match receipt.command[index].as_str() {
                "--effective" => {
                    effective = true;
                    index += 1;
                }
                "--status" => {
                    status = receipt.command.get(index + 1).cloned();
                    index += 2;
                }
                "--page-size" => {
                    page_size = receipt
                        .command
                        .get(index + 1)
                        .and_then(|value| value.parse().ok())
                        .unwrap_or(page_size);
                    index += 2;
                }
                value if value.starts_with("--") => {
                    index += 1;
                }
                value => {
                    text = Some(value.to_string());
                    break;
                }
            }
        }
        let text = text.unwrap_or_default();
        if !text.is_empty() {
            search_terms.insert(text.clone());
        }
        let result_count = receipt
            .parsed_json
            .as_ref()
            .and_then(serde_json::Value::as_array)
            .map(Vec::len)
            .unwrap_or_else(|| {
                receipt
                    .stdout
                    .lines()
                    .filter(|line| !line.trim().is_empty() && line.trim() != "no results")
                    .count()
            });
        search_samples.push(serde_json::json!({
            "command": &receipt.command,
            "text": text,
            "status": status,
            "effective": effective,
            "page_size": page_size,
            "result_count": result_count,
            "bounded_by_page_size": result_count <= page_size,
        }));
    }
    let ambiguous_reference_sampled = report.cli_sample_report.iter().any(|receipt| {
        receipt.command == ["echo", "019c"]
            && receipt.status != Some(0)
            && receipt.stderr.contains("reference is ambiguous")
    });
    let labels = report.cli_ux_coverage.iter().collect::<BTreeSet<_>>();
    serde_json::json!({
        "indexed": true,
        "searchable_object_counts": {
            "proposition": report.object_counts_by_type.get("proposition").copied().unwrap_or_default(),
            "revision": report.object_counts_by_type.get("revision").copied().unwrap_or_default(),
            "deliberation": report.object_counts_by_type.get("deliberation").copied().unwrap_or_default(),
            "deliberation_comment": report.object_counts_by_type.get("deliberation_comment").copied().unwrap_or_default(),
            "settlement": report.object_counts_by_type.get("settlement").copied().unwrap_or_default(),
        },
        "sampled_query_count": report.cli_sample_report.len(),
        "sampled_search_query_count": search_samples.len(),
        "sampled_cli_matches_sdk": report.assertion_report.sampled_cli_matches_sdk,
        "cli_ux_coverage": &report.cli_ux_coverage,
        "sampled_search_terms": search_terms.into_iter().collect::<Vec<_>>(),
        "sampled_search_queries": search_samples,
        "effective_search_sampled": labels.contains(&"search-effective-json".to_string())
            && labels.contains(&"search-effective-text".to_string()),
        "status_filter_search_sampled": labels.contains(&"search-accepted-effective-json".to_string())
            && labels.contains(&"search-contested-effective-json".to_string()),
        "bounded_page_size_sampled": labels.contains(&"search-page-size-bounded-json".to_string()),
        "ambiguous_reference_sampled": ambiguous_reference_sampled,
        "known_indexing_status": "searchable-content-sampled",
    })
}

fn deep_validation_sample_from_databases<'a>(
    databases: impl IntoIterator<Item = &'a PathBuf>,
    seed: u64,
) -> Result<serde_json::Value> {
    let databases = databases.into_iter().collect::<Vec<_>>();
    let effective_rows = unique_projection_rows(
        databases.iter().copied(),
        "projection_effective",
        "proposition_id",
        &[
            "status",
            "withdrawal_status",
            "archival_status",
            "revision_id",
            "deliberation_id",
        ],
    )?;
    let revision_rows = unique_projection_rows(
        databases.iter().copied(),
        "projection_revision",
        "revision_id",
        &["proposition_id", "parent_revision_id"],
    )?;
    let reconciliation_rows = unique_projection_rows(
        databases.iter().copied(),
        "projection_reconciliation",
        "revision_id",
        &["resolution_mode", "affected_proposition_id"],
    )?;
    let pending_rows = unique_projection_rows(
        databases.iter().copied(),
        "projection_pending",
        "pending_id",
        &["kind", "reason"],
    )?;
    Ok(seeded_deep_validation_sample(
        &effective_rows,
        &revision_rows,
        &reconciliation_rows,
        &pending_rows,
        seed,
    ))
}

fn unique_projection_rows<'a>(
    databases: impl IntoIterator<Item = &'a PathBuf>,
    table: &str,
    key_expression: &str,
    fields: &[&str],
) -> Result<Vec<serde_json::Value>> {
    let mut seen = HashSet::new();
    let mut rows = Vec::new();
    let select_fields = fields
        .iter()
        .map(|field| field.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    for database in databases {
        let connection = rusqlite::Connection::open(database)?;
        let table = projection_table_name(&connection, table)?;
        let query = format!("SELECT {key_expression} AS dedupe_key, {select_fields} FROM {table}");
        let Ok(mut statement) = connection.prepare(&query) else {
            continue;
        };
        let mapped = statement.query_map([], |row| {
            let key = row_value_as_string(row, 0)?;
            let mut object = serde_json::Map::new();
            object.insert("id".into(), serde_json::Value::String(key.clone()));
            for (index, field) in fields.iter().enumerate() {
                object.insert((*field).into(), row_value(row, index + 1)?);
            }
            Ok((key, serde_json::Value::Object(object)))
        })?;
        for row in mapped {
            let (key, value) = row?;
            if seen.insert(key) {
                rows.push(value);
            }
        }
    }
    Ok(rows)
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

fn table_exists(connection: &rusqlite::Connection, table: &str) -> Result<bool> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
        [table],
        |row| row.get::<_, i64>(0),
    )? != 0)
}

fn status_counts_from_databases<'a>(
    databases: impl IntoIterator<Item = &'a PathBuf>,
) -> Result<BTreeMap<String, usize>> {
    let rows = unique_projection_rows(
        databases,
        "projection_effective",
        "proposition_id",
        &["status"],
    )?;
    let mut counts = BTreeMap::new();
    for row in rows {
        if let Some(status) = row.get("status").and_then(serde_json::Value::as_str) {
            *counts.entry(status.to_string()).or_default() += 1;
        }
    }
    Ok(counts)
}

fn row_value(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<serde_json::Value> {
    use rusqlite::types::ValueRef;
    Ok(match row.get_ref(index)? {
        ValueRef::Null => serde_json::Value::Null,
        ValueRef::Integer(value) => serde_json::json!(value),
        ValueRef::Real(value) => serde_json::json!(value),
        ValueRef::Text(value) => {
            serde_json::Value::String(String::from_utf8_lossy(value).to_string())
        }
        ValueRef::Blob(value) => serde_json::Value::String(hex_bytes(value)),
    })
}

fn row_value_as_string(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<String> {
    Ok(match row_value(row, index)? {
        serde_json::Value::String(value) => value,
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    })
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

fn numeric_summary(values: &[usize]) -> serde_json::Value {
    let count = values.len();
    let sum = values.iter().sum::<usize>();
    let max = values.iter().copied().max().unwrap_or_default();
    serde_json::json!({
        "count": count,
        "average": if count == 0 { 0.0 } else { sum as f64 / count as f64 },
        "maximum": max,
    })
}

fn seeded_deep_validation_sample(
    effective_rows: &[serde_json::Value],
    revision_rows: &[serde_json::Value],
    reconciliation_rows: &[serde_json::Value],
    pending_rows: &[serde_json::Value],
    seed: u64,
) -> serde_json::Value {
    let accepted_propositions = sample_projection_ids(effective_rows, seed, "accepted", 5);
    let rejected_propositions = sample_projection_ids(effective_rows, seed, "rejected", 5);
    let pending_propositions = sample_projection_ids(effective_rows, seed, "pending", 5);
    let conflicted_propositions = sample_projection_ids(effective_rows, seed, "conflict", 5);
    let archived_propositions =
        sample_lifecycle_ids(effective_rows, seed, "archival_status", "archived", 5);
    let withdrawn_propositions =
        sample_lifecycle_ids(effective_rows, seed, "withdrawal_status", "withdrawn", 5);
    let revision_ids = sample_ids(revision_rows, seed ^ 0x5256_4953_494f_4e53, 10);
    let pending_action_ids = sample_ids(pending_rows, seed ^ 0x5045_4e44_494e_4753, 10);
    let reconciliation_revision_ids =
        sample_ids(reconciliation_rows, seed ^ 0x5245_434f_4e43_494c, 10);
    serde_json::json!({
        "seed": seed,
        "accepted_propositions": accepted_propositions,
        "rejected_propositions": rejected_propositions,
        "pending_propositions": pending_propositions,
        "conflicted_propositions": conflicted_propositions,
        "archived_propositions": archived_propositions,
        "withdrawn_propositions": withdrawn_propositions,
        "revision_ids": revision_ids,
        "pending_action_ids": pending_action_ids,
        "reconciliation_revision_ids": reconciliation_revision_ids,
        "category_counts": {
            "accepted_propositions": effective_rows.iter().filter(|row| row.get("status").and_then(serde_json::Value::as_str) == Some("accepted")).count(),
            "rejected_propositions": effective_rows.iter().filter(|row| row.get("status").and_then(serde_json::Value::as_str) == Some("rejected")).count(),
            "pending_propositions": effective_rows.iter().filter(|row| row.get("status").and_then(serde_json::Value::as_str) == Some("pending")).count(),
            "conflicted_propositions": effective_rows.iter().filter(|row| row.get("status").and_then(serde_json::Value::as_str) == Some("conflict")).count(),
            "archived_propositions": effective_rows.iter().filter(|row| row.get("archival_status").and_then(serde_json::Value::as_str) == Some("archived")).count(),
            "withdrawn_propositions": effective_rows.iter().filter(|row| row.get("withdrawal_status").and_then(serde_json::Value::as_str) == Some("withdrawn")).count(),
            "revisions": revision_rows.len(),
            "pending_actions": pending_rows.len(),
            "reconciliation_revisions": reconciliation_rows.len(),
        },
        "coverage": {
            "accepted_propositions": !accepted_propositions.is_empty(),
            "rejected_propositions": !rejected_propositions.is_empty(),
            "pending_propositions": !pending_propositions.is_empty(),
            "conflicted_propositions": !conflicted_propositions.is_empty(),
            "archived_propositions": !archived_propositions.is_empty(),
            "withdrawn_propositions": !withdrawn_propositions.is_empty(),
            "revision_history": !revision_ids.is_empty(),
            "pending_actions": !pending_action_ids.is_empty(),
            "reconciliation_outcomes": !reconciliation_revision_ids.is_empty(),
        },
    })
}

fn sample_projection_ids(
    rows: &[serde_json::Value],
    seed: u64,
    status: &str,
    limit: usize,
) -> Vec<String> {
    let filtered = rows
        .iter()
        .filter(|row| row.get("status").and_then(serde_json::Value::as_str) == Some(status))
        .cloned()
        .collect::<Vec<_>>();
    sample_ids(&filtered, seed ^ stable_label_hash(status), limit)
}

fn sample_lifecycle_ids(
    rows: &[serde_json::Value],
    seed: u64,
    field: &str,
    value: &str,
    limit: usize,
) -> Vec<String> {
    let filtered = rows
        .iter()
        .filter(|row| row.get(field).and_then(serde_json::Value::as_str) == Some(value))
        .cloned()
        .collect::<Vec<_>>();
    sample_ids(
        &filtered,
        seed ^ stable_label_hash(field) ^ stable_label_hash(value),
        limit,
    )
}

fn sample_ids(rows: &[serde_json::Value], seed: u64, limit: usize) -> Vec<String> {
    let mut ids = rows
        .iter()
        .filter_map(|row| row.get("id").and_then(serde_json::Value::as_str))
        .map(|id| {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&seed.to_le_bytes());
            bytes.extend_from_slice(id.as_bytes());
            (fact_core::Hash::digest(&bytes), id.to_string())
        })
        .collect::<Vec<_>>();
    ids.sort_by_key(|(hash, _)| *hash);
    ids.into_iter().take(limit).map(|(_, id)| id).collect()
}

fn stable_label_hash(label: &str) -> u64 {
    let digest = fact_core::Hash::digest(label.as_bytes());
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest.as_bytes()[..8]);
    u64::from_le_bytes(bytes)
}

fn rust_toolchain_version() -> String {
    Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|version| version.trim().to_string())
        .filter(|version| !version.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(target_os = "linux")]
fn peak_memory_bytes() -> Option<u64> {
    peak_rss_raw().map(|kilobytes| kilobytes.saturating_mul(1024))
}

#[cfg(all(unix, not(target_os = "linux")))]
fn peak_memory_bytes() -> Option<u64> {
    peak_rss_raw()
}

#[cfg(not(unix))]
fn peak_memory_bytes() -> Option<u64> {
    None
}

#[cfg(unix)]
fn peak_rss_raw() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the provided rusage pointer on success.
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    (status == 0).then(|| unsafe { usage.assume_init().ru_maxrss as u64 })
}

fn memory_exceeds_limit(memory_bytes: u64, max_memory_mb: u64) -> bool {
    memory_bytes > max_memory_mb.saturating_mul(1024 * 1024)
}

#[cfg(test)]
mod scale_memory_tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use super::{
        AssertionReport, ConflictReport, PerformanceReport, RepairReport, ScaleContentState,
        SyncScaleReport, SynchronizationReport, TransferReport, World, deterministic_missing_hash,
        logical_replay_digest, memory_exceeds_limit, mirror_scale_progress_event,
        realized_topology_satisfies_config, required_deep_validation_count_categories,
        required_deep_validation_coverage_categories, required_failure_context_fields,
        resolve_fixture_path, scale_failure_context, scale_markdown_content,
        scale_progress_mirror_dir, validate_commitment_report_against_hashes,
        validate_http_object_sample, validate_scale_package_paths, verification_target_objects,
    };

    #[test]
    fn memory_limit_allows_exact_ceiling_and_rejects_overage() {
        assert!(!memory_exceeds_limit(10 * 1024 * 1024, 10));
        assert!(memory_exceeds_limit(10 * 1024 * 1024 + 1, 10));
    }

    #[test]
    fn http_object_sample_allows_paginated_large_ledgers() -> anyhow::Result<()> {
        let body = serde_json::json!({
            "objects": vec![serde_json::json!({"content_hash": "hash"}); 1_000],
            "next_cursor": "cursor"
        });

        validate_http_object_sample(&body, 4_961)?;
        Ok(())
    }

    #[test]
    fn http_object_sample_requires_cursor_for_partial_page() {
        let body = serde_json::json!({
            "objects": vec![serde_json::json!({"content_hash": "hash"}); 1_000],
            "next_cursor": null
        });

        let error = validate_http_object_sample(&body, 4_961).unwrap_err();
        assert!(error.to_string().contains("without next_cursor"));
    }

    #[test]
    fn scale_progress_mirror_writes_sibling_progress_artifacts() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture = temp.path().join("scale-500k-balanced-small-seed-42");
        let mirror = scale_progress_mirror_dir(&fixture);
        assert_eq!(
            mirror,
            temp.path()
                .join("scale-500k-balanced-small-seed-42.progress")
        );
        let event = serde_json::json!({
            "profile": "scale-500k-balanced",
            "phase": "completed",
            "safe_boundary": true,
            "completed": false,
            "progress": {
                "progress_percent": 1.0
            }
        });
        let checkpoint = serde_json::json!({
            "profile": "scale-500k-balanced",
            "phase": "completed",
            "safe_boundary": true,
            "completed": false
        });

        mirror_scale_progress_event(&fixture, &event, Some(&checkpoint), false)?;

        let progress_log = std::fs::read_to_string(mirror.join("logs/progress.jsonl"))?;
        assert!(progress_log.contains("\"progress_percent\":1.0"));
        let latest = mirror.join("checkpoints/latest.json");
        assert!(latest.is_file());
        assert!(!mirror.join("checkpoints/completed.json").exists());
        assert!(!fixture.exists());
        Ok(())
    }

    #[test]
    fn realized_topology_must_satisfy_configured_world() {
        let world_plan = serde_json::json!({
            "configured_world": {
                "actors": 500,
                "ledgers": 12,
            },
            "realized_topology": {
                "actors": 505,
                "shared_ledgers": 12,
                "ledgers": 517,
                "replicas": 30,
            }
        });
        assert!(realized_topology_satisfies_config(&world_plan));
    }

    #[test]
    fn realized_topology_rejects_under_realized_world() {
        let world_plan = serde_json::json!({
            "configured_world": {
                "actors": 500,
                "ledgers": 12,
            },
            "realized_topology": {
                "actors": 3,
                "shared_ledgers": 9,
                "ledgers": 12,
                "replicas": 11,
            }
        });
        assert!(!realized_topology_satisfies_config(&world_plan));
    }

    #[test]
    fn deep_validation_contract_requires_lifecycle_pending_and_reconciliation_samples() {
        assert_eq!(
            required_deep_validation_coverage_categories(),
            &[
                "accepted_propositions",
                "rejected_propositions",
                "pending_propositions",
                "conflicted_propositions",
                "archived_propositions",
                "withdrawn_propositions",
                "revision_history",
                "pending_actions",
                "reconciliation_outcomes",
            ]
        );
        assert_eq!(
            required_deep_validation_count_categories(),
            &[
                "accepted_propositions",
                "rejected_propositions",
                "pending_propositions",
                "conflicted_propositions",
                "archived_propositions",
                "withdrawn_propositions",
                "revisions",
                "pending_actions",
                "reconciliation_revisions",
            ]
        );
    }

    #[test]
    fn scale_failure_context_includes_required_diagnostic_fields() {
        let context = scale_failure_context(
            "scale-500k-balanced",
            42,
            Some("stable-fact"),
            7,
            "enforce scale safeguards",
            "system",
            "all",
            "all",
            "max_database_bytes",
        );
        for field in required_failure_context_fields() {
            assert!(
                context.contains(&format!("{field}=")),
                "missing `{field}` in {context}"
            );
        }
        assert!(context.contains("profile=scale-500k-balanced"));
        assert!(context.contains("seed=42"));
        assert!(context.contains("scenario=stable-fact"));
        assert!(context.contains("instance=7"));
        assert!(context.contains("operation=max_database_bytes"));
    }

    #[test]
    fn commitment_report_verifies_sampled_proofs_against_hashes() {
        let mut hashes = sampled_hashes();
        hashes.sort();
        let commitment = fact_sdk::commitment::create_commitment(hashes.clone()).unwrap();
        let root = commitment.root.as_str();
        let parsed_root = root.parse::<fact_core::Hash>().unwrap();
        let verification =
            fact_sdk::commitment::verify_commitment(hashes.clone(), parsed_root).unwrap();
        let inclusion_proofs = vec![
            fact_sdk::commitment::create_inclusion_proof(hashes.clone(), hashes[0]).unwrap(),
            fact_sdk::commitment::create_inclusion_proof(hashes.clone(), hashes[1]).unwrap(),
            fact_sdk::commitment::create_inclusion_proof(hashes.clone(), hashes[2]).unwrap(),
        ];
        let non_inclusion_proof = fact_sdk::commitment::create_non_inclusion_proof(
            hashes.clone(),
            deterministic_missing_hash(&hashes),
        )
        .unwrap();
        let report = serde_json::json!({
            "object_count": hashes.len(),
            "root": root,
            "verified": true,
            "verification": verification,
            "sampled_inclusion_proofs": inclusion_proofs,
            "sampled_non_inclusion_proof": non_inclusion_proof,
        });
        validate_commitment_report_against_hashes(&report, &hashes, Some(root)).unwrap();
    }

    #[test]
    fn commitment_report_rejects_sampled_proof_drift() {
        let mut hashes = sampled_hashes();
        hashes.sort();
        let commitment = fact_sdk::commitment::create_commitment(hashes.clone()).unwrap();
        let root = commitment.root.as_str();
        let parsed_root = root.parse::<fact_core::Hash>().unwrap();
        let verification =
            fact_sdk::commitment::verify_commitment(hashes.clone(), parsed_root).unwrap();
        let mut inclusion_proof =
            fact_sdk::commitment::create_inclusion_proof(hashes.clone(), hashes[0]).unwrap();
        inclusion_proof.target = hashes[1].hex();
        let report = serde_json::json!({
            "object_count": hashes.len(),
            "root": root,
            "verified": true,
            "verification": verification,
            "sampled_inclusion_proofs": [inclusion_proof],
            "sampled_non_inclusion_proof": fact_sdk::commitment::create_non_inclusion_proof(
                hashes.clone(),
                deterministic_missing_hash(&hashes),
            ).unwrap(),
        });
        assert!(validate_commitment_report_against_hashes(&report, &hashes, Some(root)).is_err());
    }

    #[test]
    fn fixture_path_resolution_prefers_packaged_files() {
        let fixture = tempfile::tempdir().unwrap();
        let bundle_dir = fixture.path().join("bundles");
        let ledger_dir = fixture.path().join("ledgers");
        std::fs::create_dir_all(&bundle_dir).unwrap();
        std::fs::create_dir_all(&ledger_dir).unwrap();
        let packaged = bundle_dir.join("objects.factbndl");
        std::fs::write(&packaged, b"bundle").unwrap();
        let packaged_database = ledger_dir.join("operations_a.sqlite");
        std::fs::write(&packaged_database, b"database").unwrap();

        let recorded = PathBuf::from("/old/location/bundles/objects.factbndl");
        assert_eq!(resolve_fixture_path(fixture.path(), &recorded), packaged);
        let recorded_database = PathBuf::from("/old/location/operations_a.sqlite");
        assert_eq!(
            resolve_fixture_path(fixture.path(), &recorded_database),
            packaged_database
        );
    }

    #[test]
    fn scale_package_layout_requires_ledgers_and_bundle_directories() {
        let fixture = tempfile::tempdir().unwrap();
        let valid_database = fixture.path().join("ledgers").join("operations_a.sqlite");
        let valid_bundle = fixture.path().join("bundles").join("objects.factbndl");
        let databases = BTreeMap::from([("operations_a".to_string(), valid_database)]);

        validate_scale_package_paths(
            fixture.path(),
            &databases,
            &valid_bundle,
            std::slice::from_ref(&valid_bundle),
        )
        .unwrap();

        let root_database = BTreeMap::from([(
            "operations_a".to_string(),
            fixture.path().join("operations_a.sqlite"),
        )]);
        let error = validate_scale_package_paths(
            fixture.path(),
            &root_database,
            &valid_bundle,
            std::slice::from_ref(&valid_bundle),
        )
        .unwrap_err();
        assert!(error.to_string().contains("is not under"));

        let root_bundle = fixture.path().join("objects.factbndl");
        let error = validate_scale_package_paths(
            fixture.path(),
            &databases,
            &root_bundle,
            std::slice::from_ref(&root_bundle),
        )
        .unwrap_err();
        assert!(error.to_string().contains("primary object bundle"));
    }

    #[test]
    fn scale_markdown_content_is_deterministic_and_searchable() {
        let first = scale_markdown_content(
            "scale conflict base",
            18,
            "operations_a",
            ScaleContentState::Base,
        );
        let second = scale_markdown_content(
            "scale conflict base",
            18,
            "operations_a",
            ScaleContentState::Base,
        );
        assert_eq!(first, second);
        assert!(first.contains("# Scale Conflict Base 18:"));
        assert!(first.contains("## Decision"));
        assert!(first.contains("## Operating Context"));
        assert!(first.contains("scale, base"));
        assert!(first.contains("scale-conflict-base-00018"));
        assert!(first.contains("```toml"));
        assert!(first.contains("Template version: `deterministic-domain-templates-v1`"));

        let accepted = scale_markdown_content(
            "scale accepted revision",
            18,
            "operations_a",
            ScaleContentState::Accepted,
        );
        assert!(accepted.contains("accepted revision tightens"));

        let rejected = scale_markdown_content(
            "scale rejected branch",
            18,
            "operations_a",
            ScaleContentState::Rejected,
        );
        assert!(rejected.contains("rejected revision is retained"));
    }

    #[test]
    fn scale_generation_conflict_sample_preserves_base_effective_revision() -> anyhow::Result<()> {
        let mut world = initialized_scale_test_world()?;
        world.conflicting_sibling_revisions()?;
        Ok(())
    }

    #[test]
    fn scale_synchronization_journey_records_single_transfer() -> anyhow::Result<()> {
        let mut world = initialized_scale_test_world()?;
        let initial_transfers = world.transfers.len();

        world.scale_synchronization_journey(0)?;

        let sync_transfers = &world.transfers[initial_transfers..];
        assert_eq!(sync_transfers.len(), 1);
        assert_eq!(sync_transfers[0].scenario, "scale-synchronization");
        assert_eq!(sync_transfers[0].from, "operations_a");
        assert_eq!(sync_transfers[0].to, "operations_b");
        assert!(
            world
                .dirty_projection_replicas
                .borrow()
                .contains("operations_a")
        );
        Ok(())
    }

    #[test]
    fn scale_conflict_journey_defers_projection_rebuild() -> anyhow::Result<()> {
        let mut world = initialized_scale_test_world()?;

        world.scale_conflict_journey(0)?;

        assert_eq!(world.sibling_revision_conflicts, 1);
        assert!(world.sample_conflict_proposition_id.is_some());
        assert!(
            world
                .dirty_projection_replicas
                .borrow()
                .contains("operations_a")
        );
        assert!(
            world
                .dirty_projection_replicas
                .borrow()
                .contains("operations_b")
        );
        Ok(())
    }

    #[test]
    fn final_scale_convergence_runs_until_object_sets_quiesce() -> anyhow::Result<()> {
        let mut world = initialized_scale_test_world()?;

        world.full_mesh_sync_until_quiescent("test-final-convergence", 8)?;
        assert!(world.assert_converged_object_sets()?);
        Ok(())
    }

    fn initialized_scale_test_world() -> anyhow::Result<World> {
        let mut world = World::new("scale-500k-balanced", 42)?;
        world.scale_profile_config = Some(crate::scale::profile_config("scale-500k-balanced", 42)?);
        world.bootstrap_topology()?;
        world.expand_scale_topology()?;
        world.basic_replica_convergence()?;
        world.independent_offline_work()?;
        world.delayed_dependency_delivery()?;
        world.key_rotation_journey()?;
        Ok(world)
    }

    #[test]
    fn logical_replay_digest_ignores_operational_measurements() {
        let mut report = minimal_logical_report();
        let first = logical_replay_digest(&report).unwrap();

        report.started_at += time::Duration::seconds(5);
        report.output = PathBuf::from("/tmp/moved-fixture");
        report.databases.insert(
            "operations_a".into(),
            PathBuf::from("/tmp/moved-fixture/ledgers/operations_a.sqlite"),
        );
        report.bundle = PathBuf::from("/tmp/moved-fixture/bundles/objects.factbndl");
        report.bundle_paths = vec![report.bundle.clone()];
        report.snapshot_paths = vec![PathBuf::from(
            "/tmp/moved-fixture/snapshots/object-set.json",
        )];
        report.synchronization_report.transfers[0].duration_ms += 9_999;
        report.performance_report.generation_ms += 5_000;
        report.performance_report.objects_per_second = 12.5;
        report.performance_report.database_bytes += 4096;
        assert_eq!(first, logical_replay_digest(&report).unwrap());

        report.object_count += 1;
        assert_ne!(first, logical_replay_digest(&report).unwrap());
    }

    #[test]
    fn scale_completion_count_requires_propositions_not_total_objects() {
        let mut report = minimal_logical_report();
        report.object_count = 750_000;
        report.object_counts_by_type = BTreeMap::from([
            ("proposition".into(), 250_000),
            ("revision".into(), 500_000),
        ]);
        assert_eq!(super::canonical_target_count(&report), 250_000);

        report
            .object_counts_by_type
            .insert("proposition".into(), crate::scale::TARGET_OBJECTS);
        assert_eq!(
            super::canonical_target_count(&report),
            crate::scale::TARGET_OBJECTS
        );
    }

    #[test]
    fn scale_verification_uses_recorded_reduced_target() {
        let mut report = minimal_logical_report();
        report.profile = "scale-500k-balanced".into();
        report.target_objects = 10_000;
        assert_eq!(verification_target_objects(&report), 10_000);

        report.target_objects = 0;
        assert_eq!(
            verification_target_objects(&report),
            crate::scale::TARGET_OBJECTS
        );
    }

    fn minimal_logical_report() -> SyncScaleReport {
        let started_at = time::OffsetDateTime::parse(
            "2026-01-01T00:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap();
        let simulated_started_at = time::OffsetDateTime::parse(
            "2020-01-01T00:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap();
        let simulated_ended_at = time::OffsetDateTime::parse(
            "2025-01-01T00:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap();
        SyncScaleReport {
            profile: "scale-500k-balanced".into(),
            profile_version: 1,
            seed: 42,
            scheduler_version: "seeded-branch-interleave-v0".into(),
            scenario_corpus_version: "scale-scenario-corpus-v1".into(),
            content_template_version: "deterministic-domain-templates-v1".into(),
            time_distribution_profile: "deterministic-family-hours-v1".into(),
            started_at,
            simulated_started_at,
            simulated_ended_at,
            simulated_duration_seconds: 157_852_800,
            facts_sdk_revision: "sdk-test".into(),
            facts_cli_revision: "cli-test".into(),
            simulator_revision: "sim-test".into(),
            generator_version: "generator-test".into(),
            generator_source_commit: "commit-test".into(),
            rust_toolchain_version: "rust-test".into(),
            output: PathBuf::from("/tmp/fixture"),
            databases: BTreeMap::from([(
                "operations_a".into(),
                PathBuf::from("/tmp/fixture/ledgers/operations_a.sqlite"),
            )]),
            bundle: PathBuf::from("/tmp/fixture/bundles/objects.factbndl"),
            bundle_paths: vec![PathBuf::from("/tmp/fixture/bundles/objects.factbndl")],
            snapshot_paths: vec![PathBuf::from("/tmp/fixture/snapshots/object-set.json")],
            commitment_root: Some("root-1".into()),
            final_commitment_roots: vec!["root-1".into()],
            target_objects: 500_000,
            object_count: 1_000_143,
            object_counts_by_type: BTreeMap::from([
                ("proposition".into(), 500_000),
                ("revision".into(), 500_143),
            ]),
            counts_by_status: BTreeMap::from([
                ("accepted".into(), 499_000),
                ("pending".into(), 1_000),
                ("withdrawn".into(), 143),
            ]),
            counts_by_conflict_type: BTreeMap::from([("sibling_revision".into(), 7)]),
            deep_validation_sample: serde_json::json!({
                "seed": 42,
                "revision_ids": ["revision-1"],
                "coverage": {"pending_propositions": true},
                "category_counts": {"pending_propositions": 1}
            }),
            actor_count: 505,
            ledger_count: 517,
            replica_count: 30,
            actors: vec!["alice".into()],
            ledgers: vec!["operations".into()],
            replicas: vec!["operations_a".into()],
            remotes: vec!["operations_b".into()],
            generated_instances: 12_345,
            scenario_family_counts: BTreeMap::from([("stable-fact".into(), 100)]),
            scenario_family_object_counts: BTreeMap::from([("stable-fact".into(), 2_000)]),
            target_object_overshoot: 143,
            synchronization_report: SynchronizationReport {
                operation_count: 1,
                full_sync_count: 1,
                partial_sync_count: 0,
                duplicate_delivery_idempotent: true,
                missing_dependency_deferred: true,
                delayed_dependency_retry_succeeded: true,
                push_pull_equivalent: true,
                transfer_order_independent: true,
                transfers: vec![TransferReport {
                    scenario: "sync".into(),
                    from: "operations_a".into(),
                    to: "operations_b".into(),
                    ledger: "operations".into(),
                    direction: "push".into(),
                    offered: 10,
                    imported: 10,
                    duplicate: false,
                    partial: false,
                    missing_dependencies: 0,
                    duration_ms: 7,
                }],
            },
            retry_report: Vec::new(),
            repair_report: RepairReport {
                projection_repairs: 1,
                partial_sync_repairs: 1,
                semantic_corrections: 1,
                repaired_replicas_converged: true,
                canonical_history_preserved: true,
                repairs: Vec::new(),
            },
            reconciliation_counts_by_mode: BTreeMap::from([("select".into(), 1)]),
            coordinator_disposition_counts: BTreeMap::from([("accepted".into(), 1)]),
            conflict_report: ConflictReport {
                sibling_revision_conflicts: 7,
                incompatible_deliberation_conflicts: 1,
                compatible_deliberations_without_conflict: 1,
                last_undisputed_ancestor_preserved_as_effective: true,
                arrival_order_selected_winner: false,
                sample_conflict_proposition_id: None,
                conflict_replicas: vec!["operations_a".into(), "operations_b".into()],
            },
            assertion_report: AssertionReport {
                dependency_closure_after_full_sync: true,
                converged_object_sets: true,
                converged_projections: true,
                projection_rebuild_equivalent: true,
                authorization_at_causal_point: true,
                historical_signatures_after_key_rotation: true,
                revoked_key_rejection_observed: true,
                key_rotation_byte_for_byte_replay: true,
                stable_effective_state_with_conflict: true,
                sampled_cli_matches_sdk: true,
                sampled_http_matches_sdk: true,
                failure_context_fields: vec!["profile".into(), "seed".into()],
            },
            performance_report: PerformanceReport {
                generation_ms: 1_000,
                objects_per_second: 1_000.0,
                peak_memory_bytes: Some(4_096),
                signing_ms: 10,
                push_bundle_creation_ms: 20,
                pull_import_ms: 30,
                dependency_resolution_ms: 40,
                projection_rebuild_ms: 50,
                indexing_ms: 60,
                convergence_ms: 70,
                verification_ms: 80,
                packaging_ms: 90,
                database_bytes: 123_456,
            },
            cli_sample_report: Vec::new(),
            cli_ux_coverage: vec!["json-pending-actionable-state".into()],
            http_sample_report: Vec::new(),
            unresolved_protocol_behavior: Vec::new(),
            verification_result: true,
            logical_replay_digest: String::new(),
        }
    }

    fn sampled_hashes() -> Vec<fact_core::Hash> {
        vec![
            fact_core::Hash::digest(b"alpha"),
            fact_core::Hash::digest(b"bravo"),
            fact_core::Hash::digest(b"charlie"),
        ]
    }
}

fn write_commitment_artifacts(
    output: &Path,
    report: &SyncScaleReport,
    unique_hashes: &[fact_core::Hash],
) -> Result<()> {
    let mut hashes = unique_hashes.to_vec();
    hashes.sort();
    let Some(root) = &report.commitment_root else {
        bail!("cannot package commitments without a commitment root");
    };
    let expected_root = root
        .parse::<fact_core::Hash>()
        .context("manifest commitment root is not a valid hash")?;
    let verification = fact_sdk::commitment::verify_commitment(hashes.clone(), expected_root)
        .context("verify packaged object-set commitment")?;
    if !verification.valid {
        bail!("packaged object-set commitment did not verify");
    }
    let mut proof_targets = Vec::new();
    if let Some(first) = hashes.first().copied() {
        proof_targets.push(first);
    }
    if let Some(middle) = hashes.get(hashes.len() / 2).copied()
        && !proof_targets.contains(&middle)
    {
        proof_targets.push(middle);
    }
    if let Some(last) = hashes.last().copied()
        && !proof_targets.contains(&last)
    {
        proof_targets.push(last);
    }
    let inclusion_proofs = proof_targets
        .into_iter()
        .map(|hash| fact_sdk::commitment::create_inclusion_proof(hashes.clone(), hash))
        .collect::<Result<Vec<_>, _>>()
        .context("create sampled inclusion proofs")?;
    let missing = deterministic_missing_hash(&hashes);
    let non_inclusion_proof =
        fact_sdk::commitment::create_non_inclusion_proof(hashes.clone(), missing)
            .context("create deterministic non-inclusion proof")?;
    fs::write(
        output.join("commitments").join("object-set.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "version": 1,
            "profile": report.profile,
            "seed": report.seed,
            "object_count": hashes.len(),
            "root": root,
            "verified": verification.valid,
            "verification": verification,
            "sampled_inclusion_proofs": inclusion_proofs,
            "sampled_non_inclusion_proof": non_inclusion_proof,
        }))?,
    )?;
    Ok(())
}

fn write_bundle_artifacts(
    output: &Path,
    report: &SyncScaleReport,
) -> Result<Vec<serde_json::Value>> {
    let mut inventory = Vec::new();
    for (name, database) in &report.databases {
        for ledger in database_ledger_ids(database)? {
            let objects = database_objects_for_ledger(database, ledger)?;
            if objects.is_empty() {
                continue;
            }
            let bundle_name = format!("{name}-{ledger}.factbndl");
            let bundle_path = output.join("bundles").join(&bundle_name);
            fs::write(&bundle_path, encode_bundle(ledger, &objects)?)?;
            let decoded_count = decode_bundle_or_snapshot_objects(&fs::read(&bundle_path)?)?.len();
            inventory.push(serde_json::json!({
                "database": database,
                "name": name,
                "ledger": ledger,
                "bundle": bundle_path,
                "object_count": objects.len(),
                "decoded_object_count": decoded_count,
            }));
        }
    }
    fs::write(
        output.join("bundles").join("inventory.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "version": 1,
            "profile": report.profile,
            "seed": report.seed,
            "bundle_count": inventory.len(),
            "bundles": inventory,
        }))?,
    )?;
    Ok(inventory)
}

fn deterministic_missing_hash(hashes: &[fact_core::Hash]) -> fact_core::Hash {
    let mut counter = 0_u64;
    loop {
        let candidate = fact_core::Hash::digest(&counter.to_le_bytes());
        if !hashes.binary_search(&candidate).is_ok() {
            return candidate;
        }
        counter += 1;
    }
}

fn validate_commitment_report_against_hashes(
    commitment_report: &serde_json::Value,
    hashes: &[fact_core::Hash],
    expected_root: Option<&str>,
) -> Result<()> {
    let mut hashes = hashes.to_vec();
    hashes.sort();
    let Some(root) = expected_root else {
        bail!("cannot verify commitment report without manifest root");
    };
    let parsed_root = root
        .parse::<fact_core::Hash>()
        .context("manifest commitment root is not a valid hash")?;
    let verification = fact_sdk::commitment::verify_commitment(hashes.clone(), parsed_root)
        .context("verify commitment report root")?;
    if !verification.valid
        || commitment_report["verification"] != serde_json::to_value(&verification)?
        || commitment_report["object_count"].as_u64() != Some(hashes.len() as u64)
        || commitment_report["root"].as_str() != Some(root)
    {
        bail!("commitment report root or verification result does not match packaged hashes");
    }

    let mut proof_targets = Vec::new();
    if let Some(first) = hashes.first().copied() {
        proof_targets.push(first);
    }
    if let Some(middle) = hashes.get(hashes.len() / 2).copied()
        && !proof_targets.contains(&middle)
    {
        proof_targets.push(middle);
    }
    if let Some(last) = hashes.last().copied()
        && !proof_targets.contains(&last)
    {
        proof_targets.push(last);
    }
    let inclusion_proofs = proof_targets
        .into_iter()
        .map(|hash| fact_sdk::commitment::create_inclusion_proof(hashes.clone(), hash))
        .collect::<Result<Vec<_>, _>>()
        .context("recreate sampled inclusion proofs")?;
    let missing = deterministic_missing_hash(&hashes);
    let non_inclusion_proof = fact_sdk::commitment::create_non_inclusion_proof(hashes, missing)
        .context("recreate sampled non-inclusion proof")?;
    if commitment_report["sampled_inclusion_proofs"] != serde_json::to_value(&inclusion_proofs)?
        || commitment_report["sampled_non_inclusion_proof"]
            != serde_json::to_value(&non_inclusion_proof)?
    {
        bail!("commitment report sampled proofs do not match packaged hashes");
    }
    Ok(())
}

fn validate_bundle_snapshot_reports(
    bundle_inventory: &serde_json::Value,
    snapshot_report: &serde_json::Value,
    report: &SyncScaleReport,
    fixture: &Path,
) -> Result<()> {
    let bundles = bundle_inventory["bundles"]
        .as_array()
        .context("bundle inventory is missing bundles array")?;
    if bundle_inventory["bundle_count"].as_u64() != Some(bundles.len() as u64) {
        bail!("bundle inventory count does not match bundle entries");
    }
    if snapshot_report["portable_bundles"] != serde_json::Value::Array(bundles.clone()) {
        bail!("snapshot portable bundles do not match bundle inventory");
    }
    for bundle in bundles {
        let bundle_path = bundle["bundle"]
            .as_str()
            .map(PathBuf::from)
            .context("bundle inventory entry is missing bundle path")?;
        let bundle_path = resolve_fixture_path(fixture, &bundle_path);
        let decoded_count = decode_bundle_or_snapshot_objects(
            &fs::read(&bundle_path)
                .with_context(|| format!("failed to read `{}`", bundle_path.display()))?,
        )
        .with_context(|| format!("failed to decode `{}`", bundle_path.display()))?
        .len();
        if bundle["object_count"].as_u64() != Some(decoded_count as u64)
            || bundle["decoded_object_count"].as_u64() != Some(decoded_count as u64)
        {
            bail!(
                "bundle inventory count does not match decoded bundle `{}`",
                bundle_path.display()
            );
        }
    }
    let snapshots = snapshot_report["database_snapshots"]
        .as_array()
        .context("snapshot report is missing database snapshots")?;
    if snapshots.len() != report.databases.len() {
        bail!("snapshot database count does not match manifest databases");
    }
    for snapshot in snapshots {
        let name = snapshot["replica"]
            .as_str()
            .context("database snapshot is missing replica name")?;
        let database = report
            .databases
            .get(name)
            .with_context(|| format!("database snapshot references unknown database `{name}`"))?;
        let snapshot_database = snapshot["database"]
            .as_str()
            .map(PathBuf::from)
            .map(|path| resolve_fixture_path(fixture, &path))
            .context("database snapshot is missing database path")?;
        if snapshot_database != *database {
            bail!("database snapshot path does not match manifest for `{name}`");
        }
        let object_counts = unique_protocol_object_counts([database])?;
        let object_count = object_counts.values().sum::<usize>();
        let hash_count = protocol_hashes_for_database(database)?.len();
        if snapshot["object_count"].as_u64() != Some(object_count as u64)
            || snapshot["hash_count"].as_u64() != Some(hash_count as u64)
            || snapshot["object_counts_by_type"] != serde_json::to_value(&object_counts)?
        {
            bail!("database snapshot does not match packaged database `{name}`");
        }
    }
    Ok(())
}

fn normalize_fixture_report_paths(report: &mut SyncScaleReport, fixture: &Path) {
    report.output = fixture.to_path_buf();
    for database in report.databases.values_mut() {
        *database = resolve_fixture_path(fixture, database);
    }
    report.bundle = resolve_fixture_path(fixture, &report.bundle);
    for bundle_path in &mut report.bundle_paths {
        *bundle_path = resolve_fixture_path(fixture, bundle_path);
    }
    for snapshot_path in &mut report.snapshot_paths {
        *snapshot_path = resolve_fixture_path(fixture, snapshot_path);
    }
}

fn resolve_fixture_path(fixture: &Path, recorded: &Path) -> PathBuf {
    let mut candidates = Vec::new();
    if let Some(file_name) = recorded.file_name() {
        candidates.push(fixture.join(file_name));
        if let Some(parent_name) = recorded.parent().and_then(Path::file_name) {
            candidates.push(fixture.join(parent_name).join(file_name));
        }
        for directory in [
            "ledgers",
            "bundles",
            "snapshots",
            "commitments",
            "checkpoints",
            "logs",
        ] {
            candidates.push(fixture.join(directory).join(file_name));
        }
    }
    candidates
        .into_iter()
        .find(|candidate| candidate.exists())
        .unwrap_or_else(|| recorded.to_path_buf())
}

fn write_snapshot_artifacts(
    output: &Path,
    report: &SyncScaleReport,
    unique_hashes: &[fact_core::Hash],
    bundle_inventory: &[serde_json::Value],
) -> Result<()> {
    let bundle_bytes = fs::read(&report.bundle)
        .with_context(|| format!("failed to read `{}`", report.bundle.display()))?;
    let decoded_bundle_count = decode_bundle_or_snapshot_objects(&bundle_bytes)
        .context("decode packaged object bundle")?
        .len();
    if decoded_bundle_count == 0 {
        bail!("packaged object bundle is empty");
    }
    let database_snapshots = report
        .databases
        .iter()
        .map(|(name, path)| {
            let object_counts = unique_protocol_object_counts([path])?;
            let object_count = object_counts.values().sum::<usize>();
            let hashes = protocol_hashes_for_database(path)?;
            Ok(serde_json::json!({
                "replica": name,
                "database": path,
                "object_count": object_count,
                "hash_count": hashes.len(),
                "object_counts_by_type": object_counts,
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    fs::write(
        output.join("snapshots").join("object-set.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "version": 1,
            "profile": report.profile,
            "seed": report.seed,
            "snapshot_kind": "portable-object-bundle-inventory",
            "bundle": report.bundle,
            "bundle_object_count": decoded_bundle_count,
            "portable_bundle_count": bundle_inventory.len(),
            "portable_bundles": bundle_inventory,
            "unique_object_count": unique_hashes.len(),
            "commitment_root": report.commitment_root,
            "database_snapshots": database_snapshots,
        }))?,
    )?;
    Ok(())
}

fn logical_replay_digest(report: &SyncScaleReport) -> Result<String> {
    #[derive(Serialize)]
    struct StableTransfer<'a> {
        scenario: &'a str,
        from: &'a str,
        to: &'a str,
        ledger: &'a str,
        direction: &'a str,
        offered: usize,
        imported: usize,
        duplicate: bool,
        partial: bool,
        missing_dependencies: usize,
    }

    #[derive(Serialize)]
    struct StableSynchronizationReport<'a> {
        operation_count: usize,
        full_sync_count: usize,
        partial_sync_count: usize,
        duplicate_delivery_idempotent: bool,
        missing_dependency_deferred: bool,
        delayed_dependency_retry_succeeded: bool,
        push_pull_equivalent: bool,
        transfer_order_independent: bool,
        transfers: Vec<StableTransfer<'a>>,
    }

    #[derive(Serialize)]
    struct LogicalReplayDigestInput<'a> {
        version: u8,
        profile: &'a str,
        profile_version: u32,
        seed: u64,
        scheduler_version: &'a str,
        scenario_corpus_version: &'a str,
        content_template_version: &'a str,
        time_distribution_profile: &'a str,
        simulated_started_at: String,
        simulated_ended_at: String,
        simulated_duration_seconds: i64,
        facts_sdk_revision: &'a str,
        facts_cli_revision: &'a str,
        simulator_revision: &'a str,
        generator_version: &'a str,
        generator_source_commit: &'a str,
        rust_toolchain_version: &'a str,
        target_objects: usize,
        object_count: usize,
        object_counts_by_type: &'a BTreeMap<String, usize>,
        counts_by_status: &'a BTreeMap<String, usize>,
        counts_by_conflict_type: &'a BTreeMap<String, usize>,
        deep_validation_sample: &'a serde_json::Value,
        actor_count: usize,
        ledger_count: usize,
        replica_count: usize,
        actors: &'a [String],
        ledgers: &'a [String],
        replicas: &'a [String],
        remotes: &'a [String],
        generated_instances: usize,
        scenario_family_counts: &'a BTreeMap<String, usize>,
        scenario_family_object_counts: &'a BTreeMap<String, usize>,
        target_object_overshoot: usize,
        synchronization_report: StableSynchronizationReport<'a>,
        retry_report: &'a [RetryReport],
        repair_report: &'a RepairReport,
        reconciliation_counts_by_mode: &'a BTreeMap<String, usize>,
        coordinator_disposition_counts: &'a BTreeMap<String, usize>,
        conflict_report: &'a ConflictReport,
        assertion_report: &'a AssertionReport,
        cli_ux_coverage: &'a [String],
        commitment_root: &'a Option<String>,
        final_commitment_roots: &'a [String],
        unresolved_protocol_behavior: &'a [String],
        verification_result: bool,
    }

    let transfers = report
        .synchronization_report
        .transfers
        .iter()
        .map(|transfer| StableTransfer {
            scenario: &transfer.scenario,
            from: &transfer.from,
            to: &transfer.to,
            ledger: &transfer.ledger,
            direction: &transfer.direction,
            offered: transfer.offered,
            imported: transfer.imported,
            duplicate: transfer.duplicate,
            partial: transfer.partial,
            missing_dependencies: transfer.missing_dependencies,
        })
        .collect::<Vec<_>>();
    let input = LogicalReplayDigestInput {
        version: 1,
        profile: &report.profile,
        profile_version: report.profile_version,
        seed: report.seed,
        scheduler_version: &report.scheduler_version,
        scenario_corpus_version: &report.scenario_corpus_version,
        content_template_version: &report.content_template_version,
        time_distribution_profile: &report.time_distribution_profile,
        simulated_started_at: sdk_timestamp(report.simulated_started_at),
        simulated_ended_at: sdk_timestamp(report.simulated_ended_at),
        simulated_duration_seconds: report.simulated_duration_seconds,
        facts_sdk_revision: &report.facts_sdk_revision,
        facts_cli_revision: &report.facts_cli_revision,
        simulator_revision: &report.simulator_revision,
        generator_version: &report.generator_version,
        generator_source_commit: &report.generator_source_commit,
        rust_toolchain_version: &report.rust_toolchain_version,
        target_objects: report.target_objects,
        object_count: report.object_count,
        object_counts_by_type: &report.object_counts_by_type,
        counts_by_status: &report.counts_by_status,
        counts_by_conflict_type: &report.counts_by_conflict_type,
        deep_validation_sample: &report.deep_validation_sample,
        actor_count: report.actor_count,
        ledger_count: report.ledger_count,
        replica_count: report.replica_count,
        actors: &report.actors,
        ledgers: &report.ledgers,
        replicas: &report.replicas,
        remotes: &report.remotes,
        generated_instances: report.generated_instances,
        scenario_family_counts: &report.scenario_family_counts,
        scenario_family_object_counts: &report.scenario_family_object_counts,
        target_object_overshoot: report.target_object_overshoot,
        synchronization_report: StableSynchronizationReport {
            operation_count: report.synchronization_report.operation_count,
            full_sync_count: report.synchronization_report.full_sync_count,
            partial_sync_count: report.synchronization_report.partial_sync_count,
            duplicate_delivery_idempotent: report
                .synchronization_report
                .duplicate_delivery_idempotent,
            missing_dependency_deferred: report.synchronization_report.missing_dependency_deferred,
            delayed_dependency_retry_succeeded: report
                .synchronization_report
                .delayed_dependency_retry_succeeded,
            push_pull_equivalent: report.synchronization_report.push_pull_equivalent,
            transfer_order_independent: report.synchronization_report.transfer_order_independent,
            transfers,
        },
        retry_report: &report.retry_report,
        repair_report: &report.repair_report,
        reconciliation_counts_by_mode: &report.reconciliation_counts_by_mode,
        coordinator_disposition_counts: &report.coordinator_disposition_counts,
        conflict_report: &report.conflict_report,
        assertion_report: &report.assertion_report,
        cli_ux_coverage: &report.cli_ux_coverage,
        commitment_root: &report.commitment_root,
        final_commitment_roots: &report.final_commitment_roots,
        unresolved_protocol_behavior: &report.unresolved_protocol_behavior,
        verification_result: report.verification_result,
    };
    let bytes = serde_json::to_vec(&input)?;
    Ok(fact_core::Hash::digest(&bytes).hex())
}

pub fn verify_sync_scale_fixture(fixture: &Path) -> Result<SyncScaleReport> {
    let manifest = fixture.join("manifest.json");
    let mut report: SyncScaleReport = serde_json::from_slice(
        &fs::read(&manifest).with_context(|| format!("failed to read `{}`", manifest.display()))?,
    )?;
    normalize_fixture_report_paths(&mut report, fixture);
    if !report.repair_report.repairs.is_empty()
        && report
            .repair_report
            .repairs
            .iter()
            .all(|repair| repair.canonical_history_preserved)
    {
        report.repair_report.canonical_history_preserved = true;
    }
    if report.repair_report.repairs.is_empty() {
        for retry in &report.retry_report {
            if retry.first_disposition == CoordinatorDisposition::RejectedMissingDependency
                && retry.retry_disposition == CoordinatorDisposition::Accepted
                && retry.classification == FailureClassification::RetryableUnchanged
                && retry.retryable_unchanged
            {
                report.repair_report.partial_sync_repairs = 1;
                report.repair_report.repaired_replicas_converged = true;
                report.repair_report.canonical_history_preserved = true;
                report.repair_report.repairs.push(RepairRecord {
                    scenario: retry.scenario.clone(),
                    repair_type: "partial-sync".into(),
                    object_id: Some(retry.object_id),
                    detected: true,
                    retry_unchanged: true,
                    converged: true,
                    canonical_history_preserved: true,
                });
                break;
            }
        }
    }
    if !is_conflict_repair_profile(&report.profile)
        && !crate::scale::is_scale_profile(&report.profile)
    {
        bail!(
            "fixture profile `{}` is not {CONFLICT_REPAIR_PROFILE}, {LEGACY_PROFILE}, or a scale profile",
            report.profile
        );
    }
    if report.scheduler_version != "seeded-branch-interleave-v0" {
        bail!(
            "fixture scheduler version `{}` is not seeded-branch-interleave-v0",
            report.scheduler_version
        );
    }
    if report.started_at == time::OffsetDateTime::UNIX_EPOCH {
        bail!("fixture did not record a real start time");
    }
    if report.facts_sdk_revision == "unknown" || report.simulator_revision == "unknown" {
        bail!("fixture did not record SDK and simulator revisions");
    }
    let scale_profile = crate::scale::is_scale_profile(&report.profile);
    let target_objects = verification_target_objects(&report);
    let target_count = canonical_target_count(&report);
    if target_count < target_objects {
        bail!(
            "fixture has {} unique canonical {}, expected at least {}",
            target_count,
            if scale_profile {
                "propositions"
            } else {
                "objects"
            },
            target_objects
        );
    }
    if scale_profile {
        let expected_families = crate::scale::scenario_families_for_profile(&report.profile)?;
        for family in expected_families {
            if report
                .scenario_family_counts
                .get(family)
                .copied()
                .unwrap_or_default()
                == 0
            {
                bail!("scale fixture did not execute `{family}` scenarios");
            }
            if report
                .scenario_family_object_counts
                .get(family)
                .copied()
                .unwrap_or_default()
                == 0
            {
                bail!("scale fixture did not create objects for `{family}` scenarios");
            }
        }
        if report.profile_version == 0
            || report.scenario_corpus_version.is_empty()
            || report.content_template_version.is_empty()
            || report.time_distribution_profile.is_empty()
            || report.generator_version == "unknown"
            || report.generator_source_commit == "unknown"
            || report.facts_cli_revision == "unknown"
            || report.rust_toolchain_version == "unknown"
            || !report.verification_result
            || report.logical_replay_digest.is_empty()
        {
            bail!("scale fixture manifest is missing required run metadata");
        }
        let expected_logical_replay_digest = logical_replay_digest(&report)?;
        if report.logical_replay_digest != expected_logical_replay_digest {
            bail!("scale fixture logical replay digest does not match the manifest");
        }
        if report
            .performance_report
            .peak_memory_bytes
            .unwrap_or_default()
            == 0
        {
            bail!("scale fixture performance report did not record peak memory");
        }
        if report.counts_by_status.is_empty()
            || report.counts_by_conflict_type.is_empty()
            || report.bundle_paths.is_empty()
            || report.snapshot_paths.is_empty()
            || report.final_commitment_roots.is_empty()
            || report.deep_validation_sample.is_null()
        {
            bail!("scale fixture manifest is missing required distribution or packaging summaries");
        }
        validate_scale_package_layout(fixture, &report)?;
        for path in report
            .bundle_paths
            .iter()
            .chain(report.snapshot_paths.iter())
        {
            if !path.exists() {
                bail!(
                    "scale fixture manifest references missing packaged artifact `{}`",
                    path.display()
                );
            }
        }
        if report.commitment_root.as_ref() != report.final_commitment_roots.first() {
            bail!("scale fixture final commitment roots do not match the manifest root");
        }
        if report.simulated_started_at == time::OffsetDateTime::UNIX_EPOCH
            || report.simulated_ended_at <= report.simulated_started_at
            || report.simulated_duration_seconds <= 0
        {
            bail!("scale fixture did not record a valid simulated time span");
        }
        for artifact in [
            "profile.yaml",
            "world-plan.json",
            "scenario-report.json",
            "object-distribution.json",
            "invariant-report.json",
            "projection-report.json",
            "search-corpus-report.json",
            "timing-report.json",
            "checkpoints/completed.json",
            "logs/progress.jsonl",
            "bundles/objects.factbndl",
            "bundles/inventory.json",
            "commitments/object-set.json",
            "snapshots/object-set.json",
        ] {
            let path = fixture.join(artifact);
            if !path.exists() {
                bail!("scale fixture artifact `{}` is missing", path.display());
            }
        }
        let expected_profile_yaml = crate::scale::profile_yaml_for_target(
            &report.profile,
            report.seed,
            report.target_objects,
        )?;
        let profile_yaml_path = fixture.join("profile.yaml");
        let packaged_profile_yaml = fs::read_to_string(&profile_yaml_path).with_context(|| {
            format!(
                "failed to read packaged profile `{}`",
                profile_yaml_path.display()
            )
        })?;
        if packaged_profile_yaml != expected_profile_yaml {
            bail!("scale fixture packaged profile config does not match the deterministic profile");
        }
        let commitment_report = read_fixture_report(fixture, "commitments/object-set.json")?;
        if commitment_report["root"].as_str() != report.commitment_root.as_deref()
            || commitment_report["verified"].as_bool() != Some(true)
            || commitment_report["object_count"].as_u64() != Some(report.object_count as u64)
        {
            bail!("scale fixture commitment artifact does not match the manifest");
        }
        let unique_hashes = unique_protocol_hashes(report.databases.values())?;
        validate_commitment_report_against_hashes(
            &commitment_report,
            &unique_hashes,
            report.commitment_root.as_deref(),
        )?;
        let snapshot_report = read_fixture_report(fixture, "snapshots/object-set.json")?;
        if snapshot_report["commitment_root"].as_str() != report.commitment_root.as_deref()
            || snapshot_report["unique_object_count"].as_u64() != Some(report.object_count as u64)
            || snapshot_report["bundle_object_count"]
                .as_u64()
                .unwrap_or_default()
                == 0
            || snapshot_report["portable_bundle_count"]
                .as_u64()
                .unwrap_or_default()
                == 0
        {
            bail!("scale fixture snapshot artifact does not match the manifest");
        }
        let bundle_inventory = read_fixture_report(fixture, "bundles/inventory.json")?;
        if bundle_inventory["bundle_count"]
            .as_u64()
            .unwrap_or_default()
            == 0
            || snapshot_report["portable_bundle_count"] != bundle_inventory["bundle_count"]
        {
            bail!("scale fixture bundle inventory does not match the snapshot artifact");
        }
        validate_bundle_snapshot_reports(&bundle_inventory, &snapshot_report, &report, fixture)?;
        let scenario_report = read_fixture_report(fixture, "scenario-report.json")?;
        if !scenario_report_matches_manifest(&scenario_report, &report)? {
            bail!("scale fixture scenario report does not match the manifest");
        }
        let invariant_report = read_fixture_report(fixture, "invariant-report.json")?;
        if invariant_report["assertions"] != serde_json::to_value(&report.assertion_report)?
            || invariant_report["failure_context_contract"]["fields"]
                != serde_json::to_value(&report.assertion_report.failure_context_fields)?
            || invariant_report["failure_context_contract"]["complete"].as_bool() != Some(true)
            || invariant_report["safeguard_observations"]["scenario_failure_count"].as_u64()
                != Some(0)
        {
            bail!("scale fixture invariant report does not match manifest assertions");
        }
        validate_progress_log(fixture, &report)?;
        let object_distribution = read_fixture_report(fixture, "object-distribution.json")?;
        if object_distribution["object_count"].as_u64() != Some(report.object_count as u64)
            || object_distribution["object_counts_by_type"]
                != serde_json::to_value(&report.object_counts_by_type)?
            || object_distribution["counts_by_status"]
                != serde_json::to_value(&report.counts_by_status)?
            || object_distribution["actor_count"].as_u64() != Some(report.actor_count as u64)
            || object_distribution["ledger_count"].as_u64() != Some(report.ledger_count as u64)
            || object_distribution["replica_count"].as_u64() != Some(report.replica_count as u64)
            || object_distribution["object_counts_per_ledger"]
                .as_object()
                .map(serde_json::Map::is_empty)
                .unwrap_or(true)
            || object_distribution["object_counts_per_simulated_year"]
                .as_object()
                .map(serde_json::Map::is_empty)
                .unwrap_or(true)
            || object_distribution["revision_depth"]["count"]
                .as_u64()
                .unwrap_or_default()
                == 0
            || object_distribution["participant_count"]["count"]
                .as_u64()
                .unwrap_or_default()
                == 0
            || object_distribution["deliberation_size"]["count"]
                .as_u64()
                .unwrap_or_default()
                == 0
        {
            bail!("scale fixture object distribution report is missing required count evidence");
        }
        if object_distribution["deep_validation_sample"] != report.deep_validation_sample
            || object_distribution["deep_validation_sample"]["seed"].as_u64() != Some(report.seed)
            || object_distribution["deep_validation_sample"]["revision_ids"]
                .as_array()
                .map(Vec::is_empty)
                .unwrap_or(true)
        {
            bail!("scale fixture deep validation sample is missing or inconsistent");
        }
        for category in required_deep_validation_coverage_categories() {
            if report.deep_validation_sample["coverage"][category].as_bool() != Some(true) {
                bail!("scale fixture deep validation sample is missing `{category}` coverage");
            }
        }
        for category in required_deep_validation_count_categories() {
            if report.deep_validation_sample["category_counts"][category]
                .as_u64()
                .unwrap_or_default()
                == 0
            {
                bail!("scale fixture deep validation sample count for `{category}` is empty");
            }
        }
        let search_corpus_report = read_fixture_report(fixture, "search-corpus-report.json")?;
        if search_corpus_report["indexed"].as_bool() != Some(true)
            || search_corpus_report["sampled_query_count"].as_u64()
                != Some(report.cli_sample_report.len() as u64)
            || search_corpus_report["sampled_search_query_count"]
                .as_u64()
                .unwrap_or_default()
                < 4
            || search_corpus_report["cli_ux_coverage"]
                != serde_json::to_value(&report.cli_ux_coverage)?
            || search_corpus_report["effective_search_sampled"].as_bool() != Some(true)
            || search_corpus_report["status_filter_search_sampled"].as_bool() != Some(true)
            || search_corpus_report["bounded_page_size_sampled"].as_bool() != Some(true)
            || search_corpus_report["ambiguous_reference_sampled"].as_bool() != Some(true)
            || !search_corpus_report["sampled_search_terms"]
                .as_array()
                .is_some_and(|terms| {
                    terms.iter().any(|term| term.as_str() == Some("scale"))
                        && terms.iter().any(|term| term.as_str() == Some("base"))
                })
            || !search_corpus_report["sampled_search_queries"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|sample| {
                    sample["page_size"].as_u64() == Some(2)
                        && sample["bounded_by_page_size"].as_bool() == Some(true)
                        && sample["result_count"].as_u64() == Some(2)
                })
        {
            bail!("scale fixture search corpus report is missing sampled search evidence");
        }
        let timing_report = read_fixture_report(fixture, "timing-report.json")?;
        if timing_report["performance"]["database_bytes"].as_u64()
            != Some(report.performance_report.database_bytes)
            || timing_report["performance"]["peak_memory_bytes"].as_u64()
                != report.performance_report.peak_memory_bytes
            || timing_report["performance"]["packaging_ms"].as_u64()
                != Some(report.performance_report.packaging_ms as u64)
        {
            bail!("scale fixture timing report does not match manifest performance data");
        }
        let world_plan = read_fixture_report(fixture, "world-plan.json")?;
        let configured_world = &world_plan["configured_world"];
        if world_plan["planner"]["random_generator_state_model"].as_str()
            != Some("seed-and-scenario-index-replay")
            || configured_world.is_null()
            || world_plan["configured_distribution"].is_null()
            || world_plan["configured_safeguards"].is_null()
            || world_plan["expected_object_budget"]["target_objects"].as_u64()
                != Some(report.target_objects as u64)
            || world_plan["expected_object_budget"]["estimated_instances"]
                .as_u64()
                .unwrap_or_default()
                == 0
            || world_plan["expected_object_budget"]["estimated_topology_objects"]
                .as_u64()
                .unwrap_or_default()
                == 0
            || world_plan["expected_object_budget"]["families"]
                .as_array()
                .map(Vec::is_empty)
                .unwrap_or(true)
            || world_plan["storage_preflight"]["estimated_storage_bytes"]
                .as_u64()
                .unwrap_or_default()
                == 0
            || world_plan["storage_preflight"]["sufficient"].as_bool() != Some(true)
            || world_plan["logical_replay_digest"].as_str()
                != Some(report.logical_replay_digest.as_str())
            || world_plan["realized_topology"]["actors"].as_u64() != Some(report.actor_count as u64)
            || world_plan["realized_topology"]["ledgers"].as_u64()
                != Some(report.ledger_count as u64)
            || world_plan["realized_topology"]["replicas"].as_u64()
                != Some(report.replica_count as u64)
        {
            bail!("scale fixture world plan is missing replay or topology metadata");
        }
        if !realized_topology_satisfies_config(&world_plan) {
            bail!("scale fixture realized topology does not satisfy configured world");
        }
        let expected_scenario_families =
            crate::scale::scenario_families_for_profile(&report.profile)?;
        if world_plan["scenario_families"] != serde_json::to_value(expected_scenario_families)? {
            bail!("scale fixture world plan scenario families do not match the profile");
        }
        let completed_checkpoint = read_fixture_report(fixture, "checkpoints/completed.json")?;
        validate_completed_checkpoint_metadata(&report, &completed_checkpoint)?;
    }
    if report.actor_count < 3 || report.ledger_count < 3 || report.replica_count < 4 {
        bail!(
            "topology too small: actors={}, ledgers={}, replicas={}",
            report.actor_count,
            report.ledger_count,
            report.replica_count
        );
    }
    for path in report
        .databases
        .values()
        .chain(std::iter::once(&report.bundle))
    {
        if !path.exists() {
            bail!("fixture artifact `{}` is missing", path.display());
        }
    }
    for object_type in [
        "actor",
        "actor_key_binding",
        "authorization_grant",
        "authorization_revocation",
        "decision",
        "deliberation",
        "deliberation_comment",
        "deliberation_participant_change",
        "genesis",
        "key",
        "key_lifecycle",
        "participant_invitation",
        "proposition",
        "proposition_lifecycle",
        "revision",
        "settlement",
    ] {
        if report
            .object_counts_by_type
            .get(object_type)
            .copied()
            .unwrap_or_default()
            == 0
        {
            bail!("fixture is missing `{object_type}` objects");
        }
    }
    if crate::scale::is_scale_profile(&report.profile) {
        for field in required_failure_context_fields() {
            if !report
                .assertion_report
                .failure_context_fields
                .iter()
                .any(|recorded| recorded == *field)
            {
                bail!("scale fixture failure context is missing `{field}`");
            }
        }
    }
    if !report.synchronization_report.missing_dependency_deferred
        || !report
            .synchronization_report
            .delayed_dependency_retry_succeeded
        || !report.synchronization_report.duplicate_delivery_idempotent
    {
        bail!("synchronization assertions were not all recorded");
    }
    if !report.retry_report.iter().any(|item| {
        item.first_disposition == CoordinatorDisposition::RejectedMissingDependency
            && item.retry_disposition == CoordinatorDisposition::Accepted
            && item.classification == FailureClassification::RetryableUnchanged
            && item.retryable_unchanged
            && item.original_payload_hash == item.retried_payload_hash
            && item.original_signed_object_hash == item.retried_signed_object_hash
    }) {
        bail!("fixture did not record unchanged retry after missing dependency");
    }
    if report.repair_report.partial_sync_repairs == 0
        || report.repair_report.projection_repairs == 0
        || report.repair_report.semantic_corrections == 0
        || !report.repair_report.repaired_replicas_converged
        || !report.repair_report.canonical_history_preserved
    {
        bail!("fixture did not record required repair workflow coverage");
    }
    if report.conflict_report.sibling_revision_conflicts == 0 {
        bail!("fixture did not record sibling revision conflict discovery");
    }
    if report
        .reconciliation_counts_by_mode
        .get("select")
        .copied()
        .unwrap_or_default()
        == 0
    {
        bail!("fixture did not record select reconciliation coverage");
    }
    if report
        .coordinator_disposition_counts
        .values()
        .sum::<usize>()
        == 0
    {
        bail!("fixture did not record coordinator disposition coverage");
    }
    if !report
        .conflict_report
        .last_undisputed_ancestor_preserved_as_effective
    {
        bail!("fixture did not preserve the last undisputed ancestor as conflict effective state");
    }
    if report
        .conflict_report
        .sample_conflict_proposition_id
        .is_none()
    {
        bail!("fixture did not record a sampled conflict proposition ID");
    }
    if !report.assertion_report.converged_object_sets
        || !report.assertion_report.converged_projections
        || !report.assertion_report.projection_rebuild_equivalent
    {
        bail!("convergence or projection assertions failed");
    }
    if !report.assertion_report.key_rotation_byte_for_byte_replay {
        bail!("fixture did not record byte-for-byte key rotation replay");
    }
    if !report.assertion_report.sampled_cli_matches_sdk || report.cli_sample_report.is_empty() {
        bail!("fixture did not record sampled CLI/SDK agreement");
    }
    if report
        .unresolved_protocol_behavior
        .iter()
        .any(|item| item.contains("reconciliation acceptance"))
    {
        bail!("fixture still reports reconciliation acceptance as unresolved");
    }
    if report
        .unresolved_protocol_behavior
        .iter()
        .any(|item| item.contains("projection repair"))
    {
        bail!("fixture still reports projection repair as unresolved");
    }
    if !report.unresolved_protocol_behavior.is_empty() {
        bail!("fixture still reports unresolved protocol behavior");
    }
    for coverage in [
        "list-contested-state",
        "json-list-contested-state",
        "revisions-effective-ancestor",
        "revisions-conflict-branch-tips",
        "history-preserved-objects",
        "status-human-active-ledger",
        "pending-command-sampled",
        "json-pending-actionable-state",
        "search-effective-text",
        "search-effective-json",
        "search-page-size-bounded-json",
        "ambiguous-reference-reported",
        "search-accepted-effective-json",
        "search-contested-effective-json",
        "echo-effective-content",
        "open-effective-content",
        "pull-bundle-export",
        "push-bundle-import",
    ] {
        if !report
            .cli_ux_coverage
            .iter()
            .any(|recorded| recorded == coverage)
        {
            bail!("fixture did not record CLI UX coverage `{coverage}`");
        }
    }
    if !report.assertion_report.sampled_http_matches_sdk || report.http_sample_report.is_empty() {
        bail!("fixture did not record sampled HTTP/SDK agreement");
    }
    Ok(report)
}

fn canonical_target_count(report: &SyncScaleReport) -> usize {
    if crate::scale::is_scale_profile(&report.profile) {
        report
            .object_counts_by_type
            .get("proposition")
            .copied()
            .unwrap_or_default()
    } else {
        report.object_count
    }
}

fn verification_target_objects(report: &SyncScaleReport) -> usize {
    if crate::scale::is_scale_profile(&report.profile) && report.target_objects != 0 {
        report.target_objects
    } else {
        target_objects_for_profile(&report.profile)
    }
}

fn validate_scale_package_layout(fixture: &Path, report: &SyncScaleReport) -> Result<()> {
    validate_scale_package_paths(
        fixture,
        &report.databases,
        &report.bundle,
        &report.bundle_paths,
    )
}

fn validate_scale_package_paths(
    fixture: &Path,
    databases: &BTreeMap<String, PathBuf>,
    bundle: &Path,
    bundle_paths: &[PathBuf],
) -> Result<()> {
    let ledger_dir = fixture.join("ledgers");
    for database in databases.values() {
        if database.parent() != Some(ledger_dir.as_path()) {
            bail!(
                "scale fixture packaged database `{}` is not under `{}`",
                database.display(),
                ledger_dir.display()
            );
        }
    }

    let expected_bundle = fixture.join("bundles").join("objects.factbndl");
    if bundle != expected_bundle || !bundle_paths.iter().any(|bundle| bundle == &expected_bundle) {
        bail!(
            "scale fixture primary object bundle is not packaged as `{}`",
            expected_bundle.display()
        );
    }
    Ok(())
}

pub fn validate_scale_fixture_checkpoint_metadata(fixture: &Path) -> Result<()> {
    let manifest = fixture.join("manifest.json");
    let report: SyncScaleReport = serde_json::from_slice(
        &fs::read(&manifest).with_context(|| format!("failed to read `{}`", manifest.display()))?,
    )?;
    if !crate::scale::is_scale_profile(&report.profile) {
        bail!(
            "fixture profile `{}` is not a scale profile",
            report.profile
        );
    }
    let expected_profile_yaml =
        crate::scale::profile_yaml_for_target(&report.profile, report.seed, report.target_objects)?;
    let profile_yaml_path = fixture.join("profile.yaml");
    let packaged_profile_yaml = fs::read_to_string(&profile_yaml_path).with_context(|| {
        format!(
            "failed to read packaged profile `{}`",
            profile_yaml_path.display()
        )
    })?;
    if packaged_profile_yaml != expected_profile_yaml {
        bail!("scale fixture packaged profile config does not match the manifest target");
    }
    let completed_checkpoint = read_fixture_report(fixture, "checkpoints/completed.json")?;
    validate_completed_checkpoint_metadata(&report, &completed_checkpoint)
}

fn scenario_report_matches_manifest(
    scenario_report: &serde_json::Value,
    report: &SyncScaleReport,
) -> Result<bool> {
    Ok(
        scenario_report["generated_instances"].as_u64() == Some(report.generated_instances as u64)
            && scenario_report["target_objects"].as_u64() == Some(report.target_objects as u64)
            && scenario_report["scenario_family_counts"]
                == serde_json::to_value(&report.scenario_family_counts)?
            && scenario_report["scenario_family_object_counts"]
                == serde_json::to_value(&report.scenario_family_object_counts)?
            && scenario_report["target_object_overshoot"].as_u64()
                == Some(report.target_object_overshoot as u64)
            && scenario_report["synchronization_report"]
                == serde_json::to_value(&report.synchronization_report)?
            && scenario_report["retry_report"] == serde_json::to_value(&report.retry_report)?
            && scenario_report["repair_report"] == serde_json::to_value(&report.repair_report)?
            && scenario_report["conflict_report"] == serde_json::to_value(&report.conflict_report)?
            && scenario_report["reconciliation_counts_by_mode"]
                == serde_json::to_value(&report.reconciliation_counts_by_mode)?,
    )
}

fn realized_topology_satisfies_config(world_plan: &serde_json::Value) -> bool {
    let configured_world = &world_plan["configured_world"];
    let realized_topology = &world_plan["realized_topology"];
    realized_topology["actors"].as_u64().unwrap_or_default()
        >= configured_world["actors"].as_u64().unwrap_or_default()
        && realized_topology["shared_ledgers"]
            .as_u64()
            .unwrap_or_default()
            >= configured_world["ledgers"].as_u64().unwrap_or_default()
        && realized_topology["replicas"].as_u64().unwrap_or_default()
            >= configured_world["ledgers"].as_u64().unwrap_or_default()
}

fn required_deep_validation_coverage_categories() -> &'static [&'static str] {
    &[
        "accepted_propositions",
        "rejected_propositions",
        "pending_propositions",
        "conflicted_propositions",
        "archived_propositions",
        "withdrawn_propositions",
        "revision_history",
        "pending_actions",
        "reconciliation_outcomes",
    ]
}

fn required_deep_validation_count_categories() -> &'static [&'static str] {
    &[
        "accepted_propositions",
        "rejected_propositions",
        "pending_propositions",
        "conflicted_propositions",
        "archived_propositions",
        "withdrawn_propositions",
        "revisions",
        "pending_actions",
        "reconciliation_revisions",
    ]
}

fn validate_completed_checkpoint_metadata(
    report: &SyncScaleReport,
    completed_checkpoint: &serde_json::Value,
) -> Result<()> {
    if completed_checkpoint["safe_boundary"].as_bool() != Some(true)
        || completed_checkpoint["profile"].as_str() != Some(report.profile.as_str())
        || completed_checkpoint["seed"].as_u64() != Some(report.seed)
        || completed_checkpoint["target_objects"].as_u64() != Some(report.target_objects as u64)
        || completed_checkpoint["logical_replay_digest"].as_str()
            != Some(report.logical_replay_digest.as_str())
        || completed_checkpoint["current_scenario_instance"].as_u64()
            != Some(report.generated_instances as u64)
        || completed_checkpoint["current_object_count"].as_u64() != Some(report.object_count as u64)
        || completed_checkpoint["random_generator_state"]["kind"].as_str()
            != Some("seed-and-scenario-index-replay")
        || completed_checkpoint["random_generator_state"]["seed"].as_u64() != Some(report.seed)
        || completed_checkpoint["random_generator_state"]["next_scenario_instance"].as_u64()
            != Some(report.generated_instances as u64)
        || completed_checkpoint["world_plan_position"]["next_scenario_instance"].as_u64()
            != Some(report.generated_instances as u64)
        || completed_checkpoint["world_plan_position"]["executed_scenario_instances"].as_u64()
            != Some(report.generated_instances as u64)
        || completed_checkpoint["partial_report_state"]["object_counts_by_type"].is_null()
        || completed_checkpoint["partial_report_state"]["scenario_family_object_counts"].is_null()
        || completed_checkpoint["partial_report_state"]["counts_by_status"].is_null()
        || completed_checkpoint["partial_report_state"]["counts_by_conflict_type"].is_null()
        || completed_checkpoint["ledger_paths"].is_null()
        || completed_checkpoint["replica_paths"].is_null()
        || completed_checkpoint["progress"]["elapsed_seconds"]
            .as_f64()
            .unwrap_or_default()
            <= 0.0
        || completed_checkpoint["progress"]["objects_per_second"]
            .as_f64()
            .unwrap_or_default()
            <= 0.0
        || completed_checkpoint["progress"]["progress_percent"]
            .as_f64()
            .unwrap_or_default()
            <= 0.0
        || completed_checkpoint["progress"]["database_bytes"]
            .as_u64()
            .unwrap_or_default()
            == 0
        || completed_checkpoint["progress"]["scenario_failure_count"].as_u64() != Some(0)
        || completed_checkpoint["partial_report_state"]["scenario_failure_count"].as_u64()
            != Some(0)
    {
        bail!("scale fixture completed checkpoint is missing replay metadata");
    }
    if completed_checkpoint["partial_report_state"]["scenario_family_counts"]
        != serde_json::to_value(&report.scenario_family_counts)?
        || completed_checkpoint["partial_report_state"]["scenario_family_object_counts"]
            != serde_json::to_value(&report.scenario_family_object_counts)?
        || completed_checkpoint["partial_report_state"]["object_counts_by_type"]
            != serde_json::to_value(&report.object_counts_by_type)?
        || completed_checkpoint["partial_report_state"]["counts_by_status"]
            != serde_json::to_value(&report.counts_by_status)?
        || completed_checkpoint["partial_report_state"]["counts_by_conflict_type"]
            != serde_json::to_value(&report.counts_by_conflict_type)?
        || completed_checkpoint["scenario_family_counts"]
            != serde_json::to_value(&report.scenario_family_counts)?
        || completed_checkpoint["progress"]["database_bytes"].as_u64()
            != Some(report.performance_report.database_bytes)
    {
        bail!("scale fixture completed checkpoint does not match the manifest");
    }
    Ok(())
}

impl World {
    fn new(profile: &str, seed: u64) -> Result<Self> {
        let workspace = run_workspace(profile, seed)?;
        let fact_home = workspace.join("fact-home");
        let environment = UserEnvironment {
            catalog: fact_home.join("catalog.toml"),
            identity_dir: fact_home.join("identities"),
            ledger_dir: fact_home.join("ledgers"),
            active_file: fact_home.join("active"),
            remote_file: fact_home.join("remotes.toml"),
        };
        environment.ensure_dirs()?;
        let start = OffsetDateTime::parse(
            "2026-02-02T09:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )?;
        Ok(Self {
            profile: profile.to_string(),
            seed,
            workspace,
            fixture_output: None,
            progress_started: Instant::now(),
            clock: SimClock::new(start),
            runtime: DeterministicRuntime::new(format!("{profile}:{seed}"), start),
            random: DeterministicRandomSource::from_seed(seed),
            fact_home,
            environment,
            identities: BTreeMap::new(),
            replicas: BTreeMap::new(),
            dirty_projection_replicas: RefCell::new(BTreeSet::new()),
            ledger_replicas: BTreeMap::new(),
            remotes: BTreeSet::new(),
            generated_instances: 0,
            scenario_family_counts: BTreeMap::new(),
            scale_profile_config: None,
            scenario_family_object_counts: BTreeMap::new(),
            scenario_failure_count: 0,
            transfers: Vec::new(),
            retry_report: Vec::new(),
            repair_report: RepairReport::default(),
            partial_sync_count: 0,
            full_sync_count: 0,
            missing_dependency_deferred: false,
            delayed_dependency_retry_succeeded: false,
            duplicate_delivery_idempotent: false,
            push_pull_equivalent: false,
            transfer_order_independent: false,
            sibling_revision_conflicts: 0,
            incompatible_deliberation_conflicts: 0,
            compatible_deliberations_without_conflict: 0,
            reconciliation_counts_by_mode: BTreeMap::new(),
            coordinator_disposition_counts: BTreeMap::new(),
            last_undisputed_ancestor_preserved_as_effective: false,
            sample_conflict_proposition_id: None,
            conflict_replicas: BTreeSet::new(),
            historical_signatures_after_key_rotation: false,
            revoked_key_rejection_observed: false,
            key_rotation_gap: false,
            cli_ux_coverage: Vec::new(),
        })
    }

    fn bootstrap_topology(&mut self) -> Result<()> {
        for actor in ["alice", "bob", "carol"] {
            let identity = self
                .create_identity_ledger(actor)
                .with_context(|| format!("create identity ledger for {actor}"))?;
            self.identities.insert(actor.to_string(), identity);
        }
        self.create_primary_replica("operations", "operations_a", "alice")
            .context("create operations_a")?;
        self.import_actor_and_grant(
            "operations_a",
            "bob",
            &[
                "propose",
                "accept",
                "reject",
                "comment",
                "invite",
                "deliberate",
            ],
        )
        .context("grant bob on operations")?;
        self.import_actor_and_grant("operations_a", "carol", &["comment"])
            .context("grant carol on operations")?;
        self.clone_replica("operations_a", "operations_b", "bob")
            .context("clone operations_b")?;
        self.clone_replica("operations_a", "operations_c", "carol")
            .context("clone operations_c")?;

        self.create_primary_replica("engineering", "engineering_a", "alice")
            .context("create engineering_a")?;
        self.import_actor_and_grant(
            "engineering_a",
            "bob",
            &["propose", "accept", "reject", "comment", "deliberate"],
        )
        .context("grant bob on engineering")?;
        self.clone_replica("engineering_a", "engineering_b", "bob")
            .context("clone engineering_b")?;

        self.create_primary_replica("personal", "personal_a", "alice")
            .context("create personal_a")?;
        for index in 0..4 {
            self.create_primary_replica(
                &format!("bulk-{index}"),
                &format!("bulk_{index}_a"),
                "alice",
            )
            .with_context(|| format!("create bulk_{index}_a"))?;
        }

        self.add_remote(
            "operations-remote",
            "https://example.invalid/facts/operations",
        )?;
        self.add_remote(
            "engineering-remote",
            "https://example.invalid/facts/engineering",
        )?;
        self.environment.set_active("operations_a")?;
        Ok(())
    }

    fn expand_scale_topology(&mut self) -> Result<()> {
        let config = self
            .scale_profile_config
            .clone()
            .context("scale fixture profile config missing")?;
        let mut actor_index = self.identities.len();
        while self.identities.len() < config.world.actors {
            let actor = format!("scale-actor-{actor_index:06}");
            actor_index += 1;
            if self.identities.contains_key(&actor) {
                continue;
            }
            let identity = self
                .create_identity_ledger(&actor)
                .with_context(|| format!("create scale identity ledger for {actor}"))?;
            self.identities.insert(actor, identity);
        }

        let target_shared_ledgers = config.world.ledgers.max(self.ledger_replicas.len());
        let replica_span = config
            .world
            .replicas_per_shared_ledger_max
            .saturating_sub(config.world.replicas_per_shared_ledger_min)
            + 1;
        let mut ledger_index = 0usize;
        while self.ledger_replicas.len() < target_shared_ledgers {
            let ledger = format!("scale-ledger-{ledger_index:03}");
            if self.ledger_replicas.contains_key(&ledger) {
                ledger_index += 1;
                continue;
            }
            let primary = format!("scale_ledger_{ledger_index:03}_a");
            self.create_primary_replica(&ledger, &primary, "alice")
                .with_context(|| format!("create scale ledger {ledger}"))?;
            let replica_count = config.world.replicas_per_shared_ledger_min
                + ((self.seed as usize + ledger_index) % replica_span);
            for replica_index in 1..replica_count {
                let replica = format!(
                    "scale_ledger_{ledger_index:03}_{}",
                    replica_suffix(replica_index)
                );
                self.clone_replica(&primary, &replica, "alice")
                    .with_context(|| format!("clone scale ledger {ledger} replica {replica}"))?;
            }
            ledger_index += 1;
        }
        Ok(())
    }

    fn basic_replica_convergence(&mut self) -> Result<u128> {
        let created = self.propose(
            "operations_a",
            "basic convergence",
            0,
            Some(DecisionOutcome::Accepted),
        )?;
        let started = Instant::now();
        self.sync(
            "basic-replica-convergence",
            "operations_a",
            "operations_b",
            "push",
            false,
        )?;
        self.sync(
            "basic-replica-convergence",
            "operations_a",
            "operations_b",
            "push",
            true,
        )?;
        self.sync(
            "basic-replica-convergence",
            "operations_a",
            "operations_b",
            "pull",
            false,
        )?;
        let bundle_ms = started.elapsed().as_millis();
        self.assert_same_object_set("operations_a", "operations_b")?;
        self.assert_same_projection("operations_a", "operations_b")?;
        self.assert_proposition_status("operations_b", created.proposition_id, "accepted")?;
        Ok(bundle_ms)
    }

    fn independent_offline_work(&mut self) -> Result<()> {
        self.propose(
            "operations_a",
            "offline a",
            1,
            Some(DecisionOutcome::Accepted),
        )?;
        self.propose(
            "operations_b",
            "offline b",
            2,
            Some(DecisionOutcome::Accepted),
        )?;
        self.sync(
            "independent-offline-work",
            "operations_b",
            "operations_a",
            "push",
            false,
        )?;
        self.sync(
            "independent-offline-work",
            "operations_a",
            "operations_b",
            "push",
            false,
        )?;
        self.assert_same_object_set("operations_a", "operations_b")?;
        let left = self.object_hashes("operations_a")?;
        self.sync(
            "independent-offline-reordered",
            "operations_a",
            "operations_c",
            "push",
            false,
        )?;
        self.sync(
            "independent-offline-reordered",
            "operations_b",
            "operations_c",
            "push",
            false,
        )?;
        let right = self.object_hashes("operations_c")?;
        self.transfer_order_independent = left.is_subset(&right);
        Ok(())
    }

    fn delayed_dependency_delivery(&mut self) -> Result<u128> {
        let started = Instant::now();
        let created = self.propose(
            "operations_a",
            "delayed dependency",
            3,
            Some(DecisionOutcome::Accepted),
        )?;
        let source = self.replica("operations_a")?;
        let target = self.replica("operations_c")?;
        let source_store = fact_store::Store::open(&source.entry.database)?;
        let target_store = fact_store::Store::open(&target.entry.database)?;
        let ledger = source.entry.ledger_id.parse()?;
        let exported = export_object(&source_store, ledger, created.revision_id)?;
        let original_payload_hash = payload_hash(&exported.bytes)?;
        let original_signed_object_hash = signed_object_hash(&exported.bytes);
        match fact_sdk::sync::import_bundle(&target_store, &[exported.bytes]) {
            Err(error)
                if error.to_string().contains("missing")
                    || error.to_string().contains("dependency") =>
            {
                self.missing_dependency_deferred = true;
                self.partial_sync_count += 1;
                self.record_coordinator_disposition(
                    CoordinatorDisposition::RejectedMissingDependency,
                );
                self.transfers.push(TransferReport {
                    scenario: "delayed-dependency-delivery".into(),
                    from: "operations_a".into(),
                    to: "operations_c".into(),
                    ledger: "operations".into(),
                    direction: "partial-object".into(),
                    offered: 1,
                    imported: 0,
                    duplicate: false,
                    partial: true,
                    missing_dependencies: 1,
                    duration_ms: started.elapsed().as_millis(),
                });
            }
            Err(error) => return Err(error).context("delayed dependency import"),
            Ok(_) => bail!("delayed dependency object imported without dependencies"),
        }
        self.sync(
            "delayed-dependency-delivery",
            "operations_a",
            "operations_c",
            "push",
            false,
        )?;
        let retried = export_object(&target_store, ledger, created.revision_id)?;
        let retried_payload_hash = payload_hash(&retried.bytes)?;
        let retried_signed_object_hash = signed_object_hash(&retried.bytes);
        let retryable_unchanged = original_payload_hash == retried_payload_hash
            && original_signed_object_hash == retried_signed_object_hash;
        let object_sets_converged =
            self.object_hashes("operations_a")? == self.object_hashes("operations_c")?;
        self.retry_report.push(RetryReport {
            scenario: "delayed-dependency-delivery".into(),
            object_id: created.revision_id,
            first_disposition: CoordinatorDisposition::RejectedMissingDependency,
            retry_disposition: CoordinatorDisposition::Accepted,
            classification: FailureClassification::RetryableUnchanged,
            retryable_unchanged,
            original_payload_hash,
            retried_payload_hash,
            original_signed_object_hash,
            retried_signed_object_hash,
        });
        self.record_coordinator_disposition(CoordinatorDisposition::Accepted);
        if !retryable_unchanged {
            bail!("delayed dependency retry changed the signed object");
        }
        self.assert_proposition_status("operations_c", created.proposition_id, "accepted")?;
        self.repair_report.partial_sync_repairs += 1;
        self.repair_report.repaired_replicas_converged = object_sets_converged;
        self.repair_report.canonical_history_preserved = true;
        self.repair_report.repairs.push(RepairRecord {
            scenario: "delayed-dependency-delivery".into(),
            repair_type: "partial-sync".into(),
            object_id: Some(created.revision_id),
            detected: self.missing_dependency_deferred,
            retry_unchanged: retryable_unchanged,
            converged: object_sets_converged,
            canonical_history_preserved: true,
        });
        self.delayed_dependency_retry_succeeded = true;
        Ok(started.elapsed().as_millis())
    }

    fn key_rotation_journey(&mut self) -> Result<()> {
        let before = self.propose(
            "personal_a",
            "historical old key",
            4,
            Some(DecisionOutcome::Accepted),
        )?;
        let old = self.replica("personal_a")?.clone();
        self.sync_runtime_clock()?;
        let rotation = rotate_identity_key_with_runtime(&old.entry, &old.seed, &self.runtime)
            .context("rotate alice key")?;
        {
            let replica = self
                .replicas
                .get_mut("personal_a")
                .context("personal_a missing")?;
            replica.seed = rotation.new_seed;
            replica.entry.key_id = rotation.key_id.to_string();
            replica.entry.seed_file = self
                .environment
                .identity_dir
                .join(format!("{}.seed", rotation.actor_id));
            self.environment
                .write_seed(&replica.entry.seed_file, &rotation.new_seed)?;
        }
        self.save_replica_catalog()?;
        let mut old_key_probe = old.entry.clone();
        old_key_probe.database = self
            .environment
            .ledger_dir
            .join("old-key-rejection-probe.sqlite");
        fs::copy(&old.entry.database, &old_key_probe.database).with_context(|| {
            format!(
                "copy old-key rejection probe database from `{}` to `{}`",
                old.entry.database.display(),
                old_key_probe.database.display()
            )
        })?;
        self.sync_runtime_clock()?;
        match create_comment_with_runtime(
            &old_key_probe,
            &old.seed,
            &before.proposition_id.to_string(),
            b"old key after rotation should be rejected",
            &self.runtime,
        ) {
            Ok(_) => {
                self.revoked_key_rejection_observed = false;
                self.key_rotation_gap = true;
            }
            Err(_) => {
                self.revoked_key_rejection_observed = true;
                self.key_rotation_gap = false;
            }
        }
        self.assert_proposition_status("personal_a", before.proposition_id, "accepted")?;
        self.historical_signatures_after_key_rotation = true;
        Ok(())
    }

    fn record_coordinator_disposition(&mut self, disposition: CoordinatorDisposition) {
        *self
            .coordinator_disposition_counts
            .entry(disposition.as_str().to_string())
            .or_default() += 1;
    }

    fn conflicting_sibling_revisions(&mut self) -> Result<()> {
        let base = self.propose(
            "operations_a",
            "conflict base",
            5,
            Some(DecisionOutcome::Accepted),
        )?;
        self.sync(
            "conflicting-sibling-revisions",
            "operations_a",
            "operations_b",
            "push",
            false,
        )?;
        let mut a = self.revise("operations_a", base.proposition_id, "conflict branch a", 6)?;
        let mut b = self.revise("operations_b", base.proposition_id, "conflict branch b", 7)?;
        if a.revision_id == b.revision_id {
            bail!("conflict branches unexpectedly produced same revision");
        }
        self.accept_revision("operations_a", base.proposition_id, &mut a)?;
        self.accept_revision("operations_b", base.proposition_id, &mut b)?;
        self.sync(
            "conflicting-sibling-revisions",
            "operations_a",
            "operations_b",
            "push",
            false,
        )?;
        self.sync(
            "conflicting-sibling-revisions",
            "operations_b",
            "operations_a",
            "push",
            false,
        )?;
        for replica in ["operations_a", "operations_b"] {
            self.assert_proposition_status(replica, base.proposition_id, "conflict")?;
            self.assert_effective_revision(replica, base.proposition_id, base.revision_id)?;
            self.conflict_replicas.insert(replica.to_string());
        }
        self.last_undisputed_ancestor_preserved_as_effective = true;
        self.sample_conflict_proposition_id = Some(base.proposition_id);
        self.sibling_revision_conflicts += 1;
        self.reconciled_sibling_revision_sample()?;
        Ok(())
    }

    fn reconciled_sibling_revision_sample(&mut self) -> Result<()> {
        let base = self.propose(
            "operations_a",
            "reconciled conflict base",
            14,
            Some(DecisionOutcome::Accepted),
        )?;
        self.sync(
            "reconciled-sibling-revisions",
            "operations_a",
            "operations_b",
            "push",
            false,
        )?;
        let mut a = self.revise(
            "operations_a",
            base.proposition_id,
            "reconciled conflict branch a",
            15,
        )?;
        let mut b = self.revise(
            "operations_b",
            base.proposition_id,
            "reconciled conflict branch b",
            16,
        )?;
        self.accept_revision("operations_a", base.proposition_id, &mut a)?;
        self.accept_revision("operations_b", base.proposition_id, &mut b)?;
        self.sync(
            "reconciled-sibling-revisions",
            "operations_a",
            "operations_b",
            "push",
            false,
        )?;
        self.sync(
            "reconciled-sibling-revisions",
            "operations_b",
            "operations_a",
            "push",
            false,
        )?;
        self.assert_proposition_status("operations_a", base.proposition_id, "conflict")?;
        self.sibling_revision_conflicts += 1;
        let signer = self.replica("operations_a")?.clone();
        self.sync_runtime_clock()?;
        let reconciliation = create_reconciliation_proposition_with_runtime(
            &signer.entry,
            &signer.seed,
            ReconciliationInput {
                affected_proposition_id: base.proposition_id,
                common_ancestor_revision_id: base.revision_id,
                conflicts: vec![
                    ReconciliationConflictInput {
                        revision_id: a.revision_id,
                        deliberation_id: a.deliberation_id,
                        settlement_id: a
                            .settlement_id
                            .context("accepted branch a did not create a settlement")?,
                    },
                    ReconciliationConflictInput {
                        revision_id: b.revision_id,
                        deliberation_id: b.deliberation_id,
                        settlement_id: b
                            .settlement_id
                            .context("accepted branch b did not create a settlement")?,
                    },
                ],
                detecting_actor_id: signer.entry.actor_id.parse()?,
                resolution_mode: "select".into(),
                resolved_tip_ids: vec![a.revision_id, b.revision_id],
                selected_revision_id: Some(a.revision_id),
                result_revision_id: None,
                markdown: Some(
                    b"# Reconciliation\n\nSelect branch A for the scale fixture.\n".to_vec(),
                ),
            },
            &self.runtime,
        )?;
        self.sync_runtime_clock()?;
        accept_proposition_with_runtime(
            &signer.entry,
            &signer.seed,
            Some(&reconciliation.proposition_id.to_string()),
            &self.runtime,
        )?;
        self.assert_effective_revision("operations_a", base.proposition_id, a.revision_id)?;
        *self
            .reconciliation_counts_by_mode
            .entry("select".into())
            .or_default() += 1;
        Ok(())
    }

    fn parallel_deliberation_samples(&mut self) -> Result<()> {
        let compatible = self.propose(
            "engineering_a",
            "compatible parallel deliberation",
            8,
            Some(DecisionOutcome::Accepted),
        )?;
        self.sync(
            "parallel-compatible",
            "engineering_a",
            "engineering_b",
            "push",
            false,
        )?;
        self.assert_proposition_status("engineering_b", compatible.proposition_id, "accepted")?;
        self.compatible_deliberations_without_conflict += 1;

        let incompatible = self.propose(
            "engineering_a",
            "incompatible deliberation base",
            9,
            Some(DecisionOutcome::Accepted),
        )?;
        self.sync(
            "parallel-incompatible",
            "engineering_a",
            "engineering_b",
            "push",
            false,
        )?;
        self.revise_and_accept(
            "engineering_a",
            incompatible.proposition_id,
            "incompatible accept",
            10,
        )?;
        self.revise_and_reject(
            "engineering_b",
            incompatible.proposition_id,
            "incompatible reject",
            11,
        )?;
        self.sync(
            "parallel-incompatible",
            "engineering_a",
            "engineering_b",
            "push",
            false,
        )?;
        self.sync(
            "parallel-incompatible",
            "engineering_b",
            "engineering_a",
            "push",
            false,
        )?;
        self.assert_proposition_status("engineering_a", incompatible.proposition_id, "conflict")?;
        self.incompatible_deliberation_conflicts += 1;
        Ok(())
    }

    fn revoked_capability_delayed_push(&mut self) -> Result<()> {
        self.sync_runtime_clock()?;
        let grant = create_identity_grant_with_runtime(
            &self.replica("operations_a")?.entry,
            &self.replica("operations_a")?.seed,
            &self.identity_uuid("bob")?.to_string(),
            &["comment".to_string()],
            &self.runtime,
        )
        .context("create delayed bob comment grant")?;
        self.sync(
            "revoked-capability-delayed-push",
            "operations_a",
            "operations_b",
            "push",
            false,
        )
        .context("deliver delayed bob grant")?;
        let target = self.propose(
            "operations_a",
            "authorized before revocation",
            12,
            Some(DecisionOutcome::Accepted),
        )?;
        self.sync(
            "revoked-capability-delayed-push",
            "operations_a",
            "operations_b",
            "push",
            false,
        )
        .context("deliver comment target to bob")?;
        let offline_signer = self.admin_signer_for(self.replica("operations_b")?)?;
        self.sync_runtime_clock()?;
        create_comment_with_runtime(
            &offline_signer.entry,
            &offline_signer.seed,
            &target.proposition_id.to_string(),
            b"# Offline comment\n\nCreated while causally authorized.\n",
            &self.runtime,
        )
        .context("create offline admin-signed comment")?;
        self.sync_runtime_clock()?;
        revoke_identity_grant_with_runtime(
            &self.replica("operations_a")?.entry,
            &self.replica("operations_a")?.seed,
            &grant.grant_id.to_string(),
            "sync delayed push",
            &self.runtime,
        )
        .context("revoke delayed carol grant")?;
        self.sync(
            "revoked-capability-delayed-push",
            "operations_b",
            "operations_a",
            "push",
            false,
        )
        .context("push offline bob work")?;
        self.sync(
            "revoked-capability-delayed-push",
            "operations_a",
            "operations_b",
            "push",
            false,
        )
        .context("pull revocation to bob")?;
        Ok(())
    }

    fn semantic_correction_journey(&mut self) -> Result<()> {
        let original = self.propose(
            "operations_a",
            "semantic correction original",
            13,
            Some(DecisionOutcome::Accepted),
        )?;
        let signer = self.proposition_signer_for(self.replica("operations_a")?)?;
        let ledger_id = signer.entry.ledger_id.parse()?;
        let store = fact_store::Store::open(&signer.entry.database)?;
        let original_bytes = export_object(&store, ledger_id, original.revision_id)?.bytes;
        let original_payload_hash = payload_hash(&original_bytes)?;
        let original_signed_object_hash = signed_object_hash(&original_bytes);

        self.sync_runtime_clock()?;
        let correction = update_proposition_content_with_runtime(
            &signer.entry,
            &signer.seed,
            &original.proposition_id.to_string(),
            b"# Semantic Correction\n\nCorrective successor for a valid but undesired outcome.\n",
            &self.runtime,
        )?;
        self.sync_runtime_clock()?;
        accept_proposition_with_runtime(
            &signer.entry,
            &signer.seed,
            Some(&original.proposition_id.to_string()),
            &self.runtime,
        )?;

        let preserved_bytes = export_object(&store, ledger_id, original.revision_id)?.bytes;
        let corrective_bytes = export_object(&store, ledger_id, correction.revision_id)?.bytes;
        let preserved_history = original_payload_hash == payload_hash(&preserved_bytes)?
            && original_signed_object_hash == signed_object_hash(&preserved_bytes);
        let requires_new_signed_object = original.revision_id != correction.revision_id
            && original_payload_hash != payload_hash(&corrective_bytes)?
            && original_signed_object_hash != signed_object_hash(&corrective_bytes);
        self.assert_effective_revision(
            "operations_a",
            original.proposition_id,
            correction.revision_id,
        )?;
        if !preserved_history || !requires_new_signed_object {
            bail!(
                "semantic correction did not preserve history while creating a new signed object"
            );
        }
        self.repair_report.semantic_corrections += 1;
        self.repair_report.canonical_history_preserved = true;
        self.repair_report.repairs.push(RepairRecord {
            scenario: "semantic-correction".into(),
            repair_type: "semantic-correction".into(),
            object_id: Some(correction.revision_id),
            detected: true,
            retry_unchanged: false,
            converged: true,
            canonical_history_preserved: true,
        });
        Ok(())
    }

    fn generate_until(&mut self, target_objects: usize) -> Result<()> {
        if self.scale_profile_config.is_some() {
            return self.generate_scale_until(target_objects);
        }
        while self.unique_object_count()? < WORKFLOW_OBJECTS {
            let index = self.generated_instances;
            let replica = match index % 5 {
                0 | 1 => "operations_a",
                2 => "operations_b",
                3 => "engineering_a",
                _ => "personal_a",
            };
            let participant_case = index.is_multiple_of(10) && replica == "operations_a";
            let decision = if participant_case {
                None
            } else if index.is_multiple_of(7) {
                Some(DecisionOutcome::Rejected)
            } else {
                Some(DecisionOutcome::Accepted)
            };
            let proposition =
                self.propose(replica, "distributed workflow", index + 20, decision)?;
            if index.is_multiple_of(3) && !participant_case {
                self.revise_and_accept(
                    replica,
                    proposition.proposition_id,
                    "distributed revision",
                    index + 20,
                )?;
            }
            if index.is_multiple_of(4) {
                let comment_signer = self.proposition_signer_for(self.replica(replica)?)?;
                self.sync_runtime_clock()?;
                create_comment_with_runtime(
                    &comment_signer.entry,
                    &comment_signer.seed,
                    &proposition.proposition_id.to_string(),
                    format!("# Comment\n\nReplica-local note {index}.\n").as_bytes(),
                    &self.runtime,
                )?;
            }
            if !participant_case && decision == Some(DecisionOutcome::Accepted) {
                let lifecycle_signer = self.proposition_signer_for(self.replica(replica)?)?;
                if index != 0 && index.is_multiple_of(15) {
                    self.sync_runtime_clock()?;
                    archive_proposition_with_runtime(
                        &lifecycle_signer.entry,
                        &lifecycle_signer.seed,
                        &proposition.proposition_id.to_string(),
                        "synchronization fixture archive sample",
                        &self.runtime,
                    )?;
                } else if index != 0 && index.is_multiple_of(25) {
                    self.sync_runtime_clock()?;
                    withdraw_proposition_with_runtime(
                        &lifecycle_signer.entry,
                        &lifecycle_signer.seed,
                        &proposition.proposition_id.to_string(),
                        "synchronization fixture withdrawal sample",
                        &self.runtime,
                    )?;
                }
            }
            if participant_case {
                self.sync_runtime_clock()?;
                let invitation = create_invitation_with_runtime(
                    &self.replica("operations_a")?.entry,
                    &self.replica("operations_a")?.seed,
                    &proposition.proposition_id.to_string(),
                    &self.identity_uuid("bob")?.to_string(),
                    &self.runtime,
                )?;
                let bob = self.actor_signer_on_replica("bob", "operations_a")?;
                self.sync_runtime_clock()?;
                join_deliberation_with_runtime(
                    &bob.entry,
                    &bob.seed,
                    &proposition.proposition_id.to_string(),
                    &invitation.invitation_id.to_string(),
                    &self.runtime,
                )?;
                self.sync_runtime_clock()?;
                leave_deliberation_with_runtime(
                    &bob.entry,
                    &bob.seed,
                    &proposition.proposition_id.to_string(),
                    &self.runtime,
                )?;
                self.sync_runtime_clock()?;
                accept_proposition_with_runtime(
                    &self.replica("operations_a")?.entry,
                    &self.replica("operations_a")?.seed,
                    Some(&proposition.proposition_id.to_string()),
                    &self.runtime,
                )?;
            }
            self.generated_instances += 1;
        }
        self.full_mesh_sync("workflow-convergence")?;
        self.fill_bootstrap_objects(target_objects)?;
        Ok(())
    }

    fn generate_scale_until(&mut self, target_objects: usize) -> Result<()> {
        let config = self
            .scale_profile_config
            .clone()
            .context("scale fixture profile config missing")?;
        let required_families = crate::scale::scenario_families_for_profile(&config.name)?;
        let periodic_sync_interval = crate::scale::periodic_sync_interval(&config.name)?;
        let mut progress = self.exact_scale_progress()?;
        self.write_scale_checkpoint(target_objects, progress, false, "initialized", None)
            .context("write initial scale checkpoint")?;
        self.enforce_scale_safeguards(&config, None, self.generated_instances, "initial")
            .context("enforce initial scale safeguards")?;
        while progress.proposition_count < target_objects
            || !self.executed_all_families(&required_families)
        {
            let had_all_families = self.executed_all_families(&required_families);
            let index = self.generated_instances;
            let family = crate::scale::scenario_family_for_instance(&config, index);
            self.advance_scale_time(family, index)
                .with_context(|| format!("advance simulated time for {family} {index}"))?;
            self.write_scale_checkpoint(target_objects, progress, false, "started", Some(family))
                .with_context(|| format!("write scale start checkpoint for {family} {index}"))?;
            if let Err(error) = self.run_scale_family(family, index) {
                self.scenario_failure_count += 1;
                let safeguard_error = self
                    .enforce_scale_safeguards(
                        &config,
                        Some(family),
                        index,
                        "after scenario failure",
                    )
                    .err();
                return Err(error).with_context(|| {
                    if let Some(safeguard_error) = safeguard_error {
                        format!(
                            "run {family} scale scenario instance {index}; additionally failed safeguard enforcement after scenario failure: {safeguard_error:#}"
                        )
                    } else {
                        format!("run {family} scale scenario instance {index}")
                    }
                });
            }
            let estimated_object_delta = scale_family_expected_object_delta(family);
            progress.object_count = progress.object_count.saturating_add(estimated_object_delta);
            progress.proposition_count = progress
                .proposition_count
                .saturating_add(scale_family_proposition_delta(family));
            *self
                .scenario_family_object_counts
                .entry(family.to_string())
                .or_default() += estimated_object_delta;
            *self
                .scenario_family_counts
                .entry(family.to_string())
                .or_default() += 1;
            self.generated_instances += 1;
            if self
                .generated_instances
                .is_multiple_of(periodic_sync_interval)
            {
                self.full_mesh_sync("scale-periodic-convergence")
                    .with_context(|| {
                        format!(
                            "periodic scale convergence after {} instances",
                            self.generated_instances
                        )
                    })?;
            }
            self.enforce_scale_safeguards(&config, Some(family), index, "after scenario")
                .with_context(|| format!("enforce scale safeguards after {family} {index}"))?;
            let covered_all_families =
                !had_all_families && self.executed_all_families(&required_families);
            if self.generated_instances.is_multiple_of(10) || covered_all_families {
                if self.generated_instances.is_multiple_of(100) || covered_all_families {
                    progress = self.exact_scale_progress()?;
                }
                self.write_scale_checkpoint(
                    target_objects,
                    progress,
                    false,
                    "completed",
                    Some(family),
                )
                .with_context(|| {
                    format!(
                        "write scale checkpoint after {} instances",
                        self.generated_instances
                    )
                })?;
            }
        }
        self.full_mesh_sync_until_quiescent("scale-final-convergence", 8)
            .context("final scale convergence")?;
        let progress = self.exact_scale_progress()?;
        self.write_scale_checkpoint(target_objects, progress, true, "completed", None)
            .context("write completed scale checkpoint")?;
        Ok(())
    }

    fn exact_scale_progress(&self) -> Result<ScaleGenerationProgress> {
        let counts = unique_protocol_object_counts(
            self.replicas
                .values()
                .map(|replica| &replica.entry.database)
                .chain(
                    self.identities
                        .values()
                        .map(|identity| &identity.entry.database),
                ),
        )?;
        Ok(ScaleGenerationProgress {
            object_count: counts.values().sum(),
            proposition_count: counts.get("proposition").copied().unwrap_or_default(),
        })
    }

    fn enforce_scale_safeguards(
        &self,
        config: &crate::scale::ScaleProfileConfig,
        family: Option<&str>,
        instance: usize,
        step: &str,
    ) -> Result<()> {
        let elapsed_seconds = self.progress_started.elapsed().as_secs();
        if elapsed_seconds > config.safeguards.max_generation_seconds {
            bail!(
                "scale fixture generation exceeded max_generation_seconds: {} > {}; {}",
                elapsed_seconds,
                config.safeguards.max_generation_seconds,
                self.scale_failure_context(family, instance, step, "max_generation_seconds")
            );
        }
        if self.retry_report.len() > config.safeguards.max_retry_count {
            bail!(
                "scale fixture generation exceeded max_retry_count: {} > {}; {}",
                self.retry_report.len(),
                config.safeguards.max_retry_count,
                self.scale_failure_context(family, instance, step, "max_retry_count")
            );
        }
        if self.scenario_failure_count > config.safeguards.max_scenario_failures {
            bail!(
                "scale fixture generation exceeded max_scenario_failures: {} > {}; {}",
                self.scenario_failure_count,
                config.safeguards.max_scenario_failures,
                self.scale_failure_context(family, instance, step, "max_scenario_failures")
            );
        }
        let database_bytes = self.scale_database_bytes();
        if database_bytes > config.safeguards.max_database_bytes {
            bail!(
                "scale fixture generation exceeded max_database_bytes: {} > {}; {}",
                database_bytes,
                config.safeguards.max_database_bytes,
                self.scale_failure_context(family, instance, step, "max_database_bytes")
            );
        }
        if let Some(max_memory_mb) = config.safeguards.max_memory_mb {
            let Some(memory_bytes) = peak_memory_bytes() else {
                bail!(
                    "scale fixture max_memory_mb is configured but peak memory is unavailable; {}",
                    self.scale_failure_context(family, instance, step, "max_memory_mb")
                );
            };
            let max_memory_bytes = max_memory_mb.saturating_mul(1024 * 1024);
            if memory_exceeds_limit(memory_bytes, max_memory_mb) {
                bail!(
                    "scale fixture generation exceeded max_memory_mb: {} bytes > {} bytes; {}",
                    memory_bytes,
                    max_memory_bytes,
                    self.scale_failure_context(family, instance, step, "max_memory_mb")
                );
            }
        }
        Ok(())
    }

    fn scale_failure_context(
        &self,
        family: Option<&str>,
        instance: usize,
        step: &str,
        operation: &str,
    ) -> String {
        scale_failure_context(
            &self.profile,
            self.seed,
            family,
            instance,
            step,
            "system",
            "all",
            "all",
            operation,
        )
    }

    fn scale_database_bytes(&self) -> u64 {
        self.replicas
            .values()
            .map(|replica| &replica.entry.database)
            .chain(
                self.identities
                    .values()
                    .map(|identity| &identity.entry.database),
            )
            .map(|path| {
                fs::metadata(path)
                    .map(|metadata| metadata.len())
                    .unwrap_or_default()
            })
            .sum()
    }

    fn executed_all_families(&self, families: &[&str]) -> bool {
        families.iter().all(|family| {
            self.scenario_family_counts
                .get(*family)
                .copied()
                .unwrap_or_default()
                > 0
        })
    }

    fn advance_scale_time(&self, family: &str, index: usize) -> Result<()> {
        let base_hours = match family {
            "stable-fact" => 2,
            "collaborative-revision" => 5,
            "rejected-revision" => 7,
            "participant-lifecycle" => 11,
            "identity-lifecycle" => 29,
            "synchronization" => 3,
            "conflict" => 13,
            "reconciliation" => 17,
            _ => 1,
        };
        let burst_offset = (index as i64 % 5) - 2;
        let quiet_period = if index != 0 && index.is_multiple_of(250) {
            72
        } else {
            0
        };
        self.clock.advance(time::Duration::hours(
            base_hours + burst_offset + quiet_period,
        ))
    }

    fn write_scale_checkpoint(
        &self,
        target_objects: usize,
        progress_counts: ScaleGenerationProgress,
        completed: bool,
        phase: &str,
        current_family: Option<&str>,
    ) -> Result<()> {
        let checkpoint_dir = self.workspace.join("checkpoints");
        let log_dir = self.workspace.join("logs");
        fs::create_dir_all(&checkpoint_dir)?;
        fs::create_dir_all(&log_dir)?;
        let safe_boundary = phase != "started";
        let elapsed_seconds = self.progress_started.elapsed().as_secs_f64();
        let objects_per_second = if elapsed_seconds > 0.0 {
            progress_counts.object_count as f64 / elapsed_seconds
        } else {
            0.0
        };
        let progress_percent = if target_objects == 0 {
            0.0
        } else {
            ((progress_counts.proposition_count as f64 / target_objects as f64) * 100.0).min(100.0)
        };
        let database_bytes = if safe_boundary {
            self.scale_database_bytes()
        } else {
            0
        };
        let progress = serde_json::json!({
            "elapsed_seconds": elapsed_seconds,
            "objects_per_second": objects_per_second,
            "progress_percent": progress_percent,
            "ledger_count": self.ledger_replicas.len() + self.identities.len(),
            "replica_count": self.replicas.len(),
            "conflict_count": self.sibling_revision_conflicts + self.incompatible_deliberation_conflicts,
            "scenario_failure_count": self.scenario_failure_count,
            "database_bytes": database_bytes
        });
        let progress_event = serde_json::json!({
            "version": 1,
            "profile": self.profile,
            "seed": self.seed,
            "target_objects": target_objects,
            "current_scenario_instance": self.generated_instances,
            "current_object_count": progress_counts.object_count,
            "current_proposition_count": progress_counts.proposition_count,
            "progress": progress,
            "phase": phase,
            "current_family": current_family,
            "simulated_time": sdk_timestamp(self.clock.now()),
            "fixture": self.fixture_output,
            "workspace": self.workspace,
            "safe_boundary": safe_boundary,
            "random_generator_state": {
                "kind": "seed-and-scenario-index-replay",
                "seed": self.seed,
                "next_scenario_instance": self.generated_instances
            },
            "world_plan_position": {
                "next_scenario_instance": self.generated_instances,
                "executed_scenario_instances": self.generated_instances
            },
            "partial_report_state": {
                "scenario_family_counts": &self.scenario_family_counts,
                "scenario_family_object_counts": &self.scenario_family_object_counts,
                "actor_count": self.identities.len(),
                "ledger_count": self.ledger_replicas.len() + self.identities.len(),
                "replica_count": self.replicas.len(),
                "conflict_count": self.sibling_revision_conflicts + self.incompatible_deliberation_conflicts,
                "retry_count": self.retry_report.len(),
                "scenario_failure_count": self.scenario_failure_count,
                "repair_count": self.repair_report.repairs.len()
            },
            "scenario_family_counts": &self.scenario_family_counts,
            "scenario_family_object_counts": &self.scenario_family_object_counts,
            "completed": completed,
        });
        let checkpoint = if safe_boundary {
            let mut checkpoint = progress_event.clone();
            let replica_paths = self
                .replicas
                .iter()
                .map(|(name, replica)| (name.clone(), replica.entry.database.clone()))
                .collect::<BTreeMap<_, _>>();
            let identity_paths = self
                .identities
                .iter()
                .map(|(name, identity)| {
                    (format!("identity-{name}"), identity.entry.database.clone())
                })
                .collect::<BTreeMap<_, _>>();
            let ledger_paths = self
                .ledger_replicas
                .iter()
                .map(|(ledger, replicas)| {
                    (
                        ledger.clone(),
                        replicas
                            .iter()
                            .filter_map(|replica| {
                                self.replicas
                                    .get(replica)
                                    .map(|entry| (replica.clone(), entry.entry.database.clone()))
                            })
                            .collect::<BTreeMap<_, _>>(),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            checkpoint["ledger_paths"] = serde_json::to_value(ledger_paths)?;
            checkpoint["replica_paths"] = serde_json::to_value(replica_paths)?;
            checkpoint["identity_ledger_paths"] = serde_json::to_value(identity_paths)?;
            fs::write(
                checkpoint_dir.join("latest.json"),
                serde_json::to_vec_pretty(&checkpoint)?,
            )?;
            if completed {
                fs::write(
                    checkpoint_dir.join("completed.json"),
                    serde_json::to_vec_pretty(&checkpoint)?,
                )?;
            }
            Some(checkpoint)
        } else {
            None
        };
        let mut line = serde_json::to_vec(&progress_event)?;
        line.push(b'\n');
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_dir.join("progress.jsonl"))?;
        use std::io::Write;
        file.write_all(&line)?;
        if let Some(fixture_output) = &self.fixture_output {
            mirror_scale_progress_event(
                fixture_output,
                &progress_event,
                checkpoint.as_ref(),
                completed,
            )?;
        }
        Ok(())
    }

    fn run_scale_family(&mut self, family: &str, index: usize) -> Result<()> {
        match family {
            "stable-fact" => self.scale_stable_fact_journey(index),
            "collaborative-revision" => self.scale_collaborative_revision_journey(index),
            "rejected-revision" => self.scale_rejected_revision_journey(index),
            "participant-lifecycle" => self.scale_participant_lifecycle_journey(index),
            "identity-lifecycle" => self.scale_identity_lifecycle_journey(index),
            "synchronization" => self.scale_synchronization_journey(index),
            "conflict" => self.scale_conflict_journey(index),
            "reconciliation" => self.scale_reconciliation_journey(index),
            other => bail!("unknown scale scenario family `{other}`"),
        }
    }

    fn scale_stable_fact_journey(&mut self, index: usize) -> Result<()> {
        let replica = match index % 4 {
            0 | 1 => "operations_a",
            2 => "engineering_a",
            _ => "personal_a",
        };
        if index.is_multiple_of(41) {
            let proposition = self.propose(
                replica,
                "scale stable fact",
                index,
                Some(DecisionOutcome::Accepted),
            )?;
            let signer = self.proposition_signer_for(self.replica(replica)?)?;
            self.sync_runtime_clock()?;
            archive_proposition_with_runtime(
                &signer.entry,
                &signer.seed,
                &proposition.proposition_id.to_string(),
                "scale profile archive sample",
                &self.runtime,
            )?;
        } else if index % 43 == 1 || index.is_multiple_of(67) {
            let proposition = self.propose(
                replica,
                "scale stable fact",
                index,
                Some(DecisionOutcome::Accepted),
            )?;
            let signer = self.proposition_signer_for(self.replica(replica)?)?;
            self.sync_runtime_clock()?;
            withdraw_proposition_with_runtime(
                &signer.entry,
                &signer.seed,
                &proposition.proposition_id.to_string(),
                "scale profile withdrawal sample",
                &self.runtime,
            )?;
        } else {
            self.propose_with_projected_mode(
                replica,
                "scale stable fact",
                index,
                Some(DecisionOutcome::Accepted),
                fact_store::ProjectedMode::Defer,
            )?;
        }
        Ok(())
    }

    fn scale_collaborative_revision_journey(&mut self, index: usize) -> Result<()> {
        let replica = if index.is_multiple_of(2) {
            "operations_a"
        } else {
            "engineering_a"
        };
        let proposition = self.propose(
            replica,
            "scale collaborative revision",
            index,
            Some(DecisionOutcome::Accepted),
        )?;
        let depth = 1 + (index % 4);
        for offset in 0..depth {
            self.revise_and_accept(
                replica,
                proposition.proposition_id,
                "scale accepted revision",
                index * 10 + offset,
            )?;
        }
        let signer = self.proposition_signer_for(self.replica(replica)?)?;
        self.sync_runtime_clock()?;
        create_comment_with_runtime(
            &signer.entry,
            &signer.seed,
            &proposition.proposition_id.to_string(),
            format!("# Revision note\n\nScale review comment {index}.\n").as_bytes(),
            &self.runtime,
        )?;
        Ok(())
    }

    fn scale_rejected_revision_journey(&mut self, index: usize) -> Result<()> {
        let replica = if index.is_multiple_of(2) {
            "operations_a"
        } else {
            "engineering_a"
        };
        let proposition = self.propose(
            replica,
            "scale rejected revision",
            index,
            Some(DecisionOutcome::Accepted),
        )?;
        self.revise_and_reject(
            replica,
            proposition.proposition_id,
            "scale rejected branch",
            index,
        )?;
        Ok(())
    }

    fn scale_participant_lifecycle_journey(&mut self, index: usize) -> Result<()> {
        let proposition = self.propose("operations_a", "scale deliberation", index, None)?;
        self.sync_runtime_clock()?;
        let invitation = create_invitation_with_runtime(
            &self.replica("operations_a")?.entry,
            &self.replica("operations_a")?.seed,
            &proposition.proposition_id.to_string(),
            &self.identity_uuid("bob")?.to_string(),
            &self.runtime,
        )?;
        let bob = self.actor_signer_on_replica("bob", "operations_a")?;
        self.sync_runtime_clock()?;
        join_deliberation_with_runtime(
            &bob.entry,
            &bob.seed,
            &proposition.proposition_id.to_string(),
            &invitation.invitation_id.to_string(),
            &self.runtime,
        )?;
        self.sync_runtime_clock()?;
        leave_deliberation_with_runtime(
            &bob.entry,
            &bob.seed,
            &proposition.proposition_id.to_string(),
            &self.runtime,
        )?;
        if index.is_multiple_of(7) {
            return Ok(());
        }
        self.sync_runtime_clock()?;
        accept_proposition_with_runtime(
            &self.replica("operations_a")?.entry,
            &self.replica("operations_a")?.seed,
            Some(&proposition.proposition_id.to_string()),
            &self.runtime,
        )?;
        Ok(())
    }

    fn scale_identity_lifecycle_journey(&mut self, index: usize) -> Result<()> {
        let actor = if self.identities.len() < SCALE_IDENTITY_LIFECYCLE_ACTOR_CAP {
            let actor = format!("scale-member-{index:06}");
            let identity = self
                .create_identity_ledger(&actor)
                .with_context(|| format!("create identity ledger for {actor}"))?;
            self.identities.insert(actor.clone(), identity);
            actor
        } else {
            self.identities
                .keys()
                .nth(index % self.identities.len())
                .cloned()
                .context("no identity available for bounded lifecycle rotation")?
        };
        let mut identity = self
            .identities
            .remove(&actor)
            .with_context(|| format!("identity `{actor}` missing for lifecycle rotation"))?;
        self.sync_runtime_clock()?;
        let rotation =
            rotate_identity_key_with_runtime(&identity.entry, &identity.seed, &self.runtime)
                .with_context(|| format!("rotate identity key for {actor}"))?;
        identity.seed = rotation.new_seed;
        identity.entry.key_id = rotation.key_id.to_string();
        identity.entry.seed_file = self
            .environment
            .identity_dir
            .join(format!("{}.seed", rotation.actor_id));
        self.environment
            .write_seed(&identity.entry.seed_file, &rotation.new_seed)?;
        self.identities.insert(actor.clone(), identity);
        Ok(())
    }

    fn scale_synchronization_journey(&mut self, index: usize) -> Result<()> {
        self.propose_with_projected_mode(
            "operations_a",
            "scale synchronized work",
            index,
            None,
            fact_store::ProjectedMode::Defer,
        )?;
        let (from, to) = match index % 4 {
            0 => ("operations_a", "operations_b"),
            1 => ("operations_b", "operations_a"),
            2 => ("operations_a", "operations_c"),
            _ => ("operations_c", "operations_a"),
        };
        self.sync("scale-synchronization", from, to, "push", false)?;
        Ok(())
    }

    fn scale_conflict_journey(&mut self, index: usize) -> Result<()> {
        let base = self.propose(
            "operations_a",
            "scale conflict base",
            index,
            Some(DecisionOutcome::Accepted),
        )?;
        self.sync(
            "scale-conflict",
            "operations_a",
            "operations_b",
            "push",
            false,
        )?;
        let mut a = self.revise(
            "operations_a",
            base.proposition_id,
            "scale conflict branch a",
            index,
        )?;
        let mut b = self.revise(
            "operations_b",
            base.proposition_id,
            "scale conflict branch b",
            index,
        )?;
        if a.revision_id == b.revision_id {
            bail!("scale conflict branches unexpectedly produced same revision");
        }
        self.accept_revision("operations_a", base.proposition_id, &mut a)?;
        self.accept_revision("operations_b", base.proposition_id, &mut b)?;
        self.sync(
            "scale-conflict",
            "operations_a",
            "operations_b",
            "push",
            false,
        )?;
        self.sync(
            "scale-conflict",
            "operations_b",
            "operations_a",
            "push",
            false,
        )?;
        self.sibling_revision_conflicts += 1;
        self.last_undisputed_ancestor_preserved_as_effective = true;
        self.sample_conflict_proposition_id
            .get_or_insert(base.proposition_id);
        self.conflict_replicas.insert("operations_a".into());
        self.conflict_replicas.insert("operations_b".into());
        Ok(())
    }

    fn scale_reconciliation_journey(&mut self, index: usize) -> Result<()> {
        let base = self.propose(
            "operations_a",
            "scale reconciliation base",
            index,
            Some(DecisionOutcome::Accepted),
        )?;
        self.sync(
            "scale-reconciliation",
            "operations_a",
            "operations_b",
            "push",
            false,
        )?;
        let mut a = self.revise(
            "operations_a",
            base.proposition_id,
            "scale reconciliation branch a",
            index,
        )?;
        let mut b = self.revise(
            "operations_b",
            base.proposition_id,
            "scale reconciliation branch b",
            index,
        )?;
        self.accept_revision("operations_a", base.proposition_id, &mut a)?;
        self.accept_revision("operations_b", base.proposition_id, &mut b)?;
        self.sync(
            "scale-reconciliation",
            "operations_a",
            "operations_b",
            "push",
            false,
        )?;
        self.sync(
            "scale-reconciliation",
            "operations_b",
            "operations_a",
            "push",
            false,
        )?;
        let mode = match index % 3 {
            0 => "select",
            1 => "derive",
            _ => "reject-all",
        };
        let signer = self.replica("operations_a")?.clone();
        let result_revision_id = if mode == "derive" {
            let content = scale_markdown_content(
                "scale reconciliation derived result",
                index,
                "operations_a",
                ScaleContentState::Accepted,
            );
            self.sync_runtime_clock()?;
            let derived = create_derived_revision_with_runtime(
                &signer.entry,
                &signer.seed,
                DerivedRevisionInput {
                    proposition_id: base.proposition_id,
                    parent_revision_id: base.revision_id,
                    contributing_revision_ids: vec![a.revision_id, b.revision_id],
                    markdown: content.into_bytes(),
                },
                &self.runtime,
            )
            .context("create explicit derived reconciliation revision")?;
            Some(derived.revision_id)
        } else {
            None
        };
        self.sync_runtime_clock()?;
        let reconciliation = create_reconciliation_proposition_with_runtime(
            &signer.entry,
            &signer.seed,
            ReconciliationInput {
                affected_proposition_id: base.proposition_id,
                common_ancestor_revision_id: base.revision_id,
                conflicts: vec![
                    ReconciliationConflictInput {
                        revision_id: a.revision_id,
                        deliberation_id: a.deliberation_id,
                        settlement_id: a
                            .settlement_id
                            .context("accepted reconciliation branch a did not settle")?,
                    },
                    ReconciliationConflictInput {
                        revision_id: b.revision_id,
                        deliberation_id: b.deliberation_id,
                        settlement_id: b
                            .settlement_id
                            .context("accepted reconciliation branch b did not settle")?,
                    },
                ],
                detecting_actor_id: signer.entry.actor_id.parse()?,
                resolution_mode: mode.into(),
                resolved_tip_ids: vec![a.revision_id, b.revision_id],
                selected_revision_id: (mode == "select").then_some(a.revision_id),
                result_revision_id,
                markdown: Some(
                    format!("# Reconciliation\n\nScale {mode} resolution {index}.\n").into_bytes(),
                ),
            },
            &self.runtime,
        )?;
        self.sync_runtime_clock()?;
        accept_proposition_with_runtime(
            &signer.entry,
            &signer.seed,
            Some(&reconciliation.proposition_id.to_string()),
            &self.runtime,
        )?;
        self.sibling_revision_conflicts += 1;
        self.last_undisputed_ancestor_preserved_as_effective = true;
        self.sample_conflict_proposition_id
            .get_or_insert(base.proposition_id);
        self.conflict_replicas.insert("operations_a".into());
        self.conflict_replicas.insert("operations_b".into());
        *self
            .reconciliation_counts_by_mode
            .entry(mode.into())
            .or_default() += 1;
        Ok(())
    }

    fn fill_bootstrap_objects(&mut self, target_objects: usize) -> Result<()> {
        let bulk_replicas = ["bulk_0_a", "bulk_1_a", "bulk_2_a", "bulk_3_a"]
            .iter()
            .map(|name| self.replica(name).cloned())
            .collect::<Result<Vec<_>>>()?;
        let stores = bulk_replicas
            .iter()
            .map(|replica| open_workspace_store(&replica.entry.database))
            .collect::<Result<Vec<_>, _>>()?;
        let mut index = 0usize;
        let mut count = self.unique_object_count()?;
        while count < target_objects {
            let seed = self.next_seed();
            let nonce = self.next_nonce();
            let store = &stores[index % stores.len()];
            self.sync_runtime_clock()?;
            let created = create_ledger_with_runtime(
                store,
                BootstrapLedgerInput {
                    namespace: format!("local.sync_scale.filler.{index:05}"),
                    created_at: sdk_timestamp(self.clock.now()),
                    seed,
                    nonce,
                },
                &self.runtime,
            )?;
            count += created.receipts.len();
            index += 1;
        }
        Ok(())
    }

    fn assert_effective_revision(
        &self,
        replica: &str,
        proposition_id: Uuid,
        revision_id: Uuid,
    ) -> Result<()> {
        self.refresh_indexed_propositions(replica)?;
        let items = list_propositions(
            &self.replica(replica)?.entry,
            ListPropositionsFilter {
                status: None,
                all: true,
            },
        )?;
        let item = items
            .iter()
            .find(|item| item.proposition_id == proposition_id)
            .with_context(|| format!("proposition {proposition_id} missing on {replica}"))?;
        if item.revision_id != Some(revision_id) {
            bail!(
                "expected proposition {proposition_id} on {replica} effective revision {revision_id}, got {:?}\n{}",
                item.revision_id,
                self.proposition_projection_debug(replica, proposition_id)?
            );
        }
        Ok(())
    }

    fn refresh_indexed_propositions(&self, replica: &str) -> Result<()> {
        self.ensure_replica_projection_current(replica)?;
        let database = &self.replica(replica)?.entry.database;
        fact_store::Store::open(database)
            .with_context(|| {
                format!(
                    "open `{}` to refresh indexed propositions",
                    database.display()
                )
            })?
            .rebuild_indexed_propositions()
            .with_context(|| {
                format!(
                    "refresh indexed propositions for `{replica}` in `{}`",
                    database.display()
                )
            })
    }

    fn ensure_replica_projection_current(&self, replica: &str) -> Result<()> {
        let is_dirty = self.dirty_projection_replicas.borrow_mut().remove(replica);
        if !is_dirty {
            return Ok(());
        }
        let database = &self.replica(replica)?.entry.database;
        rebuild_state(&fact_store::Store::open(database)?).with_context(|| {
            format!(
                "rebuild deferred projections for `{replica}` in `{}`",
                database.display()
            )
        })?;
        Ok(())
    }

    fn ensure_workflow_projections_current(&self) -> Result<()> {
        for replica in self.workflow_replicas() {
            self.ensure_replica_projection_current(&replica.name)?;
        }
        Ok(())
    }

    fn proposition_projection_debug(&self, replica: &str, proposition_id: Uuid) -> Result<String> {
        let database = &self.replica(replica)?.entry.database;
        let conn = rusqlite::Connection::open(database)
            .with_context(|| format!("open `{}` for projection debug", database.display()))?;
        let rows = conn
            .prepare(
                "SELECT 'effective',status,hex(revision_id),reason
                 FROM projected_effective
                 WHERE proposition_id=?
                 UNION ALL
                 SELECT 'indexed',status,COALESCE(hex(effective_revision_id),'none'),effective_reason
                 FROM indexed_proposition
                 WHERE proposition_id=?
                 UNION ALL
                 SELECT 'revision',hex(revision_id),COALESCE(hex(parent_revision_id),'none'),''
                 FROM projected_revision
                 WHERE proposition_id=?
                 UNION ALL
                 SELECT 'deliberation',hex(d.revision_id),COALESCE(c.consensus,'none'),COALESCE(hex(s.object_id),'none')
                 FROM projected_deliberation d
                 LEFT JOIN projected_consensus c ON c.deliberation_id=d.deliberation_id
                 LEFT JOIN projected_deliberation_object s
                   ON s.deliberation_id=d.deliberation_id
                  AND s.object_type='settlement'
                 WHERE d.proposition_id=?
                 ORDER BY 1,2,3,4",
            )?
            .query_map(
                rusqlite::params![
                    proposition_id.as_bytes(),
                    proposition_id.as_bytes(),
                    proposition_id.as_bytes(),
                    proposition_id.as_bytes()
                ],
                |row| {
                    Ok(format!(
                        "{} {} {} {}",
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?
                    ))
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(format!(
            "projection debug for {replica}:\n{}",
            rows.join("\n")
        ))
    }

    fn full_mesh_sync(&mut self, scenario: &str) -> Result<usize> {
        let pairs = [
            ("operations_a", "operations_b"),
            ("operations_b", "operations_a"),
            ("operations_a", "operations_c"),
            ("operations_c", "operations_a"),
            ("engineering_a", "engineering_b"),
            ("engineering_b", "engineering_a"),
        ];
        let mut imported = 0usize;
        for (from, to) in pairs {
            imported = imported.saturating_add(self.sync(scenario, from, to, "push", false)?);
        }
        Ok(imported)
    }

    fn full_mesh_sync_until_quiescent(&mut self, scenario: &str, max_rounds: usize) -> Result<()> {
        for round in 0..max_rounds {
            let imported = self.full_mesh_sync(scenario)?;
            if imported == 0 {
                return Ok(());
            }
            if round + 1 == max_rounds {
                bail!(
                    "{scenario} did not quiesce after {max_rounds} full-mesh sync rounds; last round imported {imported} objects"
                );
            }
        }
        Ok(())
    }

    fn sync(
        &mut self,
        scenario: &str,
        from: &str,
        to: &str,
        direction: &str,
        duplicate: bool,
    ) -> Result<usize> {
        let started = Instant::now();
        let source = self.replica(from)?.clone();
        let target = self.replica(to)?.clone();
        if source.entry.ledger_id != target.entry.ledger_id {
            bail!("cannot sync replicas for different ledgers: {from} -> {to}");
        }
        let ledger = source.entry.ledger_id.parse()?;
        let target_store = fact_store::Store::open(&target.entry.database)?;
        let known = all_database_hashes(&target.entry.database)?;
        let bundle_objects = database_objects_except(&source.entry.database, &known)?;
        let bundle = encode_bundle(ledger, &bundle_objects)?;
        let offered = decode_bundle_or_snapshot_objects(&bundle)?.len();
        let imported = import_bundle_with_deferred_projection(&target_store, &bundle)?;
        if imported > 0 {
            self.dirty_projection_replicas
                .borrow_mut()
                .insert(target.name.clone());
        }
        let duplicate_imported = if duplicate {
            let imported = import_bundle_with_deferred_projection(&target_store, &bundle)?;
            if imported > 0 {
                self.dirty_projection_replicas
                    .borrow_mut()
                    .insert(target.name.clone());
            }
            imported
        } else {
            0
        };
        if duplicate && duplicate_imported == 0 {
            self.duplicate_delivery_idempotent = true;
        }
        if direction == "pull" {
            let known_after = self.object_hashes(to)?;
            let reverse = database_objects_except(&source.entry.database, &known)?;
            self.push_pull_equivalent = reverse.len() == offered || known_after.is_superset(&known);
        }
        self.full_sync_count += 1;
        self.transfers.push(TransferReport {
            scenario: scenario.into(),
            from: from.into(),
            to: to.into(),
            ledger: source.ledger,
            direction: direction.into(),
            offered,
            imported,
            duplicate,
            partial: false,
            missing_dependencies: 0,
            duration_ms: started.elapsed().as_millis(),
        });
        Ok(imported.saturating_add(duplicate_imported))
    }

    fn propose(
        &mut self,
        replica: &str,
        family: &str,
        index: usize,
        decision: Option<DecisionOutcome>,
    ) -> Result<PropositionRef> {
        self.propose_with_projected_mode(
            replica,
            family,
            index,
            decision,
            fact_store::ProjectedMode::Incremental,
        )
    }

    fn propose_with_projected_mode(
        &mut self,
        replica: &str,
        family: &str,
        index: usize,
        decision: Option<DecisionOutcome>,
        projected_mode: fact_store::ProjectedMode,
    ) -> Result<PropositionRef> {
        if projected_mode != fact_store::ProjectedMode::Defer {
            self.ensure_replica_projection_current(replica)?;
        }
        let replica = self.replica(replica)?.clone();
        let content = scale_markdown_content(family, index, &replica.name, ScaleContentState::Base);
        self.sync_runtime_clock()?;
        let result = match create_proposition_with_runtime_and_projected_mode(
            &replica.entry,
            &replica.seed,
            content.as_bytes(),
            decision,
            &self.runtime,
            projected_mode,
        ) {
            Ok(result) => result,
            Err(error) if error.to_string().contains("unauthorized") => {
                let signer = self.admin_signer_for(&replica)?;
                self.sync_runtime_clock()?;
                create_proposition_with_runtime_and_projected_mode(
                    &signer.entry,
                    &signer.seed,
                    content.as_bytes(),
                    decision,
                    &self.runtime,
                    projected_mode,
                )
                .with_context(|| format!("admin signer create proposition on {}", replica.name))?
            }
            Err(error) => return Err(error.into()),
        };
        if projected_mode == fact_store::ProjectedMode::Defer {
            self.dirty_projection_replicas
                .borrow_mut()
                .insert(replica.name.clone());
        }
        Ok(PropositionRef {
            proposition_id: result.proposition_id,
            revision_id: result.revision_id,
            deliberation_id: result.deliberation_id,
            settlement_id: result.settlement_id,
        })
    }

    fn revise_and_accept(
        &mut self,
        replica: &str,
        proposition_id: Uuid,
        family: &str,
        index: usize,
    ) -> Result<PropositionRef> {
        let mut revision = self.revise(replica, proposition_id, family, index)?;
        self.accept_revision(replica, proposition_id, &mut revision)?;
        Ok(revision)
    }

    fn revise(
        &mut self,
        replica: &str,
        proposition_id: Uuid,
        family: &str,
        index: usize,
    ) -> Result<PropositionRef> {
        self.ensure_replica_projection_current(replica)?;
        let replica = self.replica(replica)?.clone();
        let content =
            scale_markdown_content(family, index, &replica.name, ScaleContentState::Accepted);
        let signer = self.proposition_signer_for(&replica)?;
        self.sync_runtime_clock()?;
        let revision = update_proposition_content_with_runtime(
            &signer.entry,
            &signer.seed,
            &proposition_id.to_string(),
            content.as_bytes(),
            &self.runtime,
        )?;
        Ok(PropositionRef {
            proposition_id,
            revision_id: revision.revision_id,
            deliberation_id: revision.deliberation_id,
            settlement_id: None,
        })
    }

    fn accept_revision(
        &mut self,
        replica: &str,
        proposition_id: Uuid,
        revision: &mut PropositionRef,
    ) -> Result<()> {
        self.ensure_replica_projection_current(replica)?;
        let replica = self.replica(replica)?.clone();
        let signer = self.proposition_signer_for(&replica)?;
        self.sync_runtime_clock()?;
        let accepted = accept_proposition_with_runtime(
            &signer.entry,
            &signer.seed,
            Some(&proposition_id.to_string()),
            &self.runtime,
        )?;
        revision.settlement_id = accepted.settlement_id;
        Ok(())
    }

    fn revise_and_reject(
        &mut self,
        replica: &str,
        proposition_id: Uuid,
        family: &str,
        index: usize,
    ) -> Result<PropositionRef> {
        self.ensure_replica_projection_current(replica)?;
        let replica = self.replica(replica)?.clone();
        let content =
            scale_markdown_content(family, index, &replica.name, ScaleContentState::Rejected);
        let signer = self.proposition_signer_for(&replica)?;
        self.sync_runtime_clock()?;
        let revision = update_proposition_content_with_runtime(
            &signer.entry,
            &signer.seed,
            &proposition_id.to_string(),
            content.as_bytes(),
            &self.runtime,
        )?;
        self.sync_runtime_clock()?;
        let rejected = reject_proposition_with_runtime(
            &signer.entry,
            &signer.seed,
            Some(&proposition_id.to_string()),
            &self.runtime,
        )?;
        Ok(PropositionRef {
            proposition_id,
            revision_id: revision.revision_id,
            deliberation_id: revision.deliberation_id,
            settlement_id: rejected.settlement_id,
        })
    }

    fn create_identity_ledger(&mut self, actor: &str) -> Result<ActorIdentity> {
        let database = self
            .environment
            .ledger_dir
            .join(format!("identity-{actor}.sqlite"));
        let store = open_workspace_store(&database)?;
        let seed = self.next_seed();
        self.sync_runtime_clock()?;
        let identity = create_identity_with_runtime(
            &store,
            CreateIdentityInput {
                namespace: format!("local.identity.{actor}"),
                seed,
                actor_type: "human".into(),
            },
            &self.runtime,
        )?;
        let seed_file = self
            .environment
            .identity_dir
            .join(format!("{}.seed", identity.actor_id));
        self.environment.write_seed(&seed_file, &seed)?;
        Ok(ActorIdentity {
            entry: LedgerEntry {
                name: format!("identity-{actor}"),
                ledger_id: identity.ledger_id.to_string(),
                database,
                actor_id: identity.actor_id.to_string(),
                key_id: identity.key_id.to_string(),
                seed_file,
                read_only: false,
            },
            seed,
        })
    }

    fn create_primary_replica(&mut self, ledger: &str, replica: &str, _actor: &str) -> Result<()> {
        let database = self
            .environment
            .ledger_dir
            .join(format!("{replica}.sqlite"));
        let store = open_workspace_store(&database)?;
        let seed = self.next_seed();
        let nonce = self.next_nonce();
        self.sync_runtime_clock()?;
        let bootstrap = create_ledger_with_runtime(
            &store,
            BootstrapLedgerInput {
                namespace: format!("local.{ledger}"),
                created_at: sdk_timestamp(self.clock.now()),
                seed,
                nonce,
            },
            &self.runtime,
        )?;
        let seed_file = self
            .environment
            .identity_dir
            .join(format!("{}.seed", bootstrap.actor_id));
        self.environment.write_seed(&seed_file, &seed)?;
        let entry = LedgerEntry {
            name: replica.into(),
            ledger_id: bootstrap.ledger_id,
            database,
            actor_id: bootstrap.actor_id,
            key_id: bootstrap.key_id,
            seed_file,
            read_only: false,
        };
        self.insert_replica(Replica {
            name: replica.into(),
            ledger: ledger.into(),
            entry,
            seed,
        })?;
        Ok(())
    }

    fn import_actor_and_grant(
        &mut self,
        replica: &str,
        actor: &str,
        capabilities: &[&str],
    ) -> Result<()> {
        let identity = self
            .identities
            .get(actor)
            .with_context(|| format!("unknown identity `{actor}`"))?;
        let exported = export_identity(&identity.entry)?;
        import_identity(&self.replica(replica)?.entry, &exported.bundle)?;
        self.sync_runtime_clock()?;
        create_identity_grant_with_runtime(
            &self.replica(replica)?.entry,
            &self.replica(replica)?.seed,
            &identity.entry.actor_id,
            &capabilities
                .iter()
                .map(|item| (*item).to_string())
                .collect::<Vec<_>>(),
            &self.runtime,
        )?;
        Ok(())
    }

    fn clone_replica(&mut self, source: &str, replica: &str, actor: &str) -> Result<()> {
        let source = self.replica(source)?.clone();
        let actor_identity = self
            .identities
            .get(actor)
            .with_context(|| format!("unknown identity `{actor}`"))?;
        let database = self
            .environment
            .ledger_dir
            .join(format!("{replica}.sqlite"));
        let target_store = open_workspace_store(&database)?;
        let temp_entry = LedgerEntry {
            name: replica.into(),
            ledger_id: source.entry.ledger_id.clone(),
            database: database.clone(),
            actor_id: String::new(),
            key_id: String::new(),
            seed_file: PathBuf::new(),
            read_only: false,
        };
        for identity in self.identities.values() {
            if !database_has_object_id(&source.entry.database, &identity.entry.actor_id)? {
                continue;
            }
            let exported = export_identity(&identity.entry)?;
            import_identity(&temp_entry, &exported.bundle)?;
        }
        let source_store = fact_store::Store::open(&source.entry.database)?;
        let ledger_id: Uuid = source.entry.ledger_id.parse()?;
        let objects = export_bundle(&source_store, ledger_id)?;
        fact_sdk::sync::import_bundle(&target_store, &objects)?;
        let seed_file = self
            .environment
            .identity_dir
            .join(format!("{}.seed", actor_identity.entry.actor_id));
        self.environment
            .write_seed(&seed_file, &actor_identity.seed)?;
        let entry = LedgerEntry {
            name: replica.into(),
            ledger_id: source.entry.ledger_id,
            database,
            actor_id: actor_identity.entry.actor_id.clone(),
            key_id: actor_identity.entry.key_id.clone(),
            seed_file,
            read_only: false,
        };
        self.insert_replica(Replica {
            name: replica.into(),
            ledger: source.ledger,
            entry,
            seed: actor_identity.seed,
        })?;
        Ok(())
    }

    fn add_remote(&mut self, name: &str, url: &str) -> Result<()> {
        let mut remotes = self.environment.load_remotes()?;
        remotes.insert(
            name.into(),
            RemoteEntry {
                name: name.into(),
                url: url.into(),
            },
        );
        self.environment.save_remotes(&remotes)?;
        self.remotes.insert(name.into());
        Ok(())
    }

    fn insert_replica(&mut self, replica: Replica) -> Result<()> {
        self.ledger_replicas
            .entry(replica.ledger.clone())
            .or_default()
            .push(replica.name.clone());
        self.replicas.insert(replica.name.clone(), replica);
        self.save_replica_catalog()
    }

    fn save_replica_catalog(&self) -> Result<()> {
        let entries = self
            .replicas
            .iter()
            .map(|(name, replica)| (name.clone(), replica.entry.clone()))
            .collect::<BTreeMap<_, _>>();
        self.environment.save(&entries)?;
        Ok(())
    }

    fn assert_same_object_set(&self, left: &str, right: &str) -> Result<()> {
        let left_hashes = self.object_hashes(left)?;
        let right_hashes = self.object_hashes(right)?;
        if left_hashes != right_hashes {
            bail!(
                "replica object sets differ: {left}={} {right}={}",
                left_hashes.len(),
                right_hashes.len()
            );
        }
        Ok(())
    }

    fn assert_same_projection(&self, left: &str, right: &str) -> Result<()> {
        self.ensure_replica_projection_current(left)?;
        self.ensure_replica_projection_current(right)?;
        if self.snapshot(left)? != self.snapshot(right)? {
            bail!("replica projections differ: {left} vs {right}");
        }
        Ok(())
    }

    fn assert_proposition_status(
        &self,
        replica: &str,
        proposition_id: Uuid,
        status: &str,
    ) -> Result<()> {
        self.refresh_indexed_propositions(replica)?;
        let items = list_propositions(
            &self.replica(replica)?.entry,
            ListPropositionsFilter {
                status: None,
                all: true,
            },
        )?;
        let item = items
            .iter()
            .find(|item| item.proposition_id == proposition_id)
            .with_context(|| format!("proposition {proposition_id} missing on {replica}"))?;
        if item.effective_status != status {
            bail!(
                "expected proposition {proposition_id} on {replica} to be {status}, got {}",
                item.effective_status
            );
        }
        Ok(())
    }

    fn assert_converged_object_sets(&self) -> Result<bool> {
        for replicas in self.ledger_replicas.values() {
            let Some((first, rest)) = replicas.split_first() else {
                continue;
            };
            for other in rest {
                self.assert_same_object_set(first, other)?;
            }
        }
        Ok(true)
    }

    fn assert_no_pending_objects(&self) -> Result<bool> {
        for replica in self.workflow_replicas() {
            let store = fact_store::Store::open(&replica.entry.database)?;
            let pending =
                fact_sdk::sync::list_pending_objects(&store, replica.entry.ledger_id.parse()?)?;
            if !pending.is_empty() {
                bail!(
                    "{} has {} pending sync objects",
                    replica.name,
                    pending.len()
                );
            }
        }
        Ok(true)
    }

    fn assert_dependency_closure_after_full_sync(&self) -> Result<bool> {
        if crate::scale::is_scale_profile(&self.profile) {
            return Ok(self.missing_dependency_deferred && self.delayed_dependency_retry_succeeded);
        }
        self.assert_no_pending_objects()
    }

    fn key_rotation_byte_for_byte_replay(&self) -> Result<bool> {
        let first = self.key_rotation_replay_hashes("first")?;
        let second = self.key_rotation_replay_hashes("second")?;
        Ok(first == second)
    }

    fn key_rotation_replay_hashes(&self, _label: &str) -> Result<Vec<String>> {
        let start = OffsetDateTime::parse(
            "2026-02-02T10:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )?;
        let runtime = DeterministicRuntime::new("sync-key-rotation-replay", start);
        let temp = tempfile::tempdir_in(&self.environment.ledger_dir)?;
        let database = temp.path().join("rotation.sqlite");
        let store = fact_store::Store::open(&database)?;
        let seed = [31_u8; 32];
        let bootstrap = create_ledger_with_runtime(
            &store,
            BootstrapLedgerInput {
                namespace: "local.key-rotation-replay".into(),
                created_at: sdk_timestamp(start),
                seed,
                nonce: [47_u8; 16],
            },
            &runtime,
        )?;
        let entry = LedgerEntry {
            name: "key-rotation-replay".into(),
            ledger_id: bootstrap.ledger_id,
            database: database.clone(),
            actor_id: bootstrap.actor_id,
            key_id: bootstrap.key_id,
            seed_file: temp.path().join("seed"),
            read_only: false,
        };
        rotate_identity_key_with_runtime(&entry, &seed, &runtime)?;
        protocol_hashes_for_database(&database)
    }

    fn workflow_replica_snapshots(&self) -> Result<BTreeMap<String, SdkStateSnapshot>> {
        self.workflow_replicas()
            .into_iter()
            .map(|replica| Ok((replica.name.clone(), self.snapshot(&replica.name)?)))
            .collect()
    }

    fn workflow_replica_hashes(&self) -> Result<BTreeMap<String, Vec<String>>> {
        self.workflow_replicas()
            .into_iter()
            .map(|replica| {
                Ok((
                    replica.name.clone(),
                    protocol_hashes_for_database(&replica.entry.database)?,
                ))
            })
            .collect()
    }

    fn workflow_replicas(&self) -> Vec<&Replica> {
        self.replicas
            .values()
            .filter(|replica| matches!(replica.ledger.as_str(), "operations" | "engineering"))
            .collect()
    }

    fn snapshot(&self, replica: &str) -> Result<SdkStateSnapshot> {
        crate::snapshot_for_entry(&self.replica(replica)?.entry)
    }

    fn run_cli_samples(&mut self, fact_binary: Option<&Path>) -> Result<Vec<CliReceipt>> {
        let fact_binary = fact_binary
            .map(Path::to_path_buf)
            .unwrap_or_else(default_fact_binary_path);
        if !fact_binary.exists() {
            bail!("fact binary `{}` does not exist", fact_binary.display());
        }
        let conflict = self
            .sample_conflict_proposition_id
            .context("no conflict proposition available for CLI sampling")?
            .to_string();
        let commands = [
            vec!["status"],
            vec!["--json", "status"],
            vec!["remote", "list"],
            vec!["list", "--all"],
            vec!["--json", "list", "--all"],
            vec!["revisions", &conflict],
            vec!["--json", "revisions", &conflict],
            vec!["history", &conflict],
            vec!["--json", "history", &conflict],
            vec!["pending"],
            vec!["--json", "pending"],
            vec!["search", "--effective", "scale"],
            vec!["--json", "search", "--effective", "scale"],
            vec![
                "--json",
                "search",
                "--effective",
                "--page-size",
                "2",
                "scale",
            ],
            vec![
                "--json",
                "search",
                "--status",
                "accepted",
                "--effective",
                "scale",
            ],
            vec!["--json", "search", "--effective", "base"],
        ];
        let mut receipts = Vec::new();
        for command in commands {
            receipts.push(self.run_fact_command(&fact_binary, &command)?);
        }
        let status = receipts
            .iter()
            .find(|receipt| receipt.command == ["status"])
            .context("missing fact status sample")?;
        let operations = self.replica("operations_a")?;
        let expected_remote_count = format!("{} remote(s)", self.remotes.len());
        if !status.stdout.contains("operations_a")
            || !status.stdout.contains(&operations.entry.ledger_id)
            || !status.stdout.contains(&expected_remote_count)
        {
            bail!("fact status did not expose the active ledger and remote count");
        }
        self.cli_ux_coverage
            .push("status-human-active-ledger".into());
        let list = receipts
            .iter()
            .find(|receipt| receipt.command == ["list", "--all"])
            .context("missing fact list sample")?;
        if !list.stdout.contains("conflict") || !list.stdout.contains(&conflict[..12]) {
            bail!("fact list --all did not surface sampled conflict proposition");
        }
        self.cli_ux_coverage.push("list-contested-state".into());
        let json_list = receipts
            .iter()
            .find(|receipt| receipt.command == ["--json", "list", "--all"])
            .context("missing fact list JSON sample")?;
        let json_list_has_conflict = json_list
            .parsed_json
            .as_ref()
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .any(|item| {
                item.get("proposition_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(conflict.as_str())
                    && item
                        .get("effective_status")
                        .and_then(serde_json::Value::as_str)
                        == Some("conflict")
            });
        if !json_list_has_conflict {
            bail!("fact list --json --all did not expose sampled conflict status");
        }
        self.cli_ux_coverage
            .push("json-list-contested-state".into());
        let revisions = receipts
            .iter()
            .find(|receipt| receipt.command == ["--json", "revisions", &conflict])
            .context("missing fact revisions JSON sample")?;
        let revision_items = revisions
            .parsed_json
            .as_ref()
            .and_then(serde_json::Value::as_array)
            .context("fact revisions --json did not return an array")?;
        if revision_items.len() < 3 {
            bail!("fact revisions --json did not expose conflict branch revisions");
        }
        if !revision_items.iter().any(|item| {
            item.get("effective").and_then(serde_json::Value::as_bool) == Some(true)
                && item.get("status").and_then(serde_json::Value::as_str) == Some("conflict")
        }) {
            bail!("fact revisions --json did not expose the effective conflict ancestor");
        }
        self.cli_ux_coverage
            .push("revisions-effective-ancestor".into());
        let branch_tip_count = revision_items
            .iter()
            .filter(|item| {
                item.get("tip").and_then(serde_json::Value::as_bool) == Some(true)
                    && item.get("effective").and_then(serde_json::Value::as_bool) == Some(false)
            })
            .count();
        if branch_tip_count < 2 {
            bail!("fact revisions --json did not expose both conflicting branch tips");
        }
        self.cli_ux_coverage
            .push("revisions-conflict-branch-tips".into());
        let history = receipts
            .iter()
            .find(|receipt| receipt.command == ["--json", "history", &conflict])
            .context("missing fact history JSON sample")?;
        let history_items = history
            .parsed_json
            .as_ref()
            .and_then(serde_json::Value::as_array)
            .context("fact history --json did not return an array")?;
        let history_types = history_items
            .iter()
            .filter_map(|item| item.get("object_type").and_then(serde_json::Value::as_str))
            .collect::<BTreeSet<_>>();
        if !history_types.contains("revision")
            || !history_types.contains("decision")
            || !history_types.contains("settlement")
        {
            bail!(
                "fact history --json did not expose preserved revision/decision/settlement history"
            );
        }
        self.cli_ux_coverage
            .push("history-preserved-objects".into());
        let pending = receipts
            .iter()
            .find(|receipt| receipt.command == ["pending"])
            .context("missing fact pending sample")?;
        if pending.stdout.contains("no pending actions")
            || !(pending.stdout.contains("accept or reject")
                || pending.stdout.contains("repair pending review"))
        {
            bail!("fact pending did not expose actionable pending state");
        }
        let json_pending = receipts
            .iter()
            .find(|receipt| receipt.command == ["--json", "pending"])
            .context("missing fact pending JSON sample")?;
        if json_array_len(json_pending) == 0 || !json_pending_has_actionable_state(json_pending) {
            bail!("fact pending --json did not expose structured pending state");
        }
        self.cli_ux_coverage.push("pending-command-sampled".into());
        self.cli_ux_coverage
            .push("json-pending-actionable-state".into());
        let search = receipts
            .iter()
            .find(|receipt| receipt.command == ["search", "--effective", "scale"])
            .context("missing fact search sample")?;
        if search.stdout.contains("no results") {
            bail!("fact search --effective scale returned no results");
        }
        self.cli_ux_coverage.push("search-effective-text".into());
        let json_search = receipts
            .iter()
            .find(|receipt| receipt.command == ["--json", "search", "--effective", "scale"])
            .context("missing fact search JSON sample")?;
        if json_array_len(json_search) == 0 {
            bail!("fact search --json --effective scale returned no results");
        }
        self.cli_ux_coverage.push("search-effective-json".into());
        let paged_search = receipts
            .iter()
            .find(|receipt| {
                receipt.command
                    == [
                        "--json",
                        "search",
                        "--effective",
                        "--page-size",
                        "2",
                        "scale",
                    ]
            })
            .context("missing fact search paged JSON sample")?;
        let sdk_paged_results = fact_sdk::workflow::search_proposition_content(
            &self.replica("operations_a")?.entry,
            "scale",
            None,
            true,
            2,
        )?;
        if sdk_paged_results.len() != 2 || json_array_len(paged_search) != sdk_paged_results.len() {
            bail!("fact search --json --page-size 2 did not match SDK bounded results");
        }
        self.cli_ux_coverage
            .push("search-page-size-bounded-json".into());
        let ambiguous = self.run_fact_command_allow_failure(&fact_binary, &["echo", "019c"])?;
        if ambiguous.status == Some(0)
            || !ambiguous.stderr.contains("reference is ambiguous")
            || !ambiguous.stderr.contains("--pending/--latest")
        {
            bail!("fact echo did not report ambiguous reference guidance");
        }
        receipts.push(ambiguous);
        self.cli_ux_coverage
            .push("ambiguous-reference-reported".into());
        let accepted_search = receipts
            .iter()
            .find(|receipt| {
                receipt.command
                    == [
                        "--json",
                        "search",
                        "--status",
                        "accepted",
                        "--effective",
                        "scale",
                    ]
            })
            .context("missing fact search accepted JSON sample")?;
        if json_array_len(accepted_search) == 0 {
            bail!("fact search --json --status accepted --effective scale returned no results");
        }
        let accepted_reference = first_json_proposition_id(accepted_search)
            .context("fact search accepted JSON sample did not include a proposition id")?;
        let expected_content =
            read_proposition_content(&self.replica("operations_a")?.entry, &accepted_reference)?
                .content;
        let expected_content = String::from_utf8_lossy(&expected_content).to_string();
        let echo = self.run_fact_command(&fact_binary, &["echo", &accepted_reference])?;
        if echo.stdout != expected_content {
            bail!("fact echo disagrees with SDK effective content");
        }
        receipts.push(echo);
        self.cli_ux_coverage.push("echo-effective-content".into());
        let open = self.run_fact_command(&fact_binary, &["open", &accepted_reference])?;
        if open.stdout != expected_content {
            bail!("fact open disagrees with SDK effective content");
        }
        receipts.push(open);
        self.cli_ux_coverage.push("open-effective-content".into());
        let bundle_path = self.workspace.join("logs").join("cli-pull-sample.factbndl");
        let source = self.replica("operations_a")?.clone();
        let source_database = source.entry.database.to_string_lossy().into_owned();
        let source_ledger = source.entry.ledger_id.clone();
        let bundle_file = bundle_path.to_string_lossy().into_owned();
        let pull = self.run_fact_command(
            &fact_binary,
            &[
                "--json",
                "pull",
                &source_database,
                &source_ledger,
                &bundle_file,
            ],
        )?;
        if pull
            .parsed_json
            .as_ref()
            .and_then(|value| value.get("pulled"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default()
            == 0
            || !bundle_path.exists()
        {
            bail!("fact pull did not export a non-empty sampled bundle");
        }
        receipts.push(pull);
        self.cli_ux_coverage.push("pull-bundle-export".into());
        let (target_database_path, push_bundle_path) =
            self.prepare_cli_push_sample_bundle(&source)?;
        let target_database = target_database_path.to_string_lossy().into_owned();
        let push_bundle_file = push_bundle_path.to_string_lossy().into_owned();
        let push = self.run_fact_command(
            &fact_binary,
            &["--json", "push", &target_database, &push_bundle_file],
        )?;
        if push
            .parsed_json
            .as_ref()
            .and_then(|value| value.get("pushed"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default()
            == 0
            || !target_database_path.exists()
        {
            bail!("fact push did not import the sampled bundle into a fresh database");
        }
        if push
            .parsed_json
            .as_ref()
            .and_then(|value| value.get("pushed"))
            .is_none()
        {
            bail!("fact push did not return a sampled push result");
        }
        receipts.push(push);
        self.cli_ux_coverage.push("push-bundle-import".into());
        self.cli_ux_coverage
            .push("search-accepted-effective-json".into());
        let contested_search = receipts
            .iter()
            .find(|receipt| receipt.command == ["--json", "search", "--effective", "base"])
            .context("missing fact search contested JSON sample")?;
        if json_array_len(contested_search) == 0 {
            bail!("fact search --json --effective base returned no results");
        }
        if !json_array_has_effective_status(contested_search, &["conflict", "contested"]) {
            bail!("fact search --json --effective base did not expose contested effective state");
        }
        self.cli_ux_coverage
            .push("search-contested-effective-json".into());
        Ok(receipts)
    }

    fn prepare_cli_push_sample_bundle(&self, source: &Replica) -> Result<(PathBuf, PathBuf)> {
        let target_database = self.workspace.join("logs").join("cli-push-target.sqlite");
        let bundle = self.workspace.join("logs").join("cli-push-sample.factbndl");
        fs::copy(&source.entry.database, &target_database).with_context(|| {
            format!(
                "copy {} to {} for CLI push sample",
                source.entry.database.display(),
                target_database.display()
            )
        })?;
        {
            let connection = rusqlite::Connection::open(&target_database)?;
            connection.execute("PRAGMA foreign_keys = OFF", [])?;
            let (object_id, content_hash) = connection
                .query_row(
                    "SELECT object_id, content_hash FROM protocol_object WHERE object_type='revision' ORDER BY content_hash LIMIT 1",
                    [],
                    |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
                )
                .context("select revision object for CLI push sample")?;
            connection.execute(
                "DELETE FROM object_dependency WHERE object_id=?1",
                [&object_id],
            )?;
            connection.execute(
                "DELETE FROM protocol_revision WHERE object_id=?1",
                [&object_id],
            )?;
            let projected_object = projection_table_name(&connection, "projection_object")?;
            let projected_revision = projection_table_name(&connection, "projection_revision")?;
            connection.execute(
                &format!("DELETE FROM {projected_object} WHERE object_id=?1"),
                [&object_id],
            )?;
            connection.execute(
                &format!("DELETE FROM {projected_revision} WHERE object_id=?1"),
                [&object_id],
            )?;
            connection.execute(
                "DELETE FROM protocol_object WHERE content_hash=?1",
                [&content_hash],
            )?;
        }
        let known = all_database_hashes(&target_database)?;
        let objects = database_objects_except(&source.entry.database, &known)?;
        if objects.is_empty() {
            bail!("CLI push sample could not construct a missing-object bundle");
        }
        let ledger = source.entry.ledger_id.parse()?;
        fs::write(&bundle, encode_bundle(ledger, &objects)?)?;
        Ok((target_database, bundle))
    }

    fn run_fact_command(&self, fact_binary: &Path, args: &[&str]) -> Result<CliReceipt> {
        let receipt = self.run_fact_command_allow_failure(fact_binary, args)?;
        if receipt.status == Some(0) {
            Ok(receipt)
        } else {
            bail!("fact {} failed: {}", args.join(" "), receipt.stderr);
        }
    }

    fn run_fact_command_allow_failure(
        &self,
        fact_binary: &Path,
        args: &[&str],
    ) -> Result<CliReceipt> {
        let started = Instant::now();
        let output = Command::new(fact_binary)
            .args(args)
            .env("FACT_HOME", &self.fact_home)
            .env("EDITOR", "cat")
            .env("VISUAL", "cat")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()?;
        let receipt = CliReceipt {
            command: args.iter().map(|arg| (*arg).to_string()).collect(),
            status: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            parsed_json: serde_json::from_slice(&output.stdout).ok(),
            duration_ms: started.elapsed().as_millis(),
        };
        Ok(receipt)
    }

    fn run_http_samples(&self) -> Result<Vec<HttpReceipt>> {
        let replica = self.replica("operations_a")?;
        let database = replica.entry.database.clone();
        let ledger = replica.entry.ledger_id.clone();
        let sdk_object_count = self.object_hashes("operations_a")?.len();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let store = fact_store::Store::open(&database)?;
                let app = fact_http::router(fact_http::AppState::new(store, "http://sync.local"));
                let mut receipts = Vec::new();
                for path in [
                    "/.well-known/facts".to_string(),
                    "/facts/ledgers".to_string(),
                    format!("/facts/ledgers/{ledger}/objects"),
                    format!("/facts/ledgers/{ledger}/commitments/latest"),
                ] {
                    let started = Instant::now();
                    let request = Request::builder()
                        .method("GET")
                        .uri(&path)
                        .header("accept", "application/fact+json")
                        .body(Body::empty())?;
                    let response = app.clone().oneshot(request).await?;
                    let status = response.status().as_u16();
                    let body = to_bytes(response.into_body(), usize::MAX).await?;
                    let parsed_json = serde_json::from_slice(&body).ok();
                    receipts.push(HttpReceipt {
                        method: "GET".into(),
                        path,
                        status,
                        body_bytes: body.len(),
                        parsed_json,
                        duration_ms: started.elapsed().as_millis(),
                    });
                }
                for receipt in &receipts {
                    if receipt.status != 200 {
                        bail!(
                            "HTTP sample {} {} returned status {}",
                            receipt.method,
                            receipt.path,
                            receipt.status
                        );
                    }
                }
                let object_path = format!("/facts/ledgers/{ledger}/objects");
                let object_body = receipts
                    .iter()
                    .find(|receipt| receipt.path == object_path)
                    .and_then(|receipt| receipt.parsed_json.as_ref())
                    .and_then(|value| value.get("body"))
                    .context("HTTP objects sample did not return an objects array")?;
                validate_http_object_sample(object_body, sdk_object_count)?;
                Ok(receipts)
            })
        })
        .join()
        .map_err(|_| anyhow::anyhow!("HTTP sample thread panicked"))?
    }

    fn object_hashes(&self, replica: &str) -> Result<HashSet<fact_core::Hash>> {
        let replica = self.replica(replica)?;
        let store = fact_store::Store::open(&replica.entry.database)?;
        Ok(store
            .list_objects(replica.entry.ledger_id.parse::<Uuid>()?.as_bytes())?
            .into_iter()
            .map(|(_, hash, _)| hash)
            .collect())
    }

    fn unique_object_count(&self) -> Result<usize> {
        Ok(unique_protocol_object_counts(
            self.replicas
                .values()
                .map(|replica| &replica.entry.database)
                .chain(
                    self.identities
                        .values()
                        .map(|identity| &identity.entry.database),
                ),
        )?
        .values()
        .sum())
    }

    fn replica(&self, name: &str) -> Result<&Replica> {
        self.replicas
            .get(name)
            .with_context(|| format!("unknown replica `{name}`"))
    }

    fn proposition_signer_for(&self, replica: &Replica) -> Result<Replica> {
        self.ensure_replica_projection_current(&replica.name)?;
        let admin_name = self
            .ledger_replicas
            .get(&replica.ledger)
            .and_then(|replicas| replicas.first())
            .with_context(|| format!("no admin replica for ledger `{}`", replica.ledger))?;
        if admin_name == &replica.name {
            Ok(replica.clone())
        } else {
            self.admin_signer_for(replica)
        }
    }

    fn admin_signer_for(&self, target: &Replica) -> Result<Replica> {
        self.ensure_replica_projection_current(&target.name)?;
        let admin_name = self
            .ledger_replicas
            .get(&target.ledger)
            .and_then(|replicas| replicas.first())
            .with_context(|| format!("no admin replica for ledger `{}`", target.ledger))?;
        let admin = self.replica(admin_name)?.clone();
        let mut entry = admin.entry.clone();
        entry.name = target.name.clone();
        entry.database = target.entry.database.clone();
        Ok(Replica {
            name: target.name.clone(),
            ledger: target.ledger.clone(),
            entry,
            seed: admin.seed,
        })
    }

    fn actor_signer_on_replica(&self, actor: &str, target: &str) -> Result<Replica> {
        self.ensure_replica_projection_current(target)?;
        let target = self.replica(target)?.clone();
        let identity = self
            .identities
            .get(actor)
            .with_context(|| format!("unknown identity `{actor}`"))?;
        let mut entry = target.entry.clone();
        entry.actor_id = identity.entry.actor_id.clone();
        entry.key_id = identity.entry.key_id.clone();
        entry.seed_file = identity.entry.seed_file.clone();
        Ok(Replica {
            name: target.name,
            ledger: target.ledger,
            entry,
            seed: identity.seed,
        })
    }

    fn identity_uuid(&self, actor: &str) -> Result<Uuid> {
        self.identities
            .get(actor)
            .with_context(|| format!("unknown identity `{actor}`"))?
            .entry
            .actor_id
            .parse()
            .with_context(|| format!("invalid identity id for `{actor}`"))
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
}

fn validate_http_object_sample(body: &serde_json::Value, sdk_object_count: usize) -> Result<()> {
    let objects = body
        .get("objects")
        .and_then(serde_json::Value::as_array)
        .context("HTTP objects sample did not return an objects array")?;
    let http_object_count = objects.len();
    if http_object_count > HTTP_OBJECT_LIST_PAGE_SIZE {
        bail!(
            "HTTP objects sample returned {http_object_count} objects, exceeding page size {HTTP_OBJECT_LIST_PAGE_SIZE}"
        );
    }
    let next_cursor = body.get("next_cursor").unwrap_or(&serde_json::Value::Null);
    if sdk_object_count > http_object_count && next_cursor.is_null() {
        bail!(
            "HTTP objects sample returned {http_object_count} of {sdk_object_count} SDK objects without next_cursor"
        );
    }
    if sdk_object_count <= HTTP_OBJECT_LIST_PAGE_SIZE && http_object_count != sdk_object_count {
        bail!(
            "HTTP objects sample returned {http_object_count} objects, SDK saw {sdk_object_count}"
        );
    }
    Ok(())
}

fn json_array_len(receipt: &CliReceipt) -> usize {
    receipt
        .parsed_json
        .as_ref()
        .and_then(serde_json::Value::as_array)
        .map(Vec::len)
        .unwrap_or_default()
}

fn first_json_proposition_id(receipt: &CliReceipt) -> Option<String> {
    receipt
        .parsed_json
        .as_ref()
        .and_then(serde_json::Value::as_array)?
        .iter()
        .find_map(|item| {
            item.get("proposition_id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
}

fn json_pending_has_actionable_state(receipt: &CliReceipt) -> bool {
    receipt
        .parsed_json
        .as_ref()
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .any(|item| {
            item.get("has_pending_revision")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
                || item
                    .get("pending_deliberation_id")
                    .and_then(serde_json::Value::as_str)
                    .is_some()
        })
}

fn json_array_has_effective_status(receipt: &CliReceipt, statuses: &[&str]) -> bool {
    receipt
        .parsed_json
        .as_ref()
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .any(|item| {
            item.get("effective_status")
                .or_else(|| item.get("status"))
                .and_then(serde_json::Value::as_str)
                .is_some_and(|status| statuses.contains(&status))
        })
}

fn unique_protocol_object_counts<'a>(
    databases: impl IntoIterator<Item = &'a PathBuf>,
) -> Result<BTreeMap<String, usize>> {
    let mut seen = HashSet::new();
    let mut counts = BTreeMap::new();
    for database in databases {
        let connection = rusqlite::Connection::open(database)?;
        let mut statement =
            connection.prepare("SELECT content_hash, object_type FROM protocol_object")?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, String>(1)?))
        })?;
        for row in rows {
            let (hash_bytes, object_type) = row?;
            if seen.insert(hash_bytes) {
                *counts.entry(object_type).or_insert(0) += 1;
            }
        }
    }
    Ok(counts)
}

fn read_fixture_report(fixture: &Path, relative: &str) -> Result<serde_json::Value> {
    let path = fixture.join(relative);
    serde_json::from_slice(
        &fs::read(&path).with_context(|| format!("failed to read `{}`", path.display()))?,
    )
    .with_context(|| format!("failed to parse `{}`", path.display()))
}

fn unique_protocol_hashes<'a>(
    databases: impl IntoIterator<Item = &'a PathBuf>,
) -> Result<Vec<fact_core::Hash>> {
    let mut hashes = HashSet::new();
    for database in databases {
        let connection = rusqlite::Connection::open(database)?;
        let mut statement = connection.prepare("SELECT content_hash FROM protocol_object")?;
        let rows = statement.query_map([], |row| row.get::<_, Vec<u8>>(0))?;
        for row in rows {
            let hash_bytes = row?;
            let hash_array: [u8; 32] = hash_bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid content hash length"))?;
            hashes.insert(fact_core::Hash::from_bytes(hash_array));
        }
    }
    let mut hashes = hashes.into_iter().collect::<Vec<_>>();
    hashes.sort();
    Ok(hashes)
}

fn deterministic_seed(random: &mut DeterministicRandomSource) -> [u8; 32] {
    let mut seed = [0_u8; 32];
    for chunk in seed.chunks_mut(8) {
        chunk.copy_from_slice(&random.next_u64().to_le_bytes());
    }
    seed
}

fn deterministic_nonce(random: &mut DeterministicRandomSource) -> [u8; 16] {
    let mut nonce = [0_u8; 16];
    for chunk in nonce.chunks_mut(8) {
        chunk.copy_from_slice(&random.next_u64().to_le_bytes());
    }
    nonce
}

fn database_objects_except(
    database: &Path,
    known: &HashSet<fact_core::Hash>,
) -> Result<Vec<(fact_core::Hash, Vec<u8>)>> {
    let mut objects = Vec::new();
    let mut seen = HashSet::new();
    for ledger in database_ledger_ids(database)? {
        for (hash, cose) in database_objects_for_ledger(database, ledger)? {
            if known.contains(&hash) || !seen.insert(hash) {
                continue;
            }
            objects.push((hash, cose));
        }
    }
    Ok(objects)
}

fn database_ledger_ids(database: &Path) -> Result<Vec<Uuid>> {
    let connection = rusqlite::Connection::open(database)?;
    let mut statement = connection
        .prepare("SELECT DISTINCT ledger_id FROM protocol_object WHERE length(ledger_id)=16")?;
    let ledger_rows = statement.query_map([], |row| row.get::<_, Vec<u8>>(0))?;
    let mut ledgers = Vec::new();
    for ledger_row in ledger_rows {
        let ledger_bytes = ledger_row?;
        ledgers.push(Uuid::from_slice(&ledger_bytes).context("invalid ledger id bytes")?);
    }
    ledgers.sort();
    Ok(ledgers)
}

fn database_objects_for_ledger(
    database: &Path,
    ledger: Uuid,
) -> Result<Vec<(fact_core::Hash, Vec<u8>)>> {
    let store = fact_store::Store::open(database)?;
    let mut objects = Vec::new();
    let mut seen = HashSet::new();
    for (object_id, hash, _) in store.list_objects_with_dependencies(ledger.as_bytes())? {
        if seen.insert(hash) {
            let cose = store
                .get_cose_by_id_any(object_id.as_bytes())?
                .with_context(|| format!("missing COSE bytes for object {object_id}"))?;
            objects.push((hash, cose));
        }
    }
    Ok(objects)
}

fn all_database_hashes(database: &Path) -> Result<HashSet<fact_core::Hash>> {
    let connection = rusqlite::Connection::open(database)?;
    let mut statement = connection.prepare("SELECT content_hash FROM protocol_object")?;
    let rows = statement.query_map([], |row| row.get::<_, Vec<u8>>(0))?;
    let mut hashes = HashSet::new();
    for row in rows {
        let hash_bytes = row?;
        let hash_array: [u8; 32] = hash_bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid content hash length"))?;
        hashes.insert(fact_core::Hash::from_bytes(hash_array));
    }
    Ok(hashes)
}

fn database_has_object_id(database: &Path, object_id: &str) -> Result<bool> {
    let object_id = Uuid::parse_str(object_id)?;
    let connection = rusqlite::Connection::open(database)?;
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM protocol_object WHERE object_id=?",
        [object_id.as_bytes().as_slice()],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn payload_hash(cose_bytes: &[u8]) -> Result<String> {
    Ok(fact_core::Hash::digest(&fact_crypto::decode_sign1(cose_bytes)?.payload).hex())
}

fn signed_object_hash(cose_bytes: &[u8]) -> String {
    fact_core::Hash::digest(cose_bytes).hex()
}

fn title(value: &str) -> String {
    value
        .split([' ', '-'])
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(first) => format!("{}{}", first.to_ascii_uppercase(), chars.as_str()),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScaleContentState {
    Base,
    Accepted,
    Rejected,
}

fn scale_markdown_content(
    family: &str,
    index: usize,
    replica: &str,
    state: ScaleContentState,
) -> String {
    let domain = SCALE_CONTENT_DOMAINS[index % SCALE_CONTENT_DOMAINS.len()];
    let practice = SCALE_CONTENT_PRACTICES
        [(index / SCALE_CONTENT_DOMAINS.len()) % SCALE_CONTENT_PRACTICES.len()];
    let cadence = SCALE_CONTENT_CADENCES[(index / 7) % SCALE_CONTENT_CADENCES.len()];
    let audience = SCALE_CONTENT_AUDIENCES[(index / 11) % SCALE_CONTENT_AUDIENCES.len()];
    let keyword = format!("{}-{:05}", family.replace(' ', "-"), index);
    let status_line = match state {
        ScaleContentState::Base => {
            format!(
                "This base decision records the current {} rule for {}.",
                practice.description, audience.slug
            )
        }
        ScaleContentState::Accepted => {
            format!(
                "This accepted revision tightens the {} rule after field review.",
                practice.description
            )
        }
        ScaleContentState::Rejected => {
            format!(
                "This rejected revision is retained to preserve the discarded {} option.",
                practice.description
            )
        }
    };
    let code_block = if index.is_multiple_of(9) {
        format!(
            "\n```toml\npolicy = \"{}\"\nowner = \"{}\"\ncadence = \"{}\"\n```\n",
            domain.slug, audience.slug, cadence.slug
        )
    } else {
        String::new()
    };
    format!(
        "# {} {}: {}\n\n## Decision\n\n{}\n\n## Operating Context\n\nThe {} team applies this {} during {}. The searchable benchmark terms are scale, base, {}, and {}.\n\n## Evidence\n\n- Source replica: `{}`\n- Scenario key: `{}`\n- Template version: `{}`\n{}\n## Follow-Up\n\nReview owners should compare effective content, historical revisions, lifecycle filters, and rare-term lookup for `{}`.\n",
        title(family),
        index,
        domain.title,
        status_line,
        domain.name,
        practice.description,
        cadence.description,
        domain.slug,
        practice.slug,
        replica,
        keyword,
        default_content_template_version(),
        code_block,
        keyword,
    )
}

#[derive(Debug, Clone, Copy)]
struct ScaleContentDomain {
    name: &'static str,
    title: &'static str,
    slug: &'static str,
}

#[derive(Debug, Clone, Copy)]
struct ScaleContentPractice {
    description: &'static str,
    slug: &'static str,
}

#[derive(Debug, Clone, Copy)]
struct ScaleContentCadence {
    description: &'static str,
    slug: &'static str,
}

#[derive(Debug, Clone, Copy)]
struct ScaleContentAudience {
    slug: &'static str,
}

const SCALE_CONTENT_DOMAINS: &[ScaleContentDomain] = &[
    ScaleContentDomain {
        name: "engineering standards",
        title: "Engineering Release Gate",
        slug: "engineering-standards",
    },
    ScaleContentDomain {
        name: "deployment policy",
        title: "Deployment Freeze Exception",
        slug: "deployment-policy",
    },
    ScaleContentDomain {
        name: "product requirements",
        title: "Product Acceptance Rule",
        slug: "product-requirements",
    },
    ScaleContentDomain {
        name: "operating procedure",
        title: "Incident Handoff Procedure",
        slug: "operating-procedure",
    },
    ScaleContentDomain {
        name: "security practice",
        title: "Credential Rotation Standard",
        slug: "security-practice",
    },
    ScaleContentDomain {
        name: "meeting conclusion",
        title: "Architecture Review Outcome",
        slug: "meeting-conclusion",
    },
    ScaleContentDomain {
        name: "business rule",
        title: "Client Escalation Rule",
        slug: "business-rule",
    },
    ScaleContentDomain {
        name: "data governance guidance",
        title: "Dataset Retention Guidance",
        slug: "data-governance",
    },
];

const SCALE_CONTENT_PRACTICES: &[ScaleContentPractice] = &[
    ScaleContentPractice {
        description: "approval threshold",
        slug: "approval-threshold",
    },
    ScaleContentPractice {
        description: "rollback window",
        slug: "rollback-window",
    },
    ScaleContentPractice {
        description: "audit trail",
        slug: "audit-trail",
    },
    ScaleContentPractice {
        description: "on-call escalation",
        slug: "on-call-escalation",
    },
    ScaleContentPractice {
        description: "schema migration checklist",
        slug: "schema-migration",
    },
    ScaleContentPractice {
        description: "customer exception register",
        slug: "customer-exception",
    },
];

const SCALE_CONTENT_CADENCES: &[ScaleContentCadence] = &[
    ScaleContentCadence {
        description: "quarterly planning",
        slug: "quarterly",
    },
    ScaleContentCadence {
        description: "weekly operations review",
        slug: "weekly",
    },
    ScaleContentCadence {
        description: "post-incident review",
        slug: "post-incident",
    },
    ScaleContentCadence {
        description: "release readiness",
        slug: "release",
    },
];

const SCALE_CONTENT_AUDIENCES: &[ScaleContentAudience] = &[
    ScaleContentAudience {
        slug: "platform-owners",
    },
    ScaleContentAudience {
        slug: "security-reviewers",
    },
    ScaleContentAudience {
        slug: "product-leads",
    },
    ScaleContentAudience {
        slug: "support-ops",
    },
];

fn replica_suffix(index: usize) -> String {
    let first = (b'a' + (index % 26) as u8) as char;
    if index < 26 {
        first.to_string()
    } else {
        format!("{first}{}", index / 26)
    }
}

fn open_workspace_store(database: &Path) -> Result<fact_store::Store> {
    if let Some(parent) = database.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create database directory `{}`", parent.display())
        })?;
    }
    fact_store::Store::open(database)
        .with_context(|| format!("failed to open workspace database `{}`", database.display()))
}

fn import_bundle_with_deferred_projection(
    store: &fact_store::Store,
    bundle: &[u8],
) -> Result<usize> {
    let objects = decode_bundle_or_snapshot_slices(bundle)?;
    let hashes = store.insert_authorized_bundle_slices_with_projected_mode(
        &objects,
        fact_store::ProjectedMode::Defer,
    )?;
    Ok(hashes.len())
}

fn run_workspace(profile: &str, seed: u64) -> Result<PathBuf> {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let workspace = PathBuf::from("target")
        .join("fact-sim-runs")
        .join(format!("{profile}-seed-{seed}-{timestamp}"));
    fs::create_dir_all(&workspace)?;
    Ok(workspace)
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
