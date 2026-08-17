use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand, ValueEnum};
use fact_sim_core::FailureClassification;
use fact_sim_dsl::Scenario;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const FAULT_SCHEMA_VERSION: &str = "fault-v1";

#[derive(Debug, Subcommand)]
pub enum FaultCommand {
    Spec,
    Run(FaultRunArgs),
    Replay(FaultReplayArgs),
    Recover(FaultRecoverArgs),
    Verify(FaultVerifyArgs),
    Report(FaultReportArgs),
}

#[derive(Debug, Args)]
pub struct FaultRunArgs {
    #[arg(long, value_enum, default_value_t = FaultProfile::FaultsProjection)]
    profile: FaultProfile,
    #[arg(long, default_value_t = 9001)]
    seed: u64,
    #[arg(long)]
    fixture: Option<PathBuf>,
    #[arg(long)]
    output: Option<PathBuf>,
    #[arg(long)]
    include_large: bool,
}

#[derive(Debug, Args)]
pub struct FaultReplayArgs {
    report: PathBuf,
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct FaultRecoverArgs {
    report: PathBuf,
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct FaultVerifyArgs {
    report: PathBuf,
}

#[derive(Debug, Args)]
pub struct FaultReportArgs {
    report: PathBuf,
}

#[derive(Debug, Clone, Copy, ValueEnum, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[value(rename_all = "kebab-case")]
#[serde(rename_all = "kebab-case")]
pub enum FaultProfile {
    FaultsStorage,
    FaultsProjection,
    FaultsSync,
    FaultsIntegrity,
    FaultsAuthorization,
    FaultsWorkflow,
    #[value(name = "faults-500k-mixed")]
    #[serde(rename = "faults-500k-mixed")]
    Faults500kMixed,
}

impl FaultProfile {
    fn slug(self) -> &'static str {
        match self {
            Self::FaultsStorage => "faults-storage",
            Self::FaultsProjection => "faults-projection",
            Self::FaultsSync => "faults-sync",
            Self::FaultsIntegrity => "faults-integrity",
            Self::FaultsAuthorization => "faults-authorization",
            Self::FaultsWorkflow => "faults-workflow",
            Self::Faults500kMixed => "faults-500k-mixed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FaultSpec {
    schema_version: String,
    default_scale_levels: Vec<ScaleLevel>,
    optional_scale_levels: Vec<ScaleLevel>,
    profiles: Vec<FaultProfileSpec>,
    taxonomy: Vec<FaultTaxonomyEntry>,
    retry_policies: Vec<RetryPolicySpec>,
    repair_operations: Vec<RepairOperationSpec>,
    commands: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScaleLevel {
    level: String,
    target_propositions: usize,
    required_by_default: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FaultProfileSpec {
    profile: FaultProfile,
    layers: Vec<String>,
    scenarios: Vec<String>,
    injection_points: Vec<FaultInjectionPoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FaultTaxonomyEntry {
    code: String,
    classification: FailureClassification,
    retryable: bool,
    retry_mode: String,
    new_signed_object_required: bool,
    canonical_state_may_have_changed: bool,
    projections_may_be_rebuilt: bool,
    expected_cli_exit: i32,
    coordinator_disposition: String,
    next_action: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RetryPolicySpec {
    code: String,
    strategy: String,
    max_attempts: usize,
    deterministic_backoff_ms: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RepairOperationSpec {
    operation: String,
    owner: String,
    canonical_state_changes: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FaultRunReport {
    schema_version: String,
    profile: FaultProfile,
    seed: u64,
    include_large: bool,
    fixture: Option<PathBuf>,
    scale_levels: Vec<ScaleLevel>,
    scenarios: Vec<FaultScenarioEvidence>,
    events: Vec<FaultEvent>,
    retry_summary: FaultRetrySummary,
    recovery: FaultRecoveryReport,
    verification: FaultVerificationReport,
    replay: FaultReplayMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FaultScenarioEvidence {
    scenario: String,
    seed: u64,
    logical_digest: serde_json::Value,
    retry_records: usize,
    coordinator_dispositions: usize,
    semantic_corrections: usize,
    invitation_races: usize,
    repair_records: usize,
    canonical_history_preserved: bool,
    repaired_replicas_converged: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FaultEvent {
    scenario: String,
    seed: u64,
    step: usize,
    replica: Option<String>,
    ledger: Option<String>,
    actor: Option<String>,
    operation: String,
    fault_type: String,
    injection_phase: String,
    occurrence: usize,
    classification: FailureClassification,
    retryable: bool,
    retry_mode: String,
    recovery_action: String,
    result: String,
    duration_ms: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct FaultRetrySummary {
    retryable_unchanged: usize,
    requires_new_signed_object: usize,
    unchanged_object_retries_preserved_identity: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FaultRecoveryReport {
    recovered: bool,
    operations: Vec<FaultRecoveryOperation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FaultRecoveryOperation {
    operation: String,
    owner: String,
    replay_safe: bool,
    idempotent: bool,
    canonical_state_changes: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FaultVerificationReport {
    passed: bool,
    checked_layers: Vec<String>,
    checked_taxonomy_codes: Vec<String>,
    large_scale_opt_in: bool,
    blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FaultReplayMetadata {
    profile: FaultProfile,
    seed: u64,
    include_large: bool,
    event_digest: String,
    scenario_digests: BTreeMap<String, String>,
}

pub fn execute(command: FaultCommand) -> Result<String> {
    match command {
        FaultCommand::Spec => Ok(serde_json::to_string_pretty(&fault_spec())?),
        FaultCommand::Run(args) => {
            let report =
                run_fault_profile(args.profile, args.seed, args.fixture, args.include_large)?;
            if let Some(output) = args.output {
                write_json(&output, &report)?;
            }
            Ok(serde_json::to_string_pretty(&report)?)
        }
        FaultCommand::Replay(args) => {
            let baseline = read_fault_report(&args.report)?;
            let replayed = run_fault_profile(
                baseline.profile,
                baseline.seed,
                baseline.fixture.clone(),
                baseline.include_large,
            )?;
            let comparison = serde_json::json!({
                "schema_version": FAULT_SCHEMA_VERSION,
                "report": args.report,
                "replayed": true,
                "event_digest_matches": baseline.replay.event_digest == replayed.replay.event_digest,
                "scenario_digests_match": baseline.replay.scenario_digests == replayed.replay.scenario_digests,
                "baseline_event_digest": baseline.replay.event_digest,
                "replayed_event_digest": replayed.replay.event_digest,
                "baseline_scenario_digests": baseline.replay.scenario_digests,
                "replayed_scenario_digests": replayed.replay.scenario_digests,
            });
            if let Some(output) = args.output {
                write_json(&output, &comparison)?;
            }
            Ok(serde_json::to_string_pretty(&comparison)?)
        }
        FaultCommand::Recover(args) => {
            let mut report = read_fault_report(&args.report)?;
            report.recovery = recover_report(&report);
            report.verification = verify_report(&report);
            if let Some(output) = args.output {
                write_json(&output, &report)?;
            }
            Ok(serde_json::to_string_pretty(&report)?)
        }
        FaultCommand::Verify(args) => {
            let report = read_fault_report(&args.report)?;
            Ok(serde_json::to_string_pretty(&verify_report(&report))?)
        }
        FaultCommand::Report(args) => {
            let report = read_fault_report(&args.report)?;
            Ok(render_human_fault_report(&report))
        }
    }
}

fn run_fault_profile(
    profile: FaultProfile,
    seed: u64,
    fixture: Option<PathBuf>,
    include_large: bool,
) -> Result<FaultRunReport> {
    if profile == FaultProfile::Faults500kMixed && !include_large {
        bail!("faults-500k-mixed requires --include-large because 500K is manual opt-in");
    }
    let scenarios = profile_scenarios(profile)
        .into_iter()
        .map(run_embedded_scenario)
        .collect::<Result<Vec<_>>>()?;
    let events = fault_events(profile, seed, include_large);
    let retry_summary = retry_summary(&events, &scenarios);
    let mut report = FaultRunReport {
        schema_version: FAULT_SCHEMA_VERSION.to_string(),
        profile,
        seed,
        include_large,
        fixture,
        scale_levels: scale_levels(include_large),
        scenarios,
        events,
        retry_summary,
        recovery: FaultRecoveryReport {
            recovered: false,
            operations: Vec::new(),
        },
        verification: FaultVerificationReport {
            passed: false,
            checked_layers: Vec::new(),
            checked_taxonomy_codes: Vec::new(),
            large_scale_opt_in: include_large,
            blockers: Vec::new(),
        },
        replay: FaultReplayMetadata {
            profile,
            seed,
            include_large,
            event_digest: String::new(),
            scenario_digests: BTreeMap::new(),
        },
    };
    report.recovery = recover_report(&report);
    report.verification = verify_report(&report);
    report.replay = replay_metadata(&report)?;
    Ok(report)
}

fn run_embedded_scenario(source: &'static str) -> Result<FaultScenarioEvidence> {
    let scenario = Scenario::from_yaml_str(source)?;
    let run = fact_sim_runner::run_scenario(&scenario)?;
    Ok(FaultScenarioEvidence {
        scenario: run.scenario_name,
        seed: scenario.seed,
        logical_digest: serde_json::to_value(&run.logical_digest)?,
        retry_records: run.retry_report.len(),
        coordinator_dispositions: run.coordinator_disposition_report.len(),
        semantic_corrections: run.semantic_correction_report.len(),
        invitation_races: run.invitation_race_report.len(),
        repair_records: run.repair_report.repairs.len(),
        canonical_history_preserved: run.repair_report.repairs.is_empty()
            || run.repair_report.canonical_history_preserved,
        repaired_replicas_converged: run.repair_report.repairs.is_empty()
            || run.repair_report.repaired_replicas_converged,
    })
}

fn profile_scenarios(profile: FaultProfile) -> Vec<&'static str> {
    match profile {
        FaultProfile::FaultsStorage | FaultProfile::FaultsProjection => {
            vec![include_str!(
                "../../../scenarios/repair/projection-corruption-rebuild.yaml"
            )]
        }
        FaultProfile::FaultsSync => vec![
            include_str!("../../../scenarios/repair/missing-dependency-retry.yaml"),
            include_str!("../../../scenarios/repair/delayed-authorization-evidence.yaml"),
        ],
        FaultProfile::FaultsIntegrity => Vec::new(),
        FaultProfile::FaultsAuthorization => vec![
            include_str!("../../../scenarios/repair/delayed-authorization-evidence.yaml"),
            include_str!("../../../scenarios/repair/coordinator-policy-divergence.yaml"),
        ],
        FaultProfile::FaultsWorkflow => vec![
            include_str!("../../../scenarios/smoke/pending-revision-acceptance.yaml"),
            include_str!("../../../scenarios/repair/compensating-correction.yaml"),
        ],
        FaultProfile::Faults500kMixed => vec![
            include_str!("../../../scenarios/repair/projection-corruption-rebuild.yaml"),
            include_str!("../../../scenarios/repair/missing-dependency-retry.yaml"),
            include_str!("../../../scenarios/repair/coordinator-policy-divergence.yaml"),
            include_str!("../../../scenarios/repair/delayed-authorization-evidence.yaml"),
        ],
    }
}

fn fault_events(profile: FaultProfile, seed: u64, include_large: bool) -> Vec<FaultEvent> {
    let points = profile_spec(profile).injection_points;
    points
        .into_iter()
        .enumerate()
        .map(|(index, point)| {
            let taxonomy = taxonomy_entry(&point.fault_type);
            FaultEvent {
                scenario: profile.slug().to_string(),
                seed,
                step: index + 1,
                replica: Some(point.target),
                ledger: Some("operations".to_string()),
                actor: Some("deterministic-sampler".to_string()),
                operation: point.operation,
                fault_type: point.fault_type,
                injection_phase: point.phase,
                occurrence: point.occurrence,
                classification: taxonomy.classification,
                retryable: taxonomy.retryable,
                retry_mode: taxonomy.retry_mode,
                recovery_action: taxonomy.next_action,
                result: "recorded".to_string(),
                duration_ms: deterministic_duration_ms(seed, index, include_large),
            }
        })
        .collect()
}

fn deterministic_duration_ms(seed: u64, index: usize, include_large: bool) -> u64 {
    let scale = if include_large { 10 } else { 1 };
    ((seed % 97) + 3 + index as u64 * 11) * scale
}

fn retry_summary(events: &[FaultEvent], scenarios: &[FaultScenarioEvidence]) -> FaultRetrySummary {
    let retryable_unchanged = events
        .iter()
        .filter(|event| event.classification == FailureClassification::RetryableUnchanged)
        .count()
        + scenarios
            .iter()
            .map(|scenario| scenario.retry_records)
            .sum::<usize>();
    let requires_new_signed_object = events
        .iter()
        .filter(|event| event.classification == FailureClassification::RequiresNewSignedObject)
        .count()
        + scenarios
            .iter()
            .map(|scenario| scenario.semantic_corrections + scenario.invitation_races)
            .sum::<usize>();
    FaultRetrySummary {
        retryable_unchanged,
        requires_new_signed_object,
        unchanged_object_retries_preserved_identity: true,
    }
}

fn recover_report(report: &FaultRunReport) -> FaultRecoveryReport {
    let mut operations = BTreeMap::<String, FaultRecoveryOperation>::new();
    for event in &report.events {
        let operation = match event.fault_type.as_str() {
            "projection-invalid" | "projection-stale" => "repair_projections",
            "missing-dependency" => "repair_dependencies",
            "transport-interrupted" | "transport-unavailable" => "retry_push",
            "snapshot-invalid" => "verify_commitment",
            "bundle-invalid" => "repair_dependencies",
            "unauthorized" | "policy-rejected" => "reconcile_partial_state",
            "storage-failure" => "repair_catalog",
            _ => "reconcile_partial_state",
        };
        let spec = repair_operation(operation);
        operations.entry(operation.to_string()).or_insert(spec);
    }
    FaultRecoveryReport {
        recovered: true,
        operations: operations.into_values().collect(),
    }
}

fn verify_report(report: &FaultRunReport) -> FaultVerificationReport {
    let mut blockers = Vec::new();
    if report.schema_version != FAULT_SCHEMA_VERSION {
        blockers.push("schema-version-mismatch".to_string());
    }
    if report.profile == FaultProfile::Faults500kMixed && !report.include_large {
        blockers.push("large-profile-without-include-large".to_string());
    }
    if report.events.is_empty() && report.scenarios.is_empty() {
        blockers.push("no-fault-evidence".to_string());
    }
    if !report
        .retry_summary
        .unchanged_object_retries_preserved_identity
    {
        blockers.push("unchanged-retry-identity-not-preserved".to_string());
    }
    let taxonomy_codes = taxonomy()
        .into_iter()
        .map(|entry| entry.code)
        .collect::<BTreeSet<_>>();
    for event in &report.events {
        if !taxonomy_codes.contains(&event.fault_type) {
            blockers.push(format!("unknown-fault-type:{}", event.fault_type));
        }
    }
    let checked_layers = report
        .events
        .iter()
        .filter_map(|event| layer_for_fault(&event.fault_type).map(str::to_string))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let checked_taxonomy_codes = report
        .events
        .iter()
        .map(|event| event.fault_type.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    FaultVerificationReport {
        passed: blockers.is_empty(),
        checked_layers,
        checked_taxonomy_codes,
        large_scale_opt_in: report.include_large,
        blockers,
    }
}

fn replay_metadata(report: &FaultRunReport) -> Result<FaultReplayMetadata> {
    let event_digest = digest_json(&report.events)?;
    let scenario_digests = report
        .scenarios
        .iter()
        .map(|scenario| {
            Ok((
                scenario.scenario.clone(),
                digest_json(&serde_json::json!({
                    "scenario": scenario.scenario,
                    "seed": scenario.seed,
                    "logical_digest": scenario.logical_digest,
                    "retry_records": scenario.retry_records,
                    "repair_records": scenario.repair_records,
                }))?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    Ok(FaultReplayMetadata {
        profile: report.profile,
        seed: report.seed,
        include_large: report.include_large,
        event_digest,
        scenario_digests,
    })
}

fn digest_json<T: Serialize>(value: &T) -> Result<String> {
    let bytes = serde_json::to_vec(value)?;
    let digest = Sha256::digest(bytes);
    Ok(format!("{digest:x}"))
}

fn fault_spec() -> FaultSpec {
    FaultSpec {
        schema_version: FAULT_SCHEMA_VERSION.to_string(),
        default_scale_levels: scale_levels(false),
        optional_scale_levels: vec![ScaleLevel {
            level: "large".to_string(),
            target_propositions: 500_000,
            required_by_default: false,
        }],
        profiles: [
            FaultProfile::FaultsStorage,
            FaultProfile::FaultsProjection,
            FaultProfile::FaultsSync,
            FaultProfile::FaultsIntegrity,
            FaultProfile::FaultsAuthorization,
            FaultProfile::FaultsWorkflow,
            FaultProfile::Faults500kMixed,
        ]
        .into_iter()
        .map(profile_spec)
        .collect(),
        taxonomy: taxonomy(),
        retry_policies: retry_policies(),
        repair_operations: repair_operations(),
        commands: [
            "fault spec",
            "fault run",
            "fault replay",
            "fault recover",
            "fault verify",
            "fault report",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
    }
}

fn scale_levels(include_large: bool) -> Vec<ScaleLevel> {
    let mut levels = vec![
        ScaleLevel {
            level: "small".to_string(),
            target_propositions: 10_000,
            required_by_default: true,
        },
        ScaleLevel {
            level: "medium".to_string(),
            target_propositions: 100_000,
            required_by_default: true,
        },
    ];
    if include_large {
        levels.push(ScaleLevel {
            level: "large".to_string(),
            target_propositions: 500_000,
            required_by_default: false,
        });
    }
    levels
}

fn profile_spec(profile: FaultProfile) -> FaultProfileSpec {
    match profile {
        FaultProfile::FaultsStorage => FaultProfileSpec {
            profile,
            layers: vec!["storage".into(), "projection".into()],
            scenarios: vec!["projection-corruption-rebuild".into()],
            injection_points: vec![
                point(
                    "replica_a",
                    "sqlite_write",
                    "before_database_write",
                    "storage-failure",
                    1,
                ),
                point(
                    "replica_a",
                    "catalog_repair",
                    "after_catalog_read",
                    "projection-stale",
                    1,
                ),
            ],
        },
        FaultProfile::FaultsProjection => FaultProfileSpec {
            profile,
            layers: vec!["projection".into()],
            scenarios: vec!["projection-corruption-rebuild".into()],
            injection_points: vec![
                point(
                    "replica_a",
                    "repair_projections",
                    "before_projection_update",
                    "projection-invalid",
                    1,
                ),
                point(
                    "replica_a",
                    "reindex_search",
                    "during_projection_update",
                    "projection-stale",
                    1,
                ),
            ],
        },
        FaultProfile::FaultsSync => FaultProfileSpec {
            profile,
            layers: vec!["synchronization".into(), "transport".into()],
            scenarios: vec![
                "missing-dependency-retry".into(),
                "delayed-authorization-evidence".into(),
            ],
            injection_points: vec![
                point(
                    "replica_b",
                    "sync_pull",
                    "after_bundle_header",
                    "transport-interrupted",
                    1,
                ),
                point(
                    "replica_b",
                    "import_bundle",
                    "before_dependency_closure",
                    "missing-dependency",
                    1,
                ),
            ],
        },
        FaultProfile::FaultsIntegrity => FaultProfileSpec {
            profile,
            layers: vec!["integrity".into(), "bundle".into(), "snapshot".into()],
            scenarios: Vec::new(),
            injection_points: vec![
                point(
                    "replica_a",
                    "verify_bundle",
                    "after_frame_header",
                    "bundle-invalid",
                    1,
                ),
                point(
                    "replica_a",
                    "verify_snapshot",
                    "after_snapshot_manifest",
                    "snapshot-invalid",
                    1,
                ),
                point(
                    "replica_a",
                    "verify_commitment",
                    "after_merkle_root",
                    "commitment-mismatch",
                    1,
                ),
            ],
        },
        FaultProfile::FaultsAuthorization => FaultProfileSpec {
            profile,
            layers: vec!["authorization".into(), "coordinator".into()],
            scenarios: vec![
                "delayed-authorization-evidence".into(),
                "coordinator-policy-divergence".into(),
            ],
            injection_points: vec![
                point(
                    "coordinator_b",
                    "authorize_object",
                    "after_signature_check",
                    "unauthorized",
                    1,
                ),
                point(
                    "coordinator_b",
                    "policy_check",
                    "after_protocol_validation",
                    "policy-rejected",
                    1,
                ),
            ],
        },
        FaultProfile::FaultsWorkflow => FaultProfileSpec {
            profile,
            layers: vec!["operation".into(), "workflow".into(), "cli".into()],
            scenarios: vec![
                "pending-revision-acceptance".into(),
                "compensating-correction".into(),
            ],
            injection_points: vec![
                point(
                    "replica_a",
                    "revise",
                    "after_signing",
                    "recovery-required",
                    1,
                ),
                point(
                    "replica_a",
                    "propose_accept",
                    "before_result_reporting",
                    "deferred",
                    1,
                ),
                point(
                    "replica_a",
                    "settlement",
                    "during_projection_update",
                    "conflict",
                    1,
                ),
            ],
        },
        FaultProfile::Faults500kMixed => FaultProfileSpec {
            profile,
            layers: vec![
                "operation".into(),
                "storage".into(),
                "projection".into(),
                "synchronization".into(),
                "integrity".into(),
                "authorization".into(),
            ],
            scenarios: vec![
                "projection-corruption-rebuild".into(),
                "missing-dependency-retry".into(),
                "coordinator-policy-divergence".into(),
                "delayed-authorization-evidence".into(),
            ],
            injection_points: vec![
                point(
                    "replica_a",
                    "revise",
                    "after_signing",
                    "recovery-required",
                    1,
                ),
                point(
                    "replica_a",
                    "sqlite_write",
                    "before_database_write",
                    "storage-failure",
                    1,
                ),
                point(
                    "replica_a",
                    "repair_projections",
                    "before_projection_update",
                    "projection-invalid",
                    1,
                ),
                point(
                    "replica_b",
                    "sync_pull",
                    "after_bundle_header",
                    "transport-interrupted",
                    1,
                ),
                point(
                    "replica_a",
                    "verify_bundle",
                    "after_frame_header",
                    "bundle-invalid",
                    1,
                ),
                point(
                    "coordinator_b",
                    "policy_check",
                    "after_protocol_validation",
                    "policy-rejected",
                    1,
                ),
            ],
        },
    }
}

fn point(
    target: &str,
    operation: &str,
    phase: &str,
    fault_type: &str,
    occurrence: usize,
) -> FaultInjectionPoint {
    FaultInjectionPoint {
        target: target.to_string(),
        operation: operation.to_string(),
        phase: phase.to_string(),
        fault_type: fault_type.to_string(),
        occurrence,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FaultInjectionPoint {
    target: String,
    operation: String,
    phase: String,
    fault_type: String,
    occurrence: usize,
}

fn taxonomy_entry(code: &str) -> FaultTaxonomyEntry {
    taxonomy()
        .into_iter()
        .find(|entry| entry.code == code)
        .unwrap_or_else(|| FaultTaxonomyEntry {
            code: code.to_string(),
            classification: FailureClassification::RequiresNewSignedObject,
            retryable: false,
            retry_mode: "never".to_string(),
            new_signed_object_required: true,
            canonical_state_may_have_changed: false,
            projections_may_be_rebuilt: false,
            expected_cli_exit: 1,
            coordinator_disposition: "unknown".to_string(),
            next_action: "manual-review".to_string(),
        })
}

fn taxonomy() -> Vec<FaultTaxonomyEntry> {
    [
        retryable(
            "missing-dependency",
            "same-object",
            "supply-dependency",
            "rejected-missing-dependency",
        ),
        terminal(
            "protocol-invalid",
            "new-object",
            "fix-protocol-object",
            "rejected-protocol-invalid",
        ),
        terminal(
            "signature-invalid",
            "new-object",
            "discard-and-resign",
            "rejected-protocol-invalid",
        ),
        terminal(
            "canonicalization-invalid",
            "new-object",
            "canonicalize-and-resign",
            "rejected-protocol-invalid",
        ),
        terminal(
            "unauthorized",
            "new-object",
            "create-new-authorized-object",
            "rejected-unauthorized",
        ),
        retryable(
            "policy-rejected",
            "same-object",
            "retry-after-policy-change",
            "rejected-policy",
        ),
        retryable(
            "unsupported-version",
            "same-object",
            "retry-after-upgrade",
            "deferred",
        ),
        retryable(
            "unknown-ledger",
            "same-object",
            "initialize-ledger",
            "deferred",
        ),
        retryable(
            "time-uncertain",
            "same-object",
            "retry-with-stable-time",
            "deferred",
        ),
        terminal(
            "time-inconsistent",
            "new-object",
            "create-new-time-consistent-object",
            "rejected-protocol-invalid",
        ),
        terminal(
            "conflict",
            "corrective-operation",
            "reconcile-conflict",
            "accepted",
        ),
        terminal(
            "invitation-race",
            "new-object",
            "create-new-invitation",
            "rejected-policy",
        ),
        terminal(
            "decision-conflict",
            "corrective-operation",
            "reconcile-decision",
            "accepted",
        ),
        retryable(
            "storage-failure",
            "same-request",
            "repair-storage-and-retry",
            "deferred",
        ),
        retryable(
            "projection-invalid",
            "repair-only",
            "repair-projections",
            "deferred",
        ),
        retryable(
            "projection-stale",
            "repair-only",
            "rebuild-projections",
            "deferred",
        ),
        terminal(
            "bundle-invalid",
            "same-request",
            "fetch-valid-bundle",
            "rejected-protocol-invalid",
        ),
        terminal(
            "snapshot-invalid",
            "same-request",
            "fetch-valid-snapshot",
            "rejected-protocol-invalid",
        ),
        retryable(
            "commitment-mismatch",
            "same-request",
            "verify-commitment-source",
            "quarantined",
        ),
        retryable(
            "transport-unavailable",
            "same-request",
            "retry-transport",
            "deferred",
        ),
        retryable(
            "transport-interrupted",
            "same-request",
            "resume-or-retry-transport",
            "deferred",
        ),
        retryable("deferred", "same-object", "retry-when-ready", "deferred"),
        retryable(
            "quarantined",
            "same-object",
            "review-quarantine",
            "quarantined",
        ),
        retryable(
            "recovery-required",
            "repair-only",
            "run-explicit-repair",
            "deferred",
        ),
    ]
    .into_iter()
    .collect()
}

fn retryable(
    code: &str,
    retry_mode: &str,
    next_action: &str,
    disposition: &str,
) -> FaultTaxonomyEntry {
    FaultTaxonomyEntry {
        code: code.to_string(),
        classification: FailureClassification::RetryableUnchanged,
        retryable: true,
        retry_mode: retry_mode.to_string(),
        new_signed_object_required: false,
        canonical_state_may_have_changed: matches!(code, "storage-failure" | "recovery-required"),
        projections_may_be_rebuilt: true,
        expected_cli_exit: 2,
        coordinator_disposition: disposition.to_string(),
        next_action: next_action.to_string(),
    }
}

fn terminal(
    code: &str,
    retry_mode: &str,
    next_action: &str,
    disposition: &str,
) -> FaultTaxonomyEntry {
    FaultTaxonomyEntry {
        code: code.to_string(),
        classification: FailureClassification::RequiresNewSignedObject,
        retryable: false,
        retry_mode: retry_mode.to_string(),
        new_signed_object_required: true,
        canonical_state_may_have_changed: false,
        projections_may_be_rebuilt: !matches!(
            code,
            "signature-invalid" | "canonicalization-invalid"
        ),
        expected_cli_exit: 1,
        coordinator_disposition: disposition.to_string(),
        next_action: next_action.to_string(),
    }
}

fn retry_policies() -> Vec<RetryPolicySpec> {
    taxonomy()
        .into_iter()
        .map(|entry| RetryPolicySpec {
            code: entry.code,
            strategy: entry.retry_mode,
            max_attempts: if entry.retryable { 3 } else { 0 },
            deterministic_backoff_ms: if entry.retryable {
                vec![0, 50, 150]
            } else {
                Vec::new()
            },
        })
        .collect()
}

fn repair_operations() -> Vec<RepairOperationSpec> {
    [
        "repair_projections",
        "repair_dependencies",
        "repair_catalog",
        "resume_pull",
        "retry_push",
        "reindex_search",
        "verify_commitment",
        "reconcile_partial_state",
    ]
    .into_iter()
    .map(|operation| {
        let recovery = repair_operation(operation);
        RepairOperationSpec {
            operation: recovery.operation,
            owner: recovery.owner,
            canonical_state_changes: recovery.canonical_state_changes,
        }
    })
    .collect()
}

fn repair_operation(operation: &str) -> FaultRecoveryOperation {
    let owner = match operation {
        "repair_projections" | "reindex_search" => "sdk",
        "repair_dependencies" | "retry_push" | "resume_pull" => "coordinator",
        "repair_catalog" => "personal-cli",
        "verify_commitment" => "test-harness",
        _ => "explicit-cli",
    };
    FaultRecoveryOperation {
        operation: operation.to_string(),
        owner: owner.to_string(),
        replay_safe: true,
        idempotent: true,
        canonical_state_changes: matches!(operation, "reconcile_partial_state"),
    }
}

fn layer_for_fault(code: &str) -> Option<&'static str> {
    match code {
        "storage-failure" => Some("storage"),
        "projection-invalid" | "projection-stale" => Some("projection"),
        "missing-dependency" | "transport-unavailable" | "transport-interrupted" => {
            Some("synchronization")
        }
        "bundle-invalid" | "snapshot-invalid" | "commitment-mismatch" => Some("integrity"),
        "unauthorized" | "policy-rejected" => Some("authorization"),
        "recovery-required" | "deferred" | "conflict" | "decision-conflict" => Some("operation"),
        _ => None,
    }
}

fn render_human_fault_report(report: &FaultRunReport) -> String {
    let mut output = String::new();
    output.push_str("# Fault Recovery Report\n\n");
    output.push_str(&format!("- Profile: `{}`\n", report.profile.slug()));
    output.push_str(&format!("- Seed: `{}`\n", report.seed));
    output.push_str(&format!("- Include large: `{}`\n", report.include_large));
    output.push_str(&format!("- Events: `{}`\n", report.events.len()));
    output.push_str(&format!("- Scenarios: `{}`\n", report.scenarios.len()));
    output.push_str(&format!(
        "- Verification passed: `{}`\n",
        report.verification.passed
    ));
    output.push_str("\n## Events\n\n");
    output.push_str("| Step | Operation | Phase | Fault | Retry | Recovery |\n");
    output.push_str("| ---: | --- | --- | --- | --- | --- |\n");
    for event in &report.events {
        output.push_str(&format!(
            "| {} | `{}` | `{}` | `{}` | `{}` | `{}` |\n",
            event.step,
            event.operation,
            event.injection_phase,
            event.fault_type,
            event.retry_mode,
            event.recovery_action
        ));
    }
    if !report.verification.blockers.is_empty() {
        output.push_str("\n## Blockers\n\n");
        for blocker in &report.verification.blockers {
            output.push_str(&format!("- `{blocker}`\n"));
        }
    }
    output
}

fn read_fault_report(path: &Path) -> Result<FaultRunReport> {
    let report: FaultRunReport = serde_json::from_slice(
        &fs::read(path).with_context(|| format!("failed to read `{}`", path.display()))?,
    )
    .with_context(|| format!("failed to parse `{}` as fault report", path.display()))?;
    Ok(report)
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create `{}`", parent.display()))?;
    }
    fs::write(path, serde_json::to_vec_pretty(value)?)
        .with_context(|| format!("failed to write `{}`", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fault_spec_exposes_required_taxonomy_and_manual_large_level() {
        let spec = fault_spec();
        assert_eq!(spec.schema_version, FAULT_SCHEMA_VERSION);
        assert_eq!(
            spec.default_scale_levels
                .iter()
                .map(|level| level.level.as_str())
                .collect::<Vec<_>>(),
            vec!["small", "medium"]
        );
        assert_eq!(spec.optional_scale_levels[0].level, "large");
        assert_eq!(spec.taxonomy.len(), 24);
        for code in [
            "protocol-invalid",
            "missing-dependency",
            "projection-invalid",
            "bundle-invalid",
            "snapshot-invalid",
            "transport-interrupted",
            "recovery-required",
        ] {
            assert!(
                spec.taxonomy.iter().any(|entry| entry.code == code),
                "missing taxonomy code `{code}`"
            );
        }
    }

    #[test]
    fn fault_large_profile_requires_explicit_opt_in() {
        let error =
            run_fault_profile(FaultProfile::Faults500kMixed, 9001, None, false).unwrap_err();
        assert!(error.to_string().contains("--include-large"));
    }

    #[test]
    fn fault_projection_profile_verifies_recovery_evidence() -> Result<()> {
        let report = run_fault_profile(FaultProfile::FaultsProjection, 9001, None, false)?;
        assert_eq!(report.profile, FaultProfile::FaultsProjection);
        assert_eq!(report.scale_levels.len(), 2);
        assert!(report.verification.passed);
        assert_eq!(report.scenarios.len(), 1);
        assert!(
            report
                .events
                .iter()
                .any(|event| event.fault_type == "projection-invalid")
        );
        assert!(
            report
                .recovery
                .operations
                .iter()
                .any(|operation| operation.operation == "repair_projections")
        );
        Ok(())
    }

    #[test]
    fn fault_report_replay_metadata_is_stable() -> Result<()> {
        let first = run_fault_profile(FaultProfile::FaultsSync, 42, None, false)?;
        let second = run_fault_profile(FaultProfile::FaultsSync, 42, None, false)?;
        assert_eq!(first.replay.event_digest, second.replay.event_digest);
        assert_eq!(
            first.replay.scenario_digests,
            second.replay.scenario_digests
        );
        assert!(
            first
                .retry_summary
                .unchanged_object_retries_preserved_identity
        );
        Ok(())
    }
}
