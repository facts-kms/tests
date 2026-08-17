use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const UX_SCHEMA_VERSION: &str = "ux-v1";
const DEFAULT_SEED: u64 = 8008;

#[derive(Debug, Subcommand)]
pub enum UxCommand {
    Spec,
    Run(UxRunArgs),
    Replay(UxReplayArgs),
    Report(UxReportArgs),
    Compare(UxCompareArgs),
}

#[derive(Debug, Args)]
pub struct UxRunArgs {
    #[arg(long, value_enum, default_value_t = UxSuite::Casual)]
    suite: UxSuite,
    #[arg(long)]
    scenario: Option<String>,
    #[arg(long, default_value_t = DEFAULT_SEED)]
    seed: u64,
    #[arg(long)]
    fixture: Option<PathBuf>,
    #[arg(long)]
    output: Option<PathBuf>,
    #[arg(long, env = "FACT_BINARY")]
    fact_binary: Option<PathBuf>,
    #[arg(long)]
    include_large: bool,
}

#[derive(Debug, Args)]
pub struct UxReplayArgs {
    report: PathBuf,
    #[arg(long)]
    output: Option<PathBuf>,
    #[arg(long, env = "FACT_BINARY")]
    fact_binary: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct UxReportArgs {
    report: PathBuf,
}

#[derive(Debug, Args)]
pub struct UxCompareArgs {
    baseline: PathBuf,
    current: PathBuf,
}

#[derive(Debug, Clone, Copy, ValueEnum, Serialize, Deserialize, PartialEq, Eq)]
#[value(rename_all = "kebab-case")]
#[serde(rename_all = "kebab-case")]
pub enum UxSuite {
    Smoke,
    Casual,
    Help,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UxSpec {
    schema_version: String,
    default_scale_levels: Vec<UxScaleLevel>,
    optional_scale_levels: Vec<UxScaleLevel>,
    suites: Vec<UxSuiteSpec>,
    defect_categories: Vec<String>,
    required_commands: Vec<String>,
    reports: Vec<String>,
    commands: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UxSuiteSpec {
    suite: UxSuite,
    scenarios: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UxScaleLevel {
    level: String,
    target_propositions: usize,
    required_by_default: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UxRunReport {
    schema_version: String,
    suite: UxSuite,
    seed: u64,
    fixture: Option<PathBuf>,
    include_large: bool,
    fact_binary: PathBuf,
    scale_levels: Vec<UxScaleLevel>,
    scenarios: Vec<UxScenarioReport>,
    command_coverage: UxCommandCoverageReport,
    user_belief_report: UxUserBeliefReport,
    cross_command_consistency_report: UxConsistencyReport,
    human_json_report: UxHumanJsonReport,
    help_terminology_report: UxHelpTerminologyReport,
    ambiguity_report: UxAmbiguityReport,
    error_quality_report: UxErrorQualityReport,
    missing_inspection_path_report: UxMissingInspectionPathReport,
    command_symmetry_report: UxCommandSymmetryReport,
    scale_sampling_report: UxScaleSamplingReport,
    ux_defect_summary: UxDefectSummary,
    replay: UxReplayMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UxScenarioReport {
    scenario: String,
    seed: u64,
    fixture: Option<PathBuf>,
    steps: Vec<UxStepReport>,
    canonical_state: UxCanonicalState,
    defects: Vec<UxDefect>,
    passed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UxStepReport {
    step: usize,
    intent: String,
    command: Vec<String>,
    receipt: UxCommandReceipt,
    parsed_semantics: UxParsedSemantics,
    belief: UxBelief,
    assertion: UxAssertion,
    next_action_clear: bool,
    cognitive_load: UxCognitiveLoad,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UxCommandReceipt {
    command_line: String,
    environment: BTreeMap<String, String>,
    exit_status: i32,
    stdout: String,
    stderr: String,
    json_output: Option<serde_json::Value>,
    duration_ms: u128,
    active_ledger: Option<String>,
    active_actor: Option<String>,
    protocol_references: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct UxParsedSemantics {
    command_kind: String,
    proposition_reference: Option<String>,
    proposition_id: Option<String>,
    revision_id: Option<String>,
    status: Option<String>,
    effective_status: Option<String>,
    latest_revision_status: Option<String>,
    summary: Option<String>,
    pending: Option<bool>,
    next_action: Option<String>,
    error_category: Option<String>,
    retryable: Option<bool>,
    state_changed: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct UxBelief {
    proposition: Option<String>,
    status: Option<String>,
    effective_revision: Option<String>,
    latest_revision: Option<String>,
    pending_action: Option<String>,
    source_command: String,
    supported: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UxAssertion {
    passed: bool,
    expected_belief: UxBelief,
    actual_belief: UxBelief,
    canonical_state: UxCanonicalState,
    classifications: Vec<UxDefectCategory>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct UxCanonicalState {
    active_ledger: Option<String>,
    proposition_id: Option<String>,
    reference: Option<String>,
    effective_revision_id: Option<String>,
    latest_revision_id: Option<String>,
    status: Option<String>,
    latest_revision_status: Option<String>,
    summary: Option<String>,
    pending_count: usize,
    archived: bool,
    withdrawn: bool,
    contested: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UxCognitiveLoad {
    concepts: Vec<String>,
    identifiers: Vec<String>,
    excessive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UxDefect {
    scenario: String,
    step: usize,
    command: String,
    fixture: Option<PathBuf>,
    seed: u64,
    expected_belief: UxBelief,
    actual_output: String,
    canonical_state: UxCanonicalState,
    classification: UxDefectCategory,
    suggested_resolution: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
enum UxDefectCategory {
    StaleDisplay,
    CrossCommandInconsistency,
    EffectiveStateMismatch,
    PendingStateMismatch,
    MissingNextAction,
    AmbiguousTargetSelected,
    InsufficientOutput,
    MisleadingSuccess,
    MisleadingError,
    HumanJsonMismatch,
    TerminologyOverload,
    MissingInspectionPath,
    InterfaceAsymmetry,
    UnexpectedSideEffect,
    HiddenPartialState,
    ReferenceResolutionError,
    MissingCommand,
    CliUnavailable,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct UxCommandCoverageReport {
    required_commands: Vec<String>,
    observed_commands: Vec<String>,
    missing_commands: Vec<String>,
    unsupported_commands: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct UxUserBeliefReport {
    beliefs: Vec<UxBelief>,
    unsupported_beliefs: Vec<UxBelief>,
    passed: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct UxConsistencyReport {
    checked_questions: Vec<String>,
    contradictions: Vec<String>,
    passed: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct UxHumanJsonReport {
    pairs_checked: usize,
    mismatches: Vec<String>,
    passed: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct UxHelpTerminologyReport {
    checked_commands: Vec<String>,
    jargon_hits: Vec<UxTerminologyHit>,
    missing_help_commands: Vec<String>,
    passed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UxTerminologyHit {
    command: String,
    term: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct UxAmbiguityReport {
    checked: Vec<String>,
    unsafe_guesses: Vec<String>,
    passed: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct UxErrorQualityReport {
    checked_errors: Vec<String>,
    defects: Vec<String>,
    passed: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct UxMissingInspectionPathReport {
    mutations_checked: Vec<String>,
    missing_paths: Vec<String>,
    passed: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct UxCommandSymmetryReport {
    pairs_checked: Vec<String>,
    asymmetries: Vec<String>,
    passed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UxScaleSamplingReport {
    levels: Vec<UxScaleLevel>,
    deterministic_sampling: bool,
    include_large: bool,
    samples: Vec<UxScaleSample>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UxScaleSample {
    level: String,
    target_propositions: usize,
    sample_count: usize,
    fixture_required: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct UxDefectSummary {
    total: usize,
    by_classification: BTreeMap<String, usize>,
    defects: Vec<UxDefect>,
    passed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UxReplayMetadata {
    suite: UxSuite,
    scenario_filter: Option<String>,
    seed: u64,
    include_large: bool,
    semantic_digest: String,
    command_digest: String,
}

pub fn execute(command: UxCommand) -> Result<String> {
    match command {
        UxCommand::Spec => Ok(serde_json::to_string_pretty(&ux_spec())?),
        UxCommand::Run(args) => {
            let report = run_ux_suite(&args)?;
            if let Some(output) = args.output {
                write_json(&output, &report)?;
            }
            Ok(serde_json::to_string_pretty(&report)?)
        }
        UxCommand::Replay(args) => {
            let baseline = read_ux_report(&args.report)?;
            let run_args = UxRunArgs {
                suite: baseline.suite,
                scenario: baseline.replay.scenario_filter.clone(),
                seed: baseline.seed,
                fixture: baseline.fixture.clone(),
                output: None,
                fact_binary: args.fact_binary.or(Some(baseline.fact_binary.clone())),
                include_large: baseline.include_large,
            };
            let replayed = run_ux_suite(&run_args)?;
            let comparison = serde_json::json!({
                "schema_version": UX_SCHEMA_VERSION,
                "report": args.report,
                "replayed": true,
                "semantic_digest_matches": baseline.replay.semantic_digest == replayed.replay.semantic_digest,
                "command_digest_matches": baseline.replay.command_digest == replayed.replay.command_digest,
                "baseline_semantic_digest": baseline.replay.semantic_digest,
                "replayed_semantic_digest": replayed.replay.semantic_digest,
                "baseline_command_digest": baseline.replay.command_digest,
                "replayed_command_digest": replayed.replay.command_digest,
            });
            if let Some(output) = args.output {
                write_json(&output, &comparison)?;
            }
            Ok(serde_json::to_string_pretty(&comparison)?)
        }
        UxCommand::Report(args) => {
            let report = read_ux_report(&args.report)?;
            Ok(render_human_report(&report))
        }
        UxCommand::Compare(args) => {
            let baseline = read_ux_report(&args.baseline)?;
            let current = read_ux_report(&args.current)?;
            Ok(serde_json::to_string_pretty(&compare_reports(
                &args.baseline,
                &baseline,
                &args.current,
                &current,
            )?)?)
        }
    }
}

fn run_ux_suite(args: &UxRunArgs) -> Result<UxRunReport> {
    if args
        .fixture
        .as_ref()
        .is_some_and(|fixture| fixture.to_string_lossy().contains("scale-500k"))
        && !args.include_large
    {
        bail!("500K UX fixtures require --include-large because 500K is manual opt-in");
    }
    let fact_binary = args
        .fact_binary
        .clone()
        .unwrap_or_else(default_fact_binary_path);
    if !fact_binary.is_file() {
        bail!(
            "fact binary `{}` does not exist; set FACT_BINARY or pass --fact-binary",
            fact_binary.display()
        );
    }
    let scenario_names = suite_scenarios(args.suite, args.scenario.as_deref())?;
    let mut scenarios = Vec::new();
    for (index, scenario) in scenario_names.iter().enumerate() {
        let seed = args.seed + index as u64;
        scenarios.push(run_scenario(
            scenario,
            seed,
            args.fixture.clone(),
            &fact_binary,
        )?);
    }
    let command_coverage = command_coverage(&fact_binary)?;
    let user_belief_report = user_belief_report(&scenarios);
    let cross_command_consistency_report = consistency_report(&scenarios);
    let human_json_report = human_json_report(&scenarios);
    let help_terminology_report = help_terminology_report(&fact_binary)?;
    let ambiguity_report = ambiguity_report(&scenarios);
    let error_quality_report = error_quality_report(&scenarios);
    let missing_inspection_path_report = missing_inspection_path_report(&scenarios);
    let command_symmetry_report = command_symmetry_report(&fact_binary)?;
    let scale_sampling_report = scale_sampling_report(args.include_large);
    let ux_defect_summary = defect_summary(&scenarios, &command_coverage, &help_terminology_report);
    let mut report = UxRunReport {
        schema_version: UX_SCHEMA_VERSION.to_string(),
        suite: args.suite,
        seed: args.seed,
        fixture: args.fixture.clone(),
        include_large: args.include_large,
        fact_binary,
        scale_levels: scale_levels(args.include_large),
        scenarios,
        command_coverage,
        user_belief_report,
        cross_command_consistency_report,
        human_json_report,
        help_terminology_report,
        ambiguity_report,
        error_quality_report,
        missing_inspection_path_report,
        command_symmetry_report,
        scale_sampling_report,
        ux_defect_summary,
        replay: UxReplayMetadata {
            suite: args.suite,
            scenario_filter: args.scenario.clone(),
            seed: args.seed,
            include_large: args.include_large,
            semantic_digest: String::new(),
            command_digest: String::new(),
        },
    };
    report.replay.semantic_digest = digest_json(&semantic_replay_payload(&report))?;
    report.replay.command_digest = digest_json(&command_replay_payload(&report))?;
    Ok(report)
}

fn run_scenario(
    scenario: &str,
    seed: u64,
    fixture: Option<PathBuf>,
    fact_binary: &Path,
) -> Result<UxScenarioReport> {
    match scenario {
        "first-run" => run_first_run(seed, fixture, fact_binary),
        "immediate-personal-acceptance" => {
            run_immediate_personal_acceptance(seed, fixture, fact_binary)
        }
        "immediate-rejection" => run_immediate_rejection(seed, fixture, fact_binary),
        "accepted-revision" => run_accepted_revision(seed, fixture, fact_binary),
        "rejected-revision" => run_rejected_revision(seed, fixture, fact_binary),
        "comments-and-revision-context" => {
            run_comments_and_revision_context(seed, fixture, fact_binary)
        }
        "invitation-and-participation" => {
            run_invitation_and_participation(seed, fixture, fact_binary)
        }
        "multi-participant-acceptance" => {
            run_multi_participant_acceptance(seed, fixture, fact_binary)
        }
        "archive-and-withdrawal" => run_archive_and_withdrawal(seed, fixture, fact_binary),
        "ledger-switching" => run_ledger_switching(seed, fixture, fact_binary),
        "remote-workflow" => run_remote_workflow(seed, fixture, fact_binary),
        "ambiguous-target-workflow" => run_ambiguous_target_workflow(seed, fixture, fact_binary),
        "failure-and-recovery" => run_failure_and_recovery(seed, fixture, fact_binary),
        "contested-proposition" => run_contested_proposition(seed, fixture, fact_binary),
        "help-discoverability" => run_help_discoverability(seed, fixture, fact_binary),
        other => Ok(gap_scenario(other, seed, fixture)),
    }
}

fn run_first_run(
    seed: u64,
    fixture: Option<PathBuf>,
    fact_binary: &Path,
) -> Result<UxScenarioReport> {
    let temp = tempfile::Builder::new()
        .prefix("fact-ux-first-run-")
        .tempdir_in("/private/tmp")?;
    let home = temp.path().to_path_buf();
    let mut steps = Vec::new();
    let init = run_fact(fact_binary, &home, &["init", "ux-first-run", "--no-pager"])?;
    steps.push(step(
        1,
        "create a local ledger",
        init,
        UxCanonicalState {
            active_ledger: Some("ux-first-run".into()),
            ..UxCanonicalState::default()
        },
    ));
    let status = run_fact(fact_binary, &home, &["status", "--json", "--no-pager"])?;
    let active_ledger = status
        .json_output
        .as_ref()
        .and_then(|json| json["active"].as_str().or_else(|| json["ledger"].as_str()))
        .map(str::to_string)
        .or_else(|| Some("ux-first-run".into()));
    steps.push(step(
        2,
        "inspect active ledger",
        status,
        UxCanonicalState {
            active_ledger,
            ..UxCanonicalState::default()
        },
    ));
    let proposed = run_fact(
        fact_binary,
        &home,
        &[
            "propose",
            "--message",
            "Deployment policy v1",
            "--json",
            "--no-pager",
        ],
    )?;
    let proposition_id = json_string(&proposed, "proposition_id");
    let revision_id = json_string(&proposed, "revision_id");
    let summary = json_string(&proposed, "summary");
    let reference = proposition_id.clone();
    let pending_state = UxCanonicalState {
        active_ledger: Some("ux-first-run".into()),
        proposition_id: proposition_id.clone(),
        reference: reference.clone(),
        effective_revision_id: revision_id.clone(),
        latest_revision_id: revision_id.clone(),
        status: Some("pending".into()),
        latest_revision_status: Some("pending".into()),
        summary: summary.clone(),
        pending_count: 1,
        ..UxCanonicalState::default()
    };
    steps.push(step(3, "propose a fact", proposed, pending_state.clone()));
    let accepted = run_fact(fact_binary, &home, &["accept", "--json", "--no-pager"])?;
    let accepted_state = UxCanonicalState {
        status: Some("accepted".into()),
        latest_revision_status: Some("accepted".into()),
        pending_count: 0,
        ..pending_state.clone()
    };
    steps.push(step(
        4,
        "accept the pending fact",
        accepted,
        accepted_state.clone(),
    ));
    let list_human = run_fact(fact_binary, &home, &["list", "--no-pager"])?;
    steps.push(step(
        5,
        "list current facts",
        list_human,
        accepted_state.clone(),
    ));
    let list_json = run_fact(fact_binary, &home, &["list", "--json", "--no-pager"])?;
    steps.push(step(
        6,
        "list current facts as JSON",
        list_json,
        accepted_state.clone(),
    ));
    if let Some(reference) = &accepted_state.proposition_id {
        let echo = run_fact(fact_binary, &home, &["echo", reference, "--no-pager"])?;
        steps.push(step(
            7,
            "read effective content",
            echo,
            accepted_state.clone(),
        ));
        let search = run_fact(
            fact_binary,
            &home,
            &["search", "Deployment", "--json", "--no-pager"],
        )?;
        steps.push(step(
            8,
            "search for effective content",
            search,
            accepted_state.clone(),
        ));
    }
    finalize_scenario("first-run", seed, fixture, steps, accepted_state)
}

fn run_immediate_personal_acceptance(
    seed: u64,
    fixture: Option<PathBuf>,
    fact_binary: &Path,
) -> Result<UxScenarioReport> {
    let temp = tempfile::Builder::new()
        .prefix("fact-ux-immediate-")
        .tempdir_in("/private/tmp")?;
    let home = temp.path().to_path_buf();
    let mut steps = Vec::new();
    let init = run_fact(fact_binary, &home, &["init", "ux-immediate", "--no-pager"])?;
    steps.push(step(
        1,
        "create a local ledger",
        init,
        UxCanonicalState::default(),
    ));
    let proposed = run_fact(
        fact_binary,
        &home,
        &[
            "propose",
            "--message",
            "Immediate acceptance policy",
            "--decision",
            "accept",
            "--json",
            "--no-pager",
        ],
    )?;
    let proposition_id = json_string(&proposed, "proposition_id");
    let revision_id = json_string(&proposed, "revision_id");
    let state = UxCanonicalState {
        active_ledger: Some("ux-immediate".into()),
        proposition_id: proposition_id.clone(),
        reference: proposition_id.clone(),
        effective_revision_id: revision_id.clone(),
        latest_revision_id: revision_id,
        status: Some("accepted".into()),
        latest_revision_status: Some("accepted".into()),
        summary: Some("Immediate acceptance policy".into()),
        pending_count: 0,
        ..UxCanonicalState::default()
    };
    steps.push(step(
        2,
        "propose and accept in one command",
        proposed,
        state.clone(),
    ));
    let list = run_fact(fact_binary, &home, &["list", "--json", "--no-pager"])?;
    steps.push(step(3, "inspect accepted result", list, state.clone()));
    finalize_scenario("immediate-personal-acceptance", seed, fixture, steps, state)
}

fn run_immediate_rejection(
    seed: u64,
    fixture: Option<PathBuf>,
    fact_binary: &Path,
) -> Result<UxScenarioReport> {
    let temp = tempfile::Builder::new()
        .prefix("fact-ux-immediate-reject-")
        .tempdir_in("/private/tmp")?;
    let home = temp.path().to_path_buf();
    let mut steps = Vec::new();
    steps.push(step(
        1,
        "create a local ledger",
        run_fact(
            fact_binary,
            &home,
            &["init", "ux-immediate-reject", "--no-pager"],
        )?,
        UxCanonicalState {
            active_ledger: Some("ux-immediate-reject".into()),
            ..UxCanonicalState::default()
        },
    ));
    let rejected = run_fact(
        fact_binary,
        &home,
        &[
            "propose",
            "--message",
            "Rejected onboarding policy",
            "--decision",
            "reject",
            "--json",
            "--no-pager",
        ],
    )?;
    let proposition_id = json_string(&rejected, "proposition_id");
    let revision_id = json_string(&rejected, "revision_id");
    let state = UxCanonicalState {
        active_ledger: Some("ux-immediate-reject".into()),
        proposition_id: proposition_id.clone(),
        reference: proposition_id.clone(),
        effective_revision_id: revision_id.clone(),
        latest_revision_id: revision_id,
        status: Some("rejected".into()),
        latest_revision_status: Some("rejected".into()),
        summary: Some("Rejected onboarding policy".into()),
        pending_count: 0,
        ..UxCanonicalState::default()
    };
    steps.push(step(
        2,
        "propose and reject in one command",
        rejected,
        state.clone(),
    ));
    let history = if let Some(reference) = &state.proposition_id {
        run_fact(
            fact_binary,
            &home,
            &["history", reference, "--json", "--no-pager"],
        )?
    } else {
        run_fact(fact_binary, &home, &["history", "--json", "--no-pager"])?
    };
    steps.push(step(3, "inspect rejected history", history, state.clone()));
    let list = run_fact(
        fact_binary,
        &home,
        &["list", "--all", "--json", "--no-pager"],
    )?;
    steps.push(step(
        4,
        "list including rejected propositions",
        list,
        state.clone(),
    ));
    finalize_scenario("immediate-rejection", seed, fixture, steps, state)
}

fn run_accepted_revision(
    seed: u64,
    fixture: Option<PathBuf>,
    fact_binary: &Path,
) -> Result<UxScenarioReport> {
    let temp = tempfile::Builder::new()
        .prefix("fact-ux-revision-")
        .tempdir_in("/private/tmp")?;
    let home = temp.path().to_path_buf();
    let mut steps = Vec::new();
    steps.push(step(
        1,
        "create a local ledger",
        run_fact(fact_binary, &home, &["init", "ux-revision", "--no-pager"])?,
        UxCanonicalState::default(),
    ));
    let created = run_fact(
        fact_binary,
        &home,
        &[
            "propose",
            "--message",
            "Revision policy v1",
            "--decision",
            "accept",
            "--json",
            "--no-pager",
        ],
    )?;
    let proposition_id = json_string(&created, "proposition_id");
    let first_revision = json_string(&created, "revision_id");
    let reference = proposition_id.clone();
    let accepted_state = UxCanonicalState {
        active_ledger: Some("ux-revision".into()),
        proposition_id: proposition_id.clone(),
        reference: reference.clone(),
        effective_revision_id: first_revision.clone(),
        latest_revision_id: first_revision.clone(),
        status: Some("accepted".into()),
        latest_revision_status: Some("accepted".into()),
        summary: Some("Revision policy v1".into()),
        pending_count: 0,
        ..UxCanonicalState::default()
    };
    steps.push(step(
        2,
        "create accepted fact",
        created,
        accepted_state.clone(),
    ));
    if let Some(reference) = &reference {
        let revised = run_fact(
            fact_binary,
            &home,
            &[
                "revise",
                reference,
                "--message",
                "Revision policy v2",
                "--json",
                "--no-pager",
            ],
        )?;
        let pending_revision = json_string(&revised, "revision_id");
        let pending_state = UxCanonicalState {
            latest_revision_id: pending_revision,
            latest_revision_status: Some("pending".into()),
            summary: Some("Revision policy v1".into()),
            pending_count: 1,
            ..accepted_state.clone()
        };
        steps.push(step(
            3,
            "revise accepted fact",
            revised,
            pending_state.clone(),
        ));
        let list = run_fact(fact_binary, &home, &["list", "--json", "--no-pager"])?;
        steps.push(step(
            4,
            "list effective state with pending update",
            list,
            pending_state.clone(),
        ));
        let pending = run_fact(fact_binary, &home, &["pending", "--json", "--no-pager"])?;
        steps.push(step(
            5,
            "inspect pending update",
            pending,
            pending_state.clone(),
        ));
        let revisions = run_fact(
            fact_binary,
            &home,
            &["revisions", reference, "--json", "--no-pager"],
        )?;
        steps.push(step(
            6,
            "inspect revision history",
            revisions,
            pending_state.clone(),
        ));
        let accepted = run_fact(fact_binary, &home, &["accept", "--json", "--no-pager"])?;
        let final_revision = json_string(&accepted, "revision_id");
        let final_state = UxCanonicalState {
            effective_revision_id: final_revision.clone(),
            latest_revision_id: final_revision,
            latest_revision_status: Some("accepted".into()),
            summary: Some("Revision policy v2".into()),
            pending_count: 0,
            ..accepted_state.clone()
        };
        steps.push(step(
            7,
            "accept pending update",
            accepted,
            final_state.clone(),
        ));
        let list_after = run_fact(fact_binary, &home, &["list", "--json", "--no-pager"])?;
        steps.push(step(
            8,
            "list final effective state",
            list_after,
            final_state.clone(),
        ));
        return finalize_scenario("accepted-revision", seed, fixture, steps, final_state);
    }
    finalize_scenario("accepted-revision", seed, fixture, steps, accepted_state)
}

fn run_rejected_revision(
    seed: u64,
    fixture: Option<PathBuf>,
    fact_binary: &Path,
) -> Result<UxScenarioReport> {
    let temp = tempfile::Builder::new()
        .prefix("fact-ux-rejected-revision-")
        .tempdir_in("/private/tmp")?;
    let home = temp.path().to_path_buf();
    let mut steps = Vec::new();
    steps.push(step(
        1,
        "create a local ledger",
        run_fact(
            fact_binary,
            &home,
            &["init", "ux-rejected-revision", "--no-pager"],
        )?,
        UxCanonicalState {
            active_ledger: Some("ux-rejected-revision".into()),
            ..UxCanonicalState::default()
        },
    ));
    let created = run_fact(
        fact_binary,
        &home,
        &[
            "propose",
            "--message",
            "Rejected revision policy v1",
            "--decision",
            "accept",
            "--json",
            "--no-pager",
        ],
    )?;
    let proposition_id = json_string(&created, "proposition_id");
    let first_revision = json_string(&created, "revision_id");
    let accepted_state = UxCanonicalState {
        active_ledger: Some("ux-rejected-revision".into()),
        proposition_id: proposition_id.clone(),
        reference: proposition_id.clone(),
        effective_revision_id: first_revision.clone(),
        latest_revision_id: first_revision.clone(),
        status: Some("accepted".into()),
        latest_revision_status: Some("accepted".into()),
        summary: Some("Rejected revision policy v1".into()),
        pending_count: 0,
        ..UxCanonicalState::default()
    };
    steps.push(step(
        2,
        "create accepted fact",
        created,
        accepted_state.clone(),
    ));
    if let Some(reference) = &proposition_id {
        let revised = run_fact(
            fact_binary,
            &home,
            &[
                "revise",
                reference,
                "--message",
                "Rejected revision policy v2",
                "--json",
                "--no-pager",
            ],
        )?;
        let pending_revision = json_string(&revised, "revision_id");
        let pending_state = UxCanonicalState {
            latest_revision_id: pending_revision,
            latest_revision_status: Some("pending".into()),
            pending_count: 1,
            ..accepted_state.clone()
        };
        steps.push(step(
            3,
            "revise accepted fact",
            revised,
            pending_state.clone(),
        ));
        let rejected = run_fact(fact_binary, &home, &["reject", "--json", "--no-pager"])?;
        let rejected_state = UxCanonicalState {
            latest_revision_status: Some("rejected".into()),
            pending_count: 0,
            ..accepted_state.clone()
        };
        steps.push(step(
            4,
            "reject pending revision",
            rejected,
            rejected_state.clone(),
        ));
        let list = run_fact(fact_binary, &home, &["list", "--json", "--no-pager"])?;
        steps.push(step(
            5,
            "verify prior revision remains effective",
            list,
            rejected_state.clone(),
        ));
        let revisions = run_fact(
            fact_binary,
            &home,
            &["revisions", reference, "--json", "--no-pager"],
        )?;
        steps.push(step(
            6,
            "inspect rejected revision in history",
            revisions,
            rejected_state.clone(),
        ));
        return finalize_scenario("rejected-revision", seed, fixture, steps, rejected_state);
    }
    finalize_scenario("rejected-revision", seed, fixture, steps, accepted_state)
}

fn run_comments_and_revision_context(
    seed: u64,
    fixture: Option<PathBuf>,
    fact_binary: &Path,
) -> Result<UxScenarioReport> {
    let temp = tempfile::Builder::new()
        .prefix("fact-ux-comments-")
        .tempdir_in("/private/tmp")?;
    let home = temp.path().to_path_buf();
    let mut steps = Vec::new();
    steps.push(step(
        1,
        "create a local ledger",
        run_fact(fact_binary, &home, &["init", "ux-comments", "--no-pager"])?,
        UxCanonicalState {
            active_ledger: Some("ux-comments".into()),
            ..UxCanonicalState::default()
        },
    ));
    let created = run_fact(
        fact_binary,
        &home,
        &[
            "propose",
            "--message",
            "Commented policy v1",
            "--decision",
            "accept",
            "--json",
            "--no-pager",
        ],
    )?;
    let proposition_id = json_string(&created, "proposition_id");
    let first_revision = json_string(&created, "revision_id");
    let accepted_state = UxCanonicalState {
        active_ledger: Some("ux-comments".into()),
        proposition_id: proposition_id.clone(),
        reference: proposition_id.clone(),
        effective_revision_id: first_revision.clone(),
        latest_revision_id: first_revision.clone(),
        status: Some("accepted".into()),
        latest_revision_status: Some("accepted".into()),
        summary: Some("Commented policy v1".into()),
        pending_count: 0,
        ..UxCanonicalState::default()
    };
    steps.push(step(
        2,
        "create accepted fact",
        created,
        accepted_state.clone(),
    ));
    if let Some(reference) = &proposition_id {
        let comment = run_fact(
            fact_binary,
            &home,
            &[
                "comment",
                reference,
                "--message",
                "This comment belongs to the proposition discussion.",
                "--json",
                "--no-pager",
            ],
        )?;
        steps.push(step(
            3,
            "comment on proposition",
            comment,
            accepted_state.clone(),
        ));
        if let Some(revision) = &first_revision {
            let revision_comment = run_fact(
                fact_binary,
                &home,
                &[
                    "comment",
                    revision,
                    "--message",
                    "This comment belongs to the first revision.",
                    "--json",
                    "--no-pager",
                ],
            )?;
            steps.push(step(
                4,
                "comment on revision",
                revision_comment,
                accepted_state.clone(),
            ));
        }
        let revised = run_fact(
            fact_binary,
            &home,
            &[
                "revise",
                reference,
                "--message",
                "Commented policy v2",
                "--json",
                "--no-pager",
            ],
        )?;
        let pending_revision = json_string(&revised, "revision_id");
        let pending_state = UxCanonicalState {
            latest_revision_id: pending_revision,
            latest_revision_status: Some("pending".into()),
            pending_count: 1,
            ..accepted_state.clone()
        };
        steps.push(step(
            5,
            "revise after comments",
            revised,
            pending_state.clone(),
        ));
        let show = run_fact(
            fact_binary,
            &home,
            &[
                "show",
                reference,
                "--comments",
                "0",
                "--history",
                "--json",
                "--no-pager",
            ],
        )?;
        steps.push(step(
            6,
            "inspect comments and revision context",
            show,
            pending_state.clone(),
        ));
        return finalize_scenario(
            "comments-and-revision-context",
            seed,
            fixture,
            steps,
            pending_state,
        );
    }
    finalize_scenario(
        "comments-and-revision-context",
        seed,
        fixture,
        steps,
        accepted_state,
    )
}

fn run_invitation_and_participation(
    seed: u64,
    fixture: Option<PathBuf>,
    fact_binary: &Path,
) -> Result<UxScenarioReport> {
    let temp = tempfile::Builder::new()
        .prefix("fact-ux-invitation-")
        .tempdir_in("/private/tmp")?;
    let home = temp.path().to_path_buf();
    let mut steps = vec![
        step(
            1,
            "create a local ledger",
            run_fact(
                fact_binary,
                &home,
                &["init", "ux-invitation", "--json", "--no-pager"],
            )?,
            UxCanonicalState {
                active_ledger: Some("ux-invitation".into()),
                ..UxCanonicalState::default()
            },
        ),
        step(
            2,
            "name the initial authority",
            run_fact(
                fact_binary,
                &home,
                &[
                    "as",
                    "Owner",
                    "--alias",
                    "owner",
                    "--self",
                    "--json",
                    "--no-pager",
                ],
            )?,
            UxCanonicalState {
                active_ledger: Some("ux-invitation".into()),
                ..UxCanonicalState::default()
            },
        ),
        step(
            3,
            "create invited participant identity",
            run_fact(
                fact_binary,
                &home,
                &[
                    "as",
                    "Bob",
                    "--alias",
                    "bob",
                    "--participate",
                    "--json",
                    "--no-pager",
                ],
            )?,
            UxCanonicalState {
                active_ledger: Some("ux-invitation".into()),
                ..UxCanonicalState::default()
            },
        ),
        step(
            4,
            "switch back to authority",
            run_fact(
                fact_binary,
                &home,
                &["as", "owner", "--no-create", "--json", "--no-pager"],
            )?,
            UxCanonicalState {
                active_ledger: Some("ux-invitation".into()),
                ..UxCanonicalState::default()
            },
        ),
        step(
            5,
            "create inviter identity",
            run_fact(
                fact_binary,
                &home,
                &[
                    "as",
                    "Alice",
                    "--alias",
                    "alice",
                    "--participate",
                    "--permission",
                    "invite",
                    "--json",
                    "--no-pager",
                ],
            )?,
            UxCanonicalState {
                active_ledger: Some("ux-invitation".into()),
                ..UxCanonicalState::default()
            },
        ),
    ];
    let bob = run_fact(
        fact_binary,
        &home,
        &["directory", "resolve", "bob", "--json", "--no-pager"],
    )?;
    let bob_id = json_string(&bob, "actor_id");
    steps.push(step(
        6,
        "resolve invited participant",
        bob,
        UxCanonicalState {
            active_ledger: Some("ux-invitation".into()),
            ..UxCanonicalState::default()
        },
    ));
    steps.push(step(
        7,
        "switch back to inviter",
        run_fact(
            fact_binary,
            &home,
            &["as", "alice", "--no-create", "--json", "--no-pager"],
        )?,
        UxCanonicalState {
            active_ledger: Some("ux-invitation".into()),
            ..UxCanonicalState::default()
        },
    ));
    let proposed = run_fact(
        fact_binary,
        &home,
        &[
            "propose",
            "--message",
            "Invitation policy",
            "--json",
            "--no-pager",
        ],
    )?;
    let proposition_id = json_string(&proposed, "proposition_id");
    let revision_id = json_string(&proposed, "revision_id");
    let pending_state = UxCanonicalState {
        active_ledger: Some("ux-invitation".into()),
        proposition_id: proposition_id.clone(),
        reference: proposition_id.clone(),
        effective_revision_id: revision_id.clone(),
        latest_revision_id: revision_id,
        status: Some("pending".into()),
        latest_revision_status: Some("pending".into()),
        summary: Some("Invitation policy".into()),
        pending_count: 1,
        ..UxCanonicalState::default()
    };
    steps.push(step(
        8,
        "create a proposition for participation",
        proposed,
        pending_state.clone(),
    ));
    let mut invitation_id = None;
    if let (Some(reference), Some(actor)) = (&proposition_id, &bob_id) {
        let invited = run_fact(
            fact_binary,
            &home,
            &["invite", reference, actor, "--json", "--no-pager"],
        )?;
        invitation_id = json_string(&invited, "invitation_id");
        steps.push(step(
            9,
            "invite another actor",
            invited,
            pending_state.clone(),
        ));
    }
    steps.push(step(
        10,
        "switch to invited participant",
        run_fact(
            fact_binary,
            &home,
            &["as", "bob", "--no-create", "--json", "--no-pager"],
        )?,
        pending_state.clone(),
    ));
    if let Some(reference) = &proposition_id {
        let join = if let Some(invitation_id) = &invitation_id {
            run_fact(
                fact_binary,
                &home,
                &[
                    "join",
                    reference,
                    "--invitation",
                    invitation_id,
                    "--json",
                    "--no-pager",
                ],
            )?
        } else {
            run_fact(
                fact_binary,
                &home,
                &["join", reference, "--json", "--no-pager"],
            )?
        };
        steps.push(step(
            11,
            "join invited discussion",
            join,
            pending_state.clone(),
        ));
        let pending = run_fact(fact_binary, &home, &["pending", "--json", "--no-pager"])?;
        steps.push(step(
            12,
            "inspect participant pending actions",
            pending,
            pending_state.clone(),
        ));
        let leave = run_fact(
            fact_binary,
            &home,
            &["leave", reference, "--json", "--no-pager"],
        )?;
        steps.push(step(
            13,
            "leave before deciding",
            leave,
            pending_state.clone(),
        ));
        return finalize_scenario(
            "invitation-and-participation",
            seed,
            fixture,
            steps,
            pending_state,
        );
    }
    finalize_scenario(
        "invitation-and-participation",
        seed,
        fixture,
        steps,
        pending_state,
    )
}

fn run_archive_and_withdrawal(
    seed: u64,
    fixture: Option<PathBuf>,
    fact_binary: &Path,
) -> Result<UxScenarioReport> {
    let temp = tempfile::Builder::new()
        .prefix("fact-ux-retirement-")
        .tempdir_in("/private/tmp")?;
    let home = temp.path().to_path_buf();
    let mut steps = Vec::new();
    steps.push(step(
        1,
        "create a local ledger",
        run_fact(fact_binary, &home, &["init", "ux-retirement", "--no-pager"])?,
        UxCanonicalState {
            active_ledger: Some("ux-retirement".into()),
            ..UxCanonicalState::default()
        },
    ));
    let archived_created = run_fact(
        fact_binary,
        &home,
        &[
            "propose",
            "--message",
            "Archive candidate policy",
            "--decision",
            "accept",
            "--json",
            "--no-pager",
        ],
    )?;
    let archived_id = json_string(&archived_created, "proposition_id");
    let archived_revision = json_string(&archived_created, "revision_id");
    let archived_state = UxCanonicalState {
        active_ledger: Some("ux-retirement".into()),
        proposition_id: archived_id.clone(),
        reference: archived_id.clone(),
        effective_revision_id: archived_revision.clone(),
        latest_revision_id: archived_revision,
        status: Some("accepted".into()),
        latest_revision_status: Some("accepted".into()),
        summary: Some("Archive candidate policy".into()),
        pending_count: 0,
        ..UxCanonicalState::default()
    };
    steps.push(step(
        2,
        "create archive candidate",
        archived_created,
        archived_state.clone(),
    ));
    let withdrawn_created = run_fact(
        fact_binary,
        &home,
        &[
            "propose",
            "--message",
            "Withdrawal candidate policy",
            "--decision",
            "accept",
            "--json",
            "--no-pager",
        ],
    )?;
    let withdrawn_id = json_string(&withdrawn_created, "proposition_id");
    let withdrawn_revision = json_string(&withdrawn_created, "revision_id");
    let withdrawn_state = UxCanonicalState {
        active_ledger: Some("ux-retirement".into()),
        proposition_id: withdrawn_id.clone(),
        reference: withdrawn_id.clone(),
        effective_revision_id: withdrawn_revision.clone(),
        latest_revision_id: withdrawn_revision,
        status: Some("accepted".into()),
        latest_revision_status: Some("accepted".into()),
        summary: Some("Withdrawal candidate policy".into()),
        pending_count: 0,
        ..UxCanonicalState::default()
    };
    steps.push(step(
        3,
        "create withdrawal candidate",
        withdrawn_created,
        withdrawn_state.clone(),
    ));
    if let Some(reference) = &archived_id {
        let archived = run_fact(
            fact_binary,
            &home,
            &[
                "archive",
                reference,
                "--reason",
                "superseded by another record",
                "--json",
                "--no-pager",
            ],
        )?;
        let archived_state = UxCanonicalState {
            archived: true,
            ..archived_state.clone()
        };
        steps.push(step(
            4,
            "archive a proposition",
            archived,
            archived_state.clone(),
        ));
        let list = run_fact(fact_binary, &home, &["list", "--json", "--no-pager"])?;
        steps.push(step(
            5,
            "verify archived proposition leaves default list",
            list,
            archived_state.clone(),
        ));
        let history = run_fact(
            fact_binary,
            &home,
            &["history", reference, "--json", "--no-pager"],
        )?;
        steps.push(step(
            6,
            "verify archived history remains available",
            history,
            archived_state,
        ));
    }
    if let Some(reference) = &withdrawn_id {
        let withdrawn = run_fact(
            fact_binary,
            &home,
            &[
                "withdraw",
                reference,
                "--reason",
                "not project policy",
                "--json",
                "--no-pager",
            ],
        )?;
        let withdrawn_state = UxCanonicalState {
            withdrawn: true,
            status: Some("withdrawn".into()),
            latest_revision_status: Some("accepted".into()),
            ..withdrawn_state.clone()
        };
        steps.push(step(
            7,
            "withdraw a proposition",
            withdrawn,
            withdrawn_state.clone(),
        ));
        let show = run_fact(
            fact_binary,
            &home,
            &["show", reference, "--all", "--json", "--no-pager"],
        )?;
        steps.push(step(
            8,
            "verify withdrawn history remains available",
            show,
            withdrawn_state.clone(),
        ));
        return finalize_scenario(
            "archive-and-withdrawal",
            seed,
            fixture,
            steps,
            withdrawn_state,
        );
    }
    finalize_scenario(
        "archive-and-withdrawal",
        seed,
        fixture,
        steps,
        UxCanonicalState::default(),
    )
}

fn run_multi_participant_acceptance(
    seed: u64,
    fixture: Option<PathBuf>,
    fact_binary: &Path,
) -> Result<UxScenarioReport> {
    let temp = tempfile::Builder::new()
        .prefix("fact-ux-multi-accept-")
        .tempdir_in("/private/tmp")?;
    let home = temp.path().to_path_buf();
    let mut steps = Vec::new();
    steps.push(step(
        1,
        "create a local ledger",
        run_fact(
            fact_binary,
            &home,
            &["init", "ux-multi-accept", "--json", "--no-pager"],
        )?,
        UxCanonicalState {
            active_ledger: Some("ux-multi-accept".into()),
            ..UxCanonicalState::default()
        },
    ));
    for (index, (args, intent)) in [
        (
            vec![
                "as",
                "Owner",
                "--alias",
                "owner",
                "--self",
                "--json",
                "--no-pager",
            ],
            "name the initial authority",
        ),
        (
            vec![
                "as",
                "Bob",
                "--alias",
                "bob",
                "--participate",
                "--json",
                "--no-pager",
            ],
            "create first participant identity",
        ),
        (
            vec!["as", "owner", "--no-create", "--json", "--no-pager"],
            "switch back to authority",
        ),
        (
            vec![
                "as",
                "Carol",
                "--alias",
                "carol",
                "--participate",
                "--json",
                "--no-pager",
            ],
            "create second participant identity",
        ),
        (
            vec!["as", "owner", "--no-create", "--json", "--no-pager"],
            "switch back to authority",
        ),
        (
            vec![
                "as",
                "Alice",
                "--alias",
                "alice",
                "--participate",
                "--permission",
                "invite",
                "--json",
                "--no-pager",
            ],
            "create proposer identity",
        ),
        (
            vec!["as", "alice", "--no-create", "--json", "--no-pager"],
            "switch to proposer",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        steps.push(step(
            index + 2,
            intent,
            run_fact(fact_binary, &home, &args)?,
            UxCanonicalState {
                active_ledger: Some("ux-multi-accept".into()),
                ..UxCanonicalState::default()
            },
        ));
    }
    let bob = run_fact(
        fact_binary,
        &home,
        &["directory", "resolve", "bob", "--json", "--no-pager"],
    )?;
    let bob_id = json_string(&bob, "actor_id");
    steps.push(step(
        9,
        "resolve first participant",
        bob,
        UxCanonicalState {
            active_ledger: Some("ux-multi-accept".into()),
            ..UxCanonicalState::default()
        },
    ));
    let carol = run_fact(
        fact_binary,
        &home,
        &["directory", "resolve", "carol", "--json", "--no-pager"],
    )?;
    let carol_id = json_string(&carol, "actor_id");
    steps.push(step(
        10,
        "resolve second participant",
        carol,
        UxCanonicalState {
            active_ledger: Some("ux-multi-accept".into()),
            ..UxCanonicalState::default()
        },
    ));
    let proposed = run_fact(
        fact_binary,
        &home,
        &[
            "propose",
            "--message",
            "Multi participant policy",
            "--json",
            "--no-pager",
        ],
    )?;
    let proposition_id = json_string(&proposed, "proposition_id");
    let revision_id = json_string(&proposed, "revision_id");
    let pending_state = UxCanonicalState {
        active_ledger: Some("ux-multi-accept".into()),
        proposition_id: proposition_id.clone(),
        reference: proposition_id.clone(),
        effective_revision_id: revision_id.clone(),
        latest_revision_id: revision_id.clone(),
        status: Some("pending".into()),
        latest_revision_status: Some("pending".into()),
        summary: Some("Multi participant policy".into()),
        pending_count: 1,
        ..UxCanonicalState::default()
    };
    steps.push(step(
        11,
        "create a proposition requiring participant decisions",
        proposed,
        pending_state.clone(),
    ));
    let mut bob_invitation = None;
    let mut carol_invitation = None;
    if let (Some(reference), Some(actor)) = (&proposition_id, &bob_id) {
        let invited = run_fact(
            fact_binary,
            &home,
            &["invite", reference, actor, "--json", "--no-pager"],
        )?;
        bob_invitation = json_string(&invited, "invitation_id");
        steps.push(step(
            12,
            "invite first participant",
            invited,
            pending_state.clone(),
        ));
    }
    if let (Some(reference), Some(actor)) = (&proposition_id, &carol_id) {
        let invited = run_fact(
            fact_binary,
            &home,
            &["invite", reference, actor, "--json", "--no-pager"],
        )?;
        carol_invitation = json_string(&invited, "invitation_id");
        steps.push(step(
            13,
            "invite second participant",
            invited,
            pending_state.clone(),
        ));
    }
    if let Some(reference) = &proposition_id {
        steps.push(step(
            14,
            "switch to first participant",
            run_fact(
                fact_binary,
                &home,
                &["as", "bob", "--no-create", "--json", "--no-pager"],
            )?,
            pending_state.clone(),
        ));
        let join = if let Some(invitation) = &bob_invitation {
            run_fact(
                fact_binary,
                &home,
                &[
                    "join",
                    reference,
                    "--invitation",
                    invitation,
                    "--json",
                    "--no-pager",
                ],
            )?
        } else {
            run_fact(
                fact_binary,
                &home,
                &["join", reference, "--json", "--no-pager"],
            )?
        };
        steps.push(step(
            15,
            "first participant joins",
            join,
            pending_state.clone(),
        ));
        steps.push(step(
            16,
            "switch to second participant",
            run_fact(
                fact_binary,
                &home,
                &["as", "carol", "--no-create", "--json", "--no-pager"],
            )?,
            pending_state.clone(),
        ));
        let join = if let Some(invitation) = &carol_invitation {
            run_fact(
                fact_binary,
                &home,
                &[
                    "join",
                    reference,
                    "--invitation",
                    invitation,
                    "--json",
                    "--no-pager",
                ],
            )?
        } else {
            run_fact(
                fact_binary,
                &home,
                &["join", reference, "--json", "--no-pager"],
            )?
        };
        steps.push(step(
            17,
            "second participant joins",
            join,
            pending_state.clone(),
        ));
        steps.push(step(
            18,
            "switch back to first participant",
            run_fact(
                fact_binary,
                &home,
                &["as", "bob", "--no-create", "--json", "--no-pager"],
            )?,
            pending_state.clone(),
        ));
        let bob_accept = run_fact(
            fact_binary,
            &home,
            &["accept", reference, "--json", "--no-pager"],
        )?;
        steps.push(step(
            19,
            "first participant accepts",
            bob_accept,
            pending_state.clone(),
        ));
        steps.push(step(
            20,
            "inspect pending state after first decision",
            run_fact(fact_binary, &home, &["pending", "--json", "--no-pager"])?,
            pending_state.clone(),
        ));
        steps.push(step(
            21,
            "switch back to second participant",
            run_fact(
                fact_binary,
                &home,
                &["as", "carol", "--no-create", "--json", "--no-pager"],
            )?,
            pending_state.clone(),
        ));
        let carol_accept = run_fact(
            fact_binary,
            &home,
            &["accept", reference, "--json", "--no-pager"],
        )?;
        steps.push(step(
            22,
            "second participant accepts",
            carol_accept,
            pending_state.clone(),
        ));
        steps.push(step(
            23,
            "inspect pending state after participant decisions",
            run_fact(fact_binary, &home, &["pending", "--json", "--no-pager"])?,
            pending_state.clone(),
        ));
        steps.push(step(
            24,
            "switch back to proposer",
            run_fact(
                fact_binary,
                &home,
                &["as", "alice", "--no-create", "--json", "--no-pager"],
            )?,
            pending_state.clone(),
        ));
        let accepted = run_fact(
            fact_binary,
            &home,
            &["accept", reference, "--json", "--no-pager"],
        )?;
        let accepted_revision = json_string(&accepted, "revision_id").or(revision_id);
        let accepted_state = UxCanonicalState {
            status: Some("accepted".into()),
            latest_revision_status: Some("accepted".into()),
            effective_revision_id: accepted_revision.clone(),
            latest_revision_id: accepted_revision,
            pending_count: 0,
            ..pending_state.clone()
        };
        steps.push(step(
            25,
            "proposer accepts and settles",
            accepted,
            accepted_state.clone(),
        ));
        steps.push(step(
            26,
            "list settled proposition",
            run_fact(fact_binary, &home, &["list", "--json", "--no-pager"])?,
            accepted_state.clone(),
        ));
        steps.push(step(
            27,
            "inspect decision history",
            run_fact(
                fact_binary,
                &home,
                &["history", reference, "--json", "--no-pager"],
            )?,
            accepted_state.clone(),
        ));
        return finalize_scenario(
            "multi-participant-acceptance",
            seed,
            fixture,
            steps,
            accepted_state,
        );
    }
    finalize_scenario(
        "multi-participant-acceptance",
        seed,
        fixture,
        steps,
        pending_state,
    )
}

fn run_ledger_switching(
    seed: u64,
    fixture: Option<PathBuf>,
    fact_binary: &Path,
) -> Result<UxScenarioReport> {
    let temp = tempfile::Builder::new()
        .prefix("fact-ux-ledgers-")
        .tempdir_in("/private/tmp")?;
    let home = temp.path().to_path_buf();
    let mut steps = Vec::new();
    steps.push(step(
        1,
        "create first ledger",
        run_fact(
            fact_binary,
            &home,
            &["init", "ux-ledger-a", "--json", "--no-pager"],
        )?,
        UxCanonicalState {
            active_ledger: Some("ux-ledger-a".into()),
            ..UxCanonicalState::default()
        },
    ));
    steps.push(step(
        2,
        "create second ledger without switching",
        run_fact(
            fact_binary,
            &home,
            &["new", "ux-ledger-b", "--json", "--no-pager"],
        )?,
        UxCanonicalState {
            active_ledger: Some("ux-ledger-a".into()),
            ..UxCanonicalState::default()
        },
    ));
    let created_a = run_fact(
        fact_binary,
        &home,
        &[
            "propose",
            "--message",
            "Ledger A policy",
            "--decision",
            "accept",
            "--json",
            "--no-pager",
        ],
    )?;
    let a_id = json_string(&created_a, "proposition_id");
    let a_revision = json_string(&created_a, "revision_id");
    let a_state = UxCanonicalState {
        active_ledger: Some("ux-ledger-a".into()),
        proposition_id: a_id.clone(),
        reference: a_id.clone(),
        effective_revision_id: a_revision.clone(),
        latest_revision_id: a_revision,
        status: Some("accepted".into()),
        latest_revision_status: Some("accepted".into()),
        summary: Some("Ledger A policy".into()),
        pending_count: 0,
        ..UxCanonicalState::default()
    };
    steps.push(step(
        3,
        "create content in first ledger",
        created_a,
        a_state.clone(),
    ));
    steps.push(step(
        4,
        "switch to second ledger",
        run_fact(
            fact_binary,
            &home,
            &["use", "ux-ledger-b", "--json", "--no-pager"],
        )?,
        UxCanonicalState {
            active_ledger: Some("ux-ledger-b".into()),
            ..UxCanonicalState::default()
        },
    ));
    steps.push(step(
        5,
        "inspect selected ledger",
        run_fact(fact_binary, &home, &["status", "--json", "--no-pager"])?,
        UxCanonicalState {
            active_ledger: Some("ux-ledger-b".into()),
            ..UxCanonicalState::default()
        },
    ));
    let created_b = run_fact(
        fact_binary,
        &home,
        &[
            "propose",
            "--message",
            "Ledger B policy",
            "--decision",
            "accept",
            "--json",
            "--no-pager",
        ],
    )?;
    let b_id = json_string(&created_b, "proposition_id");
    let b_revision = json_string(&created_b, "revision_id");
    let b_state = UxCanonicalState {
        active_ledger: Some("ux-ledger-b".into()),
        proposition_id: b_id.clone(),
        reference: b_id.clone(),
        effective_revision_id: b_revision.clone(),
        latest_revision_id: b_revision,
        status: Some("accepted".into()),
        latest_revision_status: Some("accepted".into()),
        summary: Some("Ledger B policy".into()),
        pending_count: 0,
        ..UxCanonicalState::default()
    };
    steps.push(step(
        6,
        "create content in second ledger",
        created_b,
        b_state.clone(),
    ));
    steps.push(step(
        7,
        "list selected ledger",
        run_fact(fact_binary, &home, &["list", "--json", "--no-pager"])?,
        b_state.clone(),
    ));
    steps.push(step(
        8,
        "list first ledger with explicit override",
        run_fact(
            fact_binary,
            &home,
            &["list", "--ledger", "ux-ledger-a", "--json", "--no-pager"],
        )?,
        a_state,
    ));
    finalize_scenario("ledger-switching", seed, fixture, steps, b_state)
}

fn run_ambiguous_target_workflow(
    seed: u64,
    fixture: Option<PathBuf>,
    fact_binary: &Path,
) -> Result<UxScenarioReport> {
    let temp = tempfile::Builder::new()
        .prefix("fact-ux-ambiguous-")
        .tempdir_in("/private/tmp")?;
    let home = temp.path().to_path_buf();
    let mut steps = Vec::new();
    steps.push(step(
        1,
        "create a local ledger",
        run_fact(fact_binary, &home, &["init", "ux-ambiguous", "--no-pager"])?,
        UxCanonicalState {
            active_ledger: Some("ux-ambiguous".into()),
            ..UxCanonicalState::default()
        },
    ));
    for index in 0..2 {
        let proposed = run_fact(
            fact_binary,
            &home,
            &[
                "propose",
                "--message",
                if index == 0 {
                    "Ambiguous pending policy A"
                } else {
                    "Ambiguous pending policy B"
                },
                "--json",
                "--no-pager",
            ],
        )?;
        steps.push(step(
            index + 2,
            "create another pending proposition",
            proposed,
            UxCanonicalState {
                active_ledger: Some("ux-ambiguous".into()),
                pending_count: index + 1,
                ..UxCanonicalState::default()
            },
        ));
    }
    let omitted_accept = run_fact(fact_binary, &home, &["accept", "--json", "--no-pager"])?;
    steps.push(step(
        4,
        "try accepting without a reference when multiple targets are pending",
        omitted_accept,
        UxCanonicalState {
            active_ledger: Some("ux-ambiguous".into()),
            pending_count: 2,
            ..UxCanonicalState::default()
        },
    ));
    let pending = run_fact(fact_binary, &home, &["pending", "--json", "--no-pager"])?;
    steps.push(step(
        5,
        "inspect available references after ambiguity",
        pending,
        UxCanonicalState {
            active_ledger: Some("ux-ambiguous".into()),
            pending_count: 2,
            ..UxCanonicalState::default()
        },
    ));
    finalize_scenario(
        "ambiguous-target-workflow",
        seed,
        fixture,
        steps,
        UxCanonicalState {
            active_ledger: Some("ux-ambiguous".into()),
            pending_count: 2,
            ..UxCanonicalState::default()
        },
    )
}

fn run_remote_workflow(
    seed: u64,
    fixture: Option<PathBuf>,
    fact_binary: &Path,
) -> Result<UxScenarioReport> {
    let temp = tempfile::Builder::new()
        .prefix("fact-ux-remote-")
        .tempdir_in("/private/tmp")?;
    let home = temp.path().to_path_buf();
    let bundle = temp.path().join("remote.bundle");
    let imported_database = temp.path().join("imported.sqlite");
    let mut steps = Vec::new();
    steps.push(step(
        1,
        "create a local ledger",
        run_fact(
            fact_binary,
            &home,
            &["init", "ux-remote", "--json", "--no-pager"],
        )?,
        UxCanonicalState {
            active_ledger: Some("ux-remote".into()),
            ..UxCanonicalState::default()
        },
    ));
    let created = run_fact(
        fact_binary,
        &home,
        &[
            "propose",
            "--message",
            "Remote sync policy",
            "--decision",
            "accept",
            "--json",
            "--no-pager",
        ],
    )?;
    let proposition_id = json_string(&created, "proposition_id");
    let revision_id = json_string(&created, "revision_id");
    let state = UxCanonicalState {
        active_ledger: Some("ux-remote".into()),
        proposition_id: proposition_id.clone(),
        reference: proposition_id.clone(),
        effective_revision_id: revision_id.clone(),
        latest_revision_id: revision_id.clone(),
        status: Some("accepted".into()),
        latest_revision_status: Some("accepted".into()),
        summary: Some("Remote sync policy".into()),
        pending_count: 0,
        ..UxCanonicalState::default()
    };
    steps.push(step(2, "create syncable content", created, state.clone()));
    let status = run_fact(fact_binary, &home, &["status", "--json", "--no-pager"])?;
    let database = json_string(&status, "database");
    let ledger = json_string(&status, "ledger_id");
    steps.push(step(3, "inspect local sync context", status, state.clone()));
    steps.push(step(
        4,
        "configure a named remote",
        run_fact(
            fact_binary,
            &home,
            &[
                "remote",
                "add",
                "origin",
                "http://127.0.0.1:9/facts",
                "--json",
                "--no-pager",
            ],
        )?,
        state.clone(),
    ));
    steps.push(step(
        5,
        "list configured remotes",
        run_fact(
            fact_binary,
            &home,
            &["remote", "list", "--json", "--no-pager"],
        )?,
        state.clone(),
    ));
    if let (Some(database), Some(ledger)) = (&database, &ledger) {
        let bundle_arg = bundle.to_string_lossy().to_string();
        let imported_arg = imported_database.to_string_lossy().to_string();
        let exported = run_fact(
            fact_binary,
            &home,
            &[
                "pull",
                database,
                ledger,
                bundle_arg.as_str(),
                "--json",
                "--no-pager",
            ],
        )?;
        steps.push(step(
            6,
            "pull local ledger state into an exchange bundle",
            exported,
            state.clone(),
        ));
        let imported = run_fact(
            fact_binary,
            &home,
            &[
                "push",
                imported_arg.as_str(),
                bundle_arg.as_str(),
                "--json",
                "--no-pager",
            ],
        )?;
        steps.push(step(
            7,
            "push the exchange bundle into another store",
            imported,
            state.clone(),
        ));
        steps.push(step(
            8,
            "register imported ledger",
            run_fact(
                fact_binary,
                &home,
                &[
                    "from",
                    imported_arg.as_str(),
                    "ux-remote-imported",
                    "--ledger",
                    ledger,
                    "--json",
                    "--no-pager",
                ],
            )?,
            UxCanonicalState {
                active_ledger: Some("ux-remote-imported".into()),
                ..state.clone()
            },
        ));
        steps.push(step(
            9,
            "switch to imported ledger",
            run_fact(
                fact_binary,
                &home,
                &["use", "ux-remote-imported", "--json", "--no-pager"],
            )?,
            UxCanonicalState {
                active_ledger: Some("ux-remote-imported".into()),
                ..state.clone()
            },
        ));
        let imported_state = UxCanonicalState {
            active_ledger: Some("ux-remote-imported".into()),
            ..state.clone()
        };
        steps.push(step(
            10,
            "list imported ledger content",
            run_fact(fact_binary, &home, &["list", "--json", "--no-pager"])?,
            imported_state.clone(),
        ));
        if let Some(reference) = &proposition_id {
            steps.push(step(
                11,
                "inspect imported history",
                run_fact(
                    fact_binary,
                    &home,
                    &["history", reference, "--json", "--no-pager"],
                )?,
                imported_state.clone(),
            ));
        }
        return finalize_scenario("remote-workflow", seed, fixture, steps, imported_state);
    }
    finalize_scenario("remote-workflow", seed, fixture, steps, state)
}

fn run_failure_and_recovery(
    seed: u64,
    fixture: Option<PathBuf>,
    fact_binary: &Path,
) -> Result<UxScenarioReport> {
    let temp = tempfile::Builder::new()
        .prefix("fact-ux-failure-")
        .tempdir_in("/private/tmp")?;
    let home = temp.path().to_path_buf();
    let bad_bundle = temp.path().join("not-a-bundle.factbndl");
    fs::write(&bad_bundle, b"not a Fact bundle\n")?;
    let mut steps = Vec::new();
    steps.push(step(
        1,
        "create a local ledger",
        run_fact(
            fact_binary,
            &home,
            &["init", "ux-failure", "--json", "--no-pager"],
        )?,
        UxCanonicalState {
            active_ledger: Some("ux-failure".into()),
            ..UxCanonicalState::default()
        },
    ));
    let created = run_fact(
        fact_binary,
        &home,
        &[
            "propose",
            "--message",
            "Failure recovery policy",
            "--decision",
            "accept",
            "--json",
            "--no-pager",
        ],
    )?;
    let proposition_id = json_string(&created, "proposition_id");
    let revision_id = json_string(&created, "revision_id");
    let state = UxCanonicalState {
        active_ledger: Some("ux-failure".into()),
        proposition_id: proposition_id.clone(),
        reference: proposition_id.clone(),
        effective_revision_id: revision_id.clone(),
        latest_revision_id: revision_id,
        status: Some("accepted".into()),
        latest_revision_status: Some("accepted".into()),
        summary: Some("Failure recovery policy".into()),
        pending_count: 0,
        ..UxCanonicalState::default()
    };
    steps.push(step(
        2,
        "create durable state before failures",
        created,
        state.clone(),
    ));
    steps.push(step(
        3,
        "try pushing without a configured remote",
        run_fact(fact_binary, &home, &["push", "--json", "--no-pager"])?,
        state.clone(),
    ));
    steps.push(step(
        4,
        "configure an unavailable remote",
        run_fact(
            fact_binary,
            &home,
            &[
                "remote",
                "add",
                "offline",
                "http://127.0.0.1:9/facts",
                "--json",
                "--no-pager",
            ],
        )?,
        state.clone(),
    ));
    steps.push(step(
        5,
        "try pulling from unavailable remote",
        run_fact(
            fact_binary,
            &home,
            &["pull", "--remote", "offline", "--json", "--no-pager"],
        )?,
        state.clone(),
    ));
    let status = run_fact(fact_binary, &home, &["status", "--json", "--no-pager"])?;
    let database = json_string(&status, "database");
    steps.push(step(
        6,
        "verify local state after remote failure",
        status,
        state.clone(),
    ));
    if let Some(database) = database {
        let bad_bundle = bad_bundle.to_string_lossy().to_string();
        steps.push(step(
            7,
            "retry an invalid interrupted bundle",
            run_fact(
                fact_binary,
                &home,
                &[
                    "sync",
                    "retry",
                    database.as_str(),
                    bad_bundle.as_str(),
                    "--json",
                    "--no-pager",
                ],
            )?,
            state.clone(),
        ));
    }
    if let Some(reference) = &proposition_id {
        steps.push(step(
            8,
            "inspect history after failed operations",
            run_fact(
                fact_binary,
                &home,
                &["history", reference, "--json", "--no-pager"],
            )?,
            state.clone(),
        ));
    }
    finalize_scenario("failure-and-recovery", seed, fixture, steps, state)
}

fn run_contested_proposition(
    seed: u64,
    fixture: Option<PathBuf>,
    fact_binary: &Path,
) -> Result<UxScenarioReport> {
    let temp = tempfile::Builder::new()
        .prefix("fact-ux-contested-")
        .tempdir_in("/private/tmp")?;
    let home = temp.path().to_path_buf();
    let mut steps = Vec::new();
    steps.push(step(
        1,
        "create a local ledger",
        run_fact(
            fact_binary,
            &home,
            &["init", "ux-contested", "--json", "--no-pager"],
        )?,
        UxCanonicalState {
            active_ledger: Some("ux-contested".into()),
            ..UxCanonicalState::default()
        },
    ));
    let created = run_fact(
        fact_binary,
        &home,
        &[
            "propose",
            "--message",
            "Contested base policy",
            "--decision",
            "accept",
            "--json",
            "--no-pager",
        ],
    )?;
    let proposition_id = json_string(&created, "proposition_id");
    let revision_id = json_string(&created, "revision_id");
    let accepted_state = UxCanonicalState {
        active_ledger: Some("ux-contested".into()),
        proposition_id: proposition_id.clone(),
        reference: proposition_id.clone(),
        effective_revision_id: revision_id.clone(),
        latest_revision_id: revision_id.clone(),
        status: Some("accepted".into()),
        latest_revision_status: Some("accepted".into()),
        summary: Some("Contested base policy".into()),
        pending_count: 0,
        ..UxCanonicalState::default()
    };
    steps.push(step(
        2,
        "create accepted base proposition",
        created,
        accepted_state.clone(),
    ));
    if let Some(reference) = &proposition_id {
        let revised = run_fact(
            fact_binary,
            &home,
            &[
                "revise",
                reference,
                "--message",
                "Contested branch candidate",
                "--json",
                "--no-pager",
            ],
        )?;
        let pending_revision = json_string(&revised, "revision_id");
        let contested_state = UxCanonicalState {
            latest_revision_id: pending_revision,
            latest_revision_status: Some("pending".into()),
            pending_count: 1,
            contested: true,
            ..accepted_state.clone()
        };
        steps.push(step(
            3,
            "create unresolved branch candidate",
            revised,
            contested_state.clone(),
        ));
        let status = run_fact(fact_binary, &home, &["status", "--json", "--no-pager"])?;
        let database = json_string(&status, "database");
        let ledger = json_string(&status, "ledger_id");
        let actor = json_string(&status, "actor_id");
        let key_id = json_string(&status, "key_id");
        steps.push(step(
            4,
            "inspect local context before creating parallel branch",
            status,
            contested_state.clone(),
        ));
        if let (Some(database), Some(ledger), Some(actor), Some(key_id), Some(parent_revision)) =
            (&database, &ledger, &actor, &key_id, &revision_id)
        {
            insert_parallel_revision_tip(ParallelRevisionTip {
                database: Path::new(database),
                fact_home: &home,
                ledger,
                actor,
                key_id,
                proposition_id: reference,
                parent_revision_id: parent_revision,
                markdown: b"# Contested branch\n\nA parallel branch conflicts with the pending candidate.\n",
            })?;
        }
        let contested_state = UxCanonicalState {
            status: Some("contested".into()),
            latest_revision_status: Some("contested".into()),
            pending_count: 0,
            contested: true,
            ..accepted_state.clone()
        };
        steps.push(step(
            5,
            "inspect conflict command output",
            run_fact(
                fact_binary,
                &home,
                &["conflicts", reference, "--json", "--no-pager"],
            )?,
            contested_state.clone(),
        ));
        steps.push(step(
            6,
            "inspect proposition with conflict section",
            run_fact(
                fact_binary,
                &home,
                &["show", reference, "--conflicts", "--json", "--no-pager"],
            )?,
            contested_state.clone(),
        ));
        steps.push(step(
            7,
            "inspect revision history during contested review",
            run_fact(
                fact_binary,
                &home,
                &["revisions", reference, "--json", "--no-pager"],
            )?,
            contested_state.clone(),
        ));
        steps.push(step(
            8,
            "inspect immutable history during contested review",
            run_fact(
                fact_binary,
                &home,
                &["history", reference, "--json", "--no-pager"],
            )?,
            contested_state.clone(),
        ));
        return finalize_scenario(
            "contested-proposition",
            seed,
            fixture,
            steps,
            contested_state,
        );
    }
    finalize_scenario(
        "contested-proposition",
        seed,
        fixture,
        steps,
        accepted_state,
    )
}

fn run_help_discoverability(
    seed: u64,
    fixture: Option<PathBuf>,
    fact_binary: &Path,
) -> Result<UxScenarioReport> {
    let temp = tempfile::Builder::new()
        .prefix("fact-ux-help-")
        .tempdir_in("/private/tmp")?;
    let home = temp.path().to_path_buf();
    let mut steps = Vec::new();
    for (index, args) in [
        vec!["help"],
        vec!["help", "--all"],
        vec!["help", "propose"],
        vec!["help", "accept"],
        vec!["help", "reject"],
    ]
    .into_iter()
    .enumerate()
    {
        steps.push(step(
            index + 1,
            "inspect help",
            run_fact(fact_binary, &home, &args)?,
            UxCanonicalState::default(),
        ));
    }
    finalize_scenario(
        "help-discoverability",
        seed,
        fixture,
        steps,
        UxCanonicalState::default(),
    )
}

fn finalize_scenario(
    scenario: &str,
    seed: u64,
    fixture: Option<PathBuf>,
    mut steps: Vec<UxStepReport>,
    canonical_state: UxCanonicalState,
) -> Result<UxScenarioReport> {
    let mut defects = Vec::new();
    for step in &mut steps {
        step.assertion = assert_step_belief(step, &step.assertion.canonical_state);
        if !step.assertion.passed {
            for classification in &step.assertion.classifications {
                defects.push(UxDefect {
                    scenario: scenario.to_string(),
                    step: step.step,
                    command: step.receipt.command_line.clone(),
                    fixture: fixture.clone(),
                    seed,
                    expected_belief: step.assertion.expected_belief.clone(),
                    actual_output: combined_output(&step.receipt),
                    canonical_state: step.assertion.canonical_state.clone(),
                    classification: *classification,
                    suggested_resolution: suggested_resolution(*classification),
                });
            }
        }
    }
    Ok(UxScenarioReport {
        scenario: scenario.to_string(),
        seed,
        fixture,
        steps,
        canonical_state,
        passed: defects.is_empty(),
        defects,
    })
}

fn gap_scenario(scenario: &str, seed: u64, fixture: Option<PathBuf>) -> UxScenarioReport {
    let canonical_state = UxCanonicalState::default();
    let defect = UxDefect {
        scenario: scenario.to_string(),
        step: 0,
        command: scenario.to_string(),
        fixture: fixture.clone(),
        seed,
        expected_belief: UxBelief::default(),
        actual_output: "journey is not implemented".to_string(),
        canonical_state: canonical_state.clone(),
        classification: UxDefectCategory::MissingInspectionPath,
        suggested_resolution: "add a deterministic CLI journey for this required UX family".into(),
    };
    UxScenarioReport {
        scenario: scenario.to_string(),
        seed,
        fixture,
        steps: Vec::new(),
        canonical_state,
        defects: vec![defect],
        passed: false,
    }
}

fn step(
    step: usize,
    intent: &str,
    receipt: UxCommandReceipt,
    canonical_state: UxCanonicalState,
) -> UxStepReport {
    let parsed_semantics = parse_semantics(&receipt);
    let belief = belief_from_semantics(&receipt, &parsed_semantics);
    let next_action_clear = next_action_clear(&receipt, &parsed_semantics);
    let cognitive_load = cognitive_load(&receipt);
    let assertion = UxAssertion {
        passed: true,
        expected_belief: belief.clone(),
        actual_belief: belief.clone(),
        canonical_state,
        classifications: Vec::new(),
    };
    UxStepReport {
        step,
        intent: intent.to_string(),
        command: command_words(&receipt.command_line),
        receipt,
        parsed_semantics,
        belief,
        assertion,
        next_action_clear,
        cognitive_load,
    }
}

fn run_fact(fact_binary: &Path, fact_home: &Path, args: &[&str]) -> Result<UxCommandReceipt> {
    let mut environment = BTreeMap::new();
    environment.insert("FACT_HOME".to_string(), fact_home.display().to_string());
    environment.insert("NO_COLOR".to_string(), "1".to_string());
    environment.insert("TERM".to_string(), "dumb".to_string());
    let started = Instant::now();
    let output = Command::new(fact_binary)
        .args(args)
        .env("FACT_HOME", fact_home)
        .env("NO_COLOR", "1")
        .env("TERM", "dumb")
        .env("PAGER", "cat")
        .output()
        .with_context(|| format!("failed to run `{}`", fact_binary.display()))?;
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let json_output = serde_json::from_str(&stdout).ok();
    let protocol_references = if json_output.is_some() {
        Vec::new()
    } else {
        protocol_references(&stdout, &stderr)
    };
    Ok(UxCommandReceipt {
        command_line: format!("fact {}", args.join(" ")),
        environment,
        exit_status: output.status.code().unwrap_or(-1),
        stdout,
        stderr,
        json_output,
        duration_ms: started.elapsed().as_millis(),
        active_ledger: None,
        active_actor: None,
        protocol_references,
    })
}

fn parse_semantics(receipt: &UxCommandReceipt) -> UxParsedSemantics {
    let command = receipt
        .command_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("unknown");
    let mut semantics = UxParsedSemantics {
        command_kind: command.to_string(),
        state_changed: Some(matches!(
            command,
            "init"
                | "new"
                | "use"
                | "propose"
                | "revise"
                | "accept"
                | "reject"
                | "comment"
                | "invite"
                | "join"
                | "leave"
                | "archive"
                | "withdraw"
                | "push"
                | "pull"
        )),
        ..UxParsedSemantics::default()
    };
    if receipt.exit_status != 0 {
        semantics.error_category = Some("cli-error".into());
        semantics.retryable = Some(false);
    }
    if let Some(json) = &receipt.json_output {
        if let Some(array) = json.as_array() {
            if let Some(first) = array.first() {
                fill_semantics_from_json(&mut semantics, first);
            }
        } else {
            fill_semantics_from_json(&mut semantics, json);
        }
    }
    if semantics.summary.is_none() {
        semantics.summary = output_summary(&receipt.stdout);
    }
    if semantics.status.is_none() && receipt.json_output.is_none() {
        semantics.status = status_from_output(&receipt.stdout);
    }
    if semantics.proposition_reference.is_none() && receipt.json_output.is_none() {
        semantics.proposition_reference = receipt.protocol_references.first().cloned();
    }
    if semantics.next_action.is_none() {
        semantics.next_action = next_action_from_output(&receipt.stdout, &receipt.stderr);
    }
    if semantics.pending.is_none() {
        semantics.pending = Some(
            semantics.status.as_deref() == Some("pending")
                || semantics.latest_revision_status.as_deref() == Some("pending")
                || (receipt.json_output.is_none()
                    && combined_output(receipt).to_lowercase().contains("pending")),
        );
    }
    semantics
}

fn fill_semantics_from_json(semantics: &mut UxParsedSemantics, json: &serde_json::Value) {
    let json = json.get("proposition").unwrap_or(json);
    semantics.proposition_id = json_string_value(json, "proposition_id");
    semantics.proposition_reference =
        json_string_value(json, "reference").or_else(|| semantics.proposition_id.clone());
    semantics.revision_id = json_string_value(json, "revision_id")
        .or_else(|| json_string_value(json, "latest_revision_id"));
    semantics.status = json_string_value(json, "status");
    semantics.effective_status = json_string_value(json, "effective_status");
    semantics.latest_revision_status = json_string_value(json, "latest_revision_status");
    semantics.summary = json_string_value(json, "summary");
    if let Some(pending) = json["has_pending_revision"].as_bool() {
        semantics.pending = Some(pending);
    } else if let Some(count) = json["pending_participant_count"].as_u64() {
        semantics.pending = Some(count > 0);
    } else if let Some(count) = json["pending_actions"].as_u64() {
        semantics.pending = Some(count > 0);
    }
}

fn belief_from_semantics(receipt: &UxCommandReceipt, semantics: &UxParsedSemantics) -> UxBelief {
    let pending_action = if semantics.pending == Some(true) {
        Some("decision-required".to_string())
    } else {
        None
    };
    UxBelief {
        proposition: semantics
            .proposition_reference
            .clone()
            .or_else(|| semantics.proposition_id.clone()),
        status: semantics
            .effective_status
            .clone()
            .or_else(|| semantics.status.clone()),
        effective_revision: semantics.revision_id.clone(),
        latest_revision: semantics.revision_id.clone(),
        pending_action,
        source_command: receipt.command_line.clone(),
        supported: receipt.exit_status == 0,
    }
}

fn assert_step_belief(step: &UxStepReport, canonical: &UxCanonicalState) -> UxAssertion {
    let mut classifications = Vec::new();
    let expected = UxBelief {
        proposition: canonical.reference.clone(),
        status: canonical.status.clone(),
        effective_revision: canonical.effective_revision_id.clone(),
        latest_revision: canonical.latest_revision_id.clone(),
        pending_action: (canonical.pending_count > 0).then(|| "decision-required".into()),
        source_command: step.receipt.command_line.clone(),
        supported: step.receipt.exit_status == 0,
    };
    let actual = step.belief.clone();
    if step.receipt.exit_status != 0 {
        classifications.push(UxDefectCategory::MisleadingError);
    }
    if canonical.status.is_some()
        && actual.status.is_some()
        && actual.status != canonical.status
        && command_answers_status(&step.parsed_semantics.command_kind)
    {
        classifications.push(UxDefectCategory::EffectiveStateMismatch);
    }
    if (canonical.pending_count > 0) != actual.pending_action.is_some() {
        classifications.push(UxDefectCategory::PendingStateMismatch);
    }
    if is_mutation_command(&step.parsed_semantics.command_kind) && !step.next_action_clear {
        classifications.push(UxDefectCategory::MissingNextAction);
    }
    if step.cognitive_load.excessive {
        classifications.push(UxDefectCategory::TerminologyOverload);
    }
    classifications.sort();
    classifications.dedup();
    UxAssertion {
        passed: classifications.is_empty(),
        expected_belief: expected,
        actual_belief: actual,
        canonical_state: canonical.clone(),
        classifications,
    }
}

fn command_coverage(fact_binary: &Path) -> Result<UxCommandCoverageReport> {
    let help = Command::new(fact_binary)
        .arg("--help")
        .env("NO_COLOR", "1")
        .env("TERM", "dumb")
        .output()
        .with_context(|| format!("failed to inspect `{}` help", fact_binary.display()))?;
    let help_text = String::from_utf8_lossy(&help.stdout);
    let mut observed = Vec::new();
    let mut unsupported = Vec::new();
    for command in required_commands() {
        if help_text.contains(&format!("  {command}"))
            || Command::new(fact_binary)
                .args(["help", command])
                .env("NO_COLOR", "1")
                .env("TERM", "dumb")
                .output()
                .is_ok_and(|output| output.status.success())
        {
            observed.push(command.to_string());
        } else {
            unsupported.push(command.to_string());
        }
    }
    let observed_set = observed.iter().cloned().collect::<BTreeSet<_>>();
    let missing = required_commands()
        .into_iter()
        .filter(|command| !observed_set.contains(*command))
        .map(str::to_string)
        .collect();
    Ok(UxCommandCoverageReport {
        required_commands: required_commands()
            .into_iter()
            .map(str::to_string)
            .collect(),
        observed_commands: observed,
        missing_commands: missing,
        unsupported_commands: unsupported,
    })
}

fn user_belief_report(scenarios: &[UxScenarioReport]) -> UxUserBeliefReport {
    let beliefs = scenarios
        .iter()
        .flat_map(|scenario| scenario.steps.iter().map(|step| step.belief.clone()))
        .collect::<Vec<_>>();
    let unsupported_beliefs = beliefs
        .iter()
        .filter(|belief| !belief.supported)
        .cloned()
        .collect::<Vec<_>>();
    UxUserBeliefReport {
        beliefs,
        passed: unsupported_beliefs.is_empty(),
        unsupported_beliefs,
    }
}

fn consistency_report(scenarios: &[UxScenarioReport]) -> UxConsistencyReport {
    let mut contradictions = Vec::new();
    let mut checked = Vec::new();
    for scenario in scenarios {
        let statuses = scenario
            .steps
            .iter()
            .filter(|step| {
                matches!(
                    step.parsed_semantics.command_kind.as_str(),
                    "list" | "echo" | "search" | "revisions" | "pending"
                )
            })
            .filter_map(|step| step.belief.status.clone())
            .collect::<BTreeSet<_>>();
        if !statuses.is_empty() {
            checked.push(format!("{}:effective-status", scenario.scenario));
        }
        if statuses.len() > 1 && scenario.canonical_state.pending_count == 0 {
            contradictions.push(format!(
                "{} reports conflicting statuses: {:?}",
                scenario.scenario, statuses
            ));
        }
    }
    UxConsistencyReport {
        passed: contradictions.is_empty(),
        checked_questions: checked,
        contradictions,
    }
}

fn human_json_report(scenarios: &[UxScenarioReport]) -> UxHumanJsonReport {
    let mut mismatches = Vec::new();
    let mut pairs_checked = 0;
    for scenario in scenarios {
        let mut by_kind: BTreeMap<&str, Vec<&UxStepReport>> = BTreeMap::new();
        for step in &scenario.steps {
            by_kind
                .entry(step.parsed_semantics.command_kind.as_str())
                .or_default()
                .push(step);
        }
        for (kind, steps) in by_kind {
            let human = steps.iter().find(|step| step.receipt.json_output.is_none());
            let json = steps.iter().find(|step| step.receipt.json_output.is_some());
            if let (Some(human), Some(json)) = (human, json) {
                pairs_checked += 1;
                if human.belief.status != json.belief.status {
                    mismatches.push(format!(
                        "{}:{kind}: human status {:?} != json status {:?}",
                        scenario.scenario, human.belief.status, json.belief.status
                    ));
                }
            }
        }
    }
    UxHumanJsonReport {
        pairs_checked,
        passed: mismatches.is_empty(),
        mismatches,
    }
}

fn help_terminology_report(fact_binary: &Path) -> Result<UxHelpTerminologyReport> {
    let commands = [
        "help", "propose", "accept", "reject", "list", "pending", "search", "show",
    ];
    let jargon = [
        "Lifecycle object",
        "Settlement envelope",
        "Projection",
        "Dependency closure",
        "Canonical object",
        "Authorization grant",
        "Merkle commitment",
        "Deliberation tip",
    ];
    let mut checked = Vec::new();
    let mut hits = Vec::new();
    let mut missing = Vec::new();
    for command in commands {
        let args = if command == "help" {
            vec!["help"]
        } else {
            vec!["help", command]
        };
        let output = Command::new(fact_binary)
            .args(args)
            .env("NO_COLOR", "1")
            .env("TERM", "dumb")
            .output()
            .with_context(|| format!("failed to inspect help for `{command}`"))?;
        if !output.status.success() {
            missing.push(command.to_string());
            continue;
        }
        checked.push(command.to_string());
        let text = String::from_utf8_lossy(&output.stdout).to_lowercase();
        for term in jargon {
            if text.contains(&term.to_lowercase()) {
                hits.push(UxTerminologyHit {
                    command: command.to_string(),
                    term: term.to_string(),
                });
            }
        }
    }
    Ok(UxHelpTerminologyReport {
        passed: hits.is_empty() && missing.is_empty(),
        checked_commands: checked,
        jargon_hits: hits,
        missing_help_commands: missing,
    })
}

fn ambiguity_report(scenarios: &[UxScenarioReport]) -> UxAmbiguityReport {
    let mut checked = Vec::new();
    let mut unsafe_guesses = Vec::new();
    for scenario in scenarios
        .iter()
        .filter(|scenario| scenario.scenario.contains("ambiguous"))
    {
        checked.push(scenario.scenario.clone());
        for step in &scenario.steps {
            if step.intent.contains("without a reference")
                && step.receipt.exit_status == 0
                && matches!(
                    step.parsed_semantics.command_kind.as_str(),
                    "accept" | "reject" | "archive" | "withdraw"
                )
            {
                unsafe_guesses.push(step.receipt.command_line.clone());
            }
        }
    }
    UxAmbiguityReport {
        checked,
        passed: unsafe_guesses.is_empty(),
        unsafe_guesses,
    }
}

fn error_quality_report(scenarios: &[UxScenarioReport]) -> UxErrorQualityReport {
    let mut checked_errors = Vec::new();
    let mut defects = Vec::new();
    for scenario in scenarios {
        for step in &scenario.steps {
            if step.receipt.exit_status != 0 {
                checked_errors.push(step.receipt.command_line.clone());
                let error = step.receipt.stderr.to_lowercase();
                if !(error.contains("error")
                    && (error.contains("try") || error.contains("run") || error.contains("set")))
                {
                    defects.push(step.receipt.command_line.clone());
                }
            }
        }
    }
    UxErrorQualityReport {
        passed: defects.is_empty(),
        checked_errors,
        defects,
    }
}

fn missing_inspection_path_report(scenarios: &[UxScenarioReport]) -> UxMissingInspectionPathReport {
    let mut mutations = Vec::new();
    let mut missing = Vec::new();
    for scenario in scenarios {
        let inspected = scenario.steps.iter().any(|step| {
            matches!(
                step.parsed_semantics.command_kind.as_str(),
                "list" | "show" | "echo" | "pending" | "revisions" | "history" | "search"
            )
        });
        for step in &scenario.steps {
            if is_mutation_command(&step.parsed_semantics.command_kind) {
                mutations.push(format!(
                    "{}:{}",
                    scenario.scenario, step.parsed_semantics.command_kind
                ));
                if !inspected {
                    missing.push(format!(
                        "{}:{}",
                        scenario.scenario, step.parsed_semantics.command_kind
                    ));
                }
            }
        }
    }
    UxMissingInspectionPathReport {
        passed: missing.is_empty(),
        mutations_checked: mutations,
        missing_paths: missing,
    }
}

fn command_symmetry_report(fact_binary: &Path) -> Result<UxCommandSymmetryReport> {
    let pairs = [
        ("accept", "reject"),
        ("join", "leave"),
        ("push", "pull"),
        ("archive", "withdraw"),
    ];
    let mut checked = Vec::new();
    let mut asymmetries = Vec::new();
    for (left, right) in pairs {
        let left_help = help_text(fact_binary, left)?;
        let right_help = help_text(fact_binary, right)?;
        checked.push(format!("{left}/{right}"));
        if left_help.is_none() || right_help.is_none() {
            asymmetries.push(format!("{left}/{right}: one or both commands are missing"));
            continue;
        }
        let left_has_json = left_help
            .as_ref()
            .is_some_and(|text| text.contains("--json"));
        let right_has_json = right_help
            .as_ref()
            .is_some_and(|text| text.contains("--json"));
        if left_has_json != right_has_json {
            asymmetries.push(format!("{left}/{right}: --json support differs"));
        }
    }
    Ok(UxCommandSymmetryReport {
        passed: asymmetries.is_empty(),
        pairs_checked: checked,
        asymmetries,
    })
}

fn scale_sampling_report(include_large: bool) -> UxScaleSamplingReport {
    let levels = scale_levels(include_large);
    let samples = levels
        .iter()
        .map(|level| UxScaleSample {
            level: level.level.clone(),
            target_propositions: level.target_propositions,
            sample_count: match level.level.as_str() {
                "small" => 12,
                "medium" => 24,
                _ => 48,
            },
            fixture_required: level.level != "large" || include_large,
        })
        .collect();
    UxScaleSamplingReport {
        levels,
        deterministic_sampling: true,
        include_large,
        samples,
    }
}

fn defect_summary(
    scenarios: &[UxScenarioReport],
    coverage: &UxCommandCoverageReport,
    terminology: &UxHelpTerminologyReport,
) -> UxDefectSummary {
    let mut defects = scenarios
        .iter()
        .flat_map(|scenario| scenario.defects.clone())
        .collect::<Vec<_>>();
    for command in &coverage.unsupported_commands {
        defects.push(UxDefect {
            scenario: "command-coverage".into(),
            step: 0,
            command: format!("fact {command}"),
            fixture: None,
            seed: DEFAULT_SEED,
            expected_belief: UxBelief::default(),
            actual_output: "command was not found in current fact help".into(),
            canonical_state: UxCanonicalState::default(),
            classification: UxDefectCategory::MissingCommand,
            suggested_resolution:
                "record the gap or add the command before requiring it in UX coverage".into(),
        });
    }
    for hit in &terminology.jargon_hits {
        defects.push(UxDefect {
            scenario: "help-terminology".into(),
            step: 0,
            command: format!("fact help {}", hit.command),
            fixture: None,
            seed: DEFAULT_SEED,
            expected_belief: UxBelief::default(),
            actual_output: hit.term.clone(),
            canonical_state: UxCanonicalState::default(),
            classification: UxDefectCategory::TerminologyOverload,
            suggested_resolution:
                "replace unexplained protocol jargon in default help or move it to extended help"
                    .into(),
        });
    }
    let mut by_classification = BTreeMap::new();
    for defect in &defects {
        *by_classification
            .entry(format!("{:?}", defect.classification))
            .or_insert(0) += 1;
    }
    UxDefectSummary {
        total: defects.len(),
        passed: defects.is_empty(),
        by_classification,
        defects,
    }
}

fn ux_spec() -> UxSpec {
    UxSpec {
        schema_version: UX_SCHEMA_VERSION.to_string(),
        default_scale_levels: scale_levels(false),
        optional_scale_levels: vec![UxScaleLevel {
            level: "large".into(),
            target_propositions: 500_000,
            required_by_default: false,
        }],
        suites: vec![
            UxSuiteSpec {
                suite: UxSuite::Smoke,
                scenarios: vec!["first-run".into()],
            },
            UxSuiteSpec {
                suite: UxSuite::Casual,
                scenarios: suite_scenarios(UxSuite::Casual, None).unwrap_or_default(),
            },
            UxSuiteSpec {
                suite: UxSuite::Help,
                scenarios: vec!["help-discoverability".into()],
            },
        ],
        defect_categories: defect_categories(),
        required_commands: required_commands()
            .into_iter()
            .map(str::to_string)
            .collect(),
        reports: [
            "journey_execution_report",
            "user_belief_report",
            "cross_command_consistency_report",
            "human_json_report",
            "help_terminology_report",
            "ambiguity_report",
            "error_quality_report",
            "missing_inspection_path_report",
            "command_symmetry_report",
            "scale_sampling_report",
            "ux_defect_summary",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
        commands: ["ux spec", "ux run", "ux replay", "ux report", "ux compare"]
            .into_iter()
            .map(str::to_string)
            .collect(),
    }
}

fn suite_scenarios(suite: UxSuite, filter: Option<&str>) -> Result<Vec<String>> {
    let scenarios = match suite {
        UxSuite::Smoke => vec!["first-run"],
        UxSuite::Casual => vec![
            "first-run",
            "immediate-personal-acceptance",
            "accepted-revision",
            "help-discoverability",
            "immediate-rejection",
            "rejected-revision",
            "comments-and-revision-context",
            "invitation-and-participation",
            "multi-participant-acceptance",
            "archive-and-withdrawal",
            "ledger-switching",
            "remote-workflow",
            "ambiguous-target-workflow",
            "failure-and-recovery",
            "contested-proposition",
        ],
        UxSuite::Help => vec!["help-discoverability"],
    };
    let scenarios = scenarios
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    if let Some(filter) = filter {
        if scenarios.iter().any(|scenario| scenario == filter) {
            return Ok(vec![filter.to_string()]);
        }
        bail!("unknown UX scenario `{filter}` for suite {:?}", suite);
    }
    Ok(scenarios)
}

fn scale_levels(include_large: bool) -> Vec<UxScaleLevel> {
    let mut levels = vec![
        UxScaleLevel {
            level: "small".into(),
            target_propositions: 10_000,
            required_by_default: true,
        },
        UxScaleLevel {
            level: "medium".into(),
            target_propositions: 100_000,
            required_by_default: true,
        },
    ];
    if include_large {
        levels.push(UxScaleLevel {
            level: "large".into(),
            target_propositions: 500_000,
            required_by_default: false,
        });
    }
    levels
}

fn required_commands() -> Vec<&'static str> {
    vec![
        "init",
        "use",
        "status",
        "propose",
        "write",
        "revise",
        "edit",
        "accept",
        "reject",
        "comment",
        "invite",
        "join",
        "leave",
        "list",
        "pending",
        "search",
        "open",
        "echo",
        "read",
        "revisions",
        "history",
        "log",
        "archive",
        "withdraw",
        "remote",
        "clone",
        "push",
        "pull",
        "help",
    ]
}

fn defect_categories() -> Vec<String> {
    [
        "stale-display",
        "cross-command-inconsistency",
        "effective-state-mismatch",
        "pending-state-mismatch",
        "missing-next-action",
        "ambiguous-target-selected",
        "insufficient-output",
        "misleading-success",
        "misleading-error",
        "human-json-mismatch",
        "terminology-overload",
        "missing-inspection-path",
        "interface-asymmetry",
        "unexpected-side-effect",
        "hidden-partial-state",
        "reference-resolution-error",
        "missing-command",
        "cli-unavailable",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn semantic_replay_payload(report: &UxRunReport) -> serde_json::Value {
    serde_json::json!({
        "suite": report.suite,
        "scenario_names": report.scenarios.iter().map(|scenario| &scenario.scenario).collect::<Vec<_>>(),
        "scenario_passed": report.scenarios.iter().map(|scenario| (&scenario.scenario, scenario.passed)).collect::<Vec<_>>(),
        "coverage_missing": report.command_coverage.missing_commands,
        "defect_summary": report.ux_defect_summary.by_classification,
        "scale_levels": report.scale_levels,
    })
}

fn command_replay_payload(report: &UxRunReport) -> serde_json::Value {
    serde_json::json!({
        "commands": report.scenarios.iter().flat_map(|scenario| {
            scenario.steps.iter().map(|step| normalize_command_line(&step.receipt.command_line))
        }).collect::<Vec<_>>(),
        "exit_statuses": report.scenarios.iter().flat_map(|scenario| {
            scenario.steps.iter().map(|step| step.receipt.exit_status)
        }).collect::<Vec<_>>(),
    })
}

fn normalize_command_line(command: &str) -> String {
    command
        .split_whitespace()
        .map(|word| {
            if looks_like_uuid(word) || looks_like_short_reference(word) {
                "<ref>".to_string()
            } else {
                word.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn looks_like_uuid(word: &str) -> bool {
    let parts = word.split('-').collect::<Vec<_>>();
    parts.len() == 5
        && parts.iter().map(|part| part.len()).collect::<Vec<_>>() == vec![8, 4, 4, 4, 12]
        && parts
            .iter()
            .all(|part| part.chars().all(|char| char.is_ascii_hexdigit()))
}

fn looks_like_short_reference(word: &str) -> bool {
    let parts = word.split('-').collect::<Vec<_>>();
    parts.len() == 2
        && parts
            .iter()
            .all(|part| part.len() == 5 && part.chars().all(|char| char.is_ascii_hexdigit()))
}

fn compare_reports(
    baseline_path: &Path,
    baseline: &UxRunReport,
    current_path: &Path,
    current: &UxRunReport,
) -> Result<serde_json::Value> {
    Ok(serde_json::json!({
        "schema_version": UX_SCHEMA_VERSION,
        "baseline": baseline_path,
        "current": current_path,
        "semantic_digest_matches": baseline.replay.semantic_digest == current.replay.semantic_digest,
        "command_digest_matches": baseline.replay.command_digest == current.replay.command_digest,
        "baseline_defects": baseline.ux_defect_summary.total,
        "current_defects": current.ux_defect_summary.total,
        "defect_delta": current.ux_defect_summary.total as i64 - baseline.ux_defect_summary.total as i64,
    }))
}

fn render_human_report(report: &UxRunReport) -> String {
    let mut out = String::new();
    out.push_str("# CLI UX Report\n\n");
    out.push_str(&format!("- Suite: `{:?}`\n", report.suite));
    out.push_str(&format!("- Seed: `{}`\n", report.seed));
    out.push_str(&format!("- Include large: `{}`\n", report.include_large));
    out.push_str(&format!("- Scenarios: `{}`\n", report.scenarios.len()));
    out.push_str(&format!(
        "- Defects: `{}`\n",
        report.ux_defect_summary.total
    ));
    out.push_str("\n## Scenarios\n\n");
    for scenario in &report.scenarios {
        out.push_str(&format!(
            "- `{}`: passed={}, steps={}, defects={}\n",
            scenario.scenario,
            scenario.passed,
            scenario.steps.len(),
            scenario.defects.len()
        ));
    }
    if !report.command_coverage.unsupported_commands.is_empty() {
        out.push_str("\n## Unsupported Commands\n\n");
        for command in &report.command_coverage.unsupported_commands {
            out.push_str(&format!("- `fact {command}`\n"));
        }
    }
    out
}

fn read_ux_report(path: &Path) -> Result<UxRunReport> {
    let report = serde_json::from_slice(
        &fs::read(path).with_context(|| format!("failed to read `{}`", path.display()))?,
    )
    .with_context(|| format!("failed to parse `{}` as UX report", path.display()))?;
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

fn digest_json<T: Serialize>(value: &T) -> Result<String> {
    let bytes = serde_json::to_vec(value)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn default_fact_binary_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .join("cli")
        .join("target")
        .join("debug")
        .join("fact")
}

fn json_string(receipt: &UxCommandReceipt, key: &str) -> Option<String> {
    receipt
        .json_output
        .as_ref()
        .and_then(|json| json_string_value(json, key))
}

fn json_string_value(json: &serde_json::Value, key: &str) -> Option<String> {
    json[key].as_str().map(str::to_string)
}

struct ParallelRevisionTip<'a> {
    database: &'a Path,
    fact_home: &'a Path,
    ledger: &'a str,
    actor: &'a str,
    key_id: &'a str,
    proposition_id: &'a str,
    parent_revision_id: &'a str,
    markdown: &'a [u8],
}

fn insert_parallel_revision_tip(tip: ParallelRevisionTip<'_>) -> Result<()> {
    let ledger = parse_uuid7(tip.ledger, "ledger")?;
    let actor = parse_uuid7(tip.actor, "actor")?;
    let actor_id = actor.to_string();
    let key_id_uuid = parse_uuid7(tip.key_id, "key")?;
    let proposition_id = parse_uuid7(tip.proposition_id, "proposition")?;
    let parent_revision_id = parse_uuid7(tip.parent_revision_id, "parent revision")?;
    let seed = read_hex_seed(
        &tip.fact_home
            .join("identities")
            .join(format!("{actor_id}.seed")),
    )?;
    let key = fact_crypto::SigningKey::from_seed(&seed)?;
    let store = fact_store::Store::open(tip.database)?;
    let proposition = store
        .get_cose_by_id(ledger.as_bytes(), proposition_id.as_bytes())?
        .ok_or_else(|| anyhow::anyhow!("missing proposition object for contested UX fixture"))?;
    let parent_revision = store
        .get_cose_by_id(ledger.as_bytes(), parent_revision_id.as_bytes())?
        .ok_or_else(|| anyhow::anyhow!("missing parent revision for contested UX fixture"))?;
    let runtime = fact_sdk::runtime::production_runtime();
    let revision_id = runtime.next_uuid_v7()?;
    let revision = signed_envelope(
        revision_id,
        ledger,
        "revision",
        actor,
        key_id_uuid,
        serde_json::json!({
            "proposition_id": proposition_id,
            "revision_id": revision_id,
            "parent_revision_id": parent_revision_id,
            "content": content_value(tip.markdown),
            "relationships": [],
            "reconciliation_manifest": null,
        }),
        vec![
            dependency_value(&proposition, "proposition")?,
            dependency_value(&parent_revision, "parent-revision")?,
        ],
        &key,
        runtime.as_ref(),
    )?;
    store.insert_authorized_object(&revision)?;
    Ok(())
}

fn read_hex_seed(path: &Path) -> Result<[u8; 32]> {
    let value = fs::read_to_string(path)
        .with_context(|| format!("failed to read signing seed `{}`", path.display()))?;
    let bytes = decode_hex(value.trim())?;
    bytes
        .try_into()
        .map_err(|bytes: Vec<u8>| anyhow::anyhow!("expected 32-byte seed, got {}", bytes.len()))
}

fn decode_hex(value: &str) -> Result<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        bail!("hex string must have an even number of characters");
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_digit(pair[0])?;
            let low = hex_digit(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_digit(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => bail!("invalid hex digit"),
    }
}

fn parse_uuid7(value: &str, field: &str) -> Result<uuid::Uuid> {
    let uuid = uuid::Uuid::parse_str(value)?;
    if uuid.get_version_num() != 7 || uuid.to_string() != value {
        bail!("{field} must be lowercase canonical UUIDv7");
    }
    Ok(uuid)
}

fn dependency_value(cose_bytes: &[u8], role: &str) -> Result<serde_json::Value> {
    let cose = fact_crypto::decode_sign1(cose_bytes)?;
    let value: serde_json::Value = serde_json::from_slice(&cose.payload)?;
    Ok(serde_json::json!({
        "object_id": value["id"],
        "content_hash": fact_core::Hash::digest(&cose.payload).hex(),
        "role": role,
    }))
}

#[allow(clippy::too_many_arguments)]
fn signed_envelope(
    id: uuid::Uuid,
    ledger: uuid::Uuid,
    object_type: &str,
    actor: uuid::Uuid,
    key_id: uuid::Uuid,
    body: serde_json::Value,
    dependencies: Vec<serde_json::Value>,
    key: &fact_crypto::SigningKey,
    runtime: &dyn fact_sdk::runtime::SdkRuntime,
) -> Result<Vec<u8>> {
    let value = serde_json::json!({
        "id": id.to_string(),
        "ledger_id": ledger.to_string(),
        "object_type": object_type,
        "schema_version": "0",
        "actor_id": actor.to_string(),
        "signing_key_id": key_id.to_string(),
        "created_at": runtime.timestamp(),
        "dependencies": dependencies,
        "body": body,
    });
    let payload = fact_canonical::encode(&serde_json::to_vec(&value)?)?;
    let protected = fact_crypto::protocol_protected(
        key.public_key(),
        object_type,
        "0",
        Some(*ledger.as_bytes()),
    );
    Ok(fact_crypto::encode_sign1(&fact_crypto::sign1(
        &protected, &payload, key,
    )))
}

fn content_value(markdown: &[u8]) -> serde_json::Value {
    serde_json::json!({
        "media_type": "text/markdown; charset=utf-8; variant=fact-v0",
        "bytes": base64url(markdown),
        "hash": fact_core::Hash::digest(markdown).hex(),
    })
}

fn base64url(bytes: &[u8]) -> String {
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut output = String::new();
    for chunk in bytes.chunks(3) {
        let value = ((chunk[0] as u32) << 16)
            | ((chunk.get(1).copied().unwrap_or(0) as u32) << 8)
            | chunk.get(2).copied().unwrap_or(0) as u32;
        output.push(TABLE[((value >> 18) & 63) as usize] as char);
        output.push(TABLE[((value >> 12) & 63) as usize] as char);
        if chunk.len() > 1 {
            output.push(TABLE[((value >> 6) & 63) as usize] as char);
        }
        if chunk.len() > 2 {
            output.push(TABLE[(value & 63) as usize] as char);
        }
    }
    output
}

fn protocol_references(stdout: &str, stderr: &str) -> Vec<String> {
    let mut refs = BTreeSet::new();
    for token in stdout
        .split_whitespace()
        .chain(stderr.split_whitespace())
        .map(|token| token.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '-'))
    {
        if token.len() >= 11 && token.contains('-') && token.chars().any(|c| c.is_ascii_digit()) {
            refs.insert(token.to_string());
        }
    }
    refs.into_iter().collect()
}

fn output_summary(stdout: &str) -> Option<String> {
    stdout
        .lines()
        .find_map(|line| line.strip_prefix("Current summary: ").map(str::to_string))
        .or_else(|| {
            stdout
                .lines()
                .find(|line| !line.trim().is_empty())
                .map(|line| line.trim().to_string())
        })
}

fn status_from_output(stdout: &str) -> Option<String> {
    let lower = stdout.to_lowercase();
    for status in [
        "accepted",
        "pending",
        "rejected",
        "contested",
        "withdrawn",
        "archived",
    ] {
        if lower.lines().any(|line| line_has_status(line, status)) {
            return Some(status.to_string());
        }
    }
    None
}

fn line_has_status(line: &str, status: &str) -> bool {
    let line = line.trim();
    line == status
        || line
            .strip_prefix("status:")
            .is_some_and(|rest| rest.trim_start().starts_with(status))
        || line
            .strip_prefix("status ")
            .is_some_and(|rest| rest.trim_start().starts_with(status))
        || line.starts_with(&format!("{status} "))
}

fn next_action_from_output(stdout: &str, stderr: &str) -> Option<String> {
    let combined = format!("{stdout}\n{stderr}").to_lowercase();
    if combined.contains("fact ") || combined.contains("run ") || combined.contains("try ") {
        Some("command-suggested".into())
    } else {
        None
    }
}

fn next_action_clear(receipt: &UxCommandReceipt, semantics: &UxParsedSemantics) -> bool {
    if !is_mutation_command(&semantics.command_kind) {
        return true;
    }
    semantics.next_action.is_some()
        || matches!(
            semantics.command_kind.as_str(),
            "init" | "accept" | "reject" | "archive" | "withdraw"
        )
        || receipt.stdout.to_lowercase().contains("effective")
}

fn cognitive_load(receipt: &UxCommandReceipt) -> UxCognitiveLoad {
    if receipt.json_output.is_some() {
        return UxCognitiveLoad {
            concepts: Vec::new(),
            identifiers: Vec::new(),
            excessive: false,
        };
    }
    let text = format!("{}\n{}", receipt.stdout, receipt.stderr).to_lowercase();
    let concepts = [
        "ledger",
        "proposition",
        "revision",
        "deliberation",
        "settlement",
        "actor",
        "key",
        "database",
        "remote",
    ]
    .into_iter()
    .filter(|concept| text.contains(concept))
    .map(str::to_string)
    .collect::<Vec<_>>();
    let identifiers = receipt.protocol_references.clone();
    UxCognitiveLoad {
        excessive: concepts.len() > 5 || identifiers.len() > 4,
        concepts,
        identifiers,
    }
}

fn is_mutation_command(command: &str) -> bool {
    matches!(
        command,
        "init"
            | "new"
            | "use"
            | "propose"
            | "revise"
            | "accept"
            | "reject"
            | "comment"
            | "invite"
            | "join"
            | "leave"
            | "archive"
            | "withdraw"
            | "push"
            | "pull"
    )
}

fn command_answers_status(command: &str) -> bool {
    matches!(
        command,
        "propose"
            | "revise"
            | "accept"
            | "reject"
            | "list"
            | "pending"
            | "search"
            | "show"
            | "open"
            | "echo"
            | "revisions"
            | "history"
            | "archive"
            | "withdraw"
    )
}

fn combined_output(receipt: &UxCommandReceipt) -> String {
    format!("{}\n{}", receipt.stdout, receipt.stderr)
}

fn command_words(command_line: &str) -> Vec<String> {
    command_line
        .split_whitespace()
        .map(str::to_string)
        .collect()
}

fn suggested_resolution(classification: UxDefectCategory) -> String {
    match classification {
        UxDefectCategory::EffectiveStateMismatch => {
            "make default output derive status and summary from effective state".into()
        }
        UxDefectCategory::PendingStateMismatch => {
            "show pending updates distinctly from the effective revision".into()
        }
        UxDefectCategory::MissingNextAction => {
            "include the next inspection or decision command in mutation output".into()
        }
        UxDefectCategory::TerminologyOverload => {
            "reduce protocol identifiers or explain them in plain language".into()
        }
        _ => "adjust CLI output or the UX expectation so the user's belief matches canonical state"
            .into(),
    }
}

fn help_text(fact_binary: &Path, command: &str) -> Result<Option<String>> {
    let output = Command::new(fact_binary)
        .args(["help", command])
        .env("NO_COLOR", "1")
        .env("TERM", "dumb")
        .output()
        .with_context(|| format!("failed to run help for `{command}`"))?;
    if output.status.success() {
        Ok(Some(String::from_utf8_lossy(&output.stdout).to_string()))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ux_spec_keeps_large_manual_opt_in() {
        let spec = ux_spec();
        assert_eq!(spec.schema_version, UX_SCHEMA_VERSION);
        assert_eq!(
            spec.default_scale_levels
                .iter()
                .map(|level| level.level.as_str())
                .collect::<Vec<_>>(),
            vec!["small", "medium"]
        );
        assert_eq!(spec.optional_scale_levels[0].level, "large");
        assert!(!spec.optional_scale_levels[0].required_by_default);
        assert!(spec.commands.contains(&"ux replay".to_string()));
    }

    #[test]
    fn semantic_parser_reads_json_status() {
        let receipt = UxCommandReceipt {
            command_line: "fact list --json".into(),
            environment: BTreeMap::new(),
            exit_status: 0,
            stdout: r#"[{"reference":"01a00-996cd","status":"accepted","summary":"Policy","revision_id":"rev"}]"#.into(),
            stderr: String::new(),
            json_output: Some(serde_json::json!([{
                "reference": "01a00-996cd",
                "status": "accepted",
                "summary": "Policy",
                "revision_id": "rev"
            }])),
            duration_ms: 1,
            active_ledger: None,
            active_actor: None,
            protocol_references: Vec::new(),
        };
        let semantics = parse_semantics(&receipt);
        assert_eq!(semantics.status.as_deref(), Some("accepted"));
        assert_eq!(semantics.summary.as_deref(), Some("Policy"));
    }

    #[test]
    fn semantic_parser_reads_nested_show_json() {
        let receipt = UxCommandReceipt {
            command_line: "fact show 01a00 --json".into(),
            environment: BTreeMap::new(),
            exit_status: 0,
            stdout: "{}".into(),
            stderr: String::new(),
            json_output: Some(serde_json::json!({
                "query": "01a00",
                "proposition": {
                    "proposition_id": "prop",
                    "reference": "01a00-prop",
                    "status": "accepted",
                    "effective_status": "accepted",
                    "summary": "Policy",
                    "revision_id": "rev1",
                    "latest_revision_id": "rev2",
                    "latest_revision_status": "pending",
                    "has_pending_revision": true
                }
            })),
            duration_ms: 1,
            active_ledger: None,
            active_actor: None,
            protocol_references: Vec::new(),
        };
        let semantics = parse_semantics(&receipt);
        assert_eq!(semantics.proposition_id.as_deref(), Some("prop"));
        assert_eq!(semantics.status.as_deref(), Some("accepted"));
        assert_eq!(semantics.latest_revision_status.as_deref(), Some("pending"));
        assert_eq!(semantics.pending, Some(true));
    }

    #[test]
    fn status_parser_ignores_status_words_inside_names() {
        assert_eq!(
            status_from_output("initialized ledger ux-rejected-revision (01a00)\n"),
            None
        );
        assert_eq!(
            status_from_output("status: rejected\n"),
            Some("rejected".into())
        );
    }

    #[test]
    fn cognitive_load_ignores_machine_json() {
        let receipt = UxCommandReceipt {
            command_line: "fact show --json".into(),
            environment: BTreeMap::new(),
            exit_status: 0,
            stdout: r#"{"proposition_id":"p","revision_id":"r","deliberation_id":"d","settlement_id":"s","actor_id":"a","key_id":"k"}"#.into(),
            stderr: String::new(),
            json_output: Some(serde_json::json!({
                "proposition_id": "p",
                "revision_id": "r",
                "deliberation_id": "d",
                "settlement_id": "s",
                "actor_id": "a",
                "key_id": "k"
            })),
            duration_ms: 1,
            active_ledger: None,
            active_actor: None,
            protocol_references: Vec::new(),
        };
        assert!(!cognitive_load(&receipt).excessive);
    }

    #[test]
    fn missing_required_commands_are_reported_as_defects() {
        let coverage = UxCommandCoverageReport {
            required_commands: vec!["read".into()],
            observed_commands: Vec::new(),
            missing_commands: vec!["read".into()],
            unsupported_commands: vec!["read".into()],
        };
        let summary = defect_summary(
            &[],
            &coverage,
            &UxHelpTerminologyReport {
                passed: true,
                ..UxHelpTerminologyReport::default()
            },
        );
        assert_eq!(summary.total, 1);
        assert_eq!(
            summary.defects[0].classification,
            UxDefectCategory::MissingCommand
        );
    }
}
