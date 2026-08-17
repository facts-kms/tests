use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

const CI_SCHEMA_VERSION: &str = "ci-v1";
const DEFAULT_REPORT_DIR: &str = "reports/ci";
const LARGE_PROFILE: &str = "scale-500k-balanced";

#[derive(Debug, Subcommand)]
pub enum CiCommand {
    Spec,
    Local(CiRunArgs),
    PullRequest(CiRunArgs),
    Main(CiRunArgs),
    Scheduled(CiScheduledArgs),
    Release(CiRunArgs),
    Test(CiTestArgs),
    Summary(CiSummaryArgs),
}

#[derive(Debug, Args, Clone)]
pub struct CiRunArgs {
    #[arg(long)]
    dry_run: bool,
    #[arg(long, default_value = DEFAULT_REPORT_DIR)]
    report_dir: PathBuf,
    #[arg(long)]
    output: Option<PathBuf>,
    #[arg(long, default_value = "fixtures")]
    fixture_base: PathBuf,
    #[arg(long)]
    baseline_summary: Option<PathBuf>,
    #[arg(long)]
    thresholds: Option<PathBuf>,
    #[arg(long, env = "FACT_BINARY")]
    fact_binary: Option<PathBuf>,
    #[arg(long)]
    include_large: bool,
}

#[derive(Debug, Args, Clone)]
pub struct CiScheduledArgs {
    #[command(flatten)]
    run: CiRunArgs,
    #[arg(long, default_value = LARGE_PROFILE)]
    profile: String,
    #[arg(long, default_value_t = 42)]
    seed: u64,
}

#[derive(Debug, Args)]
pub struct CiTestArgs {
    #[arg(long, value_enum)]
    suite: CiFocusedSuite,
    #[arg(long)]
    dry_run: bool,
    #[arg(long, default_value = DEFAULT_REPORT_DIR)]
    report_dir: PathBuf,
    #[arg(long, env = "FACT_BINARY")]
    fact_binary: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct CiSummaryArgs {
    report: PathBuf,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[value(rename_all = "kebab-case")]
pub enum CiFocusedSuite {
    OperationsSpec,
    Ux,
    Sync,
    Faults,
    Benchmarks,
    Scenarios,
    Docs,
    CliReference,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum CiTier {
    Local,
    PullRequest,
    Main,
    Scheduled,
    Release,
    Focused,
}

impl CiTier {
    fn slug(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::PullRequest => "pull-request",
            Self::Main => "main",
            Self::Scheduled => "scheduled",
            Self::Release => "release",
            Self::Focused => "focused",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CiSpec {
    schema_version: String,
    tiers: Vec<CiTierSpec>,
    focused_suites: Vec<FocusedSuiteSpec>,
    fixture_cache_policy: FixtureCachePolicy,
    artifact_retention_policy: ArtifactRetentionPolicy,
    report_schemas: Vec<ReportSchemaSpec>,
    regression_classes: Vec<String>,
    runner_classes: Vec<RunnerClassSpec>,
    flaky_test_policy: Vec<String>,
    redaction_policy: Vec<String>,
    dependency_controls: Vec<String>,
    large_scale_policy: LargeScalePolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CiTierSpec {
    tier: CiTier,
    frequency: String,
    runner_class: String,
    includes_large_500k: bool,
    commands: Vec<String>,
    suites: Vec<String>,
    artifacts: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FocusedSuiteSpec {
    suite: CiFocusedSuite,
    commands: Vec<String>,
    validates: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FixtureCachePolicy {
    key_parts: Vec<String>,
    invalidation_rule: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArtifactRetentionPolicy {
    pull_request: Vec<String>,
    scheduled_scale: Vec<String>,
    release: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReportSchemaSpec {
    name: String,
    required_fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunnerClassSpec {
    class: String,
    operating_system: String,
    intended_suites: Vec<String>,
    large_scale_allowed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LargeScalePolicy {
    target_propositions: usize,
    default_tiers: Vec<String>,
    manual_opt_in_flag: String,
    scheduled_behavior: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CiPlan {
    schema_version: String,
    tier: CiTier,
    suite: Option<CiFocusedSuite>,
    include_large: bool,
    fixture_base: PathBuf,
    report_dir: PathBuf,
    steps: Vec<CiStep>,
    validations: Vec<CiValidation>,
    artifact_policy: Vec<String>,
    cache_key_parts: Vec<String>,
    exact_command: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CiStep {
    id: String,
    name: String,
    suite: String,
    classification: String,
    command: Vec<String>,
    required: bool,
    large_scale: bool,
    artifact_paths: Vec<PathBuf>,
    reproduction_command: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CiValidation {
    id: String,
    name: String,
    classification: String,
    required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CiRunReport {
    schema_version: String,
    suite_version: String,
    tier: CiTier,
    suite: Option<CiFocusedSuite>,
    status: CiStatus,
    dry_run: bool,
    include_large: bool,
    started_at_unix: u64,
    ended_at_unix: u64,
    source_revisions: SourceRevisions,
    environment: BTreeMap<String, String>,
    operations_spec_version: String,
    fixture_cache_key: String,
    artifact_policy: Vec<String>,
    redaction_policy: Vec<String>,
    flaky_test_policy: Vec<String>,
    steps: Vec<CiStepReport>,
    validations: Vec<CiValidationReport>,
    failures: Vec<CiFailure>,
    summary: CiSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum CiStatus {
    Pass,
    Fail,
    Warning,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SourceRevisions {
    simulator_commit: String,
    facts_source_commit: String,
    fact_sdk_commit: String,
    operations_spec_version: String,
    scenario_corpus_version: String,
    fixture_profile_version: String,
    rust_toolchain: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CiStepReport {
    step: CiStep,
    status: CiStatus,
    exit_code: Option<i32>,
    duration_ms: u128,
    stdout: String,
    stderr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CiValidationReport {
    validation: CiValidation,
    status: CiStatus,
    details: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CiFailure {
    classification: String,
    suite: String,
    scenario: Option<String>,
    seed: Option<u64>,
    fixture_profile: Option<String>,
    fixture_manifest_digest: Option<String>,
    exact_command: String,
    reproduction_command: String,
    artifact_references: Vec<PathBuf>,
    minimized_reproduction: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CiSummary {
    passed_steps: usize,
    failed_steps: usize,
    warning_steps: usize,
    passed_validations: usize,
    failed_validations: usize,
    first_failure: Option<String>,
    human_summary: String,
}

pub fn execute(command: CiCommand) -> Result<String> {
    match command {
        CiCommand::Spec => Ok(serde_json::to_string_pretty(&ci_spec())?),
        CiCommand::Local(args) => run_tier(CiTier::Local, args, None),
        CiCommand::PullRequest(args) => run_tier(CiTier::PullRequest, args, None),
        CiCommand::Main(args) => run_tier(CiTier::Main, args, None),
        CiCommand::Scheduled(args) => run_tier(CiTier::Scheduled, args.run, Some(args.profile)),
        CiCommand::Release(args) => run_tier(CiTier::Release, args, None),
        CiCommand::Test(args) => run_focused(args),
        CiCommand::Summary(args) => render_summary(&args.report),
    }
}

fn run_focused(args: CiTestArgs) -> Result<String> {
    let suite = args.suite;
    let run_args = CiRunArgs {
        dry_run: args.dry_run,
        report_dir: args.report_dir,
        output: None,
        fixture_base: PathBuf::from("fixtures"),
        baseline_summary: None,
        thresholds: None,
        fact_binary: args.fact_binary,
        include_large: false,
    };
    run_focused_suite(suite, run_args)
}

fn run_focused_suite(suite: CiFocusedSuite, args: CiRunArgs) -> Result<String> {
    let report_dir = args.report_dir.clone();
    fs::create_dir_all(&report_dir)
        .with_context(|| format!("creating CI report directory `{}`", report_dir.display()))?;
    let plan = focused_plan(suite, &args)?;
    run_plan(plan, args)
}

fn run_tier(tier: CiTier, args: CiRunArgs, selector: Option<String>) -> Result<String> {
    if args.include_large && matches!(tier, CiTier::Local | CiTier::PullRequest | CiTier::Main) {
        bail!(
            "500K validation is manual opt-in and is only allowed for scheduled or release tiers"
        );
    }
    let report_dir = args.report_dir.clone();
    fs::create_dir_all(&report_dir)
        .with_context(|| format!("creating CI report directory `{}`", report_dir.display()))?;
    let plan = tier_plan(tier, &args, selector.as_deref())?;
    run_plan(plan, args)
}

fn run_plan(plan: CiPlan, args: CiRunArgs) -> Result<String> {
    let started_at_unix = unix_now();
    let mut step_reports = Vec::new();
    let mut failures = Vec::new();
    for step in &plan.steps {
        let report = if args.dry_run {
            CiStepReport {
                step: step.clone(),
                status: CiStatus::Pass,
                exit_code: Some(0),
                duration_ms: 0,
                stdout: String::new(),
                stderr: String::new(),
            }
        } else {
            run_step(step)?
        };
        if report.status == CiStatus::Fail {
            failures.push(step_failure(step, &report));
        }
        step_reports.push(report);
    }
    let validations = plan
        .validations
        .iter()
        .map(run_validation)
        .collect::<Result<Vec<_>>>()?;
    for validation in validations
        .iter()
        .filter(|report| report.status == CiStatus::Fail)
    {
        failures.push(validation_failure(validation));
    }
    let ended_at_unix = unix_now();
    let source_revisions = source_revisions()?;
    let status = if failures.iter().any(|failure| {
        step_reports.iter().any(|report| {
            report.step.reproduction_command == failure.reproduction_command
                && report.step.required
                && report.status == CiStatus::Fail
        }) || validations.iter().any(|report| {
            report.validation.name == failure.suite
                && report.validation.required
                && report.status == CiStatus::Fail
        })
    }) {
        CiStatus::Fail
    } else {
        CiStatus::Pass
    };
    let summary = summarize(&step_reports, &validations, &failures);
    let report = CiRunReport {
        schema_version: CI_SCHEMA_VERSION.to_string(),
        suite_version: "ci".to_string(),
        tier: plan.tier,
        suite: plan.suite,
        status,
        dry_run: args.dry_run,
        include_large: plan.include_large,
        started_at_unix,
        ended_at_unix,
        source_revisions,
        environment: ci_environment(),
        operations_spec_version: operations_spec_version()?,
        fixture_cache_key: fixture_cache_key(&plan)?,
        artifact_policy: plan.artifact_policy,
        redaction_policy: redaction_policy(),
        flaky_test_policy: flaky_test_policy(),
        steps: step_reports,
        validations,
        failures,
        summary,
    };
    let output = args.output.unwrap_or_else(|| {
        args.report_dir
            .join(format!("{}-summary.json", plan.tier.slug()))
    });
    fs::write(&output, serde_json::to_vec_pretty(&report)?)
        .with_context(|| format!("writing CI report `{}`", output.display()))?;
    Ok(serde_json::to_string_pretty(&report)?)
}

fn run_step(step: &CiStep) -> Result<CiStepReport> {
    let started = std::time::Instant::now();
    let mut command = Command::new(&step.command[0]);
    command.args(&step.command[1..]);
    let output = command
        .output()
        .with_context(|| format!("running CI step `{}`", step.name))?;
    let status = if output.status.success() {
        CiStatus::Pass
    } else if step.required {
        CiStatus::Fail
    } else {
        CiStatus::Warning
    };
    Ok(CiStepReport {
        step: step.clone(),
        status,
        exit_code: output.status.code(),
        duration_ms: started.elapsed().as_millis(),
        stdout: redact(String::from_utf8_lossy(&output.stdout).as_ref()),
        stderr: redact(String::from_utf8_lossy(&output.stderr).as_ref()),
    })
}

fn run_validation(validation: &CiValidation) -> Result<CiValidationReport> {
    let details = match validation.id.as_str() {
        "operations-registry-schema" => validate_operations_registry()?,
        "cli-operation-mapping" => validate_cli_operation_mapping()?,
        "ci-spec-schema" => serde_json::to_value(ci_spec())?,
        "docs-links" => validate_docs_links()?,
        _ => serde_json::json!({"checked": true}),
    };
    let status = if details["valid"].as_bool() == Some(false) {
        CiStatus::Fail
    } else {
        CiStatus::Pass
    };
    Ok(CiValidationReport {
        validation: validation.clone(),
        status,
        details,
    })
}

fn tier_plan(tier: CiTier, args: &CiRunArgs, scheduled_profile: Option<&str>) -> Result<CiPlan> {
    let mut steps = Vec::new();
    let mut validations = core_validations();
    match tier {
        CiTier::Local => {
            steps.extend(core_static_steps());
            steps.push(step(
                "unit-tests",
                "Workspace unit tests",
                "unit",
                "correctness",
                ["cargo", "test", "--workspace", "--all-targets"],
                true,
                false,
            ));
            steps.push(step_vec(
                "smoke-scenarios",
                "Small deterministic smoke suite",
                "scenarios",
                "determinism",
                fact_sim_command(&["suite", "run", "scenarios/smoke"]),
                true,
                false,
            ));
            steps.push(step_vec(
                "ux-smoke-spec",
                "Fast CLI UX smoke contract",
                "ux",
                "cli-ux",
                fact_sim_command(&["ux", "spec"]),
                true,
                false,
            ));
        }
        CiTier::PullRequest => {
            steps.extend(core_static_steps());
            steps.push(step(
                "workspace-tests",
                "Workspace tests",
                "unit",
                "correctness",
                ["cargo", "test", "--workspace", "--all-targets"],
                true,
                false,
            ));
            steps.extend(selected_scenario_steps());
            steps.push(step_vec(
                "ux-smoke",
                "Casual-user CLI smoke spec",
                "ux",
                "cli-ux",
                fact_sim_command(&["ux", "spec"]),
                true,
                false,
            ));
            steps.push(step_vec(
                "fault-spec",
                "Fault taxonomy contract",
                "faults",
                "recovery",
                fact_sim_command(&["fault", "spec"]),
                true,
                false,
            ));
            steps.push(step_vec(
                "benchmark-spec",
                "Benchmark framework contract",
                "benchmarks",
                "performance",
                fact_sim_command(&["benchmark", "spec"]),
                true,
                false,
            ));
        }
        CiTier::Main => {
            steps.extend(core_static_steps());
            steps.push(step(
                "workspace-tests",
                "Workspace tests",
                "unit",
                "correctness",
                ["cargo", "test", "--workspace", "--all-targets"],
                true,
                false,
            ));
            steps.push(step_vec(
                "smoke-suite",
                "Complete small smoke suite",
                "scenarios",
                "determinism",
                fact_sim_command(&["suite", "run", "scenarios/smoke"]),
                true,
                false,
            ));
            steps.extend(conflict_repair_steps());
            steps.push(step_vec(
                "fault-projection",
                "Projection fault profile",
                "faults",
                "recovery",
                fact_sim_command(&["fault", "run", "--profile", "faults-projection"]),
                true,
                false,
            ));
            steps.push(step_vec(
                "benchmark-fixtures",
                "Small/medium benchmark fixture readiness",
                "benchmarks",
                "performance",
                fact_sim_command(&[
                    "benchmark",
                    "fixtures",
                    "--base",
                    "fixtures",
                    "--require-ready",
                ]),
                true,
                false,
            ));
        }
        CiTier::Scheduled => {
            let profile = scheduled_profile.unwrap_or(LARGE_PROFILE);
            if args.include_large {
                steps.push(step_vec(
                    "scheduled-fixture-plan",
                    "Manual 500K scheduled fixture plan",
                    "fixtures",
                    "fixture-generation",
                    fact_sim_command(&["plan", "--profile", profile]),
                    true,
                    true,
                ));
            }
            let mut fixture_command = fact_sim_command(&["benchmark", "fixtures", "--base"]);
            fixture_command.push(args.fixture_base.display().to_string());
            fixture_command.push("--require-ready".to_string());
            if args.include_large {
                fixture_command.push("--include-large".to_string());
            }
            steps.push(step_vec(
                "scheduled-fixture-inventory",
                "Scheduled fixture inventory",
                "fixtures",
                "fixture-generation",
                fixture_command,
                true,
                args.include_large,
            ));
            let mut benchmark_plan = fact_sim_command(&["benchmark", "plan", "--fixture-base"]);
            benchmark_plan.push(args.fixture_base.display().to_string());
            benchmark_plan.push("--report-output".to_string());
            benchmark_plan.push(args.report_dir.display().to_string());
            if args.include_large {
                benchmark_plan.push("--include-large".to_string());
            }
            steps.push(step_vec(
                "scheduled-benchmark-plan",
                "Scheduled benchmark plan",
                "benchmarks",
                "performance",
                benchmark_plan,
                true,
                args.include_large,
            ));
            steps.push(step_vec(
                "fault-sampling",
                "Scheduled fault sampling",
                "faults",
                "recovery",
                fact_sim_command(&["fault", "spec"]),
                true,
                false,
            ));
        }
        CiTier::Release => {
            steps.extend(core_static_steps());
            steps.push(step(
                "workspace-tests",
                "Workspace tests",
                "unit",
                "correctness",
                ["cargo", "test", "--workspace", "--all-targets"],
                true,
                false,
            ));
            steps.extend(selected_scenario_steps());
            steps.extend(conflict_repair_steps());
            steps.push(step_vec(
                "ux-full-spec",
                "Full CLI UX contract",
                "ux",
                "cli-ux",
                fact_sim_command(&["ux", "spec"]),
                true,
                false,
            ));
            steps.push(step_vec(
                "benchmark-plan",
                "Release benchmark matrix plan",
                "benchmarks",
                "performance",
                fact_sim_command(&["benchmark", "plan"]),
                true,
                false,
            ));
            if args.include_large {
                steps.push(step_vec(
                    "release-large-plan",
                    "Manual 500K release fixture plan",
                    "fixtures",
                    "performance",
                    fact_sim_command(&["plan", "--profile", LARGE_PROFILE]),
                    true,
                    true,
                ));
            }
        }
        CiTier::Focused => unreachable!("focused plan is built separately"),
    }
    if args.baseline_summary.is_some() {
        steps.push(baseline_comparison_step(args));
    }
    validations.push(CiValidation {
        id: "ci-spec-schema".to_string(),
        name: "CI specification schema".to_string(),
        classification: "infrastructure".to_string(),
        required: true,
    });
    Ok(CiPlan {
        schema_version: CI_SCHEMA_VERSION.to_string(),
        tier,
        suite: None,
        include_large: args.include_large,
        fixture_base: args.fixture_base.clone(),
        report_dir: args.report_dir.clone(),
        steps,
        validations,
        artifact_policy: artifact_policy_for_tier(tier),
        cache_key_parts: ci_spec().fixture_cache_policy.key_parts,
        exact_command: format!("fact-sim ci {}", tier.slug()),
    })
}

fn focused_plan(suite: CiFocusedSuite, args: &CiRunArgs) -> Result<CiPlan> {
    let steps = match suite {
        CiFocusedSuite::OperationsSpec => Vec::new(),
        CiFocusedSuite::Ux => vec![step_vec(
            "focused-ux",
            "Focused UX suite",
            "ux",
            "cli-ux",
            fact_sim_command(&["ux", "spec"]),
            true,
            false,
        )],
        CiFocusedSuite::Sync => vec![step_vec(
            "focused-sync-scenario",
            "Focused sync scenario",
            "sync",
            "synchronization",
            fact_sim_command(&[
                "scenario",
                "run",
                "scenarios/repair/missing-dependency-retry.yaml",
            ]),
            true,
            false,
        )],
        CiFocusedSuite::Faults => vec![step_vec(
            "focused-faults",
            "Focused fault contract",
            "faults",
            "recovery",
            fact_sim_command(&["fault", "spec"]),
            true,
            false,
        )],
        CiFocusedSuite::Benchmarks => vec![step_vec(
            "focused-benchmarks",
            "Focused benchmark contract",
            "benchmarks",
            "performance",
            fact_sim_command(&["benchmark", "spec"]),
            true,
            false,
        )],
        CiFocusedSuite::Scenarios => selected_scenario_steps(),
        CiFocusedSuite::Docs => Vec::new(),
        CiFocusedSuite::CliReference => vec![step_vec(
            "focused-cli-help",
            "Focused CLI help contract",
            "cli-reference",
            "documentation",
            fact_sim_command(&["--help"]),
            true,
            false,
        )],
    };
    let validations = match suite {
        CiFocusedSuite::OperationsSpec => core_validations(),
        CiFocusedSuite::Docs => vec![CiValidation {
            id: "docs-links".to_string(),
            name: "Documentation cross references".to_string(),
            classification: "documentation".to_string(),
            required: true,
        }],
        CiFocusedSuite::CliReference => vec![CiValidation {
            id: "cli-operation-mapping".to_string(),
            name: "CLI operation mapping".to_string(),
            classification: "operations-spec-alignment".to_string(),
            required: true,
        }],
        _ => core_validations(),
    };
    Ok(CiPlan {
        schema_version: CI_SCHEMA_VERSION.to_string(),
        tier: CiTier::Focused,
        suite: Some(suite),
        include_large: false,
        fixture_base: args.fixture_base.clone(),
        report_dir: args.report_dir.clone(),
        steps,
        validations,
        artifact_policy: artifact_policy_for_tier(CiTier::Focused),
        cache_key_parts: ci_spec().fixture_cache_policy.key_parts,
        exact_command: format!("fact-sim ci test --suite {}", suite_slug(suite)),
    })
}

fn core_static_steps() -> Vec<CiStep> {
    vec![
        step(
            "fmt",
            "Formatting",
            "build",
            "documentation",
            ["cargo", "fmt", "--all", "--", "--check"],
            true,
            false,
        ),
        step(
            "clippy",
            "Clippy",
            "build",
            "correctness",
            [
                "cargo",
                "clippy",
                "--workspace",
                "--all-targets",
                "--all-features",
                "--",
                "-D",
                "warnings",
            ],
            true,
            false,
        ),
        step(
            "check",
            "Workspace compilation",
            "build",
            "correctness",
            ["cargo", "check", "--workspace", "--all-targets"],
            true,
            false,
        ),
    ]
}

fn selected_scenario_steps() -> Vec<CiStep> {
    vec![
        step_vec(
            "scenario-acceptance",
            "Initial proposition acceptance",
            "scenarios",
            "protocol-behavior",
            fact_sim_command(&[
                "scenario",
                "run",
                "scenarios/smoke/pending-revision-acceptance.yaml",
            ]),
            true,
            false,
        ),
        step_vec(
            "scenario-conflict",
            "Accepted sibling revision conflict",
            "scenarios",
            "operation-semantics",
            fact_sim_command(&[
                "scenario",
                "run",
                "scenarios/conflict/accepted-sibling-revisions.yaml",
            ]),
            true,
            false,
        ),
        step_vec(
            "scenario-reconciliation",
            "Reconciliation select scenario",
            "scenarios",
            "operation-semantics",
            fact_sim_command(&["scenario", "run", "scenarios/reconciliation/select.yaml"]),
            true,
            false,
        ),
    ]
}

fn conflict_repair_steps() -> Vec<CiStep> {
    vec![
        step_vec(
            "scenario-missing-dependency",
            "Missing dependency retry",
            "repair",
            "recovery",
            fact_sim_command(&[
                "scenario",
                "run",
                "scenarios/repair/missing-dependency-retry.yaml",
            ]),
            true,
            false,
        ),
        step_vec(
            "scenario-projection-repair",
            "Projection repair",
            "repair",
            "projection",
            fact_sim_command(&[
                "scenario",
                "run",
                "scenarios/repair/projection-corruption-rebuild.yaml",
            ]),
            true,
            false,
        ),
    ]
}

fn core_validations() -> Vec<CiValidation> {
    vec![
        CiValidation {
            id: "operations-registry-schema".to_string(),
            name: "Operations registry schema".to_string(),
            classification: "operations-spec-alignment".to_string(),
            required: true,
        },
        CiValidation {
            id: "cli-operation-mapping".to_string(),
            name: "CLI operation mapping".to_string(),
            classification: "operations-spec-alignment".to_string(),
            required: true,
        },
    ]
}

fn baseline_comparison_step(args: &CiRunArgs) -> CiStep {
    let mut command = fact_sim_command(&["benchmark", "compare-matrix"]);
    if let Some(thresholds) = &args.thresholds {
        command.push("--thresholds".to_string());
        command.push(thresholds.display().to_string());
    }
    command.push(
        args.baseline_summary
            .as_ref()
            .expect("baseline summary exists")
            .display()
            .to_string(),
    );
    command.push(
        args.report_dir
            .join("warm-baseline-summary.json")
            .display()
            .to_string(),
    );
    step_vec(
        "benchmark-comparison",
        "Benchmark baseline comparison",
        "benchmarks",
        "performance",
        command,
        false,
        args.include_large,
    )
}

fn fact_sim_command(args: &[&str]) -> Vec<String> {
    ["cargo", "run", "-p", "fact-sim-cli", "--"]
        .into_iter()
        .chain(args.iter().copied())
        .map(str::to_string)
        .collect()
}

fn step<const N: usize>(
    id: &str,
    name: &str,
    suite: &str,
    classification: &str,
    command: [&str; N],
    required: bool,
    large_scale: bool,
) -> CiStep {
    step_vec(
        id,
        name,
        suite,
        classification,
        command.iter().map(|part| (*part).to_string()).collect(),
        required,
        large_scale,
    )
}

fn step_vec(
    id: &str,
    name: &str,
    suite: &str,
    classification: &str,
    command: Vec<String>,
    required: bool,
    large_scale: bool,
) -> CiStep {
    CiStep {
        id: id.to_string(),
        name: name.to_string(),
        suite: suite.to_string(),
        classification: classification.to_string(),
        reproduction_command: command.join(" "),
        command,
        required,
        large_scale,
        artifact_paths: vec![PathBuf::from(DEFAULT_REPORT_DIR)],
    }
}

fn validate_operations_registry() -> Result<Value> {
    let registry_path = repo_root().join("docs/operations/registry.json");
    let registry: Value = serde_json::from_slice(
        &fs::read(&registry_path)
            .with_context(|| format!("reading {}", registry_path.display()))?,
    )?;
    let required_fields = [
        "name",
        "version",
        "purpose",
        "inputs",
        "optional_inputs",
        "inferred_context",
        "explicit_context",
        "preconditions",
        "authorization",
        "validation_steps",
        "reads",
        "creates",
        "causal_references",
        "atomicity",
        "projection_effects",
        "effective_state_effects",
        "idempotency",
        "retry",
        "partial_success",
        "failure_classes",
        "observable_result",
        "mappings",
        "conformance",
    ];
    let operations = registry["operations"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut names = BTreeSet::new();
    let mut duplicate_names = Vec::new();
    let mut missing_required_fields = Vec::new();
    let mut missing_mappings = Vec::new();
    for operation in &operations {
        let name = operation["name"]
            .as_str()
            .unwrap_or("<missing>")
            .to_string();
        if !names.insert(name.clone()) {
            duplicate_names.push(name.clone());
        }
        let missing = required_fields
            .iter()
            .filter(|field| operation.get(**field).is_none())
            .map(|field| (*field).to_string())
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            missing_required_fields.push(serde_json::json!({
                "operation": name,
                "missing": missing,
            }));
        }
        let mapping = &operation["mappings"];
        let missing = ["sdk", "cli", "http", "simulator"]
            .iter()
            .filter(|field| mapping.get(**field).is_none())
            .map(|field| (*field).to_string())
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            missing_mappings.push(serde_json::json!({
                "operation": name,
                "missing": missing,
            }));
        }
    }
    let valid = registry["schema_version"] == "facts-operations-registry-v0"
        && registry["large_scale_policy"]["large_500k"]
            == "manual-opt-in-performance-configuration"
        && !operations.is_empty()
        && duplicate_names.is_empty()
        && missing_required_fields.is_empty()
        && missing_mappings.is_empty();
    Ok(serde_json::json!({
        "valid": valid,
        "operation_count": operations.len(),
        "duplicate_names": duplicate_names,
        "missing_required_fields": missing_required_fields,
        "missing_mappings": missing_mappings,
        "large_scale_policy": registry["large_scale_policy"],
    }))
}

fn validate_cli_operation_mapping() -> Result<Value> {
    let root = repo_root();
    let registry: Value =
        serde_json::from_slice(&fs::read(root.join("docs/operations/registry.json"))?)?;
    let empty = Vec::new();
    let known_operations = registry["operations"]
        .as_array()
        .unwrap_or(&empty)
        .iter()
        .filter_map(|operation| operation["name"].as_str())
        .collect::<BTreeSet<_>>();
    let mapping = fs::read_to_string(root.join("docs/operations/interface-mapping.md"))?;
    let missing_from_mapping = known_operations
        .iter()
        .filter(|operation| !mapping.contains(**operation))
        .map(|operation| (*operation).to_string())
        .collect::<Vec<_>>();
    let required_cli_commands = [
        "fact propose",
        "fact revise",
        "fact accept",
        "fact reject",
        "fact invite",
        "fact join",
        "fact leave",
        "fact comment",
        "fact archive",
        "fact withdraw",
        "fact push",
        "fact pull",
        "fact list",
        "fact search",
        "fact find",
    ];
    let missing_cli_commands = required_cli_commands
        .iter()
        .filter(|command| !mapping.contains(**command))
        .map(|command| (*command).to_string())
        .collect::<Vec<_>>();
    let known_missing_commands_tracked =
        mapping.contains("`fact read` is missing") && mapping.contains("`fact write` is missing");
    Ok(serde_json::json!({
        "valid": missing_from_mapping.is_empty() && missing_cli_commands.is_empty() && known_missing_commands_tracked,
        "mapped_operations": known_operations.len(),
        "missing_operations": missing_from_mapping,
        "missing_cli_commands": missing_cli_commands,
        "known_missing_commands_tracked": known_missing_commands_tracked,
    }))
}

fn validate_docs_links() -> Result<Value> {
    let root = repo_root();
    let required = [
        "docs/ci.md",
        "docs/operations/00-introduction.md",
        "docs/operations/registry.json",
        "docs/operations/interface-mapping.md",
        "docs/operations/conformance-mapping.md",
    ];
    let missing = required
        .iter()
        .filter(|path| !root.join(path).exists())
        .map(|path| (*path).to_string())
        .collect::<Vec<_>>();
    Ok(serde_json::json!({
        "valid": missing.is_empty(),
        "missing": missing,
    }))
}

fn step_failure(step: &CiStep, _report: &CiStepReport) -> CiFailure {
    CiFailure {
        classification: step.classification.clone(),
        suite: step.suite.clone(),
        scenario: scenario_from_command(&step.command),
        seed: seed_from_command(&step.command),
        fixture_profile: profile_from_command(&step.command),
        fixture_manifest_digest: None,
        exact_command: step.command.join(" "),
        reproduction_command: step.reproduction_command.clone(),
        artifact_references: step.artifact_paths.clone(),
        minimized_reproduction: scenario_from_command(&step.command)
            .unwrap_or_else(|| step.reproduction_command.clone()),
    }
}

fn validation_failure(report: &CiValidationReport) -> CiFailure {
    CiFailure {
        classification: report.validation.classification.clone(),
        suite: report.validation.name.clone(),
        scenario: None,
        seed: None,
        fixture_profile: None,
        fixture_manifest_digest: None,
        exact_command: format!("fact-sim ci test --suite {}", report.validation.id),
        reproduction_command: format!("fact-sim ci test --suite {}", report.validation.id),
        artifact_references: vec![PathBuf::from(DEFAULT_REPORT_DIR)],
        minimized_reproduction: report.validation.id.clone(),
    }
}

fn summarize(
    steps: &[CiStepReport],
    validations: &[CiValidationReport],
    failures: &[CiFailure],
) -> CiSummary {
    let passed_steps = steps
        .iter()
        .filter(|step| step.status == CiStatus::Pass)
        .count();
    let failed_steps = steps
        .iter()
        .filter(|step| step.status == CiStatus::Fail)
        .count();
    let warning_steps = steps
        .iter()
        .filter(|step| step.status == CiStatus::Warning)
        .count();
    let passed_validations = validations
        .iter()
        .filter(|validation| validation.status == CiStatus::Pass)
        .count();
    let failed_validations = validations
        .iter()
        .filter(|validation| validation.status == CiStatus::Fail)
        .count();
    let first_failure = failures
        .first()
        .map(|failure| failure.reproduction_command.clone());
    let human_summary = if failures.is_empty() {
        format!(
            "{passed_steps} steps and {passed_validations} validations passed; no failures recorded"
        )
    } else {
        format!(
            "{failed_steps} steps and {failed_validations} validations failed; first reproduction command: {}",
            first_failure.as_deref().unwrap_or("unavailable")
        )
    };
    CiSummary {
        passed_steps,
        failed_steps,
        warning_steps,
        passed_validations,
        failed_validations,
        first_failure,
        human_summary,
    }
}

fn render_summary(path: &Path) -> Result<String> {
    let report: CiRunReport = serde_json::from_slice(&fs::read(path)?)?;
    let mut lines = Vec::new();
    lines.push(format!("# CI Summary: {}", report.tier.slug()));
    lines.push(String::new());
    lines.push(format!("- Status: {:?}", report.status));
    lines.push(format!("- Dry run: {}", report.dry_run));
    lines.push(format!("- Large 500K included: {}", report.include_large));
    lines.push(format!(
        "- Steps: {} passed, {} failed, {} warnings",
        report.summary.passed_steps, report.summary.failed_steps, report.summary.warning_steps
    ));
    lines.push(format!(
        "- Validations: {} passed, {} failed",
        report.summary.passed_validations, report.summary.failed_validations
    ));
    if let Some(command) = report.summary.first_failure {
        lines.push(format!("- First reproduction command: `{command}`"));
    }
    lines.push(format!(
        "- Simulator commit: `{}`",
        report.source_revisions.simulator_commit
    ));
    lines.push(format!(
        "- Facts commit: `{}`",
        report.source_revisions.facts_source_commit
    ));
    lines.push(format!(
        "- Operations spec: `{}`",
        report.operations_spec_version
    ));
    Ok(lines.join("\n"))
}

fn ci_spec() -> CiSpec {
    CiSpec {
        schema_version: CI_SCHEMA_VERSION.to_string(),
        tiers: vec![
            tier_spec(
                CiTier::Local,
                "developer-invoked",
                "fast-test",
                false,
                ["fact-sim ci local"],
                [
                    "formatting",
                    "linting",
                    "compilation",
                    "unit tests",
                    "small deterministic scenarios",
                    "operations registry",
                    "CLI UX smoke",
                ],
            ),
            tier_spec(
                CiTier::PullRequest,
                "pull_request",
                "fast-test",
                false,
                ["fact-sim ci pull-request"],
                [
                    "all local checks",
                    "selected sync/conflict/recovery scenarios",
                    "operations mapping",
                    "small performance contracts",
                ],
            ),
            tier_spec(
                CiTier::Main,
                "push-to-main",
                "medium-test",
                false,
                ["fact-sim ci main"],
                [
                    "pull-request checks",
                    "complete small suite",
                    "medium fixture readiness",
                    "fault sampling",
                    "projection repair",
                ],
            ),
            tier_spec(
                CiTier::Scheduled,
                "schedule-or-manual-dispatch",
                "scale-test",
                false,
                ["fact-sim ci scheduled", "fact-sim ci scheduled --include-large"],
                [
                    "fixture inventory",
                    "benchmark plan",
                    "fault sampling",
                    "manual 500K validation when --include-large is provided",
                ],
            ),
            tier_spec(
                CiTier::Release,
                "tag-or-manual-dispatch",
                "release",
                false,
                ["fact-sim ci release", "fact-sim ci release --include-large"],
                [
                    "main checks",
                    "release benchmark plan",
                    "operations alignment",
                    "manual 500K release plan when --include-large is provided",
                ],
            ),
        ],
        focused_suites: focused_suite_specs(),
        fixture_cache_policy: FixtureCachePolicy {
            key_parts: vec![
                "fixture_profile".to_string(),
                "profile_version".to_string(),
                "seed".to_string(),
                "start_time".to_string(),
                "simulator_commit".to_string(),
                "facts_sdk_commit".to_string(),
                "operations_spec_version".to_string(),
                "scenario_corpus_version".to_string(),
                "content_template_version".to_string(),
                "platform".to_string(),
            ],
            invalidation_rule:
                "Do not reuse a cached fixture when any key part changes; every cache entry must include a verified manifest and commitment"
                    .to_string(),
        },
        artifact_retention_policy: ArtifactRetentionPolicy {
            pull_request: vec![
                "CI summary JSON and Markdown".to_string(),
                "failing scenario report".to_string(),
                "CLI transcript with redaction".to_string(),
                "operations-spec mismatch report".to_string(),
                "test logs".to_string(),
            ],
            scheduled_scale: vec![
                "run manifest".to_string(),
                "fixture manifest".to_string(),
                "object distribution report".to_string(),
                "benchmark report".to_string(),
                "environment manifest".to_string(),
                "projection verification report".to_string(),
                "UX sample report".to_string(),
                "fault report".to_string(),
                "commitment roots".to_string(),
                "failure reproduction commands".to_string(),
            ],
            release: vec![
                "release validation report".to_string(),
                "operations specification".to_string(),
                "registry".to_string(),
                "command reference".to_string(),
                "benchmark summary".to_string(),
                "checksums".to_string(),
            ],
        },
        report_schemas: vec![
            report_schema("ci-summary"),
            report_schema("scenario-failure"),
            report_schema("determinism"),
            report_schema("fixture-verification"),
            report_schema("benchmark-result"),
            report_schema("performance-comparison"),
            report_schema("ux-defects"),
            report_schema("fault-recovery"),
            report_schema("operations-spec-alignment"),
        ],
        regression_classes: [
            "protocol-behavior",
            "operation-semantics",
            "projection",
            "synchronization",
            "recovery",
            "cli-ux",
            "performance",
            "determinism",
            "fixture-generation",
            "documentation",
            "operations-spec-alignment",
            "infrastructure",
        ]
        .iter()
        .map(|value| (*value).to_string())
        .collect(),
        runner_classes: vec![
            runner("fast-test", "macOS and Linux", ["build", "unit", "smoke"], false),
            runner(
                "medium-test",
                "macOS and Linux",
                ["small fixtures", "medium fixture readiness", "fault sampling"],
                false,
            ),
            runner(
                "scale-test",
                "self-hosted stable macOS benchmark runner",
                ["manual large fixtures", "benchmarks", "fixture integrity"],
                true,
            ),
            runner(
                "release",
                "macOS and Linux plus benchmark runner",
                ["release validation", "cross-platform builds"],
                true,
            ),
        ],
        flaky_test_policy: flaky_test_policy(),
        redaction_policy: redaction_policy(),
        dependency_controls: vec![
            "CI checks out the Facts SDK repository as ../sdk or sets FACT_SOURCE_DIR before building".to_string(),
            "Reports record simulator, Facts, SDK, operations-spec, fixture-profile, scenario-corpus, and Rust toolchain revisions".to_string(),
            "A green CI report is invalid if source revisions are unknown".to_string(),
        ],
        large_scale_policy: LargeScalePolicy {
            target_propositions: 500_000,
            default_tiers: vec![
                "local".to_string(),
                "pull-request".to_string(),
                "main".to_string(),
            ],
            manual_opt_in_flag: "--include-large".to_string(),
            scheduled_behavior: "Scheduled workflows run small/medium automation by default; 500K validation requires manual dispatch with include_large=true".to_string(),
        },
    }
}

fn tier_spec<const C: usize, const S: usize>(
    tier: CiTier,
    frequency: &str,
    runner_class: &str,
    includes_large_500k: bool,
    commands: [&str; C],
    suites: [&str; S],
) -> CiTierSpec {
    CiTierSpec {
        tier,
        frequency: frequency.to_string(),
        runner_class: runner_class.to_string(),
        includes_large_500k,
        commands: commands.iter().map(|value| (*value).to_string()).collect(),
        suites: suites.iter().map(|value| (*value).to_string()).collect(),
        artifacts: artifact_policy_for_tier(tier),
    }
}

fn focused_suite_specs() -> Vec<FocusedSuiteSpec> {
    [
        (
            CiFocusedSuite::OperationsSpec,
            vec!["fact-sim ci test --suite operations-spec"],
            vec![
                "operations registry schema",
                "SDK/CLI/HTTP/simulator mappings",
                "required failure classes",
            ],
        ),
        (
            CiFocusedSuite::Ux,
            vec!["fact-sim ci test --suite ux"],
            vec!["CLI UX smoke", "human/JSON consistency", "help contract"],
        ),
        (
            CiFocusedSuite::Sync,
            vec!["fact-sim ci test --suite sync"],
            vec!["missing dependency retry", "convergence scenario"],
        ),
        (
            CiFocusedSuite::Faults,
            vec!["fact-sim ci test --suite faults"],
            vec!["fault taxonomy", "replay metadata"],
        ),
        (
            CiFocusedSuite::Benchmarks,
            vec!["fact-sim ci test --suite benchmarks"],
            vec!["benchmark spec", "baseline report contract"],
        ),
        (
            CiFocusedSuite::Scenarios,
            vec!["fact-sim ci test --suite scenarios"],
            vec!["selected permanent regression scenarios"],
        ),
        (
            CiFocusedSuite::Docs,
            vec!["fact-sim ci test --suite docs"],
            vec!["documentation cross references"],
        ),
        (
            CiFocusedSuite::CliReference,
            vec!["fact-sim ci test --suite cli-reference"],
            vec!["CLI command mapping", "missing read/write tracked"],
        ),
    ]
    .into_iter()
    .map(|(suite, commands, validates)| FocusedSuiteSpec {
        suite,
        commands: commands.into_iter().map(str::to_string).collect(),
        validates: validates.into_iter().map(str::to_string).collect(),
    })
    .collect()
}

fn report_schema(name: &str) -> ReportSchemaSpec {
    ReportSchemaSpec {
        name: name.to_string(),
        required_fields: [
            "schema_version",
            "suite_version",
            "source_revisions",
            "environment",
            "started_at_unix",
            "ended_at_unix",
            "status",
            "artifact_references",
            "reproduction_command",
        ]
        .iter()
        .map(|value| (*value).to_string())
        .collect(),
    }
}

fn runner<const N: usize>(
    class: &str,
    operating_system: &str,
    intended_suites: [&str; N],
    large_scale_allowed: bool,
) -> RunnerClassSpec {
    RunnerClassSpec {
        class: class.to_string(),
        operating_system: operating_system.to_string(),
        intended_suites: intended_suites
            .iter()
            .map(|value| (*value).to_string())
            .collect(),
        large_scale_allowed,
    }
}

fn artifact_policy_for_tier(tier: CiTier) -> Vec<String> {
    match tier {
        CiTier::Local | CiTier::Focused => vec![
            "ci summary".to_string(),
            "first failing command".to_string(),
            "operations-spec mismatch report".to_string(),
        ],
        CiTier::PullRequest => vec![
            "CI summary JSON and Markdown".to_string(),
            "failing scenario report".to_string(),
            "CLI transcript with redaction".to_string(),
            "operations-spec mismatch report".to_string(),
            "test logs".to_string(),
        ],
        CiTier::Main => vec![
            "ci summary".to_string(),
            "scenario reports".to_string(),
            "fault reports".to_string(),
            "fixture inventory".to_string(),
            "benchmark comparison".to_string(),
        ],
        CiTier::Scheduled => vec![
            "run manifest".to_string(),
            "fixture manifest".to_string(),
            "object distribution report".to_string(),
            "benchmark report".to_string(),
            "environment manifest".to_string(),
            "projection verification report".to_string(),
            "UX sample report".to_string(),
            "fault report".to_string(),
            "commitment roots".to_string(),
            "failure reproduction commands".to_string(),
        ],
        CiTier::Release => vec![
            "release validation report".to_string(),
            "operations specification".to_string(),
            "registry".to_string(),
            "command reference".to_string(),
            "benchmark summary".to_string(),
            "checksums".to_string(),
        ],
    }
}

fn flaky_test_policy() -> Vec<String> {
    vec![
        "Retries may gather evidence but must not hide instability".to_string(),
        "Nondeterministic behavior is classified separately from correctness failures".to_string(),
        "Reports must record seed, schedule, suite, platform, and exact command".to_string(),
        "Quarantine requires a tracked issue and does not count as a green validation".to_string(),
    ]
}

fn redaction_policy() -> Vec<String> {
    vec![
        "redact signing seeds".to_string(),
        "redact private keys".to_string(),
        "redact authentication tokens".to_string(),
        "redact private remote URLs".to_string(),
        "redact sensitive local paths outside the workspace".to_string(),
    ]
}

fn ci_environment() -> BTreeMap<String, String> {
    let mut environment = BTreeMap::new();
    for key in ["CI", "GITHUB_ACTIONS", "RUNNER_OS", "RUNNER_ARCH"] {
        if let Ok(value) = std::env::var(key) {
            environment.insert(key.to_string(), redact(&value));
        }
    }
    environment.insert(
        "current_dir".to_string(),
        std::env::current_dir()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|_| "unknown".to_string()),
    );
    environment
}

fn source_revisions() -> Result<SourceRevisions> {
    let operations_spec_version = operations_spec_version()?;
    Ok(SourceRevisions {
        simulator_commit: git_rev_parse(&repo_root()),
        facts_source_commit: facts_git_commit(),
        fact_sdk_commit: facts_git_commit(),
        operations_spec_version,
        scenario_corpus_version: digest_paths(&repo_root().join("scenarios"), "yaml")?,
        fixture_profile_version: digest_paths(&repo_root().join("docs"), "md")?,
        rust_toolchain: rust_toolchain(),
    })
}

fn facts_git_commit() -> String {
    std::env::var("FACT_SOURCE_DIR")
        .ok()
        .map(PathBuf::from)
        .or_else(|| Some(PathBuf::from("../sdk")))
        .map(|path| git_rev_parse(&path))
        .unwrap_or_else(|| "unknown".to_string())
}

fn git_rev_parse(path: &Path) -> String {
    Command::new("git")
        .arg("-C")
        .arg(path)
        .arg("rev-parse")
        .arg("HEAD")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn rust_toolchain() -> String {
    Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn operations_spec_version() -> Result<String> {
    let registry: Value = serde_json::from_slice(&fs::read(
        repo_root().join("docs/operations/registry.json"),
    )?)?;
    Ok(registry["spec_version"]
        .as_str()
        .unwrap_or("unknown")
        .to_string())
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn fixture_cache_key(plan: &CiPlan) -> Result<String> {
    let source = source_revisions()?;
    let payload = serde_json::json!({
        "tier": plan.tier,
        "suite": plan.suite,
        "include_large": plan.include_large,
        "fixture_base": plan.fixture_base,
        "simulator_commit": source.simulator_commit,
        "facts_sdk_commit": source.fact_sdk_commit,
        "operations_spec_version": source.operations_spec_version,
        "scenario_corpus_version": source.scenario_corpus_version,
        "fixture_profile_version": source.fixture_profile_version,
        "platform": std::env::consts::OS,
    });
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&payload)?)
    ))
}

fn digest_paths(base: &Path, extension: &str) -> Result<String> {
    let mut files = Vec::new();
    collect_files(base, extension, &mut files)?;
    let mut hasher = Sha256::new();
    for path in files {
        hasher.update(path.display().to_string());
        hasher.update(fs::read(path)?);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn collect_files(base: &Path, extension: &str, files: &mut Vec<PathBuf>) -> Result<()> {
    if !base.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(base)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_files(&path, extension, files)?;
        } else if path.extension().and_then(|value| value.to_str()) == Some(extension) {
            files.push(path);
        }
    }
    files.sort();
    Ok(())
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn redact(input: &str) -> String {
    input
        .split_whitespace()
        .map(|token| {
            for marker in ["TOKEN=", "SECRET=", "PRIVATE_KEY=", "SIGNING_SEED="] {
                if token.starts_with(marker) {
                    return format!("{marker}[REDACTED]");
                }
            }
            token.to_string()
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn scenario_from_command(command: &[String]) -> Option<String> {
    command
        .windows(2)
        .find(|window| window[0] == "run" || window[0] == "validate")
        .map(|window| window[1].clone())
}

fn seed_from_command(command: &[String]) -> Option<u64> {
    command
        .windows(2)
        .find(|window| window[0] == "--seed")
        .and_then(|window| window[1].parse().ok())
}

fn profile_from_command(command: &[String]) -> Option<String> {
    command
        .windows(2)
        .find(|window| window[0] == "--profile")
        .map(|window| window[1].clone())
}

fn suite_slug(suite: CiFocusedSuite) -> &'static str {
    match suite {
        CiFocusedSuite::OperationsSpec => "operations-spec",
        CiFocusedSuite::Ux => "ux",
        CiFocusedSuite::Sync => "sync",
        CiFocusedSuite::Faults => "faults",
        CiFocusedSuite::Benchmarks => "benchmarks",
        CiFocusedSuite::Scenarios => "scenarios",
        CiFocusedSuite::Docs => "docs",
        CiFocusedSuite::CliReference => "cli-reference",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ci_spec_keeps_large_manual_opt_in() {
        let spec = ci_spec();
        assert_eq!(spec.schema_version, CI_SCHEMA_VERSION);
        assert_eq!(
            spec.large_scale_policy.scheduled_behavior,
            "Scheduled workflows run small/medium automation by default; 500K validation requires manual dispatch with include_large=true"
        );
        assert_eq!(
            spec.large_scale_policy.manual_opt_in_flag,
            "--include-large"
        );
        assert!(
            spec.tiers
                .iter()
                .filter(|tier| matches!(
                    tier.tier,
                    CiTier::Local | CiTier::PullRequest | CiTier::Main
                ))
                .all(|tier| !tier.includes_large_500k)
        );
    }

    #[test]
    fn operations_registry_validation_rejects_missing_contract_fields() -> Result<()> {
        let validation = validate_operations_registry()?;
        assert_eq!(validation["valid"], true);
        assert_eq!(validation["operation_count"], 36);
        assert_eq!(
            validation["missing_required_fields"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        Ok(())
    }

    #[test]
    fn pull_request_plan_includes_required_fast_suites() -> Result<()> {
        let plan = tier_plan(
            CiTier::PullRequest,
            &CiRunArgs {
                dry_run: true,
                report_dir: PathBuf::from(DEFAULT_REPORT_DIR),
                output: None,
                fixture_base: PathBuf::from("fixtures"),
                baseline_summary: None,
                thresholds: None,
                fact_binary: None,
                include_large: false,
            },
            None,
        )?;
        let ids = plan
            .steps
            .iter()
            .map(|step| step.id.as_str())
            .collect::<BTreeSet<_>>();
        for required in [
            "fmt",
            "clippy",
            "check",
            "workspace-tests",
            "scenario-acceptance",
            "scenario-conflict",
            "scenario-reconciliation",
            "ux-smoke",
        ] {
            assert!(ids.contains(required), "missing {required}");
        }
        assert!(!plan.include_large);
        Ok(())
    }

    #[test]
    fn main_plan_requires_fixture_readiness() -> Result<()> {
        let plan = tier_plan(
            CiTier::Main,
            &CiRunArgs {
                dry_run: true,
                report_dir: PathBuf::from(DEFAULT_REPORT_DIR),
                output: None,
                fixture_base: PathBuf::from("fixtures"),
                baseline_summary: None,
                thresholds: None,
                fact_binary: None,
                include_large: false,
            },
            None,
        )?;
        let fixture_step = plan
            .steps
            .iter()
            .find(|step| step.id == "benchmark-fixtures")
            .expect("benchmark fixture step");
        assert!(
            fixture_step
                .command
                .contains(&"--require-ready".to_string())
        );
        Ok(())
    }

    #[test]
    fn scheduled_default_does_not_plan_large_profile() -> Result<()> {
        let plan = tier_plan(
            CiTier::Scheduled,
            &CiRunArgs {
                dry_run: true,
                report_dir: PathBuf::from(DEFAULT_REPORT_DIR),
                output: None,
                fixture_base: PathBuf::from("fixtures"),
                baseline_summary: None,
                thresholds: None,
                fact_binary: None,
                include_large: false,
            },
            Some(LARGE_PROFILE),
        )?;
        assert!(!plan.include_large);
        assert!(plan.steps.iter().all(|step| !step.large_scale));
        assert!(
            plan.steps
                .iter()
                .all(|step| !step.command.contains(&"--include-large".to_string()))
        );
        assert!(
            plan.steps
                .iter()
                .all(|step| !step.command.contains(&LARGE_PROFILE.to_string()))
        );
        Ok(())
    }

    #[test]
    fn pull_request_rejects_large_opt_in() {
        let error = run_tier(
            CiTier::PullRequest,
            CiRunArgs {
                dry_run: true,
                report_dir: PathBuf::from(DEFAULT_REPORT_DIR),
                output: None,
                fixture_base: PathBuf::from("fixtures"),
                baseline_summary: None,
                thresholds: None,
                fact_binary: None,
                include_large: true,
            },
            None,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("500K validation is manual opt-in")
        );
    }

    #[test]
    fn redaction_removes_sensitive_values() {
        assert_eq!(
            redact("TOKEN=abc SECRET=def ok"),
            "TOKEN=[REDACTED] SECRET=[REDACTED] ok"
        );
    }
}
