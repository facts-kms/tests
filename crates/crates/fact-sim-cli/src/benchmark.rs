use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use axum::body::{Body, to_bytes};
use clap::{Args, Subcommand, ValueEnum};
use http::Request;
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tower::ServiceExt;

const REPORT_SCHEMA_VERSION: &str = "benchmark-v1";
const RELEASE_FACT_SIM_COMMAND: &str = "./target/release/fact-sim";
const REQUIRED_BENCHMARK_LEVELS: [&str; 2] = ["small", "medium"];
const OPTIONAL_BENCHMARK_LEVELS: [&str; 1] = ["large"];
const REQUIRED_BENCHMARK_PROFILES: [&str; 6] = [
    "scale-500k-balanced",
    "scale-500k-proposition-heavy",
    "scale-500k-revision-heavy",
    "scale-500k-deliberation-heavy",
    "scale-500k-sync-heavy",
    "scale-500k-conflict-heavy",
];
const MIN_READY_BENCHMARK_ITERATIONS: usize = 2;

type BenchmarkIdentityKey = (
    PathBuf,
    String,
    String,
    Option<u64>,
    BenchmarkSuite,
    String,
    String,
);
type SnapshotFrameObjects = (uuid::Uuid, Vec<(fact_core::Hash, Vec<u8>)>);

#[derive(Debug, Subcommand)]
pub enum BenchmarkCommand {
    Spec,
    Run(BenchmarkRunArgs),
    Baseline(BenchmarkBaselineArgs),
    Plan(BenchmarkPlanArgs),
    Audit(BenchmarkAuditArgs),
    Analyze(BenchmarkAnalyzeArgs),
    Budgets(BenchmarkBudgetsArgs),
    CheckBudgets(BenchmarkCheckBudgetsArgs),
    ProfilePlan(BenchmarkProfilePlanArgs),
    Accept(BenchmarkAcceptArgs),
    Compare(BenchmarkCompareArgs),
    CompareMatrix(BenchmarkCompareMatrixArgs),
    Report(BenchmarkReportArgs),
    Fixtures(BenchmarkFixturesArgs),
}

#[derive(Debug, Args)]
pub struct BenchmarkRunArgs {
    #[arg(long, value_enum)]
    suite: BenchmarkSuite,
    #[arg(long)]
    fixture: PathBuf,
    #[arg(long)]
    output: Option<PathBuf>,
    #[arg(long, default_value_t = 10)]
    iterations: usize,
    #[arg(long, default_value_t = 2)]
    warmups: usize,
    #[arg(long, default_value = "warm-filesystem")]
    cache_state: String,
}

#[derive(Debug, Args)]
pub struct BenchmarkBaselineArgs {
    #[arg(long, value_enum, default_value_t = BenchmarkSuite::Full)]
    suite: BenchmarkSuite,
    #[arg(long, default_value = "fixtures")]
    base: PathBuf,
    #[arg(long, default_value = "reports/benchmarks")]
    output: PathBuf,
    #[arg(long, default_value_t = 10)]
    iterations: usize,
    #[arg(long, default_value_t = 2)]
    warmups: usize,
    #[arg(long, default_value = "warm-filesystem")]
    cache_state: String,
    #[arg(long)]
    require_ready: bool,
    #[arg(long)]
    include_large: bool,
}

#[derive(Debug, Args)]
pub struct BenchmarkPlanArgs {
    #[arg(long, value_enum, default_value_t = BenchmarkSuite::Full)]
    suite: BenchmarkSuite,
    #[arg(long, default_value = "fixtures/benchmark-matrix")]
    fixture_base: PathBuf,
    #[arg(long, default_value = "reports/benchmarks")]
    report_output: PathBuf,
    #[arg(long, default_value_t = 42)]
    seed: u64,
    #[arg(long, default_value_t = 30)]
    iterations: usize,
    #[arg(long, default_value_t = 5)]
    warmups: usize,
    #[arg(long, default_value = "warm-filesystem")]
    cache_state: String,
    #[arg(long)]
    include_large: bool,
}

#[derive(Debug, Args)]
pub struct BenchmarkAuditArgs {
    #[arg(long, default_value = "fixtures")]
    base: PathBuf,
    #[arg(long = "baseline-summary")]
    baseline_summaries: Vec<PathBuf>,
    #[arg(long)]
    include_large: bool,
}

#[derive(Debug, Args)]
pub struct BenchmarkAnalyzeArgs {
    #[arg(long = "baseline-summary")]
    baseline_summaries: Vec<PathBuf>,
}

#[derive(Debug, Args)]
pub struct BenchmarkBudgetsArgs {
    #[arg(long = "baseline-summary")]
    baseline_summaries: Vec<PathBuf>,
    #[arg(long, default_value_t = 1.25)]
    warning_multiplier: f64,
    #[arg(long, default_value_t = 1.50)]
    regression_multiplier: f64,
    #[arg(long, default_value_t = 1.0)]
    minimum_warning_ms: f64,
    #[arg(long, default_value_t = 5.0)]
    minimum_regression_ms: f64,
}

#[derive(Debug, Args)]
pub struct BenchmarkCheckBudgetsArgs {
    #[arg(long)]
    budgets: PathBuf,
    #[arg(long = "baseline-summary")]
    baseline_summary: PathBuf,
}

#[derive(Debug, Args)]
pub struct BenchmarkProfilePlanArgs {
    #[arg(long = "baseline-summary")]
    baseline_summaries: Vec<PathBuf>,
    #[arg(long, default_value_t = 10)]
    limit: usize,
}

#[derive(Debug, Args)]
pub struct BenchmarkAcceptArgs {
    #[arg(long)]
    audit: PathBuf,
    #[arg(long = "growth-analysis")]
    growth_analysis: PathBuf,
    #[arg(long = "budget-check")]
    budget_check: PathBuf,
    #[arg(long = "profile-plan")]
    profile_plan: PathBuf,
}

#[derive(Debug, Args)]
pub struct BenchmarkCompareArgs {
    baseline: PathBuf,
    current: PathBuf,
    #[arg(long)]
    thresholds: Option<PathBuf>,
    #[arg(long, default_value_t = 5.0)]
    warning_threshold_percent: f64,
    #[arg(long, default_value_t = 15.0)]
    regression_threshold_percent: f64,
}

#[derive(Debug, Args)]
pub struct BenchmarkCompareMatrixArgs {
    baseline_summary: PathBuf,
    current_summary: PathBuf,
    #[arg(long)]
    thresholds: Option<PathBuf>,
    #[arg(long, default_value_t = 5.0)]
    warning_threshold_percent: f64,
    #[arg(long, default_value_t = 15.0)]
    regression_threshold_percent: f64,
}

#[derive(Debug, Args)]
pub struct BenchmarkReportArgs {
    report: PathBuf,
}

#[derive(Debug, Args)]
pub struct BenchmarkFixturesArgs {
    #[arg(long, default_value = "fixtures")]
    base: PathBuf,
    #[arg(long)]
    include_large: bool,
    #[arg(long)]
    require_ready: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum BenchmarkSuite {
    Core,
    Read,
    Search,
    Sync,
    Rebuild,
    Integrity,
    Cli,
    Conflict,
    Http,
    Full,
}

pub fn execute(command: BenchmarkCommand) -> Result<String> {
    match command {
        BenchmarkCommand::Spec => Ok(serde_json::to_string_pretty(&benchmark_spec())?),
        BenchmarkCommand::Run(args) => {
            let report = run_benchmarks(&args)?;
            let output = serde_json::to_string_pretty(&report)?;
            if let Some(path) = args.output {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)
                        .with_context(|| format!("failed to create `{}`", parent.display()))?;
                }
                std::fs::write(&path, output.as_bytes())
                    .with_context(|| format!("failed to write `{}`", path.display()))?;
            }
            Ok(output)
        }
        BenchmarkCommand::Baseline(args) => {
            let summary = run_baseline_matrix(&args)?;
            Ok(serde_json::to_string_pretty(&summary)?)
        }
        BenchmarkCommand::Plan(args) => {
            let plan = benchmark_matrix_plan(&args)?;
            Ok(serde_json::to_string_pretty(&plan)?)
        }
        BenchmarkCommand::Audit(args) => {
            let audit = audit_benchmark_readiness(&args)?;
            Ok(serde_json::to_string_pretty(&audit)?)
        }
        BenchmarkCommand::Analyze(args) => {
            let analysis = analyze_baseline_growth(&args)?;
            Ok(serde_json::to_string_pretty(&analysis)?)
        }
        BenchmarkCommand::Budgets(args) => {
            let budgets = derive_benchmark_budgets(&args)?;
            Ok(serde_json::to_string_pretty(&budgets)?)
        }
        BenchmarkCommand::CheckBudgets(args) => {
            let check = check_benchmark_budgets(&args)?;
            Ok(serde_json::to_string_pretty(&check)?)
        }
        BenchmarkCommand::ProfilePlan(args) => {
            let plan = benchmark_profile_plan(&args)?;
            Ok(serde_json::to_string_pretty(&plan)?)
        }
        BenchmarkCommand::Accept(args) => {
            let acceptance = accept_benchmark_baseline(&args)?;
            Ok(serde_json::to_string_pretty(&acceptance)?)
        }
        BenchmarkCommand::Compare(args) => {
            let comparison = compare_reports(&args)?;
            Ok(serde_json::to_string_pretty(&comparison)?)
        }
        BenchmarkCommand::CompareMatrix(args) => {
            let comparison = compare_matrix_reports(&args)?;
            Ok(serde_json::to_string_pretty(&comparison)?)
        }
        BenchmarkCommand::Report(args) => render_report_file(&args.report),
        BenchmarkCommand::Fixtures(args) => {
            let fixtures = benchmark_fixture_inventory(&args.base, args.include_large)?;
            if args.require_ready && !fixtures.ready {
                bail!(
                    "benchmark fixture matrix is not ready: missing levels {:?}, missing profiles {:?}, missing profile levels {:?}",
                    fixtures.missing_levels,
                    fixtures.missing_profiles,
                    fixtures.missing_profile_levels
                );
            }
            Ok(serde_json::to_string_pretty(&fixtures)?)
        }
    }
}

fn run_baseline_matrix(args: &BenchmarkBaselineArgs) -> Result<BenchmarkBaselineSummary> {
    if args.iterations == 0 {
        bail!("benchmark baseline requires at least one iteration");
    }
    std::fs::create_dir_all(&args.output)
        .with_context(|| format!("failed to create `{}`", args.output.display()))?;
    let inventory = benchmark_fixture_inventory(&args.base, args.include_large)?;
    if args.require_ready && !inventory.ready {
        bail!(
            "benchmark fixture matrix is not ready: missing levels {:?}, missing profiles {:?}, missing profile levels {:?}",
            inventory.missing_levels,
            inventory.missing_profiles,
            inventory.missing_profile_levels
        );
    }
    if args.require_ready && args.iterations < MIN_READY_BENCHMARK_ITERATIONS {
        bail!(
            "benchmark baseline --require-ready requires at least {MIN_READY_BENCHMARK_ITERATIONS} measured iterations"
        );
    }
    if args.require_ready
        && cache_classification(&args.cache_state)
            .is_some_and(|classification| classification.temperature == CacheTemperature::Warm)
        && args.warmups == 0
    {
        bail!(
            "benchmark baseline --require-ready requires at least one warmup iteration for warm cache labels"
        );
    }
    let mut reports = Vec::new();
    let mut failures = Vec::new();
    let mut failure_entries = Vec::new();
    let mut bottleneck_candidates = Vec::new();
    let environment_manifest = EnvironmentMetadata::collect(&args.base)?;
    for fixture in inventory
        .fixtures
        .iter()
        .filter(|fixture| args.include_large || fixture.level != "large")
    {
        let output = args.output.join(format!(
            "{}-{}-seed-{}-{}.json",
            sanitize_report_component(&fixture.profile),
            fixture.level,
            fixture
                .seed
                .map(|seed| seed.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            suite_slug(args.suite)
        ));
        let run_args = BenchmarkRunArgs {
            suite: args.suite,
            fixture: fixture.path.clone(),
            output: Some(output.clone()),
            iterations: args.iterations,
            warmups: args.warmups,
            cache_state: args.cache_state.clone(),
        };
        let reproduction =
            benchmark_reproduction_metadata(fixture, &run_args, &environment_manifest);
        match run_benchmarks(&run_args) {
            Ok(report) => {
                let json = serde_json::to_string_pretty(&report)?;
                std::fs::write(&output, json.as_bytes())
                    .with_context(|| format!("failed to write `{}`", output.display()))?;
                collect_baseline_bottlenecks(&mut bottleneck_candidates, fixture, &output, &report);
                for benchmark in &report.benchmarks {
                    if !benchmark.correctness_passed || !benchmark.failures.is_empty() {
                        failure_entries.push(BenchmarkFailureLogEntry {
                            scope: "benchmark".to_string(),
                            profile: fixture.profile.clone(),
                            level: fixture.level.clone(),
                            seed: fixture.seed,
                            fixture: fixture.path.clone(),
                            report: Some(output.clone()),
                            benchmark: Some(benchmark.name.clone()),
                            failure_kind: if benchmark.correctness_passed {
                                "benchmark-failure".to_string()
                            } else {
                                "incorrect-benchmark".to_string()
                            },
                            error: join_or_none(benchmark.failures.iter().map(String::as_str)),
                            reproduce_command: benchmark_run_command(&run_args),
                            reproduction: Some(reproduction.clone()),
                        });
                    }
                }
                reports.push(BenchmarkBaselineReport {
                    fixture: fixture.path.clone(),
                    level: fixture.level.clone(),
                    profile: fixture.profile.clone(),
                    seed: fixture.seed,
                    report: output,
                    benchmark_count: report.benchmarks.len(),
                    reproduce_command: benchmark_run_command(&run_args),
                    reproduction: Some(reproduction),
                });
            }
            Err(error) => {
                let error = format!("{error:#}");
                failure_entries.push(BenchmarkFailureLogEntry {
                    scope: "fixture".to_string(),
                    profile: fixture.profile.clone(),
                    level: fixture.level.clone(),
                    seed: fixture.seed,
                    fixture: fixture.path.clone(),
                    report: Some(output.clone()),
                    benchmark: None,
                    failure_kind: "fixture-run-failure".to_string(),
                    error: error.clone(),
                    reproduce_command: benchmark_run_command(&run_args),
                    reproduction: Some(reproduction.clone()),
                });
                failures.push(BenchmarkBaselineFailure {
                    fixture: fixture.path.clone(),
                    level: fixture.level.clone(),
                    profile: fixture.profile.clone(),
                    seed: fixture.seed,
                    error,
                });
            }
        }
    }

    let failure_log_path = args.output.join("failure-log.json");
    let failure_log = BenchmarkFailureLog {
        schema_version: REPORT_SCHEMA_VERSION.to_string(),
        generated_at_unix_ms: unix_ms(),
        baseline_output: args.output.clone(),
        suite: args.suite,
        entry_count: failure_entries.len(),
        entries: failure_entries,
    };
    std::fs::write(
        &failure_log_path,
        serde_json::to_vec_pretty(&failure_log)?.as_slice(),
    )
    .with_context(|| format!("failed to write `{}`", failure_log_path.display()))?;
    let ready = inventory.ready
        && failures.is_empty()
        && !reports.is_empty()
        && reports.len() == inventory.fixtures.len();

    Ok(BenchmarkBaselineSummary {
        schema_version: REPORT_SCHEMA_VERSION.to_string(),
        suite: args.suite,
        base: args.base.clone(),
        output: args.output.clone(),
        generated_at_unix_ms: unix_ms(),
        environment_manifest: Some(environment_manifest),
        iterations: args.iterations,
        warmups: args.warmups,
        cache_state: args.cache_state.clone(),
        levels: inventory.levels,
        missing_levels: inventory.missing_levels,
        required_profiles: inventory.required_profiles,
        profile_levels: inventory.profile_levels,
        missing_profiles: inventory.missing_profiles,
        missing_profile_levels: inventory.missing_profile_levels,
        ready,
        failure_log: Some(failure_log_path),
        reports,
        failures,
        bottlenecks: top_baseline_bottlenecks(bottleneck_candidates, 10),
    })
}

fn collect_baseline_bottlenecks(
    candidates: &mut Vec<BenchmarkBaselineBottleneck>,
    fixture: &BenchmarkFixtureEntry,
    report_path: &Path,
    report: &BenchmarkRunReport,
) {
    for benchmark in &report.benchmarks {
        let median_ms = benchmark.stats.median_ms.unwrap_or_default();
        let p95_ms = benchmark.stats.p95_ms.unwrap_or(median_ms);
        let mut reasons = Vec::new();
        if user_facing_suite(benchmark.suite) {
            reasons.push("user-facing".to_string());
        }
        if scale_sensitive_area(&benchmark.area) {
            reasons.push("scale-sensitive-area".to_string());
        }
        if median_ms >= 50.0 {
            reasons.push("high-median".to_string());
        }
        if p95_ms >= 100.0 {
            reasons.push("high-p95".to_string());
        }
        if !benchmark.correctness_passed {
            reasons.push("correctness-failure".to_string());
        }
        if let Some(sqlite) = &benchmark.diagnostics.sqlite {
            if sqlite.uses_full_scan {
                reasons.push("sqlite-full-scan".to_string());
            }
            if sqlite.uses_temporary_btree {
                reasons.push("sqlite-temp-btree".to_string());
            }
        }
        if benchmark
            .diagnostics
            .resource_delta
            .process_cpu_seconds_delta
            .is_some_and(|delta| delta >= 0.050)
        {
            reasons.push("cpu-time-delta".to_string());
        }
        if benchmark
            .diagnostics
            .resource_delta
            .process_rss_kib_delta
            .is_some_and(|delta| delta >= 1024)
        {
            reasons.push("rss-delta".to_string());
        }
        if benchmark
            .diagnostics
            .resource_delta
            .artifact_bytes_delta
            .is_some_and(|delta| delta >= 1024 * 1024)
        {
            reasons.push("artifact-growth".to_string());
        }
        reasons.sort();
        reasons.dedup();
        let priority_score = profile_priority_score(benchmark, median_ms, p95_ms, &reasons);
        candidates.push(BenchmarkBaselineBottleneck {
            profile: fixture.profile.clone(),
            level: fixture.level.clone(),
            seed: fixture.seed,
            suite: benchmark.suite,
            area: benchmark.area.clone(),
            benchmark: benchmark.name.clone(),
            median_ms,
            p95_ms,
            priority_score,
            reasons,
            source_report: report_path.to_path_buf(),
        });
    }
}

fn top_baseline_bottlenecks(
    mut candidates: Vec<BenchmarkBaselineBottleneck>,
    limit: usize,
) -> Vec<BenchmarkBaselineBottleneck> {
    candidates.sort_by(|left, right| {
        right
            .priority_score
            .total_cmp(&left.priority_score)
            .then_with(|| right.p95_ms.total_cmp(&left.p95_ms))
            .then_with(|| left.profile.cmp(&right.profile))
            .then_with(|| left.benchmark.cmp(&right.benchmark))
    });
    candidates.truncate(limit);
    candidates
}

fn benchmark_matrix_plan(args: &BenchmarkPlanArgs) -> Result<BenchmarkMatrixPlan> {
    if args.iterations == 0 {
        bail!("benchmark plan requires at least one iteration");
    }
    let levels = benchmark_level_targets(args.include_large);
    let mut fixtures = Vec::new();
    for profile in REQUIRED_BENCHMARK_PROFILES {
        for (level, target_propositions) in &levels {
            let fixture = args
                .fixture_base
                .join(format!("{profile}-{level}-seed-{}", args.seed));
            let generation = crate::scale_plan_json_for_output(
                profile,
                args.seed,
                *target_propositions,
                fixture.clone(),
            )?;
            let report = args.report_output.join(format!(
                "{}-{level}-seed-{}-{}.json",
                sanitize_report_component(profile),
                args.seed,
                suite_slug(args.suite)
            ));
            fixtures.push(BenchmarkMatrixFixturePlan {
                profile: profile.to_string(),
                level: (*level).to_string(),
                seed: args.seed,
                target_propositions: *target_propositions,
                target_objects: *target_propositions,
                estimated_total_objects: generation_object_budget_estimated_objects(&generation),
                fixture: fixture.clone(),
                report: report.clone(),
                generation,
                benchmark_command: format!(
                    "{RELEASE_FACT_SIM_COMMAND} benchmark run --suite {} --fixture {} --iterations {} --warmups {} --cache-state {} --output {}",
                    suite_slug(args.suite),
                    fixture.display(),
                    args.iterations,
                    args.warmups,
                    args.cache_state,
                    report.display()
                ),
            });
        }
    }
    let warm_summary = args.report_output.join("warm-baseline-summary.json");
    let cold_summary = args.report_output.join("cold-baseline-summary.json");
    let readiness_audit = args.report_output.join("readiness-audit.json");
    let growth_analysis = args.report_output.join("growth-analysis.json");
    let budgets = args.report_output.join("budgets.json");
    let budget_check = args.report_output.join("budget-check.json");
    let profile_plan = args.report_output.join("profile-plan.json");
    let comparison = args.report_output.join("comparison.json");
    let warm_baseline_command = benchmark_baseline_command(
        args,
        &args.report_output.join("warm"),
        "warm-filesystem",
        true,
        Some(&warm_summary),
    );
    let cold_baseline_command = benchmark_baseline_command(
        args,
        &args.report_output.join("cold"),
        "cold-filesystem",
        false,
        Some(&cold_summary),
    );
    Ok(BenchmarkMatrixPlan {
        schema_version: REPORT_SCHEMA_VERSION.to_string(),
        suite: args.suite,
        seed: args.seed,
        fixture_base: args.fixture_base.clone(),
        report_output: args.report_output.clone(),
        iterations: args.iterations,
        warmups: args.warmups,
        cache_state: args.cache_state.clone(),
        include_large: args.include_large,
        levels: levels
            .into_iter()
            .map(|(level, target_propositions)| BenchmarkLevelTarget {
                level: level.to_string(),
                target_propositions,
                target_objects: target_propositions,
            })
            .collect(),
        required_profiles: REQUIRED_BENCHMARK_PROFILES
            .into_iter()
            .map(str::to_string)
            .collect(),
        fixtures,
        fixture_inventory_command: format!(
            "{RELEASE_FACT_SIM_COMMAND} benchmark fixtures --base {}{} > {}",
            args.fixture_base.display(),
            include_large_arg(args.include_large),
            args.report_output.join("fixture-inventory.json").display()
        ),
        baseline_command: benchmark_baseline_command(
            args,
            &args.report_output,
            &args.cache_state,
            args.warmups > 0,
            None,
        ),
        baseline_commands: vec![warm_baseline_command, cold_baseline_command],
        audit_command: format!(
            "{RELEASE_FACT_SIM_COMMAND} benchmark audit --base {}{} --baseline-summary {} --baseline-summary {} > {}",
            args.fixture_base.display(),
            include_large_arg(args.include_large),
            warm_summary.display(),
            cold_summary.display(),
            readiness_audit.display()
        ),
        growth_analysis_command: format!(
            "{RELEASE_FACT_SIM_COMMAND} benchmark analyze --baseline-summary {} > {}",
            warm_summary.display(),
            growth_analysis.display()
        ),
        budgets_command: format!(
            "{RELEASE_FACT_SIM_COMMAND} benchmark budgets --baseline-summary {} > {}",
            warm_summary.display(),
            budgets.display()
        ),
        budget_check_command: format!(
            "{RELEASE_FACT_SIM_COMMAND} benchmark check-budgets --budgets {} --baseline-summary {} > {}",
            budgets.display(),
            warm_summary.display(),
            budget_check.display()
        ),
        profile_plan_command: format!(
            "{RELEASE_FACT_SIM_COMMAND} benchmark profile-plan --baseline-summary {} > {}",
            warm_summary.display(),
            profile_plan.display()
        ),
        compare_matrix_command: format!(
            "{RELEASE_FACT_SIM_COMMAND} benchmark compare-matrix <accepted-baseline-summary.json> {} > {}",
            warm_summary.display(),
            comparison.display()
        ),
        acceptance_command: format!(
            "{RELEASE_FACT_SIM_COMMAND} benchmark accept --audit {} --growth-analysis {} --budget-check {} --profile-plan {} > {}",
            readiness_audit.display(),
            growth_analysis.display(),
            budget_check.display(),
            profile_plan.display(),
            args.report_output.join("acceptance.json").display()
        ),
        cleanup_command: format!(
            "{RELEASE_FACT_SIM_COMMAND} cleanup --fixtures --all && cargo clean"
        ),
    })
}

fn benchmark_baseline_command(
    args: &BenchmarkPlanArgs,
    output: &Path,
    cache_state: &str,
    include_warmups: bool,
    summary: Option<&Path>,
) -> String {
    let warmups = if include_warmups { args.warmups } else { 0 };
    let command = format!(
        "{RELEASE_FACT_SIM_COMMAND} benchmark baseline --suite {} --base {} --output {} --iterations {} --warmups {} --cache-state {} --require-ready{}",
        suite_slug(args.suite),
        args.fixture_base.display(),
        output.display(),
        args.iterations,
        warmups,
        cache_state,
        include_large_arg(args.include_large)
    );
    match summary {
        Some(summary) => format!("{command} > {}", summary.display()),
        None => command,
    }
}

fn generation_object_budget_estimated_objects(generation: &serde_json::Value) -> Option<usize> {
    generation["object_budget"]["estimated_objects"]
        .as_u64()
        .and_then(|count| usize::try_from(count).ok())
}

fn benchmark_reproduction_metadata(
    fixture: &BenchmarkFixtureEntry,
    run_args: &BenchmarkRunArgs,
    environment_manifest: &EnvironmentMetadata,
) -> BenchmarkReproductionMetadata {
    let fixture_metadata = FixtureMetadata::from_fixture(&fixture.path).ok();
    BenchmarkReproductionMetadata {
        fixture: fixture.path.clone(),
        profile: fixture.profile.clone(),
        level: fixture.level.clone(),
        seed: fixture.seed,
        simulator_revision: fixture_metadata
            .as_ref()
            .and_then(|metadata| metadata.simulator_revision.clone()),
        facts_sdk_revision: fixture_metadata
            .as_ref()
            .and_then(|metadata| metadata.facts_sdk_revision.clone()),
        facts_implementation_revision: fixture_metadata
            .as_ref()
            .and_then(|metadata| metadata.facts_implementation_revision.clone()),
        environment_manifest: Some(environment_manifest.clone()),
        benchmark_command: benchmark_run_command(run_args),
    }
}

fn benchmark_spec() -> BenchmarkSpec {
    BenchmarkSpec {
        schema_version: REPORT_SCHEMA_VERSION.to_string(),
        scale_target: "small and medium fixture baselines by default; 500,000 propositions only for manual opt-in large runs".to_string(),
        levels: benchmark_level_targets(true)
            .into_iter()
            .map(|(level, target_propositions)| BenchmarkSpecLevel {
                level: level.to_string(),
                target_propositions,
            })
            .collect(),
        required_levels: benchmark_level_targets(false)
            .into_iter()
            .map(|(level, target_propositions)| BenchmarkSpecLevel {
                level: level.to_string(),
                target_propositions,
            })
            .collect(),
        optional_levels: optional_benchmark_level_targets()
            .into_iter()
            .map(|(level, target_propositions)| BenchmarkSpecLevel {
                level: level.to_string(),
                target_propositions,
            })
            .collect(),
        required_profiles: REQUIRED_BENCHMARK_PROFILES
            .into_iter()
            .map(str::to_string)
            .collect(),
        suites: benchmark_spec_suites(),
        required_suites: required_benchmark_suites().into_iter().collect(),
        required_areas: required_benchmark_areas().into_iter().collect(),
        required_requirements: required_benchmark_requirements().into_iter().collect(),
        required_representative_benchmarks: required_representative_benchmark_names()
            .into_iter()
            .collect(),
        required_cli_workflows: required_cli_workflows().into_iter().collect(),
        suite_coverage: benchmark_suite_coverage(),
        report_fields: [
            "proposition_count",
            "total_object_count",
            "projected_row_count",
            "search_index_row_count",
            "search_index_size_bytes",
            "database_size_bytes",
            "actor_count",
            "ledger_count",
            "replica_count",
            "sqlite_database_paths",
            "raw_samples_ms",
            "sample_observations",
            "timing_statistics",
            "outlier_summary",
            "phase_timings",
            "phase_breakdown",
            "cache_state",
            "cache_classification",
            "environment_metadata",
            "fixture_metadata",
            "resource_snapshots",
            "resource_deltas",
            "peak_rss_kib",
            "disk_read_bytes",
            "disk_write_bytes",
            "sqlite_diagnostics",
            "correctness_passed",
            "requirement_tags",
            "measured_bytes",
            "network_payload_bytes",
            "source_revisions",
            "operating_system",
            "architecture",
            "cpu_model",
            "core_count",
            "memory_bytes",
            "filesystem",
            "storage_type",
            "rust_version",
            "build_profile",
            "feature_flags",
            "fixture_path",
            "reproduction_metadata",
            "preconditions",
            "isolation_strategy",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
        cache_labels: [
            "cold-process",
            "warm-process",
            "cold-filesystem",
            "warm-filesystem",
            "cold-search-index",
            "warm-search-index",
            "first-request",
            "steady-state",
            "profiling",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
        required_commands: [
            "benchmark spec",
            "benchmark plan",
            "benchmark fixtures",
            "benchmark run",
            "benchmark baseline",
            "benchmark audit",
            "benchmark analyze",
            "benchmark budgets",
            "benchmark check-budgets",
            "benchmark profile-plan",
            "benchmark accept",
            "benchmark report",
            "benchmark compare",
            "benchmark compare-matrix",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
        readiness_gates: [
            "fixture matrix contains every required profile at small and medium levels, plus large when explicitly included",
            "baseline summaries reference existing report files",
            "baseline summaries include environment manifests",
            "environment manifests include comparable runtime and source metadata",
            "baseline report entries include reproduction commands",
            "baseline artifacts are produced by release builds",
            "baseline report entries cover every required profile and level",
            "baseline evidence includes cold and warm measurements for every required profile and level",
            "baseline summaries are ready and have no fixture failures",
            "baseline summaries reference present and internally consistent failure logs",
            "failure logs match fixture-level and benchmark-level failures",
            "baseline reports include complete fixture scale and topology metadata",
            "baseline reports distinguish projected row counts from canonical object counts",
            "baseline reports include search-index size when search-index rows are present",
            "baseline summary benchmark counts match referenced report contents",
            "benchmark cache labels and classifications match baseline cache state",
            "warm-cache baseline summaries include at least one warmup iteration",
            "required suites and benchmark areas are covered",
            "required benchmark requirement tags are covered",
            "benchmark entries include preconditions and isolation strategy",
            "baseline summaries use at least two measured samples per benchmark",
            "raw samples and per-sample observations are internally consistent",
            "phase timing totals and phase breakdown metadata are internally consistent",
            "resource snapshots and resource deltas are internally consistent",
            "SQL-backed benchmark entries include coherent SQLite diagnostics",
            "timing statistics are reproducible from raw samples",
            "sampled CLI benchmark entries cover every required CLI workflow",
            "baseline summaries identify top bottleneck candidates",
            "accepted baseline artifacts pass audit, growth, budget, and profiling gates",
            "benchmark correctness flags all pass",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
    }
}

fn audit_benchmark_readiness(args: &BenchmarkAuditArgs) -> Result<BenchmarkAuditReport> {
    let inventory = benchmark_fixture_inventory(&args.base, args.include_large)?;
    let required_suites = required_benchmark_suites();
    let required_areas = required_benchmark_areas();
    let required_requirements = required_benchmark_requirements();
    let required_representative_benchmarks = required_representative_benchmark_names();
    let required_cli_workflows = required_cli_workflows();
    let mut covered_suites = BTreeSet::new();
    let mut covered_areas = BTreeSet::new();
    let mut covered_requirements = BTreeSet::new();
    let mut covered_representative_benchmarks = BTreeSet::new();
    let mut covered_cli_workflows = BTreeSet::new();
    let mut covered_sampled_cli_workflows = BTreeSet::new();
    let mut summaries = Vec::new();
    let mut invalid_baseline_summaries = Vec::new();
    let mut missing_report_files = Vec::new();
    let mut invalid_report_files = Vec::new();
    let mut missing_failure_logs = Vec::new();
    let mut invalid_failure_logs = Vec::new();
    let mut missing_environment_manifests = Vec::new();
    let mut invalid_environment_metadata = Vec::new();
    let mut missing_reproduce_commands = Vec::new();
    let mut non_release_environment_manifests = Vec::new();
    let mut non_release_reports = Vec::new();
    let mut not_ready_summaries = Vec::new();
    let mut failing_summaries = Vec::new();
    let mut baseline_profile_levels = BTreeMap::<String, BTreeSet<String>>::new();
    let mut baseline_cache_profile_levels =
        BTreeMap::<String, BTreeMap<String, BTreeSet<String>>>::new();
    let mut fixture_metadata_mismatches = Vec::new();
    let mut missing_fixture_metadata = Vec::new();
    let mut invalid_report_counts = Vec::new();
    let mut invalid_cache_labels = Vec::new();
    let mut invalid_cache_metadata = Vec::new();
    let mut covered_cache_temperatures = BTreeSet::new();
    let mut insufficient_warmup_iterations = Vec::new();
    let mut missing_report_metadata = Vec::new();
    let mut missing_benchmark_metadata = Vec::new();
    let mut invalid_requirement_tags = Vec::new();
    let mut missing_bottleneck_summaries = Vec::new();
    let mut invalid_bottleneck_summaries = Vec::new();
    let mut insufficient_baseline_iterations = Vec::new();
    let mut insufficient_sample_benchmarks = Vec::new();
    let mut invalid_sample_observations = Vec::new();
    let mut invalid_phase_metadata = Vec::new();
    let mut invalid_resource_metadata = Vec::new();
    let mut invalid_sqlite_metadata = Vec::new();
    let mut invalid_timing_statistics = Vec::new();
    let mut incorrect_benchmarks = Vec::new();
    let mut total_reports = 0;
    let mut total_benchmarks = 0;

    for summary_path in &args.baseline_summaries {
        let summary = match read_baseline_summary(summary_path) {
            Ok(summary) => summary,
            Err(error) => {
                invalid_baseline_summaries.push(format!(
                    "{}:{}",
                    summary_path.display(),
                    format!("{error:#}").replace('\n', " ")
                ));
                continue;
            }
        };
        if !summary.ready {
            not_ready_summaries.push(summary_path.clone());
        }
        if !summary.failures.is_empty() {
            failing_summaries.push(summary_path.clone());
        }
        if summary.iterations < MIN_READY_BENCHMARK_ITERATIONS {
            insufficient_baseline_iterations.push(summary_path.clone());
        }
        let summary_failure_log = match &summary.failure_log {
            Some(failure_log) if failure_log.is_file() => match read_failure_log(failure_log) {
                Ok(failure_log) => {
                    invalid_failure_logs.extend(invalid_failure_log_entries(
                        summary_path,
                        &summary,
                        &failure_log,
                    ));
                    Some(failure_log)
                }
                Err(error) => {
                    invalid_failure_logs.push(format!(
                        "{}:{}:{}",
                        summary_path.display(),
                        failure_log.display(),
                        format!("{error:#}").replace('\n', " ")
                    ));
                    None
                }
            },
            Some(failure_log) => {
                missing_failure_logs.push(failure_log.clone());
                None
            }
            None => {
                missing_failure_logs.push(summary_path.clone());
                None
            }
        };
        if summary.bottlenecks.is_empty() && !summary.reports.is_empty() {
            missing_bottleneck_summaries.push(summary_path.clone());
        }
        if !is_known_cache_label(&summary.cache_state) {
            invalid_cache_labels.push(format!(
                "{}:{}",
                summary_path.display(),
                summary.cache_state
            ));
        }
        if let Some(classification) = cache_classification(&summary.cache_state) {
            let temperature = cache_temperature_slug(classification.temperature);
            if required_cache_temperatures().contains(temperature) {
                covered_cache_temperatures.insert(temperature.to_string());
            }
        }
        if cache_classification(&summary.cache_state)
            .is_some_and(|classification| classification.temperature == CacheTemperature::Warm)
            && summary.warmups == 0
        {
            insufficient_warmup_iterations.push(summary_path.clone());
        }
        match &summary.environment_manifest {
            Some(environment) if environment.build_profile != "release" => {
                non_release_environment_manifests.push(summary_path.clone());
            }
            Some(environment) => invalid_environment_metadata
                .extend(invalid_environment_entries(summary_path, environment)),
            None => missing_environment_manifests.push(summary_path.clone()),
        }
        if summary.suite == BenchmarkSuite::Full {
            covered_suites.extend(required_suites.iter().copied());
        } else {
            covered_suites.insert(summary.suite);
        }
        let mut summary_report_count = 0;
        let mut summary_benchmark_count = 0;
        let mut summary_report_paths = BTreeSet::new();
        let mut summary_benchmark_keys = BTreeSet::new();
        let mut expected_benchmark_failures = BTreeMap::new();
        for report_entry in &summary.reports {
            summary_report_paths.insert(report_entry.report.clone());
            baseline_profile_levels
                .entry(report_entry.profile.clone())
                .or_default()
                .insert(report_entry.level.clone());
            if let Some(classification) = cache_classification(&summary.cache_state) {
                let temperature = cache_temperature_slug(classification.temperature);
                if required_cache_temperatures().contains(temperature) {
                    baseline_cache_profile_levels
                        .entry(temperature.to_string())
                        .or_default()
                        .entry(report_entry.profile.clone())
                        .or_default()
                        .insert(report_entry.level.clone());
                }
            }
            if report_entry.reproduce_command.trim().is_empty()
                || missing_reproduction_metadata(&report_entry.reproduction)
            {
                missing_reproduce_commands.push(report_entry.report.clone());
            }
            if !report_entry.report.is_file() {
                missing_report_files.push(report_entry.report.clone());
                continue;
            }
            let report = match read_report(&report_entry.report) {
                Ok(report) => report,
                Err(error) => {
                    invalid_report_files.push(format!(
                        "{}:{}",
                        report_entry.report.display(),
                        format!("{error:#}").replace('\n', " ")
                    ));
                    continue;
                }
            };
            if report_entry.benchmark_count != report.benchmarks.len() {
                invalid_report_counts.push(format!(
                    "{}:summary={}:report={}",
                    report_entry.report.display(),
                    report_entry.benchmark_count,
                    report.benchmarks.len()
                ));
            }
            if report.environment.build_profile != "release" {
                non_release_reports.push(report_entry.report.clone());
            }
            invalid_environment_metadata.extend(invalid_environment_entries(
                &report_entry.report,
                &report.environment,
            ));
            if report.fixture.profile != report_entry.profile
                || report.fixture.path != report_entry.fixture
                || fixture_level(report.fixture.proposition_count) != report_entry.level
                || report.fixture.seed != report_entry.seed
            {
                fixture_metadata_mismatches.push(report_entry.report.clone());
            }
            missing_fixture_metadata.extend(missing_fixture_metadata_entries(
                &report_entry.report,
                &report.fixture,
            ));
            if missing_source_revision_metadata(&report) {
                missing_report_metadata.push(report_entry.report.clone());
            }
            summary_report_count += 1;
            for benchmark in &report.benchmarks {
                summary_benchmark_keys.insert((
                    report_entry.report.clone(),
                    report_entry.profile.clone(),
                    report_entry.level.clone(),
                    report_entry.seed,
                    benchmark.suite,
                    benchmark.area.clone(),
                    benchmark.name.clone(),
                ));
                covered_suites.insert(benchmark.suite);
                covered_areas.insert(benchmark.area.clone());
                covered_requirements.extend(benchmark.requirement_tags.iter().cloned());
                if required_representative_benchmarks.contains(&benchmark.name) {
                    covered_representative_benchmarks.insert(benchmark.name.clone());
                }
                invalid_requirement_tags.extend(invalid_requirement_tags_for_benchmark(
                    &report_entry.report,
                    benchmark,
                    &required_requirements,
                ));
                covered_cli_workflows.extend(cli_workflows_for_benchmark(&benchmark.name));
                if benchmark.diagnostics.operation_kind == "cli-workflow" {
                    covered_sampled_cli_workflows
                        .extend(cli_workflows_for_benchmark(&benchmark.name));
                }
                summary_benchmark_count += 1;
                if !is_known_cache_label(&benchmark.cache_state) {
                    invalid_cache_labels.push(format!(
                        "{}:{}:{}",
                        report_entry.report.display(),
                        benchmark.name,
                        benchmark.cache_state
                    ));
                }
                if let Some(error) =
                    invalid_cache_metadata_for_benchmark(&summary.cache_state, benchmark)
                {
                    invalid_cache_metadata.push(format!(
                        "{}:{}:{error}",
                        report_entry.report.display(),
                        benchmark.name
                    ));
                }
                for reason in missing_benchmark_metadata_reasons(benchmark) {
                    missing_benchmark_metadata.push(format!(
                        "{}:{}:{reason}",
                        report_entry.report.display(),
                        benchmark.name,
                    ));
                }
                if benchmark.samples_ms.len() != summary.iterations
                    || benchmark.stats.samples != summary.iterations
                    || benchmark.sample_observations.len() != benchmark.samples_ms.len()
                {
                    insufficient_sample_benchmarks.push(format!(
                        "{}:{}:{}",
                        report_entry.report.display(),
                        benchmark.name,
                        benchmark.samples_ms.len()
                    ));
                }
                if let Some(error) = invalid_sample_observation(benchmark) {
                    invalid_sample_observations.push(format!(
                        "{}:{}:{error}",
                        report_entry.report.display(),
                        benchmark.name
                    ));
                }
                if let Some(error) = invalid_phase_metadata_for_benchmark(benchmark) {
                    invalid_phase_metadata.push(format!(
                        "{}:{}:{error}",
                        report_entry.report.display(),
                        benchmark.name
                    ));
                }
                if let Some(error) = invalid_resource_metadata_for_benchmark(benchmark) {
                    invalid_resource_metadata.push(format!(
                        "{}:{}:{error}",
                        report_entry.report.display(),
                        benchmark.name
                    ));
                }
                if let Some(error) = invalid_sqlite_metadata_for_benchmark(benchmark) {
                    invalid_sqlite_metadata.push(format!(
                        "{}:{}:{error}",
                        report_entry.report.display(),
                        benchmark.name
                    ));
                }
                if let Some(error) = invalid_timing_statistics_for_benchmark(benchmark) {
                    invalid_timing_statistics.push(format!(
                        "{}:{}:{error}",
                        report_entry.report.display(),
                        benchmark.name
                    ));
                }
                if !benchmark.correctness_passed {
                    incorrect_benchmarks.push(format!(
                        "{}:{}",
                        report_entry.report.display(),
                        benchmark.name
                    ));
                }
                if !benchmark.correctness_passed || !benchmark.failures.is_empty() {
                    expected_benchmark_failures.insert(
                        benchmark_failure_key(
                            &report_entry.profile,
                            &report_entry.level,
                            report_entry.seed,
                            Some(&report_entry.report),
                            Some(&benchmark.name),
                        ),
                        if benchmark.correctness_passed {
                            "benchmark-failure".to_string()
                        } else {
                            "incorrect-benchmark".to_string()
                        },
                    );
                }
            }
        }
        if let Some(failure_log) = &summary_failure_log {
            invalid_failure_logs.extend(invalid_benchmark_failure_log_entries(
                summary_path,
                failure_log,
                &expected_benchmark_failures,
            ));
        }
        invalid_bottleneck_summaries.extend(invalid_bottleneck_summary_entries(
            summary_path,
            &summary,
            &summary_report_paths,
            &summary_benchmark_keys,
        ));
        total_reports += summary_report_count;
        total_benchmarks += summary_benchmark_count;
        summaries.push(BenchmarkAuditSummaryInput {
            path: summary_path.clone(),
            suite: summary.suite,
            ready: summary.ready,
            report_count: summary_report_count,
            benchmark_count: summary_benchmark_count,
            failure_count: summary.failures.len(),
        });
    }

    let missing_suites = required_suites
        .iter()
        .filter(|suite| !covered_suites.contains(*suite))
        .copied()
        .collect::<Vec<_>>();
    let missing_areas = required_areas
        .iter()
        .filter(|area| !covered_areas.contains(*area))
        .cloned()
        .collect::<Vec<_>>();
    let missing_requirements = required_requirements
        .iter()
        .filter(|requirement| !covered_requirements.contains(*requirement))
        .cloned()
        .collect::<Vec<_>>();
    let missing_representative_benchmarks = required_representative_benchmarks
        .iter()
        .filter(|benchmark| !covered_representative_benchmarks.contains(*benchmark))
        .cloned()
        .collect::<Vec<_>>();
    let missing_cli_workflows = required_cli_workflows
        .iter()
        .filter(|workflow| !covered_cli_workflows.contains(*workflow))
        .cloned()
        .collect::<Vec<_>>();
    let missing_sampled_cli_workflows = required_cli_workflows
        .iter()
        .filter(|workflow| !covered_sampled_cli_workflows.contains(*workflow))
        .cloned()
        .collect::<Vec<_>>();
    let missing_cache_temperatures = required_cache_temperatures()
        .into_iter()
        .filter(|temperature| !covered_cache_temperatures.contains(*temperature))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let missing_baseline_profile_levels =
        missing_required_profile_levels(&baseline_profile_levels, args.include_large);
    let missing_cache_profile_levels =
        missing_required_cache_profile_levels(&baseline_cache_profile_levels, args.include_large);
    let remediation_commands = audit_remediation_commands(
        &args.base,
        &inventory.missing_profile_levels,
        &missing_baseline_profile_levels,
        &missing_cache_profile_levels,
        !missing_suites.is_empty()
            || !missing_areas.is_empty()
            || !missing_requirements.is_empty()
            || !missing_representative_benchmarks.is_empty()
            || !missing_cli_workflows.is_empty()
            || !missing_sampled_cli_workflows.is_empty()
            || !missing_cache_temperatures.is_empty(),
    );
    let baseline_ready = inventory.ready
        && !args.baseline_summaries.is_empty()
        && missing_suites.is_empty()
        && missing_areas.is_empty()
        && missing_requirements.is_empty()
        && missing_representative_benchmarks.is_empty()
        && missing_cli_workflows.is_empty()
        && missing_sampled_cli_workflows.is_empty()
        && missing_cache_temperatures.is_empty()
        && missing_baseline_profile_levels.is_empty()
        && missing_cache_profile_levels.is_empty()
        && invalid_baseline_summaries.is_empty()
        && missing_report_files.is_empty()
        && invalid_report_files.is_empty()
        && missing_failure_logs.is_empty()
        && invalid_failure_logs.is_empty()
        && missing_environment_manifests.is_empty()
        && invalid_environment_metadata.is_empty()
        && missing_reproduce_commands.is_empty()
        && non_release_environment_manifests.is_empty()
        && non_release_reports.is_empty()
        && not_ready_summaries.is_empty()
        && failing_summaries.is_empty()
        && fixture_metadata_mismatches.is_empty()
        && missing_fixture_metadata.is_empty()
        && invalid_report_counts.is_empty()
        && invalid_cache_labels.is_empty()
        && invalid_cache_metadata.is_empty()
        && insufficient_warmup_iterations.is_empty()
        && missing_report_metadata.is_empty()
        && missing_benchmark_metadata.is_empty()
        && invalid_requirement_tags.is_empty()
        && missing_bottleneck_summaries.is_empty()
        && invalid_bottleneck_summaries.is_empty()
        && insufficient_baseline_iterations.is_empty()
        && insufficient_sample_benchmarks.is_empty()
        && invalid_sample_observations.is_empty()
        && invalid_phase_metadata.is_empty()
        && invalid_resource_metadata.is_empty()
        && invalid_sqlite_metadata.is_empty()
        && invalid_timing_statistics.is_empty()
        && incorrect_benchmarks.is_empty()
        && total_reports > 0
        && total_benchmarks > 0;

    Ok(BenchmarkAuditReport {
        schema_version: REPORT_SCHEMA_VERSION.to_string(),
        generated_at_unix_ms: unix_ms(),
        base: args.base.clone(),
        fixture_matrix_ready: inventory.ready,
        inventory,
        required_suites: required_suites.into_iter().collect(),
        covered_suites: covered_suites.into_iter().collect(),
        missing_suites,
        required_areas: required_areas.iter().cloned().collect(),
        covered_areas: covered_areas.into_iter().collect(),
        missing_areas,
        required_requirements: required_requirements.into_iter().collect(),
        covered_requirements: covered_requirements.into_iter().collect(),
        missing_requirements,
        required_representative_benchmarks: required_representative_benchmarks
            .into_iter()
            .collect(),
        covered_representative_benchmarks: covered_representative_benchmarks.into_iter().collect(),
        missing_representative_benchmarks,
        required_cli_workflows: required_cli_workflows.into_iter().collect(),
        covered_cli_workflows: covered_cli_workflows.into_iter().collect(),
        missing_cli_workflows,
        covered_sampled_cli_workflows: covered_sampled_cli_workflows.into_iter().collect(),
        missing_sampled_cli_workflows,
        covered_cache_temperatures: covered_cache_temperatures.into_iter().collect(),
        missing_cache_temperatures,
        baseline_profile_levels,
        missing_baseline_profile_levels,
        baseline_cache_profile_levels,
        missing_cache_profile_levels,
        baseline_summaries: summaries,
        invalid_baseline_summaries,
        missing_report_files,
        invalid_report_files,
        missing_failure_logs,
        invalid_failure_logs,
        missing_environment_manifests,
        invalid_environment_metadata,
        missing_reproduce_commands,
        non_release_environment_manifests,
        non_release_reports,
        not_ready_summaries,
        failing_summaries,
        fixture_metadata_mismatches,
        missing_fixture_metadata,
        invalid_report_counts,
        invalid_cache_labels,
        invalid_cache_metadata,
        insufficient_warmup_iterations,
        missing_report_metadata,
        missing_benchmark_metadata,
        invalid_requirement_tags,
        missing_bottleneck_summaries,
        invalid_bottleneck_summaries,
        insufficient_baseline_iterations,
        insufficient_sample_benchmarks,
        invalid_sample_observations,
        invalid_phase_metadata,
        invalid_resource_metadata,
        invalid_sqlite_metadata,
        invalid_timing_statistics,
        incorrect_benchmarks,
        remediation_commands,
        total_reports,
        total_benchmarks,
        ready: baseline_ready,
    })
}

fn audit_remediation_commands(
    fixture_base: &Path,
    missing_fixture_profile_levels: &[BenchmarkMissingProfileLevel],
    missing_baseline_profile_levels: &[BenchmarkMissingProfileLevel],
    missing_cache_profile_levels: &[BenchmarkMissingCacheProfileLevel],
    missing_coverage_evidence: bool,
) -> Vec<BenchmarkAuditRemediationCommand> {
    let mut commands = Vec::new();
    let targets = benchmark_level_targets(true)
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    for missing in missing_fixture_profile_levels {
        let Some(target_propositions) = targets.get(missing.level.as_str()) else {
            continue;
        };
        let fixture = fixture_base.join(format!("{}-{}-seed-42", missing.profile, missing.level));
        commands.push(BenchmarkAuditRemediationCommand {
            kind: "generate-fixture".to_string(),
            profile: Some(missing.profile.clone()),
            level: Some(missing.level.clone()),
            cache_temperature: None,
            command: format!(
                "{RELEASE_FACT_SIM_COMMAND} generate --profile {} --seed 42 --target-propositions {} --output {}",
                missing.profile,
                target_propositions,
                fixture.display()
            ),
        });
        commands.push(BenchmarkAuditRemediationCommand {
            kind: "verify-fixture".to_string(),
            profile: Some(missing.profile.clone()),
            level: Some(missing.level.clone()),
            cache_temperature: None,
            command: format!("{RELEASE_FACT_SIM_COMMAND} verify {}", fixture.display()),
        });
    }

    if missing_coverage_evidence
        || !missing_baseline_profile_levels.is_empty()
        || !missing_cache_profile_levels.is_empty()
    {
        commands.push(BenchmarkAuditRemediationCommand {
            kind: "run-warm-baseline".to_string(),
            profile: None,
            level: None,
            cache_temperature: Some("warm".to_string()),
            command: format!(
                "{RELEASE_FACT_SIM_COMMAND} benchmark baseline --suite full --base {} --output reports/benchmarks/warm --iterations 30 --warmups 5 --cache-state warm-filesystem --require-ready > reports/benchmarks/warm-baseline-summary.json",
                fixture_base.display()
            ),
        });
        commands.push(BenchmarkAuditRemediationCommand {
            kind: "run-cold-baseline".to_string(),
            profile: None,
            level: None,
            cache_temperature: Some("cold".to_string()),
            command: format!(
                "{RELEASE_FACT_SIM_COMMAND} benchmark baseline --suite full --base {} --output reports/benchmarks/cold --iterations 30 --warmups 0 --cache-state cold-filesystem --require-ready > reports/benchmarks/cold-baseline-summary.json",
                fixture_base.display()
            ),
        });
        commands.push(BenchmarkAuditRemediationCommand {
            kind: "rerun-audit".to_string(),
            profile: None,
            level: None,
            cache_temperature: None,
            command: format!(
                "{RELEASE_FACT_SIM_COMMAND} benchmark audit --base {} --baseline-summary reports/benchmarks/warm-baseline-summary.json --baseline-summary reports/benchmarks/cold-baseline-summary.json > reports/benchmarks/readiness-audit.json",
                fixture_base.display()
            ),
        });
    }
    commands
}

fn missing_expected_byte_metadata(benchmark: &BenchmarkResult) -> bool {
    let operation_kind = benchmark.diagnostics.operation_kind.as_str();
    let requires_measured_bytes = matches!(
        operation_kind,
        "file-inventory"
            | "process"
            | "http-router"
            | "validation"
            | "snapshot-sidecar"
            | "snapshot-frame"
            | "table-size"
    );
    if requires_measured_bytes && benchmark.measured_bytes.is_none() {
        return true;
    }
    let models_transferable_payload = benchmark
        .requirement_tags
        .iter()
        .any(|tag| matches!(tag.as_str(), "push-pull-sync" | "http-push" | "http-pull"));
    let missing_file_inventory_network_payload = operation_kind == "file-inventory"
        && models_transferable_payload
        && benchmark.network_payload_bytes.is_none();
    let missing_http_payload = operation_kind == "http-router"
        && (benchmark.measured_bytes.is_none() || benchmark.network_payload_bytes.is_none());
    missing_file_inventory_network_payload || missing_http_payload
}

fn missing_benchmark_metadata_reasons(benchmark: &BenchmarkResult) -> Vec<&'static str> {
    let mut reasons = Vec::new();
    if benchmark.preconditions.is_empty() {
        reasons.push("missing-preconditions");
    }
    if benchmark.isolation_strategy.trim().is_empty() {
        reasons.push("missing-isolation-strategy");
    }
    if invalid_read_only_metadata_for_benchmark(benchmark).is_some() {
        reasons.push("invalid-read-only-metadata");
    }
    if invalid_isolation_strategy_for_benchmark(benchmark).is_some() {
        reasons.push("invalid-isolation-strategy");
    }
    if benchmark.phase_breakdown.is_empty() {
        reasons.push("missing-phase-breakdown");
    }
    if benchmark.cache_classification.is_none() {
        reasons.push("missing-cache-classification");
    }
    if missing_expected_byte_metadata(benchmark) {
        reasons.push("missing-byte-metadata");
    }
    reasons
}

fn invalid_isolation_strategy_for_benchmark(benchmark: &BenchmarkResult) -> Option<String> {
    if benchmark.read_only {
        return None;
    }
    let strategy = benchmark.isolation_strategy.to_ascii_lowercase();
    let uses_isolated_write_state = strategy.contains("temporary")
        || strategy.contains("fresh")
        || strategy.contains("newly initialized");
    if uses_isolated_write_state {
        None
    } else {
        Some("write-benchmark-without-temporary-state".to_string())
    }
}

fn invalid_read_only_metadata_for_benchmark(benchmark: &BenchmarkResult) -> Option<String> {
    let operation_kind = benchmark.diagnostics.operation_kind.as_str();
    let write_capable_operation = matches!(operation_kind, "scenario" | "cli-workflow");
    match (benchmark.read_only, write_capable_operation) {
        (true, true) => Some("write-capable-operation-marked-read-only".to_string()),
        (false, false) => Some("read-only-operation-marked-write".to_string()),
        _ => None,
    }
}

fn invalid_requirement_tags_for_benchmark(
    report: &Path,
    benchmark: &BenchmarkResult,
    required_requirements: &BTreeSet<String>,
) -> Vec<String> {
    if benchmark.requirement_tags.is_empty() {
        return vec![format!("{}:{}:missing", report.display(), benchmark.name)];
    }
    benchmark
        .requirement_tags
        .iter()
        .filter(|tag| tag.trim().is_empty() || !required_requirements.contains(tag.as_str()))
        .map(|tag| format!("{}:{}:{tag}", report.display(), benchmark.name))
        .collect()
}

fn invalid_cache_metadata_for_benchmark(
    baseline_cache_state: &str,
    benchmark: &BenchmarkResult,
) -> Option<String> {
    if benchmark.cache_state != baseline_cache_state {
        return Some(format!(
            "cache-state-mismatch-{}-{}",
            baseline_cache_state, benchmark.cache_state
        ));
    }
    let expected = cache_classification(&benchmark.cache_state);
    if benchmark.cache_classification != expected {
        return Some("cache-classification-mismatch".to_string());
    }
    None
}

fn missing_source_revision_metadata(report: &BenchmarkRunReport) -> bool {
    report.fixture.simulator_revision.is_none()
        || report.fixture.facts_sdk_revision.is_none()
        || report.fixture.facts_implementation_revision.is_none()
        || report.environment.benchmark_project_commit.is_none()
        || report.environment.sdk_source_commit.is_none()
        || report.environment.facts_source_commit.is_none()
}

fn invalid_environment_entries(report: &Path, environment: &EnvironmentMetadata) -> Vec<String> {
    let mut invalid = Vec::new();
    if environment.operating_system.trim().is_empty() {
        invalid.push("operating-system");
    }
    if environment.architecture.trim().is_empty() {
        invalid.push("architecture");
    }
    if environment.cpu_model.as_deref().is_none_or(str::is_empty) {
        invalid.push("cpu-model");
    }
    match environment.core_count {
        Some(0) | None => invalid.push("core-count"),
        Some(_) => {}
    }
    match environment.memory_bytes {
        Some(0) | None => invalid.push("memory-bytes"),
        Some(_) => {}
    }
    if environment.filesystem.as_deref().is_none_or(str::is_empty) {
        invalid.push("filesystem");
    }
    if environment
        .storage_type
        .as_deref()
        .is_none_or(str::is_empty)
    {
        invalid.push("storage-type");
    }
    if environment
        .rust_version
        .as_deref()
        .is_none_or(str::is_empty)
    {
        invalid.push("rust-version");
    }
    if environment.build_profile.trim().is_empty() {
        invalid.push("build-profile");
    }
    if environment.fixture_path.as_os_str().is_empty() {
        invalid.push("fixture-path");
    }
    if environment
        .facts_source_commit
        .as_deref()
        .is_none_or(str::is_empty)
    {
        invalid.push("facts-source-commit");
    }
    if environment
        .sdk_source_commit
        .as_deref()
        .is_none_or(str::is_empty)
    {
        invalid.push("sdk-source-commit");
    }
    if environment
        .benchmark_project_commit
        .as_deref()
        .is_none_or(str::is_empty)
    {
        invalid.push("benchmark-project-commit");
    }
    invalid
        .into_iter()
        .map(|field| format!("{}:{field}", report.display()))
        .collect()
}

fn missing_fixture_metadata_entries(report: &Path, fixture: &FixtureMetadata) -> Vec<String> {
    let mut missing = Vec::new();
    if fixture.profile.trim().is_empty() {
        missing.push("profile");
    }
    if fixture.seed.is_none() {
        missing.push("seed");
    }
    if fixture.proposition_count == 0 {
        missing.push("proposition-count");
    }
    if fixture.total_object_count == 0 {
        missing.push("total-object-count");
    }
    if fixture.total_object_count < fixture.proposition_count {
        missing.push("object-count-less-than-propositions");
    }
    if fixture.projected_row_count == 0 {
        missing.push("projected-row-count");
    }
    if fixture.search_index_row_count > 0 && fixture.search_index_size_bytes == 0 {
        missing.push("search-index-size-bytes");
    }
    if fixture.database_size_bytes == 0 {
        missing.push("database-size-bytes");
    }
    match fixture.actor_count {
        Some(0) | None => missing.push("actor-count"),
        Some(_) => {}
    }
    match fixture.ledger_count {
        Some(0) | None => missing.push("ledger-count"),
        Some(_) => {}
    }
    if fixture.replica_count.is_none() {
        missing.push("replica-count");
    }
    if fixture.sqlite_databases.is_empty() {
        missing.push("sqlite-database-paths");
    }
    missing
        .into_iter()
        .map(|field| format!("{}:{field}", report.display()))
        .collect()
}

fn missing_reproduction_metadata(reproduction: &Option<BenchmarkReproductionMetadata>) -> bool {
    let Some(reproduction) = reproduction else {
        return true;
    };
    reproduction.benchmark_command.trim().is_empty()
        || reproduction.environment_manifest.is_none()
        || reproduction.simulator_revision.is_none()
        || reproduction.facts_sdk_revision.is_none()
        || reproduction.facts_implementation_revision.is_none()
}

fn invalid_failure_log_entries(
    summary_path: &Path,
    summary: &BenchmarkBaselineSummary,
    failure_log: &BenchmarkFailureLog,
) -> Vec<String> {
    let mut invalid = Vec::new();
    if failure_log.schema_version != REPORT_SCHEMA_VERSION {
        invalid.push(format!("{}:schema-version", summary_path.display()));
    }
    if failure_log.suite != summary.suite {
        invalid.push(format!("{}:suite-mismatch", summary_path.display()));
    }
    if failure_log.baseline_output != summary.output {
        invalid.push(format!(
            "{}:baseline-output-mismatch",
            summary_path.display()
        ));
    }
    if failure_log.entry_count != failure_log.entries.len() {
        invalid.push(format!("{}:entry-count-mismatch", summary_path.display()));
    }
    let fixture_failure_count = failure_log
        .entries
        .iter()
        .filter(|entry| entry.scope == "fixture")
        .count();
    if fixture_failure_count != summary.failures.len() {
        invalid.push(format!(
            "{}:fixture-failure-count-mismatch",
            summary_path.display()
        ));
    }
    for (index, entry) in failure_log.entries.iter().enumerate() {
        let prefix = format!("{}:entry-{index}", summary_path.display());
        if entry.scope.trim().is_empty()
            || entry.profile.trim().is_empty()
            || entry.level.trim().is_empty()
            || entry.fixture.as_os_str().is_empty()
            || entry.failure_kind.trim().is_empty()
            || entry.error.trim().is_empty()
        {
            invalid.push(format!("{prefix}:missing-identity"));
        }
        if entry.reproduce_command.trim().is_empty()
            || missing_reproduction_metadata(&entry.reproduction)
        {
            invalid.push(format!("{prefix}:missing-reproduction"));
        }
        match entry.scope.as_str() {
            "fixture" if entry.benchmark.is_some() => {
                invalid.push(format!("{prefix}:fixture-benchmark-set"));
            }
            "benchmark" if entry.benchmark.as_deref().is_none_or(str::is_empty) => {
                invalid.push(format!("{prefix}:benchmark-missing-name"));
            }
            "fixture" | "benchmark" => {}
            _ => invalid.push(format!("{prefix}:unknown-scope")),
        }
    }
    invalid.sort();
    invalid.dedup();
    invalid
}

fn invalid_benchmark_failure_log_entries(
    summary_path: &Path,
    failure_log: &BenchmarkFailureLog,
    expected_failures: &BTreeMap<String, String>,
) -> Vec<String> {
    let mut invalid = Vec::new();
    let mut actual_failures = BTreeMap::new();
    for (index, entry) in failure_log.entries.iter().enumerate() {
        if entry.scope != "benchmark" {
            continue;
        }
        let prefix = format!("{}:entry-{index}", summary_path.display());
        let key = benchmark_failure_key(
            &entry.profile,
            &entry.level,
            entry.seed,
            entry.report.as_ref(),
            entry.benchmark.as_ref(),
        );
        if entry.report.is_none() {
            invalid.push(format!("{prefix}:benchmark-missing-report"));
        }
        match expected_failures.get(&key) {
            Some(expected_kind) if expected_kind == &entry.failure_kind => {
                actual_failures.insert(key, entry.failure_kind.clone());
            }
            Some(expected_kind) => {
                invalid.push(format!(
                    "{prefix}:benchmark-failure-kind-mismatch:{}:{}",
                    entry.failure_kind, expected_kind
                ));
                actual_failures.insert(key, entry.failure_kind.clone());
            }
            None => invalid.push(format!("{prefix}:unexpected-benchmark-failure")),
        }
    }
    for key in expected_failures.keys() {
        if !actual_failures.contains_key(key) {
            invalid.push(format!(
                "{}:{}:benchmark-failure-missing",
                summary_path.display(),
                key
            ));
        }
    }
    invalid.sort();
    invalid.dedup();
    invalid
}

fn benchmark_failure_key(
    profile: &str,
    level: &str,
    seed: Option<u64>,
    report: Option<&PathBuf>,
    benchmark: Option<&String>,
) -> String {
    format!(
        "{}|{}|{}|{}|{}",
        profile,
        level,
        seed.map_or_else(|| "unknown".to_string(), |seed| seed.to_string()),
        report
            .map(|report| report.display().to_string())
            .unwrap_or_else(|| "unknown-report".to_string()),
        benchmark.map(String::as_str).unwrap_or("unknown-benchmark")
    )
}

fn invalid_sample_observation(benchmark: &BenchmarkResult) -> Option<String> {
    if benchmark.samples_ms.len() != benchmark.sample_observations.len() {
        return Some("observation-count-mismatch".to_string());
    }
    for (index, (sample, observation)) in benchmark
        .samples_ms
        .iter()
        .zip(&benchmark.sample_observations)
        .enumerate()
    {
        if !sample.is_finite() || *sample < 0.0 {
            return Some(format!("invalid-sample-{index}"));
        }
        if observation.sample_index != index {
            return Some(format!(
                "sample-index-mismatch-{index}-{}",
                observation.sample_index
            ));
        }
        if !observation.elapsed_ms.is_finite() || observation.elapsed_ms < 0.0 {
            return Some(format!("invalid-observation-elapsed-{index}"));
        }
        if (observation.elapsed_ms - *sample).abs() > f64::EPSILON {
            return Some(format!("sample-observation-elapsed-mismatch-{index}"));
        }
        if observation.rows_returned != benchmark.rows_returned {
            return Some(format!("sample-observation-rows-mismatch-{index}"));
        }
        if observation.measured_bytes != benchmark.measured_bytes {
            return Some(format!(
                "sample-observation-measured-bytes-mismatch-{index}"
            ));
        }
        if observation.network_payload_bytes != benchmark.network_payload_bytes {
            return Some(format!(
                "sample-observation-network-payload-bytes-mismatch-{index}"
            ));
        }
    }
    None
}

fn invalid_phase_metadata_for_benchmark(benchmark: &BenchmarkResult) -> Option<String> {
    let timings = &benchmark.phase_timings;
    for (name, value) in [
        ("setup", timings.setup_ms),
        ("warmup", timings.warmup_total_ms),
        ("measurement", timings.measurement_total_ms),
    ] {
        if !value.is_finite() || value < 0.0 {
            return Some(format!("invalid-{name}-timing"));
        }
    }

    let mut required = BTreeMap::from([
        ("setup", timings.setup_ms),
        ("warmup", timings.warmup_total_ms),
        ("measurement", timings.measurement_total_ms),
    ]);
    for phase in &benchmark.phase_breakdown {
        if phase.phase.trim().is_empty() {
            return Some("missing-phase-name".to_string());
        }
        if phase.measurement == BenchmarkPhaseMeasurement::Measured {
            let Some(elapsed_ms) = phase.elapsed_ms else {
                return Some(format!("missing-measured-elapsed-{}", phase.phase));
            };
            if !elapsed_ms.is_finite() || elapsed_ms < 0.0 {
                return Some(format!("invalid-measured-elapsed-{}", phase.phase));
            }
            if let Some(expected) = required.remove(phase.phase.as_str()) {
                let tolerance = f64::EPSILON.max(expected.abs() * 1.0e-9);
                if (elapsed_ms - expected).abs() > tolerance {
                    return Some(format!("phase-elapsed-mismatch-{}", phase.phase));
                }
            }
        } else if phase.elapsed_ms.is_some() {
            return Some(format!("unmeasured-phase-has-elapsed-{}", phase.phase));
        }
    }
    if let Some(missing) = required.keys().next() {
        return Some(format!("missing-required-phase-{missing}"));
    }
    None
}

fn invalid_resource_metadata_for_benchmark(benchmark: &BenchmarkResult) -> Option<String> {
    let diagnostics = &benchmark.diagnostics;
    let before = &diagnostics.resources_before;
    let after = &diagnostics.resources_after;
    let delta = &diagnostics.resource_delta;

    for (name, value) in [
        ("before-cpu", before.process_cpu_seconds),
        ("after-cpu", after.process_cpu_seconds),
        ("cpu-delta", delta.process_cpu_seconds_delta),
    ] {
        if let Some(value) = value
            && (!value.is_finite() || value < 0.0)
        {
            return Some(format!("invalid-{name}"));
        }
    }

    if let Some(error) = invalid_i64_delta(
        "rss",
        before.process_rss_kib,
        after.process_rss_kib,
        delta.process_rss_kib_delta,
    ) {
        return Some(error);
    }
    if let Some(error) = invalid_i64_delta(
        "peak-rss",
        before.process_peak_rss_kib,
        after.process_peak_rss_kib,
        delta.process_peak_rss_kib_delta,
    ) {
        return Some(error);
    }
    if let Some(error) = invalid_i64_delta(
        "artifact-bytes",
        before.artifact_bytes,
        after.artifact_bytes,
        delta.artifact_bytes_delta,
    ) {
        return Some(error);
    }
    if let Some(error) = invalid_i64_delta(
        "disk-read-bytes",
        before.disk_read_bytes,
        after.disk_read_bytes,
        delta.disk_read_bytes_delta,
    ) {
        return Some(error);
    }
    if let Some(error) = invalid_i64_delta(
        "disk-write-bytes",
        before.disk_write_bytes,
        after.disk_write_bytes,
        delta.disk_write_bytes_delta,
    ) {
        return Some(error);
    }
    match (
        before.process_cpu_seconds,
        after.process_cpu_seconds,
        delta.process_cpu_seconds_delta,
    ) {
        (Some(before), Some(after), Some(delta)) => {
            let expected = after - before;
            let tolerance = f64::EPSILON.max(expected.abs() * 1.0e-9);
            if expected < 0.0 {
                return Some("negative-cpu-delta".to_string());
            }
            if (delta - expected).abs() > tolerance {
                return Some("cpu-delta-mismatch".to_string());
            }
        }
        (Some(_), Some(_), None) => return Some("missing-cpu-delta".to_string()),
        (None, None, Some(_)) | (Some(_), None, Some(_)) | (None, Some(_), Some(_)) => {
            return Some("unexpected-cpu-delta".to_string());
        }
        _ => {}
    }

    None
}

fn invalid_sqlite_metadata_for_benchmark(benchmark: &BenchmarkResult) -> Option<String> {
    if benchmark.diagnostics.operation_kind != "sql" {
        if benchmark.diagnostics.sqlite.is_some() {
            return Some("unexpected-sqlite-diagnostics".to_string());
        }
        return None;
    }
    let Some(sqlite) = &benchmark.diagnostics.sqlite else {
        return Some("missing-sqlite-diagnostics".to_string());
    };
    if sqlite.database.as_os_str().is_empty() {
        return Some("missing-sqlite-database".to_string());
    }
    if sqlite.database_size_bytes == 0 {
        return Some("invalid-sqlite-database-size".to_string());
    }
    if sqlite.page_count == Some(0) {
        return Some("invalid-sqlite-page-count".to_string());
    }
    if sqlite.page_size == Some(0) {
        return Some("invalid-sqlite-page-size".to_string());
    }
    if sqlite.query_plan.is_empty() || sqlite.query_plan.iter().any(|line| line.trim().is_empty()) {
        return Some("missing-sqlite-query-plan".to_string());
    }
    let upper_plan = sqlite.query_plan.join(" ").to_ascii_uppercase();
    if sqlite.uses_full_scan != upper_plan.contains("SCAN ") {
        return Some("sqlite-full-scan-flag-mismatch".to_string());
    }
    if sqlite.uses_temporary_btree != upper_plan.contains("TEMP B-TREE") {
        return Some("sqlite-temp-btree-flag-mismatch".to_string());
    }
    None
}

fn invalid_timing_statistics_for_benchmark(benchmark: &BenchmarkResult) -> Option<String> {
    let expected = TimingStats::from_samples(&benchmark.samples_ms);
    if benchmark.stats.samples != expected.samples {
        return Some("stats-sample-count-mismatch".to_string());
    }
    for (field, actual, expected) in [
        ("min", benchmark.stats.min_ms, expected.min_ms),
        ("mean", benchmark.stats.mean_ms, expected.mean_ms),
        ("median", benchmark.stats.median_ms, expected.median_ms),
        ("p95", benchmark.stats.p95_ms, expected.p95_ms),
        ("max", benchmark.stats.max_ms, expected.max_ms),
    ] {
        if !optional_f64_equal(actual, expected) {
            return Some(format!("stats-{field}-mismatch"));
        }
    }
    if benchmark.stats.outlier_count != expected.outlier_count {
        return Some("stats-outlier-count-mismatch".to_string());
    }
    if benchmark.stats.outliers_ms.len() != expected.outliers_ms.len() {
        return Some("stats-outlier-list-mismatch".to_string());
    }
    for (index, (actual, expected)) in benchmark
        .stats
        .outliers_ms
        .iter()
        .zip(&expected.outliers_ms)
        .enumerate()
    {
        if !f64_nearly_equal(*actual, *expected) {
            return Some(format!("stats-outlier-{index}-mismatch"));
        }
    }
    None
}

fn optional_f64_equal(actual: Option<f64>, expected: Option<f64>) -> bool {
    match (actual, expected) {
        (Some(actual), Some(expected)) => f64_nearly_equal(actual, expected),
        (None, None) => true,
        _ => false,
    }
}

fn f64_nearly_equal(actual: f64, expected: f64) -> bool {
    actual.is_finite()
        && expected.is_finite()
        && (actual - expected).abs() <= f64::EPSILON.max(expected.abs() * 1.0e-9)
}

fn invalid_i64_delta(
    name: &str,
    before: Option<u64>,
    after: Option<u64>,
    delta: Option<i64>,
) -> Option<String> {
    match (before, after, delta) {
        (Some(before), Some(after), Some(delta)) => {
            let Ok(after) = i64::try_from(after) else {
                return Some(format!("{name}-delta-overflow"));
            };
            let Ok(before) = i64::try_from(before) else {
                return Some(format!("{name}-delta-overflow"));
            };
            let expected = after - before;
            if delta != expected {
                return Some(format!("{name}-delta-mismatch"));
            }
        }
        (Some(_), Some(_), None) => return Some(format!("missing-{name}-delta")),
        (None, None, Some(_)) | (Some(_), None, Some(_)) | (None, Some(_), Some(_)) => {
            return Some(format!("unexpected-{name}-delta"));
        }
        _ => {}
    }
    None
}

fn invalid_bottleneck_summary_entries(
    summary_path: &Path,
    summary: &BenchmarkBaselineSummary,
    report_paths: &BTreeSet<PathBuf>,
    benchmark_keys: &BTreeSet<BenchmarkIdentityKey>,
) -> Vec<String> {
    let mut invalid = Vec::new();
    for bottleneck in &summary.bottlenecks {
        if bottleneck.benchmark.trim().is_empty()
            || bottleneck.area.trim().is_empty()
            || bottleneck.profile.trim().is_empty()
            || bottleneck.level.trim().is_empty()
        {
            invalid.push(format!(
                "{}:{}:missing-identity",
                summary_path.display(),
                bottleneck.benchmark
            ));
        }
        if !report_paths.contains(&bottleneck.source_report) {
            invalid.push(format!(
                "{}:{}:unknown-report:{}",
                summary_path.display(),
                bottleneck.benchmark,
                bottleneck.source_report.display()
            ));
        }
        if !benchmark_keys.contains(&(
            bottleneck.source_report.clone(),
            bottleneck.profile.clone(),
            bottleneck.level.clone(),
            bottleneck.seed,
            bottleneck.suite,
            bottleneck.area.clone(),
            bottleneck.benchmark.clone(),
        )) {
            invalid.push(format!(
                "{}:{}:unknown-benchmark",
                summary_path.display(),
                bottleneck.benchmark
            ));
        }
        if !bottleneck.median_ms.is_finite()
            || !bottleneck.p95_ms.is_finite()
            || !bottleneck.priority_score.is_finite()
            || bottleneck.median_ms < 0.0
            || bottleneck.p95_ms < 0.0
            || bottleneck.priority_score <= 0.0
        {
            invalid.push(format!(
                "{}:{}:invalid-score",
                summary_path.display(),
                bottleneck.benchmark
            ));
        }
        if bottleneck.reasons.is_empty() {
            invalid.push(format!(
                "{}:{}:missing-reasons",
                summary_path.display(),
                bottleneck.benchmark
            ));
        }
    }
    invalid.sort();
    invalid.dedup();
    invalid
}

fn analyze_baseline_growth(args: &BenchmarkAnalyzeArgs) -> Result<BenchmarkGrowthAnalysis> {
    if args.baseline_summaries.is_empty() {
        bail!("benchmark analyze requires at least one --baseline-summary");
    }
    let mut groups = BTreeMap::<(String, String), Vec<BenchmarkGrowthPoint>>::new();
    let mut failures = Vec::new();

    for summary_path in &args.baseline_summaries {
        let summary = match read_baseline_summary(summary_path) {
            Ok(summary) => summary,
            Err(error) => {
                failures.push(BenchmarkGrowthAnalysisFailure {
                    summary: summary_path.clone(),
                    report: summary_path.clone(),
                    error: format!("{error:#}"),
                });
                continue;
            }
        };
        for entry in &summary.reports {
            match read_report(&entry.report) {
                Ok(report) => {
                    for benchmark in &report.benchmarks {
                        let Some(median_ms) = benchmark.stats.median_ms else {
                            continue;
                        };
                        groups
                            .entry((entry.profile.clone(), benchmark.name.clone()))
                            .or_default()
                            .push(BenchmarkGrowthPoint {
                                summary: summary_path.clone(),
                                report: entry.report.clone(),
                                level: entry.level.clone(),
                                fixture: entry.fixture.clone(),
                                seed: entry.seed,
                                proposition_count: report.fixture.proposition_count,
                                total_object_count: report.fixture.total_object_count,
                                projected_row_count: report.fixture.projected_row_count,
                                search_index_row_count: report.fixture.search_index_row_count,
                                search_index_size_bytes: report.fixture.search_index_size_bytes,
                                database_size_bytes: report.fixture.database_size_bytes,
                                rows_returned: benchmark.rows_returned,
                                samples: benchmark.stats.samples,
                                median_ms,
                                p95_ms: benchmark.stats.p95_ms,
                                cache_state: benchmark.cache_state.clone(),
                                correctness_passed: benchmark.correctness_passed,
                            });
                    }
                }
                Err(error) => failures.push(BenchmarkGrowthAnalysisFailure {
                    summary: summary_path.clone(),
                    report: entry.report.clone(),
                    error: format!("{error:#}"),
                }),
            }
        }
    }

    let mut trends = groups
        .into_iter()
        .map(|((profile, benchmark), mut points)| {
            points.sort_by(|left, right| {
                left.proposition_count
                    .cmp(&right.proposition_count)
                    .then_with(|| left.level.cmp(&right.level))
                    .then_with(|| left.report.cmp(&right.report))
            });
            let classification = classify_growth(&profile, &points);
            BenchmarkGrowthTrend {
                profile,
                benchmark,
                classification,
                points,
            }
        })
        .collect::<Vec<_>>();
    trends.sort_by(|left, right| {
        left.profile
            .cmp(&right.profile)
            .then_with(|| left.benchmark.cmp(&right.benchmark))
    });

    let complete_trends = trends
        .iter()
        .filter(|trend| {
            REQUIRED_BENCHMARK_LEVELS
                .iter()
                .all(|level| trend.points.iter().any(|point| point.level == *level))
        })
        .count();
    let insufficient_trends = trends
        .iter()
        .filter(|trend| trend.classification.shape == BenchmarkGrowthShape::InsufficientData)
        .count();
    let incorrect_trends = trends
        .iter()
        .filter(|trend| trend.points.iter().any(|point| !point.correctness_passed))
        .count();

    Ok(BenchmarkGrowthAnalysis {
        schema_version: REPORT_SCHEMA_VERSION.to_string(),
        generated_at_unix_ms: unix_ms(),
        baseline_summaries: args.baseline_summaries.clone(),
        trend_count: trends.len(),
        complete_trend_count: complete_trends,
        insufficient_trend_count: insufficient_trends,
        incorrect_trend_count: incorrect_trends,
        failures,
        trends,
    })
}

fn derive_benchmark_budgets(args: &BenchmarkBudgetsArgs) -> Result<BenchmarkBudgets> {
    if args.baseline_summaries.is_empty() {
        bail!("benchmark budgets requires at least one --baseline-summary");
    }
    if args.warning_multiplier <= 1.0 {
        bail!("warning multiplier must be greater than 1.0");
    }
    if args.regression_multiplier < args.warning_multiplier {
        bail!("regression multiplier must be greater than or equal to warning multiplier");
    }
    if args.minimum_warning_ms < 0.0 || args.minimum_regression_ms < 0.0 {
        bail!("minimum budget margins must be non-negative");
    }

    let mut entries = Vec::new();
    let mut failures = Vec::new();
    let mut incorrect_entry_count = 0;
    for summary_path in &args.baseline_summaries {
        let summary = match read_baseline_summary(summary_path) {
            Ok(summary) => summary,
            Err(error) => {
                failures.push(BenchmarkBudgetFailure {
                    summary: summary_path.clone(),
                    report: summary_path.clone(),
                    error: format!("{error:#}"),
                });
                continue;
            }
        };
        for report_entry in &summary.reports {
            match read_report(&report_entry.report) {
                Ok(report) => {
                    for benchmark in &report.benchmarks {
                        if !benchmark.correctness_passed || !benchmark.failures.is_empty() {
                            incorrect_entry_count += 1;
                            continue;
                        }
                        let Some(median_ms) = benchmark.stats.median_ms else {
                            continue;
                        };
                        let p95_ms = benchmark.stats.p95_ms;
                        let warning_budget_ms = (median_ms * args.warning_multiplier)
                            .max(median_ms + args.minimum_warning_ms);
                        let regression_budget_ms = p95_ms
                            .unwrap_or(median_ms)
                            .max(median_ms * args.regression_multiplier)
                            .max(median_ms + args.minimum_regression_ms);
                        entries.push(BenchmarkBudgetEntry {
                            profile: report_entry.profile.clone(),
                            level: report_entry.level.clone(),
                            seed: report_entry.seed,
                            suite: benchmark.suite,
                            area: benchmark.area.clone(),
                            benchmark: benchmark.name.clone(),
                            cache_state: benchmark.cache_state.clone(),
                            baseline_median_ms: median_ms,
                            baseline_p95_ms: p95_ms,
                            warning_budget_ms,
                            regression_budget_ms,
                            samples: benchmark.stats.samples,
                            correctness_passed: benchmark.correctness_passed,
                            source_summary: summary_path.clone(),
                            source_report: report_entry.report.clone(),
                        });
                    }
                }
                Err(error) => failures.push(BenchmarkBudgetFailure {
                    summary: summary_path.clone(),
                    report: report_entry.report.clone(),
                    error: format!("{error:#}"),
                }),
            }
        }
    }
    entries.sort_by(|left, right| {
        left.profile
            .cmp(&right.profile)
            .then_with(|| left.level.cmp(&right.level))
            .then_with(|| left.suite.cmp(&right.suite))
            .then_with(|| left.benchmark.cmp(&right.benchmark))
    });
    Ok(BenchmarkBudgets {
        schema_version: REPORT_SCHEMA_VERSION.to_string(),
        generated_at_unix_ms: unix_ms(),
        baseline_summaries: args.baseline_summaries.clone(),
        warning_multiplier: args.warning_multiplier,
        regression_multiplier: args.regression_multiplier,
        minimum_warning_ms: args.minimum_warning_ms,
        minimum_regression_ms: args.minimum_regression_ms,
        entry_count: entries.len(),
        incorrect_entry_count,
        failures,
        budgets: entries,
    })
}

fn check_benchmark_budgets(args: &BenchmarkCheckBudgetsArgs) -> Result<BenchmarkBudgetCheck> {
    let budgets = read_budgets(&args.budgets)?;
    let summary = read_baseline_summary(&args.baseline_summary)?;
    let budget_map = benchmark_budget_map(&budgets);
    let mut results = Vec::new();
    let mut missing_budgets = Vec::new();
    let mut failures = Vec::new();

    for report_entry in &summary.reports {
        match read_report(&report_entry.report) {
            Ok(report) => {
                for benchmark in &report.benchmarks {
                    let key = benchmark_budget_key(
                        &report_entry.profile,
                        &report_entry.level,
                        report_entry.seed,
                        &benchmark.name,
                    );
                    let Some(budget) = budget_map.get(&key) else {
                        missing_budgets.push(BenchmarkMissingBudget {
                            key,
                            profile: report_entry.profile.clone(),
                            level: report_entry.level.clone(),
                            seed: report_entry.seed,
                            benchmark: benchmark.name.clone(),
                            report: report_entry.report.clone(),
                        });
                        continue;
                    };
                    if budget.cache_state != benchmark.cache_state {
                        failures.push(BenchmarkBudgetCheckFailure {
                            key,
                            report: report_entry.report.clone(),
                            error: format!(
                                "budget cache state `{}` does not match benchmark cache state `{}`",
                                budget.cache_state, benchmark.cache_state
                            ),
                        });
                        continue;
                    }
                    let Some(current_median_ms) = benchmark.stats.median_ms else {
                        failures.push(BenchmarkBudgetCheckFailure {
                            key,
                            report: report_entry.report.clone(),
                            error: "benchmark has no median timing".to_string(),
                        });
                        continue;
                    };
                    let classification = if current_median_ms >= budget.regression_budget_ms {
                        BenchmarkBudgetStatus::Regression
                    } else if current_median_ms >= budget.warning_budget_ms {
                        BenchmarkBudgetStatus::Warning
                    } else {
                        BenchmarkBudgetStatus::WithinBudget
                    };
                    results.push(BenchmarkBudgetCheckResult {
                        key,
                        profile: report_entry.profile.clone(),
                        level: report_entry.level.clone(),
                        seed: report_entry.seed,
                        suite: benchmark.suite,
                        area: benchmark.area.clone(),
                        benchmark: benchmark.name.clone(),
                        cache_state: benchmark.cache_state.clone(),
                        baseline_median_ms: budget.baseline_median_ms,
                        warning_budget_ms: budget.warning_budget_ms,
                        regression_budget_ms: budget.regression_budget_ms,
                        current_median_ms,
                        current_p95_ms: benchmark.stats.p95_ms,
                        percentage_over_baseline: if budget.baseline_median_ms > 0.0 {
                            Some(
                                (current_median_ms - budget.baseline_median_ms)
                                    / budget.baseline_median_ms
                                    * 100.0,
                            )
                        } else {
                            None
                        },
                        classification,
                        correctness_passed: benchmark.correctness_passed,
                        source_report: report_entry.report.clone(),
                    });
                }
            }
            Err(error) => failures.push(BenchmarkBudgetCheckFailure {
                key: baseline_report_key(report_entry),
                report: report_entry.report.clone(),
                error: format!("{error:#}"),
            }),
        }
    }

    results.sort_by(|left, right| {
        left.profile
            .cmp(&right.profile)
            .then_with(|| left.level.cmp(&right.level))
            .then_with(|| left.suite.cmp(&right.suite))
            .then_with(|| left.benchmark.cmp(&right.benchmark))
    });
    missing_budgets.sort_by(|left, right| left.key.cmp(&right.key));
    let regression_count = results
        .iter()
        .filter(|result| result.classification == BenchmarkBudgetStatus::Regression)
        .count();
    let warning_count = results
        .iter()
        .filter(|result| result.classification == BenchmarkBudgetStatus::Warning)
        .count();
    let incorrect_count = results
        .iter()
        .filter(|result| !result.correctness_passed)
        .count();
    let passed = regression_count == 0
        && incorrect_count == 0
        && missing_budgets.is_empty()
        && failures.is_empty();

    Ok(BenchmarkBudgetCheck {
        schema_version: REPORT_SCHEMA_VERSION.to_string(),
        generated_at_unix_ms: unix_ms(),
        budgets: args.budgets.clone(),
        baseline_summary: args.baseline_summary.clone(),
        checked_count: results.len(),
        warning_count,
        regression_count,
        incorrect_count,
        missing_budget_count: missing_budgets.len(),
        failure_count: failures.len(),
        passed,
        results,
        missing_budgets,
        failures,
    })
}

fn benchmark_profile_plan(args: &BenchmarkProfilePlanArgs) -> Result<BenchmarkProfilePlan> {
    if args.baseline_summaries.is_empty() {
        bail!("benchmark profile-plan requires at least one --baseline-summary");
    }
    if args.limit == 0 {
        bail!("benchmark profile-plan --limit must be greater than zero");
    }
    let mut candidates = Vec::new();
    let mut failures = Vec::new();

    for summary_path in &args.baseline_summaries {
        let summary = match read_baseline_summary(summary_path) {
            Ok(summary) => summary,
            Err(error) => {
                failures.push(BenchmarkProfilePlanFailure {
                    summary: summary_path.clone(),
                    report: summary_path.clone(),
                    error: format!("{error:#}"),
                });
                continue;
            }
        };
        for report_entry in &summary.reports {
            match read_report(&report_entry.report) {
                Ok(report) => {
                    for benchmark in &report.benchmarks {
                        let median_ms = benchmark.stats.median_ms.unwrap_or_default();
                        let p95_ms = benchmark.stats.p95_ms.unwrap_or(median_ms);
                        let mut reasons = Vec::new();
                        if user_facing_suite(benchmark.suite) {
                            reasons.push("user-facing".to_string());
                        }
                        if p95_ms >= 100.0 {
                            reasons.push("high-p95".to_string());
                        }
                        if median_ms >= 50.0 {
                            reasons.push("high-median".to_string());
                        }
                        if !benchmark.correctness_passed {
                            reasons.push("correctness-failure".to_string());
                        }
                        if let Some(sqlite) = &benchmark.diagnostics.sqlite {
                            if sqlite.uses_full_scan {
                                reasons.push("sqlite-full-scan".to_string());
                            }
                            if sqlite.uses_temporary_btree {
                                reasons.push("sqlite-temp-btree".to_string());
                            }
                        }
                        if benchmark
                            .diagnostics
                            .resource_delta
                            .process_cpu_seconds_delta
                            .is_some_and(|delta| delta >= 0.050)
                        {
                            reasons.push("cpu-time-delta".to_string());
                        }
                        if benchmark
                            .diagnostics
                            .resource_delta
                            .process_rss_kib_delta
                            .is_some_and(|delta| delta >= 1024)
                        {
                            reasons.push("rss-delta".to_string());
                        }
                        if benchmark
                            .diagnostics
                            .resource_delta
                            .process_peak_rss_kib_delta
                            .is_some_and(|delta| delta >= 1024)
                        {
                            reasons.push("peak-rss-delta".to_string());
                        }
                        if benchmark
                            .diagnostics
                            .resource_delta
                            .artifact_bytes_delta
                            .is_some_and(|delta| delta >= 1024 * 1024)
                        {
                            reasons.push("artifact-growth".to_string());
                        }
                        if benchmark
                            .diagnostics
                            .resource_delta
                            .disk_read_bytes_delta
                            .is_some_and(|delta| delta >= 1024 * 1024)
                        {
                            reasons.push("disk-read-delta".to_string());
                        }
                        if benchmark
                            .diagnostics
                            .resource_delta
                            .disk_write_bytes_delta
                            .is_some_and(|delta| delta >= 1024 * 1024)
                        {
                            reasons.push("disk-write-delta".to_string());
                        }
                        if scale_sensitive_area(&benchmark.area) {
                            reasons.push("scale-sensitive-area".to_string());
                        }
                        reasons.sort();
                        reasons.dedup();
                        let priority_score =
                            profile_priority_score(benchmark, median_ms, p95_ms, &reasons);
                        candidates.push(BenchmarkProfileCandidate {
                            profile: report_entry.profile.clone(),
                            level: report_entry.level.clone(),
                            seed: report_entry.seed,
                            suite: benchmark.suite,
                            area: benchmark.area.clone(),
                            benchmark: benchmark.name.clone(),
                            operation_kind: benchmark.diagnostics.operation_kind.clone(),
                            median_ms,
                            p95_ms,
                            rows_returned: benchmark.rows_returned,
                            measured_bytes: benchmark.measured_bytes,
                            network_payload_bytes: benchmark.network_payload_bytes,
                            process_rss_kib_delta: benchmark
                                .diagnostics
                                .resource_delta
                                .process_rss_kib_delta,
                            process_peak_rss_kib_delta: benchmark
                                .diagnostics
                                .resource_delta
                                .process_peak_rss_kib_delta,
                            process_cpu_seconds_delta: benchmark
                                .diagnostics
                                .resource_delta
                                .process_cpu_seconds_delta,
                            artifact_bytes_delta: benchmark
                                .diagnostics
                                .resource_delta
                                .artifact_bytes_delta,
                            disk_read_bytes_delta: benchmark
                                .diagnostics
                                .resource_delta
                                .disk_read_bytes_delta,
                            disk_write_bytes_delta: benchmark
                                .diagnostics
                                .resource_delta
                                .disk_write_bytes_delta,
                            correctness_passed: benchmark.correctness_passed,
                            priority_score,
                            reasons,
                            source_summary: summary_path.clone(),
                            source_report: report_entry.report.clone(),
                            sqlite_query_plan: benchmark
                                .diagnostics
                                .sqlite
                                .as_ref()
                                .map(|sqlite| sqlite.query_plan.clone())
                                .unwrap_or_default(),
                            suggested_commands: profiling_commands(&report_entry.report, benchmark),
                        });
                    }
                }
                Err(error) => failures.push(BenchmarkProfilePlanFailure {
                    summary: summary_path.clone(),
                    report: report_entry.report.clone(),
                    error: format!("{error:#}"),
                }),
            }
        }
    }

    candidates.sort_by(|left, right| {
        right
            .priority_score
            .total_cmp(&left.priority_score)
            .then_with(|| right.p95_ms.total_cmp(&left.p95_ms))
            .then_with(|| left.profile.cmp(&right.profile))
            .then_with(|| left.benchmark.cmp(&right.benchmark))
    });
    candidates.truncate(args.limit);

    Ok(BenchmarkProfilePlan {
        schema_version: REPORT_SCHEMA_VERSION.to_string(),
        generated_at_unix_ms: unix_ms(),
        baseline_summaries: args.baseline_summaries.clone(),
        limit: args.limit,
        candidate_count: candidates.len(),
        failure_count: failures.len(),
        candidates,
        failures,
    })
}

fn accept_benchmark_baseline(args: &BenchmarkAcceptArgs) -> Result<BenchmarkAcceptanceReport> {
    let audit = read_audit_report(&args.audit)?;
    let growth_analysis = read_growth_analysis(&args.growth_analysis)?;
    let budget_check = read_budget_check(&args.budget_check)?;
    let profile_plan = read_profile_plan(&args.profile_plan)?;
    let mut blockers = Vec::new();
    let mut blocker_evidence = BTreeMap::new();

    blocker_evidence.insert(
        "audit-missing-suites".to_string(),
        audit.missing_suites.len(),
    );
    blocker_evidence.insert("audit-missing-areas".to_string(), audit.missing_areas.len());
    blocker_evidence.insert(
        "audit-missing-requirements".to_string(),
        audit.missing_requirements.len(),
    );
    blocker_evidence.insert(
        "audit-missing-representative-benchmarks".to_string(),
        audit.missing_representative_benchmarks.len(),
    );
    blocker_evidence.insert(
        "audit-missing-cli-workflows".to_string(),
        audit.missing_cli_workflows.len(),
    );
    blocker_evidence.insert(
        "audit-missing-sampled-cli-workflows".to_string(),
        audit.missing_sampled_cli_workflows.len(),
    );
    blocker_evidence.insert(
        "audit-missing-cache-temperatures".to_string(),
        audit.missing_cache_temperatures.len(),
    );
    blocker_evidence.insert(
        "audit-missing-baseline-profile-levels".to_string(),
        audit.missing_baseline_profile_levels.len(),
    );
    blocker_evidence.insert(
        "audit-missing-cache-profile-levels".to_string(),
        audit.missing_cache_profile_levels.len(),
    );
    blocker_evidence.insert(
        "audit-missing-fixture-levels".to_string(),
        audit.inventory.missing_levels.len(),
    );
    blocker_evidence.insert(
        "audit-missing-fixture-profiles".to_string(),
        audit.inventory.missing_profiles.len(),
    );
    blocker_evidence.insert(
        "audit-missing-fixture-profile-levels".to_string(),
        audit.inventory.missing_profile_levels.len(),
    );
    blocker_evidence.insert(
        "audit-invalid-baseline-summaries".to_string(),
        audit.invalid_baseline_summaries.len(),
    );
    blocker_evidence.insert(
        "audit-missing-report-files".to_string(),
        audit.missing_report_files.len(),
    );
    blocker_evidence.insert(
        "audit-invalid-report-files".to_string(),
        audit.invalid_report_files.len(),
    );
    blocker_evidence.insert(
        "audit-missing-failure-logs".to_string(),
        audit.missing_failure_logs.len(),
    );
    blocker_evidence.insert(
        "audit-invalid-failure-logs".to_string(),
        audit.invalid_failure_logs.len(),
    );
    blocker_evidence.insert(
        "audit-invalid-environment-metadata".to_string(),
        audit.invalid_environment_metadata.len(),
    );
    blocker_evidence.insert(
        "audit-missing-environment-manifests".to_string(),
        audit.missing_environment_manifests.len(),
    );
    blocker_evidence.insert(
        "audit-missing-reproduce-commands".to_string(),
        audit.missing_reproduce_commands.len(),
    );
    blocker_evidence.insert(
        "audit-non-release-environment-manifests".to_string(),
        audit.non_release_environment_manifests.len(),
    );
    blocker_evidence.insert(
        "audit-non-release-reports".to_string(),
        audit.non_release_reports.len(),
    );
    blocker_evidence.insert(
        "audit-not-ready-summaries".to_string(),
        audit.not_ready_summaries.len(),
    );
    blocker_evidence.insert(
        "audit-failing-summaries".to_string(),
        audit.failing_summaries.len(),
    );
    blocker_evidence.insert(
        "audit-fixture-metadata-mismatches".to_string(),
        audit.fixture_metadata_mismatches.len(),
    );
    blocker_evidence.insert(
        "audit-missing-fixture-metadata".to_string(),
        audit.missing_fixture_metadata.len(),
    );
    blocker_evidence.insert(
        "audit-invalid-report-counts".to_string(),
        audit.invalid_report_counts.len(),
    );
    blocker_evidence.insert(
        "audit-invalid-cache-labels".to_string(),
        audit.invalid_cache_labels.len(),
    );
    blocker_evidence.insert(
        "audit-invalid-cache-metadata".to_string(),
        audit.invalid_cache_metadata.len(),
    );
    blocker_evidence.insert(
        "audit-insufficient-warmup-iterations".to_string(),
        audit.insufficient_warmup_iterations.len(),
    );
    blocker_evidence.insert(
        "audit-missing-report-metadata".to_string(),
        audit.missing_report_metadata.len(),
    );
    blocker_evidence.insert(
        "audit-missing-benchmark-metadata".to_string(),
        audit.missing_benchmark_metadata.len(),
    );
    blocker_evidence.insert(
        "audit-invalid-requirement-tags".to_string(),
        audit.invalid_requirement_tags.len(),
    );
    blocker_evidence.insert(
        "audit-missing-bottleneck-summaries".to_string(),
        audit.missing_bottleneck_summaries.len(),
    );
    blocker_evidence.insert(
        "audit-invalid-bottleneck-summaries".to_string(),
        audit.invalid_bottleneck_summaries.len(),
    );
    blocker_evidence.insert(
        "audit-insufficient-baseline-iterations".to_string(),
        audit.insufficient_baseline_iterations.len(),
    );
    blocker_evidence.insert(
        "audit-insufficient-sample-benchmarks".to_string(),
        audit.insufficient_sample_benchmarks.len(),
    );
    blocker_evidence.insert(
        "audit-invalid-sample-observations".to_string(),
        audit.invalid_sample_observations.len(),
    );
    blocker_evidence.insert(
        "audit-invalid-phase-metadata".to_string(),
        audit.invalid_phase_metadata.len(),
    );
    blocker_evidence.insert(
        "audit-invalid-resource-metadata".to_string(),
        audit.invalid_resource_metadata.len(),
    );
    blocker_evidence.insert(
        "audit-invalid-sqlite-metadata".to_string(),
        audit.invalid_sqlite_metadata.len(),
    );
    blocker_evidence.insert(
        "audit-invalid-timing-statistics".to_string(),
        audit.invalid_timing_statistics.len(),
    );
    blocker_evidence.insert(
        "audit-incorrect-benchmarks".to_string(),
        audit.incorrect_benchmarks.len(),
    );
    blocker_evidence.insert("growth-trends".to_string(), growth_analysis.trend_count);
    blocker_evidence.insert(
        "growth-complete-trends".to_string(),
        growth_analysis.complete_trend_count,
    );
    blocker_evidence.insert(
        "growth-insufficient-trends".to_string(),
        growth_analysis.insufficient_trend_count,
    );
    blocker_evidence.insert(
        "growth-incorrect-trends".to_string(),
        growth_analysis.incorrect_trend_count,
    );
    blocker_evidence.insert(
        "growth-analysis-failures".to_string(),
        growth_analysis.failures.len(),
    );
    blocker_evidence.insert("budget-checked".to_string(), budget_check.checked_count);
    blocker_evidence.insert("budget-warnings".to_string(), budget_check.warning_count);
    blocker_evidence.insert(
        "budget-regressions".to_string(),
        budget_check.regression_count,
    );
    blocker_evidence.insert(
        "budget-missing".to_string(),
        budget_check.missing_budget_count,
    );
    blocker_evidence.insert("budget-incorrect".to_string(), budget_check.incorrect_count);
    blocker_evidence.insert(
        "budget-check-failures".to_string(),
        budget_check.failure_count,
    );
    blocker_evidence.insert(
        "profile-plan-failures".to_string(),
        profile_plan.failure_count,
    );
    blocker_evidence.insert(
        "profile-plan-candidates".to_string(),
        profile_plan.candidate_count,
    );

    if !audit.ready {
        blockers.push("readiness-audit-not-ready".to_string());
    }
    if audit.total_reports == 0 {
        blockers.push("no-baseline-reports".to_string());
    }
    if audit.total_benchmarks == 0 {
        blockers.push("no-baseline-benchmarks".to_string());
    }
    if !growth_analysis.failures.is_empty() {
        blockers.push("growth-analysis-failures".to_string());
    }
    if growth_analysis.trend_count == 0 {
        blockers.push("no-growth-trends".to_string());
    }
    if growth_analysis.complete_trend_count != growth_analysis.trend_count {
        blockers.push("incomplete-growth-trends".to_string());
    }
    if growth_analysis.insufficient_trend_count > 0 {
        blockers.push("insufficient-growth-data".to_string());
    }
    if growth_analysis.incorrect_trend_count > 0 {
        blockers.push("incorrect-growth-trends".to_string());
    }
    if !budget_check.passed {
        blockers.push("budget-check-failed".to_string());
    }
    if profile_plan.failure_count > 0 {
        blockers.push("profile-plan-failures".to_string());
    }
    if profile_plan.candidate_count == 0 {
        blockers.push("no-profile-candidates".to_string());
    }
    blockers.sort();
    blockers.dedup();

    Ok(BenchmarkAcceptanceReport {
        schema_version: REPORT_SCHEMA_VERSION.to_string(),
        generated_at_unix_ms: unix_ms(),
        audit: args.audit.clone(),
        growth_analysis: args.growth_analysis.clone(),
        budget_check: args.budget_check.clone(),
        profile_plan: args.profile_plan.clone(),
        accepted: blockers.is_empty(),
        blockers,
        blocker_evidence,
        remediation_commands: audit.remediation_commands.clone(),
        total_reports: audit.total_reports,
        total_benchmarks: audit.total_benchmarks,
        fixture_matrix_ready: audit.fixture_matrix_ready,
        readiness_ready: audit.ready,
        growth_trend_count: growth_analysis.trend_count,
        complete_growth_trend_count: growth_analysis.complete_trend_count,
        insufficient_growth_trend_count: growth_analysis.insufficient_trend_count,
        incorrect_growth_trend_count: growth_analysis.incorrect_trend_count,
        budget_checked_count: budget_check.checked_count,
        budget_warning_count: budget_check.warning_count,
        budget_regression_count: budget_check.regression_count,
        budget_missing_count: budget_check.missing_budget_count,
        profile_candidate_count: profile_plan.candidate_count,
    })
}

fn profile_priority_score(
    benchmark: &BenchmarkResult,
    median_ms: f64,
    p95_ms: f64,
    reasons: &[String],
) -> f64 {
    let mut score = p95_ms + median_ms;
    if user_facing_suite(benchmark.suite) {
        score += 100.0;
    }
    if scale_sensitive_area(&benchmark.area) {
        score += 75.0;
    }
    if !benchmark.correctness_passed {
        score += 200.0;
    }
    score + reasons.len() as f64 * 10.0
}

fn user_facing_suite(suite: BenchmarkSuite) -> bool {
    matches!(
        suite,
        BenchmarkSuite::Core | BenchmarkSuite::Read | BenchmarkSuite::Search | BenchmarkSuite::Cli
    )
}

fn scale_sensitive_area(area: &str) -> bool {
    matches!(
        area,
        "read" | "search" | "sync" | "rebuild" | "integrity" | "conflict-state"
    )
}

fn profiling_commands(report: &Path, benchmark: &BenchmarkResult) -> Vec<String> {
    vec![
        format!(
            "cargo flamegraph --bin fact-sim -- benchmark run --suite {} --fixture <fixture> --iterations 1 --warmups 0 --cache-state profiling --output reports/benchmarks/profile-{}.json",
            suite_slug(benchmark.suite),
            sanitize_report_component(&benchmark.name)
        ),
        format!("cargo sim benchmark report {}", report.display()),
    ]
}

fn classify_growth(
    profile: &str,
    points: &[BenchmarkGrowthPoint],
) -> BenchmarkGrowthClassification {
    let mut unique_points = points
        .iter()
        .filter(|point| point.proposition_count > 0 && point.median_ms > 0.0)
        .collect::<Vec<_>>();
    unique_points.sort_by_key(|point| point.proposition_count);
    unique_points.dedup_by_key(|point| point.proposition_count);
    if unique_points.len() < 2 {
        return BenchmarkGrowthClassification {
            shape: BenchmarkGrowthShape::InsufficientData,
            data_factor: None,
            latency_factor: None,
            rows_factor: None,
            likely_driver: "unknown".to_string(),
            notes: vec!["requires at least two measured proposition levels".to_string()],
        };
    }

    let first = unique_points.first().expect("checked non-empty");
    let last = unique_points.last().expect("checked non-empty");
    let data_factor = last.proposition_count as f64 / first.proposition_count as f64;
    let latency_factor = last.median_ms / first.median_ms;
    let rows_factor = if first.rows_returned > 0 {
        Some(last.rows_returned as f64 / first.rows_returned as f64)
    } else {
        None
    };
    let mut notes = Vec::new();
    if unique_points.len() < REQUIRED_BENCHMARK_LEVELS.len() {
        notes.push("classification uses a partial level set".to_string());
    }
    if points.iter().any(|point| !point.correctness_passed) {
        notes.push("one or more points failed correctness checks".to_string());
    }
    if points
        .iter()
        .map(|point| point.cache_state.as_str())
        .collect::<BTreeSet<_>>()
        .len()
        > 1
    {
        notes.push("mixed cache-state labels reduce comparability".to_string());
    }

    let shape = if rows_factor.is_some_and(|factor| {
        factor > 1.5 && latency_factor >= factor * 0.5 && latency_factor <= factor * 1.5
    }) {
        BenchmarkGrowthShape::DominatedByResultSize
    } else if latency_factor <= 1.5 {
        BenchmarkGrowthShape::Constant
    } else if latency_factor > data_factor * 1.5 {
        BenchmarkGrowthShape::Superlinear
    } else if latency_factor >= data_factor * 0.5 && latency_factor <= data_factor * 1.5 {
        BenchmarkGrowthShape::Linear
    } else {
        BenchmarkGrowthShape::Logarithmic
    };
    let likely_driver = match shape {
        BenchmarkGrowthShape::Constant => "constant".to_string(),
        BenchmarkGrowthShape::DominatedByResultSize => "result-size".to_string(),
        BenchmarkGrowthShape::Superlinear | BenchmarkGrowthShape::Linear => {
            if profile.contains("revision-heavy") {
                "revision-depth-or-total-ledger-size".to_string()
            } else if profile.contains("deliberation-heavy") {
                "deliberation-depth-or-total-ledger-size".to_string()
            } else if profile.contains("sync-heavy") {
                "sync-fanout-or-total-ledger-size".to_string()
            } else if profile.contains("conflict-heavy") {
                "conflict-state-or-total-ledger-size".to_string()
            } else {
                "total-ledger-size".to_string()
            }
        }
        BenchmarkGrowthShape::Logarithmic => "indexed-lookup-or-cache".to_string(),
        BenchmarkGrowthShape::InsufficientData => "unknown".to_string(),
    };

    BenchmarkGrowthClassification {
        shape,
        data_factor: Some(data_factor),
        latency_factor: Some(latency_factor),
        rows_factor,
        likely_driver,
        notes,
    }
}

fn run_benchmarks(args: &BenchmarkRunArgs) -> Result<BenchmarkRunReport> {
    if args.iterations == 0 {
        bail!("benchmark run requires at least one iteration");
    }
    let fixture = FixtureMetadata::from_fixture(&args.fixture)?;
    let environment = EnvironmentMetadata::collect(&args.fixture)?;
    let sqlite = benchmark_database_for_fixture(&fixture)?;
    let operations = operations_for_suite(args.suite, &args.fixture, &sqlite)?;
    if operations.is_empty() {
        bail!(
            "benchmark suite {:?} has no runnable operations",
            args.suite
        );
    }

    let mut benchmarks = Vec::new();
    for operation in operations {
        benchmarks.push(run_operation(
            operation,
            args.iterations,
            args.warmups,
            &args.cache_state,
        ));
    }

    Ok(BenchmarkRunReport {
        schema_version: REPORT_SCHEMA_VERSION.to_string(),
        suite: args.suite,
        command: benchmark_run_command(args),
        generated_at_unix_ms: unix_ms(),
        environment,
        fixture,
        benchmarks,
    })
}

fn benchmark_run_command(args: &BenchmarkRunArgs) -> String {
    let mut command = format!(
        "fact-sim benchmark run --suite {} --fixture {} --iterations {} --warmups {} --cache-state {}",
        suite_slug(args.suite),
        args.fixture.display(),
        args.iterations,
        args.warmups,
        args.cache_state
    );
    if let Some(output) = &args.output {
        command.push_str(&format!(" --output {}", output.display()));
    }
    command
}

fn run_operation(
    operation: BenchmarkOperation,
    iterations: usize,
    warmups: usize,
    cache_state: &str,
) -> BenchmarkResult {
    let setup_started = Instant::now();
    let diagnostics_before = BenchmarkDiagnostics::collect(&operation);
    let setup_ms = setup_started.elapsed().as_secs_f64() * 1000.0;
    let mut failures = Vec::new();
    let warmup_started = Instant::now();
    for _ in 0..warmups {
        if let Err(error) = execute_operation(&operation) {
            failures.push(format!("warmup failed: {error:#}"));
            break;
        }
    }
    let warmup_total_ms = warmup_started.elapsed().as_secs_f64() * 1000.0;

    let mut samples = Vec::new();
    let mut rows_returned = Vec::new();
    let mut measured_bytes = Vec::new();
    let mut network_payload_bytes = Vec::new();
    let mut sample_observations = Vec::new();
    let measurement_started = Instant::now();
    for _ in 0..iterations {
        let started = Instant::now();
        match execute_operation(&operation) {
            Ok(measurement) => {
                let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
                samples.push(elapsed_ms);
                rows_returned.push(measurement.rows);
                if let Some(bytes) = measurement.bytes {
                    measured_bytes.push(bytes);
                }
                if let Some(bytes) = measurement.network_payload_bytes {
                    network_payload_bytes.push(bytes);
                }
                sample_observations.push(BenchmarkSampleObservation {
                    sample_index: sample_observations.len(),
                    elapsed_ms,
                    rows_returned: measurement.rows,
                    measured_bytes: measurement.bytes,
                    network_payload_bytes: measurement.network_payload_bytes,
                });
            }
            Err(error) => failures.push(format!("{error:#}")),
        }
    }
    let measurement_total_ms = measurement_started.elapsed().as_secs_f64() * 1000.0;
    let stats = TimingStats::from_samples(&samples);
    let result_count = rows_returned.iter().copied().max().unwrap_or_default();
    let mut diagnostics = diagnostics_before;
    diagnostics.resources_after = ResourceSnapshot::collect(&operation);
    diagnostics.resource_delta =
        ResourceDelta::between(&diagnostics.resources_before, &diagnostics.resources_after);
    let requirement_tags = operation_requirement_tags(&operation);
    let preconditions = operation_preconditions(&operation);
    let isolation_strategy = operation_isolation_strategy(&operation);
    let phase_breakdown =
        operation_phase_breakdown(&operation, setup_ms, warmup_total_ms, measurement_total_ms);
    let correctness_passed = failures.is_empty()
        && samples.len() == iterations
        && operation
            .minimum_rows
            .is_none_or(|minimum| result_count >= minimum);

    BenchmarkResult {
        name: operation.name,
        suite: operation.suite,
        area: operation.area,
        cache_state: cache_state.to_string(),
        cache_classification: cache_classification(cache_state),
        read_only: operation.read_only,
        correctness_passed,
        requirement_tags,
        preconditions,
        isolation_strategy,
        samples_ms: samples,
        sample_observations,
        stats,
        phase_timings: PhaseTimings {
            setup_ms,
            warmup_total_ms,
            measurement_total_ms,
        },
        phase_breakdown,
        rows_returned: result_count,
        measured_bytes: measured_bytes.iter().copied().max(),
        network_payload_bytes: network_payload_bytes.iter().copied().max(),
        failures,
        notes: operation.notes,
        diagnostics,
    }
}

fn execute_operation(operation: &BenchmarkOperation) -> Result<BenchmarkOperationMeasurement> {
    match &operation.kind {
        BenchmarkOperationKind::Sql { database, sql } => {
            let connection = rusqlite::Connection::open(database)
                .with_context(|| format!("failed to open `{}`", database.display()))?;
            let mut statement = connection
                .prepare(sql)
                .with_context(|| format!("failed to prepare benchmark SQL `{sql}`"))?;
            let mut rows = statement.query([])?;
            let mut count = 0;
            while rows.next()?.is_some() {
                count += 1;
            }
            Ok(BenchmarkOperationMeasurement::rows(count))
        }
        BenchmarkOperationKind::TableSize { database, tables } => {
            let connection = rusqlite::Connection::open(database)
                .with_context(|| format!("failed to open `{}`", database.display()))?;
            let mut bytes = 0;
            for table in tables {
                bytes += sqlite_table_bytes(&connection, table).unwrap_or_default();
            }
            Ok(BenchmarkOperationMeasurement {
                rows: tables.len(),
                bytes: Some(bytes),
                network_payload_bytes: None,
            })
        }
        BenchmarkOperationKind::FileInventory {
            root,
            extensions,
            bytes_kind,
        } => {
            let mut count = 0;
            let mut bytes = 0;
            for path in recursive_files(root)? {
                if path
                    .extension()
                    .and_then(|extension| extension.to_str())
                    .is_some_and(|extension| extensions.contains(extension))
                {
                    bytes += std::fs::metadata(&path)?.len();
                    count += 1;
                }
            }
            Ok(BenchmarkOperationMeasurement {
                rows: count,
                bytes: Some(bytes),
                network_payload_bytes: bytes_kind.is_network_payload().then_some(bytes),
            })
        }
        BenchmarkOperationKind::Process { program, args } => {
            let output = Command::new(program)
                .args(args)
                .output()
                .with_context(|| format!("failed to run `{}`", program.display()))?;
            if !output.status.success() {
                bail!(
                    "process benchmark `{}` failed with status {:?}: {}",
                    program.display(),
                    output.status.code(),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            Ok(BenchmarkOperationMeasurement {
                rows: output.stdout.len(),
                bytes: Some(output.stdout.len() as u64),
                network_payload_bytes: None,
            })
        }
        BenchmarkOperationKind::CliWorkflow { program, workflow } => {
            execute_cli_workflow_operation(program, workflow)
        }
        BenchmarkOperationKind::Scenario { yaml } => {
            let scenario = fact_sim_dsl::Scenario::from_yaml_str(yaml)?;
            let run = fact_sim_runner::run_scenario(&scenario)?;
            Ok(BenchmarkOperationMeasurement::rows(
                run.receipts.len() + run.cli_receipts.len(),
            ))
        }
        BenchmarkOperationKind::Commitment { database, mode } => {
            execute_commitment_operation(database, *mode)
        }
        BenchmarkOperationKind::Validation { database, mode } => {
            execute_validation_operation(database, *mode)
        }
        BenchmarkOperationKind::SnapshotSidecarVerify {
            fixture,
            database,
            snapshot,
        } => execute_snapshot_sidecar_verify(fixture, database, snapshot),
        BenchmarkOperationKind::SnapshotFrame { database, mode } => {
            execute_snapshot_frame_operation(database, *mode)
        }
        BenchmarkOperationKind::SearchIndex {
            database,
            ledger_id,
            query,
            limit,
        } => execute_search_index_operation(database, *ledger_id, query, *limit),
        BenchmarkOperationKind::HttpRouter {
            database,
            method,
            path,
            headers,
            expected_status,
            caller_auth,
            body,
        } => execute_http_router_operation(
            database,
            method,
            path,
            headers,
            *expected_status,
            *caller_auth,
            body.as_deref(),
        ),
        BenchmarkOperationKind::HttpFixtureRoute { database, route } => {
            let request = http_fixture_request(database, *route)?;
            execute_http_router_operation(
                database,
                &request.method,
                &request.path,
                &request.headers,
                request.expected_status,
                request.caller_auth,
                request.body.as_deref(),
            )
        }
    }
}

fn execute_search_index_operation(
    database: &Path,
    ledger_id: uuid::Uuid,
    query: &str,
    limit: usize,
) -> Result<BenchmarkOperationMeasurement> {
    let store = fact_store::Store::open(database).with_context(|| {
        format!(
            "failed to open `{}` for search benchmark",
            database.display()
        )
    })?;
    let hits = store
        .search_markdown_index(ledger_id.as_bytes(), query, limit)
        .with_context(|| format!("failed to search markdown index for `{query}`"))?;
    let bytes = hits
        .iter()
        .map(|hit| {
            hit.object_type.len()
                + hit.content_hash.hex().len()
                + hit.score.len()
                + hit.extraction_profile.len()
        })
        .sum::<usize>();
    Ok(BenchmarkOperationMeasurement {
        rows: hits.len(),
        bytes: Some(bytes as u64),
        network_payload_bytes: None,
    })
}

#[derive(Debug)]
struct HttpFixtureRequest {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    expected_status: Option<u16>,
    caller_auth: HttpCallerAuth,
    body: Option<String>,
}

#[derive(Debug, Clone, Copy)]
enum HttpCallerAuth {
    Reference,
    Disabled,
}

#[derive(Debug)]
struct HttpFixtureIdentity {
    ledger_id: uuid::Uuid,
    object_id: uuid::Uuid,
    content_hash: fact_core::Hash,
}

fn http_fixture_request(database: &Path, route: HttpFixtureRoute) -> Result<HttpFixtureRequest> {
    let identity = http_fixture_identity(database)?.with_context(|| {
        format!(
            "`{}` does not contain usable ledger_id/object_id fixture metadata for HTTP route benchmarks",
            database.display()
        )
    })?;
    match route {
        HttpFixtureRoute::ObjectFetch => Ok(HttpFixtureRequest {
            method: "GET".to_string(),
            path: format!(
                "/facts/ledgers/{}/objects/{}",
                identity.ledger_id, identity.object_id
            ),
            headers: BTreeMap::new(),
            expected_status: None,
            caller_auth: HttpCallerAuth::Reference,
            body: None,
        }),
        HttpFixtureRoute::BatchFetch => {
            let body = canonical_http_json(&serde_json::json!({
                "schema": "facts-protocol-fetch-v0",
                "ids": [identity.object_id.to_string()],
                "hashes": [identity.content_hash.hex()],
                "include_missing": true
            }))?;
            Ok(HttpFixtureRequest {
                method: "POST".to_string(),
                path: format!("/facts/ledgers/{}/objects:fetch", identity.ledger_id),
                headers: http_json_headers("application/fact+json", &body),
                expected_status: None,
                caller_auth: HttpCallerAuth::Reference,
                body: Some(body),
            })
        }
        HttpFixtureRoute::Query => {
            let body = canonical_http_json(&serde_json::json!({
                "schema": "facts-protocol-query-v0",
                "query_type": "object",
                "search_text": null,
                "ledger_ids": [identity.ledger_id.to_string()],
                "object_types": [],
                "scope": {
                    "actor_ids": [],
                    "deliberation_ids": [],
                    "proposition_ids": [],
                    "revision_ids": []
                },
                "status": {
                    "accepted": null,
                    "archived": null,
                    "divergent": null,
                    "rejected": null,
                    "settled": null,
                    "withdrawn": null
                },
                "relationships": [],
                "search_profile": {
                    "id": "hash-asc-v0",
                    "version": "0"
                },
                "extraction_profile": {
                    "id": "facts-markdown-extraction-v0",
                    "version": "0"
                },
                "embedding_model": null,
                "ordering_profile": "hash-asc-v0",
                "page_size": 100,
                "prior_cursor": null
            }))?;
            Ok(HttpFixtureRequest {
                method: "POST".to_string(),
                path: format!("/facts/ledgers/{}/query", identity.ledger_id),
                headers: http_json_headers("application/fact-query+json", &body),
                expected_status: None,
                caller_auth: HttpCallerAuth::Reference,
                body: Some(body),
            })
        }
        HttpFixtureRoute::Pull => {
            let body = canonical_http_json(&serde_json::json!({
                "schema": "facts-protocol-pull-v0",
                "scope": http_full_ledger_scope(identity.ledger_id),
                "known_commitment_hash": null,
                "known_object_hashes": [],
                "limit": 1000,
                "cursor": null,
                "prefer_snapshot": false
            }))?;
            Ok(HttpFixtureRequest {
                method: "POST".to_string(),
                path: format!("/facts/ledgers/{}/objects:pull", identity.ledger_id),
                headers: http_json_headers("application/fact+json", &body),
                expected_status: None,
                caller_auth: HttpCallerAuth::Reference,
                body: Some(body),
            })
        }
        HttpFixtureRoute::MalformedPushPayload => {
            let body = String::new();
            Ok(HttpFixtureRequest {
                method: "POST".to_string(),
                path: format!("/facts/ledgers/{}/objects:push", identity.ledger_id),
                headers: BTreeMap::from([
                    (
                        "content-type".to_string(),
                        "application/fact-bundle".to_string(),
                    ),
                    (
                        "content-digest".to_string(),
                        content_digest(body.as_bytes()),
                    ),
                    ("facts-ledger".to_string(), identity.ledger_id.to_string()),
                ]),
                expected_status: Some(400),
                caller_auth: HttpCallerAuth::Disabled,
                body: Some(body),
            })
        }
        HttpFixtureRoute::AuthChallenge => {
            let body = Vec::new();
            Ok(HttpFixtureRequest {
                method: "POST".to_string(),
                path: format!("/facts/ledgers/{}/objects:push", identity.ledger_id),
                headers: BTreeMap::from([
                    (
                        "content-type".to_string(),
                        "application/fact-bundle".to_string(),
                    ),
                    ("content-digest".to_string(), content_digest(&body)),
                    ("facts-ledger".to_string(), identity.ledger_id.to_string()),
                ]),
                expected_status: Some(401),
                caller_auth: HttpCallerAuth::Reference,
                body: Some(String::new()),
            })
        }
        HttpFixtureRoute::InvalidDigest => Ok(HttpFixtureRequest {
            method: "POST".to_string(),
            path: format!("/facts/ledgers/{}/objects:fetch", identity.ledger_id),
            headers: BTreeMap::from([
                (
                    "content-type".to_string(),
                    "application/fact+json".to_string(),
                ),
                (
                    "content-digest".to_string(),
                    "sha-256=:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=:".to_string(),
                ),
            ]),
            expected_status: Some(400),
            caller_auth: HttpCallerAuth::Reference,
            body: Some(String::new()),
        }),
        HttpFixtureRoute::Commitment => Ok(HttpFixtureRequest {
            method: "GET".to_string(),
            path: format!("/facts/ledgers/{}/commitment", identity.ledger_id),
            headers: BTreeMap::new(),
            expected_status: None,
            caller_auth: HttpCallerAuth::Reference,
            body: None,
        }),
    }
}

fn http_fixture_identity(database: &Path) -> Result<Option<HttpFixtureIdentity>> {
    let connection = rusqlite::Connection::open(database)
        .with_context(|| format!("failed to open `{}`", database.display()))?;
    let columns = table_columns(&connection, "protocol_object")?;
    if !columns.contains("ledger_id")
        || !columns.contains("object_id")
        || !columns.contains("content_hash")
    {
        return Ok(None);
    }
    let identity = connection
        .query_row(
            "SELECT ledger_id,object_id,content_hash FROM protocol_object WHERE ledger_id IS NOT NULL ORDER BY object_id LIMIT 1",
            [],
            |row| {
                Ok((
                    uuid_from_row_index(row, 0)?,
                    uuid_from_row_index(row, 1)?,
                    content_hash_from_row_index(row, 2).ok(),
                ))
            },
        )
        .optional()?;
    Ok(identity.and_then(|(ledger_id, object_id, content_hash)| {
        Some(HttpFixtureIdentity {
            ledger_id: ledger_id?,
            object_id: object_id?,
            content_hash: content_hash?,
        })
    }))
}

fn fixture_ledger_id(database: &Path) -> Result<Option<uuid::Uuid>> {
    let connection = rusqlite::Connection::open(database)
        .with_context(|| format!("failed to open `{}`", database.display()))?;
    let columns = table_columns(&connection, "protocol_object")?;
    if !columns.contains("ledger_id") {
        return Ok(None);
    }
    Ok(connection
        .query_row(
            "SELECT ledger_id FROM protocol_object WHERE ledger_id IS NOT NULL ORDER BY object_id LIMIT 1",
            [],
            |row| uuid_from_row_index(row, 0),
        )
        .optional()?
        .flatten())
}

fn http_full_ledger_scope(ledger_id: uuid::Uuid) -> serde_json::Value {
    serde_json::json!({
        "ledger_id": ledger_id.to_string(),
        "snapshot_boundary": null,
        "query_digest": null,
        "object_types": [],
        "actor_ids": [],
        "proposition_ids": [],
        "revision_ids": [],
        "deliberation_ids": [],
        "filters": {}
    })
}

fn canonical_http_json(value: &serde_json::Value) -> Result<String> {
    let json = serde_json::to_vec(value)?;
    let canonical = fact_canonical::encode(&json).context("failed to canonicalize HTTP body")?;
    String::from_utf8(canonical).context("canonical HTTP body was not UTF-8")
}

fn http_json_headers(content_type: &str, body: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("content-type".to_string(), content_type.to_string()),
        (
            "content-digest".to_string(),
            content_digest(body.as_bytes()),
        ),
    ])
}

fn content_digest(body: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(body);
    format!("sha-256=:{}:", base64_encode(&hash.finalize()))
}

fn base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        output.push(ALPHABET[(first >> 2) as usize] as char);
        output.push(ALPHABET[(((first & 0b0000_0011) << 4) | (second >> 4)) as usize] as char);
        if chunk.len() > 1 {
            output.push(ALPHABET[(((second & 0b0000_1111) << 2) | (third >> 6)) as usize] as char);
        } else {
            output.push('=');
        }
        if chunk.len() > 2 {
            output.push(ALPHABET[(third & 0b0011_1111) as usize] as char);
        } else {
            output.push('=');
        }
    }
    output
}

fn execute_http_router_operation(
    database: &Path,
    method: &str,
    path: &str,
    headers: &BTreeMap<String, String>,
    expected_status: Option<u16>,
    caller_auth: HttpCallerAuth,
    body: Option<&str>,
) -> Result<BenchmarkOperationMeasurement> {
    let database = database.to_path_buf();
    let method = method.to_string();
    let path = path.to_string();
    let headers = headers.clone();
    let body = body.map(str::to_string);
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("failed to build HTTP benchmark runtime")?;
        runtime.block_on(async move {
            let store = fact_store::Store::open(&database).with_context(|| {
                format!("failed to open `{}` for HTTP benchmark", database.display())
            })?;
            let state = match caller_auth {
                HttpCallerAuth::Reference => {
                    fact_http::AppState::new(store, "http://127.0.0.1/facts")
                }
                HttpCallerAuth::Disabled => fact_http::AppState::new_without_caller_auth(
                    store,
                    "http://127.0.0.1/facts",
                    fact_crypto::SigningKey::from_seed(&[7_u8; 32])
                        .context("failed to build HTTP benchmark coordinator key")?,
                    fact_core::ObjectId::new_v7(),
                ),
            };
            let app = fact_http::router(state);
            let mut request = Request::builder()
                .method(method.as_str())
                .uri(path.as_str())
                .header("facts-protocol-version", "0");
            for (name, value) in headers {
                request = request.header(name, value);
            }
            let request = request
                .body(Body::from(body.unwrap_or_default()))
                .with_context(|| format!("failed to build HTTP benchmark request `{path}`"))?;
            let response = app
                .oneshot(request)
                .await
                .with_context(|| format!("HTTP benchmark request `{path}` failed"))?;
            let status = response.status();
            let bytes = to_bytes(response.into_body(), usize::MAX)
                .await
                .with_context(|| format!("failed to read HTTP benchmark response `{path}`"))?;
            let status_matches = expected_status
                .map(|expected| status.as_u16() == expected)
                .unwrap_or_else(|| status.is_success());
            if !status_matches {
                bail!(
                    "HTTP benchmark request `{}` returned {} instead of {}: {}",
                    path,
                    status,
                    expected_status
                        .map(|status| status.to_string())
                        .unwrap_or_else(|| "2xx".to_string()),
                    String::from_utf8_lossy(&bytes)
                );
            }
            Ok(BenchmarkOperationMeasurement {
                rows: 1,
                bytes: Some(bytes.len() as u64),
                network_payload_bytes: Some(bytes.len() as u64),
            })
        })
    })
    .join()
    .map_err(|_| anyhow::anyhow!("HTTP benchmark runtime thread panicked"))?
}

fn execute_commitment_operation(
    database: &Path,
    mode: CommitmentBenchmarkMode,
) -> Result<BenchmarkOperationMeasurement> {
    let hashes = content_hashes_for_database(database)?;
    if hashes.is_empty() {
        bail!(
            "commitment benchmark `{}` has no content hashes",
            database.display()
        );
    }
    match mode {
        CommitmentBenchmarkMode::Create => {
            fact_sdk::commitment::create_commitment(hashes.clone())?;
            Ok(BenchmarkOperationMeasurement::rows(hashes.len()))
        }
        CommitmentBenchmarkMode::Verify => {
            let commitment = fact_sdk::commitment::create_commitment(hashes.clone())?;
            let expected = commitment.root.parse::<fact_core::Hash>()?;
            let verification = fact_sdk::commitment::verify_commitment(hashes.clone(), expected)?;
            if !verification.valid {
                bail!(
                    "commitment verification failed for `{}`",
                    database.display()
                );
            }
            Ok(BenchmarkOperationMeasurement::rows(hashes.len()))
        }
        CommitmentBenchmarkMode::InclusionProof => {
            let target = hashes[hashes.len() / 2];
            let proof = fact_sdk::commitment::create_inclusion_proof(hashes, target)?;
            Ok(BenchmarkOperationMeasurement::rows(proof.steps.len() + 1))
        }
        CommitmentBenchmarkMode::NonInclusionProof => {
            let target = missing_content_hash(&hashes);
            let proof = fact_sdk::commitment::create_non_inclusion_proof(hashes, target)?;
            Ok(BenchmarkOperationMeasurement::rows(
                usize::from(proof.left.is_some()) + usize::from(proof.right.is_some()),
            ))
        }
    }
}

fn execute_validation_operation(
    database: &Path,
    mode: ValidationBenchmarkMode,
) -> Result<BenchmarkOperationMeasurement> {
    let payloads = protocol_payloads_for_database(database)?;
    if payloads.is_empty() {
        bail!(
            "validation benchmark `{}` has no protocol payloads",
            database.display()
        );
    }
    match mode {
        ValidationBenchmarkMode::ValidPayloads => {
            for payload in &payloads {
                fact_sdk::validation::validate_object(payload)?;
            }
            let bytes = payloads.iter().map(Vec::len).sum::<usize>() as u64;
            Ok(BenchmarkOperationMeasurement {
                rows: payloads.len(),
                bytes: Some(bytes),
                network_payload_bytes: None,
            })
        }
        ValidationBenchmarkMode::InvalidObjectRejection => {
            let mut rejected = 0;
            for _ in payloads.iter().take(100) {
                if fact_sdk::validation::validate_object(b"{}").is_err() {
                    rejected += 1;
                }
            }
            if rejected == 0 {
                bail!("invalid-object rejection benchmark did not reject any payloads");
            }
            Ok(BenchmarkOperationMeasurement {
                rows: rejected,
                bytes: Some(rejected as u64 * 2),
                network_payload_bytes: None,
            })
        }
    }
}

fn protocol_payloads_for_database(database: &Path) -> Result<Vec<Vec<u8>>> {
    let connection = rusqlite::Connection::open(database)
        .with_context(|| format!("failed to open `{}`", database.display()))?;
    let mut statement = connection
        .prepare("SELECT payload FROM protocol_object ORDER BY object_id")
        .context("failed to prepare protocol-payload enumeration")?;
    let rows = statement.query_map([], |row| row.get::<_, Vec<u8>>(0))?;
    Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
}

fn execute_snapshot_sidecar_verify(
    fixture: &Path,
    database: &Path,
    snapshot: &Path,
) -> Result<BenchmarkOperationMeasurement> {
    let report = read_json_file(snapshot)
        .with_context(|| format!("failed to read snapshot sidecar `{}`", snapshot.display()))?;
    if report["snapshot_kind"].as_str() != Some("portable-object-bundle-inventory") {
        bail!(
            "snapshot sidecar `{}` has an unknown kind",
            snapshot.display()
        );
    }
    if report["bundle_object_count"].as_u64().unwrap_or_default() == 0 {
        bail!(
            "snapshot sidecar `{}` reports an empty bundle",
            snapshot.display()
        );
    }
    if report["portable_bundle_count"].as_u64().unwrap_or_default() == 0 {
        bail!(
            "snapshot sidecar `{}` reports no portable bundles",
            snapshot.display()
        );
    }
    let hashes = content_hashes_for_database(database)?;
    let object_count = protocol_object_count(database)?;
    let snapshots = report["database_snapshots"]
        .as_array()
        .context("snapshot sidecar is missing database snapshots")?;
    let database_entry = matching_snapshot_database_entry(fixture, database, snapshots)
        .with_context(|| {
            format!(
                "snapshot sidecar does not describe `{}`",
                database.display()
            )
        })?;
    if database_entry["hash_count"].as_u64() != Some(hashes.len() as u64) {
        bail!("snapshot sidecar hash count does not match database");
    }
    if database_entry["object_count"].as_u64() != Some(object_count) {
        bail!("snapshot sidecar object count does not match database");
    }
    let type_total: u64 = database_entry["object_counts_by_type"]
        .as_object()
        .map(|counts| counts.values().filter_map(serde_json::Value::as_u64).sum())
        .unwrap_or_default();
    if type_total != object_count {
        bail!("snapshot sidecar object type counts do not sum to database object count");
    }
    let snapshot_bytes = std::fs::metadata(snapshot)?.len();
    Ok(BenchmarkOperationMeasurement {
        rows: hashes.len(),
        bytes: Some(snapshot_bytes),
        network_payload_bytes: None,
    })
}

fn execute_cli_workflow_operation(
    program: &Path,
    workflow: &str,
) -> Result<BenchmarkOperationMeasurement> {
    let home = tempfile::tempdir().context("failed to create temporary FACT_HOME")?;
    let measured = match workflow {
        "status" => {
            run_fact_command(program, home.path(), &["init"])?;
            run_fact_command(program, home.path(), &["--json", "status"])?
        }
        "list" => {
            seed_fact_proposition(program, home.path(), true)?;
            run_fact_command(program, home.path(), &["--json", "list", "--limit", "20"])?
        }
        "search" => {
            seed_fact_proposition(program, home.path(), true)?;
            run_fact_command(program, home.path(), &["--json", "search", "Benchmark"])?
        }
        "pending" => {
            seed_fact_proposition(program, home.path(), false)?;
            run_fact_command(program, home.path(), &["--json", "pending"])?
        }
        "revisions" => {
            let reference = seed_fact_proposition(program, home.path(), true)?;
            run_fact_command(program, home.path(), &["--json", "revisions", &reference])?
        }
        "history" => {
            let reference = seed_fact_proposition(program, home.path(), true)?;
            run_fact_command(program, home.path(), &["--json", "history", &reference])?
        }
        "echo" => {
            let reference = seed_fact_proposition(program, home.path(), true)?;
            run_fact_command(program, home.path(), &["echo", &reference])?
        }
        "propose" => {
            run_fact_command(program, home.path(), &["init"])?;
            run_fact_command(
                program,
                home.path(),
                &[
                    "--json",
                    "propose",
                    "--message",
                    "# Benchmark CLI proposition\n\nMeasured propose command.",
                ],
            )?
        }
        "revise" => {
            let reference = seed_fact_proposition(program, home.path(), true)?;
            run_fact_command(
                program,
                home.path(),
                &[
                    "--json",
                    "revise",
                    &reference,
                    "--message",
                    "# Benchmark CLI proposition\n\nMeasured revise command.",
                ],
            )?
        }
        "accept" => {
            let reference = seed_fact_proposition(program, home.path(), false)?;
            run_fact_command(program, home.path(), &["--json", "accept", &reference])?
        }
        "push" => {
            let source_home = tempfile::tempdir().context("failed to create source FACT_HOME")?;
            seed_fact_proposition(program, source_home.path(), true)?;
            let source_status =
                run_fact_json_command(program, source_home.path(), &["--json", "status"])?;
            let ledger_id = source_status["ledger_id"]
                .as_str()
                .context("fact status JSON omitted ledger_id")?
                .to_string();
            let source_database = source_home.path().join("ledgers").join("default.sqlite");
            let bundle = source_home.path().join("push.bundle");
            run_fact_command(
                program,
                source_home.path(),
                &[
                    "--json",
                    "pull",
                    &source_database.display().to_string(),
                    &ledger_id,
                    &bundle.display().to_string(),
                ],
            )?;
            run_fact_command(program, home.path(), &["init"])?;
            let target_database = home.path().join("ledgers").join("default.sqlite");
            run_fact_command(
                program,
                home.path(),
                &[
                    "--json",
                    "push",
                    &target_database.display().to_string(),
                    &bundle.display().to_string(),
                ],
            )?
        }
        "pull" => {
            seed_fact_proposition(program, home.path(), true)?;
            let source_status = run_fact_json_command(program, home.path(), &["--json", "status"])?;
            let ledger_id = source_status["ledger_id"]
                .as_str()
                .context("fact status JSON omitted ledger_id")?
                .to_string();
            let source_database = home.path().join("ledgers").join("default.sqlite");
            let bundle = home.path().join("pull.bundle");
            run_fact_command(
                program,
                home.path(),
                &[
                    "--json",
                    "pull",
                    &source_database.display().to_string(),
                    &ledger_id,
                    &bundle.display().to_string(),
                ],
            )?
        }
        other => bail!("unknown CLI workflow benchmark `{other}`"),
    };
    Ok(BenchmarkOperationMeasurement {
        rows: measured.stdout.len(),
        bytes: Some(measured.stdout.len() as u64),
        network_payload_bytes: None,
    })
}

fn seed_fact_proposition(program: &Path, home: &Path, accepted: bool) -> Result<String> {
    run_fact_command(program, home, &["init"])?;
    let mut args = vec![
        "--json",
        "propose",
        "--message",
        "# Benchmark CLI proposition\n\nSeed content for sampled CLI workflow.",
    ];
    if accepted {
        args.extend(["--decision", "accept"]);
    }
    let value = run_fact_json_command(program, home, &args)?;
    value["proposition_id"]
        .as_str()
        .map(str::to_string)
        .context("fact propose JSON omitted proposition_id")
}

fn run_fact_json_command(program: &Path, home: &Path, args: &[&str]) -> Result<serde_json::Value> {
    let output = run_fact_command(program, home, args)?;
    serde_json::from_slice(&output.stdout)
        .with_context(|| format!("failed to parse `fact {}` JSON output", args.join(" ")))
}

fn run_fact_command(program: &Path, home: &Path, args: &[&str]) -> Result<std::process::Output> {
    let output = Command::new(program)
        .env("FACT_HOME", home)
        .args(args)
        .output()
        .with_context(|| format!("failed to run `{}`", program.display()))?;
    if !output.status.success() {
        bail!(
            "fact CLI benchmark `{}` failed with status {:?}: {}",
            args.join(" "),
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(output)
}

fn execute_snapshot_frame_operation(
    database: &Path,
    mode: SnapshotFrameBenchmarkMode,
) -> Result<BenchmarkOperationMeasurement> {
    let (ledger_id, objects) = snapshot_frame_objects_for_database(database)?;
    if objects.is_empty() {
        bail!(
            "snapshot frame benchmark `{}` has no protocol objects",
            database.display()
        );
    }
    let manifest = snapshot_manifest_for_objects(ledger_id, &objects)?;
    let snapshot = fact_commitment::encode_snapshot(&manifest, &objects)
        .context("failed to encode FACTSNAP frame")?;
    match mode {
        SnapshotFrameBenchmarkMode::Encode => Ok(BenchmarkOperationMeasurement {
            rows: objects.len(),
            bytes: Some(snapshot.len() as u64),
            network_payload_bytes: None,
        }),
        SnapshotFrameBenchmarkMode::Decode => {
            let decoded = fact_commitment::decode_snapshot(&snapshot)
                .context("failed to decode FACTSNAP frame")?;
            if decoded.objects.len() != objects.len() || decoded.manifest != manifest {
                bail!("decoded FACTSNAP frame does not match encoded manifest/object count");
            }
            Ok(BenchmarkOperationMeasurement {
                rows: decoded.objects.len(),
                bytes: Some(snapshot.len() as u64),
                network_payload_bytes: None,
            })
        }
    }
}

fn snapshot_frame_objects_for_database(database: &Path) -> Result<SnapshotFrameObjects> {
    let connection = rusqlite::Connection::open(database)
        .with_context(|| format!("failed to open `{}`", database.display()))?;
    let has_ledger_id = table_columns(&connection, "protocol_object")?
        .iter()
        .any(|column| column == "ledger_id");
    let sql = if has_ledger_id {
        "SELECT ledger_id,content_hash,cose FROM protocol_object ORDER BY content_hash"
    } else {
        "SELECT NULL,content_hash,cose FROM protocol_object ORDER BY content_hash"
    };
    let mut statement = connection
        .prepare(sql)
        .context("failed to prepare snapshot object enumeration")?;
    let mut rows = statement.query([])?;
    let mut ledger_id = None;
    let mut objects = Vec::new();
    while let Some(row) = rows.next()? {
        if ledger_id.is_none() {
            ledger_id = ledger_uuid_from_row(row)?;
        }
        let hash = content_hash_from_row_index(row, 1)?;
        let cose: Vec<u8> = row.get(2)?;
        objects.push((hash, cose));
    }
    Ok((ledger_id.unwrap_or_else(uuid::Uuid::now_v7), objects))
}

fn table_columns(connection: &rusqlite::Connection, table_name: &str) -> Result<BTreeSet<String>> {
    let escaped_table_name = table_name.replace('\'', "''");
    let mut statement = connection
        .prepare(&format!("PRAGMA table_info('{escaped_table_name}')"))
        .with_context(|| format!("failed to inspect `{table_name}` columns"))?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(1))
        .with_context(|| format!("failed to read `{table_name}` column metadata"))?;
    let mut columns = BTreeSet::new();
    for row in rows {
        columns.insert(row?);
    }
    Ok(columns)
}

fn ledger_uuid_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Option<uuid::Uuid>> {
    uuid_from_row_index(row, 0)
}

fn uuid_from_row_index(
    row: &rusqlite::Row<'_>,
    index: usize,
) -> rusqlite::Result<Option<uuid::Uuid>> {
    match row.get_ref(index)? {
        rusqlite::types::ValueRef::Null => Ok(None),
        rusqlite::types::ValueRef::Blob(bytes) => Ok(uuid::Uuid::from_slice(bytes).ok()),
        rusqlite::types::ValueRef::Text(bytes) => Ok(std::str::from_utf8(bytes)
            .ok()
            .and_then(|value| value.parse::<uuid::Uuid>().ok())),
        _ => Ok(None),
    }
}

fn snapshot_manifest_for_objects(
    ledger_id: uuid::Uuid,
    objects: &[(fact_core::Hash, Vec<u8>)],
) -> Result<Vec<u8>> {
    let scope = serde_json::json!({
        "ledger_id": ledger_id,
        "snapshot_boundary": null,
        "query_digest": null,
        "object_types": [],
        "actor_ids": [],
        "proposition_ids": [],
        "revision_ids": [],
        "deliberation_ids": [],
        "filters": {}
    });
    let scope_hash =
        fact_core::Hash::digest(&fact_canonical::encode(&serde_json::to_vec(&scope)?)?);
    let tree =
        fact_commitment::MerkleTree::new(objects.iter().map(|(hash, _)| *hash).collect::<Vec<_>>())
            .context("failed to build snapshot commitment tree")?;
    let key = fact_crypto::SigningKey::from_seed(&[6_u8; 32])
        .context("failed to build benchmark snapshot signing key")?;
    let mut commitment = serde_json::json!({
        "schema": "facts-protocol-commitment-v0",
        "coordinator_actor_id": uuid::Uuid::now_v7(),
        "ledger_id": ledger_id,
        "scope": scope,
        "scope_hash": scope_hash.hex(),
        "snapshot_id": null,
        "tree_profile": "facts-protocol-merkle-v0",
        "root_hash": tree.root.hex(),
        "object_count": objects.len(),
        "created_at": "2026-07-30T00:00:00.000Z",
        "previous_commitment_hash": null,
        "signing_key_fingerprint": key.fingerprint().hex()
    });
    let preimage = fact_canonical::encode(&serde_json::to_vec(&commitment)?)?;
    commitment["snapshot_id"] = serde_json::json!(fact_core::Hash::digest(&preimage).hex());
    let commitment_payload = fact_canonical::encode(&serde_json::to_vec(&commitment)?)?;
    let protected = fact_crypto::coordinator_protected(
        key.public_key(),
        "commitment",
        "0",
        Some(*ledger_id.as_bytes()),
    );
    let signed_commitment =
        fact_crypto::encode_sign1(&fact_crypto::sign1(&protected, &commitment_payload, &key));
    fact_canonical::encode(&serde_json::to_vec(&serde_json::json!({
        "schema": "facts-protocol-snapshot-v0",
        "protocol_version": 0,
        "ledger_id": ledger_id,
        "scope": commitment["scope"],
        "filters": {},
        "commitment": encode_base64url(&signed_commitment),
        "object_count": objects.len(),
        "profile": "facts-protocol-snapshot-v0"
    }))?)
    .context("failed to canonicalize snapshot manifest")
}

fn encode_base64url(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    let mut index = 0;
    while index + 3 <= bytes.len() {
        let block = ((bytes[index] as u32) << 16)
            | ((bytes[index + 1] as u32) << 8)
            | bytes[index + 2] as u32;
        output.push(TABLE[((block >> 18) & 0x3f) as usize] as char);
        output.push(TABLE[((block >> 12) & 0x3f) as usize] as char);
        output.push(TABLE[((block >> 6) & 0x3f) as usize] as char);
        output.push(TABLE[(block & 0x3f) as usize] as char);
        index += 3;
    }
    match bytes.len() - index {
        1 => {
            let block = (bytes[index] as u32) << 16;
            output.push(TABLE[((block >> 18) & 0x3f) as usize] as char);
            output.push(TABLE[((block >> 12) & 0x3f) as usize] as char);
        }
        2 => {
            let block = ((bytes[index] as u32) << 16) | ((bytes[index + 1] as u32) << 8);
            output.push(TABLE[((block >> 18) & 0x3f) as usize] as char);
            output.push(TABLE[((block >> 12) & 0x3f) as usize] as char);
            output.push(TABLE[((block >> 6) & 0x3f) as usize] as char);
        }
        _ => {}
    }
    output
}

#[derive(Debug, Clone, Copy)]
struct BenchmarkOperationMeasurement {
    rows: usize,
    bytes: Option<u64>,
    network_payload_bytes: Option<u64>,
}

impl BenchmarkOperationMeasurement {
    fn rows(rows: usize) -> Self {
        Self {
            rows,
            bytes: None,
            network_payload_bytes: None,
        }
    }
}

fn matching_snapshot_database_entry<'a>(
    fixture: &Path,
    database: &Path,
    snapshots: &'a [serde_json::Value],
) -> Option<&'a serde_json::Value> {
    if snapshots.len() == 1 {
        return snapshots.first();
    }
    let database_file = database.file_name();
    let canonical_database = database.canonicalize().ok();
    snapshots.iter().find(|entry| {
        let Some(path) = entry["database"].as_str() else {
            return false;
        };
        let snapshot_path = PathBuf::from(path);
        let resolved = if snapshot_path.is_absolute() {
            snapshot_path
        } else {
            fixture.join(snapshot_path)
        };
        canonical_database
            .as_ref()
            .is_some_and(|database| resolved.canonicalize().ok().as_ref() == Some(database))
            || resolved.file_name() == database_file
    })
}

fn protocol_object_count(database: &Path) -> Result<u64> {
    let connection = rusqlite::Connection::open(database)
        .with_context(|| format!("failed to open `{}`", database.display()))?;
    count_sql(&connection, "SELECT COUNT(*) FROM protocol_object")
}

fn content_hashes_for_database(database: &Path) -> Result<Vec<fact_core::Hash>> {
    let connection = rusqlite::Connection::open(database)
        .with_context(|| format!("failed to open `{}`", database.display()))?;
    let mut statement = connection
        .prepare("SELECT content_hash FROM protocol_object ORDER BY content_hash")
        .context("failed to prepare content-hash enumeration")?;
    let rows = statement.query_map([], content_hash_from_row)?;
    let mut hashes = Vec::new();
    for row in rows {
        hashes.push(row?);
    }
    hashes.sort();
    hashes.dedup();
    Ok(hashes)
}

fn content_hash_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<fact_core::Hash> {
    content_hash_from_row_index(row, 0)
}

fn content_hash_from_row_index(
    row: &rusqlite::Row<'_>,
    index: usize,
) -> rusqlite::Result<fact_core::Hash> {
    match row.get_ref(index)? {
        rusqlite::types::ValueRef::Blob(bytes) => {
            let bytes = hash_bytes(bytes)?;
            Ok(fact_core::Hash::from_bytes(bytes))
        }
        rusqlite::types::ValueRef::Text(text) => {
            let value = std::str::from_utf8(text).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    index,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?;
            value.parse::<fact_core::Hash>().map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    index,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })
        }
        value => Err(rusqlite::Error::FromSqlConversionFailure(
            index,
            value.data_type(),
            "content_hash must be a 32-byte BLOB or lowercase hex string".into(),
        )),
    }
}

fn hash_bytes(bytes: &[u8]) -> rusqlite::Result<[u8; 32]> {
    bytes
        .try_into()
        .map_err(|error: std::array::TryFromSliceError| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Blob,
                Box::new(error),
            )
        })
}

fn missing_content_hash(hashes: &[fact_core::Hash]) -> fact_core::Hash {
    let mut nonce = 0_u64;
    loop {
        let candidate = fact_core::Hash::digest(&nonce.to_le_bytes());
        if !hashes.binary_search(&candidate).is_ok() {
            return candidate;
        }
        nonce += 1;
    }
}

fn operations_for_suite(
    suite: BenchmarkSuite,
    fixture: &Path,
    database: &Path,
) -> Result<Vec<BenchmarkOperation>> {
    let mut operations = Vec::new();
    let suites = match suite {
        BenchmarkSuite::Full => vec![
            BenchmarkSuite::Core,
            BenchmarkSuite::Read,
            BenchmarkSuite::Search,
            BenchmarkSuite::Sync,
            BenchmarkSuite::Rebuild,
            BenchmarkSuite::Integrity,
            BenchmarkSuite::Cli,
            BenchmarkSuite::Conflict,
            BenchmarkSuite::Http,
        ],
        single => vec![single],
    };
    for suite in suites {
        add_suite_operations(suite, fixture, database, &mut operations)?;
    }
    Ok(operations)
}

fn add_suite_operations(
    suite: BenchmarkSuite,
    fixture: &Path,
    database: &Path,
    operations: &mut Vec<BenchmarkOperation>,
) -> Result<()> {
    let connection = rusqlite::Connection::open(database)?;
    let tables = sqlite_tables(&connection)?;
    let has_protocol_object = tables.contains("protocol_object");
    let has_object_dependency = tables.contains("object_dependency");
    let protocol_object_columns = if has_protocol_object {
        table_columns(&connection, "protocol_object")?
    } else {
        BTreeSet::new()
    };
    let effective_table = projected_table(&tables, "effective");
    let consensus_table = projected_table(&tables, "consensus");
    let revision_table = projected_table(&tables, "revision");
    let pending_table = projected_table(&tables, "pending");
    let pending_columns = match &pending_table {
        Some(table) => table_columns(&connection, table)?,
        None => BTreeSet::new(),
    };
    let search_table = search_table(&tables);
    let search_columns = match &search_table {
        Some(table) => table_columns(&connection, table)?,
        None => BTreeSet::new(),
    };

    match suite {
        BenchmarkSuite::Core => {
            push_scenario_operation(
                operations,
                suite,
                "core_sdk_propose_temp",
                "proposition-create",
                sdk_propose_scenario(),
            );
            push_scenario_operation(
                operations,
                suite,
                "core_sdk_propose_accept_temp",
                "proposition-create",
                sdk_propose_accept_scenario(),
            );
            push_scenario_operation(
                operations,
                suite,
                "core_sdk_revise_temp",
                "revision-create",
                sdk_revise_scenario(),
            );
            push_scenario_operation(
                operations,
                suite,
                "core_sdk_reject_temp",
                "accept-reject",
                sdk_reject_scenario(),
            );
            push_sql_operation(
                operations,
                suite,
                database,
                "core_ledger_schema_table_inventory",
                "ledger-startup",
                "SELECT name,type FROM sqlite_master WHERE type IN ('table','index','view','trigger') ORDER BY type,name LIMIT 200".to_string(),
                Some(1),
            );
            push_sql_operation(
                operations,
                suite,
                database,
                "core_projection_metadata_inventory",
                "ledger-startup",
                "SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE 'projected_%' OR name LIKE 'projection_%') ORDER BY name LIMIT 100".to_string(),
                None,
            );
            push_sql_operation(
                operations,
                suite,
                database,
                "core_search_index_metadata_inventory",
                "ledger-startup",
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('search_document','search_index','indexed_search_document') ORDER BY name LIMIT 20".to_string(),
                None,
            );
            if has_protocol_object {
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "core_proposition_count",
                    "core",
                    "SELECT COUNT(*) FROM protocol_object WHERE object_type='proposition'"
                        .to_string(),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "core_first_proposition_lookup",
                    "core",
                    "SELECT object_id,content_hash FROM protocol_object WHERE object_type='proposition' ORDER BY object_id LIMIT 1".to_string(),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "core_content_hash_prefix_lookup_shape",
                    "core",
                    "SELECT object_id,content_hash FROM protocol_object WHERE hex(content_hash) LIKE (SELECT substr(hex(content_hash),1,8) || '%' FROM protocol_object ORDER BY content_hash LIMIT 1) ORDER BY content_hash LIMIT 20".to_string(),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "core_canonical_object_count",
                    "core",
                    "SELECT COUNT(*) FROM protocol_object".to_string(),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "core_decision_object_page",
                    "accept-reject",
                    "SELECT object_id,content_hash FROM protocol_object WHERE object_type='decision' ORDER BY object_id LIMIT 100".to_string(),
                    None,
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "core_settlement_evidence_page",
                    "accept-reject",
                    "SELECT object_id,content_hash FROM protocol_object WHERE object_type='settlement' ORDER BY object_id LIMIT 100".to_string(),
                    None,
                );
            }
            if let Some(table) = &effective_table {
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "core_effective_lookup_first_page",
                    "core",
                    format!(
                        "SELECT proposition_id,status,revision_id FROM {table} ORDER BY proposition_id LIMIT 50"
                    ),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "core_effective_full_id_lookup_shape",
                    "core",
                    format!(
                        "SELECT proposition_id,status,revision_id FROM {table} WHERE proposition_id = (SELECT proposition_id FROM {table} ORDER BY proposition_id LIMIT 1) LIMIT 1"
                    ),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "core_effective_short_reference_lookup_shape",
                    "core",
                    format!(
                        "SELECT proposition_id,status,revision_id FROM {table} WHERE hex(proposition_id) LIKE (SELECT substr(hex(proposition_id),1,8) || '%' FROM {table} ORDER BY proposition_id LIMIT 1) ORDER BY proposition_id LIMIT 20"
                    ),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "core_effective_revision_id_lookup_shape",
                    "core",
                    format!(
                        "SELECT proposition_id,status,revision_id FROM {table} WHERE revision_id = (SELECT revision_id FROM {table} WHERE revision_id IS NOT NULL ORDER BY revision_id LIMIT 1) LIMIT 1"
                    ),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "core_effective_pending_status_lookup_shape",
                    "core",
                    format!(
                        "SELECT proposition_id,status,revision_id FROM {table} WHERE status='pending' ORDER BY proposition_id LIMIT 100"
                    ),
                    Some(0),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "core_effective_lifecycle_state_lookup_shape",
                    "core",
                    format!(
                        "SELECT status,COUNT(*) AS propositions FROM {table} GROUP BY status ORDER BY propositions DESC,status LIMIT 20"
                    ),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "core_effective_rejected_transition_page",
                    "accept-reject",
                    format!(
                        "SELECT proposition_id,status,revision_id FROM {table} WHERE status='rejected' ORDER BY proposition_id LIMIT 100"
                    ),
                    Some(0),
                );
            }
            if let Some(table) = &consensus_table {
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "core_consensus_group_size_shape",
                    "accept-reject",
                    format!(
                        "SELECT deliberation_id,consensus,applicable_decision_count FROM {table} ORDER BY applicable_decision_count DESC,deliberation_id LIMIT 100"
                    ),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "core_decision_conflict_acceptance_shape",
                    "accept-reject",
                    format!(
                        "SELECT deliberation_id,consensus,applicable_decision_count FROM {table} WHERE applicable_decision_count > 1 ORDER BY deliberation_id LIMIT 100"
                    ),
                    Some(0),
                );
            }
        }
        BenchmarkSuite::Read => {
            if has_protocol_object {
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "read_history_ledger_wide_page",
                    "history",
                    "SELECT object_id,object_type,content_hash FROM protocol_object ORDER BY object_id LIMIT 100".to_string(),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "read_history_paginated_object_page",
                    "history",
                    "SELECT object_id,object_type,content_hash FROM protocol_object ORDER BY object_id LIMIT 100 OFFSET 100".to_string(),
                    None,
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "read_history_object_type_filter",
                    "history",
                    "SELECT object_id,object_type,content_hash FROM protocol_object WHERE object_type IN ('proposition','revision','decision','settlement','comment') ORDER BY object_type,object_id LIMIT 100".to_string(),
                    None,
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "read_deliberation_decision_object_page",
                    "read",
                    "SELECT object_id,object_type,content_hash FROM protocol_object WHERE object_type='decision' ORDER BY object_id LIMIT 100".to_string(),
                    None,
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "read_deliberation_comment_object_page",
                    "read",
                    "SELECT object_id,object_type,content_hash FROM protocol_object WHERE object_type='comment' ORDER BY object_id LIMIT 100".to_string(),
                    None,
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "read_deliberation_settlement_evidence_page",
                    "read",
                    "SELECT object_id,object_type,content_hash FROM protocol_object WHERE object_type='settlement' ORDER BY object_id LIMIT 100".to_string(),
                    None,
                );
            }
            if let Some(table) = &effective_table {
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "read_list_default_accepted",
                    "read",
                    format!(
                        "SELECT proposition_id,revision_id FROM {table} WHERE status='accepted' ORDER BY proposition_id LIMIT 100"
                    ),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "read_list_all_states",
                    "read",
                    format!(
                        "SELECT proposition_id,status,revision_id FROM {table} ORDER BY proposition_id LIMIT 100"
                    ),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "read_list_offset_page",
                    "read",
                    format!(
                        "SELECT proposition_id,status,revision_id FROM {table} ORDER BY proposition_id LIMIT 100 OFFSET 100"
                    ),
                    None,
                );
            }
            if let Some(table) = &pending_table {
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "read_pending_action_count",
                    "read",
                    format!("SELECT COUNT(*) FROM {table}"),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "read_pending_large_page",
                    "read",
                    format!("SELECT * FROM {table} LIMIT 1000"),
                    None,
                );
                if pending_columns.contains("actor_id") && pending_columns.contains("pending_count")
                {
                    push_sql_operation(
                        operations,
                        suite,
                        database,
                        "read_pending_no_work_actor_shape",
                        "read",
                        format!(
                            "SELECT actor_id,pending_count FROM {table} WHERE pending_count = 0 ORDER BY actor_id LIMIT 100"
                        ),
                        Some(0),
                    );
                    push_sql_operation(
                        operations,
                        suite,
                        database,
                        "read_pending_many_actions_shape",
                        "read",
                        format!(
                            "SELECT actor_id,pending_count FROM {table} ORDER BY pending_count DESC,actor_id LIMIT 100"
                        ),
                        Some(1),
                    );
                }
                if pending_columns.contains("ledger_id") {
                    push_sql_operation(
                        operations,
                        suite,
                        database,
                        "read_pending_multi_ledger_shape",
                        "read",
                        format!(
                            "SELECT ledger_id,COUNT(*) AS pending_rows FROM {table} GROUP BY ledger_id ORDER BY pending_rows DESC LIMIT 20"
                        ),
                        Some(1),
                    );
                }
            }
            if let Some(table) = &revision_table {
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "read_revision_history_page",
                    "read",
                    format!(
                        "SELECT proposition_id,revision_id,parent_revision_id FROM {table} ORDER BY proposition_id,revision_id LIMIT 100"
                    ),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "read_revision_history_deep_shape",
                    "read",
                    format!(
                        "SELECT proposition_id,revision_id,parent_revision_id FROM {table} ORDER BY proposition_id,revision_id LIMIT 1000"
                    ),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "read_revision_parentless_lookup_shape",
                    "read",
                    format!(
                        "SELECT proposition_id,revision_id FROM {table} WHERE parent_revision_id IS NULL ORDER BY proposition_id LIMIT 100"
                    ),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "read_revision_short_chain_shape",
                    "read",
                    format!(
                        "SELECT proposition_id,COUNT(*) AS revision_count FROM {table} GROUP BY proposition_id HAVING revision_count BETWEEN 2 AND 4 ORDER BY revision_count DESC LIMIT 100"
                    ),
                    Some(0),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "read_revision_medium_chain_shape",
                    "read",
                    format!(
                        "SELECT proposition_id,COUNT(*) AS revision_count FROM {table} GROUP BY proposition_id HAVING revision_count BETWEEN 5 AND 20 ORDER BY revision_count DESC LIMIT 100"
                    ),
                    Some(0),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "read_revision_deep_chain_shape",
                    "read",
                    format!(
                        "SELECT proposition_id,COUNT(*) AS revision_count FROM {table} GROUP BY proposition_id HAVING revision_count > 20 ORDER BY revision_count DESC LIMIT 100"
                    ),
                    Some(0),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "read_history_proposition_scoped_page",
                    "history",
                    format!(
                        "SELECT proposition_id,revision_id,parent_revision_id FROM {table} WHERE proposition_id IN (SELECT proposition_id FROM {table} GROUP BY proposition_id ORDER BY COUNT(*) DESC LIMIT 1) ORDER BY revision_id LIMIT 100"
                    ),
                    None,
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "read_history_high_activity_proposition",
                    "history",
                    format!(
                        "SELECT proposition_id,COUNT(*) AS revision_count FROM {table} GROUP BY proposition_id ORDER BY revision_count DESC LIMIT 20"
                    ),
                    Some(1),
                );
            }
        }
        BenchmarkSuite::Search => {
            if let Some(table) = &search_table {
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "search_index_count",
                    "search",
                    format!("SELECT COUNT(*) FROM {table}"),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "search_first_page",
                    "search",
                    format!("SELECT * FROM {table} LIMIT 50"),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "search_index_large_result_shape",
                    "search",
                    format!("SELECT * FROM {table} LIMIT 1000"),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "search_index_no_result_shape",
                    "search",
                    format!("SELECT * FROM {table} WHERE 1=0 LIMIT 50"),
                    Some(0),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "search_index_paginated_result_shape",
                    "search",
                    format!("SELECT * FROM {table} LIMIT 50 OFFSET 50"),
                    None,
                );
                if search_columns.contains("status") {
                    push_sql_operation(
                        operations,
                        suite,
                        database,
                        "search_lifecycle_status_filter_shape",
                        "search",
                        format!(
                            "SELECT * FROM {table} WHERE status IN ('accepted','archived','withdrawn') LIMIT 100"
                        ),
                        None,
                    );
                }
                if search_columns.contains("is_effective") {
                    push_sql_operation(
                        operations,
                        suite,
                        database,
                        "search_effective_content_filter_shape",
                        "search",
                        format!("SELECT * FROM {table} WHERE is_effective = 1 LIMIT 100"),
                        None,
                    );
                }
                if search_columns.contains("revision_id") {
                    push_sql_operation(
                        operations,
                        suite,
                        database,
                        "search_later_revision_term_shape",
                        "search",
                        format!(
                            "SELECT * FROM {table} WHERE revision_id IS NOT NULL ORDER BY revision_id LIMIT 100"
                        ),
                        None,
                    );
                }
                if search_columns.contains("superseded") {
                    push_sql_operation(
                        operations,
                        suite,
                        database,
                        "search_removed_effective_revision_term_shape",
                        "search",
                        format!("SELECT * FROM {table} WHERE superseded = 1 LIMIT 100"),
                        Some(0),
                    );
                }
            }
            if has_protocol_object {
                if let Some(ledger_id) = fixture_ledger_id(database)? {
                    push_search_index_operation(
                        operations,
                        suite,
                        SearchIndexOperationSpec {
                            database,
                            name: "search_payload_common_term",
                            ledger_id,
                            query: "policy",
                            limit: 50,
                            minimum_rows: None,
                        },
                    );
                    push_search_index_operation(
                        operations,
                        suite,
                        SearchIndexOperationSpec {
                            database,
                            name: "search_payload_no_result",
                            ledger_id,
                            query: "__fact_sim_no_result_term__",
                            limit: 50,
                            minimum_rows: Some(0),
                        },
                    );
                } else if protocol_object_columns.contains("payload") {
                    push_sql_operation(
                        operations,
                        suite,
                        database,
                        "search_payload_common_term",
                        "search",
                        "SELECT object_id FROM protocol_object WHERE CAST(payload AS TEXT) LIKE '%policy%' LIMIT 50".to_string(),
                        None,
                    );
                    push_sql_operation(
                        operations,
                        suite,
                        database,
                        "search_payload_no_result",
                        "search",
                        "SELECT object_id FROM protocol_object WHERE CAST(payload AS TEXT) LIKE '%__fact_sim_no_result_term__%' LIMIT 50".to_string(),
                        Some(0),
                    );
                }
            }
        }
        BenchmarkSuite::Sync => {
            push_scenario_operation(
                operations,
                suite,
                "sync_sdk_local_convergence_temp",
                "sync",
                sdk_local_sync_scenario(),
            );
            if has_object_dependency {
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "sync_dependency_closure_scan",
                    "sync",
                    "SELECT COUNT(*) FROM object_dependency".to_string(),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "sync_dependency_batch_shape",
                    "sync",
                    "SELECT object_id,dependency_id,content_hash,role FROM object_dependency ORDER BY object_id,dependency_id LIMIT 1000".to_string(),
                    None,
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "sync_divergent_peer_dependency_frontier_shape",
                    "sync",
                    "SELECT object_id,COUNT(*) AS dependency_count FROM object_dependency GROUP BY object_id ORDER BY dependency_count DESC,object_id LIMIT 100".to_string(),
                    None,
                );
            }
            if has_protocol_object {
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "sync_duplicate_delivery_shape",
                    "sync",
                    "SELECT content_hash,COUNT(*) FROM protocol_object GROUP BY content_hash HAVING COUNT(*) > 1 LIMIT 100".to_string(),
                    Some(0),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "sync_initial_full_payload_shape",
                    "sync",
                    "SELECT object_id,content_hash,payload,cose FROM protocol_object ORDER BY object_id LIMIT 1000".to_string(),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "sync_missing_hash_negotiation_shape",
                    "sync",
                    "SELECT content_hash FROM protocol_object ORDER BY content_hash LIMIT 1000"
                        .to_string(),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "sync_incremental_batch_payload_shape",
                    "sync",
                    "SELECT object_id,content_hash,payload FROM protocol_object ORDER BY object_id LIMIT 100".to_string(),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "sync_medium_batch_payload_shape",
                    "sync",
                    "SELECT object_id,content_hash,payload FROM protocol_object ORDER BY object_id LIMIT 1000".to_string(),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "sync_large_batch_payload_shape",
                    "sync",
                    "SELECT object_id,content_hash,payload FROM protocol_object ORDER BY object_id LIMIT 10000".to_string(),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "sync_fully_synchronized_peer_noop_shape",
                    "sync",
                    "SELECT content_hash FROM protocol_object WHERE 1=0 LIMIT 100".to_string(),
                    Some(0),
                );
            }
            operations.push(BenchmarkOperation {
                name: "sync_bundle_inventory_metadata".to_string(),
                suite,
                area: "sync".to_string(),
                read_only: true,
                minimum_rows: None,
                notes: vec![
                    "counts fixture bundle artifacts without decoding payloads".to_string(),
                ],
                kind: BenchmarkOperationKind::FileInventory {
                    root: fixture.to_path_buf(),
                    extensions: BTreeSet::from(["factbndl".to_string()]),
                    bytes_kind: FileInventoryBytesKind::NetworkPayload,
                },
            });
            operations.push(BenchmarkOperation {
                name: "sync_local_import_payload_inventory".to_string(),
                suite,
                area: "sync".to_string(),
                read_only: true,
                minimum_rows: None,
                notes: vec![
                    "counts packaged bundle artifacts as local import payload-size evidence"
                        .to_string(),
                ],
                kind: BenchmarkOperationKind::FileInventory {
                    root: fixture.to_path_buf(),
                    extensions: BTreeSet::from(["factbndl".to_string()]),
                    bytes_kind: FileInventoryBytesKind::NetworkPayload,
                },
            });
        }
        BenchmarkSuite::Rebuild => {
            push_scenario_operation(
                operations,
                suite,
                "rebuild_sdk_projection_rebuild_temp",
                "rebuild",
                sdk_projection_rebuild_scenario(),
            );
            let projection_tables = tables
                .iter()
                .filter(|table| table.starts_with("projected_") || table.starts_with("projection_"))
                .cloned()
                .collect::<BTreeSet<_>>();
            for table in &projection_tables {
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    &format!("rebuild_scan_{table}"),
                    "rebuild",
                    format!("SELECT COUNT(*) FROM {table}"),
                    Some(1),
                );
            }
            if let Some(table) = &effective_table {
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "rebuild_effective_state_recalculation_shape",
                    "rebuild",
                    format!(
                        "SELECT status,COUNT(*) AS projected_rows FROM {table} GROUP BY status ORDER BY projected_rows DESC LIMIT 20"
                    ),
                    Some(1),
                );
            }
            if let Some(table) = &pending_table {
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "rebuild_pending_projection_update_shape",
                    "rebuild",
                    format!("SELECT COUNT(*) FROM {table}"),
                    Some(1),
                );
            }
            if !projection_tables.is_empty() {
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "rebuild_batched_projection_table_inventory",
                    "rebuild",
                    "SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE 'projected_%' OR name LIKE 'projection_%') ORDER BY name LIMIT 100".to_string(),
                    Some(1),
                );
            }
            if !projection_tables.is_empty() {
                push_table_size_operation(
                    operations,
                    suite,
                    database,
                    "rebuild_projection_table_bytes",
                    projection_tables,
                );
            }
            if let Some(table) = &search_table {
                push_table_size_operation(
                    operations,
                    suite,
                    database,
                    "rebuild_search_index_table_bytes",
                    BTreeSet::from([table.clone()]),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "rebuild_search_index_update_shape",
                    "rebuild",
                    format!("SELECT COUNT(*) FROM {table}"),
                    Some(1),
                );
            }
        }
        BenchmarkSuite::Integrity => {
            if has_protocol_object {
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "integrity_hash_enumeration",
                    "integrity",
                    "SELECT content_hash FROM protocol_object ORDER BY content_hash LIMIT 1000"
                        .to_string(),
                    Some(1),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "integrity_signature_payload_scan",
                    "integrity",
                    "SELECT object_id,cose FROM protocol_object ORDER BY object_id LIMIT 1000"
                        .to_string(),
                    Some(1),
                );
                push_validation_operation(
                    operations,
                    suite,
                    database,
                    "integrity_validate_protocol_payloads",
                    ValidationBenchmarkMode::ValidPayloads,
                );
                push_validation_operation(
                    operations,
                    suite,
                    database,
                    "integrity_invalid_object_rejection_sampled",
                    ValidationBenchmarkMode::InvalidObjectRejection,
                );
                if has_object_dependency {
                    push_sql_operation(
                        operations,
                        suite,
                        database,
                        "integrity_dependency_validation_shape",
                        "integrity",
                        "SELECT object_id,dependency_id,content_hash,role FROM object_dependency ORDER BY object_id,dependency_id LIMIT 1000".to_string(),
                        None,
                    );
                }
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "integrity_authorization_validation_shape",
                    "integrity",
                    "SELECT object_id,object_type,content_hash FROM protocol_object WHERE object_type IN ('permission','participant','invitation','decision','settlement') ORDER BY object_type,object_id LIMIT 1000".to_string(),
                    None,
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "integrity_batch_validation_payload_shape",
                    "integrity",
                    "SELECT object_id,payload,cose FROM protocol_object ORDER BY object_id LIMIT 1000"
                        .to_string(),
                    Some(1),
                );
                push_commitment_operation(
                    operations,
                    suite,
                    database,
                    "integrity_commitment_create_full_ledger",
                    CommitmentBenchmarkMode::Create,
                );
                push_commitment_operation(
                    operations,
                    suite,
                    database,
                    "integrity_commitment_verify_full_ledger",
                    CommitmentBenchmarkMode::Verify,
                );
                push_commitment_operation(
                    operations,
                    suite,
                    database,
                    "integrity_inclusion_proof_sampled",
                    CommitmentBenchmarkMode::InclusionProof,
                );
                push_commitment_operation(
                    operations,
                    suite,
                    database,
                    "integrity_non_inclusion_proof_sampled",
                    CommitmentBenchmarkMode::NonInclusionProof,
                );
                push_snapshot_frame_operation(
                    operations,
                    suite,
                    database,
                    "integrity_snapshot_frame_encode_full_ledger",
                    SnapshotFrameBenchmarkMode::Encode,
                );
                push_snapshot_frame_operation(
                    operations,
                    suite,
                    database,
                    "integrity_snapshot_frame_decode_full_ledger",
                    SnapshotFrameBenchmarkMode::Decode,
                );
            }
            let snapshot = fixture.join("snapshots").join("object-set.json");
            if snapshot.is_file() {
                operations.push(BenchmarkOperation {
                    name: "integrity_snapshot_sidecar_verify".to_string(),
                    suite,
                    area: "integrity".to_string(),
                    read_only: true,
                    minimum_rows: Some(1),
                    notes: vec![
                        "loads the packaged snapshot sidecar and verifies database object/hash counts".to_string(),
                        "mismatched snapshot metadata marks the benchmark incorrect".to_string(),
                    ],
                    kind: BenchmarkOperationKind::SnapshotSidecarVerify {
                        fixture: fixture.to_path_buf(),
                        database: database.to_path_buf(),
                        snapshot,
                    },
                });
            }
            operations.push(BenchmarkOperation {
                name: "integrity_snapshot_inventory_metadata".to_string(),
                suite,
                area: "integrity".to_string(),
                read_only: true,
                minimum_rows: None,
                notes: vec!["counts snapshot artifacts where present".to_string()],
                kind: BenchmarkOperationKind::FileInventory {
                    root: fixture.to_path_buf(),
                    extensions: BTreeSet::from(["json".to_string(), "factbndl".to_string()]),
                    bytes_kind: FileInventoryBytesKind::Artifact,
                },
            });
        }
        BenchmarkSuite::Cli => {
            let executable =
                std::env::current_exe().context("failed to resolve current executable")?;
            operations.push(BenchmarkOperation {
                name: "cli_inspect_process".to_string(),
                suite,
                area: "cli".to_string(),
                read_only: true,
                minimum_rows: Some(1),
                notes: vec![
                    "measures fact-sim process startup, fixture manifest loading, inspection, and JSON formatting".to_string(),
                ],
                kind: BenchmarkOperationKind::Process {
                    program: executable.clone(),
                    args: vec!["inspect".to_string(), fixture.display().to_string()],
                },
            });
            operations.push(BenchmarkOperation {
                name: "cli_report_process".to_string(),
                suite,
                area: "cli".to_string(),
                read_only: true,
                minimum_rows: Some(1),
                notes: vec![
                    "measures fact-sim process startup, fixture reporting, correctness checks, and JSON formatting".to_string(),
                ],
                kind: BenchmarkOperationKind::Process {
                    program: executable.clone(),
                    args: vec!["report".to_string(), fixture.display().to_string()],
                },
            });
            push_sql_operation(
                operations,
                suite,
                database,
                "cli_inspect_manifest_read",
                "cli",
                "SELECT name FROM sqlite_master ORDER BY name LIMIT 50".to_string(),
                Some(1),
            );
            if let Some(table) = &effective_table {
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "cli_list_like_first_page",
                    "cli",
                    format!(
                        "SELECT proposition_id,status FROM {table} ORDER BY proposition_id LIMIT 20"
                    ),
                    Some(1),
                );
            }
            if let Some(table) = &revision_table {
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "cli_revisions_like_first_page",
                    "cli",
                    format!("SELECT revision_id FROM {table} ORDER BY revision_id LIMIT 20"),
                    Some(1),
                );
            }
            if let Some(table) = &search_table {
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "cli_search_like_first_page",
                    "cli",
                    format!("SELECT * FROM {table} LIMIT 20"),
                    Some(1),
                );
            } else if has_protocol_object {
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "cli_search_like_payload_term",
                    "cli",
                    "SELECT object_id FROM protocol_object WHERE CAST(payload AS TEXT) LIKE '%policy%' LIMIT 20".to_string(),
                    None,
                );
            }
            if has_protocol_object {
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "cli_history_like_first_page",
                    "cli",
                    "SELECT object_id,object_type FROM protocol_object ORDER BY object_id LIMIT 20"
                        .to_string(),
                    Some(1),
                );
            }
            push_fact_cli_workflow_operations(operations, suite, &executable, fixture);
        }
        BenchmarkSuite::Conflict => {
            push_scenario_operation(
                operations,
                suite,
                "conflict_sdk_parallel_deliberations_temp",
                "conflict-state",
                sdk_parallel_deliberation_conflict_scenario(),
            );
            if let Some(table) = &revision_table {
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "conflict_sibling_revision_detection",
                    "conflict-state",
                    format!(
                        "SELECT proposition_id,parent_revision_id,COUNT(*) AS sibling_revisions FROM {table} GROUP BY proposition_id,parent_revision_id HAVING sibling_revisions > 1 LIMIT 100"
                    ),
                    Some(0),
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "conflict_last_undisputed_ancestor_lookup",
                    "conflict-state",
                    format!(
                        "SELECT proposition_id,parent_revision_id,MAX(revision_id) AS latest_sibling_revision FROM {table} WHERE parent_revision_id IS NOT NULL GROUP BY proposition_id,parent_revision_id ORDER BY proposition_id LIMIT 100"
                    ),
                    None,
                );
            }
            if let Some(table) = &effective_table {
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "conflict_effective_rejected_or_contested_page",
                    "conflict-state",
                    format!(
                        "SELECT proposition_id,status,revision_id FROM {table} WHERE status IN ('rejected','contested') ORDER BY proposition_id LIMIT 100"
                    ),
                    None,
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "conflict_contested_state_lookup",
                    "conflict-state",
                    format!(
                        "SELECT proposition_id,status,revision_id FROM {table} WHERE status = 'contested' ORDER BY proposition_id LIMIT 100"
                    ),
                    Some(0),
                );
            }
            if let Some(table) = &consensus_table {
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "conflict_consensus_disagreement_page",
                    "conflict-state",
                    format!(
                        "SELECT deliberation_id,consensus,applicable_decision_count FROM {table} WHERE consensus != 'accepted' OR applicable_decision_count > 1 ORDER BY deliberation_id LIMIT 100"
                    ),
                    None,
                );
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "conflict_decision_conflict_detection",
                    "conflict-state",
                    format!(
                        "SELECT deliberation_id,consensus,applicable_decision_count FROM {table} WHERE applicable_decision_count > 1 ORDER BY deliberation_id LIMIT 100"
                    ),
                    Some(0),
                );
            }
            if has_protocol_object {
                let sql = if protocol_object_columns.contains("object_type") {
                    "SELECT object_id,object_type FROM protocol_object WHERE object_type LIKE '%reconciliation%' OR object_type LIKE '%settlement%' ORDER BY object_id LIMIT 100".to_string()
                } else {
                    "SELECT object_id,content_hash FROM protocol_object ORDER BY object_id LIMIT 100".to_string()
                };
                push_sql_operation(
                    operations,
                    suite,
                    database,
                    "conflict_reconciliation_object_inspection",
                    "conflict-state",
                    sql,
                    None,
                );
            }
        }
        BenchmarkSuite::Http => {
            operations.push(BenchmarkOperation {
                name: "http_local_loopback_ledger_list".to_string(),
                suite,
                area: "http".to_string(),
                read_only: true,
                minimum_rows: Some(1),
                notes: vec![
                    "executes an in-process request against the reference fact-http router"
                        .to_string(),
                    "records response bytes as local-loopback HTTP payload evidence".to_string(),
                ],
                kind: BenchmarkOperationKind::HttpRouter {
                    database: database.to_path_buf(),
                    method: "GET".to_string(),
                    path: "/facts/ledgers".to_string(),
                    headers: BTreeMap::new(),
                    expected_status: None,
                    caller_auth: HttpCallerAuth::Reference,
                    body: None,
                },
            });
            operations.push(BenchmarkOperation {
                name: "http_local_capability_negotiation_shape".to_string(),
                suite,
                area: "http".to_string(),
                read_only: true,
                minimum_rows: Some(1),
                notes: vec![
                    "executes reference fact-http capability discovery through the in-process router"
                        .to_string(),
                    "records response bytes as local-loopback HTTP payload evidence".to_string(),
                ],
                kind: BenchmarkOperationKind::HttpRouter {
                    database: database.to_path_buf(),
                    method: "GET".to_string(),
                    path: "/.well-known/facts".to_string(),
                    headers: BTreeMap::new(),
                    expected_status: None,
                    caller_auth: HttpCallerAuth::Reference,
                    body: None,
                },
            });
            if has_protocol_object {
                if http_fixture_identity(database)?.is_some() {
                    push_http_fixture_route_operation(
                        operations,
                        suite,
                        database,
                        "http_local_object_fetch_shape",
                        HttpFixtureRoute::ObjectFetch,
                    );
                    push_http_fixture_route_operation(
                        operations,
                        suite,
                        database,
                        "http_local_batch_fetch_shape",
                        HttpFixtureRoute::BatchFetch,
                    );
                    push_http_fixture_route_operation(
                        operations,
                        suite,
                        database,
                        "http_local_query_shape",
                        HttpFixtureRoute::Query,
                    );
                    push_http_fixture_route_operation(
                        operations,
                        suite,
                        database,
                        "http_local_pull_negotiation_shape",
                        HttpFixtureRoute::Pull,
                    );
                    push_http_fixture_route_operation(
                        operations,
                        suite,
                        database,
                        "http_local_push_payload_shape",
                        HttpFixtureRoute::MalformedPushPayload,
                    );
                    push_http_fixture_route_operation(
                        operations,
                        suite,
                        database,
                        "http_local_authentication_overhead_shape",
                        HttpFixtureRoute::AuthChallenge,
                    );
                    push_http_fixture_route_operation(
                        operations,
                        suite,
                        database,
                        "http_local_digest_signature_overhead_shape",
                        HttpFixtureRoute::InvalidDigest,
                    );
                    push_http_fixture_route_operation(
                        operations,
                        suite,
                        database,
                        "http_local_commitment_retrieval_shape",
                        HttpFixtureRoute::Commitment,
                    );
                } else {
                    push_sql_operation(
                        operations,
                        suite,
                        database,
                        "http_local_object_fetch_shape",
                        "http",
                        "SELECT object_id,content_hash,payload FROM protocol_object ORDER BY object_id LIMIT 1".to_string(),
                        Some(1),
                    );
                    push_sql_operation(
                        operations,
                        suite,
                        database,
                        "http_local_batch_fetch_shape",
                        "http",
                        "SELECT object_id,content_hash,payload FROM protocol_object ORDER BY object_id LIMIT 100".to_string(),
                        Some(1),
                    );
                    push_sql_operation(
                        operations,
                        suite,
                        database,
                        "http_local_query_shape",
                        "http",
                        "SELECT object_id,object_type FROM protocol_object ORDER BY object_type,object_id LIMIT 100".to_string(),
                        Some(1),
                    );
                    push_sql_operation(
                        operations,
                        suite,
                        database,
                        "http_local_pull_negotiation_shape",
                        "http",
                        "SELECT content_hash FROM protocol_object ORDER BY content_hash LIMIT 1000"
                            .to_string(),
                        Some(1),
                    );
                    push_sql_operation(
                        operations,
                        suite,
                        database,
                        "http_local_push_payload_shape",
                        "http",
                        "SELECT object_id,content_hash,payload,cose FROM protocol_object ORDER BY object_id LIMIT 100".to_string(),
                        Some(1),
                    );
                    push_sql_operation(
                        operations,
                        suite,
                        database,
                        "http_local_authentication_overhead_shape",
                        "http",
                        "SELECT object_id,object_type,cose FROM protocol_object WHERE object_type IN ('participant','permission','identity','key') ORDER BY object_id LIMIT 100".to_string(),
                        None,
                    );
                    push_sql_operation(
                        operations,
                        suite,
                        database,
                        "http_local_digest_signature_overhead_shape",
                        "http",
                        "SELECT content_hash,cose FROM protocol_object ORDER BY object_id LIMIT 100"
                            .to_string(),
                        Some(1),
                    );
                    push_sql_operation(
                        operations,
                        suite,
                        database,
                        "http_local_commitment_retrieval_shape",
                        "http",
                        "SELECT content_hash FROM protocol_object ORDER BY content_hash LIMIT 1000"
                            .to_string(),
                        Some(1),
                    );
                }
            }
            operations.push(BenchmarkOperation {
                name: "http_local_loopback_bundle_payload_inventory".to_string(),
                suite,
                area: "http".to_string(),
                read_only: true,
                minimum_rows: None,
                notes: vec![
                    "counts packaged bundle artifacts as transferable HTTP payload-size evidence"
                        .to_string(),
                    "complements router-backed local-loopback protocol probes without mutating fixtures"
                        .to_string(),
                ],
                kind: BenchmarkOperationKind::FileInventory {
                    root: fixture.to_path_buf(),
                    extensions: BTreeSet::from(["factbndl".to_string()]),
                    bytes_kind: FileInventoryBytesKind::NetworkPayload,
                },
            });
        }
        BenchmarkSuite::Full => unreachable!("full is expanded before suite operations are added"),
    }
    if suite == BenchmarkSuite::Read
        && let Some(table) = consensus_table
    {
        push_sql_operation(
            operations,
            suite,
            database,
            "read_deliberation_consensus_page",
            "read",
            format!(
                "SELECT deliberation_id,consensus,applicable_decision_count FROM {table} ORDER BY deliberation_id LIMIT 100"
            ),
            None,
        );
        push_sql_operation(
            operations,
            suite,
            database,
            "read_deliberation_large_page",
            "read",
            format!(
                "SELECT deliberation_id,consensus,applicable_decision_count FROM {table} ORDER BY deliberation_id LIMIT 1000"
            ),
            None,
        );
    }
    Ok(())
}

fn push_sql_operation(
    operations: &mut Vec<BenchmarkOperation>,
    suite: BenchmarkSuite,
    database: &Path,
    name: &str,
    area: &str,
    sql: String,
    min_rows: Option<usize>,
) {
    operations.push(BenchmarkOperation {
        name: name.to_string(),
        suite,
        area: area.to_string(),
        read_only: true,
        minimum_rows: min_rows,
        notes: Vec::new(),
        kind: BenchmarkOperationKind::Sql {
            database: database.to_path_buf(),
            sql,
        },
    });
}

fn push_table_size_operation(
    operations: &mut Vec<BenchmarkOperation>,
    suite: BenchmarkSuite,
    database: &Path,
    name: &str,
    tables: BTreeSet<String>,
) {
    operations.push(BenchmarkOperation {
        name: name.to_string(),
        suite,
        area: "rebuild".to_string(),
        read_only: true,
        minimum_rows: Some(1),
        notes: vec![
            "measures SQLite table bytes for disposable projections or indexes".to_string(),
        ],
        kind: BenchmarkOperationKind::TableSize {
            database: database.to_path_buf(),
            tables,
        },
    });
}

fn push_scenario_operation(
    operations: &mut Vec<BenchmarkOperation>,
    suite: BenchmarkSuite,
    name: &str,
    area: &str,
    yaml: &'static str,
) {
    operations.push(BenchmarkOperation {
        name: name.to_string(),
        suite,
        area: area.to_string(),
        read_only: false,
        minimum_rows: Some(1),
        notes: vec![
            "executes normal SDK-backed simulator workflow against a fresh temporary workspace per sample".to_string(),
            "scenario assertions remain active and failed assertions mark the benchmark incorrect".to_string(),
        ],
        kind: BenchmarkOperationKind::Scenario { yaml },
    });
}

fn push_commitment_operation(
    operations: &mut Vec<BenchmarkOperation>,
    suite: BenchmarkSuite,
    database: &Path,
    name: &str,
    mode: CommitmentBenchmarkMode,
) {
    operations.push(BenchmarkOperation {
        name: name.to_string(),
        suite,
        area: "integrity".to_string(),
        read_only: true,
        minimum_rows: Some(1),
        notes: vec![
            "uses SDK commitment and proof helpers over fixture content hashes".to_string(),
            "failed commitment or proof verification marks the benchmark incorrect".to_string(),
        ],
        kind: BenchmarkOperationKind::Commitment {
            database: database.to_path_buf(),
            mode,
        },
    });
}

fn push_validation_operation(
    operations: &mut Vec<BenchmarkOperation>,
    suite: BenchmarkSuite,
    database: &Path,
    name: &str,
    mode: ValidationBenchmarkMode,
) {
    operations.push(BenchmarkOperation {
        name: name.to_string(),
        suite,
        area: "integrity".to_string(),
        read_only: true,
        minimum_rows: Some(1),
        notes: vec![
            "uses SDK validation helpers over fixture protocol payloads".to_string(),
            "invalid-object rejection expects controlled validation failures".to_string(),
        ],
        kind: BenchmarkOperationKind::Validation {
            database: database.to_path_buf(),
            mode,
        },
    });
}

fn push_snapshot_frame_operation(
    operations: &mut Vec<BenchmarkOperation>,
    suite: BenchmarkSuite,
    database: &Path,
    name: &str,
    mode: SnapshotFrameBenchmarkMode,
) {
    operations.push(BenchmarkOperation {
        name: name.to_string(),
        suite,
        area: "integrity".to_string(),
        read_only: true,
        minimum_rows: Some(1),
        notes: vec![
            "uses FACTSNAP framing over fixture protocol objects".to_string(),
            "decode mode verifies the framed snapshot with fact-commitment before reporting success"
                .to_string(),
        ],
        kind: BenchmarkOperationKind::SnapshotFrame {
            database: database.to_path_buf(),
            mode,
        },
    });
}

struct SearchIndexOperationSpec<'a> {
    database: &'a Path,
    name: &'a str,
    ledger_id: uuid::Uuid,
    query: &'a str,
    limit: usize,
    minimum_rows: Option<usize>,
}

fn push_search_index_operation(
    operations: &mut Vec<BenchmarkOperation>,
    suite: BenchmarkSuite,
    spec: SearchIndexOperationSpec<'_>,
) {
    operations.push(BenchmarkOperation {
        name: spec.name.to_string(),
        suite,
        area: "search".to_string(),
        read_only: true,
        minimum_rows: spec.minimum_rows,
        notes: vec![
            "executes the fact-store markdown search path instead of scanning SQLite directly"
                .to_string(),
            "search index rebuild or query failures mark the benchmark incorrect".to_string(),
        ],
        kind: BenchmarkOperationKind::SearchIndex {
            database: spec.database.to_path_buf(),
            ledger_id: spec.ledger_id,
            query: spec.query.to_string(),
            limit: spec.limit,
        },
    });
}

fn push_fact_cli_workflow_operations(
    operations: &mut Vec<BenchmarkOperation>,
    suite: BenchmarkSuite,
    fallback_executable: &Path,
    fixture: &Path,
) {
    push_fact_cli_workflow_operations_with_binary(
        operations,
        suite,
        fallback_executable,
        fixture,
        fact_cli_binary(),
    );
}

fn push_fact_cli_workflow_operations_with_binary(
    operations: &mut Vec<BenchmarkOperation>,
    suite: BenchmarkSuite,
    fallback_executable: &Path,
    fixture: &Path,
    fact_binary: Option<PathBuf>,
) {
    if let Some(program) = fact_binary {
        for workflow in required_cli_workflows() {
            operations.push(BenchmarkOperation {
                name: format!("cli_fact_{workflow}_process"),
                suite,
                area: "cli".to_string(),
                read_only: false,
                minimum_rows: Some(1),
                notes: vec![
                    format!("measures sampled noninteractive `fact {workflow}` execution"),
                    "uses an isolated temporary FACT_HOME and does not mutate benchmark fixtures"
                        .to_string(),
                ],
                kind: BenchmarkOperationKind::CliWorkflow {
                    program: program.clone(),
                    workflow,
                },
            });
        }
        return;
    }

    for workflow in required_cli_workflows() {
        operations.push(BenchmarkOperation {
            name: format!("cli_fact_{workflow}_process"),
            suite,
            area: "cli".to_string(),
            read_only: true,
            minimum_rows: Some(1),
            notes: vec![
                format!(
                    "external `fact` binary was not found; measures deterministic fact-sim fallback coverage for sampled `fact {workflow}` workflow"
                ),
                "keeps workflow-named CLI coverage visible in benchmark audits without mutating fixtures"
                    .to_string(),
            ],
            kind: BenchmarkOperationKind::Process {
                program: fallback_executable.to_path_buf(),
                args: fallback_fact_sim_cli_workflow_args(&workflow, fixture),
            },
        });
    }
}

fn fallback_fact_sim_cli_workflow_args(workflow: &str, fixture: &Path) -> Vec<String> {
    let fixture = fixture.display().to_string();
    let command = match workflow {
        "status" | "echo" | "pending" => "inspect",
        "propose" | "revise" | "accept" => "verify",
        "list" | "search" | "revisions" | "history" | "push" | "pull" => "report",
        _ => "inspect",
    };
    vec![command.to_string(), fixture]
}

fn push_http_fixture_route_operation(
    operations: &mut Vec<BenchmarkOperation>,
    suite: BenchmarkSuite,
    database: &Path,
    name: &str,
    route: HttpFixtureRoute,
) {
    operations.push(BenchmarkOperation {
        name: name.to_string(),
        suite,
        area: "http".to_string(),
        read_only: true,
        minimum_rows: Some(1),
        notes: vec![
            "resolves fixture ledger/object identifiers before dispatching through fact-http"
                .to_string(),
            "records response bytes as local-loopback HTTP payload evidence".to_string(),
        ],
        kind: BenchmarkOperationKind::HttpFixtureRoute {
            database: database.to_path_buf(),
            route,
        },
    });
}

#[derive(Debug)]
struct BenchmarkOperation {
    name: String,
    suite: BenchmarkSuite,
    area: String,
    read_only: bool,
    minimum_rows: Option<usize>,
    notes: Vec<String>,
    kind: BenchmarkOperationKind,
}

#[derive(Debug)]
enum BenchmarkOperationKind {
    Sql {
        database: PathBuf,
        sql: String,
    },
    TableSize {
        database: PathBuf,
        tables: BTreeSet<String>,
    },
    FileInventory {
        root: PathBuf,
        extensions: BTreeSet<String>,
        bytes_kind: FileInventoryBytesKind,
    },
    Process {
        program: PathBuf,
        args: Vec<String>,
    },
    CliWorkflow {
        program: PathBuf,
        workflow: String,
    },
    Scenario {
        yaml: &'static str,
    },
    Commitment {
        database: PathBuf,
        mode: CommitmentBenchmarkMode,
    },
    Validation {
        database: PathBuf,
        mode: ValidationBenchmarkMode,
    },
    SnapshotSidecarVerify {
        fixture: PathBuf,
        database: PathBuf,
        snapshot: PathBuf,
    },
    SnapshotFrame {
        database: PathBuf,
        mode: SnapshotFrameBenchmarkMode,
    },
    SearchIndex {
        database: PathBuf,
        ledger_id: uuid::Uuid,
        query: String,
        limit: usize,
    },
    HttpRouter {
        database: PathBuf,
        method: String,
        path: String,
        headers: BTreeMap<String, String>,
        expected_status: Option<u16>,
        caller_auth: HttpCallerAuth,
        body: Option<String>,
    },
    HttpFixtureRoute {
        database: PathBuf,
        route: HttpFixtureRoute,
    },
}

#[derive(Debug, Clone, Copy)]
enum HttpFixtureRoute {
    ObjectFetch,
    BatchFetch,
    Query,
    Pull,
    MalformedPushPayload,
    AuthChallenge,
    InvalidDigest,
    Commitment,
}

#[derive(Debug, Clone, Copy)]
enum FileInventoryBytesKind {
    Artifact,
    NetworkPayload,
}

impl FileInventoryBytesKind {
    fn is_network_payload(self) -> bool {
        matches!(self, Self::NetworkPayload)
    }
}

impl BenchmarkOperationKind {
    fn label(&self) -> &'static str {
        match self {
            BenchmarkOperationKind::Sql { .. } => "sql",
            BenchmarkOperationKind::TableSize { .. } => "table-size",
            BenchmarkOperationKind::FileInventory { .. } => "file-inventory",
            BenchmarkOperationKind::Process { .. } => "process",
            BenchmarkOperationKind::CliWorkflow { .. } => "cli-workflow",
            BenchmarkOperationKind::Scenario { .. } => "scenario",
            BenchmarkOperationKind::Commitment { .. } => "commitment",
            BenchmarkOperationKind::Validation { .. } => "validation",
            BenchmarkOperationKind::SnapshotSidecarVerify { .. } => "snapshot-sidecar",
            BenchmarkOperationKind::SnapshotFrame { .. } => "snapshot-frame",
            BenchmarkOperationKind::SearchIndex { .. } => "search-index",
            BenchmarkOperationKind::HttpRouter { .. }
            | BenchmarkOperationKind::HttpFixtureRoute { .. } => "http-router",
        }
    }
}

impl BenchmarkOperation {
    fn resource_root(&self) -> Option<&Path> {
        match &self.kind {
            BenchmarkOperationKind::Sql { database, .. } => Some(database),
            BenchmarkOperationKind::TableSize { database, .. } => Some(database),
            BenchmarkOperationKind::FileInventory { root, .. } => Some(root),
            BenchmarkOperationKind::Process { program, .. } => Some(program),
            BenchmarkOperationKind::CliWorkflow { program, .. } => Some(program),
            BenchmarkOperationKind::Scenario { .. } => None,
            BenchmarkOperationKind::Commitment { database, .. } => Some(database),
            BenchmarkOperationKind::Validation { database, .. } => Some(database),
            BenchmarkOperationKind::SnapshotSidecarVerify { snapshot, .. } => Some(snapshot),
            BenchmarkOperationKind::SnapshotFrame { database, .. } => Some(database),
            BenchmarkOperationKind::SearchIndex { database, .. } => Some(database),
            BenchmarkOperationKind::HttpRouter { database, .. }
            | BenchmarkOperationKind::HttpFixtureRoute { database, .. } => Some(database),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum CommitmentBenchmarkMode {
    Create,
    Verify,
    InclusionProof,
    NonInclusionProof,
}

#[derive(Debug, Clone, Copy)]
enum ValidationBenchmarkMode {
    ValidPayloads,
    InvalidObjectRejection,
}

#[derive(Debug, Clone, Copy)]
enum SnapshotFrameBenchmarkMode {
    Encode,
    Decode,
}

fn sdk_propose_scenario() -> &'static str {
    r#"
version: 1
name: benchmark-sdk-propose
seed: 6101
clock:
  start: 2026-02-01T00:00:00Z
characters:
  - name: alice
    environments: [bench]
ledgers:
  - name: operations
steps:
  - grant:
      ledger: operations
      actor: alice
      capabilities: [propose, accept, reject, deliberate]
  - propose:
      actor: alice
      replica: bench
      ledger: operations
      as: policy
      markdown: |
        # Benchmark policy

        Baseline SDK proposition creation.
  - assert:
      - status:
          proposition: policy
          equals: pending
"#
}

fn sdk_propose_accept_scenario() -> &'static str {
    r#"
version: 1
name: benchmark-sdk-propose-accept
seed: 6102
clock:
  start: 2026-02-01T00:00:00Z
characters:
  - name: alice
    environments: [bench]
ledgers:
  - name: operations
steps:
  - grant:
      ledger: operations
      actor: alice
      capabilities: [propose, accept, reject, deliberate]
  - propose:
      actor: alice
      replica: bench
      ledger: operations
      as: policy
      markdown: |
        # Accepted benchmark policy

        Baseline SDK proposition creation with acceptance.
  - accept:
      actor: alice
      replica: bench
      proposition: policy
  - assert:
      - status:
          proposition: policy
          equals: accepted
      - pending_action_count:
          actor: alice
          equals: 0
"#
}

fn sdk_revise_scenario() -> &'static str {
    r#"
version: 1
name: benchmark-sdk-revise
seed: 6103
clock:
  start: 2026-02-01T00:00:00Z
characters:
  - name: alice
    environments: [bench]
ledgers:
  - name: operations
steps:
  - grant:
      ledger: operations
      actor: alice
      capabilities: [propose, accept, reject, deliberate]
  - propose:
      actor: alice
      replica: bench
      ledger: operations
      as: policy
      markdown: |
        # Revision benchmark policy

        Initial accepted content.
  - accept:
      actor: alice
      replica: bench
      proposition: policy
  - revise:
      actor: alice
      replica: bench
      proposition: policy
      as: policy_v2
      markdown: |
        # Revision benchmark policy

        Revised pending content.
  - assert:
      - status:
          proposition: policy
          equals: accepted
      - latest_revision:
          proposition: policy
          equals: policy_v2
      - revision_status:
          revision: policy_v2
          equals: pending
      - pending_action_count:
          actor: alice
          equals: 1
"#
}

fn sdk_reject_scenario() -> &'static str {
    r#"
version: 1
name: benchmark-sdk-reject
seed: 6104
clock:
  start: 2026-02-01T00:00:00Z
characters:
  - name: alice
    environments: [bench]
ledgers:
  - name: operations
steps:
  - grant:
      ledger: operations
      actor: alice
      capabilities: [propose, accept, reject, deliberate]
  - propose:
      actor: alice
      replica: bench
      ledger: operations
      as: policy
      markdown: |
        # Rejection benchmark policy

        Content that will be rejected.
  - reject:
      actor: alice
      replica: bench
      proposition: policy
  - assert:
      - status:
          proposition: policy
          equals: rejected
      - pending_action_count:
          actor: alice
          equals: 0
"#
}

fn sdk_local_sync_scenario() -> &'static str {
    r#"
version: 1
name: benchmark-sdk-local-sync
seed: 6105
clock:
  start: 2026-02-01T00:00:00Z
characters:
  - name: alice
    environments: [laptop, desktop]
ledgers:
  - name: operations
steps:
  - grant:
      ledger: operations
      actor: alice
      capabilities: [propose, accept, reject, deliberate]
  - propose:
      actor: alice
      replica: laptop
      ledger: operations
      as: policy
      markdown: |
        # Sync benchmark policy

        Created on the laptop.
  - sync:
      from: laptop
      to: desktop
      ledger: operations
  - revise:
      actor: alice
      replica: desktop
      proposition: policy
      as: policy_v2
      markdown: |
        # Sync benchmark policy

        Revised on the desktop.
  - sync:
      from: desktop
      to: laptop
      ledger: operations
  - accept:
      actor: alice
      replica: laptop
      proposition: policy
  - sync:
      from: laptop
      to: desktop
      ledger: operations
  - assert:
      - status:
          proposition: policy
          equals: accepted
      - effective_revision:
          proposition: policy
          equals: policy_v2
"#
}

fn sdk_projection_rebuild_scenario() -> &'static str {
    r#"
version: 1
name: benchmark-sdk-projection-rebuild
seed: 6106
clock:
  start: 2026-02-01T00:00:00Z
characters:
  - name: alice
    environments: [bench]
ledgers:
  - name: operations
steps:
  - grant:
      ledger: operations
      actor: alice
      capabilities: [propose, accept, reject, deliberate]
  - propose:
      actor: alice
      replica: bench
      ledger: operations
      as: policy
      markdown: |
        # Rebuild benchmark policy

        Canonical history must survive projection repair.
  - accept:
      actor: alice
      replica: bench
      proposition: policy
  - corrupt_projections: {}
  - rebuild_projections: {}
  - assert:
      - projection_rebuild_equivalent:
          equals: true
      - canonical_history_unchanged:
          equals: true
      - status:
          proposition: policy
          equals: accepted
      - effective_revision:
          proposition: policy
          equals: policy
"#
}

fn sdk_parallel_deliberation_conflict_scenario() -> &'static str {
    r#"
version: 1
name: benchmark-sdk-parallel-deliberation-conflict
seed: 6107
clock:
  start: 2026-02-01T00:00:00Z
characters:
  - name: alice
    environments: [replica_a, replica_b]
ledgers:
  - name: operations
steps:
  - grant:
      ledger: operations
      actor: alice
      capabilities: [propose, accept, reject, deliberate]
  - propose:
      actor: alice
      replica: replica_a
      ledger: operations
      as: policy
      markdown: |
        # Conflict benchmark policy

        Parallel deliberations must resolve deterministically.
  - sync:
      from: replica_a
      to: replica_b
      ledger: operations
  - parallel:
      branches:
        - replica: replica_a
          steps:
            - decide:
                actor: alice
                proposition: policy
                deliberation: policy
                value: accepted
                as: accepted_vote
            - settle:
                actor: alice
                proposition: policy
                revision: policy
                deliberation: policy
                as: accepted_settlement
        - replica: replica_b
          steps:
            - open_deliberation:
                actor: alice
                proposition: policy
                revision: policy
                as: reject_deliberation
            - decide:
                actor: alice
                proposition: policy
                deliberation: reject_deliberation
                value: rejected
                as: rejected_vote
            - settle:
                actor: alice
                proposition: policy
                revision: policy
                deliberation: reject_deliberation
                as: rejected_settlement
  - sync:
      from: replica_a
      to: replica_b
      ledger: operations
  - sync:
      from: replica_b
      to: replica_a
      ledger: operations
  - assert:
      - status:
          proposition: policy
          equals: rejected
      - effective_revision:
          proposition: policy
          equals: policy
      - deliberation_conflict:
          proposition: policy
          revision: policy
          equals: false
          deliberations: [policy, reject_deliberation]
"#
}

#[cfg(test)]
fn sdk_cli_check_scenario() -> &'static str {
    include_str!("../../../scenarios/smoke/pending-revision-acceptance.yaml")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkRunReport {
    schema_version: String,
    suite: BenchmarkSuite,
    command: String,
    generated_at_unix_ms: u128,
    environment: EnvironmentMetadata,
    fixture: FixtureMetadata,
    benchmarks: Vec<BenchmarkResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkResult {
    name: String,
    suite: BenchmarkSuite,
    area: String,
    cache_state: String,
    #[serde(default)]
    cache_classification: Option<CacheClassification>,
    read_only: bool,
    correctness_passed: bool,
    #[serde(default)]
    requirement_tags: Vec<String>,
    #[serde(default)]
    preconditions: Vec<String>,
    #[serde(default)]
    isolation_strategy: String,
    samples_ms: Vec<f64>,
    #[serde(default)]
    sample_observations: Vec<BenchmarkSampleObservation>,
    stats: TimingStats,
    #[serde(default)]
    phase_timings: PhaseTimings,
    #[serde(default)]
    phase_breakdown: Vec<BenchmarkPhaseBreakdown>,
    rows_returned: usize,
    #[serde(default)]
    measured_bytes: Option<u64>,
    #[serde(default)]
    network_payload_bytes: Option<u64>,
    failures: Vec<String>,
    notes: Vec<String>,
    diagnostics: BenchmarkDiagnostics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkSampleObservation {
    sample_index: usize,
    elapsed_ms: f64,
    rows_returned: usize,
    measured_bytes: Option<u64>,
    network_payload_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct CacheClassification {
    temperature: CacheTemperature,
    scope: CacheScope,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum CacheTemperature {
    Cold,
    Warm,
    First,
    Steady,
    Profiling,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum CacheScope {
    Process,
    Filesystem,
    SearchIndex,
    Request,
    Profiling,
}

fn cache_classification(label: &str) -> Option<CacheClassification> {
    let (temperature, scope) = match label {
        "cold-process" => (CacheTemperature::Cold, CacheScope::Process),
        "warm-process" => (CacheTemperature::Warm, CacheScope::Process),
        "cold-filesystem" => (CacheTemperature::Cold, CacheScope::Filesystem),
        "warm-filesystem" => (CacheTemperature::Warm, CacheScope::Filesystem),
        "cold-search-index" => (CacheTemperature::Cold, CacheScope::SearchIndex),
        "warm-search-index" => (CacheTemperature::Warm, CacheScope::SearchIndex),
        "first-request" => (CacheTemperature::First, CacheScope::Request),
        "steady-state" => (CacheTemperature::Steady, CacheScope::Request),
        "profiling" => (CacheTemperature::Profiling, CacheScope::Profiling),
        _ => return None,
    };
    Some(CacheClassification { temperature, scope })
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PhaseTimings {
    setup_ms: f64,
    warmup_total_ms: f64,
    measurement_total_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkPhaseBreakdown {
    phase: String,
    measurement: BenchmarkPhaseMeasurement,
    elapsed_ms: Option<f64>,
    notes: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum BenchmarkPhaseMeasurement {
    Measured,
    IncludedInParent,
    NotSeparatelyObservable,
}

fn operation_phase_breakdown(
    operation: &BenchmarkOperation,
    setup_ms: f64,
    warmup_total_ms: f64,
    measurement_total_ms: f64,
) -> Vec<BenchmarkPhaseBreakdown> {
    let mut phases = vec![
        BenchmarkPhaseBreakdown {
            phase: "setup".to_string(),
            measurement: BenchmarkPhaseMeasurement::Measured,
            elapsed_ms: Some(setup_ms),
            notes: vec!["collects operation diagnostics before warmup".to_string()],
        },
        BenchmarkPhaseBreakdown {
            phase: "warmup".to_string(),
            measurement: BenchmarkPhaseMeasurement::Measured,
            elapsed_ms: Some(warmup_total_ms),
            notes: vec!["runs configured warmup iterations outside measured samples".to_string()],
        },
        BenchmarkPhaseBreakdown {
            phase: "measurement".to_string(),
            measurement: BenchmarkPhaseMeasurement::Measured,
            elapsed_ms: Some(measurement_total_ms),
            notes: vec!["wall-clock total for measured benchmark iterations".to_string()],
        },
    ];

    phases.extend(operation_subphase_breakdown(operation));

    phases
}

fn operation_subphase_breakdown(operation: &BenchmarkOperation) -> Vec<BenchmarkPhaseBreakdown> {
    let phases = match &operation.kind {
        BenchmarkOperationKind::Sql { .. } => vec![
            (
                "database-read",
                "opens SQLite and executes the benchmark query",
            ),
            (
                "query-plan-inspection",
                "captures EXPLAIN QUERY PLAN outside the measured loop",
            ),
            (
                "result-materialization",
                "counts returned rows and payload bytes",
            ),
        ],
        BenchmarkOperationKind::TableSize { .. } => vec![
            ("sqlite-dbstat", "reads SQLite dbstat page-size metadata"),
            (
                "size-aggregation",
                "aggregates measured bytes for selected projection or index tables",
            ),
        ],
        BenchmarkOperationKind::FileInventory { bytes_kind, .. } => vec![
            (
                "artifact-scan",
                "walks fixture artifacts matching configured extensions",
            ),
            (
                if bytes_kind.is_network_payload() {
                    "network-payload-accounting"
                } else {
                    "artifact-byte-accounting"
                },
                "records matched artifact bytes separately from elapsed time",
            ),
        ],
        BenchmarkOperationKind::Process { .. } | BenchmarkOperationKind::CliWorkflow { .. } => {
            vec![
                (
                    "process-startup",
                    "included in each process sample wall-clock time",
                ),
                (
                    "configuration-loading",
                    "included when the child command loads configuration",
                ),
                (
                    "core-operation",
                    "included in process benchmark wall-clock time",
                ),
                (
                    "output-formatting",
                    "included in process benchmark wall-clock time and measured output bytes",
                ),
            ]
        }
        BenchmarkOperationKind::Scenario { .. } => vec![
            ("scenario-parse", "parses deterministic scenario YAML"),
            (
                "canonicalization",
                "included in SDK-backed scenario execution",
            ),
            ("signing", "included in SDK-backed scenario execution"),
            (
                "database-writes",
                "included in SDK-backed scenario execution",
            ),
            (
                "projection-updates",
                "included in SDK-backed scenario execution",
            ),
            ("verification", "scenario assertions guard correctness"),
        ],
        BenchmarkOperationKind::Commitment { .. } => vec![
            ("hash-enumeration", "loads fixture content hashes"),
            (
                "commitment-construction",
                "builds or verifies Merkle commitments",
            ),
            (
                "proof-verification",
                "creates or verifies sampled proofs when configured",
            ),
        ],
        BenchmarkOperationKind::Validation { .. } => vec![
            (
                "payload-loading",
                "loads protocol payloads from the fixture database",
            ),
            ("object-validation", "runs SDK validation helpers"),
            (
                "invalid-object-rejection",
                "included when the benchmark mode exercises invalid payloads",
            ),
        ],
        BenchmarkOperationKind::SnapshotSidecarVerify { .. } => vec![
            (
                "snapshot-loading",
                "loads packaged snapshot sidecar metadata",
            ),
            ("database-read", "loads fixture object and hash counts"),
            (
                "verification",
                "compares sidecar counts with SQLite contents",
            ),
        ],
        BenchmarkOperationKind::SnapshotFrame { .. } => vec![
            (
                "snapshot-generation",
                "loads fixture protocol objects and builds a snapshot manifest",
            ),
            (
                "snapshot-serialization",
                "encodes a FACTSNAP frame with fact-commitment",
            ),
            (
                "snapshot-loading",
                "decodes and verifies a generated FACTSNAP frame in decode mode",
            ),
        ],
        BenchmarkOperationKind::SearchIndex { .. } => vec![
            (
                "search-index-readiness",
                "opens fact-store and verifies or rebuilds stale markdown search index metadata",
            ),
            (
                "candidate-selection",
                "loads matching search candidates through fact-store",
            ),
            (
                "result-ranking",
                "returns ranked search hits with object identifiers and scores",
            ),
        ],
        BenchmarkOperationKind::HttpRouter { .. }
        | BenchmarkOperationKind::HttpFixtureRoute { .. } => vec![
            ("request-construction", "builds an in-process HTTP request"),
            (
                "router-dispatch",
                "dispatches through the reference fact-http router",
            ),
            ("response-serialization", "records response payload bytes"),
        ],
    };
    phases
        .into_iter()
        .map(|(phase, note)| BenchmarkPhaseBreakdown {
            phase: phase.to_string(),
            measurement: BenchmarkPhaseMeasurement::IncludedInParent,
            elapsed_ms: None,
            notes: vec![
                note.to_string(),
                "not independently timed without lower-level instrumentation".to_string(),
            ],
        })
        .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkDiagnostics {
    operation_kind: String,
    resources_before: ResourceSnapshot,
    resources_after: ResourceSnapshot,
    #[serde(default)]
    resource_delta: ResourceDelta,
    sqlite: Option<SqliteDiagnostics>,
}

impl BenchmarkDiagnostics {
    fn collect(operation: &BenchmarkOperation) -> Self {
        Self {
            operation_kind: operation.kind.label().to_string(),
            resources_before: ResourceSnapshot::collect(operation),
            resources_after: ResourceSnapshot::default(),
            resource_delta: ResourceDelta::default(),
            sqlite: collect_sqlite_diagnostics(operation).ok().flatten(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ResourceDelta {
    process_rss_kib_delta: Option<i64>,
    #[serde(default)]
    process_peak_rss_kib_delta: Option<i64>,
    process_cpu_seconds_delta: Option<f64>,
    artifact_bytes_delta: Option<i64>,
    #[serde(default)]
    disk_read_bytes_delta: Option<i64>,
    #[serde(default)]
    disk_write_bytes_delta: Option<i64>,
}

impl ResourceDelta {
    fn between(before: &ResourceSnapshot, after: &ResourceSnapshot) -> Self {
        Self {
            process_rss_kib_delta: option_delta_u64(before.process_rss_kib, after.process_rss_kib),
            process_peak_rss_kib_delta: option_delta_u64(
                before.process_peak_rss_kib,
                after.process_peak_rss_kib,
            ),
            process_cpu_seconds_delta: option_delta_f64(
                before.process_cpu_seconds,
                after.process_cpu_seconds,
            ),
            artifact_bytes_delta: option_delta_u64(before.artifact_bytes, after.artifact_bytes),
            disk_read_bytes_delta: option_delta_u64(before.disk_read_bytes, after.disk_read_bytes),
            disk_write_bytes_delta: option_delta_u64(
                before.disk_write_bytes,
                after.disk_write_bytes,
            ),
        }
    }
}

fn option_delta_u64(before: Option<u64>, after: Option<u64>) -> Option<i64> {
    Some(i64::try_from(after?).ok()? - i64::try_from(before?).ok()?)
}

fn option_delta_f64(before: Option<f64>, after: Option<f64>) -> Option<f64> {
    Some(after? - before?)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ResourceSnapshot {
    process_rss_kib: Option<u64>,
    #[serde(default)]
    process_peak_rss_kib: Option<u64>,
    process_cpu_seconds: Option<f64>,
    artifact_bytes: Option<u64>,
    #[serde(default)]
    disk_read_bytes: Option<u64>,
    #[serde(default)]
    disk_write_bytes: Option<u64>,
}

impl ResourceSnapshot {
    fn collect(operation: &BenchmarkOperation) -> Self {
        let process_io = process_io_bytes();
        Self {
            process_rss_kib: process_rss_kib(),
            process_peak_rss_kib: process_peak_rss_kib(),
            process_cpu_seconds: process_cpu_seconds(),
            artifact_bytes: operation
                .resource_root()
                .and_then(|path| path_bytes(path).ok()),
            disk_read_bytes: process_io.as_ref().map(|io| io.read_bytes),
            disk_write_bytes: process_io.as_ref().map(|io| io.write_bytes),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SqliteDiagnostics {
    database: PathBuf,
    database_size_bytes: u64,
    page_count: Option<u64>,
    page_size: Option<u64>,
    journal_mode: Option<String>,
    query_plan: Vec<String>,
    uses_full_scan: bool,
    uses_temporary_btree: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TimingStats {
    samples: usize,
    min_ms: Option<f64>,
    mean_ms: Option<f64>,
    median_ms: Option<f64>,
    p95_ms: Option<f64>,
    max_ms: Option<f64>,
    #[serde(default)]
    outlier_count: usize,
    #[serde(default)]
    outliers_ms: Vec<f64>,
}

impl TimingStats {
    fn from_samples(samples: &[f64]) -> Self {
        if samples.is_empty() {
            return Self {
                samples: 0,
                min_ms: None,
                mean_ms: None,
                median_ms: None,
                p95_ms: None,
                max_ms: None,
                outlier_count: 0,
                outliers_ms: Vec::new(),
            };
        }
        let mut sorted = samples.to_vec();
        sorted.sort_by(f64::total_cmp);
        let mean = sorted.iter().sum::<f64>() / sorted.len() as f64;
        let outliers = timing_outliers(&sorted);
        Self {
            samples: sorted.len(),
            min_ms: sorted.first().copied(),
            mean_ms: Some(mean),
            median_ms: percentile(&sorted, 0.50),
            p95_ms: percentile(&sorted, 0.95),
            max_ms: sorted.last().copied(),
            outlier_count: outliers.len(),
            outliers_ms: outliers,
        }
    }
}

fn timing_outliers(sorted: &[f64]) -> Vec<f64> {
    if sorted.len() < 4 {
        return Vec::new();
    }
    let Some(median) = percentile(sorted, 0.50) else {
        return Vec::new();
    };
    let mut deviations = sorted
        .iter()
        .map(|sample| (sample - median).abs())
        .collect::<Vec<_>>();
    deviations.sort_by(f64::total_cmp);
    let Some(median_absolute_deviation) = percentile(&deviations, 0.50) else {
        return Vec::new();
    };
    if median_absolute_deviation == 0.0 {
        return sorted
            .iter()
            .copied()
            .filter(|sample| (sample - median).abs() > median.max(1.0) * 0.25)
            .collect();
    }
    sorted
        .iter()
        .copied()
        .filter(|sample| ((sample - median).abs() / median_absolute_deviation) > 6.0)
        .collect()
}

fn percentile(sorted: &[f64], percentile: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let index = ((sorted.len() - 1) as f64 * percentile).ceil() as usize;
    sorted.get(index).copied()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FixtureMetadata {
    path: PathBuf,
    profile: String,
    seed: Option<u64>,
    proposition_count: u64,
    total_object_count: u64,
    projected_row_count: u64,
    search_index_row_count: u64,
    #[serde(default)]
    search_index_size_bytes: u64,
    database_size_bytes: u64,
    actor_count: Option<u64>,
    ledger_count: Option<u64>,
    replica_count: Option<u64>,
    #[serde(default)]
    simulator_revision: Option<String>,
    #[serde(default)]
    facts_sdk_revision: Option<String>,
    #[serde(default)]
    facts_implementation_revision: Option<String>,
    sqlite_databases: Vec<PathBuf>,
}

impl FixtureMetadata {
    fn from_fixture(fixture: &Path) -> Result<Self> {
        let manifest_path = fixture.join("manifest.json");
        let manifest = read_json_file(&manifest_path)?;
        let profile = manifest["profile"]
            .as_str()
            .unwrap_or("unknown")
            .to_string();
        let seed = manifest["seed"].as_u64();
        let sqlite_databases = sqlite_databases(fixture)?;
        let database_size_bytes = sqlite_databases
            .iter()
            .map(|path| {
                std::fs::metadata(path)
                    .map(|metadata| metadata.len())
                    .unwrap_or(0)
            })
            .sum();

        let mut measured_proposition_count = 0;
        let mut measured_object_count = 0;
        let mut measured_projected_row_count = 0;
        let mut measured_search_index_row_count = 0;
        let mut measured_search_index_size_bytes = 0;

        for database in &sqlite_databases {
            let connection = rusqlite::Connection::open(database)?;
            let tables = sqlite_tables(&connection)?;
            if tables.contains("protocol_object") {
                measured_proposition_count += count_sql(
                    &connection,
                    "SELECT COUNT(*) FROM protocol_object WHERE object_type='proposition'",
                )?;
                measured_object_count +=
                    count_sql(&connection, "SELECT COUNT(*) FROM protocol_object")?;
            }
            for table in tables
                .iter()
                .filter(|table| table.starts_with("projected_") || table.starts_with("projection_"))
            {
                measured_projected_row_count +=
                    count_sql(&connection, &format!("SELECT COUNT(*) FROM {table}"))?;
            }
            if let Some(table) = search_table(&tables) {
                measured_search_index_row_count +=
                    count_sql(&connection, &format!("SELECT COUNT(*) FROM {table}"))?;
                measured_search_index_size_bytes += sqlite_table_bytes(&connection, &table)
                    .unwrap_or_else(|| manifest["search_index_size_bytes"].as_u64().unwrap_or(0));
            }
        }
        let manifest_proposition_count = manifest["proposition_count"]
            .as_u64()
            .or_else(|| manifest["target_objects"].as_u64())
            .unwrap_or_default();
        let proposition_count = if measured_proposition_count > 0 {
            measured_proposition_count
        } else {
            manifest_proposition_count
        };
        let total_object_count = if measured_object_count > 0 {
            measured_object_count
        } else {
            manifest["object_count"].as_u64().unwrap_or_default()
        };
        let projected_row_count = if measured_projected_row_count > 0 {
            measured_projected_row_count
        } else {
            manifest["projected_row_count"].as_u64().unwrap_or_default()
        };
        let search_index_row_count = if measured_search_index_row_count > 0 {
            measured_search_index_row_count
        } else {
            manifest["search_index_row_count"]
                .as_u64()
                .unwrap_or_default()
        };
        let search_index_size_bytes = if measured_search_index_size_bytes > 0 {
            measured_search_index_size_bytes
        } else {
            manifest["search_index_size_bytes"]
                .as_u64()
                .unwrap_or_default()
        };

        Ok(Self {
            path: fixture.to_path_buf(),
            profile,
            seed,
            proposition_count,
            total_object_count,
            projected_row_count,
            search_index_row_count,
            search_index_size_bytes,
            database_size_bytes,
            actor_count: manifest["actor_count"].as_u64(),
            ledger_count: manifest["ledger_count"].as_u64(),
            replica_count: manifest["replica_count"].as_u64(),
            simulator_revision: manifest_revision(
                &manifest,
                &[
                    "simulator_revision",
                    "benchmark_project_commit",
                    "generator_revision",
                ],
            )
            .or_else(|| git_commit(&benchmark_repo_path())),
            facts_sdk_revision: manifest_revision(
                &manifest,
                &["facts_sdk_revision", "sdk_source_commit"],
            )
            .or_else(|| git_commit(&facts_repo_path())),
            facts_implementation_revision: manifest_revision(
                &manifest,
                &["facts_implementation_revision", "facts_source_commit"],
            )
            .or_else(|| git_commit(&facts_repo_path())),
            sqlite_databases,
        })
    }
}

fn manifest_revision(manifest: &serde_json::Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .filter_map(|key| manifest[*key].as_str())
        .map(str::trim)
        .find(|value| !value.is_empty() && *value != "unknown")
        .map(str::to_string)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EnvironmentMetadata {
    operating_system: String,
    architecture: String,
    cpu_model: Option<String>,
    core_count: Option<usize>,
    memory_bytes: Option<u64>,
    filesystem: Option<String>,
    storage_type: Option<String>,
    rust_version: Option<String>,
    build_profile: String,
    feature_flags: Vec<String>,
    facts_source_commit: Option<String>,
    sdk_source_commit: Option<String>,
    benchmark_project_commit: Option<String>,
    fixture_path: PathBuf,
}

impl EnvironmentMetadata {
    fn collect(fixture: &Path) -> Result<Self> {
        Ok(Self {
            operating_system: std::env::consts::OS.to_string(),
            architecture: std::env::consts::ARCH.to_string(),
            cpu_model: cpu_model(),
            core_count: std::thread::available_parallelism().ok().map(usize::from),
            memory_bytes: memory_bytes(),
            filesystem: command_stdout("df", &["-T", "."]).or_else(|| command_stdout("df", &["."])),
            storage_type: storage_type(),
            rust_version: command_stdout("rustc", &["--version"]),
            build_profile: if cfg!(debug_assertions) {
                "debug".to_string()
            } else {
                "release".to_string()
            },
            feature_flags: Vec::new(),
            facts_source_commit: git_commit(&facts_repo_path()),
            sdk_source_commit: git_commit(&facts_repo_path()),
            benchmark_project_commit: git_commit(&benchmark_repo_path()),
            fixture_path: fixture.to_path_buf(),
        })
    }
}

fn environment_differences(
    baseline: &EnvironmentMetadata,
    current: &EnvironmentMetadata,
) -> Vec<EnvironmentDifference> {
    let mut differences = Vec::new();
    push_environment_difference(
        &mut differences,
        "operating_system",
        Some(baseline.operating_system.as_str()),
        Some(current.operating_system.as_str()),
    );
    push_environment_difference(
        &mut differences,
        "architecture",
        Some(baseline.architecture.as_str()),
        Some(current.architecture.as_str()),
    );
    push_environment_difference(
        &mut differences,
        "cpu_model",
        baseline.cpu_model.as_deref(),
        current.cpu_model.as_deref(),
    );
    if baseline.core_count != current.core_count {
        differences.push(EnvironmentDifference {
            field: "core_count".to_string(),
            baseline: baseline.core_count.map(|count| count.to_string()),
            current: current.core_count.map(|count| count.to_string()),
        });
    }
    push_environment_u64_difference(
        &mut differences,
        "memory_bytes",
        baseline.memory_bytes,
        current.memory_bytes,
    );
    push_environment_difference(
        &mut differences,
        "filesystem",
        baseline.filesystem.as_deref(),
        current.filesystem.as_deref(),
    );
    push_environment_difference(
        &mut differences,
        "storage_type",
        baseline.storage_type.as_deref(),
        current.storage_type.as_deref(),
    );
    push_environment_difference(
        &mut differences,
        "build_profile",
        Some(baseline.build_profile.as_str()),
        Some(current.build_profile.as_str()),
    );
    push_environment_vec_difference(
        &mut differences,
        "feature_flags",
        &baseline.feature_flags,
        &current.feature_flags,
    );
    push_environment_difference(
        &mut differences,
        "rust_version",
        baseline.rust_version.as_deref(),
        current.rust_version.as_deref(),
    );
    push_environment_difference(
        &mut differences,
        "facts_source_commit",
        baseline.facts_source_commit.as_deref(),
        current.facts_source_commit.as_deref(),
    );
    push_environment_difference(
        &mut differences,
        "sdk_source_commit",
        baseline.sdk_source_commit.as_deref(),
        current.sdk_source_commit.as_deref(),
    );
    push_environment_difference(
        &mut differences,
        "benchmark_project_commit",
        baseline.benchmark_project_commit.as_deref(),
        current.benchmark_project_commit.as_deref(),
    );
    differences
}

fn push_environment_vec_difference(
    differences: &mut Vec<EnvironmentDifference>,
    field: &str,
    baseline: &[String],
    current: &[String],
) {
    if baseline != current {
        differences.push(EnvironmentDifference {
            field: field.to_string(),
            baseline: Some(join_or_none(baseline.iter().map(String::as_str))),
            current: Some(join_or_none(current.iter().map(String::as_str))),
        });
    }
}

fn push_environment_difference(
    differences: &mut Vec<EnvironmentDifference>,
    field: &str,
    baseline: Option<&str>,
    current: Option<&str>,
) {
    if baseline != current {
        differences.push(EnvironmentDifference {
            field: field.to_string(),
            baseline: baseline.map(str::to_string),
            current: current.map(str::to_string),
        });
    }
}

fn push_environment_u64_difference(
    differences: &mut Vec<EnvironmentDifference>,
    field: &str,
    baseline: Option<u64>,
    current: Option<u64>,
) {
    if baseline != current {
        differences.push(EnvironmentDifference {
            field: field.to_string(),
            baseline: baseline.map(|value| value.to_string()),
            current: current.map(|value| value.to_string()),
        });
    }
}

fn fixture_differences(
    baseline: &FixtureMetadata,
    current: &FixtureMetadata,
) -> Vec<BenchmarkFixtureDifference> {
    let mut differences = Vec::new();
    push_fixture_difference(
        &mut differences,
        "profile",
        Some(baseline.profile.as_str()),
        Some(current.profile.as_str()),
    );
    push_fixture_u64_difference(&mut differences, "seed", baseline.seed, current.seed);
    push_fixture_u64_difference(
        &mut differences,
        "proposition_count",
        Some(baseline.proposition_count),
        Some(current.proposition_count),
    );
    push_fixture_u64_difference(
        &mut differences,
        "total_object_count",
        Some(baseline.total_object_count),
        Some(current.total_object_count),
    );
    push_fixture_u64_difference(
        &mut differences,
        "projected_row_count",
        Some(baseline.projected_row_count),
        Some(current.projected_row_count),
    );
    push_fixture_u64_difference(
        &mut differences,
        "search_index_row_count",
        Some(baseline.search_index_row_count),
        Some(current.search_index_row_count),
    );
    push_fixture_u64_difference(
        &mut differences,
        "search_index_size_bytes",
        Some(baseline.search_index_size_bytes),
        Some(current.search_index_size_bytes),
    );
    push_fixture_u64_difference(
        &mut differences,
        "database_size_bytes",
        Some(baseline.database_size_bytes),
        Some(current.database_size_bytes),
    );
    push_fixture_u64_difference(
        &mut differences,
        "actor_count",
        baseline.actor_count,
        current.actor_count,
    );
    push_fixture_u64_difference(
        &mut differences,
        "ledger_count",
        baseline.ledger_count,
        current.ledger_count,
    );
    push_fixture_u64_difference(
        &mut differences,
        "replica_count",
        baseline.replica_count,
        current.replica_count,
    );
    push_fixture_difference(
        &mut differences,
        "simulator_revision",
        baseline.simulator_revision.as_deref(),
        current.simulator_revision.as_deref(),
    );
    push_fixture_difference(
        &mut differences,
        "facts_sdk_revision",
        baseline.facts_sdk_revision.as_deref(),
        current.facts_sdk_revision.as_deref(),
    );
    push_fixture_difference(
        &mut differences,
        "facts_implementation_revision",
        baseline.facts_implementation_revision.as_deref(),
        current.facts_implementation_revision.as_deref(),
    );
    differences
}

fn push_fixture_difference(
    differences: &mut Vec<BenchmarkFixtureDifference>,
    field: &str,
    baseline: Option<&str>,
    current: Option<&str>,
) {
    if baseline != current {
        differences.push(BenchmarkFixtureDifference {
            field: field.to_string(),
            baseline: baseline.map(str::to_string),
            current: current.map(str::to_string),
        });
    }
}

fn push_fixture_u64_difference(
    differences: &mut Vec<BenchmarkFixtureDifference>,
    field: &str,
    baseline: Option<u64>,
    current: Option<u64>,
) {
    if baseline != current {
        differences.push(BenchmarkFixtureDifference {
            field: field.to_string(),
            baseline: baseline.map(|value| value.to_string()),
            current: current.map(|value| value.to_string()),
        });
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkComparison {
    schema_version: String,
    baseline: PathBuf,
    current: PathBuf,
    warning_threshold_percent: f64,
    regression_threshold_percent: f64,
    #[serde(default)]
    environment_compatible: bool,
    #[serde(default)]
    environment_differences: Vec<EnvironmentDifference>,
    #[serde(default)]
    fixture_compatible: bool,
    #[serde(default)]
    fixture_differences: Vec<BenchmarkFixtureDifference>,
    #[serde(default)]
    cache_compatible: bool,
    #[serde(default)]
    cache_differences: Vec<BenchmarkCacheDifference>,
    regressions: Vec<BenchmarkDelta>,
    warnings: Vec<BenchmarkDelta>,
    improvements: Vec<BenchmarkDelta>,
    informational: Vec<BenchmarkDelta>,
    missing_in_current: Vec<String>,
    new_in_current: Vec<String>,
    #[serde(default)]
    incorrect_baseline_benchmarks: Vec<BenchmarkComparisonCorrectnessIssue>,
    #[serde(default)]
    incorrect_current_benchmarks: Vec<BenchmarkComparisonCorrectnessIssue>,
    #[serde(default)]
    regression_decision_ready: bool,
    #[serde(default)]
    regression_decision_blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkCacheDifference {
    benchmark: String,
    baseline_cache_state: String,
    current_cache_state: String,
    baseline_classification: Option<CacheClassification>,
    current_classification: Option<CacheClassification>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkComparisonCorrectnessIssue {
    benchmark: String,
    correctness_passed: bool,
    failures: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EnvironmentDifference {
    field: String,
    baseline: Option<String>,
    current: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkFixtureDifference {
    field: String,
    baseline: Option<String>,
    current: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkMatrixComparison {
    schema_version: String,
    baseline_summary: PathBuf,
    current_summary: PathBuf,
    warning_threshold_percent: f64,
    regression_threshold_percent: f64,
    #[serde(default)]
    baseline_summary_ready: bool,
    #[serde(default)]
    current_summary_ready: bool,
    #[serde(default)]
    baseline_failure_count: usize,
    #[serde(default)]
    current_failure_count: usize,
    compared_reports: Vec<BenchmarkMatrixReportComparison>,
    missing_in_current: Vec<String>,
    new_in_current: Vec<String>,
    failures: Vec<BenchmarkMatrixComparisonFailure>,
    total_regressions: usize,
    total_warnings: usize,
    total_improvements: usize,
    #[serde(default)]
    total_environment_incompatible_reports: usize,
    #[serde(default)]
    total_environment_differences: usize,
    #[serde(default)]
    total_cache_incompatible_reports: usize,
    #[serde(default)]
    total_fixture_incompatible_reports: usize,
    #[serde(default)]
    total_fixture_differences: usize,
    #[serde(default)]
    total_cache_differences: usize,
    #[serde(default)]
    total_incorrect_current_benchmarks: usize,
    #[serde(default)]
    regression_decision_ready: bool,
    #[serde(default)]
    regression_decision_blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkMatrixReportComparison {
    key: String,
    profile: String,
    level: String,
    seed: Option<u64>,
    baseline_report: PathBuf,
    current_report: PathBuf,
    environment_compatible: bool,
    #[serde(default)]
    environment_difference_count: usize,
    #[serde(default)]
    fixture_compatible: bool,
    #[serde(default)]
    fixture_difference_count: usize,
    #[serde(default)]
    cache_compatible: bool,
    #[serde(default)]
    cache_difference_count: usize,
    regression_count: usize,
    warning_count: usize,
    improvement_count: usize,
    missing_benchmark_count: usize,
    new_benchmark_count: usize,
    #[serde(default)]
    incorrect_current_count: usize,
    #[serde(default)]
    regression_decision_ready: bool,
    #[serde(default)]
    regression_decision_blockers: Vec<String>,
    comparison: BenchmarkComparison,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkMatrixComparisonFailure {
    key: String,
    baseline_report: Option<PathBuf>,
    current_report: Option<PathBuf>,
    error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkAuditReport {
    schema_version: String,
    generated_at_unix_ms: u128,
    base: PathBuf,
    fixture_matrix_ready: bool,
    inventory: BenchmarkFixtureInventory,
    required_suites: Vec<BenchmarkSuite>,
    covered_suites: Vec<BenchmarkSuite>,
    missing_suites: Vec<BenchmarkSuite>,
    required_areas: Vec<String>,
    covered_areas: Vec<String>,
    missing_areas: Vec<String>,
    #[serde(default)]
    required_requirements: Vec<String>,
    #[serde(default)]
    covered_requirements: Vec<String>,
    #[serde(default)]
    missing_requirements: Vec<String>,
    #[serde(default)]
    required_representative_benchmarks: Vec<String>,
    #[serde(default)]
    covered_representative_benchmarks: Vec<String>,
    #[serde(default)]
    missing_representative_benchmarks: Vec<String>,
    #[serde(default)]
    required_cli_workflows: Vec<String>,
    #[serde(default)]
    covered_cli_workflows: Vec<String>,
    #[serde(default)]
    missing_cli_workflows: Vec<String>,
    #[serde(default)]
    covered_sampled_cli_workflows: Vec<String>,
    #[serde(default)]
    missing_sampled_cli_workflows: Vec<String>,
    #[serde(default)]
    covered_cache_temperatures: Vec<String>,
    #[serde(default)]
    missing_cache_temperatures: Vec<String>,
    baseline_profile_levels: BTreeMap<String, BTreeSet<String>>,
    missing_baseline_profile_levels: Vec<BenchmarkMissingProfileLevel>,
    #[serde(default)]
    baseline_cache_profile_levels: BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
    #[serde(default)]
    missing_cache_profile_levels: Vec<BenchmarkMissingCacheProfileLevel>,
    baseline_summaries: Vec<BenchmarkAuditSummaryInput>,
    #[serde(default)]
    invalid_baseline_summaries: Vec<String>,
    missing_report_files: Vec<PathBuf>,
    #[serde(default)]
    invalid_report_files: Vec<String>,
    missing_environment_manifests: Vec<PathBuf>,
    #[serde(default)]
    invalid_environment_metadata: Vec<String>,
    missing_reproduce_commands: Vec<PathBuf>,
    non_release_environment_manifests: Vec<PathBuf>,
    non_release_reports: Vec<PathBuf>,
    not_ready_summaries: Vec<PathBuf>,
    failing_summaries: Vec<PathBuf>,
    fixture_metadata_mismatches: Vec<PathBuf>,
    #[serde(default)]
    missing_fixture_metadata: Vec<String>,
    #[serde(default)]
    invalid_report_counts: Vec<String>,
    invalid_cache_labels: Vec<String>,
    #[serde(default)]
    invalid_cache_metadata: Vec<String>,
    #[serde(default)]
    insufficient_warmup_iterations: Vec<PathBuf>,
    #[serde(default)]
    missing_failure_logs: Vec<PathBuf>,
    #[serde(default)]
    invalid_failure_logs: Vec<String>,
    #[serde(default)]
    missing_report_metadata: Vec<PathBuf>,
    #[serde(default)]
    missing_benchmark_metadata: Vec<String>,
    #[serde(default)]
    invalid_requirement_tags: Vec<String>,
    #[serde(default)]
    missing_bottleneck_summaries: Vec<PathBuf>,
    #[serde(default)]
    invalid_bottleneck_summaries: Vec<String>,
    #[serde(default)]
    insufficient_baseline_iterations: Vec<PathBuf>,
    insufficient_sample_benchmarks: Vec<String>,
    #[serde(default)]
    invalid_sample_observations: Vec<String>,
    #[serde(default)]
    invalid_phase_metadata: Vec<String>,
    #[serde(default)]
    invalid_resource_metadata: Vec<String>,
    #[serde(default)]
    invalid_sqlite_metadata: Vec<String>,
    #[serde(default)]
    invalid_timing_statistics: Vec<String>,
    incorrect_benchmarks: Vec<String>,
    #[serde(default)]
    remediation_commands: Vec<BenchmarkAuditRemediationCommand>,
    total_reports: usize,
    total_benchmarks: usize,
    ready: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct BenchmarkAuditRemediationCommand {
    kind: String,
    #[serde(default)]
    profile: Option<String>,
    #[serde(default)]
    level: Option<String>,
    #[serde(default)]
    cache_temperature: Option<String>,
    command: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkAuditSummaryInput {
    path: PathBuf,
    suite: BenchmarkSuite,
    ready: bool,
    report_count: usize,
    benchmark_count: usize,
    failure_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkGrowthAnalysis {
    schema_version: String,
    generated_at_unix_ms: u128,
    baseline_summaries: Vec<PathBuf>,
    trend_count: usize,
    complete_trend_count: usize,
    insufficient_trend_count: usize,
    incorrect_trend_count: usize,
    failures: Vec<BenchmarkGrowthAnalysisFailure>,
    trends: Vec<BenchmarkGrowthTrend>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkGrowthTrend {
    profile: String,
    benchmark: String,
    classification: BenchmarkGrowthClassification,
    points: Vec<BenchmarkGrowthPoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkGrowthClassification {
    shape: BenchmarkGrowthShape,
    data_factor: Option<f64>,
    latency_factor: Option<f64>,
    rows_factor: Option<f64>,
    likely_driver: String,
    notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum BenchmarkGrowthShape {
    Constant,
    Logarithmic,
    Linear,
    Superlinear,
    DominatedByResultSize,
    InsufficientData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkGrowthPoint {
    summary: PathBuf,
    report: PathBuf,
    level: String,
    fixture: PathBuf,
    seed: Option<u64>,
    proposition_count: u64,
    total_object_count: u64,
    projected_row_count: u64,
    search_index_row_count: u64,
    #[serde(default)]
    search_index_size_bytes: u64,
    database_size_bytes: u64,
    rows_returned: usize,
    samples: usize,
    median_ms: f64,
    p95_ms: Option<f64>,
    cache_state: String,
    correctness_passed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkGrowthAnalysisFailure {
    summary: PathBuf,
    report: PathBuf,
    error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkBudgets {
    schema_version: String,
    generated_at_unix_ms: u128,
    baseline_summaries: Vec<PathBuf>,
    warning_multiplier: f64,
    regression_multiplier: f64,
    minimum_warning_ms: f64,
    minimum_regression_ms: f64,
    entry_count: usize,
    incorrect_entry_count: usize,
    failures: Vec<BenchmarkBudgetFailure>,
    budgets: Vec<BenchmarkBudgetEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkBudgetEntry {
    profile: String,
    level: String,
    seed: Option<u64>,
    suite: BenchmarkSuite,
    area: String,
    benchmark: String,
    cache_state: String,
    baseline_median_ms: f64,
    baseline_p95_ms: Option<f64>,
    warning_budget_ms: f64,
    regression_budget_ms: f64,
    samples: usize,
    correctness_passed: bool,
    source_summary: PathBuf,
    source_report: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkBudgetFailure {
    summary: PathBuf,
    report: PathBuf,
    error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkBudgetCheck {
    schema_version: String,
    generated_at_unix_ms: u128,
    budgets: PathBuf,
    baseline_summary: PathBuf,
    checked_count: usize,
    warning_count: usize,
    regression_count: usize,
    incorrect_count: usize,
    missing_budget_count: usize,
    failure_count: usize,
    passed: bool,
    results: Vec<BenchmarkBudgetCheckResult>,
    missing_budgets: Vec<BenchmarkMissingBudget>,
    failures: Vec<BenchmarkBudgetCheckFailure>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkBudgetCheckResult {
    key: String,
    profile: String,
    level: String,
    seed: Option<u64>,
    suite: BenchmarkSuite,
    area: String,
    benchmark: String,
    cache_state: String,
    baseline_median_ms: f64,
    warning_budget_ms: f64,
    regression_budget_ms: f64,
    current_median_ms: f64,
    current_p95_ms: Option<f64>,
    percentage_over_baseline: Option<f64>,
    classification: BenchmarkBudgetStatus,
    correctness_passed: bool,
    source_report: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkMissingBudget {
    key: String,
    profile: String,
    level: String,
    seed: Option<u64>,
    benchmark: String,
    report: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkBudgetCheckFailure {
    key: String,
    report: PathBuf,
    error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum BenchmarkBudgetStatus {
    WithinBudget,
    Warning,
    Regression,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkProfilePlan {
    schema_version: String,
    generated_at_unix_ms: u128,
    baseline_summaries: Vec<PathBuf>,
    limit: usize,
    candidate_count: usize,
    failure_count: usize,
    candidates: Vec<BenchmarkProfileCandidate>,
    failures: Vec<BenchmarkProfilePlanFailure>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkProfileCandidate {
    profile: String,
    level: String,
    seed: Option<u64>,
    suite: BenchmarkSuite,
    area: String,
    benchmark: String,
    operation_kind: String,
    median_ms: f64,
    p95_ms: f64,
    rows_returned: usize,
    #[serde(default)]
    measured_bytes: Option<u64>,
    #[serde(default)]
    network_payload_bytes: Option<u64>,
    #[serde(default)]
    process_rss_kib_delta: Option<i64>,
    #[serde(default)]
    process_peak_rss_kib_delta: Option<i64>,
    #[serde(default)]
    process_cpu_seconds_delta: Option<f64>,
    #[serde(default)]
    artifact_bytes_delta: Option<i64>,
    #[serde(default)]
    disk_read_bytes_delta: Option<i64>,
    #[serde(default)]
    disk_write_bytes_delta: Option<i64>,
    correctness_passed: bool,
    priority_score: f64,
    reasons: Vec<String>,
    source_summary: PathBuf,
    source_report: PathBuf,
    sqlite_query_plan: Vec<String>,
    suggested_commands: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkProfilePlanFailure {
    summary: PathBuf,
    report: PathBuf,
    error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkAcceptanceReport {
    schema_version: String,
    generated_at_unix_ms: u128,
    audit: PathBuf,
    growth_analysis: PathBuf,
    budget_check: PathBuf,
    profile_plan: PathBuf,
    accepted: bool,
    blockers: Vec<String>,
    #[serde(default)]
    blocker_evidence: BTreeMap<String, usize>,
    #[serde(default)]
    remediation_commands: Vec<BenchmarkAuditRemediationCommand>,
    total_reports: usize,
    total_benchmarks: usize,
    fixture_matrix_ready: bool,
    readiness_ready: bool,
    growth_trend_count: usize,
    complete_growth_trend_count: usize,
    insufficient_growth_trend_count: usize,
    incorrect_growth_trend_count: usize,
    budget_checked_count: usize,
    budget_warning_count: usize,
    budget_regression_count: usize,
    budget_missing_count: usize,
    profile_candidate_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkSpec {
    schema_version: String,
    scale_target: String,
    levels: Vec<BenchmarkSpecLevel>,
    required_levels: Vec<BenchmarkSpecLevel>,
    optional_levels: Vec<BenchmarkSpecLevel>,
    required_profiles: Vec<String>,
    #[serde(default)]
    suites: Vec<BenchmarkSpecSuite>,
    required_suites: Vec<BenchmarkSuite>,
    required_areas: Vec<String>,
    required_requirements: Vec<String>,
    required_representative_benchmarks: Vec<String>,
    required_cli_workflows: Vec<String>,
    suite_coverage: Vec<BenchmarkSuiteCoverage>,
    report_fields: Vec<String>,
    cache_labels: Vec<String>,
    required_commands: Vec<String>,
    readiness_gates: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkSpecLevel {
    level: String,
    target_propositions: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkSpecSuite {
    suite: BenchmarkSuite,
    slug: String,
    scope: String,
    required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkSuiteCoverage {
    suite: BenchmarkSuite,
    requirements: Vec<String>,
    representative_benchmarks: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkBaselineSummary {
    schema_version: String,
    suite: BenchmarkSuite,
    base: PathBuf,
    output: PathBuf,
    generated_at_unix_ms: u128,
    #[serde(default)]
    environment_manifest: Option<EnvironmentMetadata>,
    iterations: usize,
    warmups: usize,
    cache_state: String,
    levels: BTreeMap<String, usize>,
    missing_levels: Vec<String>,
    required_profiles: Vec<String>,
    profile_levels: BTreeMap<String, BTreeSet<String>>,
    missing_profiles: Vec<String>,
    missing_profile_levels: Vec<BenchmarkMissingProfileLevel>,
    ready: bool,
    #[serde(default)]
    failure_log: Option<PathBuf>,
    reports: Vec<BenchmarkBaselineReport>,
    failures: Vec<BenchmarkBaselineFailure>,
    #[serde(default)]
    bottlenecks: Vec<BenchmarkBaselineBottleneck>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkBaselineBottleneck {
    profile: String,
    level: String,
    seed: Option<u64>,
    suite: BenchmarkSuite,
    area: String,
    benchmark: String,
    median_ms: f64,
    p95_ms: f64,
    priority_score: f64,
    reasons: Vec<String>,
    source_report: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkBaselineReport {
    fixture: PathBuf,
    level: String,
    profile: String,
    seed: Option<u64>,
    report: PathBuf,
    benchmark_count: usize,
    #[serde(default)]
    reproduce_command: String,
    #[serde(default)]
    reproduction: Option<BenchmarkReproductionMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkBaselineFailure {
    fixture: PathBuf,
    level: String,
    profile: String,
    seed: Option<u64>,
    error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkFailureLog {
    schema_version: String,
    generated_at_unix_ms: u128,
    baseline_output: PathBuf,
    suite: BenchmarkSuite,
    entry_count: usize,
    entries: Vec<BenchmarkFailureLogEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkFailureLogEntry {
    scope: String,
    profile: String,
    level: String,
    seed: Option<u64>,
    fixture: PathBuf,
    report: Option<PathBuf>,
    benchmark: Option<String>,
    failure_kind: String,
    error: String,
    reproduce_command: String,
    #[serde(default)]
    reproduction: Option<BenchmarkReproductionMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkReproductionMetadata {
    fixture: PathBuf,
    profile: String,
    level: String,
    seed: Option<u64>,
    #[serde(default)]
    simulator_revision: Option<String>,
    #[serde(default)]
    facts_sdk_revision: Option<String>,
    #[serde(default)]
    facts_implementation_revision: Option<String>,
    #[serde(default)]
    environment_manifest: Option<EnvironmentMetadata>,
    benchmark_command: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkMatrixPlan {
    schema_version: String,
    suite: BenchmarkSuite,
    seed: u64,
    fixture_base: PathBuf,
    report_output: PathBuf,
    iterations: usize,
    warmups: usize,
    cache_state: String,
    include_large: bool,
    levels: Vec<BenchmarkLevelTarget>,
    required_profiles: Vec<String>,
    fixtures: Vec<BenchmarkMatrixFixturePlan>,
    #[serde(default)]
    fixture_inventory_command: String,
    baseline_command: String,
    #[serde(default)]
    baseline_commands: Vec<String>,
    #[serde(default)]
    audit_command: String,
    #[serde(default)]
    growth_analysis_command: String,
    #[serde(default)]
    budgets_command: String,
    #[serde(default)]
    budget_check_command: String,
    #[serde(default)]
    profile_plan_command: String,
    #[serde(default)]
    compare_matrix_command: String,
    #[serde(default)]
    acceptance_command: String,
    cleanup_command: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkLevelTarget {
    level: String,
    target_propositions: usize,
    target_objects: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkMatrixFixturePlan {
    profile: String,
    level: String,
    seed: u64,
    target_propositions: usize,
    target_objects: usize,
    #[serde(default)]
    estimated_total_objects: Option<usize>,
    fixture: PathBuf,
    report: PathBuf,
    generation: serde_json::Value,
    benchmark_command: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkDelta {
    benchmark: String,
    baseline_median_ms: f64,
    current_median_ms: f64,
    absolute_difference_ms: f64,
    percentage_difference: f64,
    #[serde(default)]
    noise: BenchmarkNoiseEstimate,
    #[serde(default)]
    warning_threshold_percent: f64,
    #[serde(default)]
    regression_threshold_percent: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct BenchmarkNoiseEstimate {
    supported: bool,
    baseline_samples: usize,
    current_samples: usize,
    baseline_stddev_ms: Option<f64>,
    current_stddev_ms: Option<f64>,
    baseline_cv_percent: Option<f64>,
    current_cv_percent: Option<f64>,
    noise_threshold_percent: Option<f64>,
    difference_exceeds_noise_threshold: Option<bool>,
}

#[derive(Debug, Clone)]
struct BenchmarkThresholds {
    default: BenchmarkThreshold,
    benchmarks: BTreeMap<String, BenchmarkThreshold>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct BenchmarkThreshold {
    warning_percent: f64,
    regression_percent: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkThresholdConfig {
    default: Option<BenchmarkThresholdOverride>,
    #[serde(default)]
    benchmarks: BTreeMap<String, BenchmarkThresholdOverride>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct BenchmarkThresholdOverride {
    warning_percent: Option<f64>,
    regression_percent: Option<f64>,
}

impl BenchmarkThresholds {
    fn from_args(
        warning_percent: f64,
        regression_percent: f64,
        path: Option<&Path>,
    ) -> Result<Self> {
        let default = BenchmarkThreshold::new(warning_percent, regression_percent)?;
        let Some(path) = path else {
            return Ok(Self {
                default,
                benchmarks: BTreeMap::new(),
            });
        };
        let config: BenchmarkThresholdConfig = serde_json::from_slice(
            &std::fs::read(path).with_context(|| format!("failed to read `{}`", path.display()))?,
        )
        .with_context(|| format!("failed to parse `{}`", path.display()))?;
        let default = config
            .default
            .map(|override_| override_.apply(default))
            .transpose()?
            .unwrap_or(default);
        let benchmarks = config
            .benchmarks
            .into_iter()
            .map(|(name, override_)| Ok((name, override_.apply(default)?)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        Ok(Self {
            default,
            benchmarks,
        })
    }

    fn for_benchmark(&self, name: &str) -> BenchmarkThreshold {
        self.benchmarks.get(name).copied().unwrap_or(self.default)
    }
}

impl BenchmarkThreshold {
    fn new(warning_percent: f64, regression_percent: f64) -> Result<Self> {
        if !warning_percent.is_finite() || !regression_percent.is_finite() {
            bail!("benchmark thresholds must be finite percentages");
        }
        if warning_percent < 0.0 || regression_percent < 0.0 {
            bail!("benchmark thresholds cannot be negative");
        }
        if warning_percent > regression_percent {
            bail!(
                "warning threshold {warning_percent} cannot exceed regression threshold {regression_percent}"
            );
        }
        Ok(Self {
            warning_percent,
            regression_percent,
        })
    }
}

impl BenchmarkThresholdOverride {
    fn apply(self, base: BenchmarkThreshold) -> Result<BenchmarkThreshold> {
        BenchmarkThreshold::new(
            self.warning_percent.unwrap_or(base.warning_percent),
            self.regression_percent.unwrap_or(base.regression_percent),
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkFixtureInventory {
    base: PathBuf,
    fixtures: Vec<BenchmarkFixtureEntry>,
    levels: BTreeMap<String, usize>,
    missing_levels: Vec<String>,
    required_profiles: Vec<String>,
    profile_levels: BTreeMap<String, BTreeSet<String>>,
    missing_profiles: Vec<String>,
    missing_profile_levels: Vec<BenchmarkMissingProfileLevel>,
    ready: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct BenchmarkMissingProfileLevel {
    profile: String,
    level: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct BenchmarkMissingCacheProfileLevel {
    cache_temperature: String,
    profile: String,
    level: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkFixtureEntry {
    path: PathBuf,
    profile: String,
    seed: Option<u64>,
    level: String,
    proposition_count: u64,
    total_object_count: u64,
    projected_row_count: u64,
    search_index_row_count: u64,
    search_index_size_bytes: u64,
    database_size_bytes: u64,
}

fn benchmark_fixture_inventory(
    base: &Path,
    include_large: bool,
) -> Result<BenchmarkFixtureInventory> {
    let mut fixtures = Vec::new();
    if base.exists() {
        let mut pending = vec![base.to_path_buf()];
        while let Some(directory) = pending.pop() {
            for entry in std::fs::read_dir(&directory)
                .with_context(|| format!("failed to read fixture base `{}`", directory.display()))?
            {
                let path = entry?.path();
                if !path.is_dir() {
                    continue;
                }
                if path.extension().and_then(|extension| extension.to_str()) == Some("progress") {
                    continue;
                }
                if path.join("manifest.json").is_file() {
                    let metadata = FixtureMetadata::from_fixture(&path)?;
                    fixtures.push(BenchmarkFixtureEntry {
                        path,
                        profile: metadata.profile,
                        seed: metadata.seed,
                        level: fixture_level(metadata.proposition_count).to_string(),
                        proposition_count: metadata.proposition_count,
                        total_object_count: metadata.total_object_count,
                        projected_row_count: metadata.projected_row_count,
                        search_index_row_count: metadata.search_index_row_count,
                        search_index_size_bytes: metadata.search_index_size_bytes,
                        database_size_bytes: metadata.database_size_bytes,
                    });
                    continue;
                }
                pending.push(path);
            }
        }
    }
    fixtures.sort_by(|left, right| left.path.cmp(&right.path));
    let mut levels = BTreeMap::new();
    let mut profile_levels = BTreeMap::<String, BTreeSet<String>>::new();
    for fixture in &fixtures {
        *levels.entry(fixture.level.clone()).or_default() += 1;
        profile_levels
            .entry(fixture.profile.clone())
            .or_default()
            .insert(fixture.level.clone());
    }
    let required_levels = required_benchmark_levels(include_large);
    let missing_levels = required_levels
        .iter()
        .copied()
        .filter(|level| !levels.contains_key(*level))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let required_profiles = REQUIRED_BENCHMARK_PROFILES
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let missing_profiles = REQUIRED_BENCHMARK_PROFILES
        .into_iter()
        .filter(|profile| !profile_levels.contains_key(*profile))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let missing_profile_levels = missing_required_profile_levels(&profile_levels, include_large);
    let ready = missing_levels.is_empty()
        && missing_profiles.is_empty()
        && missing_profile_levels.is_empty();
    Ok(BenchmarkFixtureInventory {
        base: base.to_path_buf(),
        fixtures,
        levels,
        missing_levels,
        required_profiles,
        profile_levels,
        missing_profiles,
        missing_profile_levels,
        ready,
    })
}

fn missing_required_profile_levels(
    profile_levels: &BTreeMap<String, BTreeSet<String>>,
    include_large: bool,
) -> Vec<BenchmarkMissingProfileLevel> {
    let mut missing_profile_levels = Vec::new();
    for profile in REQUIRED_BENCHMARK_PROFILES {
        for level in required_benchmark_levels(include_large) {
            if !profile_levels
                .get(profile)
                .is_some_and(|levels| levels.contains(level))
            {
                missing_profile_levels.push(BenchmarkMissingProfileLevel {
                    profile: profile.to_string(),
                    level: level.to_string(),
                });
            }
        }
    }
    missing_profile_levels
}

fn missing_required_cache_profile_levels(
    cache_profile_levels: &BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
    include_large: bool,
) -> Vec<BenchmarkMissingCacheProfileLevel> {
    let mut missing = Vec::new();
    for cache_temperature in required_cache_temperatures() {
        let profile_levels = cache_profile_levels.get(cache_temperature);
        for profile in REQUIRED_BENCHMARK_PROFILES {
            for level in required_benchmark_levels(include_large) {
                if !profile_levels
                    .and_then(|levels| levels.get(profile))
                    .is_some_and(|levels| levels.contains(level))
                {
                    missing.push(BenchmarkMissingCacheProfileLevel {
                        cache_temperature: cache_temperature.to_string(),
                        profile: profile.to_string(),
                        level: level.to_string(),
                    });
                }
            }
        }
    }
    missing
}

fn fixture_level(propositions: u64) -> &'static str {
    match propositions {
        0..=49_999 => "small",
        50_000..=249_999 => "medium",
        _ => "large",
    }
}

fn benchmark_level_targets(include_large: bool) -> Vec<(&'static str, usize)> {
    let mut targets = vec![("small", 10_000), ("medium", 100_000)];
    if include_large {
        targets.extend(optional_benchmark_level_targets());
    }
    targets
}

fn optional_benchmark_level_targets() -> Vec<(&'static str, usize)> {
    vec![("large", 500_000)]
}

fn required_benchmark_levels(include_large: bool) -> Vec<&'static str> {
    let mut levels = REQUIRED_BENCHMARK_LEVELS.to_vec();
    if include_large {
        levels.extend(OPTIONAL_BENCHMARK_LEVELS);
    }
    levels
}

fn include_large_arg(include_large: bool) -> &'static str {
    if include_large {
        " --include-large"
    } else {
        ""
    }
}

fn suite_slug(suite: BenchmarkSuite) -> &'static str {
    match suite {
        BenchmarkSuite::Core => "core",
        BenchmarkSuite::Read => "read",
        BenchmarkSuite::Search => "search",
        BenchmarkSuite::Sync => "sync",
        BenchmarkSuite::Rebuild => "rebuild",
        BenchmarkSuite::Integrity => "integrity",
        BenchmarkSuite::Cli => "cli",
        BenchmarkSuite::Conflict => "conflict",
        BenchmarkSuite::Http => "http",
        BenchmarkSuite::Full => "full",
    }
}

fn benchmark_spec_suites() -> Vec<BenchmarkSpecSuite> {
    [
        BenchmarkSuite::Core,
        BenchmarkSuite::Read,
        BenchmarkSuite::Search,
        BenchmarkSuite::Sync,
        BenchmarkSuite::Rebuild,
        BenchmarkSuite::Integrity,
        BenchmarkSuite::Cli,
        BenchmarkSuite::Conflict,
        BenchmarkSuite::Http,
        BenchmarkSuite::Full,
    ]
    .into_iter()
    .map(|suite| BenchmarkSpecSuite {
        suite,
        slug: suite_slug(suite).to_string(),
        scope: suite_scope(suite).to_string(),
        required: suite != BenchmarkSuite::Full,
    })
    .collect()
}

fn suite_scope(suite: BenchmarkSuite) -> &'static str {
    match suite {
        BenchmarkSuite::Core => {
            "SDK write scenarios, ledger startup/open metadata, effective lookup variants, canonical object baselines, and acceptance/rejection projection probes"
        }
        BenchmarkSuite::Read => {
            "Listing, pagination, pending-action shapes, revision depth buckets, history/event inspection, and deliberation inspection read paths"
        }
        BenchmarkSuite::Search => {
            "Search-index count, first-page, pagination, lifecycle/effective filters, revision-term shapes, no-result, or payload-search baselines"
        }
        BenchmarkSuite::Sync => {
            "SDK local convergence plus missing-hash negotiation, incremental/medium/large batches, dependency/divergence, duplicate-delivery, no-op peer, full-payload, bundle, and local-import baselines"
        }
        BenchmarkSuite::Rebuild => {
            "SDK projection rebuild scenarios plus disposable projection scan, incremental/batched projection-shape, search-index update-shape, and table-byte baselines"
        }
        BenchmarkSuite::Integrity => {
            "Hash scans, signature-payload scans, SDK validation, dependency/authorization/batch validation shapes, commitment/proof baselines, snapshot sidecar verification, and snapshot inventory"
        }
        BenchmarkSuite::Cli => {
            "Sampled noninteractive fact and fact-sim process workflows plus CLI-shaped read queries"
        }
        BenchmarkSuite::Conflict => {
            "SDK conflict-state scenarios plus sibling-revision, contested-state, decision-conflict, ancestor, and reconciliation inspection queries"
        }
        BenchmarkSuite::Http => {
            "In-process reference fact-http router probes plus fixture-backed fetch, query, synchronization, and payload-shape probes"
        }
        BenchmarkSuite::Full => "All supported suites",
    }
}

fn required_benchmark_suites() -> BTreeSet<BenchmarkSuite> {
    BTreeSet::from([
        BenchmarkSuite::Core,
        BenchmarkSuite::Read,
        BenchmarkSuite::Search,
        BenchmarkSuite::Sync,
        BenchmarkSuite::Rebuild,
        BenchmarkSuite::Integrity,
        BenchmarkSuite::Cli,
        BenchmarkSuite::Conflict,
        BenchmarkSuite::Http,
    ])
}

fn required_benchmark_areas() -> BTreeSet<String> {
    [
        "core",
        "proposition-create",
        "revision-create",
        "accept-reject",
        "read",
        "search",
        "sync",
        "rebuild",
        "integrity",
        "cli",
        "conflict-state",
        "http",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn required_benchmark_requirements() -> BTreeSet<String> {
    [
        "ledger-startup",
        "proposition-creation",
        "revision-creation",
        "acceptance-rejection",
        "effective-lookup",
        "fact-listing",
        "pending-actions",
        "revision-history",
        "deliberation-participant-inspection",
        "search-correctness",
        "history-event-inspection",
        "push-pull-sync",
        "projection-updates",
        "full-state-rebuild",
        "commitments-proofs",
        "snapshot-creation-loading",
        "snapshot-inventory",
        "snapshot-verification",
        "object-validation",
        "conflict-state-computation",
        "sampled-cli-workflows",
        "http-local-loopback",
        "http-object-fetch",
        "http-batch-fetch",
        "http-push",
        "http-pull",
        "http-query",
        "http-commitment-retrieval",
        "http-capability-negotiation",
        "http-authentication-overhead",
        "http-digest-signature-overhead",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn required_cli_workflows() -> BTreeSet<String> {
    [
        "status",
        "list",
        "search",
        "pending",
        "revisions",
        "history",
        "echo",
        "propose",
        "revise",
        "accept",
        "push",
        "pull",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn required_cache_temperatures() -> BTreeSet<&'static str> {
    BTreeSet::from(["cold", "warm"])
}

fn required_representative_benchmark_names() -> BTreeSet<String> {
    required_benchmark_suites()
        .into_iter()
        .filter(|suite| *suite != BenchmarkSuite::Full)
        .flat_map(representative_suite_benchmarks)
        .collect()
}

fn cli_workflows_for_benchmark(benchmark_name: &str) -> BTreeSet<String> {
    let mut workflows = BTreeSet::new();
    for workflow in required_cli_workflows() {
        if benchmark_name.contains(&workflow) {
            workflows.insert(workflow);
        }
    }
    workflows
}

fn benchmark_suite_coverage() -> Vec<BenchmarkSuiteCoverage> {
    required_benchmark_suites()
        .into_iter()
        .map(|suite| {
            let representative_benchmarks = representative_suite_benchmarks(suite);
            let requirements = representative_benchmarks
                .iter()
                .flat_map(|benchmark| requirement_tags_for_benchmark(suite, benchmark, ""))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            BenchmarkSuiteCoverage {
                suite,
                requirements,
                representative_benchmarks,
            }
        })
        .collect()
}

fn representative_suite_benchmarks(suite: BenchmarkSuite) -> Vec<String> {
    let names = match suite {
        BenchmarkSuite::Core => vec![
            "core_sdk_propose_temp",
            "core_sdk_propose_accept_temp",
            "core_sdk_revise_temp",
            "core_sdk_reject_temp",
            "core_ledger_schema_table_inventory",
            "core_projection_metadata_inventory",
            "core_search_index_metadata_inventory",
            "core_first_proposition_lookup",
            "core_content_hash_prefix_lookup_shape",
            "core_effective_lookup_first_page",
            "core_effective_full_id_lookup_shape",
            "core_effective_short_reference_lookup_shape",
            "core_effective_revision_id_lookup_shape",
            "core_effective_pending_status_lookup_shape",
            "core_effective_lifecycle_state_lookup_shape",
            "core_decision_object_page",
            "core_settlement_evidence_page",
            "core_effective_rejected_transition_page",
            "core_consensus_group_size_shape",
            "core_decision_conflict_acceptance_shape",
        ],
        BenchmarkSuite::Read => vec![
            "read_history_ledger_wide_page",
            "read_history_paginated_object_page",
            "read_history_object_type_filter",
            "read_list_default_accepted",
            "read_list_all_states",
            "read_list_offset_page",
            "read_pending_action_count",
            "read_pending_large_page",
            "read_pending_no_work_actor_shape",
            "read_pending_many_actions_shape",
            "read_pending_multi_ledger_shape",
            "read_revision_history_page",
            "read_revision_history_deep_shape",
            "read_revision_parentless_lookup_shape",
            "read_revision_short_chain_shape",
            "read_revision_medium_chain_shape",
            "read_revision_deep_chain_shape",
            "read_history_proposition_scoped_page",
            "read_history_high_activity_proposition",
            "read_deliberation_decision_object_page",
            "read_deliberation_comment_object_page",
            "read_deliberation_settlement_evidence_page",
            "read_deliberation_consensus_page",
            "read_deliberation_large_page",
        ],
        BenchmarkSuite::Search => vec![
            "search_index_count",
            "search_first_page",
            "search_index_large_result_shape",
            "search_index_no_result_shape",
            "search_index_paginated_result_shape",
            "search_lifecycle_status_filter_shape",
            "search_effective_content_filter_shape",
            "search_later_revision_term_shape",
            "search_removed_effective_revision_term_shape",
            "search_payload_common_term",
            "search_payload_no_result",
        ],
        BenchmarkSuite::Sync => vec![
            "sync_sdk_local_convergence_temp",
            "sync_dependency_closure_scan",
            "sync_dependency_batch_shape",
            "sync_divergent_peer_dependency_frontier_shape",
            "sync_duplicate_delivery_shape",
            "sync_initial_full_payload_shape",
            "sync_missing_hash_negotiation_shape",
            "sync_incremental_batch_payload_shape",
            "sync_medium_batch_payload_shape",
            "sync_large_batch_payload_shape",
            "sync_fully_synchronized_peer_noop_shape",
            "sync_bundle_inventory_metadata",
            "sync_local_import_payload_inventory",
        ],
        BenchmarkSuite::Rebuild => vec![
            "rebuild_sdk_projection_rebuild_temp",
            "rebuild_scan_projected_effective",
            "rebuild_effective_state_recalculation_shape",
            "rebuild_pending_projection_update_shape",
            "rebuild_batched_projection_table_inventory",
            "rebuild_projection_table_bytes",
            "rebuild_search_index_table_bytes",
            "rebuild_search_index_update_shape",
        ],
        BenchmarkSuite::Integrity => vec![
            "integrity_hash_enumeration",
            "integrity_validate_protocol_payloads",
            "integrity_invalid_object_rejection_sampled",
            "integrity_dependency_validation_shape",
            "integrity_authorization_validation_shape",
            "integrity_batch_validation_payload_shape",
            "integrity_commitment_create_full_ledger",
            "integrity_commitment_verify_full_ledger",
            "integrity_inclusion_proof_sampled",
            "integrity_non_inclusion_proof_sampled",
            "integrity_snapshot_frame_encode_full_ledger",
            "integrity_snapshot_frame_decode_full_ledger",
            "integrity_snapshot_sidecar_verify",
            "integrity_snapshot_inventory_metadata",
        ],
        BenchmarkSuite::Cli => vec![
            "cli_inspect_process",
            "cli_report_process",
            "cli_list_like_first_page",
            "cli_revisions_like_first_page",
            "cli_fact_accept_process",
            "cli_fact_echo_process",
            "cli_fact_history_process",
            "cli_fact_list_process",
            "cli_fact_pending_process",
            "cli_fact_propose_process",
            "cli_fact_pull_process",
            "cli_fact_push_process",
            "cli_fact_revise_process",
            "cli_fact_revisions_process",
            "cli_fact_search_process",
            "cli_fact_status_process",
        ],
        BenchmarkSuite::Conflict => vec![
            "conflict_sdk_parallel_deliberations_temp",
            "conflict_sibling_revision_detection",
            "conflict_last_undisputed_ancestor_lookup",
            "conflict_effective_rejected_or_contested_page",
            "conflict_contested_state_lookup",
            "conflict_consensus_disagreement_page",
            "conflict_decision_conflict_detection",
            "conflict_reconciliation_object_inspection",
        ],
        BenchmarkSuite::Http => vec![
            "http_local_loopback_ledger_list",
            "http_local_object_fetch_shape",
            "http_local_batch_fetch_shape",
            "http_local_query_shape",
            "http_local_pull_negotiation_shape",
            "http_local_push_payload_shape",
            "http_local_commitment_retrieval_shape",
            "http_local_capability_negotiation_shape",
            "http_local_authentication_overhead_shape",
            "http_local_digest_signature_overhead_shape",
            "http_local_loopback_bundle_payload_inventory",
        ],
        BenchmarkSuite::Full => Vec::new(),
    };
    names.into_iter().map(str::to_string).collect()
}

fn operation_requirement_tags(operation: &BenchmarkOperation) -> Vec<String> {
    requirement_tags_for_benchmark(operation.suite, &operation.name, &operation.area)
}

fn operation_preconditions(operation: &BenchmarkOperation) -> Vec<String> {
    let mut preconditions = Vec::new();
    match &operation.kind {
        BenchmarkOperationKind::Sql { database, sql } => {
            preconditions.push(format!("SQLite database `{}` exists", database.display()));
            preconditions.push(format!("SQL query prepares successfully: {sql}"));
        }
        BenchmarkOperationKind::TableSize { database, tables } => {
            preconditions.push(format!("SQLite database `{}` exists", database.display()));
            preconditions.push(format!(
                "SQLite dbstat can report table bytes for: {}",
                join_or_none(tables.iter().map(String::as_str))
            ));
        }
        BenchmarkOperationKind::FileInventory {
            root, extensions, ..
        } => {
            preconditions.push(format!("Fixture artifact root `{}` exists", root.display()));
            preconditions.push(format!(
                "Artifact extensions are present or intentionally absent: {}",
                join_or_none(extensions.iter().map(String::as_str))
            ));
        }
        BenchmarkOperationKind::Process { program, args } => {
            preconditions.push(format!("Process `{}` is executable", program.display()));
            preconditions.push(format!("Arguments are noninteractive: {}", args.join(" ")));
        }
        BenchmarkOperationKind::CliWorkflow { program, workflow } => {
            preconditions.push(format!("Fact CLI `{}` is executable", program.display()));
            preconditions.push(format!(
                "Sampled `fact {workflow}` workflow runs with isolated FACT_HOME"
            ));
            preconditions.push("Workflow setup uses noninteractive CLI inputs".to_string());
        }
        BenchmarkOperationKind::Scenario { .. } => {
            preconditions.push("Scenario YAML parses successfully".to_string());
            preconditions.push(
                "Scenario assertions are enabled and failed assertions mark correctness false"
                    .to_string(),
            );
        }
        BenchmarkOperationKind::Commitment { database, .. } => {
            preconditions.push(format!(
                "SQLite database `{}` contains protocol_object content hashes",
                database.display()
            ));
            preconditions.push("SDK commitment/proof helpers are available".to_string());
        }
        BenchmarkOperationKind::Validation { database, .. } => {
            preconditions.push(format!(
                "SQLite database `{}` contains protocol_object payloads",
                database.display()
            ));
            preconditions.push("SDK object validation helpers are available".to_string());
        }
        BenchmarkOperationKind::SnapshotSidecarVerify {
            database, snapshot, ..
        } => {
            preconditions.push(format!("Snapshot sidecar `{}` exists", snapshot.display()));
            preconditions.push(format!(
                "SQLite database `{}` contains protocol_object rows",
                database.display()
            ));
        }
        BenchmarkOperationKind::SnapshotFrame { database, .. } => {
            preconditions.push(format!(
                "SQLite database `{}` contains protocol_object COSE rows",
                database.display()
            ));
            preconditions.push("Protocol object COSE hashes match content_hash values".to_string());
            preconditions
                .push("fact-commitment FACTSNAP framing helpers are available".to_string());
        }
        BenchmarkOperationKind::SearchIndex {
            database,
            ledger_id,
            query,
            limit,
        } => {
            preconditions.push(format!(
                "SQLite database `{}` can be opened by fact-store",
                database.display()
            ));
            preconditions.push(format!(
                "Fixture protocol_object rows contain routeable ledger_id `{ledger_id}`"
            ));
            preconditions.push(format!(
                "fact-store markdown search handles query `{query}` with limit {limit}"
            ));
        }
        BenchmarkOperationKind::HttpRouter {
            database,
            method,
            path,
            ..
        } => {
            preconditions.push(format!(
                "SQLite database `{}` can be opened by fact-store",
                database.display()
            ));
            preconditions.push(format!(
                "Reference fact-http router handles noninteractive `{method} {path}`"
            ));
        }
        BenchmarkOperationKind::HttpFixtureRoute { database, route } => {
            preconditions.push(format!(
                "SQLite database `{}` can be opened by fact-store",
                database.display()
            ));
            preconditions.push(format!(
                "Fixture protocol_object rows contain UUID ledger_id and object_id values for `{route:?}`"
            ));
            preconditions.push(
                "Reference fact-http router handles the resolved noninteractive fixture route"
                    .to_string(),
            );
        }
    }
    preconditions
}

fn operation_isolation_strategy(operation: &BenchmarkOperation) -> String {
    match &operation.kind {
        BenchmarkOperationKind::Scenario { .. } => {
            "write benchmark runs the normal SDK-backed scenario in a fresh temporary workspace per sample".to_string()
        }
        BenchmarkOperationKind::Process { .. } => {
            "process benchmark isolates startup/config/loading/output cost in a child process per sample".to_string()
        }
        BenchmarkOperationKind::CliWorkflow { .. } => {
            "CLI workflow benchmark prepares a temporary FACT_HOME per sample and measures one noninteractive fact command".to_string()
        }
        BenchmarkOperationKind::Sql { .. }
        | BenchmarkOperationKind::TableSize { .. }
        | BenchmarkOperationKind::FileInventory { .. }
        | BenchmarkOperationKind::Commitment { .. }
        | BenchmarkOperationKind::Validation { .. }
        | BenchmarkOperationKind::SnapshotSidecarVerify { .. }
        | BenchmarkOperationKind::SnapshotFrame { .. }
        | BenchmarkOperationKind::SearchIndex { .. }
        | BenchmarkOperationKind::HttpRouter { .. }
        | BenchmarkOperationKind::HttpFixtureRoute { .. } => {
            "read-only benchmark opens fixture artifacts without mutating them".to_string()
        }
    }
}

fn requirement_tags_for_benchmark(
    suite: BenchmarkSuite,
    benchmark_name: &str,
    area: &str,
) -> Vec<String> {
    let mut tags = BTreeSet::new();
    match suite {
        BenchmarkSuite::Core => {
            tags.insert("effective-lookup");
            if benchmark_name.contains("ledger_schema")
                || benchmark_name.contains("projection_metadata")
                || benchmark_name.contains("search_index_metadata")
                || area == "ledger-startup"
            {
                tags.insert("ledger-startup");
            }
            if benchmark_name.contains("propose") {
                tags.insert("proposition-creation");
            }
            if benchmark_name.contains("revise") {
                tags.insert("revision-creation");
            }
            if benchmark_name.contains("accept") || benchmark_name.contains("reject") {
                tags.insert("acceptance-rejection");
            }
        }
        BenchmarkSuite::Read => {
            tags.insert("ledger-startup");
            if benchmark_name.contains("list") {
                tags.insert("fact-listing");
                tags.insert("effective-lookup");
            }
            if benchmark_name.contains("pending") {
                tags.insert("pending-actions");
            }
            if benchmark_name.contains("revision") {
                tags.insert("revision-history");
            }
            if benchmark_name.contains("history") || area == "history" {
                tags.insert("history-event-inspection");
            }
            if benchmark_name.contains("deliberation") || benchmark_name.contains("consensus") {
                tags.insert("deliberation-participant-inspection");
            }
        }
        BenchmarkSuite::Search => {
            tags.insert("search-correctness");
        }
        BenchmarkSuite::Sync => {
            tags.insert("push-pull-sync");
        }
        BenchmarkSuite::Rebuild => {
            tags.insert("projection-updates");
            tags.insert("full-state-rebuild");
        }
        BenchmarkSuite::Integrity => {
            if benchmark_name.contains("commitment")
                || benchmark_name.contains("proof")
                || benchmark_name.contains("hash")
            {
                tags.insert("commitments-proofs");
            }
            if benchmark_name.contains("snapshot_sidecar_verify") {
                tags.insert("snapshot-verification");
            }
            if benchmark_name.contains("snapshot") {
                tags.insert("snapshot-creation-loading");
                tags.insert("snapshot-inventory");
            }
            if benchmark_name.contains("validate")
                || benchmark_name.contains("validation")
                || benchmark_name.contains("rejection")
            {
                tags.insert("object-validation");
            }
        }
        BenchmarkSuite::Cli => {
            tags.insert("sampled-cli-workflows");
            tags.insert("ledger-startup");
            if benchmark_name.contains("list") {
                tags.insert("fact-listing");
            }
            if benchmark_name.contains("echo") {
                tags.insert("effective-lookup");
            }
            if benchmark_name.contains("search") {
                tags.insert("search-correctness");
            }
            if benchmark_name.contains("revision") {
                tags.insert("revision-history");
            }
            if benchmark_name.contains("pending") {
                tags.insert("pending-actions");
            }
            if benchmark_name.contains("propose") {
                tags.insert("proposition-creation");
            }
            if benchmark_name.contains("revise") {
                tags.insert("revision-creation");
            }
            if benchmark_name.contains("accept") {
                tags.insert("acceptance-rejection");
            }
            if benchmark_name.contains("push") || benchmark_name.contains("pull") {
                tags.insert("push-pull-sync");
            }
        }
        BenchmarkSuite::Conflict => {
            tags.insert("conflict-state-computation");
            tags.insert("acceptance-rejection");
        }
        BenchmarkSuite::Http => {
            tags.insert("http-local-loopback");
            tags.insert("push-pull-sync");
            if benchmark_name.contains("object_fetch") {
                tags.insert("http-object-fetch");
                tags.insert("effective-lookup");
            }
            if benchmark_name.contains("batch_fetch") {
                tags.insert("http-batch-fetch");
                tags.insert("effective-lookup");
            }
            if benchmark_name.contains("query") {
                tags.insert("http-query");
                tags.insert("effective-lookup");
            }
            if benchmark_name.contains("push") || benchmark_name.contains("payload") {
                tags.insert("http-push");
                tags.insert("push-pull-sync");
            }
            if benchmark_name.contains("pull") {
                tags.insert("http-pull");
                tags.insert("push-pull-sync");
            }
            if benchmark_name.contains("commitment") {
                tags.insert("http-commitment-retrieval");
                tags.insert("commitments-proofs");
            }
            if benchmark_name.contains("capability") {
                tags.insert("http-capability-negotiation");
            }
            if benchmark_name.contains("authentication") {
                tags.insert("http-authentication-overhead");
            }
            if benchmark_name.contains("digest") || benchmark_name.contains("signature") {
                tags.insert("http-digest-signature-overhead");
            }
        }
        BenchmarkSuite::Full => {}
    }
    if area == "sync" {
        tags.insert("push-pull-sync");
    }
    if area == "rebuild" {
        tags.insert("projection-updates");
        tags.insert("full-state-rebuild");
    }
    if benchmark_name.contains("history") {
        tags.insert("history-event-inspection");
    }
    if benchmark_name.contains("dependency") {
        tags.insert("history-event-inspection");
    }
    tags.into_iter().map(str::to_string).collect()
}

fn is_known_cache_label(label: &str) -> bool {
    cache_classification(label).is_some()
}

fn sanitize_report_component(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string();
    if sanitized.is_empty() {
        "fixture".to_string()
    } else {
        sanitized
    }
}

fn fact_cli_binary() -> Option<PathBuf> {
    let configured = std::env::var_os("FACT_BINARY")
        .map(PathBuf::from)
        .unwrap_or_else(default_fact_binary_path);
    configured.is_file().then_some(configured)
}

fn default_fact_binary_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .join("cli")
        .join("target")
        .join("debug")
        .join("fact")
}

fn compare_reports(args: &BenchmarkCompareArgs) -> Result<BenchmarkComparison> {
    let baseline = read_report(&args.baseline)?;
    let current = read_report(&args.current)?;
    let thresholds = BenchmarkThresholds::from_args(
        args.warning_threshold_percent,
        args.regression_threshold_percent,
        args.thresholds.as_deref(),
    )?;
    compare_loaded_reports(
        args.baseline.clone(),
        args.current.clone(),
        &baseline,
        &current,
        &thresholds,
    )
}

fn compare_matrix_reports(args: &BenchmarkCompareMatrixArgs) -> Result<BenchmarkMatrixComparison> {
    let baseline = read_baseline_summary(&args.baseline_summary)?;
    let current = read_baseline_summary(&args.current_summary)?;
    let thresholds = BenchmarkThresholds::from_args(
        args.warning_threshold_percent,
        args.regression_threshold_percent,
        args.thresholds.as_deref(),
    )?;
    let baseline_reports = baseline_report_map(&baseline);
    let current_reports = baseline_report_map(&current);
    let mut compared_reports = Vec::new();
    let mut failures = Vec::new();
    let mut missing_in_current = Vec::new();

    for (key, baseline_report) in &baseline_reports {
        let Some(current_report) = current_reports.get(key) else {
            missing_in_current.push(key.clone());
            continue;
        };
        match compare_report_paths(&baseline_report.report, &current_report.report, &thresholds) {
            Ok(comparison) => compared_reports.push(BenchmarkMatrixReportComparison {
                key: key.clone(),
                profile: baseline_report.profile.clone(),
                level: baseline_report.level.clone(),
                seed: baseline_report.seed,
                baseline_report: baseline_report.report.clone(),
                current_report: current_report.report.clone(),
                environment_compatible: comparison.environment_compatible,
                environment_difference_count: comparison.environment_differences.len(),
                fixture_compatible: comparison.fixture_compatible,
                fixture_difference_count: comparison.fixture_differences.len(),
                cache_compatible: comparison.cache_compatible,
                cache_difference_count: comparison.cache_differences.len(),
                regression_count: comparison.regressions.len(),
                warning_count: comparison.warnings.len(),
                improvement_count: comparison.improvements.len(),
                missing_benchmark_count: comparison.missing_in_current.len(),
                new_benchmark_count: comparison.new_in_current.len(),
                incorrect_current_count: comparison.incorrect_current_benchmarks.len(),
                regression_decision_ready: comparison.regression_decision_ready,
                regression_decision_blockers: comparison.regression_decision_blockers.clone(),
                comparison,
            }),
            Err(error) => failures.push(BenchmarkMatrixComparisonFailure {
                key: key.clone(),
                baseline_report: Some(baseline_report.report.clone()),
                current_report: Some(current_report.report.clone()),
                error: format!("{error:#}"),
            }),
        }
    }

    let new_in_current = current_reports
        .keys()
        .filter(|key| !baseline_reports.contains_key(*key))
        .cloned()
        .collect::<Vec<_>>();
    let total_regressions = compared_reports
        .iter()
        .map(|comparison| comparison.regression_count)
        .sum();
    let total_warnings = compared_reports
        .iter()
        .map(|comparison| comparison.warning_count)
        .sum();
    let total_improvements = compared_reports
        .iter()
        .map(|comparison| comparison.improvement_count)
        .sum();
    let total_environment_incompatible_reports = compared_reports
        .iter()
        .filter(|comparison| !comparison.environment_compatible)
        .count();
    let total_environment_differences = compared_reports
        .iter()
        .map(|comparison| comparison.environment_difference_count)
        .sum();
    let total_cache_incompatible_reports = compared_reports
        .iter()
        .filter(|comparison| !comparison.cache_compatible)
        .count();
    let total_fixture_incompatible_reports = compared_reports
        .iter()
        .filter(|comparison| !comparison.fixture_compatible)
        .count();
    let total_fixture_differences = compared_reports
        .iter()
        .map(|comparison| comparison.fixture_difference_count)
        .sum();
    let total_cache_differences = compared_reports
        .iter()
        .map(|comparison| comparison.cache_difference_count)
        .sum();
    let total_incorrect_current_benchmarks = compared_reports
        .iter()
        .map(|comparison| comparison.incorrect_current_count)
        .sum();
    let regression_decision_blockers =
        matrix_regression_decision_blockers(MatrixRegressionDecisionInputs {
            baseline_summary_ready: baseline.ready,
            current_summary_ready: current.ready,
            baseline_failure_count: baseline.failures.len(),
            current_failure_count: current.failures.len(),
            compared_reports: &compared_reports,
            missing_in_current: &missing_in_current,
            new_in_current: &new_in_current,
            failures: &failures,
        });
    let regression_decision_ready = regression_decision_blockers.is_empty();

    Ok(BenchmarkMatrixComparison {
        schema_version: REPORT_SCHEMA_VERSION.to_string(),
        baseline_summary: args.baseline_summary.clone(),
        current_summary: args.current_summary.clone(),
        warning_threshold_percent: thresholds.default.warning_percent,
        regression_threshold_percent: thresholds.default.regression_percent,
        baseline_summary_ready: baseline.ready,
        current_summary_ready: current.ready,
        baseline_failure_count: baseline.failures.len(),
        current_failure_count: current.failures.len(),
        compared_reports,
        missing_in_current,
        new_in_current,
        failures,
        total_regressions,
        total_warnings,
        total_improvements,
        total_environment_incompatible_reports,
        total_environment_differences,
        total_cache_incompatible_reports,
        total_fixture_incompatible_reports,
        total_fixture_differences,
        total_cache_differences,
        total_incorrect_current_benchmarks,
        regression_decision_ready,
        regression_decision_blockers,
    })
}

fn compare_report_paths(
    baseline: &Path,
    current: &Path,
    thresholds: &BenchmarkThresholds,
) -> Result<BenchmarkComparison> {
    let baseline_report = read_report(baseline)?;
    let current_report = read_report(current)?;
    compare_loaded_reports(
        baseline.to_path_buf(),
        current.to_path_buf(),
        &baseline_report,
        &current_report,
        thresholds,
    )
}

fn compare_loaded_reports(
    baseline_path: PathBuf,
    current_path: PathBuf,
    baseline: &BenchmarkRunReport,
    current: &BenchmarkRunReport,
    thresholds: &BenchmarkThresholds,
) -> Result<BenchmarkComparison> {
    let baseline_by_name = benchmark_map(baseline);
    let current_by_name = benchmark_map(current);
    let mut regressions = Vec::new();
    let mut warnings = Vec::new();
    let mut improvements = Vec::new();
    let mut informational = Vec::new();
    let mut missing_in_current = Vec::new();
    let mut incorrect_baseline_benchmarks = Vec::new();
    let mut incorrect_current_benchmarks = Vec::new();
    let environment_differences =
        environment_differences(&baseline.environment, &current.environment);
    let fixture_differences = fixture_differences(&baseline.fixture, &current.fixture);
    let mut cache_differences = Vec::new();

    for (name, baseline_benchmark) in &baseline_by_name {
        let Some(current_benchmark) = current_by_name.get(name) else {
            missing_in_current.push(name.clone());
            continue;
        };
        if baseline_benchmark.cache_classification != current_benchmark.cache_classification {
            cache_differences.push(BenchmarkCacheDifference {
                benchmark: name.clone(),
                baseline_cache_state: baseline_benchmark.cache_state.clone(),
                current_cache_state: current_benchmark.cache_state.clone(),
                baseline_classification: baseline_benchmark.cache_classification.clone(),
                current_classification: current_benchmark.cache_classification.clone(),
            });
        }
        if !baseline_benchmark.correctness_passed || !baseline_benchmark.failures.is_empty() {
            incorrect_baseline_benchmarks.push(BenchmarkComparisonCorrectnessIssue {
                benchmark: name.clone(),
                correctness_passed: baseline_benchmark.correctness_passed,
                failures: baseline_benchmark.failures.clone(),
            });
        }
        if !current_benchmark.correctness_passed || !current_benchmark.failures.is_empty() {
            incorrect_current_benchmarks.push(BenchmarkComparisonCorrectnessIssue {
                benchmark: name.clone(),
                correctness_passed: current_benchmark.correctness_passed,
                failures: current_benchmark.failures.clone(),
            });
            continue;
        }
        let Some(baseline_median) = baseline_benchmark.stats.median_ms else {
            continue;
        };
        let Some(current_median) = current_benchmark.stats.median_ms else {
            continue;
        };
        if baseline_median == 0.0 {
            continue;
        }
        let absolute = current_median - baseline_median;
        let percentage = absolute / baseline_median * 100.0;
        let threshold = thresholds.for_benchmark(name);
        let noise =
            benchmark_noise_estimate(baseline_benchmark, current_benchmark, percentage.abs());
        let delta = BenchmarkDelta {
            benchmark: name.clone(),
            baseline_median_ms: baseline_median,
            current_median_ms: current_median,
            absolute_difference_ms: absolute,
            percentage_difference: percentage,
            noise,
            warning_threshold_percent: threshold.warning_percent,
            regression_threshold_percent: threshold.regression_percent,
        };
        if percentage >= threshold.regression_percent {
            regressions.push(delta);
        } else if percentage >= threshold.warning_percent {
            warnings.push(delta);
        } else if percentage <= -threshold.warning_percent {
            improvements.push(delta);
        } else {
            informational.push(delta);
        }
    }

    let new_in_current = current_by_name
        .keys()
        .filter(|name| !baseline_by_name.contains_key(*name))
        .cloned()
        .collect::<Vec<_>>();
    let regression_decision_blockers = regression_decision_blockers(
        &environment_differences,
        &fixture_differences,
        &cache_differences,
        &missing_in_current,
        &new_in_current,
        &incorrect_baseline_benchmarks,
        &incorrect_current_benchmarks,
    );
    let regression_decision_ready = regression_decision_blockers.is_empty();

    Ok(BenchmarkComparison {
        schema_version: REPORT_SCHEMA_VERSION.to_string(),
        baseline: baseline_path,
        current: current_path,
        warning_threshold_percent: thresholds.default.warning_percent,
        regression_threshold_percent: thresholds.default.regression_percent,
        environment_compatible: environment_differences.is_empty(),
        environment_differences,
        fixture_compatible: fixture_differences.is_empty(),
        fixture_differences,
        cache_compatible: cache_differences.is_empty(),
        cache_differences,
        regressions,
        warnings,
        improvements,
        informational,
        missing_in_current,
        new_in_current,
        incorrect_baseline_benchmarks,
        incorrect_current_benchmarks,
        regression_decision_ready,
        regression_decision_blockers,
    })
}

fn regression_decision_blockers(
    environment_differences: &[EnvironmentDifference],
    fixture_differences: &[BenchmarkFixtureDifference],
    cache_differences: &[BenchmarkCacheDifference],
    missing_in_current: &[String],
    new_in_current: &[String],
    incorrect_baseline_benchmarks: &[BenchmarkComparisonCorrectnessIssue],
    incorrect_current_benchmarks: &[BenchmarkComparisonCorrectnessIssue],
) -> Vec<String> {
    let mut blockers = Vec::new();
    if !environment_differences.is_empty() {
        blockers.push("environment-differences".to_string());
    }
    if !fixture_differences.is_empty() {
        blockers.push("fixture-differences".to_string());
    }
    if !cache_differences.is_empty() {
        blockers.push("cache-differences".to_string());
    }
    if !missing_in_current.is_empty() {
        blockers.push("missing-benchmarks".to_string());
    }
    if !new_in_current.is_empty() {
        blockers.push("new-benchmarks".to_string());
    }
    if !incorrect_baseline_benchmarks.is_empty() {
        blockers.push("incorrect-baseline-benchmarks".to_string());
    }
    if !incorrect_current_benchmarks.is_empty() {
        blockers.push("incorrect-current-benchmarks".to_string());
    }
    blockers
}

struct MatrixRegressionDecisionInputs<'a> {
    baseline_summary_ready: bool,
    current_summary_ready: bool,
    baseline_failure_count: usize,
    current_failure_count: usize,
    compared_reports: &'a [BenchmarkMatrixReportComparison],
    missing_in_current: &'a [String],
    new_in_current: &'a [String],
    failures: &'a [BenchmarkMatrixComparisonFailure],
}

fn matrix_regression_decision_blockers(inputs: MatrixRegressionDecisionInputs<'_>) -> Vec<String> {
    let mut blockers = Vec::new();
    if !inputs.baseline_summary_ready {
        blockers.push("baseline-summary-not-ready".to_string());
    }
    if !inputs.current_summary_ready {
        blockers.push("current-summary-not-ready".to_string());
    }
    if inputs.baseline_failure_count > 0 {
        blockers.push("baseline-summary-failures".to_string());
    }
    if inputs.current_failure_count > 0 {
        blockers.push("current-summary-failures".to_string());
    }
    if !inputs.missing_in_current.is_empty() {
        blockers.push("missing-report-pairs".to_string());
    }
    if !inputs.new_in_current.is_empty() {
        blockers.push("new-report-pairs".to_string());
    }
    if !inputs.failures.is_empty() {
        blockers.push("comparison-failures".to_string());
    }
    if inputs
        .compared_reports
        .iter()
        .any(|comparison| !comparison.regression_decision_ready)
    {
        blockers.push("report-comparison-blockers".to_string());
    }
    blockers
}

fn benchmark_noise_estimate(
    baseline: &BenchmarkResult,
    current: &BenchmarkResult,
    absolute_difference_percent: f64,
) -> BenchmarkNoiseEstimate {
    let baseline_samples = baseline.samples_ms.len();
    let current_samples = current.samples_ms.len();
    let baseline_stddev = sample_stddev(&baseline.samples_ms);
    let current_stddev = sample_stddev(&current.samples_ms);
    let baseline_cv = coefficient_of_variation_percent(baseline_stddev, baseline.stats.median_ms);
    let current_cv = coefficient_of_variation_percent(current_stddev, current.stats.median_ms);
    let noise_threshold = baseline_cv
        .zip(current_cv)
        .map(|(baseline_cv, current_cv)| (baseline_cv.powi(2) + current_cv.powi(2)).sqrt());

    BenchmarkNoiseEstimate {
        supported: noise_threshold.is_some(),
        baseline_samples,
        current_samples,
        baseline_stddev_ms: baseline_stddev,
        current_stddev_ms: current_stddev,
        baseline_cv_percent: baseline_cv,
        current_cv_percent: current_cv,
        noise_threshold_percent: noise_threshold,
        difference_exceeds_noise_threshold: noise_threshold
            .map(|threshold| absolute_difference_percent >= threshold),
    }
}

fn sample_stddev(samples: &[f64]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let mean = samples.iter().sum::<f64>() / samples.len() as f64;
    let variance = samples
        .iter()
        .map(|sample| (sample - mean).powi(2))
        .sum::<f64>()
        / (samples.len() - 1) as f64;
    Some(variance.sqrt())
}

fn coefficient_of_variation_percent(stddev: Option<f64>, median: Option<f64>) -> Option<f64> {
    let median = median?;
    if median == 0.0 {
        return None;
    }
    Some(stddev? / median.abs() * 100.0)
}

fn baseline_report_map(
    summary: &BenchmarkBaselineSummary,
) -> BTreeMap<String, &BenchmarkBaselineReport> {
    summary
        .reports
        .iter()
        .map(|report| (baseline_report_key(report), report))
        .collect()
}

fn benchmark_budget_map(budgets: &BenchmarkBudgets) -> BTreeMap<String, &BenchmarkBudgetEntry> {
    budgets
        .budgets
        .iter()
        .map(|budget| {
            (
                benchmark_budget_key(
                    &budget.profile,
                    &budget.level,
                    budget.seed,
                    &budget.benchmark,
                ),
                budget,
            )
        })
        .collect()
}

fn baseline_report_key(report: &BenchmarkBaselineReport) -> String {
    format!(
        "{}:{}:{}",
        report.profile,
        report.level,
        report
            .seed
            .map(|seed| seed.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    )
}

fn benchmark_budget_key(profile: &str, level: &str, seed: Option<u64>, benchmark: &str) -> String {
    format!(
        "{}:{}:{}:{}",
        profile,
        level,
        seed.map(|seed| seed.to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        benchmark
    )
}

fn benchmark_map(report: &BenchmarkRunReport) -> BTreeMap<String, &BenchmarkResult> {
    report
        .benchmarks
        .iter()
        .map(|benchmark| (benchmark.name.clone(), benchmark))
        .collect()
}

fn render_report_file(path: &Path) -> Result<String> {
    let value = read_json_file(path)?;
    if value.get("accepted").is_some() && value.get("blockers").is_some() {
        let acceptance = serde_json::from_value(value).with_context(|| {
            format!(
                "failed to parse `{}` as benchmark acceptance report",
                path.display()
            )
        })?;
        return Ok(render_human_acceptance_report(&acceptance));
    }
    if value.get("compared_reports").is_some() {
        let comparison = serde_json::from_value(value).with_context(|| {
            format!(
                "failed to parse `{}` as benchmark matrix comparison",
                path.display()
            )
        })?;
        return Ok(render_human_matrix_comparison(&comparison));
    }
    if value.get("regressions").is_some()
        && value.get("warnings").is_some()
        && value.get("improvements").is_some()
    {
        let comparison = serde_json::from_value(value).with_context(|| {
            format!(
                "failed to parse `{}` as benchmark comparison",
                path.display()
            )
        })?;
        return Ok(render_human_comparison(&comparison));
    }
    if value.get("benchmarks").is_some() {
        let report = serde_json::from_value(value).with_context(|| {
            format!(
                "failed to parse `{}` as benchmark run report",
                path.display()
            )
        })?;
        return Ok(render_human_report(&report));
    }
    if value.get("reports").is_some() {
        let summary = serde_json::from_value(value).with_context(|| {
            format!(
                "failed to parse `{}` as benchmark baseline summary",
                path.display()
            )
        })?;
        return Ok(render_human_baseline_summary(&summary));
    }
    if value.get("entries").is_some() && value.get("entry_count").is_some() {
        let log = serde_json::from_value(value).with_context(|| {
            format!(
                "failed to parse `{}` as benchmark failure log",
                path.display()
            )
        })?;
        return Ok(render_human_failure_log(&log));
    }
    if value.get("fixture_matrix_ready").is_some() {
        let audit = serde_json::from_value(value).with_context(|| {
            format!(
                "failed to parse `{}` as benchmark readiness audit",
                path.display()
            )
        })?;
        return Ok(render_human_audit_report(&audit));
    }
    if value.get("fixture_base").is_some() && value.get("fixtures").is_some() {
        let plan = serde_json::from_value(value).with_context(|| {
            format!(
                "failed to parse `{}` as benchmark matrix plan",
                path.display()
            )
        })?;
        return Ok(render_human_matrix_plan(&plan));
    }
    if value.get("base").is_some()
        && value.get("fixtures").is_some()
        && value.get("missing_profile_levels").is_some()
    {
        let inventory = serde_json::from_value(value).with_context(|| {
            format!(
                "failed to parse `{}` as benchmark fixture inventory",
                path.display()
            )
        })?;
        return Ok(render_human_fixture_inventory(&inventory));
    }
    if value.get("trends").is_some() && value.get("trend_count").is_some() {
        let analysis = serde_json::from_value(value).with_context(|| {
            format!(
                "failed to parse `{}` as benchmark growth analysis",
                path.display()
            )
        })?;
        return Ok(render_human_growth_analysis(&analysis));
    }
    if value.get("budgets").is_some() && value.get("entry_count").is_some() {
        let budgets = serde_json::from_value(value).with_context(|| {
            format!(
                "failed to parse `{}` as benchmark performance budgets",
                path.display()
            )
        })?;
        return Ok(render_human_budgets(&budgets));
    }
    if value.get("results").is_some() && value.get("passed").is_some() {
        let check = serde_json::from_value(value).with_context(|| {
            format!(
                "failed to parse `{}` as benchmark budget check",
                path.display()
            )
        })?;
        return Ok(render_human_budget_check(&check));
    }
    if value.get("candidates").is_some() && value.get("candidate_count").is_some() {
        let plan = serde_json::from_value(value).with_context(|| {
            format!(
                "failed to parse `{}` as benchmark profile plan",
                path.display()
            )
        })?;
        return Ok(render_human_profile_plan(&plan));
    }
    bail!(
        "`{}` is not a recognized benchmark report, baseline summary, fixture inventory, matrix plan, readiness audit, acceptance report, comparison, growth analysis, budget file, or profile plan",
        path.display()
    )
}

fn render_human_matrix_plan(plan: &BenchmarkMatrixPlan) -> String {
    let mut output = String::new();
    output.push_str("# Benchmark Matrix Plan\n\n");
    output.push_str(&format!("- Suite: `{}`\n", suite_slug(plan.suite)));
    output.push_str(&format!("- Seed: `{}`\n", plan.seed));
    output.push_str(&format!(
        "- Fixture base: `{}`\n",
        plan.fixture_base.display()
    ));
    output.push_str(&format!(
        "- Report output: `{}`\n",
        plan.report_output.display()
    ));
    output.push_str(&format!("- Iterations: `{}`\n", plan.iterations));
    output.push_str(&format!("- Warmups: `{}`\n", plan.warmups));
    output.push_str(&format!("- Cache state: `{}`\n", plan.cache_state));
    output.push_str(&format!(
        "- Required profiles: `{}`\n",
        plan.required_profiles.len()
    ));
    output.push_str(&format!(
        "- Planned fixtures: `{}`\n\n",
        plan.fixtures.len()
    ));

    output.push_str("## Levels\n\n");
    for level in &plan.levels {
        output.push_str(&format!(
            "- `{}`: `{}` target propositions\n",
            level.level, level.target_propositions
        ));
    }
    output.push('\n');

    output.push_str("## Commands\n\n");
    output.push_str(&format!(
        "- Fixture inventory: `{}`\n",
        plan.fixture_inventory_command
    ));
    output.push_str(&format!("- Baseline: `{}`\n", plan.baseline_command));
    for command in &plan.baseline_commands {
        output.push_str(&format!("- Baseline matrix: `{command}`\n"));
    }
    output.push_str(&format!("- Audit: `{}`\n", plan.audit_command));
    output.push_str(&format!(
        "- Growth analysis: `{}`\n",
        plan.growth_analysis_command
    ));
    output.push_str(&format!("- Budgets: `{}`\n", plan.budgets_command));
    output.push_str(&format!(
        "- Budget check: `{}`\n",
        plan.budget_check_command
    ));
    output.push_str(&format!(
        "- Profile plan: `{}`\n",
        plan.profile_plan_command
    ));
    output.push_str(&format!(
        "- Matrix comparison: `{}`\n",
        plan.compare_matrix_command
    ));
    output.push_str(&format!("- Acceptance: `{}`\n", plan.acceptance_command));
    output.push_str(&format!("- Cleanup: `{}`\n\n", plan.cleanup_command));

    output.push_str("## Fixture Matrix\n\n");
    for fixture in &plan.fixtures {
        let estimated = fixture
            .estimated_total_objects
            .map_or("unknown".to_string(), |count| count.to_string());
        output.push_str(&format!(
            "- `{}` / `{}`: `{}` target propositions, `{}` estimated total objects, fixture `{}`\n",
            fixture.profile,
            fixture.level,
            fixture.target_propositions,
            estimated,
            fixture.fixture.display()
        ));
    }
    output
}

fn render_human_fixture_inventory(inventory: &BenchmarkFixtureInventory) -> String {
    let mut output = String::new();
    output.push_str("# Benchmark Fixture Inventory\n\n");
    output.push_str(&format!("- Base: `{}`\n", inventory.base.display()));
    output.push_str(&format!("- Ready: `{}`\n", inventory.ready));
    output.push_str(&format!("- Fixtures: `{}`\n", inventory.fixtures.len()));
    output.push_str(&format!(
        "- Missing levels: `{}`\n",
        join_or_none(inventory.missing_levels.iter().map(String::as_str))
    ));
    output.push_str(&format!(
        "- Missing profiles: `{}`\n",
        join_or_none(inventory.missing_profiles.iter().map(String::as_str))
    ));
    output.push_str(&format!(
        "- Missing profile levels: `{}`\n\n",
        inventory.missing_profile_levels.len()
    ));

    if !inventory.levels.is_empty() {
        output.push_str("## Levels\n\n");
        for (level, count) in &inventory.levels {
            output.push_str(&format!("- `{level}`: `{count}` fixtures\n"));
        }
        output.push('\n');
    }
    if !inventory.missing_profile_levels.is_empty() {
        output.push_str("## Missing Profile Levels\n\n");
        for missing in &inventory.missing_profile_levels {
            output.push_str(&format!("- `{}` / `{}`\n", missing.profile, missing.level));
        }
        output.push('\n');
    }
    if !inventory.fixtures.is_empty() {
        output.push_str("## Fixtures\n\n");
        for fixture in &inventory.fixtures {
            output.push_str(&format!(
                "- `{}` / `{}` / seed `{}`: `{}` propositions, `{}` objects, database `{}` bytes, path `{}`\n",
                fixture.profile,
                fixture.level,
                fixture
                    .seed
                    .map_or("unknown".to_string(), |seed| seed.to_string()),
                fixture.proposition_count,
                fixture.total_object_count,
                fixture.database_size_bytes,
                fixture.path.display()
            ));
        }
    }
    output
}

fn render_human_report(report: &BenchmarkRunReport) -> String {
    let mut output = String::new();
    output.push_str(&format!("# Benchmark Report: {:?}\n\n", report.suite));
    output.push_str(&format!("- Fixture: `{}`\n", report.fixture.path.display()));
    output.push_str(&format!("- Profile: `{}`\n", report.fixture.profile));
    output.push_str(&format!(
        "- Seed: `{}`\n",
        report
            .fixture
            .seed
            .map_or("unknown".to_string(), |seed| seed.to_string())
    ));
    output.push_str(&format!(
        "- Propositions: `{}`\n",
        report.fixture.proposition_count
    ));
    output.push_str(&format!(
        "- Objects: `{}`\n",
        report.fixture.total_object_count
    ));
    output.push_str(&format!(
        "- Projected rows: `{}`\n",
        report.fixture.projected_row_count
    ));
    output.push_str(&format!(
        "- Search rows: `{}`\n",
        report.fixture.search_index_row_count
    ));
    output.push_str(&format!(
        "- Search index bytes: `{}`\n",
        report.fixture.search_index_size_bytes
    ));
    output.push_str(&format!(
        "- Database bytes: `{}`\n\n",
        report.fixture.database_size_bytes
    ));
    output.push_str("## Source Revisions\n\n");
    output.push_str(&format!(
        "- Simulator revision: `{}`\n",
        report
            .fixture
            .simulator_revision
            .as_deref()
            .unwrap_or("unknown")
    ));
    output.push_str(&format!(
        "- Facts SDK revision: `{}`\n",
        report
            .fixture
            .facts_sdk_revision
            .as_deref()
            .unwrap_or("unknown")
    ));
    output.push_str(&format!(
        "- Facts implementation revision: `{}`\n\n",
        report
            .fixture
            .facts_implementation_revision
            .as_deref()
            .unwrap_or("unknown")
    ));
    let mut slowest = report
        .benchmarks
        .iter()
        .filter_map(|benchmark| {
            benchmark
                .stats
                .p95_ms
                .map(|p95| (p95, benchmark.name.as_str()))
        })
        .collect::<Vec<_>>();
    slowest.sort_by(|left, right| right.0.total_cmp(&left.0));
    if !slowest.is_empty() {
        output.push_str("## Slowest Benchmarks\n\n");
        output.push_str("| Benchmark | P95 ms |\n");
        output.push_str("| --- | ---: |\n");
        for (p95, name) in slowest.into_iter().take(5) {
            output.push_str(&format!("| `{name}` | {} |\n", fmt_ms(Some(p95))));
        }
        output.push('\n');
    }
    output.push_str(
        "| Benchmark | Samples | Median ms | P95 ms | Outliers | Rows | Bytes | Network bytes | Correct | Phases | Diagnostics |\n",
    );
    output.push_str("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |\n");
    for benchmark in &report.benchmarks {
        output.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            benchmark.name,
            benchmark.stats.samples,
            fmt_ms(benchmark.stats.median_ms),
            fmt_ms(benchmark.stats.p95_ms),
            benchmark.stats.outlier_count,
            benchmark.rows_returned,
            benchmark
                .measured_bytes
                .map_or("-".to_string(), |bytes| bytes.to_string()),
            benchmark
                .network_payload_bytes
                .map_or("-".to_string(), |bytes| bytes.to_string()),
            benchmark.correctness_passed,
            render_phase_summary(&benchmark.phase_breakdown),
            render_diagnostics_summary(&benchmark.diagnostics)
        ));
    }
    output
}

fn render_human_baseline_summary(summary: &BenchmarkBaselineSummary) -> String {
    let mut output = String::new();
    output.push_str(&format!(
        "# Benchmark Baseline Summary: {:?}\n\n",
        summary.suite
    ));
    output.push_str(&format!("- Base: `{}`\n", summary.base.display()));
    output.push_str(&format!("- Output: `{}`\n", summary.output.display()));
    output.push_str(&format!("- Iterations: `{}`\n", summary.iterations));
    output.push_str(&format!("- Warmups: `{}`\n", summary.warmups));
    output.push_str(&format!("- Cache state: `{}`\n", summary.cache_state));
    output.push_str(&format!("- Ready: `{}`\n", summary.ready));
    output.push_str(&format!("- Reports: `{}`\n", summary.reports.len()));
    output.push_str(&format!("- Failures: `{}`\n\n", summary.failures.len()));
    if let Some(failure_log) = &summary.failure_log {
        output.push_str(&format!("- Failure log: `{}`\n\n", failure_log.display()));
    }

    if !summary.bottlenecks.is_empty() {
        output.push_str("## Bottlenecks\n\n");
        output
            .push_str("| Profile | Level | Benchmark | p95 ms | Median ms | Reasons | Report |\n");
        output.push_str("| --- | --- | --- | ---: | ---: | --- | --- |\n");
        for bottleneck in &summary.bottlenecks {
            output.push_str(&format!(
                "| `{}` | `{}` | `{}` | {:.3} | {:.3} | `{}` | `{}` |\n",
                bottleneck.profile,
                bottleneck.level,
                bottleneck.benchmark,
                bottleneck.p95_ms,
                bottleneck.median_ms,
                join_or_none(bottleneck.reasons.iter().map(String::as_str)),
                bottleneck.source_report.display()
            ));
        }
        output.push('\n');
    }

    if !summary.missing_levels.is_empty()
        || !summary.missing_profiles.is_empty()
        || !summary.missing_profile_levels.is_empty()
    {
        output.push_str("## Missing Coverage\n\n");
        output.push_str(&format!(
            "- Missing levels: `{}`\n",
            summary.missing_levels.join(", ")
        ));
        output.push_str(&format!(
            "- Missing profiles: `{}`\n",
            summary.missing_profiles.join(", ")
        ));
        output.push_str(&format!(
            "- Missing profile levels: `{}`\n\n",
            summary.missing_profile_levels.len()
        ));
    }

    if !summary.reports.is_empty() {
        output.push_str("## Reports\n\n");
        output.push_str("| Profile | Level | Seed | Benchmarks | Report | Reproduce |\n");
        output.push_str("| --- | --- | ---: | ---: | --- | --- |\n");
        for report in &summary.reports {
            output.push_str(&format!(
                "| `{}` | `{}` | {} | {} | `{}` | `{}` |\n",
                report.profile,
                report.level,
                report
                    .seed
                    .map_or("unknown".to_string(), |seed| seed.to_string()),
                report.benchmark_count,
                report.report.display(),
                report
                    .reproduction
                    .as_ref()
                    .map(|reproduction| reproduction.benchmark_command.as_str())
                    .unwrap_or(report.reproduce_command.as_str())
                    .replace('|', "\\|")
            ));
        }
        output.push('\n');
    }

    if !summary.failures.is_empty() {
        output.push_str("## Failures\n\n");
        output.push_str("| Profile | Level | Seed | Error |\n");
        output.push_str("| --- | --- | ---: | --- |\n");
        for failure in &summary.failures {
            output.push_str(&format!(
                "| `{}` | `{}` | {} | `{}` |\n",
                failure.profile,
                failure.level,
                failure
                    .seed
                    .map_or("unknown".to_string(), |seed| seed.to_string()),
                failure.error.replace('|', "\\|")
            ));
        }
        output.push('\n');
    }

    output
}

fn render_human_failure_log(log: &BenchmarkFailureLog) -> String {
    let mut output = String::new();
    output.push_str("# Benchmark Failure Log\n\n");
    output.push_str(&format!("- Suite: `{:?}`\n", log.suite));
    output.push_str(&format!(
        "- Baseline output: `{}`\n",
        log.baseline_output.display()
    ));
    output.push_str(&format!("- Entries: `{}`\n\n", log.entry_count));
    if !log.entries.is_empty() {
        output.push_str(
            "| Scope | Profile | Level | Seed | Benchmark | Kind | Error | Reproduce |\n",
        );
        output.push_str("| --- | --- | --- | ---: | --- | --- | --- | --- |\n");
        for entry in &log.entries {
            output.push_str(&format!(
                "| `{}` | `{}` | `{}` | {} | `{}` | `{}` | `{}` | `{}` |\n",
                entry.scope,
                entry.profile,
                entry.level,
                entry
                    .seed
                    .map_or("unknown".to_string(), |seed| seed.to_string()),
                entry.benchmark.as_deref().unwrap_or("-"),
                entry.failure_kind,
                entry.error.replace('|', "\\|"),
                entry
                    .reproduction
                    .as_ref()
                    .map(|reproduction| reproduction.benchmark_command.as_str())
                    .unwrap_or(entry.reproduce_command.as_str())
                    .replace('|', "\\|")
            ));
        }
        output.push('\n');
    }
    output
}

fn render_human_audit_report(audit: &BenchmarkAuditReport) -> String {
    let mut output = String::new();
    output.push_str("# Benchmark Readiness Audit\n\n");
    output.push_str(&format!("- Base: `{}`\n", audit.base.display()));
    output.push_str(&format!(
        "- Fixture matrix ready: `{}`\n",
        audit.fixture_matrix_ready
    ));
    output.push_str(&format!("- Ready: `{}`\n", audit.ready));
    output.push_str(&format!("- Total reports: `{}`\n", audit.total_reports));
    output.push_str(&format!(
        "- Total benchmarks: `{}`\n\n",
        audit.total_benchmarks
    ));

    output.push_str("## Coverage\n\n");
    output.push_str(&format!(
        "- Missing suites: `{}`\n",
        join_or_none(audit.missing_suites.iter().map(|suite| suite_slug(*suite)))
    ));
    output.push_str(&format!(
        "- Missing areas: `{}`\n",
        join_or_none(audit.missing_areas.iter().map(String::as_str))
    ));
    output.push_str(&format!(
        "- Missing requirements: `{}`\n",
        join_or_none(audit.missing_requirements.iter().map(String::as_str))
    ));
    output.push_str(&format!(
        "- Missing representative benchmarks: `{}`\n",
        join_or_none(
            audit
                .missing_representative_benchmarks
                .iter()
                .map(String::as_str)
        )
    ));
    output.push_str(&format!(
        "- Missing CLI workflows: `{}`\n",
        join_or_none(audit.missing_cli_workflows.iter().map(String::as_str))
    ));
    output.push_str(&format!(
        "- Missing sampled fact CLI workflows: `{}`\n",
        join_or_none(
            audit
                .missing_sampled_cli_workflows
                .iter()
                .map(String::as_str)
        )
    ));
    output.push_str(&format!(
        "- Missing cache temperatures: `{}`\n",
        join_or_none(audit.missing_cache_temperatures.iter().map(String::as_str))
    ));
    output.push_str(&format!(
        "- Missing fixture levels: `{}`\n",
        join_or_none(audit.inventory.missing_levels.iter().map(String::as_str))
    ));
    output.push_str(&format!(
        "- Missing fixture profiles: `{}`\n\n",
        join_or_none(audit.inventory.missing_profiles.iter().map(String::as_str))
    ));
    output.push_str(&format!(
        "- Missing fixture profile levels: `{}`\n\n",
        audit.inventory.missing_profile_levels.len()
    ));
    output.push_str(&format!(
        "- Missing baseline profile levels: `{}`\n\n",
        audit.missing_baseline_profile_levels.len()
    ));
    output.push_str(&format!(
        "- Missing cache profile levels: `{}`\n\n",
        audit.missing_cache_profile_levels.len()
    ));
    if !audit.remediation_commands.is_empty() {
        output.push_str("## Remediation Commands\n\n");
        output.push_str("| Kind | Profile | Level | Cache | Command |\n");
        output.push_str("| --- | --- | --- | --- | --- |\n");
        for remediation in &audit.remediation_commands {
            output.push_str(&format!(
                "| `{}` | `{}` | `{}` | `{}` | `{}` |\n",
                remediation.kind,
                remediation.profile.as_deref().unwrap_or("all"),
                remediation.level.as_deref().unwrap_or("all"),
                remediation.cache_temperature.as_deref().unwrap_or("n/a"),
                remediation.command.replace('|', "\\|")
            ));
        }
        output.push('\n');
    }
    if !audit.inventory.missing_profile_levels.is_empty() {
        output.push_str("### Missing Fixture Profile Levels\n\n");
        for missing in &audit.inventory.missing_profile_levels {
            output.push_str(&format!("- `{}` / `{}`\n", missing.profile, missing.level));
        }
        output.push('\n');
    }
    if !audit.missing_baseline_profile_levels.is_empty() {
        output.push_str("### Missing Baseline Profile Levels\n\n");
        for missing in &audit.missing_baseline_profile_levels {
            output.push_str(&format!("- `{}` / `{}`\n", missing.profile, missing.level));
        }
        output.push('\n');
    }
    if !audit.missing_cache_profile_levels.is_empty() {
        output.push_str("### Missing Cache Profile Levels\n\n");
        for missing in &audit.missing_cache_profile_levels {
            output.push_str(&format!(
                "- `{}` / `{}` / `{}`\n",
                missing.cache_temperature, missing.profile, missing.level
            ));
        }
        output.push('\n');
    }

    output.push_str("## Evidence Gaps\n\n");
    output.push_str(&format!(
        "- Missing report files: `{}`\n",
        audit.missing_report_files.len()
    ));
    output.push_str(&format!(
        "- Invalid report files: `{}`\n",
        audit.invalid_report_files.len()
    ));
    if !audit.invalid_report_files.is_empty() {
        output.push_str("\n### Invalid Report Files\n\n");
        for entry in audit.invalid_report_files.iter().take(20) {
            output.push_str(&format!("- `{}`\n", entry));
        }
        if audit.invalid_report_files.len() > 20 {
            output.push_str(&format!(
                "- ... `{}` more\n",
                audit.invalid_report_files.len() - 20
            ));
        }
        output.push('\n');
    }
    output.push_str(&format!(
        "- Invalid baseline summaries: `{}`\n",
        audit.invalid_baseline_summaries.len()
    ));
    if !audit.invalid_baseline_summaries.is_empty() {
        output.push_str("\n### Invalid Baseline Summaries\n\n");
        for entry in audit.invalid_baseline_summaries.iter().take(20) {
            output.push_str(&format!("- `{}`\n", entry));
        }
        if audit.invalid_baseline_summaries.len() > 20 {
            output.push_str(&format!(
                "- ... `{}` more\n",
                audit.invalid_baseline_summaries.len() - 20
            ));
        }
        output.push('\n');
    }
    output.push_str(&format!(
        "- Missing environment manifests: `{}`\n",
        audit.missing_environment_manifests.len()
    ));
    output.push_str(&format!(
        "- Invalid environment metadata: `{}`\n",
        audit.invalid_environment_metadata.len()
    ));
    output.push_str(&format!(
        "- Missing reproduction commands: `{}`\n",
        audit.missing_reproduce_commands.len()
    ));
    output.push_str(&format!(
        "- Non-release environment manifests: `{}`\n",
        audit.non_release_environment_manifests.len()
    ));
    output.push_str(&format!(
        "- Non-release reports: `{}`\n",
        audit.non_release_reports.len()
    ));
    output.push_str(&format!(
        "- Not-ready summaries: `{}`\n",
        audit.not_ready_summaries.len()
    ));
    output.push_str(&format!(
        "- Failing summaries: `{}`\n",
        audit.failing_summaries.len()
    ));
    output.push_str(&format!(
        "- Fixture metadata mismatches: `{}`\n",
        audit.fixture_metadata_mismatches.len()
    ));
    output.push_str(&format!(
        "- Missing fixture metadata: `{}`\n",
        audit.missing_fixture_metadata.len()
    ));
    output.push_str(&format!(
        "- Invalid report counts: `{}`\n",
        audit.invalid_report_counts.len()
    ));
    output.push_str(&format!(
        "- Invalid cache labels: `{}`\n",
        audit.invalid_cache_labels.len()
    ));
    output.push_str(&format!(
        "- Invalid cache metadata: `{}`\n",
        audit.invalid_cache_metadata.len()
    ));
    output.push_str(&format!(
        "- Insufficient warmup iterations: `{}`\n",
        audit.insufficient_warmup_iterations.len()
    ));
    output.push_str(&format!(
        "- Missing failure logs: `{}`\n",
        audit.missing_failure_logs.len()
    ));
    output.push_str(&format!(
        "- Invalid failure logs: `{}`\n",
        audit.invalid_failure_logs.len()
    ));
    output.push_str(&format!(
        "- Missing report metadata: `{}`\n",
        audit.missing_report_metadata.len()
    ));
    output.push_str(&format!(
        "- Missing benchmark metadata: `{}`\n",
        audit.missing_benchmark_metadata.len()
    ));
    if !audit.missing_benchmark_metadata.is_empty() {
        output.push_str("\n### Missing Benchmark Metadata\n\n");
        for entry in audit.missing_benchmark_metadata.iter().take(20) {
            output.push_str(&format!("- `{}`\n", entry));
        }
        if audit.missing_benchmark_metadata.len() > 20 {
            output.push_str(&format!(
                "- ... `{}` more\n",
                audit.missing_benchmark_metadata.len() - 20
            ));
        }
        output.push('\n');
    }
    output.push_str(&format!(
        "- Invalid requirement tags: `{}`\n",
        audit.invalid_requirement_tags.len()
    ));
    output.push_str(&format!(
        "- Missing bottleneck summaries: `{}`\n",
        audit.missing_bottleneck_summaries.len()
    ));
    output.push_str(&format!(
        "- Invalid bottleneck summaries: `{}`\n",
        audit.invalid_bottleneck_summaries.len()
    ));
    output.push_str(&format!(
        "- Insufficient baseline iterations: `{}`\n",
        audit.insufficient_baseline_iterations.len()
    ));
    output.push_str(&format!(
        "- Insufficient sample benchmarks: `{}`\n",
        audit.insufficient_sample_benchmarks.len()
    ));
    output.push_str(&format!(
        "- Invalid sample observations: `{}`\n",
        audit.invalid_sample_observations.len()
    ));
    output.push_str(&format!(
        "- Invalid phase metadata: `{}`\n",
        audit.invalid_phase_metadata.len()
    ));
    output.push_str(&format!(
        "- Invalid resource metadata: `{}`\n",
        audit.invalid_resource_metadata.len()
    ));
    output.push_str(&format!(
        "- Invalid SQLite metadata: `{}`\n",
        audit.invalid_sqlite_metadata.len()
    ));
    output.push_str(&format!(
        "- Invalid timing statistics: `{}`\n",
        audit.invalid_timing_statistics.len()
    ));
    output.push_str(&format!(
        "- Incorrect benchmarks: `{}`\n",
        audit.incorrect_benchmarks.len()
    ));

    output
}

fn render_human_acceptance_report(acceptance: &BenchmarkAcceptanceReport) -> String {
    let mut output = String::new();
    output.push_str("# Benchmark Acceptance Report\n\n");
    output.push_str(&format!("- Accepted: `{}`\n", acceptance.accepted));
    output.push_str(&format!("- Audit: `{}`\n", acceptance.audit.display()));
    output.push_str(&format!(
        "- Growth analysis: `{}`\n",
        acceptance.growth_analysis.display()
    ));
    output.push_str(&format!(
        "- Budget check: `{}`\n",
        acceptance.budget_check.display()
    ));
    output.push_str(&format!(
        "- Profile plan: `{}`\n\n",
        acceptance.profile_plan.display()
    ));
    output.push_str("## Evidence Summary\n\n");
    output.push_str(&format!(
        "- Fixture matrix ready: `{}`\n",
        acceptance.fixture_matrix_ready
    ));
    output.push_str(&format!(
        "- Readiness audit ready: `{}`\n",
        acceptance.readiness_ready
    ));
    output.push_str(&format!(
        "- Total reports: `{}`\n",
        acceptance.total_reports
    ));
    output.push_str(&format!(
        "- Total benchmarks: `{}`\n",
        acceptance.total_benchmarks
    ));
    output.push_str(&format!(
        "- Growth trends: `{}`\n",
        acceptance.growth_trend_count
    ));
    output.push_str(&format!(
        "- Complete growth trends: `{}`\n",
        acceptance.complete_growth_trend_count
    ));
    output.push_str(&format!(
        "- Insufficient growth trends: `{}`\n",
        acceptance.insufficient_growth_trend_count
    ));
    output.push_str(&format!(
        "- Incorrect growth trends: `{}`\n",
        acceptance.incorrect_growth_trend_count
    ));
    output.push_str(&format!(
        "- Budget checked entries: `{}`\n",
        acceptance.budget_checked_count
    ));
    output.push_str(&format!(
        "- Budget warnings: `{}`\n",
        acceptance.budget_warning_count
    ));
    output.push_str(&format!(
        "- Budget regressions: `{}`\n",
        acceptance.budget_regression_count
    ));
    output.push_str(&format!(
        "- Missing budgets: `{}`\n",
        acceptance.budget_missing_count
    ));
    output.push_str(&format!(
        "- Profile candidates: `{}`\n\n",
        acceptance.profile_candidate_count
    ));

    if !acceptance.blockers.is_empty() {
        output.push_str("## Blockers\n\n");
        for blocker in &acceptance.blockers {
            output.push_str(&format!("- `{blocker}`\n"));
        }
        output.push('\n');
    }
    if !acceptance.remediation_commands.is_empty() {
        output.push_str("## Remediation Commands\n\n");
        output.push_str("| Kind | Profile | Level | Cache | Command |\n");
        output.push_str("| --- | --- | --- | --- | --- |\n");
        for remediation in &acceptance.remediation_commands {
            output.push_str(&format!(
                "| `{}` | `{}` | `{}` | `{}` | `{}` |\n",
                remediation.kind,
                remediation.profile.as_deref().unwrap_or("all"),
                remediation.level.as_deref().unwrap_or("all"),
                remediation.cache_temperature.as_deref().unwrap_or("n/a"),
                remediation.command.replace('|', "\\|")
            ));
        }
        output.push('\n');
    }
    if !acceptance.blocker_evidence.is_empty() {
        output.push_str("## Blocker Evidence\n\n");
        for (name, count) in &acceptance.blocker_evidence {
            output.push_str(&format!("- `{name}`: `{count}`\n"));
        }
        output.push('\n');
    }

    output
}

fn render_human_comparison(comparison: &BenchmarkComparison) -> String {
    let mut output = String::new();
    output.push_str("# Benchmark Comparison\n\n");
    output.push_str(&format!(
        "- Baseline: `{}`\n",
        comparison.baseline.display()
    ));
    output.push_str(&format!("- Current: `{}`\n", comparison.current.display()));
    output.push_str(&format!(
        "- Warning threshold: `{:.2}%`\n",
        comparison.warning_threshold_percent
    ));
    output.push_str(&format!(
        "- Regression threshold: `{:.2}%`\n",
        comparison.regression_threshold_percent
    ));
    output.push_str(&format!(
        "- Environment compatible: `{}`\n",
        comparison.environment_compatible
    ));
    output.push_str(&format!(
        "- Fixture compatible: `{}`\n",
        comparison.fixture_compatible
    ));
    output.push_str(&format!(
        "- Cache compatible: `{}`\n",
        comparison.cache_compatible
    ));
    output.push_str(&format!(
        "- Regressions: `{}`\n",
        comparison.regressions.len()
    ));
    output.push_str(&format!("- Warnings: `{}`\n", comparison.warnings.len()));
    output.push_str(&format!(
        "- Improvements: `{}`\n",
        comparison.improvements.len()
    ));
    output.push_str(&format!(
        "- Informational: `{}`\n",
        comparison.informational.len()
    ));
    output.push_str(&format!(
        "- Missing in current: `{}`\n",
        comparison.missing_in_current.len()
    ));
    output.push_str(&format!(
        "- New in current: `{}`\n\n",
        comparison.new_in_current.len()
    ));
    output.push_str(&format!(
        "- Incorrect baseline benchmarks: `{}`\n",
        comparison.incorrect_baseline_benchmarks.len()
    ));
    output.push_str(&format!(
        "- Incorrect current benchmarks: `{}`\n\n",
        comparison.incorrect_current_benchmarks.len()
    ));
    output.push_str(&format!(
        "- Regression decision ready: `{}`\n",
        comparison.regression_decision_ready
    ));
    output.push_str(&format!(
        "- Regression decision blockers: `{}`\n\n",
        join_or_none(
            comparison
                .regression_decision_blockers
                .iter()
                .map(String::as_str)
        )
    ));

    if !comparison.environment_differences.is_empty() {
        output.push_str("## Environment Differences\n\n");
        output.push_str("| Field | Baseline | Current |\n");
        output.push_str("| --- | --- | --- |\n");
        for difference in &comparison.environment_differences {
            output.push_str(&format!(
                "| `{}` | `{}` | `{}` |\n",
                difference.field,
                difference.baseline.as_deref().unwrap_or("none"),
                difference.current.as_deref().unwrap_or("none")
            ));
        }
        output.push('\n');
    }

    if !comparison.fixture_differences.is_empty() {
        output.push_str("## Fixture Differences\n\n");
        output.push_str("| Field | Baseline | Current |\n");
        output.push_str("| --- | --- | --- |\n");
        for difference in &comparison.fixture_differences {
            output.push_str(&format!(
                "| `{}` | `{}` | `{}` |\n",
                difference.field,
                difference.baseline.as_deref().unwrap_or("none"),
                difference.current.as_deref().unwrap_or("none")
            ));
        }
        output.push('\n');
    }

    if !comparison.cache_differences.is_empty() {
        output.push_str("## Cache Differences\n\n");
        output.push_str(
            "| Benchmark | Baseline cache | Current cache | Baseline class | Current class |\n",
        );
        output.push_str("| --- | --- | --- | --- | --- |\n");
        for difference in &comparison.cache_differences {
            output.push_str(&format!(
                "| `{}` | `{}` | `{}` | `{}` | `{}` |\n",
                difference.benchmark,
                difference.baseline_cache_state,
                difference.current_cache_state,
                fmt_cache_classification(difference.baseline_classification.as_ref()),
                fmt_cache_classification(difference.current_classification.as_ref())
            ));
        }
        output.push('\n');
    }

    render_delta_table(&mut output, "Regressions", &comparison.regressions);
    render_delta_table(&mut output, "Warnings", &comparison.warnings);
    render_delta_table(&mut output, "Improvements", &comparison.improvements);
    render_delta_table(&mut output, "Informational", &comparison.informational);
    render_correctness_issue_table(
        &mut output,
        "Incorrect Baseline Benchmarks",
        &comparison.incorrect_baseline_benchmarks,
    );
    render_correctness_issue_table(
        &mut output,
        "Incorrect Current Benchmarks",
        &comparison.incorrect_current_benchmarks,
    );

    if !comparison.missing_in_current.is_empty() {
        output.push_str("## Missing In Current\n\n");
        for benchmark in &comparison.missing_in_current {
            output.push_str(&format!("- `{}`\n", benchmark));
        }
        output.push('\n');
    }

    if !comparison.new_in_current.is_empty() {
        output.push_str("## New In Current\n\n");
        for benchmark in &comparison.new_in_current {
            output.push_str(&format!("- `{}`\n", benchmark));
        }
        output.push('\n');
    }

    output
}

fn render_environment_difference_summary(differences: &[EnvironmentDifference]) -> String {
    if differences.is_empty() {
        return "none".to_string();
    }
    differences
        .iter()
        .map(|difference| {
            render_named_difference(&difference.field, &difference.baseline, &difference.current)
        })
        .collect::<Vec<_>>()
        .join("; ")
        .replace('|', "\\|")
}

fn render_fixture_difference_summary(differences: &[BenchmarkFixtureDifference]) -> String {
    if differences.is_empty() {
        return "none".to_string();
    }
    differences
        .iter()
        .map(|difference| {
            render_named_difference(&difference.field, &difference.baseline, &difference.current)
        })
        .collect::<Vec<_>>()
        .join("; ")
        .replace('|', "\\|")
}

fn render_named_difference(
    field: &str,
    baseline: &Option<String>,
    current: &Option<String>,
) -> String {
    format!(
        "{}: {} -> {}",
        field,
        baseline.as_deref().unwrap_or("none"),
        current.as_deref().unwrap_or("none")
    )
}

fn render_cache_difference_summary(differences: &[BenchmarkCacheDifference]) -> String {
    if differences.is_empty() {
        return "none".to_string();
    }
    differences
        .iter()
        .map(|difference| {
            format!(
                "{}: {} -> {}",
                difference.benchmark,
                difference.baseline_cache_state,
                difference.current_cache_state
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
        .replace('|', "\\|")
}

fn render_human_matrix_comparison(comparison: &BenchmarkMatrixComparison) -> String {
    let mut output = String::new();
    output.push_str("# Benchmark Matrix Comparison\n\n");
    output.push_str(&format!(
        "- Baseline summary: `{}`\n",
        comparison.baseline_summary.display()
    ));
    output.push_str(&format!(
        "- Current summary: `{}`\n",
        comparison.current_summary.display()
    ));
    output.push_str(&format!(
        "- Warning threshold: `{:.2}%`\n",
        comparison.warning_threshold_percent
    ));
    output.push_str(&format!(
        "- Regression threshold: `{:.2}%`\n",
        comparison.regression_threshold_percent
    ));
    output.push_str(&format!(
        "- Baseline summary ready: `{}`\n",
        comparison.baseline_summary_ready
    ));
    output.push_str(&format!(
        "- Current summary ready: `{}`\n",
        comparison.current_summary_ready
    ));
    output.push_str(&format!(
        "- Baseline summary failures: `{}`\n",
        comparison.baseline_failure_count
    ));
    output.push_str(&format!(
        "- Current summary failures: `{}`\n",
        comparison.current_failure_count
    ));
    output.push_str(&format!(
        "- Compared reports: `{}`\n",
        comparison.compared_reports.len()
    ));
    output.push_str(&format!(
        "- Total regressions: `{}`\n",
        comparison.total_regressions
    ));
    output.push_str(&format!(
        "- Total warnings: `{}`\n",
        comparison.total_warnings
    ));
    output.push_str(&format!(
        "- Total improvements: `{}`\n",
        comparison.total_improvements
    ));
    output.push_str(&format!(
        "- Environment-incompatible reports: `{}`\n",
        comparison.total_environment_incompatible_reports
    ));
    output.push_str(&format!(
        "- Environment differences: `{}`\n",
        comparison.total_environment_differences
    ));
    output.push_str(&format!(
        "- Cache-incompatible reports: `{}`\n",
        comparison.total_cache_incompatible_reports
    ));
    output.push_str(&format!(
        "- Fixture-incompatible reports: `{}`\n",
        comparison.total_fixture_incompatible_reports
    ));
    output.push_str(&format!(
        "- Fixture differences: `{}`\n",
        comparison.total_fixture_differences
    ));
    output.push_str(&format!(
        "- Cache differences: `{}`\n",
        comparison.total_cache_differences
    ));
    output.push_str(&format!(
        "- Total incorrect current benchmarks: `{}`\n",
        comparison.total_incorrect_current_benchmarks
    ));
    output.push_str(&format!(
        "- Regression decision ready: `{}`\n",
        comparison.regression_decision_ready
    ));
    output.push_str(&format!(
        "- Regression decision blockers: `{}`\n",
        join_or_none(
            comparison
                .regression_decision_blockers
                .iter()
                .map(String::as_str)
        )
    ));
    output.push_str(&format!(
        "- Missing reports in current: `{}`\n",
        comparison.missing_in_current.len()
    ));
    output.push_str(&format!(
        "- New reports in current: `{}`\n",
        comparison.new_in_current.len()
    ));
    output.push_str(&format!("- Failures: `{}`\n\n", comparison.failures.len()));

    if !comparison.compared_reports.is_empty() {
        output.push_str("## Compared Reports\n\n");
        output.push_str("| Key | Profile | Level | Seed | Decision Ready | Blockers | Environment | Environment Differences | Fixture | Fixture Differences | Cache | Cache Differences | Regressions | Warnings | Improvements | Incorrect Current | Missing Benchmarks | New Benchmarks |\n");
        output.push_str(
            "| --- | --- | --- | ---: | --- | --- | --- | ---: | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n",
        );
        for report in &comparison.compared_reports {
            output.push_str(&format!(
                "| `{}` | `{}` | `{}` | {} | `{}` | `{}` | `{}` | {} | `{}` | {} | `{}` | {} | {} | {} | {} | {} | {} | {} |\n",
                report.key,
                report.profile,
                report.level,
                report
                    .seed
                    .map_or("unknown".to_string(), |seed| seed.to_string()),
                report.regression_decision_ready,
                join_or_none(report.regression_decision_blockers.iter().map(String::as_str)),
                if report.environment_compatible {
                    "compatible"
                } else {
                    "different"
                },
                report.environment_difference_count,
                if report.fixture_compatible {
                    "compatible"
                } else {
                    "different"
                },
                report.fixture_difference_count,
                if report.cache_compatible {
                    "compatible"
                } else {
                    "different"
                },
                report.cache_difference_count,
                report.regression_count,
                report.warning_count,
                report.improvement_count,
                report.incorrect_current_count,
                report.missing_benchmark_count,
                report.new_benchmark_count
            ));
        }
        output.push('\n');
    }

    let blocked_reports = comparison
        .compared_reports
        .iter()
        .filter(|report| !report.regression_decision_ready)
        .collect::<Vec<_>>();
    if !blocked_reports.is_empty() {
        output.push_str("## Report Comparison Blockers\n\n");
        output.push_str("| Key | Blockers | Environment Differences | Fixture Differences | Cache Differences |\n");
        output.push_str("| --- | --- | --- | --- | --- |\n");
        for report in blocked_reports {
            output.push_str(&format!(
                "| `{}` | `{}` | `{}` | `{}` | `{}` |\n",
                report.key,
                join_or_none(
                    report
                        .regression_decision_blockers
                        .iter()
                        .map(String::as_str)
                ),
                render_environment_difference_summary(&report.comparison.environment_differences),
                render_fixture_difference_summary(&report.comparison.fixture_differences),
                render_cache_difference_summary(&report.comparison.cache_differences)
            ));
        }
        output.push('\n');
    }

    if !comparison.missing_in_current.is_empty() {
        output.push_str("## Missing Reports In Current\n\n");
        for key in &comparison.missing_in_current {
            output.push_str(&format!("- `{}`\n", key));
        }
        output.push('\n');
    }

    if !comparison.new_in_current.is_empty() {
        output.push_str("## New Reports In Current\n\n");
        for key in &comparison.new_in_current {
            output.push_str(&format!("- `{}`\n", key));
        }
        output.push('\n');
    }

    if !comparison.failures.is_empty() {
        output.push_str("## Failures\n\n");
        output.push_str("| Key | Baseline | Current | Error |\n");
        output.push_str("| --- | --- | --- | --- |\n");
        for failure in &comparison.failures {
            output.push_str(&format!(
                "| `{}` | `{}` | `{}` | `{}` |\n",
                failure.key,
                failure
                    .baseline_report
                    .as_deref()
                    .map_or("none".to_string(), |path| path.display().to_string()),
                failure
                    .current_report
                    .as_deref()
                    .map_or("none".to_string(), |path| path.display().to_string()),
                failure.error.replace('|', "\\|")
            ));
        }
        output.push('\n');
    }

    output
}

fn render_human_growth_analysis(analysis: &BenchmarkGrowthAnalysis) -> String {
    let mut output = String::new();
    output.push_str("# Benchmark Growth Analysis\n\n");
    output.push_str(&format!(
        "- Baseline summaries: `{}`\n",
        analysis.baseline_summaries.len()
    ));
    output.push_str(&format!("- Trends: `{}`\n", analysis.trend_count));
    output.push_str(&format!(
        "- Complete trends: `{}`\n",
        analysis.complete_trend_count
    ));
    output.push_str(&format!(
        "- Insufficient-data trends: `{}`\n",
        analysis.insufficient_trend_count
    ));
    output.push_str(&format!(
        "- Incorrect trends: `{}`\n",
        analysis.incorrect_trend_count
    ));
    output.push_str(&format!("- Failures: `{}`\n\n", analysis.failures.len()));

    if !analysis.trends.is_empty() {
        output.push_str("## Trends\n\n");
        output.push_str("| Profile | Benchmark | Shape | Driver | Data factor | Latency factor | Rows factor | Points |\n");
        output.push_str("| --- | --- | --- | --- | ---: | ---: | ---: | ---: |\n");
        for trend in &analysis.trends {
            output.push_str(&format!(
                "| `{}` | `{}` | `{}` | `{}` | {} | {} | {} | {} |\n",
                trend.profile,
                trend.benchmark,
                growth_shape_slug(&trend.classification.shape),
                trend.classification.likely_driver,
                fmt_factor(trend.classification.data_factor),
                fmt_factor(trend.classification.latency_factor),
                fmt_factor(trend.classification.rows_factor),
                trend.points.len()
            ));
        }
        output.push('\n');
    }

    if !analysis.failures.is_empty() {
        output.push_str("## Failures\n\n");
        output.push_str("| Summary | Report | Error |\n");
        output.push_str("| --- | --- | --- |\n");
        for failure in &analysis.failures {
            output.push_str(&format!(
                "| `{}` | `{}` | `{}` |\n",
                failure.summary.display(),
                failure.report.display(),
                failure.error.replace('|', "\\|")
            ));
        }
        output.push('\n');
    }

    output
}

fn render_human_budgets(budgets: &BenchmarkBudgets) -> String {
    let mut output = String::new();
    output.push_str("# Benchmark Performance Budgets\n\n");
    output.push_str(&format!(
        "- Baseline summaries: `{}`\n",
        budgets.baseline_summaries.len()
    ));
    output.push_str(&format!("- Budgets: `{}`\n", budgets.entry_count));
    output.push_str(&format!(
        "- Warning multiplier: `{:.2}x`\n",
        budgets.warning_multiplier
    ));
    output.push_str(&format!(
        "- Regression multiplier: `{:.2}x`\n",
        budgets.regression_multiplier
    ));
    output.push_str(&format!(
        "- Minimum warning margin ms: `{:.3}`\n",
        budgets.minimum_warning_ms
    ));
    output.push_str(&format!(
        "- Minimum regression margin ms: `{:.3}`\n",
        budgets.minimum_regression_ms
    ));
    output.push_str(&format!(
        "- Incorrect baseline entries: `{}`\n",
        budgets.incorrect_entry_count
    ));
    output.push_str(&format!("- Failures: `{}`\n\n", budgets.failures.len()));

    if !budgets.budgets.is_empty() {
        output.push_str("## Budgets\n\n");
        output.push_str("| Profile | Level | Suite | Benchmark | Median ms | P95 ms | Warning ms | Regression ms | Samples | Correct |\n");
        output.push_str("| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |\n");
        for entry in &budgets.budgets {
            output.push_str(&format!(
                "| `{}` | `{}` | `{}` | `{}` | {:.3} | {} | {:.3} | {:.3} | {} | {} |\n",
                entry.profile,
                entry.level,
                suite_slug(entry.suite),
                entry.benchmark,
                entry.baseline_median_ms,
                fmt_ms(entry.baseline_p95_ms),
                entry.warning_budget_ms,
                entry.regression_budget_ms,
                entry.samples,
                entry.correctness_passed
            ));
        }
        output.push('\n');
    }

    if !budgets.failures.is_empty() {
        output.push_str("## Failures\n\n");
        output.push_str("| Summary | Report | Error |\n");
        output.push_str("| --- | --- | --- |\n");
        for failure in &budgets.failures {
            output.push_str(&format!(
                "| `{}` | `{}` | `{}` |\n",
                failure.summary.display(),
                failure.report.display(),
                failure.error.replace('|', "\\|")
            ));
        }
        output.push('\n');
    }

    output
}

fn render_human_budget_check(check: &BenchmarkBudgetCheck) -> String {
    let mut output = String::new();
    output.push_str("# Benchmark Budget Check\n\n");
    output.push_str(&format!("- Budgets: `{}`\n", check.budgets.display()));
    output.push_str(&format!(
        "- Baseline summary: `{}`\n",
        check.baseline_summary.display()
    ));
    output.push_str(&format!("- Passed: `{}`\n", check.passed));
    output.push_str(&format!("- Checked: `{}`\n", check.checked_count));
    output.push_str(&format!("- Warnings: `{}`\n", check.warning_count));
    output.push_str(&format!("- Regressions: `{}`\n", check.regression_count));
    output.push_str(&format!("- Incorrect: `{}`\n", check.incorrect_count));
    output.push_str(&format!(
        "- Missing budgets: `{}`\n",
        check.missing_budget_count
    ));
    output.push_str(&format!("- Failures: `{}`\n\n", check.failure_count));
    if check.warning_count > 0 && check.passed {
        output.push_str(
            "Budget warnings are recorded for review but do not fail the budget check.\n\n",
        );
    }

    let notable = check
        .results
        .iter()
        .filter(|result| result.classification != BenchmarkBudgetStatus::WithinBudget)
        .collect::<Vec<_>>();
    if !notable.is_empty() {
        output.push_str("## Budget Findings\n\n");
        output.push_str("| Profile | Level | Benchmark | Status | Current median ms | Warning ms | Regression ms | Over baseline |\n");
        output.push_str("| --- | --- | --- | --- | ---: | ---: | ---: | ---: |\n");
        for result in notable {
            output.push_str(&format!(
                "| `{}` | `{}` | `{}` | `{}` | {:.3} | {:.3} | {:.3} | {} |\n",
                result.profile,
                result.level,
                result.benchmark,
                budget_status_slug(&result.classification),
                result.current_median_ms,
                result.warning_budget_ms,
                result.regression_budget_ms,
                fmt_percent(result.percentage_over_baseline)
            ));
        }
        output.push('\n');
    }

    if !check.missing_budgets.is_empty() {
        output.push_str("## Missing Budgets\n\n");
        for missing in &check.missing_budgets {
            output.push_str(&format!(
                "- `{}` from `{}`\n",
                missing.key,
                missing.report.display()
            ));
        }
        output.push('\n');
    }

    if !check.failures.is_empty() {
        output.push_str("## Failures\n\n");
        output.push_str("| Key | Report | Error |\n");
        output.push_str("| --- | --- | --- |\n");
        for failure in &check.failures {
            output.push_str(&format!(
                "| `{}` | `{}` | `{}` |\n",
                failure.key,
                failure.report.display(),
                failure.error.replace('|', "\\|")
            ));
        }
        output.push('\n');
    }

    output
}

fn render_human_profile_plan(plan: &BenchmarkProfilePlan) -> String {
    let mut output = String::new();
    output.push_str("# Benchmark Profile Plan\n\n");
    output.push_str(&format!(
        "- Baseline summaries: `{}`\n",
        plan.baseline_summaries.len()
    ));
    output.push_str(&format!("- Limit: `{}`\n", plan.limit));
    output.push_str(&format!("- Candidates: `{}`\n", plan.candidate_count));
    output.push_str(&format!("- Failures: `{}`\n\n", plan.failure_count));

    if !plan.candidates.is_empty() {
        output.push_str("## Candidates\n\n");
        output.push_str("| Profile | Level | Suite | Benchmark | P95 ms | Median ms | Resources | Reasons | First command |\n");
        output.push_str("| --- | --- | --- | --- | ---: | ---: | --- | --- | --- |\n");
        for candidate in &plan.candidates {
            output.push_str(&format!(
                "| `{}` | `{}` | `{}` | `{}` | {:.3} | {:.3} | `{}` | `{}` | `{}` |\n",
                candidate.profile,
                candidate.level,
                suite_slug(candidate.suite),
                candidate.benchmark,
                candidate.p95_ms,
                candidate.median_ms,
                render_profile_resource_summary(candidate),
                candidate.reasons.join(", "),
                candidate
                    .suggested_commands
                    .first()
                    .map(String::as_str)
                    .unwrap_or("none")
            ));
        }
        output.push('\n');
    }

    if !plan.failures.is_empty() {
        output.push_str("## Failures\n\n");
        output.push_str("| Summary | Report | Error |\n");
        output.push_str("| --- | --- | --- |\n");
        for failure in &plan.failures {
            output.push_str(&format!(
                "| `{}` | `{}` | `{}` |\n",
                failure.summary.display(),
                failure.report.display(),
                failure.error.replace('|', "\\|")
            ));
        }
        output.push('\n');
    }

    output
}

fn render_profile_resource_summary(candidate: &BenchmarkProfileCandidate) -> String {
    let mut parts = Vec::new();
    if let Some(bytes) = candidate.measured_bytes {
        parts.push(format!("bytes={bytes}"));
    }
    if let Some(bytes) = candidate.network_payload_bytes {
        parts.push(format!("network_bytes={bytes}"));
    }
    if let Some(delta) = candidate.process_rss_kib_delta {
        parts.push(format!("rss_delta_kib={delta}"));
    }
    if let Some(delta) = candidate.process_peak_rss_kib_delta {
        parts.push(format!("peak_rss_delta_kib={delta}"));
    }
    if let Some(delta) = candidate.process_cpu_seconds_delta {
        parts.push(format!("cpu_delta_s={delta:.6}"));
    }
    if let Some(delta) = candidate.artifact_bytes_delta {
        parts.push(format!("artifact_delta_bytes={delta}"));
    }
    if let Some(delta) = candidate.disk_read_bytes_delta {
        parts.push(format!("disk_read_delta_bytes={delta}"));
    }
    if let Some(delta) = candidate.disk_write_bytes_delta {
        parts.push(format!("disk_write_delta_bytes={delta}"));
    }
    if parts.is_empty() {
        "none".to_string()
    } else {
        parts.join(", ")
    }
}

fn fmt_cache_classification(classification: Option<&CacheClassification>) -> String {
    classification.map_or_else(
        || "none".to_string(),
        |classification| {
            format!(
                "{}/{}",
                cache_temperature_slug(classification.temperature),
                cache_scope_slug(classification.scope)
            )
        },
    )
}

fn cache_temperature_slug(temperature: CacheTemperature) -> &'static str {
    match temperature {
        CacheTemperature::Cold => "cold",
        CacheTemperature::Warm => "warm",
        CacheTemperature::First => "first",
        CacheTemperature::Steady => "steady",
        CacheTemperature::Profiling => "profiling",
    }
}

fn cache_scope_slug(scope: CacheScope) -> &'static str {
    match scope {
        CacheScope::Process => "process",
        CacheScope::Filesystem => "filesystem",
        CacheScope::SearchIndex => "search-index",
        CacheScope::Request => "request",
        CacheScope::Profiling => "profiling",
    }
}

fn render_delta_table(output: &mut String, title: &str, deltas: &[BenchmarkDelta]) {
    if deltas.is_empty() {
        return;
    }
    output.push_str(&format!("## {title}\n\n"));
    output.push_str("| Benchmark | Baseline median ms | Current median ms | Difference ms | Difference % | Noise % | Exceeds noise | Warning % | Regression % |\n");
    output.push_str("| --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: |\n");
    for delta in deltas {
        output.push_str(&format!(
            "| `{}` | {:.3} | {:.3} | {:.3} | {:.2}% | {} | {} | {:.2}% | {:.2}% |\n",
            delta.benchmark,
            delta.baseline_median_ms,
            delta.current_median_ms,
            delta.absolute_difference_ms,
            delta.percentage_difference,
            fmt_percent(delta.noise.noise_threshold_percent),
            delta
                .noise
                .difference_exceeds_noise_threshold
                .map_or("n/a".to_string(), |exceeds| exceeds.to_string()),
            delta.warning_threshold_percent,
            delta.regression_threshold_percent
        ));
    }
    output.push('\n');
}

fn render_correctness_issue_table(
    output: &mut String,
    title: &str,
    issues: &[BenchmarkComparisonCorrectnessIssue],
) {
    if issues.is_empty() {
        return;
    }
    output.push_str(&format!("## {title}\n\n"));
    output.push_str("| Benchmark | Correctness passed | Failures |\n");
    output.push_str("| --- | --- | --- |\n");
    for issue in issues {
        output.push_str(&format!(
            "| `{}` | `{}` | `{}` |\n",
            issue.benchmark,
            issue.correctness_passed,
            join_or_none(issue.failures.iter().map(String::as_str)).replace('|', "\\|")
        ));
    }
    output.push('\n');
}

fn growth_shape_slug(shape: &BenchmarkGrowthShape) -> &'static str {
    match shape {
        BenchmarkGrowthShape::Constant => "constant",
        BenchmarkGrowthShape::Logarithmic => "logarithmic",
        BenchmarkGrowthShape::Linear => "linear",
        BenchmarkGrowthShape::Superlinear => "superlinear",
        BenchmarkGrowthShape::DominatedByResultSize => "dominated-by-result-size",
        BenchmarkGrowthShape::InsufficientData => "insufficient-data",
    }
}

fn budget_status_slug(status: &BenchmarkBudgetStatus) -> &'static str {
    match status {
        BenchmarkBudgetStatus::WithinBudget => "within-budget",
        BenchmarkBudgetStatus::Warning => "warning",
        BenchmarkBudgetStatus::Regression => "regression",
    }
}

fn fmt_factor(value: Option<f64>) -> String {
    value.map_or_else(|| "n/a".to_string(), |value| format!("{value:.2}x"))
}

fn fmt_percent(value: Option<f64>) -> String {
    value.map_or_else(|| "n/a".to_string(), |value| format!("{value:.2}%"))
}

fn join_or_none<'a>(values: impl Iterator<Item = &'a str>) -> String {
    let joined = values.collect::<Vec<_>>().join(", ");
    if joined.is_empty() {
        "none".to_string()
    } else {
        joined
    }
}

fn render_diagnostics_summary(diagnostics: &BenchmarkDiagnostics) -> String {
    let mut parts = vec![diagnostics.operation_kind.clone()];
    if diagnostics.resources_after.process_rss_kib.is_some() {
        parts.push("rss".to_string());
    }
    if diagnostics.resources_after.process_peak_rss_kib.is_some() {
        parts.push("peak-rss".to_string());
    }
    if diagnostics.resources_after.process_cpu_seconds.is_some() {
        parts.push("cpu".to_string());
    }
    if diagnostics.resource_delta.artifact_bytes_delta.is_some() {
        parts.push("artifact-delta".to_string());
    }
    if diagnostics.resources_after.disk_read_bytes.is_some()
        || diagnostics.resources_after.disk_write_bytes.is_some()
    {
        parts.push("disk-io".to_string());
    }
    if diagnostics.resource_delta.process_rss_kib_delta.is_some()
        || diagnostics
            .resource_delta
            .process_peak_rss_kib_delta
            .is_some()
        || diagnostics
            .resource_delta
            .process_cpu_seconds_delta
            .is_some()
        || diagnostics.resource_delta.disk_read_bytes_delta.is_some()
        || diagnostics.resource_delta.disk_write_bytes_delta.is_some()
    {
        parts.push("resource-delta".to_string());
    }
    if let Some(sqlite) = &diagnostics.sqlite {
        if sqlite.uses_full_scan {
            parts.push("full-scan".to_string());
        }
        if sqlite.uses_temporary_btree {
            parts.push("temp-btree".to_string());
        }
    }
    parts.join(", ")
}

fn render_phase_summary(phases: &[BenchmarkPhaseBreakdown]) -> String {
    if phases.is_empty() {
        return "none".to_string();
    }
    phases
        .iter()
        .map(|phase| match phase.elapsed_ms {
            Some(elapsed_ms) => format!("{}:{elapsed_ms:.3}ms", phase.phase),
            None => format!(
                "{}:{}",
                phase.phase,
                phase_measurement_slug(phase.measurement)
            ),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn phase_measurement_slug(measurement: BenchmarkPhaseMeasurement) -> &'static str {
    match measurement {
        BenchmarkPhaseMeasurement::Measured => "measured",
        BenchmarkPhaseMeasurement::IncludedInParent => "included",
        BenchmarkPhaseMeasurement::NotSeparatelyObservable => "not-observable",
    }
}

fn fmt_ms(value: Option<f64>) -> String {
    value.map_or_else(|| "n/a".to_string(), |value| format!("{value:.3}"))
}

fn read_report(path: &Path) -> Result<BenchmarkRunReport> {
    let report: BenchmarkRunReport = serde_json::from_slice(
        &std::fs::read(path).with_context(|| format!("failed to read `{}`", path.display()))?,
    )
    .with_context(|| format!("failed to parse `{}`", path.display()))?;
    ensure_schema_version(path, &report.schema_version)?;
    Ok(report)
}

fn read_baseline_summary(path: &Path) -> Result<BenchmarkBaselineSummary> {
    let summary: BenchmarkBaselineSummary = serde_json::from_slice(
        &std::fs::read(path).with_context(|| format!("failed to read `{}`", path.display()))?,
    )
    .with_context(|| format!("failed to parse `{}`", path.display()))?;
    ensure_schema_version(path, &summary.schema_version)?;
    Ok(summary)
}

fn read_failure_log(path: &Path) -> Result<BenchmarkFailureLog> {
    let log: BenchmarkFailureLog = serde_json::from_slice(
        &std::fs::read(path).with_context(|| format!("failed to read `{}`", path.display()))?,
    )
    .with_context(|| format!("failed to parse `{}`", path.display()))?;
    ensure_schema_version(path, &log.schema_version)?;
    Ok(log)
}

fn read_budgets(path: &Path) -> Result<BenchmarkBudgets> {
    let budgets: BenchmarkBudgets = serde_json::from_slice(
        &std::fs::read(path).with_context(|| format!("failed to read `{}`", path.display()))?,
    )
    .with_context(|| format!("failed to parse `{}`", path.display()))?;
    ensure_schema_version(path, &budgets.schema_version)?;
    Ok(budgets)
}

fn read_audit_report(path: &Path) -> Result<BenchmarkAuditReport> {
    let report: BenchmarkAuditReport = serde_json::from_slice(
        &std::fs::read(path).with_context(|| format!("failed to read `{}`", path.display()))?,
    )
    .with_context(|| format!("failed to parse `{}`", path.display()))?;
    ensure_schema_version(path, &report.schema_version)?;
    Ok(report)
}

fn read_growth_analysis(path: &Path) -> Result<BenchmarkGrowthAnalysis> {
    let analysis: BenchmarkGrowthAnalysis = serde_json::from_slice(
        &std::fs::read(path).with_context(|| format!("failed to read `{}`", path.display()))?,
    )
    .with_context(|| format!("failed to parse `{}`", path.display()))?;
    ensure_schema_version(path, &analysis.schema_version)?;
    Ok(analysis)
}

fn read_budget_check(path: &Path) -> Result<BenchmarkBudgetCheck> {
    let check: BenchmarkBudgetCheck = serde_json::from_slice(
        &std::fs::read(path).with_context(|| format!("failed to read `{}`", path.display()))?,
    )
    .with_context(|| format!("failed to parse `{}`", path.display()))?;
    ensure_schema_version(path, &check.schema_version)?;
    Ok(check)
}

fn read_profile_plan(path: &Path) -> Result<BenchmarkProfilePlan> {
    let plan: BenchmarkProfilePlan = serde_json::from_slice(
        &std::fs::read(path).with_context(|| format!("failed to read `{}`", path.display()))?,
    )
    .with_context(|| format!("failed to parse `{}`", path.display()))?;
    ensure_schema_version(path, &plan.schema_version)?;
    Ok(plan)
}

fn ensure_schema_version(path: &Path, schema_version: &str) -> Result<()> {
    if schema_version != REPORT_SCHEMA_VERSION {
        bail!(
            "`{}` has schema_version `{}`; expected `{}`",
            path.display(),
            schema_version,
            REPORT_SCHEMA_VERSION
        );
    }
    Ok(())
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    serde_json::from_slice(
        &std::fs::read(path).with_context(|| format!("failed to read `{}`", path.display()))?,
    )
    .with_context(|| format!("failed to parse `{}`", path.display()))
}

fn sqlite_databases(fixture: &Path) -> Result<Vec<PathBuf>> {
    let mut databases = recursive_files(fixture)?
        .into_iter()
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("sqlite"))
        .filter(|path| {
            !path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with("-wal") || name.ends_with("-shm"))
        })
        .collect::<Vec<_>>();
    databases.sort();
    Ok(databases)
}

fn benchmark_database_for_fixture(fixture: &FixtureMetadata) -> Result<PathBuf> {
    let mut candidates = Vec::new();
    for database in &fixture.sqlite_databases {
        let connection = rusqlite::Connection::open(database)
            .with_context(|| format!("failed to open `{}`", database.display()))?;
        let tables = sqlite_tables(&connection)?;
        let proposition_count = if tables.contains("protocol_object") {
            count_sql(
                &connection,
                "SELECT COUNT(*) FROM protocol_object WHERE object_type='proposition'",
            )?
        } else {
            0
        };
        let object_count = if tables.contains("protocol_object") {
            count_sql(&connection, "SELECT COUNT(*) FROM protocol_object")?
        } else {
            0
        };
        candidates.push((proposition_count, object_count, database.clone()));
    }
    candidates
        .into_iter()
        .max_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
                .then_with(|| right.2.cmp(&left.2))
        })
        .map(|(_, _, database)| database)
        .context("benchmark fixture contains no SQLite database")
}

fn recursive_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if !root.exists() {
        return Ok(files);
    }
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        for entry in std::fs::read_dir(&path)
            .with_context(|| format!("failed to read `{}`", path.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                files.push(path);
            }
        }
    }
    Ok(files)
}

fn sqlite_tables(connection: &rusqlite::Connection) -> Result<BTreeSet<String>> {
    let mut statement = connection.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
    )?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    Ok(rows.collect::<std::result::Result<BTreeSet<_>, _>>()?)
}

fn projected_table(tables: &BTreeSet<String>, suffix: &str) -> Option<String> {
    let projected = format!("projected_{suffix}");
    let projection = format!("projection_{suffix}");
    if tables.contains(&projected) {
        Some(projected)
    } else if tables.contains(&projection) {
        Some(projection)
    } else {
        None
    }
}

fn search_table(tables: &BTreeSet<String>) -> Option<String> {
    ["search_document", "search_index", "indexed_search_document"]
        .iter()
        .find(|table| tables.contains(**table))
        .map(|table| (*table).to_string())
}

fn count_sql(connection: &rusqlite::Connection, sql: &str) -> Result<u64> {
    Ok(connection
        .query_row(sql, [], |row| row.get::<_, i64>(0))
        .optional()?
        .unwrap_or_default() as u64)
}

fn sqlite_table_bytes(connection: &rusqlite::Connection, table: &str) -> Option<u64> {
    connection
        .query_row(
            "SELECT COALESCE(SUM(pgsize), 0) FROM dbstat WHERE name = ?1",
            [table],
            |row| row.get::<_, i64>(0),
        )
        .ok()
        .map(|value| value.max(0) as u64)
}

fn collect_sqlite_diagnostics(operation: &BenchmarkOperation) -> Result<Option<SqliteDiagnostics>> {
    let BenchmarkOperationKind::Sql { database, sql } = &operation.kind else {
        return Ok(None);
    };
    let connection = rusqlite::Connection::open(database)
        .with_context(|| format!("failed to open `{}`", database.display()))?;
    let query_plan = explain_query_plan(&connection, sql)?;
    let upper_plan = query_plan.join(" ").to_ascii_uppercase();
    Ok(Some(SqliteDiagnostics {
        database: database.clone(),
        database_size_bytes: path_bytes(database)?,
        page_count: pragma_u64(&connection, "page_count"),
        page_size: pragma_u64(&connection, "page_size"),
        journal_mode: pragma_string(&connection, "journal_mode"),
        query_plan,
        uses_full_scan: upper_plan.contains("SCAN "),
        uses_temporary_btree: upper_plan.contains("TEMP B-TREE"),
    }))
}

fn explain_query_plan(connection: &rusqlite::Connection, sql: &str) -> Result<Vec<String>> {
    let mut statement = connection
        .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
        .with_context(|| format!("failed to prepare SQLite query plan for `{sql}`"))?;
    let rows = statement.query_map([], |row| {
        let id: i64 = row.get(0)?;
        let parent: i64 = row.get(1)?;
        let detail: String = row.get(3)?;
        Ok(format!("{id}:{parent}: {detail}"))
    })?;
    Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
}

fn pragma_u64(connection: &rusqlite::Connection, name: &str) -> Option<u64> {
    connection
        .query_row(&format!("PRAGMA {name}"), [], |row| row.get::<_, i64>(0))
        .ok()
        .map(|value| value as u64)
}

fn pragma_string(connection: &rusqlite::Connection, name: &str) -> Option<String> {
    connection
        .query_row(&format!("PRAGMA {name}"), [], |row| row.get::<_, String>(0))
        .ok()
}

fn path_bytes(path: &Path) -> Result<u64> {
    if path.is_file() {
        return Ok(std::fs::metadata(path)?.len());
    }
    recursive_files(path)?
        .into_iter()
        .try_fold(0_u64, |bytes, file| {
            Ok(bytes + std::fs::metadata(file)?.len())
        })
}

fn process_rss_kib() -> Option<u64> {
    let pid = std::process::id().to_string();
    command_stdout("ps", &["-o", "rss=", "-p", &pid]).and_then(|value| value.trim().parse().ok())
}

#[cfg(target_os = "linux")]
fn process_peak_rss_kib() -> Option<u64> {
    process_peak_rss_raw()
}

#[cfg(all(unix, not(target_os = "linux")))]
fn process_peak_rss_kib() -> Option<u64> {
    process_peak_rss_raw().map(|bytes| bytes / 1024)
}

#[cfg(not(unix))]
fn process_peak_rss_kib() -> Option<u64> {
    None
}

#[cfg(unix)]
fn process_peak_rss_raw() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the provided rusage pointer on success.
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    (status == 0).then(|| unsafe { usage.assume_init().ru_maxrss as u64 })
}

fn process_cpu_seconds() -> Option<f64> {
    let pid = std::process::id().to_string();
    command_stdout("ps", &["-o", "time=", "-p", &pid])
        .and_then(|value| parse_process_time_seconds(value.trim()))
}

#[derive(Debug, Clone, Copy)]
struct ProcessIoBytes {
    read_bytes: u64,
    write_bytes: u64,
}

fn process_io_bytes() -> Option<ProcessIoBytes> {
    let io = std::fs::read_to_string("/proc/self/io").ok()?;
    let mut read_bytes = None;
    let mut write_bytes = None;
    for line in io.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let Ok(value) = value.trim().parse::<u64>() else {
            continue;
        };
        match key {
            "read_bytes" => read_bytes = Some(value),
            "write_bytes" => write_bytes = Some(value),
            _ => {}
        }
    }
    Some(ProcessIoBytes {
        read_bytes: read_bytes?,
        write_bytes: write_bytes?,
    })
}

fn parse_process_time_seconds(value: &str) -> Option<f64> {
    let (days, time) = if let Some((days, time)) = value.split_once('-') {
        (days.parse().ok()?, time)
    } else {
        (0_u64, value)
    };
    let parts = time.split(':').collect::<Vec<_>>();
    let seconds = match parts.as_slice() {
        [minutes, seconds] => minutes.parse::<u64>().ok()? * 60 + seconds.parse::<u64>().ok()?,
        [hours, minutes, seconds] => {
            hours.parse::<u64>().ok()? * 60 * 60
                + minutes.parse::<u64>().ok()? * 60
                + seconds.parse::<u64>().ok()?
        }
        _ => return None,
    };
    Some((days * 24 * 60 * 60 + seconds) as f64)
}

fn command_stdout(command: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(command).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!stdout.is_empty()).then_some(stdout)
}

fn git_commit(path: &Path) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(path)
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn benchmark_repo_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn facts_repo_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .join("sdk")
}

fn cpu_model() -> Option<String> {
    command_stdout("sysctl", &["-n", "machdep.cpu.brand_string"])
        .or_else(|| command_stdout("lscpu", &[]))
}

fn memory_bytes() -> Option<u64> {
    command_stdout("sysctl", &["-n", "hw.memsize"])
        .and_then(|value| value.parse().ok())
        .or_else(linux_memory_bytes)
}

fn linux_memory_bytes() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let total_kib = meminfo.lines().find_map(|line| {
        let rest = line.strip_prefix("MemTotal:")?.trim();
        rest.split_whitespace().next()?.parse::<u64>().ok()
    })?;
    total_kib.checked_mul(1024)
}

fn storage_type() -> Option<String> {
    command_stdout("diskutil", &["info", "."])
        .and_then(|output| {
            output.lines().find_map(|line| {
                let (key, value) = line.split_once(':')?;
                (key.trim() == "Solid State").then(|| {
                    if value.trim().eq_ignore_ascii_case("yes") {
                        "ssd".to_string()
                    } else {
                        "non-ssd".to_string()
                    }
                })
            })
        })
        .or_else(|| {
            command_stdout("df", &["-T", "."]).and_then(|output| {
                output
                    .lines()
                    .nth(1)
                    .and_then(|line| line.split_whitespace().nth(1))
                    .map(|value| format!("filesystem:{value}"))
            })
        })
}

fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    type ArtifactPathSelector = fn(&BenchmarkAcceptArgs) -> &PathBuf;

    #[test]
    fn timing_stats_record_percentiles() {
        let stats = TimingStats::from_samples(&[3.0, 1.0, 2.0, 4.0]);
        assert_eq!(stats.samples, 4);
        assert_eq!(stats.min_ms, Some(1.0));
        assert_eq!(stats.median_ms, Some(3.0));
        assert_eq!(stats.p95_ms, Some(4.0));
        assert_eq!(stats.max_ms, Some(4.0));
        assert_eq!(stats.outlier_count, 0);
    }

    #[test]
    fn timing_stats_record_outliers() {
        let stats = TimingStats::from_samples(&[10.0, 10.0, 10.0, 10.0, 50.0]);
        assert_eq!(stats.samples, 5);
        assert_eq!(stats.outlier_count, 1);
        assert_eq!(stats.outliers_ms, vec![50.0]);
    }

    #[test]
    fn benchmark_spec_exposes_contract() {
        let spec = benchmark_spec();
        assert_eq!(spec.schema_version, REPORT_SCHEMA_VERSION);
        assert_eq!(spec.levels.len(), 3);
        assert!(
            spec.levels
                .iter()
                .any(|level| { level.level == "large" && level.target_propositions == 500_000 })
        );
        assert_eq!(
            spec.required_profiles.len(),
            REQUIRED_BENCHMARK_PROFILES.len()
        );
        assert_eq!(spec.suites.len(), 10);
        assert!(spec.suites.iter().any(|suite| {
            suite.suite == BenchmarkSuite::Core
                && suite.slug == "core"
                && suite.required
                && suite.scope.contains("SDK write scenarios")
        }));
        assert!(spec.suites.iter().any(|suite| {
            suite.suite == BenchmarkSuite::Full
                && suite.slug == "full"
                && !suite.required
                && suite.scope == "All supported suites"
        }));
        assert!(!spec.required_suites.contains(&BenchmarkSuite::Full));
        assert!(spec.required_suites.contains(&BenchmarkSuite::Core));
        assert!(spec.required_suites.contains(&BenchmarkSuite::Http));
        assert!(spec.required_areas.contains(&"search".to_string()));
        assert!(spec.required_areas.contains(&"http".to_string()));
        assert!(
            spec.required_requirements
                .contains(&"object-validation".to_string())
        );
        assert!(
            spec.required_requirements
                .contains(&"snapshot-creation-loading".to_string())
        );
        assert!(
            spec.required_requirements
                .contains(&"http-local-loopback".to_string())
        );
        assert!(
            spec.required_representative_benchmarks
                .contains(&"core_effective_full_id_lookup_shape".to_string())
        );
        assert!(
            spec.required_representative_benchmarks
                .contains(&"integrity_commitment_verify_full_ledger".to_string())
        );
        assert!(
            spec.required_representative_benchmarks
                .contains(&"integrity_non_inclusion_proof_sampled".to_string())
        );
        assert!(spec.suite_coverage.iter().all(|coverage| {
            !coverage.requirements.is_empty() && !coverage.representative_benchmarks.is_empty()
        }));
        let covered_requirements = spec
            .suite_coverage
            .iter()
            .flat_map(|coverage| coverage.requirements.iter().cloned())
            .collect::<BTreeSet<_>>();
        assert!(
            spec.required_requirements
                .iter()
                .all(|requirement| { covered_requirements.contains(requirement) })
        );
        assert!(spec.report_fields.contains(&"raw_samples_ms".to_string()));
        assert!(spec.report_fields.contains(&"requirement_tags".to_string()));
        assert!(spec.report_fields.contains(&"cache_state".to_string()));
        assert!(
            spec.report_fields
                .contains(&"cache_classification".to_string())
        );
        assert!(spec.report_fields.contains(&"measured_bytes".to_string()));
        assert!(
            spec.report_fields
                .contains(&"network_payload_bytes".to_string())
        );
        assert!(
            spec.report_fields
                .contains(&"search_index_size_bytes".to_string())
        );
        assert!(spec.report_fields.contains(&"operating_system".to_string()));
        assert!(spec.report_fields.contains(&"architecture".to_string()));
        assert!(spec.report_fields.contains(&"cpu_model".to_string()));
        assert!(spec.report_fields.contains(&"core_count".to_string()));
        assert!(spec.report_fields.contains(&"memory_bytes".to_string()));
        assert!(spec.report_fields.contains(&"filesystem".to_string()));
        assert!(spec.report_fields.contains(&"storage_type".to_string()));
        assert!(spec.report_fields.contains(&"feature_flags".to_string()));
        assert!(spec.report_fields.contains(&"source_revisions".to_string()));
        assert!(
            spec.report_fields
                .contains(&"reproduction_metadata".to_string())
        );
        assert!(spec.report_fields.contains(&"preconditions".to_string()));
        assert!(spec.report_fields.contains(&"phase_breakdown".to_string()));
        assert!(spec.report_fields.contains(&"resource_deltas".to_string()));
        assert!(spec.report_fields.contains(&"peak_rss_kib".to_string()));
        assert!(spec.report_fields.contains(&"disk_read_bytes".to_string()));
        assert!(spec.report_fields.contains(&"disk_write_bytes".to_string()));
        assert!(spec.report_fields.contains(&"actor_count".to_string()));
        assert!(spec.report_fields.contains(&"ledger_count".to_string()));
        assert!(spec.report_fields.contains(&"replica_count".to_string()));
        assert!(
            spec.report_fields
                .contains(&"sqlite_database_paths".to_string())
        );
        assert!(
            spec.report_fields
                .contains(&"isolation_strategy".to_string())
        );
        assert!(spec.cache_labels.contains(&"warm-filesystem".to_string()));
        assert!(
            spec.required_commands
                .contains(&"benchmark audit".to_string())
        );
        assert!(
            spec.required_commands
                .contains(&"benchmark accept".to_string())
        );
        assert!(spec.readiness_gates.iter().any(|gate| {
            gate.contains("accepted baseline artifacts")
                && gate.contains("audit")
                && gate.contains("growth")
                && gate.contains("budget")
                && gate.contains("profiling")
        }));
        assert!(spec.readiness_gates.iter().any(|gate| {
            gate.contains("cold")
                && gate.contains("warm")
                && gate.contains("every required profile and level")
        }));
        assert!(spec.readiness_gates.iter().any(|gate| {
            gate.contains("failure logs")
                && gate.contains("present")
                && gate.contains("internally consistent")
        }));
        assert!(spec.readiness_gates.iter().any(|gate| {
            gate.contains("failure logs") && gate.contains("benchmark-level failures")
        }));
        assert!(
            spec.readiness_gates.iter().any(|gate| {
                gate.contains("fixture scale") && gate.contains("topology metadata")
            })
        );
        assert!(
            spec.readiness_gates
                .iter()
                .any(|gate| { gate.contains("search-index size") })
        );
        assert!(
            spec.readiness_gates
                .iter()
                .any(|gate| { gate.contains("SQLite diagnostics") })
        );
        assert!(
            spec.readiness_gates
                .iter()
                .any(|gate| { gate.contains("timing statistics") && gate.contains("raw samples") })
        );
        assert!(
            spec.readiness_gates
                .iter()
                .any(|gate| gate.contains("at least two measured samples"))
        );
        assert!(spec.readiness_gates.iter().any(|gate| {
            gate.contains("cache labels") && gate.contains("baseline cache state")
        }));
        assert!(
            spec.readiness_gates
                .iter()
                .any(|gate| { gate.contains("warm-cache") && gate.contains("warmup iteration") })
        );
        assert!(spec.required_cli_workflows.contains(&"push".to_string()));
        assert!(spec.required_cli_workflows.contains(&"pull".to_string()));
    }

    #[test]
    fn benchmark_audit_requires_http_router_payload_metadata() {
        let mut benchmark = benchmark_result(
            "http_local_loopback_ledger_list",
            BenchmarkSuite::Http,
            "http",
        );
        benchmark.diagnostics.operation_kind = "http-router".to_string();
        benchmark.measured_bytes = Some(128);
        benchmark.network_payload_bytes = Some(128);
        assert!(!missing_expected_byte_metadata(&benchmark));

        benchmark.network_payload_bytes = None;
        assert!(missing_expected_byte_metadata(&benchmark));

        benchmark.network_payload_bytes = Some(128);
        benchmark.measured_bytes = None;
        assert!(missing_expected_byte_metadata(&benchmark));
    }

    #[test]
    fn benchmark_audit_requires_transferable_file_inventory_payload_metadata() {
        let mut benchmark = benchmark_result(
            "http_local_loopback_bundle_payload_inventory",
            BenchmarkSuite::Http,
            "http",
        );
        benchmark.diagnostics.operation_kind = "file-inventory".to_string();
        benchmark.measured_bytes = Some(256);
        benchmark.network_payload_bytes = Some(256);
        assert!(!missing_expected_byte_metadata(&benchmark));

        benchmark.network_payload_bytes = None;
        assert!(missing_expected_byte_metadata(&benchmark));
    }

    #[test]
    fn benchmark_audit_allows_artifact_file_inventory_without_network_payload_metadata() {
        let mut benchmark = benchmark_result(
            "integrity_snapshot_inventory_metadata",
            BenchmarkSuite::Integrity,
            "integrity",
        );
        benchmark.diagnostics.operation_kind = "file-inventory".to_string();
        benchmark.measured_bytes = Some(256);
        benchmark.network_payload_bytes = None;
        assert!(!missing_expected_byte_metadata(&benchmark));
    }

    #[test]
    fn benchmark_audit_requires_write_benchmark_temporary_isolation_metadata() {
        let mut benchmark = benchmark_result(
            "core_sdk_propose_temp",
            BenchmarkSuite::Core,
            "proposition-create",
        );
        benchmark.read_only = false;
        benchmark.isolation_strategy =
            "write benchmark runs against the selected fixture".to_string();
        assert_eq!(
            invalid_isolation_strategy_for_benchmark(&benchmark),
            Some("write-benchmark-without-temporary-state".to_string())
        );

        benchmark.isolation_strategy =
            "write benchmark uses a fresh temporary workspace per sample".to_string();
        assert_eq!(invalid_isolation_strategy_for_benchmark(&benchmark), None);
    }

    #[test]
    fn benchmark_audit_rejects_inconsistent_read_only_metadata() {
        let mut write_benchmark = benchmark_result(
            "core_sdk_propose_temp",
            BenchmarkSuite::Core,
            "proposition-create",
        );
        write_benchmark.diagnostics.operation_kind = "scenario".to_string();
        write_benchmark.read_only = true;
        assert_eq!(
            invalid_read_only_metadata_for_benchmark(&write_benchmark),
            Some("write-capable-operation-marked-read-only".to_string())
        );

        write_benchmark.read_only = false;
        assert_eq!(
            invalid_read_only_metadata_for_benchmark(&write_benchmark),
            None
        );

        let mut read_benchmark =
            benchmark_result("read_list_default_accepted", BenchmarkSuite::Read, "read");
        read_benchmark.diagnostics.operation_kind = "sql".to_string();
        read_benchmark.read_only = false;
        assert_eq!(
            invalid_read_only_metadata_for_benchmark(&read_benchmark),
            Some("read-only-operation-marked-write".to_string())
        );

        read_benchmark.read_only = true;
        assert_eq!(
            invalid_read_only_metadata_for_benchmark(&read_benchmark),
            None
        );
    }

    #[test]
    fn benchmark_audit_metadata_reasons_name_individual_gaps() {
        let mut benchmark = benchmark_result(
            "http_local_loopback_ledger_list",
            BenchmarkSuite::Http,
            "http",
        );
        benchmark.diagnostics.operation_kind = "http-router".to_string();
        benchmark.preconditions.clear();
        benchmark.cache_classification = None;
        benchmark.network_payload_bytes = None;

        let reasons = missing_benchmark_metadata_reasons(&benchmark);
        assert!(reasons.contains(&"missing-preconditions"));
        assert!(reasons.contains(&"missing-cache-classification"));
        assert!(reasons.contains(&"missing-byte-metadata"));
    }

    #[test]
    fn process_time_parser_accepts_ps_formats() {
        assert_eq!(parse_process_time_seconds("01:02"), Some(62.0));
        assert_eq!(parse_process_time_seconds("03:01:02"), Some(10862.0));
        assert_eq!(parse_process_time_seconds("2-03:01:02"), Some(183662.0));
        assert_eq!(parse_process_time_seconds("not-time"), None);
    }

    #[test]
    fn cache_labels_have_structured_classifications() {
        let warm = cache_classification("warm-filesystem").expect("warm label should parse");
        assert_eq!(warm.temperature, CacheTemperature::Warm);
        assert_eq!(warm.scope, CacheScope::Filesystem);
        let first = cache_classification("first-request").expect("request label should parse");
        assert_eq!(first.temperature, CacheTemperature::First);
        assert_eq!(first.scope, CacheScope::Request);
        assert!(cache_classification("unknown-cache").is_none());
    }

    #[test]
    fn benchmark_run_reports_fixture_metadata() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture = temp.path().join("fixture");
        std::fs::create_dir(&fixture)?;
        std::fs::write(
            fixture.join("manifest.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "profile": "test-profile",
                "seed": 7
            }))?,
        )?;
        let database = fixture.join("ledger.sqlite");
        let connection = rusqlite::Connection::open(&database)?;
        connection.execute_batch(
            "CREATE TABLE protocol_object(object_id BLOB PRIMARY KEY, object_type TEXT NOT NULL, content_hash BLOB NOT NULL, payload BLOB NOT NULL, cose BLOB);
             CREATE TABLE projected_effective(proposition_id BLOB PRIMARY KEY, status TEXT NOT NULL, revision_id BLOB);
             CREATE TABLE projected_revision(proposition_id BLOB NOT NULL, revision_id BLOB NOT NULL, parent_revision_id BLOB);
             CREATE TABLE projected_pending(pending_id BLOB PRIMARY KEY);
             CREATE TABLE object_dependency(object_id BLOB, dependency_id BLOB);
             INSERT INTO protocol_object(object_id,object_type,content_hash,payload,cose) VALUES(x'01','proposition',x'aa','policy',x'bb');
             INSERT INTO projected_effective(proposition_id,status,revision_id) VALUES(x'01','accepted',x'02');
             INSERT INTO projected_revision(proposition_id,revision_id,parent_revision_id) VALUES(x'01',x'02',NULL);",
        )?;

        let report = run_benchmarks(&BenchmarkRunArgs {
            suite: BenchmarkSuite::Read,
            fixture,
            output: None,
            iterations: 2,
            warmups: 0,
            cache_state: "warm-test".to_string(),
        })?;
        assert_eq!(report.fixture.profile, "test-profile");
        assert_eq!(report.fixture.proposition_count, 1);
        assert!(report.fixture.simulator_revision.is_some());
        assert!(report.fixture.facts_sdk_revision.is_some());
        assert!(report.fixture.facts_implementation_revision.is_some());
        assert!(
            report
                .benchmarks
                .iter()
                .all(|benchmark| benchmark.correctness_passed)
        );
        assert!(
            report
                .benchmarks
                .iter()
                .all(|benchmark| benchmark.phase_timings.measurement_total_ms > 0.0)
        );
        assert!(report.benchmarks.iter().all(|benchmark| {
            benchmark.sample_observations.len() == benchmark.samples_ms.len()
                && benchmark
                    .sample_observations
                    .iter()
                    .enumerate()
                    .all(|(index, observation)| observation.sample_index == index)
        }));
        let pending_count = report
            .benchmarks
            .iter()
            .find(|benchmark| benchmark.name == "read_pending_action_count")
            .context("missing pending-action benchmark")?;
        let sqlite = pending_count
            .diagnostics
            .sqlite
            .as_ref()
            .context("missing SQLite diagnostics")?;
        assert!(!sqlite.query_plan.is_empty());
        assert_eq!(sqlite.database, database);
        Ok(())
    }

    #[test]
    fn integrity_suite_runs_sdk_commitment_and_proof_benchmarks() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture = temp.path().join("fixture");
        std::fs::create_dir(&fixture)?;
        std::fs::write(
            fixture.join("manifest.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "profile": "test-profile",
                "seed": 7
            }))?,
        )?;
        let database = fixture.join("ledger.sqlite");
        let connection = rusqlite::Connection::open(&database)?;
        connection.execute_batch(
            "
            CREATE TABLE protocol_object(object_id BLOB PRIMARY KEY, object_type TEXT NOT NULL, content_hash BLOB NOT NULL, payload BLOB NOT NULL, cose BLOB);
            CREATE TABLE object_dependency(object_id BLOB, dependency_id BLOB, content_hash BLOB, role TEXT);
            ",
        )?;
        let mut type_counts = BTreeMap::<String, u64>::new();
        for index in 0_u8..3 {
            let payload = valid_actor_payload(index)?;
            let hash = fact_core::Hash::digest(&payload);
            let cose = signed_actor_cose(&payload)?;
            connection.execute(
                "INSERT INTO protocol_object(object_id,object_type,content_hash,payload,cose) VALUES(?1,'proposition',?2,?3,?4)",
                rusqlite::params![
                    vec![index],
                    hash.as_bytes().to_vec(),
                    payload,
                    cose,
                ],
            )?;
            *type_counts.entry("proposition".to_string()).or_default() += 1;
        }
        connection.execute(
            "INSERT INTO object_dependency(object_id,dependency_id,content_hash,role) VALUES(x'01',x'02',x'03','test')",
            [],
        )?;
        let snapshots = fixture.join("snapshots");
        std::fs::create_dir(&snapshots)?;
        std::fs::write(
            snapshots.join("object-set.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "version": 1,
                "profile": "test-profile",
                "seed": 7,
                "snapshot_kind": "portable-object-bundle-inventory",
                "bundle": fixture.join("bundles").join("objects.factbndl"),
                "bundle_object_count": 3,
                "portable_bundle_count": 1,
                "portable_bundles": [{"path": "bundles/objects.factbndl", "object_count": 3}],
                "unique_object_count": 3,
                "commitment_root": fact_core::Hash::digest(b"root").hex(),
                "database_snapshots": [{
                    "replica": "test",
                    "database": database,
                    "object_count": 3,
                    "hash_count": 3,
                    "object_counts_by_type": type_counts
                }]
            }))?,
        )?;

        let report = run_benchmarks(&BenchmarkRunArgs {
            suite: BenchmarkSuite::Integrity,
            fixture,
            output: None,
            iterations: 1,
            warmups: 0,
            cache_state: "warm-test".to_string(),
        })?;
        for name in [
            "integrity_commitment_create_full_ledger",
            "integrity_commitment_verify_full_ledger",
            "integrity_inclusion_proof_sampled",
            "integrity_non_inclusion_proof_sampled",
            "integrity_validate_protocol_payloads",
            "integrity_invalid_object_rejection_sampled",
            "integrity_dependency_validation_shape",
            "integrity_authorization_validation_shape",
            "integrity_batch_validation_payload_shape",
            "integrity_snapshot_frame_encode_full_ledger",
            "integrity_snapshot_frame_decode_full_ledger",
            "integrity_snapshot_sidecar_verify",
        ] {
            let benchmark = report
                .benchmarks
                .iter()
                .find(|benchmark| benchmark.name == name)
                .with_context(|| format!("missing `{name}`"))?;
            assert!(
                benchmark.correctness_passed,
                "{} failed: {:?}",
                benchmark.name, benchmark.failures
            );
            assert!(
                [
                    "commitment",
                    "validation",
                    "sql",
                    "snapshot-frame",
                    "snapshot-sidecar"
                ]
                .contains(&benchmark.diagnostics.operation_kind.as_str())
            );
            if benchmark.name.contains("snapshot") {
                assert!(benchmark.measured_bytes.is_some_and(|bytes| bytes > 0));
            }
        }
        Ok(())
    }

    #[test]
    fn benchmark_comparison_classifies_regressions() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let baseline = temp.path().join("baseline.json");
        let current = temp.path().join("current.json");
        std::fs::write(
            &baseline,
            serde_json::to_vec_pretty(&report_with_median(10.0))?,
        )?;
        std::fs::write(
            &current,
            serde_json::to_vec_pretty(&report_with_median(12.0))?,
        )?;
        let comparison = compare_reports(&BenchmarkCompareArgs {
            baseline,
            current,
            thresholds: None,
            warning_threshold_percent: 5.0,
            regression_threshold_percent: 15.0,
        })?;
        assert_eq!(comparison.regressions.len(), 1);
        assert!(comparison.regression_decision_ready);
        assert!(comparison.regression_decision_blockers.is_empty());
        Ok(())
    }

    #[test]
    fn benchmark_comparison_reports_observed_noise_estimate() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let baseline = temp.path().join("baseline.json");
        let current = temp.path().join("current.json");
        std::fs::write(
            &baseline,
            serde_json::to_vec_pretty(&report_with_samples(vec![9.0, 10.0, 11.0]))?,
        )?;
        std::fs::write(
            &current,
            serde_json::to_vec_pretty(&report_with_samples(vec![11.0, 12.0, 13.0]))?,
        )?;
        let comparison = compare_reports(&BenchmarkCompareArgs {
            baseline,
            current,
            thresholds: None,
            warning_threshold_percent: 5.0,
            regression_threshold_percent: 15.0,
        })?;
        assert_eq!(comparison.regressions.len(), 1);
        let noise = &comparison.regressions[0].noise;
        assert!(noise.supported);
        assert_eq!(noise.baseline_samples, 3);
        assert_eq!(noise.current_samples, 3);
        assert!(
            noise
                .noise_threshold_percent
                .is_some_and(|value| value > 0.0)
        );
        assert_eq!(noise.difference_exceeds_noise_threshold, Some(true));
        Ok(())
    }

    #[test]
    fn benchmark_comparison_reports_environment_differences() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let baseline = temp.path().join("baseline.json");
        let current = temp.path().join("current.json");
        let mut current_report = report_with_median(10.0);
        current_report.environment.architecture = "different-arch".to_string();
        current_report.environment.cpu_model = Some("different-cpu".to_string());
        current_report.environment.core_count = Some(2);
        current_report.environment.memory_bytes = Some(2_147_483_648);
        current_report.environment.filesystem = Some("different-fs".to_string());
        current_report.environment.storage_type = Some("different-storage".to_string());
        current_report.environment.build_profile = "debug".to_string();
        current_report.environment.feature_flags = vec!["benchmark-feature".to_string()];
        current_report.environment.benchmark_project_commit =
            Some("different-benchmark-revision".to_string());
        std::fs::write(
            &baseline,
            serde_json::to_vec_pretty(&report_with_median(10.0))?,
        )?;
        std::fs::write(&current, serde_json::to_vec_pretty(&current_report)?)?;
        let comparison = compare_reports(&BenchmarkCompareArgs {
            baseline,
            current,
            thresholds: None,
            warning_threshold_percent: 5.0,
            regression_threshold_percent: 15.0,
        })?;
        assert!(!comparison.environment_compatible);
        assert!(!comparison.regression_decision_ready);
        assert!(
            comparison
                .regression_decision_blockers
                .contains(&"environment-differences".to_string())
        );
        assert!(
            comparison
                .environment_differences
                .iter()
                .any(|difference| difference.field == "architecture")
        );
        assert!(
            comparison
                .environment_differences
                .iter()
                .any(|difference| difference.field == "cpu_model")
        );
        assert!(
            comparison
                .environment_differences
                .iter()
                .any(|difference| difference.field == "core_count")
        );
        assert!(
            comparison
                .environment_differences
                .iter()
                .any(|difference| difference.field == "memory_bytes")
        );
        assert!(
            comparison
                .environment_differences
                .iter()
                .any(|difference| difference.field == "filesystem")
        );
        assert!(
            comparison
                .environment_differences
                .iter()
                .any(|difference| difference.field == "storage_type")
        );
        assert!(
            comparison
                .environment_differences
                .iter()
                .any(|difference| difference.field == "build_profile")
        );
        assert!(
            comparison
                .environment_differences
                .iter()
                .any(|difference| difference.field == "feature_flags")
        );
        assert!(
            comparison
                .environment_differences
                .iter()
                .any(|difference| difference.field == "benchmark_project_commit")
        );
        Ok(())
    }

    #[test]
    fn benchmark_comparison_reports_cache_differences() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let baseline = temp.path().join("baseline.json");
        let current = temp.path().join("current.json");
        let mut current_report = report_with_median(10.0);
        current_report.benchmarks[0].cache_state = "cold-filesystem".to_string();
        current_report.benchmarks[0].cache_classification = cache_classification("cold-filesystem");
        std::fs::write(
            &baseline,
            serde_json::to_vec_pretty(&report_with_median(10.0))?,
        )?;
        std::fs::write(&current, serde_json::to_vec_pretty(&current_report)?)?;

        let comparison = compare_reports(&BenchmarkCompareArgs {
            baseline,
            current,
            thresholds: None,
            warning_threshold_percent: 5.0,
            regression_threshold_percent: 15.0,
        })?;
        assert!(!comparison.cache_compatible);
        assert!(!comparison.regression_decision_ready);
        assert!(
            comparison
                .regression_decision_blockers
                .contains(&"cache-differences".to_string())
        );
        assert_eq!(comparison.cache_differences.len(), 1);
        assert_eq!(comparison.cache_differences[0].benchmark, "bench");
        assert_eq!(
            comparison.cache_differences[0].baseline_cache_state,
            "warm-filesystem"
        );
        assert_eq!(
            comparison.cache_differences[0].current_cache_state,
            "cold-filesystem"
        );
        Ok(())
    }

    #[test]
    fn benchmark_comparison_reports_fixture_differences() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let baseline = temp.path().join("baseline.json");
        let current = temp.path().join("current.json");
        let mut current_report = report_with_median(10.0);
        current_report.fixture.proposition_count = 500_000;
        current_report.fixture.total_object_count = 2_000_000;
        current_report.fixture.database_size_bytes = 99_999;
        current_report.fixture.facts_sdk_revision = Some("different-sdk-fixture".to_string());
        std::fs::write(
            &baseline,
            serde_json::to_vec_pretty(&report_with_median(10.0))?,
        )?;
        std::fs::write(&current, serde_json::to_vec_pretty(&current_report)?)?;

        let comparison = compare_reports(&BenchmarkCompareArgs {
            baseline,
            current,
            thresholds: None,
            warning_threshold_percent: 5.0,
            regression_threshold_percent: 15.0,
        })?;

        assert!(!comparison.fixture_compatible);
        assert!(!comparison.regression_decision_ready);
        assert!(
            comparison
                .regression_decision_blockers
                .contains(&"fixture-differences".to_string())
        );
        for field in [
            "proposition_count",
            "total_object_count",
            "database_size_bytes",
            "facts_sdk_revision",
        ] {
            assert!(
                comparison
                    .fixture_differences
                    .iter()
                    .any(|difference| difference.field == field),
                "missing fixture difference for {field}"
            );
        }
        Ok(())
    }

    #[test]
    fn benchmark_comparison_reports_faster_incorrect_current_results() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let baseline = temp.path().join("baseline.json");
        let current = temp.path().join("current.json");
        let mut current_report = report_with_median(5.0);
        current_report.benchmarks[0].correctness_passed = false;
        current_report.benchmarks[0]
            .failures
            .push("stale result".to_string());
        std::fs::write(
            &baseline,
            serde_json::to_vec_pretty(&report_with_median(10.0))?,
        )?;
        std::fs::write(&current, serde_json::to_vec_pretty(&current_report)?)?;

        let comparison = compare_reports(&BenchmarkCompareArgs {
            baseline,
            current,
            thresholds: None,
            warning_threshold_percent: 5.0,
            regression_threshold_percent: 15.0,
        })?;

        assert_eq!(comparison.improvements.len(), 0);
        assert!(!comparison.regression_decision_ready);
        assert!(
            comparison
                .regression_decision_blockers
                .contains(&"incorrect-current-benchmarks".to_string())
        );
        assert_eq!(comparison.incorrect_current_benchmarks.len(), 1);
        assert_eq!(
            comparison.incorrect_current_benchmarks[0].benchmark,
            "bench"
        );
        assert!(
            comparison.incorrect_current_benchmarks[0]
                .failures
                .contains(&"stale result".to_string())
        );
        Ok(())
    }

    #[test]
    fn benchmark_comparison_uses_per_benchmark_threshold_file() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let baseline = temp.path().join("baseline.json");
        let current = temp.path().join("current.json");
        let thresholds = temp.path().join("thresholds.json");
        std::fs::write(
            &baseline,
            serde_json::to_vec_pretty(&report_with_median(10.0))?,
        )?;
        std::fs::write(
            &current,
            serde_json::to_vec_pretty(&report_with_median(12.0))?,
        )?;
        std::fs::write(
            &thresholds,
            serde_json::to_vec_pretty(&serde_json::json!({
                "default": {
                    "warning_percent": 5.0,
                    "regression_percent": 25.0
                },
                "benchmarks": {
                    "bench": {
                        "warning_percent": 3.0,
                        "regression_percent": 30.0
                    }
                }
            }))?,
        )?;
        let comparison = compare_reports(&BenchmarkCompareArgs {
            baseline,
            current,
            thresholds: Some(thresholds),
            warning_threshold_percent: 5.0,
            regression_threshold_percent: 15.0,
        })?;
        assert_eq!(comparison.regressions.len(), 0);
        assert_eq!(comparison.warnings.len(), 1);
        assert_eq!(comparison.warning_threshold_percent, 5.0);
        assert_eq!(comparison.regression_threshold_percent, 25.0);
        assert_eq!(comparison.warnings[0].warning_threshold_percent, 3.0);
        assert_eq!(comparison.warnings[0].regression_threshold_percent, 30.0);
        Ok(())
    }

    #[test]
    fn benchmark_matrix_comparison_rolls_up_report_pairs() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let baseline_report = temp.path().join("baseline-report.json");
        let current_report = temp.path().join("current-report.json");
        std::fs::write(
            &baseline_report,
            serde_json::to_vec_pretty(&report_with_median(10.0))?,
        )?;
        std::fs::write(
            &current_report,
            serde_json::to_vec_pretty(&report_with_median(12.0))?,
        )?;
        let baseline_summary = temp.path().join("baseline-summary.json");
        let current_summary = temp.path().join("current-summary.json");
        std::fs::write(
            &baseline_summary,
            serde_json::to_vec_pretty(&summary_with_report(&baseline_report))?,
        )?;
        std::fs::write(
            &current_summary,
            serde_json::to_vec_pretty(&summary_with_report(&current_report))?,
        )?;

        let comparison = compare_matrix_reports(&BenchmarkCompareMatrixArgs {
            baseline_summary,
            current_summary,
            thresholds: None,
            warning_threshold_percent: 5.0,
            regression_threshold_percent: 15.0,
        })?;
        assert_eq!(comparison.compared_reports.len(), 1);
        assert_eq!(comparison.total_regressions, 1);
        assert!(comparison.regression_decision_ready);
        assert!(comparison.regression_decision_blockers.is_empty());
        assert!(comparison.compared_reports[0].regression_decision_ready);
        assert_eq!(comparison.total_environment_incompatible_reports, 0);
        assert_eq!(comparison.total_environment_differences, 0);
        assert_eq!(comparison.total_cache_incompatible_reports, 0);
        assert_eq!(comparison.total_fixture_incompatible_reports, 0);
        assert_eq!(comparison.total_fixture_differences, 0);
        assert_eq!(comparison.total_cache_differences, 0);
        assert_eq!(comparison.failures.len(), 0);
        Ok(())
    }

    #[test]
    fn benchmark_matrix_comparison_rolls_up_environment_differences() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let baseline_report = temp.path().join("baseline-report.json");
        let current_report = temp.path().join("current-report.json");
        let mut current = report_with_median(10.0);
        current.environment.cpu_model = Some("different-cpu".to_string());
        current.environment.feature_flags = vec!["benchmark-feature".to_string()];
        std::fs::write(
            &baseline_report,
            serde_json::to_vec_pretty(&report_with_median(10.0))?,
        )?;
        std::fs::write(&current_report, serde_json::to_vec_pretty(&current)?)?;
        let baseline_summary = temp.path().join("baseline-summary.json");
        let current_summary = temp.path().join("current-summary.json");
        std::fs::write(
            &baseline_summary,
            serde_json::to_vec_pretty(&summary_with_report(&baseline_report))?,
        )?;
        std::fs::write(
            &current_summary,
            serde_json::to_vec_pretty(&summary_with_report(&current_report))?,
        )?;

        let comparison = compare_matrix_reports(&BenchmarkCompareMatrixArgs {
            baseline_summary,
            current_summary,
            thresholds: None,
            warning_threshold_percent: 5.0,
            regression_threshold_percent: 15.0,
        })?;

        assert_eq!(comparison.compared_reports.len(), 1);
        assert_eq!(comparison.total_environment_incompatible_reports, 1);
        assert_eq!(comparison.total_environment_differences, 2);
        assert!(!comparison.regression_decision_ready);
        assert!(
            comparison
                .regression_decision_blockers
                .contains(&"report-comparison-blockers".to_string())
        );
        assert!(!comparison.compared_reports[0].environment_compatible);
        assert_eq!(
            comparison.compared_reports[0].environment_difference_count,
            2
        );
        assert!(!comparison.compared_reports[0].regression_decision_ready);
        assert!(
            comparison.compared_reports[0]
                .regression_decision_blockers
                .contains(&"environment-differences".to_string())
        );
        Ok(())
    }

    #[test]
    fn benchmark_matrix_comparison_rolls_up_cache_differences() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let baseline_report = temp.path().join("baseline-report.json");
        let current_report = temp.path().join("current-report.json");
        let mut current = report_with_median(10.0);
        current.benchmarks[0].cache_state = "cold-filesystem".to_string();
        current.benchmarks[0].cache_classification = cache_classification("cold-filesystem");
        std::fs::write(
            &baseline_report,
            serde_json::to_vec_pretty(&report_with_median(10.0))?,
        )?;
        std::fs::write(&current_report, serde_json::to_vec_pretty(&current)?)?;
        let baseline_summary = temp.path().join("baseline-summary.json");
        let current_summary = temp.path().join("current-summary.json");
        std::fs::write(
            &baseline_summary,
            serde_json::to_vec_pretty(&summary_with_report(&baseline_report))?,
        )?;
        std::fs::write(
            &current_summary,
            serde_json::to_vec_pretty(&summary_with_report(&current_report))?,
        )?;

        let comparison = compare_matrix_reports(&BenchmarkCompareMatrixArgs {
            baseline_summary,
            current_summary,
            thresholds: None,
            warning_threshold_percent: 5.0,
            regression_threshold_percent: 15.0,
        })?;
        assert_eq!(comparison.compared_reports.len(), 1);
        assert_eq!(comparison.total_cache_incompatible_reports, 1);
        assert_eq!(comparison.total_cache_differences, 1);
        assert!(!comparison.regression_decision_ready);
        assert!(
            comparison
                .regression_decision_blockers
                .contains(&"report-comparison-blockers".to_string())
        );
        assert!(!comparison.compared_reports[0].cache_compatible);
        assert!(!comparison.compared_reports[0].regression_decision_ready);
        assert_eq!(comparison.compared_reports[0].cache_difference_count, 1);
        assert!(
            comparison.compared_reports[0]
                .regression_decision_blockers
                .contains(&"cache-differences".to_string())
        );
        Ok(())
    }

    #[test]
    fn benchmark_matrix_comparison_rolls_up_fixture_differences() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let baseline_report = temp.path().join("baseline-report.json");
        let current_report = temp.path().join("current-report.json");
        let mut current = report_with_median(10.0);
        current.fixture.proposition_count = 500_000;
        std::fs::write(
            &baseline_report,
            serde_json::to_vec_pretty(&report_with_median(10.0))?,
        )?;
        std::fs::write(&current_report, serde_json::to_vec_pretty(&current)?)?;
        let baseline_summary = temp.path().join("baseline-summary.json");
        let current_summary = temp.path().join("current-summary.json");
        std::fs::write(
            &baseline_summary,
            serde_json::to_vec_pretty(&summary_with_report(&baseline_report))?,
        )?;
        std::fs::write(
            &current_summary,
            serde_json::to_vec_pretty(&summary_with_report(&current_report))?,
        )?;

        let comparison = compare_matrix_reports(&BenchmarkCompareMatrixArgs {
            baseline_summary,
            current_summary,
            thresholds: None,
            warning_threshold_percent: 5.0,
            regression_threshold_percent: 15.0,
        })?;

        assert_eq!(comparison.compared_reports.len(), 1);
        assert_eq!(comparison.total_fixture_incompatible_reports, 1);
        assert_eq!(comparison.total_fixture_differences, 1);
        assert!(!comparison.regression_decision_ready);
        assert!(
            comparison
                .regression_decision_blockers
                .contains(&"report-comparison-blockers".to_string())
        );
        assert!(!comparison.compared_reports[0].fixture_compatible);
        assert_eq!(comparison.compared_reports[0].fixture_difference_count, 1);
        assert!(!comparison.compared_reports[0].regression_decision_ready);
        assert!(
            comparison.compared_reports[0]
                .regression_decision_blockers
                .contains(&"fixture-differences".to_string())
        );
        assert!(
            comparison.compared_reports[0]
                .comparison
                .fixture_differences
                .iter()
                .any(|difference| difference.field == "proposition_count")
        );
        Ok(())
    }

    #[test]
    fn benchmark_matrix_comparison_requires_ready_failure_free_summaries() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let baseline_report = temp.path().join("baseline-report.json");
        let current_report = temp.path().join("current-report.json");
        std::fs::write(
            &baseline_report,
            serde_json::to_vec_pretty(&report_with_median(10.0))?,
        )?;
        std::fs::write(
            &current_report,
            serde_json::to_vec_pretty(&report_with_median(10.0))?,
        )?;
        let baseline_summary = temp.path().join("baseline-summary.json");
        let current_summary = temp.path().join("current-summary.json");
        let mut baseline = summary_with_report(&baseline_report);
        baseline.ready = false;
        baseline.failures.push(BenchmarkBaselineFailure {
            fixture: PathBuf::from("fixture"),
            profile: "scale-500k-balanced".to_string(),
            level: "small".to_string(),
            seed: Some(42),
            error: "baseline fixture failure".to_string(),
        });
        let mut current = summary_with_report(&current_report);
        current.ready = false;
        current.failures.push(BenchmarkBaselineFailure {
            fixture: PathBuf::from("fixture"),
            profile: "scale-500k-balanced".to_string(),
            level: "small".to_string(),
            seed: Some(42),
            error: "current fixture failure".to_string(),
        });
        std::fs::write(&baseline_summary, serde_json::to_vec_pretty(&baseline)?)?;
        std::fs::write(&current_summary, serde_json::to_vec_pretty(&current)?)?;

        let comparison = compare_matrix_reports(&BenchmarkCompareMatrixArgs {
            baseline_summary,
            current_summary,
            thresholds: None,
            warning_threshold_percent: 5.0,
            regression_threshold_percent: 15.0,
        })?;

        assert!(!comparison.baseline_summary_ready);
        assert!(!comparison.current_summary_ready);
        assert_eq!(comparison.baseline_failure_count, 1);
        assert_eq!(comparison.current_failure_count, 1);
        assert!(!comparison.regression_decision_ready);
        for blocker in [
            "baseline-summary-not-ready",
            "current-summary-not-ready",
            "baseline-summary-failures",
            "current-summary-failures",
        ] {
            assert!(
                comparison
                    .regression_decision_blockers
                    .contains(&blocker.to_string()),
                "missing blocker {blocker}"
            );
        }
        Ok(())
    }

    #[test]
    fn benchmark_matrix_comparison_rolls_up_incorrect_current_benchmarks() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let baseline_report = temp.path().join("baseline-report.json");
        let current_report = temp.path().join("current-report.json");
        let mut current = report_with_median(5.0);
        current.benchmarks[0].correctness_passed = false;
        current.benchmarks[0]
            .failures
            .push("fast stale result".to_string());
        std::fs::write(
            &baseline_report,
            serde_json::to_vec_pretty(&report_with_median(10.0))?,
        )?;
        std::fs::write(&current_report, serde_json::to_vec_pretty(&current)?)?;
        let baseline_summary = temp.path().join("baseline-summary.json");
        let current_summary = temp.path().join("current-summary.json");
        std::fs::write(
            &baseline_summary,
            serde_json::to_vec_pretty(&summary_with_report(&baseline_report))?,
        )?;
        std::fs::write(
            &current_summary,
            serde_json::to_vec_pretty(&summary_with_report(&current_report))?,
        )?;

        let comparison = compare_matrix_reports(&BenchmarkCompareMatrixArgs {
            baseline_summary,
            current_summary,
            thresholds: None,
            warning_threshold_percent: 5.0,
            regression_threshold_percent: 15.0,
        })?;

        assert_eq!(comparison.total_improvements, 0);
        assert_eq!(comparison.total_incorrect_current_benchmarks, 1);
        assert!(!comparison.regression_decision_ready);
        assert!(
            comparison
                .regression_decision_blockers
                .contains(&"report-comparison-blockers".to_string())
        );
        assert_eq!(comparison.compared_reports[0].incorrect_current_count, 1);
        assert!(!comparison.compared_reports[0].regression_decision_ready);
        Ok(())
    }

    #[test]
    fn benchmark_fixture_inventory_classifies_required_levels() -> Result<()> {
        let temp = tempfile::tempdir()?;
        write_inventory_fixture(temp.path(), "small", 10_000)?;
        let benchmark_matrix = temp.path().join("benchmark-matrix");
        std::fs::create_dir(&benchmark_matrix)?;
        write_inventory_fixture(&benchmark_matrix, "medium", 100_000)?;
        std::fs::create_dir(benchmark_matrix.join("medium.progress"))?;
        let inventory = benchmark_fixture_inventory(temp.path(), false)?;
        assert_eq!(inventory.fixtures.len(), 2);
        assert_eq!(inventory.levels.get("small"), Some(&1));
        assert_eq!(inventory.levels.get("medium"), Some(&1));
        assert!(
            inventory
                .fixtures
                .iter()
                .any(|fixture| fixture.path.ends_with("benchmark-matrix/medium"))
        );
        let small = inventory
            .fixtures
            .iter()
            .find(|fixture| fixture.level == "small")
            .expect("small fixture");
        assert_eq!(small.projected_row_count, 20_000);
        assert_eq!(small.search_index_row_count, 5_000);
        assert_eq!(small.search_index_size_bytes, 320_000);
        assert_eq!(inventory.missing_levels, Vec::<String>::new());
        assert_eq!(inventory.required_profiles.len(), 6);
        assert_eq!(inventory.missing_profiles.len(), 6);
        assert!(!inventory.ready);
        assert_eq!(fixture_level(500_000), "large");
        Ok(())
    }

    #[test]
    fn benchmark_fixtures_require_ready_rejects_incomplete_matrix() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let error = execute(BenchmarkCommand::Fixtures(BenchmarkFixturesArgs {
            base: temp.path().to_path_buf(),
            include_large: false,
            require_ready: true,
        }))
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("benchmark fixture matrix is not ready")
        );
        Ok(())
    }

    #[test]
    fn benchmark_fixture_inventory_reports_required_profile_matrix() -> Result<()> {
        let temp = tempfile::tempdir()?;
        for profile in REQUIRED_BENCHMARK_PROFILES {
            write_inventory_fixture_with_profile(
                temp.path(),
                &format!("{profile}-small"),
                profile,
                10_000,
            )?;
            write_inventory_fixture_with_profile(
                temp.path(),
                &format!("{profile}-medium"),
                profile,
                100_000,
            )?;
            write_inventory_fixture_with_profile(
                temp.path(),
                &format!("{profile}-large"),
                profile,
                500_000,
            )?;
        }
        let inventory = benchmark_fixture_inventory(temp.path(), false)?;
        assert!(inventory.ready);
        assert_eq!(inventory.missing_levels, Vec::<String>::new());
        assert_eq!(inventory.missing_profiles, Vec::<String>::new());
        assert_eq!(inventory.missing_profile_levels.len(), 0);
        assert_eq!(
            inventory
                .profile_levels
                .get("scale-500k-conflict-heavy")
                .cloned()
                .unwrap_or_default(),
            BTreeSet::from([
                "large".to_string(),
                "medium".to_string(),
                "small".to_string()
            ])
        );
        Ok(())
    }

    #[test]
    fn benchmark_baseline_reports_missing_levels_without_fixtures() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let summary = run_baseline_matrix(&BenchmarkBaselineArgs {
            suite: BenchmarkSuite::Core,
            base: temp.path().join("fixtures"),
            output: temp.path().join("reports"),
            iterations: 1,
            warmups: 0,
            cache_state: "warm-test".to_string(),
            require_ready: false,
            include_large: false,
        })?;
        assert_eq!(summary.reports.len(), 0);
        assert_eq!(summary.failures.len(), 0);
        let failure_log = summary
            .failure_log
            .as_ref()
            .context("missing baseline failure log")?;
        assert!(failure_log.is_file());
        let failure_log = read_failure_log(failure_log)?;
        assert_eq!(failure_log.entry_count, 0);
        assert_eq!(summary.missing_levels, vec!["small", "medium"]);
        assert_eq!(summary.missing_profiles.len(), 6);
        assert!(!summary.ready);
        Ok(())
    }

    #[test]
    fn benchmark_baseline_ready_requires_successful_report_matrix() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        std::fs::create_dir(&fixture_base)?;
        for profile in REQUIRED_BENCHMARK_PROFILES {
            for (level, propositions) in [
                ("small", 10_000_u64),
                ("medium", 100_000_u64),
                ("large", 500_000_u64),
            ] {
                let fixture = fixture_base.join(format!("{profile}-{level}"));
                std::fs::create_dir(&fixture)?;
                std::fs::write(
                    fixture.join("manifest.json"),
                    serde_json::to_vec_pretty(&serde_json::json!({
                        "profile": profile,
                        "seed": 42,
                        "proposition_count": propositions,
                        "object_count": propositions * 5,
                        "projected_row_count": propositions * 2,
                        "search_index_row_count": propositions / 2,
                        "search_index_size_bytes": propositions * 32
                    }))?,
                )?;
            }
        }

        let summary = run_baseline_matrix(&BenchmarkBaselineArgs {
            suite: BenchmarkSuite::Core,
            base: fixture_base,
            output: temp.path().join("reports"),
            iterations: 1,
            warmups: 0,
            cache_state: "warm-test".to_string(),
            require_ready: false,
            include_large: false,
        })?;

        assert_eq!(summary.missing_levels, Vec::<String>::new());
        assert_eq!(summary.missing_profiles, Vec::<String>::new());
        assert_eq!(summary.missing_profile_levels, Vec::new());
        assert_eq!(summary.reports.len(), 0);
        assert_eq!(
            summary.failures.len(),
            REQUIRED_BENCHMARK_PROFILES.len() * REQUIRED_BENCHMARK_LEVELS.len()
        );
        assert!(!summary.ready);
        Ok(())
    }

    #[test]
    fn benchmark_baseline_report_includes_reproduction_metadata() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        std::fs::create_dir(&fixture_base)?;
        write_inventory_fixture_with_profile(
            &fixture_base,
            "balanced-small",
            "scale-500k-balanced",
            10_000,
        )?;
        let summary = run_baseline_matrix(&BenchmarkBaselineArgs {
            suite: BenchmarkSuite::Core,
            base: fixture_base,
            output: temp.path().join("reports"),
            iterations: 1,
            warmups: 0,
            cache_state: "warm-test".to_string(),
            require_ready: false,
            include_large: false,
        })?;
        assert_eq!(summary.reports.len(), 1);
        let reproduction = summary.reports[0]
            .reproduction
            .as_ref()
            .context("missing report reproduction metadata")?;
        assert_eq!(reproduction.profile, "scale-500k-balanced");
        assert_eq!(reproduction.level, "small");
        assert!(reproduction.benchmark_command.contains("benchmark run"));
        assert!(reproduction.environment_manifest.is_some());
        assert!(!summary.bottlenecks.is_empty());
        assert!(summary.bottlenecks.len() <= 10);
        assert_eq!(summary.bottlenecks[0].profile, "scale-500k-balanced");
        assert!(summary.bottlenecks[0].priority_score > 0.0);
        let failure_log = read_failure_log(
            summary
                .failure_log
                .as_ref()
                .context("missing baseline failure log")?,
        )?;
        assert_eq!(failure_log.entry_count, 0);
        Ok(())
    }

    #[test]
    fn benchmark_baseline_can_require_ready_fixture_matrix() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let error = run_baseline_matrix(&BenchmarkBaselineArgs {
            suite: BenchmarkSuite::Core,
            base: temp.path().join("fixtures"),
            output: temp.path().join("reports"),
            iterations: 1,
            warmups: 0,
            cache_state: "warm-test".to_string(),
            require_ready: true,
            include_large: false,
        })
        .unwrap_err()
        .to_string();
        assert!(error.contains("benchmark fixture matrix is not ready"));
        Ok(())
    }

    #[test]
    fn benchmark_baseline_require_ready_rejects_single_sample_baselines() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        write_ready_fixture_matrix(&fixture_base)?;

        let error = run_baseline_matrix(&BenchmarkBaselineArgs {
            suite: BenchmarkSuite::Core,
            base: fixture_base,
            output: temp.path().join("reports"),
            iterations: 1,
            warmups: 1,
            cache_state: "warm-filesystem".to_string(),
            require_ready: true,
            include_large: false,
        })
        .unwrap_err()
        .to_string();

        assert!(error.contains("requires at least 2 measured iterations"));
        Ok(())
    }

    #[test]
    fn benchmark_baseline_require_ready_rejects_warm_cache_without_warmups() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        write_ready_fixture_matrix(&fixture_base)?;

        let error = run_baseline_matrix(&BenchmarkBaselineArgs {
            suite: BenchmarkSuite::Core,
            base: fixture_base,
            output: temp.path().join("reports"),
            iterations: MIN_READY_BENCHMARK_ITERATIONS,
            warmups: 0,
            cache_state: "warm-filesystem".to_string(),
            require_ready: true,
            include_large: false,
        })
        .unwrap_err()
        .to_string();

        assert!(error.contains("requires at least one warmup iteration"));
        Ok(())
    }

    #[test]
    fn benchmark_audit_reports_missing_fixture_and_baseline_coverage() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: temp.path().join("fixtures"),
            baseline_summaries: Vec::new(),
            include_large: false,
        })?;
        assert!(!audit.ready);
        assert!(!audit.fixture_matrix_ready);
        assert_eq!(
            audit.missing_suites.len(),
            required_benchmark_suites().len()
        );
        assert_eq!(audit.missing_areas.len(), required_benchmark_areas().len());
        assert_eq!(
            audit.missing_requirements.len(),
            required_benchmark_requirements().len()
        );
        assert_eq!(
            audit.missing_representative_benchmarks.len(),
            required_representative_benchmark_names().len()
        );
        assert_eq!(
            audit.missing_cli_workflows.len(),
            required_cli_workflows().len()
        );
        assert_eq!(
            audit.missing_sampled_cli_workflows.len(),
            required_cli_workflows().len()
        );
        assert_eq!(
            audit.missing_cache_temperatures,
            vec!["cold".to_string(), "warm".to_string()]
        );
        assert!(audit.remediation_commands.iter().any(|command| command.kind
            == "generate-fixture"
            && command.profile.as_deref() == Some("scale-500k-balanced")
            && command.level.as_deref() == Some("small")
            && command.command.contains("--target-propositions 10000")));
        assert!(
            audit
                .remediation_commands
                .iter()
                .any(|command| command.kind == "verify-fixture"
                    && command.profile.as_deref() == Some("scale-500k-balanced")
                    && command.level.as_deref() == Some("small"))
        );
        assert!(
            audit
                .remediation_commands
                .iter()
                .any(|command| command.kind == "run-warm-baseline"
                    && command.command.contains("--cache-state warm-filesystem"))
        );
        assert!(
            audit
                .remediation_commands
                .iter()
                .any(|command| command.kind == "run-cold-baseline"
                    && command.command.contains("--cache-state cold-filesystem"))
        );
        assert!(
            audit
                .remediation_commands
                .iter()
                .any(|command| command.kind == "rerun-audit"
                    && command.command.contains("readiness-audit.json"))
        );
        assert_eq!(audit.total_reports, 0);
        let rendered = render_human_audit_report(&audit);
        assert!(rendered.contains("- Missing fixture profile levels: `12`"));
        assert!(rendered.contains("## Remediation Commands"));
        assert!(rendered.contains("generate-fixture"));
        assert!(rendered.contains("--target-propositions 10000"));
        assert!(rendered.contains("run-warm-baseline"));
        assert!(rendered.contains("run-cold-baseline"));
        assert!(rendered.contains("### Missing Fixture Profile Levels"));
        assert!(rendered.contains("### Missing Baseline Profile Levels"));
        assert!(rendered.contains("`scale-500k-balanced` / `small`"));
        assert!(rendered.contains("Missing cache temperatures: `cold, warm`"));
        assert!(rendered.contains("Missing representative benchmarks"));
        Ok(())
    }

    #[test]
    fn benchmark_audit_accepts_ready_matrix_and_full_summary_coverage() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        std::fs::create_dir(&fixture_base)?;
        for profile in REQUIRED_BENCHMARK_PROFILES {
            write_inventory_fixture_with_profile(
                &fixture_base,
                &format!("{profile}-small"),
                profile,
                10_000,
            )?;
            write_inventory_fixture_with_profile(
                &fixture_base,
                &format!("{profile}-medium"),
                profile,
                100_000,
            )?;
            write_inventory_fixture_with_profile(
                &fixture_base,
                &format!("{profile}-large"),
                profile,
                500_000,
            )?;
        }
        let warm_reports = write_required_coverage_reports_with_cache(
            &temp.path().join("warm"),
            "warm-filesystem",
        )?;
        let warm_summary = temp.path().join("warm-baseline-summary.json");
        let warm_summary_report = summary_with_full_matrix_reports(&warm_reports);
        write_summary_with_failure_log(&warm_summary, &warm_summary_report)?;
        let cold_reports = write_required_coverage_reports_with_cache(
            &temp.path().join("cold"),
            "cold-filesystem",
        )?;
        let cold_summary = temp.path().join("cold-baseline-summary.json");
        let mut cold_summary_report = summary_with_full_matrix_reports(&cold_reports);
        cold_summary_report.cache_state = "cold-filesystem".to_string();
        cold_summary_report.warmups = 0;
        write_summary_with_failure_log(&cold_summary, &cold_summary_report)?;

        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: fixture_base,
            baseline_summaries: vec![warm_summary, cold_summary],
            include_large: false,
        })?;
        assert!(audit.ready);
        assert!(audit.fixture_matrix_ready);
        assert_eq!(audit.missing_suites, Vec::<BenchmarkSuite>::new());
        assert_eq!(audit.missing_cli_workflows, Vec::<String>::new());
        assert_eq!(audit.missing_sampled_cli_workflows, Vec::<String>::new());
        assert_eq!(audit.missing_areas, Vec::<String>::new());
        assert_eq!(audit.missing_requirements, Vec::<String>::new());
        assert_eq!(
            audit.missing_representative_benchmarks,
            Vec::<String>::new()
        );
        assert_eq!(audit.total_reports, 24);
        assert_eq!(
            audit.missing_baseline_profile_levels,
            Vec::<BenchmarkMissingProfileLevel>::new()
        );
        assert_eq!(audit.missing_cache_temperatures, Vec::<String>::new());
        assert_eq!(audit.missing_environment_manifests, Vec::<PathBuf>::new());
        assert_eq!(audit.invalid_environment_metadata, Vec::<String>::new());
        assert_eq!(audit.missing_reproduce_commands, Vec::<PathBuf>::new());
        assert_eq!(
            audit.non_release_environment_manifests,
            Vec::<PathBuf>::new()
        );
        assert_eq!(audit.non_release_reports, Vec::<PathBuf>::new());
        assert_eq!(audit.fixture_metadata_mismatches, Vec::<PathBuf>::new());
        assert_eq!(audit.missing_fixture_metadata, Vec::<String>::new());
        assert_eq!(audit.invalid_report_counts, Vec::<String>::new());
        assert_eq!(audit.invalid_cache_labels, Vec::<String>::new());
        assert_eq!(audit.invalid_cache_metadata, Vec::<String>::new());
        assert_eq!(audit.insufficient_warmup_iterations, Vec::<PathBuf>::new());
        assert_eq!(audit.missing_report_metadata, Vec::<PathBuf>::new());
        assert_eq!(audit.missing_benchmark_metadata, Vec::<String>::new());
        assert_eq!(audit.invalid_requirement_tags, Vec::<String>::new());
        assert_eq!(audit.missing_bottleneck_summaries, Vec::<PathBuf>::new());
        assert_eq!(audit.invalid_bottleneck_summaries, Vec::<String>::new());
        assert_eq!(
            audit.insufficient_baseline_iterations,
            Vec::<PathBuf>::new()
        );
        assert_eq!(audit.insufficient_sample_benchmarks, Vec::<String>::new());
        assert_eq!(audit.invalid_sample_observations, Vec::<String>::new());
        assert_eq!(audit.invalid_phase_metadata, Vec::<String>::new());
        assert_eq!(audit.invalid_resource_metadata, Vec::<String>::new());
        assert_eq!(audit.invalid_sqlite_metadata, Vec::<String>::new());
        assert_eq!(audit.invalid_timing_statistics, Vec::<String>::new());
        assert_eq!(audit.incorrect_benchmarks, Vec::<String>::new());
        Ok(())
    }

    #[test]
    fn benchmark_audit_requires_real_sampled_fact_cli_workflows() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        write_ready_fixture_matrix(&fixture_base)?;
        let warm_reports = write_required_coverage_reports_with_cache(
            &temp.path().join("warm"),
            "warm-filesystem",
        )?;
        let cold_reports = write_required_coverage_reports_with_cache(
            &temp.path().join("cold"),
            "cold-filesystem",
        )?;
        for report_entry in warm_reports.iter().chain(cold_reports.iter()) {
            let mut report = read_report(&report_entry.report)?;
            for benchmark in report
                .benchmarks
                .iter_mut()
                .filter(|benchmark| benchmark.name.starts_with("cli_fact_"))
            {
                benchmark.read_only = true;
                benchmark.diagnostics.operation_kind = "process".to_string();
                benchmark.diagnostics.sqlite = None;
                benchmark.isolation_strategy =
                    "process benchmark isolates startup/config/loading/output cost in a child process per sample".to_string();
            }
            std::fs::write(&report_entry.report, serde_json::to_vec_pretty(&report)?)?;
        }
        let warm_summary = temp.path().join("warm-baseline-summary.json");
        write_summary_with_failure_log(
            &warm_summary,
            &summary_with_full_matrix_reports(&warm_reports),
        )?;
        let cold_summary = temp.path().join("cold-baseline-summary.json");
        let mut cold_summary_report = summary_with_full_matrix_reports(&cold_reports);
        cold_summary_report.cache_state = "cold-filesystem".to_string();
        cold_summary_report.warmups = 0;
        write_summary_with_failure_log(&cold_summary, &cold_summary_report)?;

        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: fixture_base,
            baseline_summaries: vec![warm_summary, cold_summary],
            include_large: false,
        })?;

        assert!(!audit.ready);
        assert_eq!(audit.missing_cli_workflows, Vec::<String>::new());
        assert_eq!(
            audit.missing_sampled_cli_workflows,
            required_cli_workflows().into_iter().collect::<Vec<_>>()
        );
        Ok(())
    }

    #[test]
    fn benchmark_audit_rejects_mismatched_summary_schema() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        write_ready_fixture_matrix(&fixture_base)?;
        let reports = write_required_coverage_reports(temp.path())?;
        let summary = temp.path().join("baseline-summary.json");
        let summary_report = summary_with_full_matrix_reports(&reports);
        write_summary_with_failure_log(&summary, &summary_report)?;
        replace_artifact_schema_version(&summary, "benchmark-v0")?;

        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: fixture_base,
            baseline_summaries: vec![summary],
            include_large: false,
        })?;

        assert!(!audit.ready);
        assert_eq!(audit.invalid_baseline_summaries.len(), 1);
        assert!(audit.invalid_baseline_summaries[0].contains("schema_version"));
        assert!(audit.invalid_baseline_summaries[0].contains("benchmark-v0"));
        assert!(audit.invalid_baseline_summaries[0].contains(REPORT_SCHEMA_VERSION));
        Ok(())
    }

    #[test]
    fn benchmark_audit_rejects_mismatched_failure_log_schema() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        write_ready_fixture_matrix(&fixture_base)?;
        let reports = write_required_coverage_reports(temp.path())?;
        let summary = temp.path().join("baseline-summary.json");
        let summary_report = summary_with_full_matrix_reports(&reports);
        write_summary_with_failure_log(&summary, &summary_report)?;
        let failure_log = summary_report
            .failure_log
            .as_ref()
            .context("missing failure log")?;
        replace_artifact_schema_version(failure_log, "benchmark-v0")?;

        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: fixture_base,
            baseline_summaries: vec![summary],
            include_large: false,
        })?;

        assert!(!audit.ready);
        assert_eq!(audit.invalid_failure_logs.len(), 1);
        assert!(audit.invalid_failure_logs[0].contains("schema_version"));
        assert!(audit.invalid_failure_logs[0].contains("benchmark-v0"));
        assert!(audit.invalid_failure_logs[0].contains(REPORT_SCHEMA_VERSION));
        Ok(())
    }

    #[test]
    fn benchmark_audit_rejects_mismatched_report_schema() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        write_ready_fixture_matrix(&fixture_base)?;
        let reports = write_required_coverage_reports(temp.path())?;
        let report_path = reports[0].report.clone();
        replace_artifact_schema_version(&report_path, "benchmark-v0")?;
        let summary = temp.path().join("baseline-summary.json");
        let summary_report = summary_with_full_matrix_reports(&reports);
        write_summary_with_failure_log(&summary, &summary_report)?;

        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: fixture_base,
            baseline_summaries: vec![summary],
            include_large: false,
        })?;

        assert!(!audit.ready);
        assert_eq!(audit.invalid_report_files.len(), 1);
        assert!(audit.invalid_report_files[0].contains("schema_version"));
        assert!(audit.invalid_report_files[0].contains("benchmark-v0"));
        assert!(audit.invalid_report_files[0].contains(REPORT_SCHEMA_VERSION));
        Ok(())
    }

    #[test]
    fn benchmark_audit_rejects_invalid_requirement_tags() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        write_ready_fixture_matrix(&fixture_base)?;
        let reports = write_required_coverage_reports(temp.path())?;
        let report_path = reports[0].report.clone();
        let mut report = read_report(&report_path)?;
        report.benchmarks[0]
            .requirement_tags
            .push("not-a-required-benchmark".to_string());
        std::fs::write(&report_path, serde_json::to_vec_pretty(&report)?)?;
        let summary = temp.path().join("baseline-summary.json");
        let summary_report = summary_with_full_matrix_reports(&reports);
        write_summary_with_failure_log(&summary, &summary_report)?;

        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: fixture_base,
            baseline_summaries: vec![summary],
            include_large: false,
        })?;

        assert!(!audit.ready);
        assert!(
            audit
                .invalid_requirement_tags
                .iter()
                .any(|entry| entry.ends_with(":not-a-required-benchmark"))
        );
        Ok(())
    }

    #[test]
    fn benchmark_audit_rejects_sample_observation_row_mismatches() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        write_ready_fixture_matrix(&fixture_base)?;
        let reports = write_required_coverage_reports(temp.path())?;
        let report_path = reports[0].report.clone();
        let mut report = read_report(&report_path)?;
        report.benchmarks[0].sample_observations[0].rows_returned += 1;
        std::fs::write(&report_path, serde_json::to_vec_pretty(&report)?)?;
        let summary = temp.path().join("baseline-summary.json");
        let summary_report = summary_with_full_matrix_reports(&reports);
        write_summary_with_failure_log(&summary, &summary_report)?;

        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: fixture_base,
            baseline_summaries: vec![summary],
            include_large: false,
        })?;

        assert!(!audit.ready);
        assert!(
            audit
                .invalid_sample_observations
                .iter()
                .any(|entry| entry.contains("sample-observation-rows-mismatch-0"))
        );
        Ok(())
    }

    #[test]
    fn benchmark_audit_rejects_sample_observation_byte_mismatches() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        write_ready_fixture_matrix(&fixture_base)?;
        let reports = write_required_coverage_reports(temp.path())?;
        let report_path = reports[0].report.clone();
        let mut report = read_report(&report_path)?;
        report.benchmarks[0].sample_observations[0].measured_bytes = Some(99);
        std::fs::write(&report_path, serde_json::to_vec_pretty(&report)?)?;
        let summary = temp.path().join("baseline-summary.json");
        let summary_report = summary_with_full_matrix_reports(&reports);
        write_summary_with_failure_log(&summary, &summary_report)?;

        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: fixture_base,
            baseline_summaries: vec![summary],
            include_large: false,
        })?;

        assert!(!audit.ready);
        assert!(
            audit
                .invalid_sample_observations
                .iter()
                .any(|entry| entry.contains("sample-observation-measured-bytes-mismatch-0"))
        );
        Ok(())
    }

    #[test]
    fn benchmark_audit_rejects_warm_cache_baselines_without_warmups() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        write_ready_fixture_matrix(&fixture_base)?;
        let reports = write_required_coverage_reports(temp.path())?;
        let summary = temp.path().join("missing-warmups-summary.json");
        let mut summary_report = summary_with_full_matrix_reports(&reports);
        summary_report.warmups = 0;
        summary_report.cache_state = "warm-filesystem".to_string();
        write_summary_with_failure_log(&summary, &summary_report)?;

        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: fixture_base,
            baseline_summaries: vec![summary.clone()],
            include_large: false,
        })?;

        assert!(!audit.ready);
        assert_eq!(audit.insufficient_warmup_iterations, vec![summary]);
        assert_eq!(audit.invalid_cache_labels, Vec::<String>::new());
        assert_eq!(audit.invalid_cache_metadata, Vec::<String>::new());
        Ok(())
    }

    #[test]
    fn benchmark_audit_rejects_single_sample_baseline_summaries() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        write_ready_fixture_matrix(&fixture_base)?;
        let reports = write_required_coverage_reports(temp.path())?;
        for report_entry in &reports {
            let mut report = read_report(&report_entry.report)?;
            for benchmark in &mut report.benchmarks {
                benchmark.samples_ms = vec![10.0];
                refresh_test_sample_observations(benchmark);
                benchmark.stats = TimingStats::from_samples(&benchmark.samples_ms);
                benchmark.phase_timings.measurement_total_ms = 10.0;
                benchmark.phase_breakdown = test_phase_breakdown(10.0);
            }
            std::fs::write(&report_entry.report, serde_json::to_vec_pretty(&report)?)?;
        }
        let summary = temp.path().join("single-sample-summary.json");
        let mut summary_report = summary_with_full_matrix_reports(&reports);
        summary_report.iterations = 1;
        write_summary_with_failure_log(&summary, &summary_report)?;

        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: fixture_base,
            baseline_summaries: vec![summary.clone()],
            include_large: false,
        })?;

        assert!(!audit.ready);
        assert_eq!(audit.insufficient_baseline_iterations, vec![summary]);
        assert_eq!(audit.insufficient_sample_benchmarks, Vec::<String>::new());
        Ok(())
    }

    #[test]
    fn benchmark_audit_rejects_inconsistent_report_count_metadata() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        write_ready_fixture_matrix(&fixture_base)?;
        let reports = write_required_coverage_reports(temp.path())?;
        let summary = temp.path().join("bad-count-summary.json");
        let mut summary_report = summary_with_full_matrix_reports(&reports);
        summary_report.reports[0].benchmark_count += 1;
        let report = summary_report.reports[0].report.clone();
        write_summary_with_failure_log(&summary, &summary_report)?;

        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: fixture_base,
            baseline_summaries: vec![summary],
            include_large: false,
        })?;

        assert!(!audit.ready);
        assert_eq!(audit.invalid_report_counts.len(), 1);
        assert!(audit.invalid_report_counts[0].contains(&report.display().to_string()));
        Ok(())
    }

    #[test]
    fn benchmark_audit_rejects_report_fixture_path_mismatches() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        write_ready_fixture_matrix(&fixture_base)?;
        let reports = write_required_coverage_reports(temp.path())?;
        let report = reports[0].report.clone();
        let mut run_report = required_coverage_report_for_entry(
            &reports[0].profile,
            &reports[0].level,
            PathBuf::from("fixtures/wrong-fixture-path"),
        );
        run_report.fixture.seed = reports[0].seed;
        std::fs::write(&report, serde_json::to_vec_pretty(&run_report)?)?;
        let summary = temp.path().join("fixture-path-summary.json");
        let summary_report = summary_with_full_matrix_reports(&reports);
        write_summary_with_failure_log(&summary, &summary_report)?;

        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: fixture_base,
            baseline_summaries: vec![summary],
            include_large: false,
        })?;

        assert!(!audit.ready);
        assert_eq!(audit.fixture_metadata_mismatches, vec![report]);
        Ok(())
    }

    #[test]
    fn benchmark_audit_rejects_debug_build_baselines() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        std::fs::create_dir(&fixture_base)?;
        for profile in REQUIRED_BENCHMARK_PROFILES {
            write_inventory_fixture_with_profile(
                &fixture_base,
                &format!("{profile}-small"),
                profile,
                10_000,
            )?;
            write_inventory_fixture_with_profile(
                &fixture_base,
                &format!("{profile}-medium"),
                profile,
                100_000,
            )?;
            write_inventory_fixture_with_profile(
                &fixture_base,
                &format!("{profile}-large"),
                profile,
                500_000,
            )?;
        }
        let reports = write_required_coverage_reports(temp.path())?;
        let report = reports[0].report.clone();
        let mut run_report = report_with_required_coverage();
        run_report.environment.build_profile = "debug".to_string();
        std::fs::write(&report, serde_json::to_vec_pretty(&run_report)?)?;
        let summary = temp.path().join("debug-summary.json");
        let mut summary_report = summary_with_full_matrix_reports(&reports);
        summary_report
            .environment_manifest
            .as_mut()
            .expect("test summary has environment")
            .build_profile = "debug".to_string();
        write_summary_with_failure_log(&summary, &summary_report)?;

        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: fixture_base,
            baseline_summaries: vec![summary.clone()],
            include_large: false,
        })?;
        assert!(!audit.ready);
        assert_eq!(audit.non_release_environment_manifests, vec![summary]);
        assert_eq!(audit.non_release_reports, vec![report]);
        Ok(())
    }

    #[test]
    fn benchmark_audit_rejects_incomplete_environment_metadata() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        write_ready_fixture_matrix(&fixture_base)?;
        let reports = write_required_coverage_reports(temp.path())?;
        let report = reports[0].report.clone();
        let mut run_report = required_coverage_report_for_entry(
            &reports[0].profile,
            &reports[0].level,
            reports[0].fixture.clone(),
        );
        run_report.environment.operating_system.clear();
        run_report.environment.cpu_model = None;
        run_report.environment.core_count = None;
        run_report.environment.memory_bytes = None;
        run_report.environment.filesystem = None;
        run_report.environment.storage_type = None;
        run_report.environment.rust_version = None;
        run_report.environment.fixture_path = PathBuf::new();
        std::fs::write(&report, serde_json::to_vec_pretty(&run_report)?)?;
        let summary = temp.path().join("environment-summary.json");
        let mut summary_report = summary_with_full_matrix_reports(&reports);
        let environment = summary_report
            .environment_manifest
            .as_mut()
            .context("test summary has environment")?;
        environment.architecture.clear();
        environment.memory_bytes = None;
        environment.filesystem = None;
        environment.storage_type = None;
        environment.benchmark_project_commit = None;
        write_summary_with_failure_log(&summary, &summary_report)?;

        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: fixture_base,
            baseline_summaries: vec![summary],
            include_large: false,
        })?;

        assert!(!audit.ready);
        assert!(
            audit
                .invalid_environment_metadata
                .iter()
                .any(|entry| entry.ends_with(":operating-system"))
        );
        assert!(
            audit
                .invalid_environment_metadata
                .iter()
                .any(|entry| entry.ends_with(":cpu-model"))
        );
        assert!(
            audit
                .invalid_environment_metadata
                .iter()
                .any(|entry| entry.ends_with(":core-count"))
        );
        assert!(
            audit
                .invalid_environment_metadata
                .iter()
                .any(|entry| entry.ends_with(":memory-bytes"))
        );
        assert!(
            audit
                .invalid_environment_metadata
                .iter()
                .any(|entry| entry.ends_with(":filesystem"))
        );
        assert!(
            audit
                .invalid_environment_metadata
                .iter()
                .any(|entry| entry.ends_with(":storage-type"))
        );
        assert!(
            audit
                .invalid_environment_metadata
                .iter()
                .any(|entry| entry.ends_with(":rust-version"))
        );
        assert!(
            audit
                .invalid_environment_metadata
                .iter()
                .any(|entry| entry.ends_with(":fixture-path"))
        );
        assert!(
            audit
                .invalid_environment_metadata
                .iter()
                .any(|entry| entry.ends_with(":architecture"))
        );
        assert!(
            audit
                .invalid_environment_metadata
                .iter()
                .any(|entry| entry.ends_with(":benchmark-project-commit"))
        );
        Ok(())
    }

    #[test]
    fn benchmark_audit_rejects_incomplete_fixture_scale_metadata() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        write_ready_fixture_matrix(&fixture_base)?;
        let reports = write_required_coverage_reports(temp.path())?;
        let report = reports[0].report.clone();
        let mut run_report = required_coverage_report_for_entry(
            &reports[0].profile,
            &reports[0].level,
            reports[0].fixture.clone(),
        );
        run_report.fixture.actor_count = None;
        run_report.fixture.ledger_count = None;
        run_report.fixture.replica_count = None;
        run_report.fixture.projected_row_count = 0;
        run_report.fixture.search_index_row_count = 10;
        run_report.fixture.search_index_size_bytes = 0;
        run_report.fixture.database_size_bytes = 0;
        run_report.fixture.sqlite_databases.clear();
        std::fs::write(&report, serde_json::to_vec_pretty(&run_report)?)?;
        let summary = temp.path().join("fixture-metadata-summary.json");
        let summary_report = summary_with_full_matrix_reports(&reports);
        write_summary_with_failure_log(&summary, &summary_report)?;

        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: fixture_base,
            baseline_summaries: vec![summary],
            include_large: false,
        })?;

        assert!(!audit.ready);
        assert_eq!(audit.fixture_metadata_mismatches, Vec::<PathBuf>::new());
        assert!(
            audit
                .missing_fixture_metadata
                .iter()
                .any(|entry| entry.ends_with(":actor-count"))
        );
        assert!(
            audit
                .missing_fixture_metadata
                .iter()
                .any(|entry| entry.ends_with(":ledger-count"))
        );
        assert!(
            audit
                .missing_fixture_metadata
                .iter()
                .any(|entry| entry.ends_with(":replica-count"))
        );
        assert!(
            audit
                .missing_fixture_metadata
                .iter()
                .any(|entry| entry.ends_with(":projected-row-count"))
        );
        assert!(
            audit
                .missing_fixture_metadata
                .iter()
                .any(|entry| entry.ends_with(":search-index-size-bytes"))
        );
        assert!(
            audit
                .missing_fixture_metadata
                .iter()
                .any(|entry| entry.ends_with(":database-size-bytes"))
        );
        assert!(
            audit
                .missing_fixture_metadata
                .iter()
                .any(|entry| entry.ends_with(":sqlite-database-paths"))
        );
        Ok(())
    }

    #[test]
    fn benchmark_audit_rejects_missing_structured_reproduction_metadata() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        std::fs::create_dir(&fixture_base)?;
        for profile in REQUIRED_BENCHMARK_PROFILES {
            write_inventory_fixture_with_profile(
                &fixture_base,
                &format!("{profile}-small"),
                profile,
                10_000,
            )?;
            write_inventory_fixture_with_profile(
                &fixture_base,
                &format!("{profile}-medium"),
                profile,
                100_000,
            )?;
            write_inventory_fixture_with_profile(
                &fixture_base,
                &format!("{profile}-large"),
                profile,
                500_000,
            )?;
        }
        let reports = write_required_coverage_reports(temp.path())?;
        let mut summary_report = summary_with_full_matrix_reports(&reports);
        summary_report.reports[0].reproduction = None;
        let summary = temp.path().join("baseline-summary.json");
        write_summary_with_failure_log(&summary, &summary_report)?;

        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: fixture_base,
            baseline_summaries: vec![summary],
            include_large: false,
        })?;
        assert!(!audit.ready);
        assert_eq!(
            audit.missing_reproduce_commands,
            vec![reports[0].report.clone()]
        );
        Ok(())
    }

    #[test]
    fn benchmark_audit_rejects_missing_bottleneck_summary_evidence() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        std::fs::create_dir(&fixture_base)?;
        for profile in REQUIRED_BENCHMARK_PROFILES {
            write_inventory_fixture_with_profile(
                &fixture_base,
                &format!("{profile}-small"),
                profile,
                10_000,
            )?;
            write_inventory_fixture_with_profile(
                &fixture_base,
                &format!("{profile}-medium"),
                profile,
                100_000,
            )?;
            write_inventory_fixture_with_profile(
                &fixture_base,
                &format!("{profile}-large"),
                profile,
                500_000,
            )?;
        }
        let reports = write_required_coverage_reports(temp.path())?;
        let mut summary_report = summary_with_full_matrix_reports(&reports);
        summary_report.bottlenecks.clear();
        let summary = temp.path().join("baseline-summary.json");
        write_summary_with_failure_log(&summary, &summary_report)?;

        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: fixture_base,
            baseline_summaries: vec![summary.clone()],
            include_large: false,
        })?;
        assert!(!audit.ready);
        assert_eq!(audit.missing_bottleneck_summaries, vec![summary]);
        Ok(())
    }

    #[test]
    fn benchmark_audit_rejects_invalid_bottleneck_summary_evidence() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        write_ready_fixture_matrix(&fixture_base)?;
        let reports = write_required_coverage_reports(temp.path())?;
        let mut summary_report = summary_with_full_matrix_reports(&reports);
        summary_report.bottlenecks[0].source_report = PathBuf::from("missing-report.json");
        summary_report.bottlenecks[0].priority_score = 0.0;
        summary_report.bottlenecks[0].reasons.clear();
        let summary = temp.path().join("baseline-summary.json");
        write_summary_with_failure_log(&summary, &summary_report)?;

        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: fixture_base,
            baseline_summaries: vec![summary],
            include_large: false,
        })?;
        assert!(!audit.ready);
        assert!(
            audit
                .invalid_bottleneck_summaries
                .iter()
                .any(|entry| entry.contains("unknown-report"))
        );
        assert!(
            audit
                .invalid_bottleneck_summaries
                .iter()
                .any(|entry| entry.contains("invalid-score"))
        );
        assert!(
            audit
                .invalid_bottleneck_summaries
                .iter()
                .any(|entry| entry.contains("missing-reasons"))
        );
        Ok(())
    }

    #[test]
    fn benchmark_audit_rejects_missing_failure_log_evidence() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        write_ready_fixture_matrix(&fixture_base)?;
        let reports = write_required_coverage_reports(temp.path())?;
        let summary_report = summary_with_full_matrix_reports(&reports);
        let missing_failure_log = summary_report
            .failure_log
            .clone()
            .context("test summary should identify a failure log")?;
        let summary = temp.path().join("baseline-summary.json");
        std::fs::write(&summary, serde_json::to_vec_pretty(&summary_report)?)?;

        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: fixture_base,
            baseline_summaries: vec![summary],
            include_large: false,
        })?;

        assert!(!audit.ready);
        assert_eq!(audit.missing_failure_logs, vec![missing_failure_log]);
        Ok(())
    }

    #[test]
    fn benchmark_audit_rejects_inconsistent_failure_log_evidence() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        write_ready_fixture_matrix(&fixture_base)?;
        let reports = write_required_coverage_reports(temp.path())?;
        let summary_report = summary_with_full_matrix_reports(&reports);
        let failure_log = summary_report
            .failure_log
            .clone()
            .context("test summary should identify a failure log")?;
        std::fs::write(
            &failure_log,
            serde_json::to_vec_pretty(&BenchmarkFailureLog {
                schema_version: REPORT_SCHEMA_VERSION.to_string(),
                generated_at_unix_ms: 0,
                baseline_output: PathBuf::from("reports/wrong-output"),
                suite: BenchmarkSuite::Core,
                entry_count: 1,
                entries: Vec::new(),
            })?,
        )?;
        let summary = temp.path().join("baseline-summary.json");
        std::fs::write(&summary, serde_json::to_vec_pretty(&summary_report)?)?;

        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: fixture_base,
            baseline_summaries: vec![summary],
            include_large: false,
        })?;

        assert!(!audit.ready);
        assert!(
            audit
                .invalid_failure_logs
                .iter()
                .any(|entry| entry.contains("baseline-output-mismatch"))
        );
        assert!(
            audit
                .invalid_failure_logs
                .iter()
                .any(|entry| entry.contains("suite-mismatch"))
        );
        assert!(
            audit
                .invalid_failure_logs
                .iter()
                .any(|entry| entry.contains("entry-count-mismatch"))
        );
        Ok(())
    }

    #[test]
    fn benchmark_audit_rejects_missing_benchmark_failure_log_entries() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        write_ready_fixture_matrix(&fixture_base)?;
        let reports = write_required_coverage_reports(temp.path())?;
        let report = reports[0].report.clone();
        let mut run_report = required_coverage_report_for_entry(
            &reports[0].profile,
            &reports[0].level,
            reports[0].fixture.clone(),
        );
        run_report.benchmarks[0].correctness_passed = false;
        run_report.benchmarks[0]
            .failures
            .push("test correctness failure".to_string());
        std::fs::write(&report, serde_json::to_vec_pretty(&run_report)?)?;
        let summary = temp.path().join("benchmark-failure-summary.json");
        let summary_report = summary_with_full_matrix_reports(&reports);
        write_summary_with_failure_log(&summary, &summary_report)?;

        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: fixture_base,
            baseline_summaries: vec![summary],
            include_large: false,
        })?;

        assert!(!audit.ready);
        assert_eq!(audit.incorrect_benchmarks.len(), 1);
        assert!(
            audit
                .invalid_failure_logs
                .iter()
                .any(|entry| entry.contains("benchmark-failure-missing"))
        );
        Ok(())
    }

    #[test]
    fn benchmark_audit_rejects_mixed_cache_state_metadata() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        write_ready_fixture_matrix(&fixture_base)?;
        let reports = write_required_coverage_reports(temp.path())?;
        let report = reports[0].report.clone();
        let mut run_report = required_coverage_report_for_entry(
            &reports[0].profile,
            &reports[0].level,
            reports[0].fixture.clone(),
        );
        run_report.benchmarks[0].cache_state = "cold-filesystem".to_string();
        run_report.benchmarks[0].cache_classification = cache_classification("cold-filesystem");
        run_report.benchmarks[1].cache_classification = cache_classification("cold-filesystem");
        std::fs::write(&report, serde_json::to_vec_pretty(&run_report)?)?;
        let summary = temp.path().join("cache-summary.json");
        let summary_report = summary_with_full_matrix_reports(&reports);
        write_summary_with_failure_log(&summary, &summary_report)?;

        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: fixture_base,
            baseline_summaries: vec![summary],
            include_large: false,
        })?;

        assert!(!audit.ready);
        assert_eq!(audit.invalid_cache_labels, Vec::<String>::new());
        assert!(
            audit
                .invalid_cache_metadata
                .iter()
                .any(|entry| entry.contains("cache-state-mismatch"))
        );
        assert!(
            audit
                .invalid_cache_metadata
                .iter()
                .any(|entry| entry.contains("cache-classification-mismatch"))
        );
        Ok(())
    }

    #[test]
    fn benchmark_audit_requires_each_profile_level_at_each_cache_temperature() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        write_ready_fixture_matrix(&fixture_base)?;
        let reports = write_required_coverage_reports_with_cache(temp.path(), "warm-filesystem")?;
        let summary = temp.path().join("warm-summary.json");
        let summary_report = summary_with_full_matrix_reports(&reports);
        write_summary_with_failure_log(&summary, &summary_report)?;

        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: fixture_base,
            baseline_summaries: vec![summary],
            include_large: false,
        })?;

        assert!(!audit.ready);
        assert_eq!(audit.missing_cache_temperatures, vec!["cold"]);
        assert_eq!(
            audit.missing_cache_profile_levels.len(),
            REQUIRED_BENCHMARK_PROFILES.len() * REQUIRED_BENCHMARK_LEVELS.len()
        );
        assert!(audit.missing_cache_profile_levels.iter().all(|missing| {
            missing.cache_temperature == "cold"
                && REQUIRED_BENCHMARK_PROFILES.contains(&missing.profile.as_str())
                && REQUIRED_BENCHMARK_LEVELS.contains(&missing.level.as_str())
        }));
        Ok(())
    }

    #[test]
    fn benchmark_audit_remediates_wrong_suite_coverage() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        write_ready_fixture_matrix(&fixture_base)?;

        let mut warm_reports = write_required_coverage_reports_with_cache(
            &temp.path().join("warm"),
            "warm-filesystem",
        )?;
        restrict_reports_to_suite(&mut warm_reports, BenchmarkSuite::Core)?;
        let mut warm_summary_report = summary_with_full_matrix_reports(&warm_reports);
        warm_summary_report.suite = BenchmarkSuite::Core;
        let warm_summary = temp.path().join("warm-summary.json");
        write_summary_with_failure_log(&warm_summary, &warm_summary_report)?;

        let mut cold_reports = write_required_coverage_reports_with_cache(
            &temp.path().join("cold"),
            "cold-filesystem",
        )?;
        restrict_reports_to_suite(&mut cold_reports, BenchmarkSuite::Core)?;
        let mut cold_summary_report = summary_with_full_matrix_reports(&cold_reports);
        cold_summary_report.suite = BenchmarkSuite::Core;
        cold_summary_report.cache_state = "cold-filesystem".to_string();
        cold_summary_report.warmups = 0;
        let cold_summary = temp.path().join("cold-summary.json");
        write_summary_with_failure_log(&cold_summary, &cold_summary_report)?;

        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: fixture_base,
            baseline_summaries: vec![warm_summary, cold_summary],
            include_large: false,
        })?;

        assert!(!audit.ready);
        assert!(audit.missing_baseline_profile_levels.is_empty());
        assert!(audit.missing_cache_profile_levels.is_empty());
        assert!(audit.missing_suites.contains(&BenchmarkSuite::Read));
        assert!(audit.remediation_commands.iter().any(|command| {
            command.kind == "run-warm-baseline" && command.command.contains("--suite full")
        }));
        assert!(audit.remediation_commands.iter().any(|command| {
            command.kind == "run-cold-baseline" && command.command.contains("--suite full")
        }));
        Ok(())
    }

    fn restrict_reports_to_suite(
        reports: &mut [BenchmarkBaselineReport],
        suite: BenchmarkSuite,
    ) -> Result<()> {
        for entry in reports {
            let mut report = read_report(&entry.report)?;
            report
                .benchmarks
                .retain(|benchmark| benchmark.suite == suite);
            entry.benchmark_count = report.benchmarks.len();
            std::fs::write(&entry.report, serde_json::to_vec_pretty(&report)?)?;
        }
        Ok(())
    }

    #[test]
    fn benchmark_audit_rejects_inconsistent_report_integrity() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture_base = temp.path().join("fixtures");
        write_ready_fixture_matrix(&fixture_base)?;
        let reports = write_required_coverage_reports(temp.path())?;
        let report = reports[0].report.clone();
        let mut run_report = required_coverage_report_for_entry(
            &reports[0].profile,
            &reports[0].level,
            reports[0].fixture.clone(),
        );
        run_report.fixture.profile = "wrong-profile".to_string();
        run_report.fixture.simulator_revision = None;
        run_report.benchmarks[0].cache_state = "unknown-cache".to_string();
        run_report.benchmarks[0].samples_ms.clear();
        run_report.benchmarks[0].sample_observations.clear();
        run_report.benchmarks[0].stats.samples = 0;
        run_report.benchmarks[1].sample_observations[0].elapsed_ms += 1.0;
        run_report.benchmarks[2].phase_breakdown[0].elapsed_ms = Some(999.0);
        run_report.benchmarks[3]
            .diagnostics
            .resources_before
            .process_rss_kib = Some(100);
        run_report.benchmarks[3]
            .diagnostics
            .resources_after
            .process_rss_kib = Some(125);
        run_report.benchmarks[3]
            .diagnostics
            .resource_delta
            .process_rss_kib_delta = Some(1);
        if let Some(sqlite) = &mut run_report.benchmarks[4].diagnostics.sqlite {
            sqlite.uses_full_scan = false;
        }
        run_report.benchmarks[5].stats.median_ms = Some(999.0);
        let sync_bundle = run_report
            .benchmarks
            .iter_mut()
            .find(|benchmark| benchmark.name == "sync_bundle_inventory_metadata")
            .context("missing sync bundle benchmark")?;
        sync_bundle.network_payload_bytes = None;
        std::fs::write(&report, serde_json::to_vec_pretty(&run_report)?)?;
        let summary = temp.path().join("integrity-summary.json");
        let summary_report = summary_with_full_matrix_reports(&reports);
        write_summary_with_failure_log(&summary, &summary_report)?;

        let audit = audit_benchmark_readiness(&BenchmarkAuditArgs {
            base: fixture_base,
            baseline_summaries: vec![summary],
            include_large: false,
        })?;
        assert!(!audit.ready);
        assert_eq!(audit.fixture_metadata_mismatches, vec![report.clone()]);
        assert_eq!(audit.invalid_cache_labels.len(), 1);
        assert_eq!(audit.missing_report_metadata, vec![report]);
        assert_eq!(audit.insufficient_sample_benchmarks.len(), 1);
        assert_eq!(audit.invalid_sample_observations.len(), 2);
        assert!(
            audit
                .invalid_sample_observations
                .iter()
                .any(|entry| { entry.contains("sample-observation-elapsed-mismatch-0") })
        );
        assert!(audit.invalid_sample_observations.iter().any(|entry| {
            entry.contains("sample-observation-network-payload-bytes-mismatch-0")
        }));
        assert_eq!(audit.invalid_phase_metadata.len(), 1);
        assert_eq!(audit.invalid_resource_metadata.len(), 1);
        assert_eq!(audit.invalid_sqlite_metadata.len(), 1);
        assert_eq!(audit.invalid_timing_statistics.len(), 2);
        assert!(
            audit
                .invalid_timing_statistics
                .iter()
                .any(|entry| entry.contains("stats-median-mismatch"))
        );
        assert_eq!(audit.missing_benchmark_metadata.len(), 1);
        assert!(
            audit
                .missing_benchmark_metadata
                .iter()
                .any(|entry| entry.contains("missing-byte-metadata"))
        );
        Ok(())
    }

    fn write_ready_fixture_matrix(fixture_base: &Path) -> Result<()> {
        std::fs::create_dir(fixture_base)?;
        for profile in REQUIRED_BENCHMARK_PROFILES {
            write_inventory_fixture_with_profile(
                fixture_base,
                &format!("{profile}-small"),
                profile,
                10_000,
            )?;
            write_inventory_fixture_with_profile(
                fixture_base,
                &format!("{profile}-medium"),
                profile,
                100_000,
            )?;
            write_inventory_fixture_with_profile(
                fixture_base,
                &format!("{profile}-large"),
                profile,
                500_000,
            )?;
        }
        Ok(())
    }

    fn complete_cache_profile_levels() -> BTreeMap<String, BTreeMap<String, BTreeSet<String>>> {
        required_cache_temperatures()
            .into_iter()
            .map(|temperature| {
                (
                    temperature.to_string(),
                    REQUIRED_BENCHMARK_PROFILES
                        .into_iter()
                        .map(|profile| {
                            (
                                profile.to_string(),
                                REQUIRED_BENCHMARK_LEVELS
                                    .into_iter()
                                    .map(str::to_string)
                                    .collect(),
                            )
                        })
                        .collect(),
                )
            })
            .collect()
    }

    fn ready_acceptance_audit_report() -> BenchmarkAuditReport {
        BenchmarkAuditReport {
            schema_version: REPORT_SCHEMA_VERSION.to_string(),
            generated_at_unix_ms: 0,
            base: PathBuf::from("fixtures"),
            fixture_matrix_ready: true,
            inventory: BenchmarkFixtureInventory {
                base: PathBuf::from("fixtures"),
                fixtures: Vec::new(),
                levels: BTreeMap::new(),
                missing_levels: Vec::new(),
                required_profiles: REQUIRED_BENCHMARK_PROFILES
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
                profile_levels: BTreeMap::new(),
                missing_profiles: Vec::new(),
                missing_profile_levels: Vec::new(),
                ready: true,
            },
            required_suites: required_benchmark_suites().into_iter().collect(),
            covered_suites: required_benchmark_suites().into_iter().collect(),
            missing_suites: Vec::new(),
            required_areas: required_benchmark_areas().into_iter().collect(),
            covered_areas: required_benchmark_areas().into_iter().collect(),
            missing_areas: Vec::new(),
            required_requirements: required_benchmark_requirements().into_iter().collect(),
            covered_requirements: required_benchmark_requirements().into_iter().collect(),
            missing_requirements: Vec::new(),
            required_representative_benchmarks: required_representative_benchmark_names()
                .into_iter()
                .collect(),
            covered_representative_benchmarks: required_representative_benchmark_names()
                .into_iter()
                .collect(),
            missing_representative_benchmarks: Vec::new(),
            required_cli_workflows: required_cli_workflows().into_iter().collect(),
            covered_cli_workflows: required_cli_workflows().into_iter().collect(),
            missing_cli_workflows: Vec::new(),
            covered_sampled_cli_workflows: required_cli_workflows().into_iter().collect(),
            missing_sampled_cli_workflows: Vec::new(),
            covered_cache_temperatures: vec!["cold".to_string(), "warm".to_string()],
            missing_cache_temperatures: Vec::new(),
            baseline_profile_levels: BTreeMap::new(),
            missing_baseline_profile_levels: Vec::new(),
            baseline_cache_profile_levels: complete_cache_profile_levels(),
            missing_cache_profile_levels: Vec::new(),
            baseline_summaries: Vec::new(),
            invalid_baseline_summaries: Vec::new(),
            missing_report_files: Vec::new(),
            invalid_report_files: Vec::new(),
            missing_failure_logs: Vec::new(),
            invalid_failure_logs: Vec::new(),
            missing_environment_manifests: Vec::new(),
            invalid_environment_metadata: Vec::new(),
            missing_reproduce_commands: Vec::new(),
            non_release_environment_manifests: Vec::new(),
            non_release_reports: Vec::new(),
            not_ready_summaries: Vec::new(),
            failing_summaries: Vec::new(),
            fixture_metadata_mismatches: Vec::new(),
            missing_fixture_metadata: Vec::new(),
            invalid_report_counts: Vec::new(),
            invalid_cache_labels: Vec::new(),
            invalid_cache_metadata: Vec::new(),
            insufficient_warmup_iterations: Vec::new(),
            missing_report_metadata: Vec::new(),
            missing_benchmark_metadata: Vec::new(),
            invalid_requirement_tags: Vec::new(),
            missing_bottleneck_summaries: Vec::new(),
            invalid_bottleneck_summaries: Vec::new(),
            insufficient_baseline_iterations: Vec::new(),
            insufficient_sample_benchmarks: Vec::new(),
            invalid_sample_observations: Vec::new(),
            invalid_phase_metadata: Vec::new(),
            invalid_resource_metadata: Vec::new(),
            invalid_sqlite_metadata: Vec::new(),
            invalid_timing_statistics: Vec::new(),
            incorrect_benchmarks: Vec::new(),
            remediation_commands: Vec::new(),
            total_reports: 18,
            total_benchmarks: 18 * report_with_required_coverage().benchmarks.len(),
            ready: true,
        }
    }

    #[test]
    fn benchmark_report_renders_baseline_summary_and_audit_json() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let report = temp.path().join("run.json");
        let summary = temp.path().join("baseline-summary.json");
        let matrix_plan = temp.path().join("matrix-plan.json");
        let fixture_inventory = temp.path().join("fixture-inventory.json");
        let audit_path = temp.path().join("readiness-audit.json");
        std::fs::write(
            &report,
            serde_json::to_vec_pretty(&report_with_required_coverage())?,
        )?;
        std::fs::write(
            &summary,
            serde_json::to_vec_pretty(&summary_with_full_report(&report))?,
        )?;
        let fixture_base = temp.path().join("fixtures");
        std::fs::create_dir(&fixture_base)?;
        write_inventory_fixture_with_profile(
            &fixture_base,
            "scale-500k-balanced-small",
            "scale-500k-balanced",
            10_000,
        )?;
        std::fs::write(
            &matrix_plan,
            serde_json::to_vec_pretty(&benchmark_matrix_plan(&BenchmarkPlanArgs {
                suite: BenchmarkSuite::Full,
                fixture_base: fixture_base.clone(),
                report_output: temp.path().join("reports"),
                seed: 42,
                iterations: 2,
                warmups: 1,
                cache_state: "warm-filesystem".to_string(),
                include_large: false,
            })?)?,
        )?;
        std::fs::write(
            &fixture_inventory,
            serde_json::to_vec_pretty(&benchmark_fixture_inventory(&fixture_base, false)?)?,
        )?;
        std::fs::write(
            &audit_path,
            serde_json::to_vec_pretty(&BenchmarkAuditReport {
                schema_version: REPORT_SCHEMA_VERSION.to_string(),
                generated_at_unix_ms: 0,
                base: PathBuf::from("fixtures"),
                fixture_matrix_ready: true,
                inventory: BenchmarkFixtureInventory {
                    base: PathBuf::from("fixtures"),
                    fixtures: Vec::new(),
                    levels: BTreeMap::new(),
                    missing_levels: Vec::new(),
                    required_profiles: REQUIRED_BENCHMARK_PROFILES
                        .into_iter()
                        .map(str::to_string)
                        .collect(),
                    profile_levels: BTreeMap::new(),
                    missing_profiles: Vec::new(),
                    missing_profile_levels: Vec::new(),
                    ready: true,
                },
                required_suites: required_benchmark_suites().into_iter().collect(),
                covered_suites: required_benchmark_suites().into_iter().collect(),
                missing_suites: Vec::new(),
                required_areas: required_benchmark_areas().into_iter().collect(),
                covered_areas: required_benchmark_areas().into_iter().collect(),
                missing_areas: Vec::new(),
                required_requirements: required_benchmark_requirements().into_iter().collect(),
                covered_requirements: required_benchmark_requirements().into_iter().collect(),
                missing_requirements: Vec::new(),
                required_representative_benchmarks: required_representative_benchmark_names()
                    .into_iter()
                    .collect(),
                covered_representative_benchmarks: required_representative_benchmark_names()
                    .into_iter()
                    .collect(),
                missing_representative_benchmarks: Vec::new(),
                required_cli_workflows: required_cli_workflows().into_iter().collect(),
                covered_cli_workflows: required_cli_workflows().into_iter().collect(),
                missing_cli_workflows: Vec::new(),
                covered_sampled_cli_workflows: required_cli_workflows().into_iter().collect(),
                missing_sampled_cli_workflows: Vec::new(),
                covered_cache_temperatures: vec!["cold".to_string(), "warm".to_string()],
                missing_cache_temperatures: Vec::new(),
                baseline_profile_levels: BTreeMap::new(),
                missing_baseline_profile_levels: Vec::new(),
                baseline_cache_profile_levels: complete_cache_profile_levels(),
                missing_cache_profile_levels: Vec::new(),
                baseline_summaries: Vec::new(),
                invalid_baseline_summaries: Vec::new(),
                missing_report_files: Vec::new(),
                invalid_report_files: Vec::new(),
                missing_failure_logs: Vec::new(),
                invalid_failure_logs: Vec::new(),
                missing_environment_manifests: Vec::new(),
                invalid_environment_metadata: Vec::new(),
                missing_reproduce_commands: Vec::new(),
                non_release_environment_manifests: Vec::new(),
                non_release_reports: Vec::new(),
                not_ready_summaries: Vec::new(),
                failing_summaries: Vec::new(),
                fixture_metadata_mismatches: Vec::new(),
                missing_fixture_metadata: Vec::new(),
                invalid_report_counts: Vec::new(),
                invalid_cache_labels: Vec::new(),
                invalid_cache_metadata: Vec::new(),
                insufficient_warmup_iterations: Vec::new(),
                missing_report_metadata: Vec::new(),
                missing_benchmark_metadata: vec![
                    "reports/read.json:read_list_default_accepted:missing-byte-metadata"
                        .to_string(),
                ],
                invalid_requirement_tags: Vec::new(),
                missing_bottleneck_summaries: Vec::new(),
                invalid_bottleneck_summaries: Vec::new(),
                insufficient_baseline_iterations: Vec::new(),
                insufficient_sample_benchmarks: Vec::new(),
                invalid_sample_observations: Vec::new(),
                invalid_phase_metadata: Vec::new(),
                invalid_resource_metadata: Vec::new(),
                invalid_sqlite_metadata: Vec::new(),
                invalid_timing_statistics: Vec::new(),
                incorrect_benchmarks: Vec::new(),
                remediation_commands: Vec::new(),
                total_reports: 1,
                total_benchmarks: 11,
                ready: false,
            })?,
        )?;

        assert!(render_report_file(&summary)?.contains("# Benchmark Baseline Summary"));
        let plan_report = render_report_file(&matrix_plan)?;
        assert!(plan_report.contains("# Benchmark Matrix Plan"));
        assert!(plan_report.contains("Fixture inventory"));
        assert!(plan_report.contains("scale-500k-balanced"));
        let inventory_report = render_report_file(&fixture_inventory)?;
        assert!(inventory_report.contains("# Benchmark Fixture Inventory"));
        assert!(inventory_report.contains("Missing profile levels"));
        assert!(inventory_report.contains("scale-500k-balanced"));
        let audit_report = render_report_file(&audit_path)?;
        assert!(audit_report.contains("# Benchmark Readiness Audit"));
        assert!(audit_report.contains("Missing requirements"));
        assert!(audit_report.contains("### Missing Benchmark Metadata"));
        assert!(audit_report.contains("missing-byte-metadata"));
        Ok(())
    }

    #[test]
    fn benchmark_report_renders_comparison_json() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let baseline_report = temp.path().join("baseline-run.json");
        let current_report = temp.path().join("current-run.json");
        let comparison_path = temp.path().join("comparison.json");
        std::fs::write(
            &baseline_report,
            serde_json::to_vec_pretty(&report_with_median(10.0))?,
        )?;
        std::fs::write(
            &current_report,
            serde_json::to_vec_pretty(&report_with_median(12.0))?,
        )?;

        let comparison = compare_reports(&BenchmarkCompareArgs {
            baseline: baseline_report,
            current: current_report,
            thresholds: None,
            warning_threshold_percent: 5.0,
            regression_threshold_percent: 15.0,
        })?;
        std::fs::write(&comparison_path, serde_json::to_vec_pretty(&comparison)?)?;

        let rendered = render_report_file(&comparison_path)?;
        assert!(rendered.contains("# Benchmark Comparison"));
        assert!(rendered.contains("## Regressions"));
        assert!(rendered.contains("Regression decision ready"));
        assert!(rendered.contains("bench"));
        Ok(())
    }

    #[test]
    fn benchmark_report_renders_matrix_comparison_json() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let baseline_report = temp.path().join("baseline-run.json");
        let current_report = temp.path().join("current-run.json");
        let baseline_summary = temp.path().join("baseline-summary.json");
        let current_summary = temp.path().join("current-summary.json");
        let comparison_path = temp.path().join("matrix-comparison.json");
        let mut current = report_with_median(12.0);
        current.environment.cpu_model = Some("different-cpu".to_string());
        current.fixture.database_size_bytes = 99_999;
        current.benchmarks[0].cache_state = "cold-filesystem".to_string();
        current.benchmarks[0].cache_classification = cache_classification("cold-filesystem");
        std::fs::write(
            &baseline_report,
            serde_json::to_vec_pretty(&report_with_median(10.0))?,
        )?;
        std::fs::write(&current_report, serde_json::to_vec_pretty(&current)?)?;
        std::fs::write(
            &baseline_summary,
            serde_json::to_vec_pretty(&summary_with_report(&baseline_report))?,
        )?;
        std::fs::write(
            &current_summary,
            serde_json::to_vec_pretty(&summary_with_report(&current_report))?,
        )?;

        let comparison = compare_matrix_reports(&BenchmarkCompareMatrixArgs {
            baseline_summary,
            current_summary,
            thresholds: None,
            warning_threshold_percent: 5.0,
            regression_threshold_percent: 15.0,
        })?;
        std::fs::write(&comparison_path, serde_json::to_vec_pretty(&comparison)?)?;

        let rendered = render_report_file(&comparison_path)?;
        assert!(rendered.contains("# Benchmark Matrix Comparison"));
        assert!(rendered.contains("- Compared reports: `1`"));
        assert!(rendered.contains("- Environment-incompatible reports: `1`"));
        assert!(rendered.contains("- Fixture-incompatible reports: `1`"));
        assert!(rendered.contains("- Cache differences: `1`"));
        assert!(rendered.contains("Regression decision ready"));
        assert!(rendered.contains("## Compared Reports"));
        assert!(rendered.contains("## Report Comparison Blockers"));
        assert!(rendered.contains("environment-differences"));
        assert!(rendered.contains("fixture-differences"));
        assert!(rendered.contains("cache-differences"));
        assert!(rendered.contains("cpu_model: test-cpu -> different-cpu"));
        assert!(rendered.contains("database_size_bytes: 1 -> 99999"));
        assert!(rendered.contains("bench: warm-filesystem -> cold-filesystem"));
        assert!(
            rendered.contains(
                "| `scale-500k-balanced:small:42` | `scale-500k-balanced` | `small` | 42 |"
            )
        );
        Ok(())
    }

    #[test]
    fn benchmark_compare_rejects_mismatched_report_schema() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let baseline_report = temp.path().join("baseline-run.json");
        let current_report = temp.path().join("current-run.json");
        std::fs::write(
            &baseline_report,
            serde_json::to_vec_pretty(&report_with_median(10.0))?,
        )?;
        std::fs::write(
            &current_report,
            serde_json::to_vec_pretty(&report_with_median(12.0))?,
        )?;
        replace_artifact_schema_version(&current_report, "benchmark-v0")?;

        let error = compare_reports(&BenchmarkCompareArgs {
            baseline: baseline_report,
            current: current_report,
            thresholds: None,
            warning_threshold_percent: 5.0,
            regression_threshold_percent: 15.0,
        })
        .unwrap_err();
        let message = format!("{error:#}");

        assert!(message.contains("schema_version"));
        assert!(message.contains("benchmark-v0"));
        assert!(message.contains(REPORT_SCHEMA_VERSION));
        Ok(())
    }

    #[test]
    fn benchmark_compare_matrix_rejects_mismatched_summary_schema() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let baseline_report = temp.path().join("baseline-run.json");
        let current_report = temp.path().join("current-run.json");
        let baseline_summary = temp.path().join("baseline-summary.json");
        let current_summary = temp.path().join("current-summary.json");
        std::fs::write(
            &baseline_report,
            serde_json::to_vec_pretty(&report_with_median(10.0))?,
        )?;
        std::fs::write(
            &current_report,
            serde_json::to_vec_pretty(&report_with_median(12.0))?,
        )?;
        std::fs::write(
            &baseline_summary,
            serde_json::to_vec_pretty(&summary_with_report(&baseline_report))?,
        )?;
        std::fs::write(
            &current_summary,
            serde_json::to_vec_pretty(&summary_with_report(&current_report))?,
        )?;
        replace_artifact_schema_version(&current_summary, "benchmark-v0")?;

        let error = compare_matrix_reports(&BenchmarkCompareMatrixArgs {
            baseline_summary,
            current_summary,
            thresholds: None,
            warning_threshold_percent: 5.0,
            regression_threshold_percent: 15.0,
        })
        .unwrap_err();
        let message = format!("{error:#}");

        assert!(message.contains("schema_version"));
        assert!(message.contains("benchmark-v0"));
        assert!(message.contains(REPORT_SCHEMA_VERSION));
        Ok(())
    }

    #[test]
    fn benchmark_analyze_classifies_growth_across_levels() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let summary = write_growth_summary(
            temp.path(),
            &[
                ("small", 10_000, 10.0),
                ("medium", 100_000, 100.0),
                ("large", 500_000, 500.0),
            ],
        )?;

        let analysis = analyze_baseline_growth(&BenchmarkAnalyzeArgs {
            baseline_summaries: vec![summary.clone()],
        })?;

        assert_eq!(analysis.trend_count, 1);
        assert_eq!(analysis.complete_trend_count, 1);
        assert_eq!(analysis.insufficient_trend_count, 0);
        assert_eq!(
            analysis.trends[0].classification.shape,
            BenchmarkGrowthShape::Linear
        );
        assert_eq!(analysis.trends[0].points.len(), 3);
        let large = analysis.trends[0]
            .points
            .iter()
            .find(|point| point.level == "large")
            .expect("large growth point");
        assert_eq!(large.search_index_size_bytes, 500_000 * 32);
        Ok(())
    }

    #[test]
    fn benchmark_analyze_records_mismatched_summary_schema_failures() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let summary = write_growth_summary(temp.path(), &[("small", 10_000, 10.0)])?;
        replace_artifact_schema_version(&summary, "benchmark-v0")?;

        let analysis = analyze_baseline_growth(&BenchmarkAnalyzeArgs {
            baseline_summaries: vec![summary],
        })?;

        assert_eq!(analysis.trend_count, 0);
        assert_eq!(analysis.failures.len(), 1);
        assert!(analysis.failures[0].error.contains("schema_version"));
        assert!(analysis.failures[0].error.contains("benchmark-v0"));
        assert!(analysis.failures[0].error.contains(REPORT_SCHEMA_VERSION));
        Ok(())
    }

    #[test]
    fn benchmark_report_renders_growth_analysis_json() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let summary = write_growth_summary(
            temp.path(),
            &[("small", 10_000, 10.0), ("large", 500_000, 10.5)],
        )?;
        let analysis_path = temp.path().join("growth-analysis.json");
        let analysis = analyze_baseline_growth(&BenchmarkAnalyzeArgs {
            baseline_summaries: vec![summary],
        })?;
        std::fs::write(&analysis_path, serde_json::to_vec_pretty(&analysis)?)?;

        let rendered = render_report_file(&analysis_path)?;
        assert!(rendered.contains("# Benchmark Growth Analysis"));
        assert!(rendered.contains("constant"));
        assert!(rendered.contains("bench"));
        Ok(())
    }

    #[test]
    fn benchmark_budgets_derive_from_baseline_reports() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let summary = write_growth_summary(temp.path(), &[("small", 10_000, 20.0)])?;

        let budgets = derive_benchmark_budgets(&BenchmarkBudgetsArgs {
            baseline_summaries: vec![summary],
            warning_multiplier: 1.25,
            regression_multiplier: 1.50,
            minimum_warning_ms: 1.0,
            minimum_regression_ms: 5.0,
        })?;

        assert_eq!(budgets.entry_count, 1);
        assert_eq!(budgets.incorrect_entry_count, 0);
        assert_eq!(budgets.budgets[0].baseline_median_ms, 20.0);
        assert_eq!(budgets.budgets[0].warning_budget_ms, 25.0);
        assert_eq!(budgets.budgets[0].regression_budget_ms, 30.0);
        Ok(())
    }

    #[test]
    fn benchmark_budgets_exclude_incorrect_baseline_entries() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let summary = write_growth_summary(temp.path(), &[("small", 10_000, 20.0)])?;
        let report_path = temp.path().join("small.json");
        let mut report = read_report(&report_path)?;
        report.benchmarks[0].correctness_passed = false;
        report.benchmarks[0]
            .failures
            .push("incorrect baseline".to_string());
        std::fs::write(&report_path, serde_json::to_vec_pretty(&report)?)?;

        let budgets = derive_benchmark_budgets(&BenchmarkBudgetsArgs {
            baseline_summaries: vec![summary],
            warning_multiplier: 1.25,
            regression_multiplier: 1.50,
            minimum_warning_ms: 1.0,
            minimum_regression_ms: 5.0,
        })?;

        assert_eq!(budgets.entry_count, 0);
        assert_eq!(budgets.incorrect_entry_count, 1);
        assert!(budgets.budgets.is_empty());
        Ok(())
    }

    #[test]
    fn benchmark_budgets_record_mismatched_summary_schema_failures() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let summary = write_growth_summary(temp.path(), &[("small", 10_000, 20.0)])?;
        replace_artifact_schema_version(&summary, "benchmark-v0")?;

        let budgets = derive_benchmark_budgets(&BenchmarkBudgetsArgs {
            baseline_summaries: vec![summary],
            warning_multiplier: 1.25,
            regression_multiplier: 1.50,
            minimum_warning_ms: 1.0,
            minimum_regression_ms: 5.0,
        })?;

        assert_eq!(budgets.entry_count, 0);
        assert_eq!(budgets.failures.len(), 1);
        assert!(budgets.failures[0].error.contains("schema_version"));
        assert!(budgets.failures[0].error.contains("benchmark-v0"));
        assert!(budgets.failures[0].error.contains(REPORT_SCHEMA_VERSION));
        Ok(())
    }

    #[test]
    fn benchmark_report_renders_budget_json() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let summary = write_growth_summary(temp.path(), &[("small", 10_000, 20.0)])?;
        let budget_path = temp.path().join("budgets.json");
        let budgets = derive_benchmark_budgets(&BenchmarkBudgetsArgs {
            baseline_summaries: vec![summary],
            warning_multiplier: 1.25,
            regression_multiplier: 1.50,
            minimum_warning_ms: 1.0,
            minimum_regression_ms: 5.0,
        })?;
        std::fs::write(&budget_path, serde_json::to_vec_pretty(&budgets)?)?;

        let rendered = render_report_file(&budget_path)?;
        assert!(rendered.contains("# Benchmark Performance Budgets"));
        assert!(rendered.contains("bench"));
        assert!(rendered.contains("30.000"));
        Ok(())
    }

    #[test]
    fn benchmark_check_budgets_classifies_regressions() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let budget_summary = write_growth_summary(temp.path(), &[("small", 10_000, 20.0)])?;
        let budgets_path = write_test_budgets(temp.path(), budget_summary)?;
        let current_summary = write_growth_summary(temp.path(), &[("small", 10_000, 40.0)])?;

        let check = check_benchmark_budgets(&BenchmarkCheckBudgetsArgs {
            budgets: budgets_path,
            baseline_summary: current_summary,
        })?;

        assert!(!check.passed);
        assert_eq!(check.checked_count, 1);
        assert_eq!(check.regression_count, 1);
        assert_eq!(check.warning_count, 0);
        assert_eq!(
            check.results[0].classification,
            BenchmarkBudgetStatus::Regression
        );
        Ok(())
    }

    #[test]
    fn benchmark_check_budgets_records_warnings_without_failing() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let budget_summary = write_growth_summary(temp.path(), &[("small", 10_000, 20.0)])?;
        let budgets_path = write_test_budgets(temp.path(), budget_summary)?;
        let current_summary = write_growth_summary(temp.path(), &[("small", 10_000, 26.0)])?;

        let check = check_benchmark_budgets(&BenchmarkCheckBudgetsArgs {
            budgets: budgets_path,
            baseline_summary: current_summary,
        })?;

        assert!(check.passed);
        assert_eq!(check.checked_count, 1);
        assert_eq!(check.warning_count, 1);
        assert_eq!(check.regression_count, 0);
        assert_eq!(
            check.results[0].classification,
            BenchmarkBudgetStatus::Warning
        );

        let check_path = temp.path().join("warning-budget-check.json");
        std::fs::write(&check_path, serde_json::to_vec_pretty(&check)?)?;
        let rendered = render_report_file(&check_path)?;
        assert!(rendered.contains("Budget warnings are recorded for review"));
        Ok(())
    }

    #[test]
    fn benchmark_check_budgets_rejects_cache_state_mismatches() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let budget_summary = write_growth_summary(temp.path(), &[("small", 10_000, 20.0)])?;
        let budgets_path = write_test_budgets(temp.path(), budget_summary)?;
        let current_summary = write_growth_summary(temp.path(), &[("small", 10_000, 20.0)])?;
        let report_path = temp.path().join("small.json");
        let mut report = read_report(&report_path)?;
        report.benchmarks[0].cache_state = "cold-filesystem".to_string();
        report.benchmarks[0].cache_classification = cache_classification("cold-filesystem");
        std::fs::write(&report_path, serde_json::to_vec_pretty(&report)?)?;

        let check = check_benchmark_budgets(&BenchmarkCheckBudgetsArgs {
            budgets: budgets_path,
            baseline_summary: current_summary,
        })?;

        assert!(!check.passed);
        assert_eq!(check.checked_count, 0);
        assert_eq!(check.failure_count, 1);
        assert_eq!(check.regression_count, 0);
        assert_eq!(check.missing_budget_count, 0);
        assert!(check.failures[0].error.contains("cache state"));
        Ok(())
    }

    #[test]
    fn benchmark_report_renders_budget_check_json() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let budget_summary = write_growth_summary(temp.path(), &[("small", 10_000, 20.0)])?;
        let budgets_path = write_test_budgets(temp.path(), budget_summary)?;
        let current_summary = write_growth_summary(temp.path(), &[("small", 10_000, 40.0)])?;
        let check_path = temp.path().join("budget-check.json");
        let check = check_benchmark_budgets(&BenchmarkCheckBudgetsArgs {
            budgets: budgets_path,
            baseline_summary: current_summary,
        })?;
        std::fs::write(&check_path, serde_json::to_vec_pretty(&check)?)?;

        let rendered = render_report_file(&check_path)?;
        assert!(rendered.contains("# Benchmark Budget Check"));
        assert!(rendered.contains("## Budget Findings"));
        assert!(rendered.contains("regression"));
        Ok(())
    }

    #[test]
    fn benchmark_check_budgets_rejects_mismatched_budget_schema() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let budget_summary = write_growth_summary(temp.path(), &[("small", 10_000, 20.0)])?;
        let budgets_path = write_test_budgets(temp.path(), budget_summary.clone())?;
        replace_artifact_schema_version(&budgets_path, "benchmark-v0")?;

        let error = check_benchmark_budgets(&BenchmarkCheckBudgetsArgs {
            budgets: budgets_path,
            baseline_summary: budget_summary,
        })
        .unwrap_err();
        let message = format!("{error:#}");

        assert!(message.contains("schema_version"));
        assert!(message.contains("benchmark-v0"));
        assert!(message.contains(REPORT_SCHEMA_VERSION));
        Ok(())
    }

    #[test]
    fn benchmark_profile_plan_selects_slow_candidates() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let summary = write_profile_plan_summary(temp.path())?;

        let plan = benchmark_profile_plan(&BenchmarkProfilePlanArgs {
            baseline_summaries: vec![summary],
            limit: 1,
        })?;

        assert_eq!(plan.candidate_count, 1);
        assert_eq!(plan.candidates[0].benchmark, "read_list_default_accepted");
        assert!(plan.candidates[0].reasons.contains(&"high-p95".to_string()));
        assert!(
            plan.candidates[0]
                .reasons
                .contains(&"user-facing".to_string())
        );
        assert!(
            plan.candidates[0]
                .reasons
                .contains(&"cpu-time-delta".to_string())
        );
        assert!(
            plan.candidates[0]
                .reasons
                .contains(&"rss-delta".to_string())
        );
        assert!(
            plan.candidates[0]
                .reasons
                .contains(&"peak-rss-delta".to_string())
        );
        assert!(
            plan.candidates[0]
                .reasons
                .contains(&"disk-read-delta".to_string())
        );
        assert!(
            plan.candidates[0]
                .reasons
                .contains(&"disk-write-delta".to_string())
        );
        assert_eq!(plan.candidates[0].measured_bytes, Some(1024));
        assert_eq!(plan.candidates[0].process_rss_kib_delta, Some(2048));
        assert_eq!(plan.candidates[0].process_peak_rss_kib_delta, Some(4096));
        assert_eq!(plan.candidates[0].artifact_bytes_delta, Some(2_097_152));
        assert_eq!(plan.candidates[0].disk_read_bytes_delta, Some(1_048_576));
        assert_eq!(plan.candidates[0].disk_write_bytes_delta, Some(2_097_152));
        Ok(())
    }

    #[test]
    fn benchmark_profile_plan_records_mismatched_summary_schema_failures() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let summary = write_profile_plan_summary(temp.path())?;
        replace_artifact_schema_version(&summary, "benchmark-v0")?;

        let plan = benchmark_profile_plan(&BenchmarkProfilePlanArgs {
            baseline_summaries: vec![summary],
            limit: 1,
        })?;

        assert_eq!(plan.candidate_count, 0);
        assert_eq!(plan.failure_count, 1);
        assert!(plan.failures[0].error.contains("schema_version"));
        assert!(plan.failures[0].error.contains("benchmark-v0"));
        assert!(plan.failures[0].error.contains(REPORT_SCHEMA_VERSION));
        Ok(())
    }

    #[test]
    fn benchmark_report_renders_profile_plan_json() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let summary = write_profile_plan_summary(temp.path())?;
        let plan_path = temp.path().join("profile-plan.json");
        let plan = benchmark_profile_plan(&BenchmarkProfilePlanArgs {
            baseline_summaries: vec![summary],
            limit: 1,
        })?;
        std::fs::write(&plan_path, serde_json::to_vec_pretty(&plan)?)?;

        let rendered = render_report_file(&plan_path)?;
        assert!(rendered.contains("# Benchmark Profile Plan"));
        assert!(rendered.contains("read_list_default_accepted"));
        assert!(rendered.contains("rss_delta_kib=2048"));
        assert!(rendered.contains("peak_rss_delta_kib=4096"));
        assert!(rendered.contains("cargo flamegraph"));
        Ok(())
    }

    #[test]
    fn benchmark_acceptance_accepts_complete_evidence_bundle() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let args = write_complete_acceptance_inputs(temp.path())?;

        let acceptance = accept_benchmark_baseline(&args)?;

        assert!(acceptance.accepted);
        assert!(acceptance.blockers.is_empty());
        assert_eq!(acceptance.total_reports, 18);
        assert_eq!(acceptance.growth_trend_count, 1);
        assert_eq!(acceptance.complete_growth_trend_count, 1);
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-missing-requirements"),
            Some(&0)
        );
        assert_eq!(acceptance.blocker_evidence.get("growth-trends"), Some(&1));
        assert_eq!(
            acceptance.blocker_evidence.get("growth-complete-trends"),
            Some(&1)
        );
        assert_eq!(acceptance.blocker_evidence.get("budget-missing"), Some(&0));

        let acceptance_path = temp.path().join("acceptance.json");
        std::fs::write(&acceptance_path, serde_json::to_vec_pretty(&acceptance)?)?;
        let rendered = render_report_file(&acceptance_path)?;
        assert!(rendered.contains("# Benchmark Acceptance Report"));
        assert!(rendered.contains("- Accepted: `true`"));
        assert!(rendered.contains("## Evidence Summary"));
        assert!(rendered.contains("## Blocker Evidence"));
        Ok(())
    }

    #[test]
    fn benchmark_acceptance_allows_budget_warnings() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let audit_path = temp.path().join("readiness-audit.json");
        std::fs::write(
            &audit_path,
            serde_json::to_vec_pretty(&ready_acceptance_audit_report())?,
        )?;

        let growth_summary = write_growth_summary(
            temp.path(),
            &[
                ("small", 10_000, 10.0),
                ("medium", 100_000, 100.0),
                ("large", 500_000, 500.0),
            ],
        )?;
        let growth_path = temp.path().join("growth-analysis.json");
        let growth = analyze_baseline_growth(&BenchmarkAnalyzeArgs {
            baseline_summaries: vec![growth_summary.clone()],
        })?;
        std::fs::write(&growth_path, serde_json::to_vec_pretty(&growth)?)?;

        let budget_check_path = temp.path().join("budget-check.json");
        std::fs::write(
            &budget_check_path,
            serde_json::to_vec_pretty(&BenchmarkBudgetCheck {
                schema_version: REPORT_SCHEMA_VERSION.to_string(),
                generated_at_unix_ms: 0,
                budgets: PathBuf::from("budgets.json"),
                baseline_summary: growth_summary,
                checked_count: 1,
                warning_count: 1,
                regression_count: 0,
                incorrect_count: 0,
                missing_budget_count: 0,
                failure_count: 0,
                passed: true,
                results: Vec::new(),
                missing_budgets: Vec::new(),
                failures: Vec::new(),
            })?,
        )?;

        let profile_summary = write_profile_plan_summary(temp.path())?;
        let profile_path = temp.path().join("profile-plan.json");
        let profile_plan = benchmark_profile_plan(&BenchmarkProfilePlanArgs {
            baseline_summaries: vec![profile_summary],
            limit: 1,
        })?;
        std::fs::write(&profile_path, serde_json::to_vec_pretty(&profile_plan)?)?;

        let acceptance = accept_benchmark_baseline(&BenchmarkAcceptArgs {
            audit: audit_path,
            growth_analysis: growth_path,
            budget_check: budget_check_path,
            profile_plan: profile_path,
        })?;

        assert!(acceptance.accepted);
        assert!(
            !acceptance
                .blockers
                .contains(&"budget-check-failed".to_string())
        );
        assert_eq!(acceptance.blocker_evidence.get("budget-warnings"), Some(&1));
        Ok(())
    }

    #[test]
    fn benchmark_acceptance_rejects_mismatched_artifact_schema() -> Result<()> {
        let temp = tempfile::tempdir()?;

        let artifacts: [(&str, ArtifactPathSelector); 4] = [
            ("readiness-audit", |args: &BenchmarkAcceptArgs| &args.audit),
            ("growth-analysis", |args: &BenchmarkAcceptArgs| {
                &args.growth_analysis
            }),
            ("budget-check", |args: &BenchmarkAcceptArgs| {
                &args.budget_check
            }),
            ("profile-plan", |args: &BenchmarkAcceptArgs| {
                &args.profile_plan
            }),
        ];
        for (artifact, path_selector) in artifacts {
            let bundle_dir = temp.path().join(artifact);
            let args = write_complete_acceptance_inputs(&bundle_dir)?;
            replace_artifact_schema_version(path_selector(&args), "benchmark-v0")?;

            let error = accept_benchmark_baseline(&args).unwrap_err();
            let message = format!("{error:#}");

            assert!(message.contains(artifact), "{artifact}: {message}");
            assert!(message.contains("schema_version"), "{artifact}: {message}");
            assert!(message.contains("benchmark-v0"), "{artifact}: {message}");
            assert!(
                message.contains(REPORT_SCHEMA_VERSION),
                "{artifact}: {message}"
            );
        }
        Ok(())
    }

    #[test]
    fn benchmark_acceptance_reports_named_blockers() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let audit_path = temp.path().join("readiness-audit.json");
        let mut audit = ready_acceptance_audit_report();
        audit.ready = false;
        audit.total_reports = 0;
        audit.inventory.missing_levels = vec!["large".to_string()];
        audit.inventory.missing_profiles = vec!["scale-500k-balanced".to_string()];
        audit.inventory.missing_profile_levels = vec![BenchmarkMissingProfileLevel {
            profile: "scale-500k-balanced".to_string(),
            level: "large".to_string(),
        }];
        audit.missing_suites = vec![BenchmarkSuite::Http];
        audit.missing_areas = vec!["http".to_string()];
        audit.missing_requirements = vec!["http-local-loopback".to_string()];
        audit.missing_cli_workflows = vec!["push".to_string()];
        audit.missing_baseline_profile_levels = vec![BenchmarkMissingProfileLevel {
            profile: "scale-500k-balanced".to_string(),
            level: "large".to_string(),
        }];
        audit.missing_cache_profile_levels = vec![BenchmarkMissingCacheProfileLevel {
            cache_temperature: "cold".to_string(),
            profile: "scale-500k-balanced".to_string(),
            level: "large".to_string(),
        }];
        audit.invalid_baseline_summaries =
            vec!["reports/warm-summary.json:schema-version-mismatch".to_string()];
        audit.missing_report_files = vec![PathBuf::from("reports/missing-read.json")];
        audit.invalid_report_files = vec!["reports/read.json:schema-version-mismatch".to_string()];
        audit.missing_failure_logs = vec![PathBuf::from("reports/failure-log.json")];
        audit.invalid_failure_logs =
            vec!["reports/failure-log.json:schema-version-mismatch".to_string()];
        audit.missing_environment_manifests = vec![PathBuf::from("reports/warm-summary.json")];
        audit.invalid_environment_metadata =
            vec!["reports/read.json:missing-rust-version".to_string()];
        audit.missing_reproduce_commands = vec![PathBuf::from("reports/read.json")];
        audit.non_release_environment_manifests = vec![PathBuf::from("reports/warm-summary.json")];
        audit.non_release_reports = vec![PathBuf::from("reports/read.json")];
        audit.not_ready_summaries = vec![PathBuf::from("reports/warm-summary.json")];
        audit.failing_summaries = vec![PathBuf::from("reports/warm-summary.json")];
        audit.fixture_metadata_mismatches = vec![PathBuf::from("reports/read.json")];
        audit.missing_fixture_metadata = vec!["reports/read.json:database-size-bytes".to_string()];
        audit.invalid_report_counts = vec!["reports/read.json:summary=1:report=2".to_string()];
        audit.invalid_cache_labels = vec!["reports/read.json:bench:unknown-cache".to_string()];
        audit.invalid_cache_metadata =
            vec!["reports/read.json:bench:cache-temperature-mismatch".to_string()];
        audit.insufficient_warmup_iterations = vec![PathBuf::from("reports/warm-summary.json")];
        audit.missing_report_metadata = vec![PathBuf::from("reports/read.json")];
        audit.missing_benchmark_metadata =
            vec!["reports/read.json:read_list_default_accepted:missing-byte-metadata".to_string()];
        audit.invalid_requirement_tags =
            vec!["reports/read.json:bench:unknown-requirement".to_string()];
        audit.missing_bottleneck_summaries = vec![PathBuf::from("reports/warm-summary.json")];
        audit.invalid_bottleneck_summaries =
            vec!["reports/warm-summary.json:bench:missing-reasons".to_string()];
        audit.insufficient_baseline_iterations = vec![PathBuf::from("reports/warm-summary.json")];
        audit.insufficient_sample_benchmarks = vec!["reports/read.json:bench:1".to_string()];
        audit.invalid_sample_observations =
            vec!["reports/read.json:bench:sample-count-mismatch".to_string()];
        audit.invalid_phase_metadata =
            vec!["reports/read.json:bench:missing-required-phase-setup".to_string()];
        audit.invalid_resource_metadata =
            vec!["reports/read.json:bench:cpu-delta-mismatch".to_string()];
        audit.invalid_sqlite_metadata =
            vec!["reports/read.json:bench:missing-sqlite-query-plan".to_string()];
        audit.invalid_timing_statistics =
            vec!["reports/read.json:bench:median-mismatch".to_string()];
        audit.remediation_commands = vec![BenchmarkAuditRemediationCommand {
            kind: "generate-fixture".to_string(),
            profile: Some("scale-500k-balanced".to_string()),
            level: Some("large".to_string()),
            cache_temperature: None,
            command: "./target/release/fact-sim generate --profile scale-500k-balanced --seed 42 --target-propositions 500000 --output fixtures/scale-500k-balanced-large-seed-42".to_string(),
        }];
        std::fs::write(&audit_path, serde_json::to_vec_pretty(&audit)?)?;
        let growth_path = temp.path().join("growth-analysis.json");
        std::fs::write(
            &growth_path,
            serde_json::to_vec_pretty(&BenchmarkGrowthAnalysis {
                schema_version: REPORT_SCHEMA_VERSION.to_string(),
                generated_at_unix_ms: 0,
                baseline_summaries: Vec::new(),
                trend_count: 1,
                complete_trend_count: 0,
                insufficient_trend_count: 1,
                incorrect_trend_count: 1,
                failures: Vec::new(),
                trends: Vec::new(),
            })?,
        )?;
        let budget_check_path = temp.path().join("budget-check.json");
        std::fs::write(
            &budget_check_path,
            serde_json::to_vec_pretty(&BenchmarkBudgetCheck {
                schema_version: REPORT_SCHEMA_VERSION.to_string(),
                generated_at_unix_ms: 0,
                budgets: PathBuf::from("budgets.json"),
                baseline_summary: PathBuf::from("baseline-summary.json"),
                checked_count: 0,
                warning_count: 1,
                regression_count: 1,
                incorrect_count: 0,
                missing_budget_count: 1,
                failure_count: 0,
                passed: false,
                results: Vec::new(),
                missing_budgets: Vec::new(),
                failures: Vec::new(),
            })?,
        )?;
        let profile_path = temp.path().join("profile-plan.json");
        std::fs::write(
            &profile_path,
            serde_json::to_vec_pretty(&BenchmarkProfilePlan {
                schema_version: REPORT_SCHEMA_VERSION.to_string(),
                generated_at_unix_ms: 0,
                baseline_summaries: Vec::new(),
                limit: 1,
                candidate_count: 0,
                failure_count: 1,
                candidates: Vec::new(),
                failures: Vec::new(),
            })?,
        )?;

        let acceptance = accept_benchmark_baseline(&BenchmarkAcceptArgs {
            audit: audit_path,
            growth_analysis: growth_path,
            budget_check: budget_check_path,
            profile_plan: profile_path,
        })?;

        assert!(!acceptance.accepted);
        for blocker in [
            "readiness-audit-not-ready",
            "no-baseline-reports",
            "incomplete-growth-trends",
            "insufficient-growth-data",
            "incorrect-growth-trends",
            "budget-check-failed",
            "profile-plan-failures",
            "no-profile-candidates",
        ] {
            assert!(
                acceptance.blockers.contains(&blocker.to_string()),
                "missing blocker {blocker}"
            );
        }
        assert_eq!(
            acceptance.blocker_evidence.get("audit-missing-suites"),
            Some(&1)
        );
        assert_eq!(
            acceptance.blocker_evidence.get("audit-missing-areas"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-missing-requirements"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-missing-cli-workflows"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-missing-baseline-profile-levels"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-missing-cache-profile-levels"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-missing-fixture-levels"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-missing-fixture-profiles"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-missing-fixture-profile-levels"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-missing-benchmark-metadata"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-invalid-baseline-summaries"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-missing-report-files"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-invalid-report-files"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-missing-failure-logs"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-invalid-failure-logs"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-invalid-environment-metadata"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-missing-environment-manifests"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-missing-reproduce-commands"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-non-release-environment-manifests"),
            Some(&1)
        );
        assert_eq!(
            acceptance.blocker_evidence.get("audit-non-release-reports"),
            Some(&1)
        );
        assert_eq!(
            acceptance.blocker_evidence.get("audit-not-ready-summaries"),
            Some(&1)
        );
        assert_eq!(
            acceptance.blocker_evidence.get("audit-failing-summaries"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-fixture-metadata-mismatches"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-missing-fixture-metadata"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-invalid-report-counts"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-invalid-cache-labels"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-invalid-cache-metadata"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-insufficient-warmup-iterations"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-missing-report-metadata"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-invalid-requirement-tags"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-missing-bottleneck-summaries"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-invalid-bottleneck-summaries"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-insufficient-baseline-iterations"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-insufficient-sample-benchmarks"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-invalid-sample-observations"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-invalid-phase-metadata"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-invalid-resource-metadata"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-invalid-sqlite-metadata"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("audit-invalid-timing-statistics"),
            Some(&1)
        );
        assert_eq!(
            acceptance
                .blocker_evidence
                .get("growth-insufficient-trends"),
            Some(&1)
        );
        assert_eq!(
            acceptance.blocker_evidence.get("growth-incorrect-trends"),
            Some(&1)
        );
        assert_eq!(acceptance.blocker_evidence.get("budget-warnings"), Some(&1));
        assert_eq!(
            acceptance.blocker_evidence.get("budget-regressions"),
            Some(&1)
        );
        assert_eq!(acceptance.blocker_evidence.get("budget-missing"), Some(&1));
        assert_eq!(
            acceptance.blocker_evidence.get("profile-plan-failures"),
            Some(&1)
        );
        assert_eq!(acceptance.remediation_commands.len(), 1);
        assert_eq!(acceptance.remediation_commands[0].kind, "generate-fixture");
        assert!(
            acceptance.remediation_commands[0]
                .command
                .contains("--target-propositions 500000")
        );
        let acceptance_path = temp.path().join("acceptance.json");
        std::fs::write(&acceptance_path, serde_json::to_vec_pretty(&acceptance)?)?;
        let rendered = render_report_file(&acceptance_path)?;
        assert!(rendered.contains("## Remediation Commands"));
        assert!(rendered.contains("generate-fixture"));
        assert!(rendered.contains("--target-propositions 500000"));
        Ok(())
    }

    #[test]
    fn benchmark_plan_emits_required_fixture_matrix_commands() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let plan = benchmark_matrix_plan(&BenchmarkPlanArgs {
            suite: BenchmarkSuite::Read,
            fixture_base: temp.path().join("fixtures"),
            report_output: temp.path().join("reports"),
            seed: 7,
            iterations: 3,
            warmups: 1,
            cache_state: "warm-test".to_string(),
            include_large: false,
        })?;
        assert_eq!(plan.fixtures.len(), 12);
        assert_eq!(plan.levels.len(), 2);
        assert_eq!(plan.required_profiles.len(), 6);
        assert!(
            plan.fixture_inventory_command
                .contains("benchmark fixtures")
        );
        assert!(
            plan.fixture_inventory_command
                .contains("fixture-inventory.json")
        );
        assert!(plan.baseline_command.contains("--require-ready"));
        assert_eq!(plan.baseline_commands.len(), 2);
        assert!(plan.baseline_command.starts_with(RELEASE_FACT_SIM_COMMAND));
        assert!(plan.baseline_commands[0].starts_with(RELEASE_FACT_SIM_COMMAND));
        assert!(plan.baseline_commands[0].contains("--cache-state warm-filesystem"));
        assert!(plan.baseline_commands[0].contains("--output"));
        assert!(plan.baseline_commands[0].contains("/warm"));
        assert!(plan.baseline_commands[0].contains("warm-baseline-summary.json"));
        assert!(plan.baseline_commands[1].starts_with(RELEASE_FACT_SIM_COMMAND));
        assert!(plan.baseline_commands[1].contains("--cache-state cold-filesystem"));
        assert!(plan.baseline_commands[1].contains("--warmups 0"));
        assert!(plan.baseline_commands[1].contains("/cold"));
        assert!(plan.baseline_commands[1].contains("cold-baseline-summary.json"));
        assert!(plan.audit_command.starts_with(RELEASE_FACT_SIM_COMMAND));
        assert!(plan.audit_command.contains("warm-baseline-summary.json"));
        assert!(plan.audit_command.contains("cold-baseline-summary.json"));
        assert!(
            plan.growth_analysis_command
                .contains("warm-baseline-summary.json")
        );
        assert!(plan.budgets_command.contains("warm-baseline-summary.json"));
        assert!(plan.budget_check_command.contains("budgets.json"));
        assert!(plan.profile_plan_command.contains("profile-plan.json"));
        assert!(
            plan.compare_matrix_command
                .contains("benchmark compare-matrix")
        );
        assert!(
            plan.compare_matrix_command
                .contains("<accepted-baseline-summary.json>")
        );
        assert!(
            plan.compare_matrix_command
                .contains("warm-baseline-summary.json")
        );
        assert!(plan.compare_matrix_command.contains("comparison.json"));
        assert!(plan.acceptance_command.contains("acceptance.json"));
        assert_eq!(plan.levels[0].target_propositions, 10_000);
        assert_eq!(plan.levels[0].target_objects, 10_000);
        let small = plan
            .fixtures
            .iter()
            .find(|fixture| fixture.profile == "scale-500k-balanced" && fixture.level == "small")
            .context("missing small balanced plan")?;
        assert_eq!(small.target_propositions, 10_000);
        assert_eq!(small.target_objects, 10_000);
        assert!(small.estimated_total_objects.is_some());
        assert!(small.estimated_total_objects.unwrap() >= small.target_propositions);
        assert!(
            small
                .benchmark_command
                .starts_with(RELEASE_FACT_SIM_COMMAND)
        );
        assert!(small.benchmark_command.contains("--suite read"));
        assert!(
            small.generation["commands"]["generate"]
                .as_str()
                .unwrap_or_default()
                .starts_with(RELEASE_FACT_SIM_COMMAND)
        );
        assert!(
            small.generation["commands"]["generate"]
                .as_str()
                .unwrap_or_default()
                .contains("--target-propositions 10000")
        );
        assert!(plan.fixtures.iter().all(|fixture| fixture.level != "large"));
        let large_plan = benchmark_matrix_plan(&BenchmarkPlanArgs {
            suite: BenchmarkSuite::Read,
            fixture_base: temp.path().join("fixtures"),
            report_output: temp.path().join("reports"),
            seed: 7,
            iterations: 3,
            warmups: 1,
            cache_state: "warm-test".to_string(),
            include_large: true,
        })?;
        assert_eq!(large_plan.fixtures.len(), 18);
        assert_eq!(large_plan.levels.len(), 3);
        assert!(large_plan.baseline_command.contains("--include-large"));
        assert!(large_plan.audit_command.contains("--include-large"));
        let large = large_plan
            .fixtures
            .iter()
            .find(|fixture| {
                fixture.profile == "scale-500k-conflict-heavy" && fixture.level == "large"
            })
            .context("missing large conflict plan")?;
        assert_eq!(large.target_propositions, 500_000);
        assert_eq!(large.target_objects, 500_000);
        assert!(large.estimated_total_objects.is_some());
        assert!(large.estimated_total_objects.unwrap() >= large.target_propositions);
        let serialized = serde_json::to_value(&plan)?;
        assert_eq!(serialized["levels"][0]["target_propositions"], 10_000);
        assert!(
            serialized["compare_matrix_command"]
                .as_str()
                .unwrap_or_default()
                .contains("benchmark compare-matrix")
        );
        assert_eq!(serialized["fixtures"][0]["target_propositions"], 10_000);
        assert!(
            serialized["fixtures"][0]["estimated_total_objects"]
                .as_u64()
                .context("missing estimated total objects")?
                >= 10_000
        );
        Ok(())
    }

    #[test]
    fn process_benchmark_phase_breakdown_reports_cli_subphases() {
        let operation = BenchmarkOperation {
            name: "test_cli_process".to_string(),
            suite: BenchmarkSuite::Cli,
            area: "cli".to_string(),
            read_only: true,
            minimum_rows: Some(1),
            notes: Vec::new(),
            kind: BenchmarkOperationKind::Process {
                program: PathBuf::from("fact-sim"),
                args: vec!["status".to_string()],
            },
        };
        let phases = operation_phase_breakdown(&operation, 1.0, 2.0, 3.0);
        let phase_names = phases
            .iter()
            .map(|phase| phase.phase.as_str())
            .collect::<BTreeSet<_>>();

        assert!(phase_names.contains("setup"));
        assert!(phase_names.contains("warmup"));
        assert!(phase_names.contains("measurement"));
        assert!(phase_names.contains("process-startup"));
        assert!(phase_names.contains("configuration-loading"));
        assert!(phase_names.contains("core-operation"));
        assert!(phase_names.contains("output-formatting"));
        assert!(phases.iter().any(|phase| {
            phase.phase == "process-startup"
                && phase.measurement == BenchmarkPhaseMeasurement::IncludedInParent
                && phase.elapsed_ms.is_none()
        }));
    }

    #[test]
    fn sql_benchmark_phase_breakdown_reports_database_subphases() {
        let operation = BenchmarkOperation {
            name: "test_sql".to_string(),
            suite: BenchmarkSuite::Read,
            area: "read".to_string(),
            read_only: true,
            minimum_rows: Some(1),
            notes: Vec::new(),
            kind: BenchmarkOperationKind::Sql {
                database: PathBuf::from("fixture/ledger.sqlite"),
                sql: "SELECT 1".to_string(),
            },
        };
        let phases = operation_phase_breakdown(&operation, 1.0, 2.0, 3.0);
        let phase_names = phases
            .iter()
            .map(|phase| phase.phase.as_str())
            .collect::<BTreeSet<_>>();

        assert!(phase_names.contains("database-read"));
        assert!(phase_names.contains("query-plan-inspection"));
        assert!(phase_names.contains("result-materialization"));
        assert!(phases.iter().any(|phase| {
            phase.phase == "database-read"
                && phase.measurement == BenchmarkPhaseMeasurement::IncludedInParent
                && phase.elapsed_ms.is_none()
        }));
    }

    #[test]
    fn scenario_benchmark_phase_breakdown_reports_sdk_subphases() {
        let operation = BenchmarkOperation {
            name: "test_scenario".to_string(),
            suite: BenchmarkSuite::Core,
            area: "proposition-create".to_string(),
            read_only: false,
            minimum_rows: Some(1),
            notes: Vec::new(),
            kind: BenchmarkOperationKind::Scenario {
                yaml: sdk_propose_scenario(),
            },
        };
        let phases = operation_phase_breakdown(&operation, 1.0, 2.0, 3.0);
        let phase_names = phases
            .iter()
            .map(|phase| phase.phase.as_str())
            .collect::<BTreeSet<_>>();

        assert!(phase_names.contains("canonicalization"));
        assert!(phase_names.contains("signing"));
        assert!(phase_names.contains("database-writes"));
        assert!(phase_names.contains("projection-updates"));
        assert!(phase_names.contains("verification"));
    }

    #[test]
    fn file_inventory_can_report_network_payload_bytes() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let bundle = temp.path().join("bundle.factbndl");
        std::fs::write(&bundle, [1_u8, 2, 3, 4])?;
        let measurement = execute_operation(&BenchmarkOperation {
            name: "test_sync_bundle_inventory".to_string(),
            suite: BenchmarkSuite::Sync,
            area: "sync".to_string(),
            read_only: true,
            minimum_rows: Some(1),
            notes: Vec::new(),
            kind: BenchmarkOperationKind::FileInventory {
                root: temp.path().to_path_buf(),
                extensions: BTreeSet::from(["factbndl".to_string()]),
                bytes_kind: FileInventoryBytesKind::NetworkPayload,
            },
        })?;

        assert_eq!(measurement.rows, 1);
        assert_eq!(measurement.bytes, Some(4));
        assert_eq!(measurement.network_payload_bytes, Some(4));
        Ok(())
    }

    #[test]
    fn http_router_benchmark_reports_loopback_payload_bytes() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let database = temp.path().join("ledger.sqlite");
        let store = fact_store::Store::open(&database)?;
        drop(store);
        let measurement = execute_operation(&BenchmarkOperation {
            name: "http_local_loopback_ledger_list".to_string(),
            suite: BenchmarkSuite::Http,
            area: "http".to_string(),
            read_only: true,
            minimum_rows: Some(1),
            notes: Vec::new(),
            kind: BenchmarkOperationKind::HttpRouter {
                database,
                method: "GET".to_string(),
                path: "/facts/ledgers".to_string(),
                headers: BTreeMap::new(),
                expected_status: None,
                caller_auth: HttpCallerAuth::Reference,
                body: None,
            },
        })?;

        assert_eq!(measurement.rows, 1);
        assert!(measurement.bytes.unwrap_or_default() > 0);
        assert_eq!(measurement.network_payload_bytes, measurement.bytes);
        Ok(())
    }

    #[test]
    fn http_fixture_request_resolves_object_route_from_fixture_ids() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let database = temp.path().join("ledger.sqlite");
        let connection = rusqlite::Connection::open(&database)?;
        let ledger_id = uuid::Uuid::now_v7();
        let object_id = uuid::Uuid::now_v7();
        let content_hash = [2_u8; 32];
        connection.execute_batch(
            "CREATE TABLE protocol_object(
                object_id BLOB PRIMARY KEY,
                ledger_id BLOB,
                object_type TEXT,
                content_hash BLOB,
                payload BLOB,
                cose BLOB
            );",
        )?;
        connection.execute(
            "INSERT INTO protocol_object(object_id,ledger_id,object_type,content_hash,payload,cose)
             VALUES(?1,?2,'proposition',?3,x'03',x'04')",
            rusqlite::params![object_id.as_bytes(), ledger_id.as_bytes(), content_hash],
        )?;

        let request = http_fixture_request(&database, HttpFixtureRoute::ObjectFetch)?;

        assert_eq!(request.method, "GET");
        assert_eq!(
            request.path,
            format!("/facts/ledgers/{ledger_id}/objects/{object_id}")
        );
        assert_eq!(request.body, None);
        let batch = http_fixture_request(&database, HttpFixtureRoute::BatchFetch)?;
        assert_eq!(batch.method, "POST");
        assert_eq!(
            batch.path,
            format!("/facts/ledgers/{ledger_id}/objects:fetch")
        );
        assert_eq!(
            batch.headers.get("content-type").map(String::as_str),
            Some("application/fact+json")
        );
        assert!(batch.headers.contains_key("content-digest"));
        assert!(
            batch
                .body
                .as_deref()
                .is_some_and(|body| body.contains("facts-protocol-fetch-v0"))
        );
        let pull = http_fixture_request(&database, HttpFixtureRoute::Pull)?;
        assert_eq!(pull.method, "POST");
        assert_eq!(
            pull.path,
            format!("/facts/ledgers/{ledger_id}/objects:pull")
        );
        assert_eq!(
            pull.headers.get("content-type").map(String::as_str),
            Some("application/fact+json")
        );
        assert!(pull.headers.contains_key("content-digest"));
        assert!(pull.body.as_deref().is_some_and(|body| {
            body.contains("facts-protocol-pull-v0") && body.contains("\"prefer_snapshot\":false")
        }));
        let push = http_fixture_request(&database, HttpFixtureRoute::MalformedPushPayload)?;
        assert_eq!(push.method, "POST");
        assert_eq!(
            push.path,
            format!("/facts/ledgers/{ledger_id}/objects:push")
        );
        assert_eq!(push.expected_status, Some(400));
        assert!(matches!(push.caller_auth, HttpCallerAuth::Disabled));
        assert_eq!(
            push.headers.get("content-type").map(String::as_str),
            Some("application/fact-bundle")
        );
        assert_eq!(
            push.headers.get("content-digest"),
            Some(&content_digest(b""))
        );
        let auth = http_fixture_request(&database, HttpFixtureRoute::AuthChallenge)?;
        assert_eq!(auth.method, "POST");
        assert_eq!(
            auth.path,
            format!("/facts/ledgers/{ledger_id}/objects:push")
        );
        assert_eq!(auth.expected_status, Some(401));
        assert_eq!(
            auth.headers.get("content-type").map(String::as_str),
            Some("application/fact-bundle")
        );
        assert!(auth.headers.contains_key("content-digest"));
        let invalid_digest = http_fixture_request(&database, HttpFixtureRoute::InvalidDigest)?;
        assert_eq!(invalid_digest.method, "POST");
        assert_eq!(
            invalid_digest.path,
            format!("/facts/ledgers/{ledger_id}/objects:fetch")
        );
        assert_eq!(invalid_digest.expected_status, Some(400));
        assert_ne!(
            invalid_digest.headers.get("content-digest"),
            Some(&content_digest(b""))
        );
        Ok(())
    }

    #[test]
    fn http_suite_uses_router_routes_when_fixture_ids_are_available() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture = temp.path().join("fixture");
        std::fs::create_dir(&fixture)?;
        let database = fixture.join("ledger.sqlite");
        let connection = rusqlite::Connection::open(&database)?;
        let ledger_id = uuid::Uuid::now_v7();
        let object_id = uuid::Uuid::now_v7();
        let content_hash = [2_u8; 32];
        connection.execute_batch(
            "CREATE TABLE protocol_object(
                object_id BLOB PRIMARY KEY,
                ledger_id BLOB,
                object_type TEXT,
                content_hash BLOB,
                payload BLOB,
                cose BLOB
            );",
        )?;
        connection.execute(
            "INSERT INTO protocol_object(object_id,ledger_id,object_type,content_hash,payload,cose)
             VALUES(?1,?2,'proposition',?3,x'03',x'04')",
            rusqlite::params![object_id.as_bytes(), ledger_id.as_bytes(), content_hash],
        )?;

        let operations = operations_for_suite(BenchmarkSuite::Http, &fixture, &database)?;
        let http_routes = operations
            .iter()
            .filter_map(|operation| match &operation.kind {
                BenchmarkOperationKind::HttpFixtureRoute { route, .. } => {
                    Some((operation.name.as_str(), *route))
                }
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(http_routes.len(), 8);
        assert!(http_routes.iter().any(|(name, route)| {
            *name == "http_local_object_fetch_shape"
                && matches!(route, HttpFixtureRoute::ObjectFetch)
        }));
        assert!(http_routes.iter().any(|(name, route)| {
            *name == "http_local_batch_fetch_shape" && matches!(route, HttpFixtureRoute::BatchFetch)
        }));
        assert!(http_routes.iter().any(|(name, route)| {
            *name == "http_local_query_shape" && matches!(route, HttpFixtureRoute::Query)
        }));
        assert!(http_routes.iter().any(|(name, route)| {
            *name == "http_local_pull_negotiation_shape" && matches!(route, HttpFixtureRoute::Pull)
        }));
        assert!(http_routes.iter().any(|(name, route)| {
            *name == "http_local_push_payload_shape"
                && matches!(route, HttpFixtureRoute::MalformedPushPayload)
        }));
        assert!(http_routes.iter().any(|(name, route)| {
            *name == "http_local_authentication_overhead_shape"
                && matches!(route, HttpFixtureRoute::AuthChallenge)
        }));
        assert!(http_routes.iter().any(|(name, route)| {
            *name == "http_local_digest_signature_overhead_shape"
                && matches!(route, HttpFixtureRoute::InvalidDigest)
        }));
        assert!(http_routes.iter().any(|(name, route)| {
            *name == "http_local_commitment_retrieval_shape"
                && matches!(route, HttpFixtureRoute::Commitment)
        }));
        assert!(operations.iter().any(|operation| operation.name
            == "http_local_capability_negotiation_shape"
            && matches!(operation.kind, BenchmarkOperationKind::HttpRouter { .. })));
        Ok(())
    }

    #[test]
    fn sdk_scenario_benchmark_runs_with_assertions() -> Result<()> {
        let measurement = execute_operation(&BenchmarkOperation {
            name: "test_sdk_reject_temp".to_string(),
            suite: BenchmarkSuite::Core,
            area: "accept-reject".to_string(),
            read_only: false,
            minimum_rows: Some(1),
            notes: Vec::new(),
            kind: BenchmarkOperationKind::Scenario {
                yaml: sdk_reject_scenario(),
            },
        })?;
        assert!(measurement.rows >= 4);
        Ok(())
    }

    #[test]
    fn suite_scenario_benchmarks_run_with_assertions() -> Result<()> {
        for (suite, area, yaml) in [
            (
                BenchmarkSuite::Sync,
                "sync",
                sdk_local_sync_scenario() as &'static str,
            ),
            (
                BenchmarkSuite::Rebuild,
                "rebuild",
                sdk_projection_rebuild_scenario(),
            ),
            (
                BenchmarkSuite::Conflict,
                "conflict-state",
                sdk_parallel_deliberation_conflict_scenario(),
            ),
        ] {
            let measurement = execute_operation(&BenchmarkOperation {
                name: format!("test_{area}"),
                suite,
                area: area.to_string(),
                read_only: false,
                minimum_rows: Some(1),
                notes: Vec::new(),
                kind: BenchmarkOperationKind::Scenario { yaml },
            })?;
            assert!(measurement.rows >= 4);
        }
        Ok(())
    }

    #[test]
    fn representative_benchmark_catalog_matches_generated_operations() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture = write_complete_benchmark_operation_fixture(temp.path())?;
        let database = fixture.join("ledger.sqlite");

        for suite in required_benchmark_suites() {
            let operations = operations_for_suite(suite, &fixture, &database)?;
            let operation_names = operations
                .iter()
                .map(|operation| operation.name.as_str())
                .collect::<BTreeSet<_>>();
            let missing = representative_suite_benchmarks(suite)
                .into_iter()
                .filter(|name| !operation_names.contains(name.as_str()))
                .collect::<Vec<_>>();

            assert!(
                missing.is_empty(),
                "suite `{}` does not generate declared representative benchmarks: {}",
                suite_slug(suite),
                missing.join(", ")
            );
        }
        Ok(())
    }

    #[test]
    fn core_suite_emits_ledger_startup_metadata_benchmarks() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture = temp.path().join("fixture");
        std::fs::create_dir(&fixture)?;
        let database = fixture.join("ledger.sqlite");
        let connection = rusqlite::Connection::open(&database)?;
        connection.execute_batch(
            "
            CREATE TABLE protocol_object(object_id BLOB PRIMARY KEY, object_type TEXT, content_hash BLOB);
            CREATE TABLE projected_effective(proposition_id BLOB, status TEXT, revision_id BLOB);
            CREATE TABLE projected_consensus(deliberation_id BLOB, consensus TEXT, applicable_decision_count INTEGER);
            CREATE TABLE search_document(rowid INTEGER PRIMARY KEY, term TEXT NOT NULL);
            INSERT INTO protocol_object(object_id,object_type,content_hash) VALUES(x'01','proposition',x'02');
            INSERT INTO protocol_object(object_id,object_type,content_hash) VALUES(x'02','decision',x'03');
            INSERT INTO protocol_object(object_id,object_type,content_hash) VALUES(x'03','settlement',x'04');
            INSERT INTO projected_effective(proposition_id,status,revision_id) VALUES(x'01','accepted',x'02');
            INSERT INTO projected_effective(proposition_id,status,revision_id) VALUES(x'02','rejected',x'03');
            INSERT INTO projected_effective(proposition_id,status,revision_id) VALUES(x'03','pending',x'04');
            INSERT INTO projected_consensus(deliberation_id,consensus,applicable_decision_count) VALUES(x'01','accepted',1);
            INSERT INTO projected_consensus(deliberation_id,consensus,applicable_decision_count) VALUES(x'02','rejected',2);
            INSERT INTO search_document(term) VALUES('policy');
            ",
        )?;

        let operations = operations_for_suite(BenchmarkSuite::Core, &fixture, &database)?;
        let operation_names = operations
            .iter()
            .map(|operation| operation.name.as_str())
            .collect::<BTreeSet<_>>();
        for expected in [
            "core_ledger_schema_table_inventory",
            "core_projection_metadata_inventory",
            "core_search_index_metadata_inventory",
            "core_content_hash_prefix_lookup_shape",
            "core_effective_full_id_lookup_shape",
            "core_effective_short_reference_lookup_shape",
            "core_effective_revision_id_lookup_shape",
            "core_effective_pending_status_lookup_shape",
            "core_effective_lifecycle_state_lookup_shape",
            "core_decision_object_page",
            "core_settlement_evidence_page",
            "core_effective_rejected_transition_page",
            "core_consensus_group_size_shape",
            "core_decision_conflict_acceptance_shape",
        ] {
            assert!(
                operation_names.contains(expected),
                "missing core startup benchmark `{expected}`"
            );
        }
        Ok(())
    }

    #[test]
    fn search_suite_emits_index_result_shape_benchmarks() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture = temp.path().join("fixture");
        std::fs::create_dir(&fixture)?;
        let database = fixture.join("ledger.sqlite");
        let connection = rusqlite::Connection::open(&database)?;
        connection.execute_batch(
            "
            CREATE TABLE search_document(
                rowid INTEGER PRIMARY KEY,
                term TEXT NOT NULL,
                status TEXT,
                is_effective INTEGER,
                revision_id BLOB,
                superseded INTEGER
            );
            CREATE TABLE protocol_object(object_id BLOB PRIMARY KEY, payload BLOB);
            INSERT INTO search_document(term,status,is_effective,revision_id,superseded)
                VALUES('policy','accepted',1,x'01',0);
            INSERT INTO search_document(term,status,is_effective,revision_id,superseded)
                VALUES('policy','archived',0,x'02',1);
            INSERT INTO search_document(term,status,is_effective,revision_id,superseded)
                VALUES('rare','withdrawn',1,x'03',0);
            INSERT INTO protocol_object(object_id,payload) VALUES(x'01','policy payload');
            ",
        )?;

        let operations = operations_for_suite(BenchmarkSuite::Search, &fixture, &database)?;
        let operation_names = operations
            .iter()
            .map(|operation| operation.name.as_str())
            .collect::<BTreeSet<_>>();
        for expected in [
            "search_index_count",
            "search_first_page",
            "search_index_large_result_shape",
            "search_index_no_result_shape",
            "search_index_paginated_result_shape",
            "search_lifecycle_status_filter_shape",
            "search_effective_content_filter_shape",
            "search_later_revision_term_shape",
            "search_removed_effective_revision_term_shape",
            "search_payload_common_term",
            "search_payload_no_result",
        ] {
            assert!(
                operation_names.contains(expected),
                "missing indexed search benchmark `{expected}`"
            );
        }
        Ok(())
    }

    #[test]
    fn search_suite_uses_fact_store_search_when_ledger_metadata_is_available() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture = temp.path().join("fixture");
        std::fs::create_dir(&fixture)?;
        let database = fixture.join("ledger.sqlite");
        let connection = rusqlite::Connection::open(&database)?;
        let ledger_id = uuid::Uuid::now_v7();
        let object_id = uuid::Uuid::now_v7();
        connection.execute_batch(
            "
            CREATE TABLE search_document(
                rowid INTEGER PRIMARY KEY,
                term TEXT NOT NULL,
                status TEXT,
                is_effective INTEGER,
                revision_id BLOB,
                superseded INTEGER
            );
            CREATE TABLE protocol_object(
                object_id BLOB PRIMARY KEY,
                ledger_id BLOB,
                payload BLOB
            );
            INSERT INTO search_document(term,status,is_effective,revision_id,superseded)
                VALUES('policy','accepted',1,x'01',0);
            ",
        )?;
        connection.execute(
            "INSERT INTO protocol_object(object_id,ledger_id,payload) VALUES(?1,?2,'policy payload')",
            rusqlite::params![object_id.as_bytes(), ledger_id.as_bytes()],
        )?;

        let operations = operations_for_suite(BenchmarkSuite::Search, &fixture, &database)?;

        let common = operations
            .iter()
            .find(|operation| operation.name == "search_payload_common_term")
            .context("missing common-term search benchmark")?;
        let BenchmarkOperationKind::SearchIndex {
            ledger_id: operation_ledger,
            query,
            limit,
            ..
        } = &common.kind
        else {
            bail!("common-term search benchmark did not use fact-store search");
        };
        assert_eq!(*operation_ledger, ledger_id);
        assert_eq!(query, "policy");
        assert_eq!(*limit, 50);

        let no_result = operations
            .iter()
            .find(|operation| operation.name == "search_payload_no_result")
            .context("missing no-result search benchmark")?;
        assert!(matches!(
            no_result.kind,
            BenchmarkOperationKind::SearchIndex { .. }
        ));
        Ok(())
    }

    #[test]
    fn read_suite_emits_pagination_and_depth_shape_benchmarks() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture = temp.path().join("fixture");
        std::fs::create_dir(&fixture)?;
        let database = fixture.join("ledger.sqlite");
        let connection = rusqlite::Connection::open(&database)?;
        connection.execute_batch(
            "
            CREATE TABLE projected_effective(proposition_id BLOB, status TEXT, revision_id BLOB);
            CREATE TABLE projected_pending(ledger_id BLOB, actor_id BLOB, pending_count INTEGER);
            CREATE TABLE projected_revision(proposition_id BLOB, revision_id BLOB, parent_revision_id BLOB);
            CREATE TABLE projected_consensus(deliberation_id BLOB, consensus TEXT, applicable_decision_count INTEGER);
            CREATE TABLE protocol_object(object_id BLOB PRIMARY KEY, object_type TEXT, content_hash BLOB);
            INSERT INTO projected_effective(proposition_id,status,revision_id) VALUES(x'01','accepted',x'02');
            INSERT INTO projected_pending(ledger_id,actor_id,pending_count) VALUES(x'01',x'01',3);
            INSERT INTO projected_pending(ledger_id,actor_id,pending_count) VALUES(x'02',x'02',0);
            INSERT INTO projected_revision(proposition_id,revision_id,parent_revision_id) VALUES(x'01',x'02',NULL);
            INSERT INTO projected_revision(proposition_id,revision_id,parent_revision_id) VALUES(x'01',x'03',x'02');
            INSERT INTO projected_consensus(deliberation_id,consensus,applicable_decision_count) VALUES(x'01','accepted',1);
            INSERT INTO protocol_object(object_id,object_type,content_hash) VALUES(x'01','proposition',x'02');
            INSERT INTO protocol_object(object_id,object_type,content_hash) VALUES(x'02','revision',x'03');
            INSERT INTO protocol_object(object_id,object_type,content_hash) VALUES(x'03','decision',x'04');
            INSERT INTO protocol_object(object_id,object_type,content_hash) VALUES(x'04','comment',x'05');
            INSERT INTO protocol_object(object_id,object_type,content_hash) VALUES(x'05','settlement',x'06');
            ",
        )?;

        let operations = operations_for_suite(BenchmarkSuite::Read, &fixture, &database)?;
        let operation_names = operations
            .iter()
            .map(|operation| operation.name.as_str())
            .collect::<BTreeSet<_>>();
        for expected in [
            "read_list_offset_page",
            "read_pending_large_page",
            "read_pending_no_work_actor_shape",
            "read_pending_many_actions_shape",
            "read_pending_multi_ledger_shape",
            "read_revision_history_deep_shape",
            "read_revision_parentless_lookup_shape",
            "read_revision_short_chain_shape",
            "read_revision_medium_chain_shape",
            "read_revision_deep_chain_shape",
            "read_deliberation_large_page",
            "read_deliberation_decision_object_page",
            "read_deliberation_comment_object_page",
            "read_deliberation_settlement_evidence_page",
            "read_history_ledger_wide_page",
            "read_history_paginated_object_page",
            "read_history_object_type_filter",
            "read_history_proposition_scoped_page",
            "read_history_high_activity_proposition",
        ] {
            assert!(
                operation_names.contains(expected),
                "missing read benchmark `{expected}`"
            );
        }
        Ok(())
    }

    #[test]
    fn sync_suite_emits_bundle_import_and_dependency_shape_benchmarks() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture = temp.path().join("fixture");
        std::fs::create_dir(&fixture)?;
        let bundles = fixture.join("bundles");
        std::fs::create_dir(&bundles)?;
        std::fs::write(bundles.join("objects.factbndl"), [1_u8, 2, 3, 4])?;
        let database = fixture.join("ledger.sqlite");
        let connection = rusqlite::Connection::open(&database)?;
        connection.execute_batch(
            "
            CREATE TABLE protocol_object(object_id BLOB PRIMARY KEY, content_hash BLOB, payload BLOB, cose BLOB);
            CREATE TABLE object_dependency(object_id BLOB, dependency_id BLOB, content_hash BLOB, role TEXT);
            INSERT INTO protocol_object(object_id,content_hash,payload,cose) VALUES(x'01',x'02',x'03',x'04');
            INSERT INTO object_dependency(object_id,dependency_id,content_hash,role) VALUES(x'02',x'03',x'04','test');
            ",
        )?;

        let operations = operations_for_suite(BenchmarkSuite::Sync, &fixture, &database)?;
        let operation_names = operations
            .iter()
            .map(|operation| operation.name.as_str())
            .collect::<BTreeSet<_>>();
        for expected in [
            "sync_dependency_closure_scan",
            "sync_dependency_batch_shape",
            "sync_divergent_peer_dependency_frontier_shape",
            "sync_duplicate_delivery_shape",
            "sync_initial_full_payload_shape",
            "sync_missing_hash_negotiation_shape",
            "sync_incremental_batch_payload_shape",
            "sync_medium_batch_payload_shape",
            "sync_large_batch_payload_shape",
            "sync_fully_synchronized_peer_noop_shape",
            "sync_bundle_inventory_metadata",
            "sync_local_import_payload_inventory",
        ] {
            assert!(
                operation_names.contains(expected),
                "missing sync benchmark `{expected}`"
            );
        }
        Ok(())
    }

    #[test]
    fn rebuild_suite_emits_projection_and_search_index_size_benchmarks() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture = temp.path().join("fixture");
        std::fs::create_dir(&fixture)?;
        let database = fixture.join("ledger.sqlite");
        let connection = rusqlite::Connection::open(&database)?;
        connection.execute_batch(
            "
            CREATE TABLE projected_effective(proposition_id BLOB, status TEXT, revision_id BLOB);
            CREATE TABLE projected_pending(actor_id BLOB, pending_count INTEGER);
            CREATE TABLE search_document(rowid INTEGER PRIMARY KEY, term TEXT NOT NULL);
            INSERT INTO projected_effective(proposition_id,status,revision_id) VALUES(x'01','accepted',x'02');
            INSERT INTO projected_pending(actor_id,pending_count) VALUES(x'01',1);
            INSERT INTO search_document(term) VALUES('policy');
            ",
        )?;

        let operations = operations_for_suite(BenchmarkSuite::Rebuild, &fixture, &database)?;
        let operation_names = operations
            .iter()
            .map(|operation| operation.name.as_str())
            .collect::<BTreeSet<_>>();
        for expected in [
            "rebuild_effective_state_recalculation_shape",
            "rebuild_pending_projection_update_shape",
            "rebuild_batched_projection_table_inventory",
            "rebuild_projection_table_bytes",
            "rebuild_search_index_table_bytes",
            "rebuild_search_index_update_shape",
        ] {
            assert!(
                operation_names.contains(expected),
                "missing rebuild benchmark `{expected}`"
            );
        }
        Ok(())
    }

    #[test]
    fn conflict_suite_emits_conflict_state_shape_benchmarks() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture = temp.path().join("fixture");
        std::fs::create_dir(&fixture)?;
        let database = fixture.join("ledger.sqlite");
        let connection = rusqlite::Connection::open(&database)?;
        connection.execute_batch(
            "
            CREATE TABLE projected_effective(proposition_id BLOB, status TEXT, revision_id BLOB);
            CREATE TABLE projected_revision(proposition_id BLOB, revision_id BLOB, parent_revision_id BLOB);
            CREATE TABLE projected_consensus(deliberation_id BLOB, consensus TEXT, applicable_decision_count INTEGER);
            CREATE TABLE protocol_object(object_id BLOB PRIMARY KEY, content_hash BLOB, object_type TEXT);
            INSERT INTO projected_effective(proposition_id,status,revision_id) VALUES(x'01','contested',x'02');
            INSERT INTO projected_revision(proposition_id,revision_id,parent_revision_id) VALUES(x'01',x'02',x'00');
            INSERT INTO projected_revision(proposition_id,revision_id,parent_revision_id) VALUES(x'01',x'03',x'00');
            INSERT INTO projected_consensus(deliberation_id,consensus,applicable_decision_count) VALUES(x'01','contested',2);
            INSERT INTO protocol_object(object_id,content_hash,object_type) VALUES(x'01',x'02','settlement');
            ",
        )?;

        let operations = operations_for_suite(BenchmarkSuite::Conflict, &fixture, &database)?;
        let operation_names = operations
            .iter()
            .map(|operation| operation.name.as_str())
            .collect::<BTreeSet<_>>();
        for expected in [
            "conflict_sibling_revision_detection",
            "conflict_last_undisputed_ancestor_lookup",
            "conflict_effective_rejected_or_contested_page",
            "conflict_contested_state_lookup",
            "conflict_consensus_disagreement_page",
            "conflict_decision_conflict_detection",
            "conflict_reconciliation_object_inspection",
        ] {
            assert!(
                operation_names.contains(expected),
                "missing conflict benchmark `{expected}`"
            );
        }
        Ok(())
    }

    #[test]
    fn cli_check_scenario_benchmark_runs_when_fact_binary_exists() -> Result<()> {
        fact_sim_dsl::Scenario::from_yaml_str(sdk_cli_check_scenario())?;
        if fact_cli_binary().is_some() {
            let measurement = execute_operation(&BenchmarkOperation {
                name: "test_cli_check".to_string(),
                suite: BenchmarkSuite::Cli,
                area: "cli".to_string(),
                read_only: false,
                minimum_rows: Some(1),
                notes: Vec::new(),
                kind: BenchmarkOperationKind::Scenario {
                    yaml: sdk_cli_check_scenario(),
                },
            })?;
            assert!(measurement.rows >= 10);
        }
        Ok(())
    }

    #[test]
    fn cli_suite_emits_distinct_fact_workflow_benchmarks() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture = temp.path().join("fixture");
        write_inventory_fixture_with_profile(temp.path(), "fixture", "profile", 1)?;
        let database = fixture.join("ledger.sqlite");
        let operations = operations_for_suite(BenchmarkSuite::Cli, &fixture, &database)?;
        let operation_names = operations
            .iter()
            .map(|operation| operation.name.clone())
            .collect::<BTreeSet<_>>();
        for workflow in required_cli_workflows() {
            let workflow_name = format!("cli_fact_{workflow}_process");
            assert!(
                operation_names.contains(&workflow_name),
                "missing CLI workflow benchmark `{workflow_name}`"
            );
        }
        Ok(())
    }

    #[test]
    fn cli_suite_uses_process_fallback_when_fact_binary_is_absent() -> Result<()> {
        let mut operations = Vec::new();
        let fixture = PathBuf::from("fixtures/example");
        let executable = PathBuf::from("fact-sim");

        push_fact_cli_workflow_operations_with_binary(
            &mut operations,
            BenchmarkSuite::Cli,
            &executable,
            &fixture,
            None,
        );

        let status = operations
            .iter()
            .find(|operation| operation.name == "cli_fact_status_process")
            .context("missing status fallback operation")?;
        assert!(status.read_only);
        assert!(matches!(
            &status.kind,
            BenchmarkOperationKind::Process { program, args }
                if program == &executable
                    && args == &vec![
                        "inspect".to_string(),
                        fixture.display().to_string()
                    ]
        ));
        Ok(())
    }

    #[test]
    fn fact_cli_workflows_run_when_binary_exists() -> Result<()> {
        if let Some(program) = fact_cli_binary() {
            for workflow in required_cli_workflows() {
                let measurement = execute_cli_workflow_operation(&program, &workflow)
                    .with_context(|| format!("fact CLI workflow `{workflow}` failed"))?;
                assert!(
                    measurement.rows > 0,
                    "workflow `{workflow}` returned no rows"
                );
                assert!(
                    measurement.bytes.is_some_and(|bytes| bytes > 0),
                    "workflow `{workflow}` returned no bytes"
                );
            }
        }
        Ok(())
    }

    fn write_complete_benchmark_operation_fixture(base: &Path) -> Result<PathBuf> {
        let fixture = base.join("fixture");
        std::fs::create_dir(&fixture)?;
        std::fs::write(
            fixture.join("manifest.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "profile": "scale-500k-balanced",
                "seed": 42,
                "proposition_count": 10_000,
                "object_count": 50_000,
                "projected_row_count": 20_000,
                "search_index_row_count": 5_000,
                "search_index_size_bytes": 320_000
            }))?,
        )?;
        let bundles = fixture.join("bundles");
        std::fs::create_dir(&bundles)?;
        std::fs::write(bundles.join("objects.factbndl"), [1_u8, 2, 3, 4])?;
        let snapshots = fixture.join("snapshots");
        std::fs::create_dir(&snapshots)?;
        std::fs::write(snapshots.join("object-set.json"), b"{}")?;

        let database = fixture.join("ledger.sqlite");
        let connection = rusqlite::Connection::open(&database)?;
        connection.execute_batch(
            "
            CREATE TABLE protocol_object(
                object_id BLOB PRIMARY KEY,
                object_type TEXT,
                content_hash BLOB,
                payload BLOB,
                cose BLOB
            );
            CREATE TABLE object_dependency(
                object_id BLOB,
                dependency_id BLOB,
                content_hash BLOB,
                role TEXT
            );
            CREATE TABLE projected_effective(
                proposition_id BLOB,
                status TEXT,
                revision_id BLOB
            );
            CREATE TABLE projected_pending(
                ledger_id BLOB,
                actor_id BLOB,
                pending_count INTEGER
            );
            CREATE TABLE projected_revision(
                proposition_id BLOB,
                revision_id BLOB,
                parent_revision_id BLOB
            );
            CREATE TABLE projected_consensus(
                deliberation_id BLOB,
                consensus TEXT,
                applicable_decision_count INTEGER
            );
            CREATE TABLE search_document(
                rowid INTEGER PRIMARY KEY,
                term TEXT NOT NULL,
                status TEXT,
                is_effective INTEGER,
                revision_id BLOB,
                superseded INTEGER
            );
            INSERT INTO protocol_object(object_id,object_type,content_hash,payload,cose)
                VALUES(x'01','proposition',x'11','policy payload',x'21');
            INSERT INTO protocol_object(object_id,object_type,content_hash,payload,cose)
                VALUES(x'02','revision',x'12','revision payload',x'22');
            INSERT INTO protocol_object(object_id,object_type,content_hash,payload,cose)
                VALUES(x'03','decision',x'13','decision payload',x'23');
            INSERT INTO protocol_object(object_id,object_type,content_hash,payload,cose)
                VALUES(x'04','settlement',x'14','settlement payload',x'24');
            INSERT INTO protocol_object(object_id,object_type,content_hash,payload,cose)
                VALUES(x'05','comment',x'15','comment payload',x'25');
            INSERT INTO protocol_object(object_id,object_type,content_hash,payload,cose)
                VALUES(x'06','participant',x'16','participant payload',x'26');
            INSERT INTO protocol_object(object_id,object_type,content_hash,payload,cose)
                VALUES(x'07','permission',x'17','permission payload',x'27');
            INSERT INTO object_dependency(object_id,dependency_id,content_hash,role)
                VALUES(x'02',x'01',x'11','parent');
            INSERT INTO projected_effective(proposition_id,status,revision_id)
                VALUES(x'01','accepted',x'02');
            INSERT INTO projected_effective(proposition_id,status,revision_id)
                VALUES(x'02','rejected',x'03');
            INSERT INTO projected_effective(proposition_id,status,revision_id)
                VALUES(x'03','pending',x'04');
            INSERT INTO projected_effective(proposition_id,status,revision_id)
                VALUES(x'04','contested',x'05');
            INSERT INTO projected_pending(ledger_id,actor_id,pending_count)
                VALUES(x'01',x'01',3);
            INSERT INTO projected_pending(ledger_id,actor_id,pending_count)
                VALUES(x'02',x'02',0);
            INSERT INTO projected_revision(proposition_id,revision_id,parent_revision_id)
                VALUES(x'01',x'02',NULL);
            INSERT INTO projected_revision(proposition_id,revision_id,parent_revision_id)
                VALUES(x'01',x'03',x'02');
            INSERT INTO projected_revision(proposition_id,revision_id,parent_revision_id)
                VALUES(x'01',x'04',x'02');
            INSERT INTO projected_consensus(deliberation_id,consensus,applicable_decision_count)
                VALUES(x'01','accepted',1);
            INSERT INTO projected_consensus(deliberation_id,consensus,applicable_decision_count)
                VALUES(x'02','contested',2);
            INSERT INTO search_document(term,status,is_effective,revision_id,superseded)
                VALUES('policy','accepted',1,x'02',0);
            INSERT INTO search_document(term,status,is_effective,revision_id,superseded)
                VALUES('policy','archived',0,x'03',1);
            ",
        )?;
        Ok(fixture)
    }

    fn write_inventory_fixture(base: &Path, name: &str, propositions: u64) -> Result<()> {
        write_inventory_fixture_with_profile(base, name, name, propositions)
    }

    fn write_inventory_fixture_with_profile(
        base: &Path,
        name: &str,
        profile: &str,
        propositions: u64,
    ) -> Result<()> {
        let fixture = base.join(name);
        std::fs::create_dir(&fixture)?;
        std::fs::write(
            fixture.join("manifest.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "profile": profile,
                "seed": 42,
                "proposition_count": propositions,
                "object_count": propositions * 5,
                "projected_row_count": propositions * 2,
                "search_index_row_count": propositions / 2,
                "search_index_size_bytes": propositions * 32
            }))?,
        )?;
        let database = fixture.join("ledger.sqlite");
        let connection = rusqlite::Connection::open(&database)?;
        connection.execute_batch("CREATE TABLE metadata(id INTEGER PRIMARY KEY);")?;
        Ok(())
    }

    #[test]
    fn benchmark_database_prefers_proposition_rich_fixture_database() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let fixture = temp.path().join("fixture");
        std::fs::create_dir(&fixture)?;
        std::fs::write(
            fixture.join("manifest.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "profile": "scale-500k-balanced",
                "seed": 42
            }))?,
        )?;
        let ledger_dir = fixture.join("ledgers");
        std::fs::create_dir(&ledger_dir)?;
        let helper = ledger_dir.join("bulk_0_a.sqlite");
        rusqlite::Connection::open(&helper)?.execute_batch(
            "CREATE TABLE protocol_object(object_id BLOB PRIMARY KEY, object_type TEXT NOT NULL);",
        )?;
        let operations = ledger_dir.join("operations_a.sqlite");
        let connection = rusqlite::Connection::open(&operations)?;
        connection.execute_batch(
            "CREATE TABLE protocol_object(object_id BLOB PRIMARY KEY, object_type TEXT NOT NULL);",
        )?;
        connection.execute(
            "INSERT INTO protocol_object(object_id,object_type) VALUES(x'01','proposition')",
            [],
        )?;

        let metadata = FixtureMetadata::from_fixture(&fixture)?;
        assert_eq!(benchmark_database_for_fixture(&metadata)?, operations);
        Ok(())
    }

    fn valid_actor_payload(index: u8) -> Result<Vec<u8>> {
        let suffix = format!("{:012}", index + 1);
        Ok(fact_canonical::encode(&serde_json::to_vec(
            &serde_json::json!({
                "id": format!("01900000-0000-7000-8000-{suffix}"),
                "object_type": "actor",
                "schema_version": "0",
                "actor_id": format!("01900000-0000-7000-8000-{suffix}"),
                "signing_key_id": "01900000-0000-7000-8000-000000000100",
                "created_at": "2026-07-30T12:00:00.000Z",
                "dependencies": [],
                "body": {
                    "actor_type": "agent",
                    "bootstrap_key_id": "01900000-0000-7000-8000-000000000100",
                    "bootstrap_binding_id": "01900000-0000-7000-8000-000000000200"
                }
            }),
        )?)?)
    }

    fn signed_actor_cose(payload: &[u8]) -> Result<Vec<u8>> {
        let key = fact_crypto::SigningKey::from_seed(&[7_u8; 32])?;
        let protected = fact_crypto::protocol_protected(key.public_key(), "actor", "0", None);
        Ok(fact_crypto::encode_sign1(&fact_crypto::sign1(
            &protected, payload, &key,
        )))
    }

    fn test_preconditions() -> Vec<String> {
        vec!["test fixture is initialized".to_string()]
    }

    fn test_isolation_strategy() -> String {
        "test benchmark is isolated".to_string()
    }

    fn test_sqlite_diagnostics() -> SqliteDiagnostics {
        SqliteDiagnostics {
            database: PathBuf::from("fixture/ledger.sqlite"),
            database_size_bytes: 1024,
            page_count: Some(1),
            page_size: Some(4096),
            journal_mode: Some("wal".to_string()),
            query_plan: vec!["0:0: SCAN protocol_object".to_string()],
            uses_full_scan: true,
            uses_temporary_btree: false,
        }
    }

    fn test_phase_breakdown(measurement_total_ms: f64) -> Vec<BenchmarkPhaseBreakdown> {
        vec![
            BenchmarkPhaseBreakdown {
                phase: "setup".to_string(),
                measurement: BenchmarkPhaseMeasurement::Measured,
                elapsed_ms: Some(1.0),
                notes: Vec::new(),
            },
            BenchmarkPhaseBreakdown {
                phase: "warmup".to_string(),
                measurement: BenchmarkPhaseMeasurement::Measured,
                elapsed_ms: Some(0.0),
                notes: Vec::new(),
            },
            BenchmarkPhaseBreakdown {
                phase: "measurement".to_string(),
                measurement: BenchmarkPhaseMeasurement::Measured,
                elapsed_ms: Some(measurement_total_ms),
                notes: Vec::new(),
            },
        ]
    }

    fn test_sample_observations(
        samples: &[f64],
        rows: usize,
        measured_bytes: Option<u64>,
        network_payload_bytes: Option<u64>,
    ) -> Vec<BenchmarkSampleObservation> {
        samples
            .iter()
            .enumerate()
            .map(|(sample_index, elapsed_ms)| BenchmarkSampleObservation {
                sample_index,
                elapsed_ms: *elapsed_ms,
                rows_returned: rows,
                measured_bytes,
                network_payload_bytes,
            })
            .collect()
    }

    fn refresh_test_sample_observations(benchmark: &mut BenchmarkResult) {
        benchmark.sample_observations = test_sample_observations(
            &benchmark.samples_ms,
            benchmark.rows_returned,
            benchmark.measured_bytes,
            benchmark.network_payload_bytes,
        );
    }

    fn report_with_median(median: f64) -> BenchmarkRunReport {
        BenchmarkRunReport {
            schema_version: REPORT_SCHEMA_VERSION.to_string(),
            suite: BenchmarkSuite::Core,
            command: "test".to_string(),
            generated_at_unix_ms: 0,
            environment: EnvironmentMetadata {
                operating_system: "test-os".to_string(),
                architecture: "test-arch".to_string(),
                cpu_model: Some("test-cpu".to_string()),
                core_count: Some(1),
                memory_bytes: Some(1_073_741_824),
                filesystem: Some("testfs".to_string()),
                storage_type: Some("ssd".to_string()),
                rust_version: Some("rustc test".to_string()),
                build_profile: "release".to_string(),
                feature_flags: Vec::new(),
                facts_source_commit: Some("facts-test-revision".to_string()),
                sdk_source_commit: Some("sdk-test-revision".to_string()),
                benchmark_project_commit: Some("sim-test-revision".to_string()),
                fixture_path: PathBuf::from("fixture"),
            },
            fixture: FixtureMetadata {
                path: PathBuf::from("fixture"),
                profile: "profile".to_string(),
                seed: Some(1),
                proposition_count: 1,
                total_object_count: 1,
                projected_row_count: 1,
                search_index_row_count: 0,
                search_index_size_bytes: 0,
                database_size_bytes: 1,
                actor_count: Some(1),
                ledger_count: Some(1),
                replica_count: Some(0),
                simulator_revision: Some("sim-test-revision".to_string()),
                facts_sdk_revision: Some("sdk-test-revision".to_string()),
                facts_implementation_revision: Some("facts-test-revision".to_string()),
                sqlite_databases: vec![PathBuf::from("fixture/ledger.sqlite")],
            },
            benchmarks: vec![BenchmarkResult {
                name: "bench".to_string(),
                suite: BenchmarkSuite::Core,
                area: "core".to_string(),
                cache_state: "warm-filesystem".to_string(),
                cache_classification: cache_classification("warm-filesystem"),
                read_only: true,
                correctness_passed: true,
                requirement_tags: requirement_tags_for_benchmark(
                    BenchmarkSuite::Core,
                    "bench",
                    "core",
                ),
                preconditions: test_preconditions(),
                isolation_strategy: test_isolation_strategy(),
                samples_ms: vec![median],
                sample_observations: test_sample_observations(&[median], 1, Some(1), None),
                stats: TimingStats {
                    samples: 1,
                    min_ms: Some(median),
                    mean_ms: Some(median),
                    median_ms: Some(median),
                    p95_ms: Some(median),
                    max_ms: Some(median),
                    outlier_count: 0,
                    outliers_ms: Vec::new(),
                },
                phase_timings: PhaseTimings {
                    setup_ms: 1.0,
                    warmup_total_ms: 0.0,
                    measurement_total_ms: median,
                },
                phase_breakdown: test_phase_breakdown(median),
                rows_returned: 1,
                measured_bytes: Some(1),
                network_payload_bytes: None,
                failures: Vec::new(),
                notes: Vec::new(),
                diagnostics: BenchmarkDiagnostics {
                    operation_kind: "sql".to_string(),
                    resources_before: ResourceSnapshot::default(),
                    resources_after: ResourceSnapshot::default(),
                    resource_delta: ResourceDelta::default(),
                    sqlite: Some(test_sqlite_diagnostics()),
                },
            }],
        }
    }

    fn report_with_samples(samples: Vec<f64>) -> BenchmarkRunReport {
        let stats = TimingStats::from_samples(&samples);
        let measurement_total_ms = samples.iter().sum::<f64>();
        let mut report = report_with_median(stats.median_ms.unwrap_or_default());
        report.benchmarks[0].samples_ms = samples;
        refresh_test_sample_observations(&mut report.benchmarks[0]);
        report.benchmarks[0].stats = stats;
        report.benchmarks[0].phase_timings.measurement_total_ms = measurement_total_ms;
        report.benchmarks[0].phase_breakdown = test_phase_breakdown(measurement_total_ms);
        report
    }

    fn report_with_required_coverage() -> BenchmarkRunReport {
        let mut report = report_with_median(10.0);
        report.suite = BenchmarkSuite::Full;
        report.benchmarks = required_benchmark_suites()
            .into_iter()
            .filter(|suite| *suite != BenchmarkSuite::Full)
            .flat_map(|suite| {
                representative_suite_benchmarks(suite)
                    .into_iter()
                    .map(move |name| {
                        let area = representative_benchmark_area(suite, &name);
                        benchmark_result(&name, suite, area)
                    })
            })
            .collect();
        if let Some(benchmark) = report
            .benchmarks
            .iter_mut()
            .find(|benchmark| benchmark.name == "sync_bundle_inventory_metadata")
        {
            benchmark.diagnostics.operation_kind = "file-inventory".to_string();
            benchmark.diagnostics.sqlite = None;
            benchmark.measured_bytes = Some(2048);
            benchmark.network_payload_bytes = Some(2048);
            refresh_test_sample_observations(benchmark);
        }
        for benchmark in report
            .benchmarks
            .iter_mut()
            .filter(|benchmark| benchmark.name.starts_with("cli_fact_"))
        {
            benchmark.read_only = false;
            benchmark.diagnostics.operation_kind = "cli-workflow".to_string();
            benchmark.diagnostics.sqlite = None;
            benchmark.isolation_strategy =
                "CLI workflow benchmark prepares a temporary FACT_HOME per sample".to_string();
        }
        report
    }

    fn representative_benchmark_area(suite: BenchmarkSuite, name: &str) -> &'static str {
        match suite {
            BenchmarkSuite::Core => {
                if name.contains("propose") {
                    "proposition-create"
                } else if name.contains("revise") {
                    "revision-create"
                } else if name.contains("reject")
                    || name.contains("decision")
                    || name.contains("settlement")
                    || name.contains("consensus")
                {
                    "accept-reject"
                } else if name.contains("ledger")
                    || name.contains("projection_metadata")
                    || name.contains("search_index_metadata")
                {
                    "ledger-startup"
                } else {
                    "core"
                }
            }
            BenchmarkSuite::Read => {
                if name.contains("history") && !name.contains("revision_history") {
                    "history"
                } else {
                    "read"
                }
            }
            BenchmarkSuite::Search => "search",
            BenchmarkSuite::Sync => "sync",
            BenchmarkSuite::Rebuild => "rebuild",
            BenchmarkSuite::Integrity => "integrity",
            BenchmarkSuite::Cli => "cli",
            BenchmarkSuite::Conflict => "conflict-state",
            BenchmarkSuite::Http => "http",
            BenchmarkSuite::Full => "core",
        }
    }

    fn benchmark_result(name: &str, suite: BenchmarkSuite, area: &str) -> BenchmarkResult {
        let samples_ms = vec![10.0, 12.0];
        let stats = TimingStats::from_samples(&samples_ms);
        let measurement_total_ms = samples_ms.iter().sum::<f64>();
        BenchmarkResult {
            name: name.to_string(),
            suite,
            area: area.to_string(),
            cache_state: "warm-filesystem".to_string(),
            cache_classification: cache_classification("warm-filesystem"),
            read_only: true,
            correctness_passed: true,
            requirement_tags: requirement_tags_for_benchmark(suite, name, area),
            preconditions: test_preconditions(),
            isolation_strategy: test_isolation_strategy(),
            samples_ms: samples_ms.clone(),
            sample_observations: test_sample_observations(&samples_ms, 1, Some(1), None),
            stats,
            phase_timings: PhaseTimings {
                setup_ms: 1.0,
                warmup_total_ms: 0.0,
                measurement_total_ms,
            },
            phase_breakdown: test_phase_breakdown(measurement_total_ms),
            rows_returned: 1,
            measured_bytes: Some(1),
            network_payload_bytes: None,
            failures: Vec::new(),
            notes: Vec::new(),
            diagnostics: BenchmarkDiagnostics {
                operation_kind: "sql".to_string(),
                resources_before: ResourceSnapshot::default(),
                resources_after: ResourceSnapshot::default(),
                resource_delta: ResourceDelta::default(),
                sqlite: Some(test_sqlite_diagnostics()),
            },
        }
    }

    fn test_reproduction_metadata(
        fixture: PathBuf,
        profile: &str,
        level: &str,
        seed: Option<u64>,
        benchmark_command: String,
    ) -> BenchmarkReproductionMetadata {
        BenchmarkReproductionMetadata {
            fixture,
            profile: profile.to_string(),
            level: level.to_string(),
            seed,
            simulator_revision: Some("sim-test-revision".to_string()),
            facts_sdk_revision: Some("sdk-test-revision".to_string()),
            facts_implementation_revision: Some("facts-test-revision".to_string()),
            environment_manifest: Some(EnvironmentMetadata {
                operating_system: "test-os".to_string(),
                architecture: "test-arch".to_string(),
                cpu_model: Some("test-cpu".to_string()),
                core_count: Some(1),
                memory_bytes: Some(1_073_741_824),
                filesystem: Some("testfs".to_string()),
                storage_type: Some("ssd".to_string()),
                rust_version: Some("rustc test".to_string()),
                build_profile: "release".to_string(),
                feature_flags: Vec::new(),
                facts_source_commit: Some("facts-test-revision".to_string()),
                sdk_source_commit: Some("sdk-test-revision".to_string()),
                benchmark_project_commit: Some("sim-test-revision".to_string()),
                fixture_path: PathBuf::from("fixtures"),
            }),
            benchmark_command,
        }
    }

    fn test_bottleneck(report: &BenchmarkBaselineReport) -> BenchmarkBaselineBottleneck {
        BenchmarkBaselineBottleneck {
            profile: report.profile.clone(),
            level: report.level.clone(),
            seed: report.seed,
            suite: BenchmarkSuite::Read,
            area: "read".to_string(),
            benchmark: "read_list_default_accepted".to_string(),
            median_ms: 20.0,
            p95_ms: 30.0,
            priority_score: 225.0,
            reasons: vec![
                "scale-sensitive-area".to_string(),
                "user-facing".to_string(),
            ],
            source_report: report.report.clone(),
        }
    }

    fn write_empty_failure_log_for_summary(summary: &BenchmarkBaselineSummary) -> Result<()> {
        let Some(failure_log) = &summary.failure_log else {
            return Ok(());
        };
        if let Some(parent) = failure_log.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(
            failure_log,
            serde_json::to_vec_pretty(&BenchmarkFailureLog {
                schema_version: REPORT_SCHEMA_VERSION.to_string(),
                generated_at_unix_ms: 0,
                baseline_output: summary.output.clone(),
                suite: summary.suite,
                entry_count: 0,
                entries: Vec::new(),
            })?,
        )?;
        Ok(())
    }

    fn write_summary_with_failure_log(
        path: &Path,
        summary: &BenchmarkBaselineSummary,
    ) -> Result<()> {
        write_empty_failure_log_for_summary(summary)?;
        std::fs::write(path, serde_json::to_vec_pretty(summary)?)?;
        Ok(())
    }

    fn write_required_coverage_reports(base: &Path) -> Result<Vec<BenchmarkBaselineReport>> {
        write_required_coverage_reports_with_cache(base, "warm-filesystem")
    }

    fn write_required_coverage_reports_with_cache(
        base: &Path,
        cache_state: &str,
    ) -> Result<Vec<BenchmarkBaselineReport>> {
        std::fs::create_dir_all(base)?;
        let mut reports = Vec::new();
        for profile in REQUIRED_BENCHMARK_PROFILES {
            for level in REQUIRED_BENCHMARK_LEVELS {
                let report = base.join(format!("{profile}-{level}.json"));
                let fixture = PathBuf::from(format!("fixtures/{profile}-{level}"));
                let mut run_report =
                    required_coverage_report_for_entry(profile, level, fixture.clone());
                set_report_cache_state(&mut run_report, cache_state);
                std::fs::write(&report, serde_json::to_vec_pretty(&run_report)?)?;
                reports.push(BenchmarkBaselineReport {
                    fixture,
                    level: level.to_string(),
                    profile: profile.to_string(),
                    seed: Some(42),
                    report: report.clone(),
                    benchmark_count: report_with_required_coverage().benchmarks.len(),
                    reproduce_command: format!(
                        "fact-sim benchmark run --suite full --fixture fixtures/{profile}-{level} --output {}",
                        report.display()
                    ),
                    reproduction: Some(test_reproduction_metadata(
                        PathBuf::from(format!("fixtures/{profile}-{level}")),
                        profile,
                        level,
                        Some(42),
                        format!(
                            "fact-sim benchmark run --suite full --fixture fixtures/{profile}-{level} --output {}",
                            report.display()
                        ),
                    )),
                });
            }
        }
        Ok(reports)
    }

    fn set_report_cache_state(report: &mut BenchmarkRunReport, cache_state: &str) {
        for benchmark in &mut report.benchmarks {
            benchmark.cache_state = cache_state.to_string();
            benchmark.cache_classification = cache_classification(cache_state);
        }
    }

    fn required_coverage_report_for_entry(
        profile: &str,
        level: &str,
        fixture: PathBuf,
    ) -> BenchmarkRunReport {
        let propositions = match level {
            "small" => 10_000,
            "medium" => 100_000,
            "large" => 500_000,
            _ => 1,
        };
        let mut report = report_with_required_coverage();
        report.fixture.path = fixture;
        report.fixture.profile = profile.to_string();
        report.fixture.seed = Some(42);
        report.fixture.proposition_count = propositions;
        report.fixture.total_object_count = propositions * 5;
        report
    }

    fn summary_with_report(report: &Path) -> BenchmarkBaselineSummary {
        BenchmarkBaselineSummary {
            schema_version: REPORT_SCHEMA_VERSION.to_string(),
            suite: BenchmarkSuite::Core,
            base: PathBuf::from("fixtures"),
            output: PathBuf::from("reports"),
            generated_at_unix_ms: 0,
            environment_manifest: Some(EnvironmentMetadata {
                operating_system: "test-os".to_string(),
                architecture: "test-arch".to_string(),
                cpu_model: Some("test-cpu".to_string()),
                core_count: Some(1),
                memory_bytes: Some(1_073_741_824),
                filesystem: Some("testfs".to_string()),
                storage_type: Some("ssd".to_string()),
                rust_version: Some("rustc test".to_string()),
                build_profile: "release".to_string(),
                feature_flags: Vec::new(),
                facts_source_commit: Some("facts-test-revision".to_string()),
                sdk_source_commit: Some("sdk-test-revision".to_string()),
                benchmark_project_commit: Some("sim-test-revision".to_string()),
                fixture_path: PathBuf::from("fixtures"),
            }),
            iterations: 1,
            warmups: 0,
            cache_state: "warm-filesystem".to_string(),
            levels: BTreeMap::from([("small".to_string(), 1)]),
            missing_levels: Vec::new(),
            required_profiles: REQUIRED_BENCHMARK_PROFILES
                .into_iter()
                .map(str::to_string)
                .collect(),
            profile_levels: BTreeMap::from([(
                "scale-500k-balanced".to_string(),
                BTreeSet::from(["small".to_string()]),
            )]),
            missing_profiles: Vec::new(),
            missing_profile_levels: Vec::new(),
            ready: true,
            failure_log: Some(
                report
                    .parent()
                    .unwrap_or_else(|| Path::new("reports"))
                    .join("failure-log.json"),
            ),
            reports: vec![BenchmarkBaselineReport {
                fixture: PathBuf::from("fixture"),
                level: "small".to_string(),
                profile: "scale-500k-balanced".to_string(),
                seed: Some(42),
                report: report.to_path_buf(),
                benchmark_count: 1,
                reproduce_command: format!(
                    "fact-sim benchmark run --suite core --fixture fixture --output {}",
                    report.display()
                ),
                reproduction: Some(test_reproduction_metadata(
                    PathBuf::from("fixture"),
                    "scale-500k-balanced",
                    "small",
                    Some(42),
                    format!(
                        "fact-sim benchmark run --suite core --fixture fixture --output {}",
                        report.display()
                    ),
                )),
            }],
            failures: Vec::new(),
            bottlenecks: Vec::new(),
        }
    }

    fn summary_with_full_report(report: &Path) -> BenchmarkBaselineSummary {
        let mut summary = summary_with_report(report);
        summary.suite = BenchmarkSuite::Full;
        summary.reports[0].benchmark_count = report_with_required_coverage().benchmarks.len();
        summary
    }

    fn summary_with_full_matrix_reports(
        reports: &[BenchmarkBaselineReport],
    ) -> BenchmarkBaselineSummary {
        let mut summary = summary_with_report(&reports[0].report);
        summary.suite = BenchmarkSuite::Full;
        summary.iterations = MIN_READY_BENCHMARK_ITERATIONS;
        summary.warmups = 1;
        summary.levels = BTreeMap::from([
            ("large".to_string(), 6),
            ("medium".to_string(), 6),
            ("small".to_string(), 6),
        ]);
        summary.profile_levels = REQUIRED_BENCHMARK_PROFILES
            .into_iter()
            .map(|profile| {
                (
                    profile.to_string(),
                    BTreeSet::from([
                        "large".to_string(),
                        "medium".to_string(),
                        "small".to_string(),
                    ]),
                )
            })
            .collect();
        summary.reports = reports.to_vec();
        summary.bottlenecks = vec![test_bottleneck(&reports[0])];
        summary
    }

    fn write_growth_summary(base: &Path, points: &[(&str, u64, f64)]) -> Result<PathBuf> {
        let mut reports = Vec::new();
        for (level, propositions, median_ms) in points {
            let report_path = base.join(format!("{level}.json"));
            let mut report = report_with_median(*median_ms);
            report.fixture.profile = "scale-500k-balanced".to_string();
            report.fixture.proposition_count = *propositions;
            report.fixture.total_object_count = propositions * 5;
            report.fixture.search_index_row_count = propositions / 2;
            report.fixture.search_index_size_bytes = propositions * 32;
            report.fixture.path = PathBuf::from(format!("fixtures/scale-500k-balanced-{level}"));
            std::fs::write(&report_path, serde_json::to_vec_pretty(&report)?)?;
            reports.push(BenchmarkBaselineReport {
                fixture: report.fixture.path.clone(),
                level: (*level).to_string(),
                profile: report.fixture.profile.clone(),
                seed: Some(42),
                report: report_path,
                benchmark_count: report.benchmarks.len(),
                reproduce_command: "fact-sim benchmark run".to_string(),
                reproduction: Some(test_reproduction_metadata(
                    report.fixture.path.clone(),
                    &report.fixture.profile,
                    level,
                    Some(42),
                    "fact-sim benchmark run".to_string(),
                )),
            });
        }
        let mut summary = summary_with_report(&reports[0].report);
        summary.levels = points
            .iter()
            .map(|(level, _, _)| ((*level).to_string(), 1))
            .collect();
        summary.profile_levels = BTreeMap::from([(
            "scale-500k-balanced".to_string(),
            points
                .iter()
                .map(|(level, _, _)| (*level).to_string())
                .collect(),
        )]);
        summary.reports = reports;
        let summary_path = base.join("growth-summary.json");
        std::fs::write(&summary_path, serde_json::to_vec_pretty(&summary)?)?;
        Ok(summary_path)
    }

    fn write_complete_acceptance_inputs(base: &Path) -> Result<BenchmarkAcceptArgs> {
        std::fs::create_dir_all(base)?;
        let audit = base.join("readiness-audit.json");
        std::fs::write(
            &audit,
            serde_json::to_vec_pretty(&ready_acceptance_audit_report())?,
        )?;

        let growth_summary = write_growth_summary(
            base,
            &[
                ("small", 10_000, 10.0),
                ("medium", 100_000, 100.0),
                ("large", 500_000, 500.0),
            ],
        )?;
        let growth_analysis = base.join("growth-analysis.json");
        let growth = analyze_baseline_growth(&BenchmarkAnalyzeArgs {
            baseline_summaries: vec![growth_summary.clone()],
        })?;
        std::fs::write(&growth_analysis, serde_json::to_vec_pretty(&growth)?)?;

        let budgets = write_test_budgets(base, growth_summary.clone())?;
        let budget_check = base.join("budget-check.json");
        let check = check_benchmark_budgets(&BenchmarkCheckBudgetsArgs {
            budgets,
            baseline_summary: growth_summary,
        })?;
        std::fs::write(&budget_check, serde_json::to_vec_pretty(&check)?)?;

        let profile_summary = write_profile_plan_summary(base)?;
        let profile_plan = base.join("profile-plan.json");
        let plan = benchmark_profile_plan(&BenchmarkProfilePlanArgs {
            baseline_summaries: vec![profile_summary],
            limit: 1,
        })?;
        std::fs::write(&profile_plan, serde_json::to_vec_pretty(&plan)?)?;

        Ok(BenchmarkAcceptArgs {
            audit,
            growth_analysis,
            budget_check,
            profile_plan,
        })
    }

    fn replace_artifact_schema_version(path: &Path, schema_version: &str) -> Result<()> {
        let mut value: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
        value["schema_version"] = serde_json::Value::String(schema_version.to_string());
        std::fs::write(path, serde_json::to_vec_pretty(&value)?)?;
        Ok(())
    }

    fn write_test_budgets(base: &Path, summary: PathBuf) -> Result<PathBuf> {
        let budgets = derive_benchmark_budgets(&BenchmarkBudgetsArgs {
            baseline_summaries: vec![summary],
            warning_multiplier: 1.25,
            regression_multiplier: 1.50,
            minimum_warning_ms: 1.0,
            minimum_regression_ms: 5.0,
        })?;
        let budgets_path = base.join("budgets.json");
        std::fs::write(&budgets_path, serde_json::to_vec_pretty(&budgets)?)?;
        Ok(budgets_path)
    }

    fn write_profile_plan_summary(base: &Path) -> Result<PathBuf> {
        let report_path = base.join("profile-plan-run.json");
        let mut report = report_with_median(10.0);
        report.benchmarks = vec![
            BenchmarkResult {
                name: "read_list_default_accepted".to_string(),
                suite: BenchmarkSuite::Read,
                area: "read".to_string(),
                cache_state: "warm-filesystem".to_string(),
                cache_classification: cache_classification("warm-filesystem"),
                read_only: true,
                correctness_passed: true,
                requirement_tags: requirement_tags_for_benchmark(
                    BenchmarkSuite::Read,
                    "read_list_default_accepted",
                    "read",
                ),
                preconditions: test_preconditions(),
                isolation_strategy: test_isolation_strategy(),
                samples_ms: vec![110.0, 125.0],
                sample_observations: vec![
                    BenchmarkSampleObservation {
                        sample_index: 0,
                        elapsed_ms: 110.0,
                        rows_returned: 100,
                        measured_bytes: Some(1024),
                        network_payload_bytes: None,
                    },
                    BenchmarkSampleObservation {
                        sample_index: 1,
                        elapsed_ms: 125.0,
                        rows_returned: 100,
                        measured_bytes: Some(1024),
                        network_payload_bytes: None,
                    },
                ],
                stats: TimingStats {
                    samples: 2,
                    min_ms: Some(110.0),
                    mean_ms: Some(117.5),
                    median_ms: Some(125.0),
                    p95_ms: Some(125.0),
                    max_ms: Some(125.0),
                    outlier_count: 0,
                    outliers_ms: Vec::new(),
                },
                phase_timings: PhaseTimings {
                    setup_ms: 1.0,
                    warmup_total_ms: 0.0,
                    measurement_total_ms: 235.0,
                },
                phase_breakdown: test_phase_breakdown(235.0),
                rows_returned: 100,
                measured_bytes: Some(1024),
                network_payload_bytes: None,
                failures: Vec::new(),
                notes: Vec::new(),
                diagnostics: BenchmarkDiagnostics {
                    operation_kind: "sql".to_string(),
                    resources_before: ResourceSnapshot::default(),
                    resources_after: ResourceSnapshot::default(),
                    resource_delta: ResourceDelta {
                        process_rss_kib_delta: Some(2048),
                        process_peak_rss_kib_delta: Some(4096),
                        process_cpu_seconds_delta: Some(0.125),
                        artifact_bytes_delta: Some(2_097_152),
                        disk_read_bytes_delta: Some(1_048_576),
                        disk_write_bytes_delta: Some(2_097_152),
                    },
                    sqlite: Some(SqliteDiagnostics {
                        database: PathBuf::from("fixture/ledger.sqlite"),
                        database_size_bytes: 1024,
                        page_count: Some(1),
                        page_size: Some(4096),
                        journal_mode: Some("wal".to_string()),
                        query_plan: vec!["0:0: SCAN projected_effective".to_string()],
                        uses_full_scan: true,
                        uses_temporary_btree: false,
                    }),
                },
            },
            benchmark_result("core_lookup", BenchmarkSuite::Core, "core"),
        ];
        std::fs::write(&report_path, serde_json::to_vec_pretty(&report)?)?;
        let summary = summary_with_report(&report_path);
        let summary_path = base.join("profile-plan-summary.json");
        std::fs::write(&summary_path, serde_json::to_vec_pretty(&summary)?)?;
        Ok(summary_path)
    }
}
