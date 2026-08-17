use std::path::PathBuf;
use std::process::Stdio;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use tokio::io::AsyncReadExt;
use tokio::process::Command;

mod benchmark;
mod ci;
mod fault;
mod ux;

#[derive(Debug, Parser)]
#[command(name = "fact-sim")]
#[command(about = "Deterministic Facts simulation harness")]
struct Cli {
    #[command(subcommand)]
    command: CommandKind,
}

#[derive(Debug, Subcommand)]
enum CommandKind {
    Scenario {
        #[command(subcommand)]
        command: ScenarioCommand,
    },
    Suite {
        #[command(subcommand)]
        command: SuiteCommand,
    },
    Generate {
        #[arg(long, conflicts_with = "all", required_unless_present = "all")]
        profile: Option<String>,
        #[arg(long)]
        all: bool,
        #[arg(long, default_value_t = 42)]
        seed: u64,
        #[arg(long)]
        output: Option<PathBuf>,
        #[arg(long, env = "FACT_BINARY")]
        fact_binary: Option<PathBuf>,
        #[arg(
            long = "target-propositions",
            visible_alias = "target-objects",
            value_name = "TARGET_PROPOSITIONS"
        )]
        target_objects: Option<usize>,
    },
    Plan {
        #[arg(long, conflicts_with = "all", required_unless_present = "all")]
        profile: Option<String>,
        #[arg(long)]
        all: bool,
        #[arg(long, default_value_t = 42)]
        seed: u64,
        #[arg(long)]
        output: Option<PathBuf>,
        #[arg(
            long = "target-propositions",
            visible_alias = "target-objects",
            value_name = "TARGET_PROPOSITIONS"
        )]
        target_objects: Option<usize>,
    },
    Verify {
        #[arg(required_unless_present = "all")]
        fixture: Option<PathBuf>,
        #[arg(long)]
        all: bool,
        #[arg(long, default_value_t = 42)]
        seed: u64,
    },
    Inspect {
        fixture: PathBuf,
    },
    Report {
        fixture: PathBuf,
        #[arg(long)]
        full: bool,
    },
    Compare {
        left: PathBuf,
        right: PathBuf,
    },
    Resume {
        checkpoint: PathBuf,
        #[arg(long, env = "FACT_BINARY")]
        fact_binary: Option<PathBuf>,
    },
    Cleanup {
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        include_target: bool,
        #[arg(long)]
        fixtures: bool,
        #[arg(long, conflicts_with = "all")]
        profile: Option<String>,
        #[arg(long)]
        all: bool,
    },
    Benchmark {
        #[command(subcommand)]
        command: benchmark::BenchmarkCommand,
    },
    Ci {
        #[command(subcommand)]
        command: ci::CiCommand,
    },
    Fault {
        #[command(subcommand)]
        command: fault::FaultCommand,
    },
    Ux {
        #[command(subcommand)]
        command: ux::UxCommand,
    },
    Cli(CliAdapterArgs),
}

#[derive(Debug, Subcommand)]
enum ScenarioCommand {
    Validate { path: PathBuf },
    Run { path: PathBuf },
}

#[derive(Debug, Subcommand)]
enum SuiteCommand {
    Run { path: PathBuf },
}

#[derive(Debug, Args)]
struct CliAdapterArgs {
    #[arg(long, env = "FACT_BINARY")]
    fact_binary: PathBuf,
    #[arg(last = true)]
    args: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    match cli.command {
        CommandKind::Scenario { command } => match command {
            ScenarioCommand::Validate { path } => {
                read_scenario(&path)?;
                println!("valid scenario: {}", path.display());
            }
            ScenarioCommand::Run { path } => {
                let scenario = read_scenario(&path)?;
                let run = fact_sim_runner::run_scenario(&scenario)?;
                println!("{}", serde_json::to_string_pretty(&run)?);
            }
        },
        CommandKind::Suite { command } => match command {
            SuiteCommand::Run { path } => run_suite(path)?,
        },
        CommandKind::Generate {
            profile,
            all,
            seed,
            output,
            fact_binary,
            target_objects,
        } => {
            if all {
                let target_objects =
                    target_objects.unwrap_or(fact_sim_runner::scale::TARGET_OBJECTS);
                let output_base = output.unwrap_or_else(|| PathBuf::from("fixtures"));
                let plans = fact_sim_runner::scale::PROFILES
                    .iter()
                    .map(|profile| {
                        let fixture_output = scale_fixture_output(&output_base, profile, seed);
                        scale_plan_json_for_output(profile, seed, target_objects, fixture_output)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let blocked_profiles = plans
                    .iter()
                    .filter(|plan| {
                        !plan["storage_preflight"]["sufficient"]
                            .as_bool()
                            .unwrap_or(false)
                    })
                    .map(|plan| {
                        serde_json::json!({
                            "profile": plan["profile"],
                            "output": plan["suggested_output"],
                            "warning": plan["storage_preflight"]["warning"],
                        })
                    })
                    .collect::<Vec<_>>();
                let aggregate_preflight = scale_all_preflight_summary(&plans);
                if !blocked_profiles.is_empty() {
                    bail!(
                        "scale fixture generate --all preflight failed for {} profiles: {}",
                        blocked_profiles.len(),
                        serde_json::to_string(&blocked_profiles)?
                    );
                }
                if aggregate_preflight["sufficient"].as_bool() != Some(true) {
                    bail!(
                        "scale fixture generate --all aggregate preflight failed: {}",
                        serde_json::to_string(&aggregate_preflight)?
                    );
                }
                let mut generated = Vec::new();
                for profile in fact_sim_runner::scale::PROFILES {
                    let output = scale_fixture_output(&output_base, profile, seed);
                    let report = fact_sim_runner::scale::generate_scale(
                        fact_sim_runner::scale::GenerateOptions {
                            profile: (*profile).to_string(),
                            seed,
                            output,
                            fact_binary: fact_binary.clone(),
                            target_objects: Some(target_objects),
                        },
                    )?;
                    generated.push(serde_json::json!({
                        "profile": report.profile,
                        "seed": report.seed,
                        "target_objects": report.target_objects,
                        "output": report.output,
                        "object_count": report.object_count,
                        "actor_count": report.actor_count,
                        "ledger_count": report.ledger_count,
                        "replica_count": report.replica_count,
                        "generated_instances": report.generated_instances,
                        "scenario_family_counts": report.scenario_family_counts,
                        "scenario_family_object_counts": report.scenario_family_object_counts,
                        "target_object_overshoot": report.target_object_overshoot,
                        "manifest": report.output.join("manifest.json"),
                    }));
                }
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "seed": seed,
                        "target_objects": target_objects,
                        "output_base": output_base,
                        "aggregate_storage_preflight": aggregate_preflight,
                        "profile_count": generated.len(),
                        "generated": generated,
                    }))?
                );
                return Ok(());
            }
            let profile = profile.context("generate requires --profile <name> or --all")?;
            let profiles = fact_sim_fixtures::initial_scale_profiles();
            if profile == "multi-actor-10k" {
                let output =
                    output.unwrap_or_else(|| PathBuf::from("fixtures").join("multi-actor-10k"));
                let report = fact_sim_runner::multi_actor::generate_multi_actor(
                    fact_sim_runner::multi_actor::GenerateOptions {
                        profile,
                        seed,
                        output,
                        fact_binary,
                    },
                )?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "profile": report.profile,
                        "seed": report.seed,
                        "output": report.output,
                        "database": report.database,
                        "bundle": report.bundle,
                        "object_count": report.object_count,
                        "actor_count": report.actor_count,
                        "generated_instances": report.generated_instances,
                        "assertion_report": report.assertion_report,
                        "manifest": report.output.join("manifest.json"),
                    }))?
                );
            } else if fact_sim_runner::conflict_repair::is_conflict_repair_profile(&profile) {
                let output = output.unwrap_or_else(|| {
                    let directory = if profile == fact_sim_runner::conflict_repair::LEGACY_PROFILE {
                        fact_sim_runner::conflict_repair::LEGACY_PROFILE
                    } else {
                        fact_sim_runner::conflict_repair::CONFLICT_REPAIR_PROFILE
                    };
                    PathBuf::from("fixtures").join(directory)
                });
                let report = fact_sim_runner::conflict_repair::generate_conflict_repair(
                    fact_sim_runner::conflict_repair::GenerateOptions {
                        profile,
                        seed,
                        output,
                        fact_binary,
                        target_objects: None,
                    },
                )?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "profile": report.profile,
                        "seed": report.seed,
                        "scheduler_version": report.scheduler_version,
                        "started_at": report.started_at,
                        "facts_sdk_revision": report.facts_sdk_revision,
                        "simulator_revision": report.simulator_revision,
                        "output": report.output,
                        "object_count": report.object_count,
                        "actor_count": report.actor_count,
                        "ledger_count": report.ledger_count,
                        "replica_count": report.replica_count,
                        "retry_report": report.retry_report,
                        "repair_report": report.repair_report,
                        "reconciliation_counts_by_mode": report.reconciliation_counts_by_mode,
                        "coordinator_disposition_counts": report.coordinator_disposition_counts,
                        "conflict_report": report.conflict_report,
                        "assertion_report": report.assertion_report,
                        "cli_ux_coverage": report.cli_ux_coverage,
                        "unresolved_protocol_behavior": report.unresolved_protocol_behavior,
                        "manifest": report.output.join("manifest.json"),
                    }))?
                );
            } else if fact_sim_runner::scale::is_scale_profile(&profile) {
                let output = output.unwrap_or_else(|| {
                    PathBuf::from("fixtures").join(format!("{profile}-seed-{seed}"))
                });
                let report = fact_sim_runner::scale::generate_scale(
                    fact_sim_runner::scale::GenerateOptions {
                        profile,
                        seed,
                        output,
                        fact_binary,
                        target_objects,
                    },
                )?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "profile": report.profile,
                        "seed": report.seed,
                        "target_objects": report.target_objects,
                        "output": report.output,
                        "object_count": report.object_count,
                        "actor_count": report.actor_count,
                        "ledger_count": report.ledger_count,
                        "replica_count": report.replica_count,
                        "conflict_report": report.conflict_report,
                        "assertion_report": report.assertion_report,
                        "manifest": report.output.join("manifest.json"),
                    }))?
                );
            } else if !profiles.iter().any(|candidate| candidate.name == profile) {
                bail!("unknown scale profile `{profile}`");
            } else {
                println!(
                    "{}",
                    serde_json::json!({
                        "profile": profile,
                        "seed": seed,
                        "status": "defined",
                        "note": "corpus generation is implemented for multi-actor-10k, conflict-repair, and scale-500k profiles"
                    })
                );
            }
        }
        CommandKind::Plan {
            profile,
            all,
            seed,
            output,
            target_objects,
        } => {
            let target_objects = target_objects.unwrap_or(fact_sim_runner::scale::TARGET_OBJECTS);
            if all {
                let output_base = output.unwrap_or_else(|| PathBuf::from("fixtures"));
                println!(
                    "{}",
                    serde_json::to_string_pretty(&scale_all_plan_json(
                        seed,
                        target_objects,
                        output_base
                    )?)?
                );
            } else {
                let profile = profile.context("plan requires --profile <name> or --all")?;
                if !fact_sim_runner::scale::is_scale_profile(&profile) {
                    bail!("plan is implemented for scale-500k profiles");
                }
                let output = output.unwrap_or_else(|| {
                    scale_fixture_output(&PathBuf::from("fixtures"), &profile, seed)
                });
                println!(
                    "{}",
                    serde_json::to_string_pretty(&scale_plan_json_for_output(
                        &profile,
                        seed,
                        target_objects,
                        output
                    )?)?
                );
            }
        }
        CommandKind::Verify { fixture, all, seed } => {
            if all {
                let base = fixture.unwrap_or_else(|| PathBuf::from("fixtures"));
                let missing = fact_sim_runner::scale::PROFILES
                    .iter()
                    .map(|profile| scale_fixture_output(&base, profile, seed))
                    .filter(|fixture| !fixture.join("manifest.json").exists())
                    .collect::<Vec<_>>();
                if !missing.is_empty() {
                    bail!(
                        "scale fixture verify --all is missing {} fixture manifests under `{}`: {}",
                        missing.len(),
                        base.display(),
                        serde_json::to_string(&missing)?
                    );
                }
                let mut verified = Vec::new();
                for profile in fact_sim_runner::scale::PROFILES {
                    let fixture = scale_fixture_output(&base, profile, seed);
                    let report = fact_sim_runner::scale::verify_scale_fixture(&fixture)?;
                    verified.push(serde_json::json!({
                        "fixture": fixture,
                        "profile": report.profile,
                        "seed": report.seed,
                        "target_objects": report.target_objects,
                        "object_count": report.object_count,
                        "generated_instances": report.generated_instances,
                        "scenario_family_counts": report.scenario_family_counts,
                        "scenario_family_object_counts": report.scenario_family_object_counts,
                        "assertion_report": report.assertion_report,
                        "verification_result": report.verification_result,
                    }));
                }
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "status": "verified",
                        "fixture_base": base,
                        "profile_count": verified.len(),
                        "verified": verified,
                    }))?
                );
                return Ok(());
            }
            let fixture = fixture.context("verify requires a fixture path or --all")?;
            let profile = fixture_profile(&fixture)?;
            if profile
                .as_deref()
                .is_some_and(fact_sim_runner::conflict_repair::is_conflict_repair_profile)
            {
                let report =
                    fact_sim_runner::conflict_repair::verify_conflict_repair_fixture(&fixture)?;
                print_conflict_repair_report(&fixture, "verified", &report, false)?;
                return Ok(());
            }
            if profile
                .as_deref()
                .is_some_and(fact_sim_runner::scale::is_scale_profile)
            {
                let report = fact_sim_runner::scale::verify_scale_fixture(&fixture)?;
                print_conflict_repair_report(&fixture, "verified", &report, false)?;
                return Ok(());
            }
            let report = fact_sim_runner::multi_actor::verify_multi_actor_fixture(&fixture)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "fixture": fixture,
                    "status": "verified",
                    "profile": report.profile,
                    "seed": report.seed,
                    "object_count": report.object_count,
                    "actor_count": report.actor_count,
                    "required_types": report.object_counts_by_type,
                    "assertion_report": report.assertion_report,
                }))?
            );
        }
        CommandKind::Inspect { fixture } => {
            let manifest_path = fixture.join("manifest.json");
            let manifest: serde_json::Value = serde_json::from_slice(
                &std::fs::read(&manifest_path)
                    .with_context(|| format!("failed to read `{}`", manifest_path.display()))?,
            )
            .with_context(|| format!("failed to parse `{}`", manifest_path.display()))?;
            let profile = manifest["profile"].as_str();
            let scale_profile = profile.is_some_and(fact_sim_runner::scale::is_scale_profile);
            let bulk_profile =
                profile.is_some_and(fact_sim_runner::scale::is_bulk_proposition_profile);
            let artifacts = inspect_artifacts(&fixture, profile);
            let world_plan = (scale_profile && !bulk_profile)
                .then(|| read_optional_report(&fixture, "world-plan.json"))
                .transpose()?
                .flatten();
            let completed_checkpoint = (scale_profile && !bulk_profile)
                .then(|| read_optional_report(&fixture, "checkpoints/completed.json"))
                .transpose()?
                .flatten();
            println!(
                "{}",
                serde_json::to_string_pretty(&inspect_fixture_json(
                    &fixture,
                    &manifest,
                    artifacts,
                    world_plan.as_ref(),
                    completed_checkpoint.as_ref(),
                ))?
            );
        }
        CommandKind::Report { fixture, full } => {
            let profile = fixture_profile(&fixture)?;
            if profile
                .as_deref()
                .is_some_and(fact_sim_runner::conflict_repair::is_conflict_repair_profile)
            {
                let report =
                    fact_sim_runner::conflict_repair::verify_conflict_repair_fixture(&fixture)?;
                print_conflict_repair_report(&fixture, "reported", &report, full)?;
                return Ok(());
            }
            if profile
                .as_deref()
                .is_some_and(fact_sim_runner::scale::is_scale_profile)
            {
                print_scale_report_allowing_reduced_target(&fixture, "reported", full)?;
                return Ok(());
            }
            let report = fact_sim_runner::multi_actor::verify_multi_actor_fixture(&fixture)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "fixture": fixture,
                    "status": "reported",
                    "profile": report.profile,
                    "seed": report.seed,
                    "object_count": report.object_count,
                    "actor_count": report.actor_count,
                    "required_types": report.object_counts_by_type,
                    "assertion_report": report.assertion_report,
                }))?
            );
        }
        CommandKind::Compare { left, right } => {
            let report = compare_fixtures(&left, &right)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        CommandKind::Resume {
            checkpoint,
            fact_binary,
        } => {
            let checkpoint: serde_json::Value = serde_json::from_slice(
                &std::fs::read(&checkpoint)
                    .with_context(|| format!("failed to read `{}`", checkpoint.display()))?,
            )
            .with_context(|| format!("failed to parse `{}`", checkpoint.display()))?;
            let profile = checkpoint["profile"]
                .as_str()
                .context("checkpoint does not record a profile")?;
            if fact_sim_runner::scale::is_scale_profile(profile) {
                let fixture = checkpoint["fixture"]
                    .as_str()
                    .map(PathBuf::from)
                    .context("checkpoint does not record a fixture path")?;
                if checkpoint["completed"].as_bool() == Some(true)
                    && fixture.join("manifest.json").exists()
                {
                    let checkpoint_target = checkpoint["target_objects"].as_u64();
                    if checkpoint_target == Some(fact_sim_runner::scale::TARGET_OBJECTS as u64)
                        || checkpoint_target.is_none()
                    {
                        let report = fact_sim_runner::scale::verify_scale_fixture(&fixture)?;
                        print_conflict_repair_report(&fixture, "resumed-complete", &report, false)?;
                    } else {
                        print_scale_report_allowing_reduced_target(
                            &fixture,
                            "resumed-complete",
                            false,
                        )?;
                    }
                    return Ok(());
                }
                let seed = checkpoint["seed"]
                    .as_u64()
                    .context("checkpoint does not record a seed")?;
                let target_objects = checkpoint["target_objects"]
                    .as_u64()
                    .map(|value| value as usize);
                ensure_scale_resume_replay_preconditions(&checkpoint, &fixture)?;
                let report = fact_sim_runner::scale::generate_scale(
                    fact_sim_runner::scale::GenerateOptions {
                        profile: profile.to_string(),
                        seed,
                        output: fixture.clone(),
                        fact_binary,
                        target_objects,
                    },
                )?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "fixture": fixture,
                        "status": "resumed-replayed",
                        "profile": report.profile,
                        "seed": report.seed,
                        "target_objects": report.target_objects,
                        "object_count": report.object_count,
                        "generated_instances": report.generated_instances,
                        "scenario_family_counts": report.scenario_family_counts,
                        "target_object_overshoot": report.target_object_overshoot,
                        "assertion_report": report.assertion_report,
                    }))?
                );
                return Ok(());
            }
            if checkpoint["completed"].as_bool() == Some(true) {
                let fixture = checkpoint["fixture"]
                    .as_str()
                    .map(PathBuf::from)
                    .with_context(|| {
                        format!("checkpoint `{}` does not record a fixture path", checkpoint)
                    })?;
                let profile = fixture_profile(&fixture)?;
                if profile
                    .as_deref()
                    .is_some_and(fact_sim_runner::scale::is_scale_profile)
                {
                    let report = fact_sim_runner::scale::verify_scale_fixture(&fixture)?;
                    print_conflict_repair_report(&fixture, "resumed-complete", &report, false)?;
                    return Ok(());
                }
            }
            bail!(
                "checkpoint resume is not implemented for this checkpoint yet: profile={}, seed={}, scenario_instance={}",
                checkpoint["profile"].as_str().unwrap_or("unknown"),
                checkpoint["seed"],
                checkpoint["current_scenario_instance"]
            );
        }
        CommandKind::Cleanup {
            dry_run,
            include_target,
            fixtures,
            profile,
            all,
        } => {
            let report =
                cleanup_generated_artifacts(dry_run, include_target, fixtures, profile, all)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        CommandKind::Benchmark { command } => {
            let output = benchmark::execute(command)?;
            println!("{}", output);
        }
        CommandKind::Ci { command } => {
            let output = ci::execute(command)?;
            println!("{}", output);
        }
        CommandKind::Fault { command } => {
            let output = fault::execute(command)?;
            println!("{}", output);
        }
        CommandKind::Ux { command } => {
            let output = ux::execute(command)?;
            println!("{}", output);
        }
        CommandKind::Cli(args) => run_fact_cli(args).await?,
    }
    Ok(())
}

fn cleanup_generated_artifacts(
    dry_run: bool,
    include_target: bool,
    fixtures: bool,
    profile: Option<String>,
    all: bool,
) -> Result<serde_json::Value> {
    if !fixtures && (profile.is_some() || all) {
        bail!("cleanup --profile and cleanup --all require --fixtures");
    }
    if fixtures && profile.is_none() && !all {
        bail!("cleanup --fixtures requires --profile <name> or --all");
    }
    let candidates = cleanup_candidates(include_target);
    let mut entries = Vec::new();
    for path in candidates {
        let exists = path.exists();
        let bytes = if exists { directory_bytes(&path)? } else { 0 };
        if exists && !dry_run {
            if path.is_file() {
                std::fs::remove_file(&path)
                    .with_context(|| format!("failed to remove `{}`", path.display()))?;
            } else {
                std::fs::remove_dir_all(&path)
                    .with_context(|| format!("failed to remove `{}`", path.display()))?;
            }
        }
        entries.push(serde_json::json!({
            "path": path,
            "exists": exists,
            "bytes": bytes,
            "removed": exists && !dry_run,
        }));
    }
    let fixture_entries = if fixtures {
        cleanup_fixture_artifacts(profile.as_deref(), all, dry_run)?
    } else {
        Vec::new()
    };
    Ok(serde_json::json!({
        "dry_run": dry_run,
        "include_target": include_target,
        "fixtures": fixtures,
        "profile": profile,
        "all": all,
        "artifacts": entries,
        "fixture_artifacts": fixture_entries,
    }))
}

fn cleanup_candidates(include_target: bool) -> Vec<PathBuf> {
    if include_target {
        return cleanup_target_candidates();
    }
    cleanup_run_workspace_candidates()
}

fn cleanup_run_workspace_candidates() -> Vec<PathBuf> {
    let mut candidates = vec![
        PathBuf::from("target/fact-sim-runs"),
        PathBuf::from("crates/fact-sim-cli/target/fact-sim-runs"),
        PathBuf::from("crates/fact-sim-runner/target/fact-sim-runs"),
    ];
    discover_named_child_dirs(
        std::path::Path::new("."),
        "target",
        "fact-sim-runs",
        &mut candidates,
    );
    normalize_cleanup_candidates(candidates)
}

fn cleanup_target_candidates() -> Vec<PathBuf> {
    let mut candidates = vec![
        PathBuf::from("target"),
        PathBuf::from("crates/fact-sim-cli/target"),
        PathBuf::from("crates/fact-sim-runner/target"),
    ];
    discover_named_dirs(std::path::Path::new("."), "target", &mut candidates);
    normalize_cleanup_candidates(candidates)
}

fn discover_named_child_dirs(
    root: &std::path::Path,
    parent_name: &str,
    child_name: &str,
    candidates: &mut Vec<PathBuf>,
) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() || is_cleanup_discovery_boundary(&path) {
            continue;
        }
        if path.file_name().and_then(|name| name.to_str()) == Some(parent_name) {
            candidates.push(path.join(child_name));
            continue;
        }
        discover_named_child_dirs(&path, parent_name, child_name, candidates);
    }
}

fn discover_named_dirs(root: &std::path::Path, name: &str, candidates: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() || is_cleanup_discovery_boundary(&path) {
            continue;
        }
        if path.file_name().and_then(|name| name.to_str()) == Some(name) {
            candidates.push(path);
            continue;
        }
        discover_named_dirs(&path, name, candidates);
    }
}

fn is_cleanup_discovery_boundary(path: &std::path::Path) -> bool {
    matches!(
        path.file_name().and_then(|name| name.to_str()),
        Some(".git" | "fixtures" | "tmp" | "reports")
    )
}

fn normalize_cleanup_candidates(candidates: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut candidates = candidates
        .into_iter()
        .map(|path| {
            path.strip_prefix(".")
                .map_or_else(|_| path.clone(), std::path::Path::to_path_buf)
        })
        .collect::<Vec<_>>();
    candidates.sort();
    candidates.dedup();
    candidates
}

fn cleanup_fixture_artifacts(
    profile: Option<&str>,
    all: bool,
    dry_run: bool,
) -> Result<Vec<serde_json::Value>> {
    let candidates = cleanup_fixture_candidates(profile, all)?;
    let mut entries = Vec::new();
    for candidate in candidates {
        let bytes = directory_bytes(&candidate.path)?;
        if !dry_run {
            std::fs::remove_dir_all(&candidate.path).with_context(|| {
                format!(
                    "failed to remove fixture directory `{}`",
                    candidate.path.display()
                )
            })?;
        }
        entries.push(serde_json::json!({
            "path": candidate.path,
            "profile": candidate.profile,
            "seed": candidate.seed,
            "kind": candidate.kind,
            "bytes": bytes,
            "removed": !dry_run,
        }));
    }
    Ok(entries)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CleanupFixtureCandidate {
    path: PathBuf,
    profile: String,
    seed: Option<u64>,
    kind: String,
}

fn cleanup_fixture_candidates(
    profile: Option<&str>,
    all: bool,
) -> Result<Vec<CleanupFixtureCandidate>> {
    cleanup_fixture_candidates_from(std::path::Path::new("fixtures"), profile, all)
}

fn cleanup_fixture_candidates_from(
    fixtures: &std::path::Path,
    profile: Option<&str>,
    all: bool,
) -> Result<Vec<CleanupFixtureCandidate>> {
    if profile.is_some() && all {
        bail!("cleanup --fixtures accepts either --profile <name> or --all, not both");
    }
    if !fixtures.exists() {
        return Ok(Vec::new());
    }
    let mut candidates = Vec::new();
    let mut pending = vec![fixtures.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).with_context(|| {
            format!(
                "failed to read fixtures directory `{}`",
                directory.display()
            )
        })? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let manifest = path.join("manifest.json");
            if manifest.is_file() {
                let manifest = read_json_file(&manifest).with_context(|| {
                    format!("failed to read fixture manifest `{}`", manifest.display())
                })?;
                let fixture_profile = manifest["profile"].as_str().with_context(|| {
                    format!("fixture `{}` manifest has no profile", path.display())
                })?;
                if all || profile == Some(fixture_profile) {
                    candidates.push(CleanupFixtureCandidate {
                        path,
                        profile: fixture_profile.to_string(),
                        seed: manifest["seed"].as_u64(),
                        kind: "fixture".to_string(),
                    });
                }
                continue;
            }

            if let Some((fixture_profile, seed)) = cleanup_progress_metadata(&path)? {
                if all || profile == Some(fixture_profile.as_str()) {
                    candidates.push(CleanupFixtureCandidate {
                        path,
                        profile: fixture_profile,
                        seed,
                        kind: "progress".to_string(),
                    });
                }
                continue;
            }

            pending.push(path);
        }
    }
    candidates.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(candidates)
}

fn cleanup_progress_metadata(path: &std::path::Path) -> Result<Option<(String, Option<u64>)>> {
    if path.extension().and_then(|extension| extension.to_str()) != Some("progress") {
        return Ok(None);
    }
    let latest = path.join("checkpoints/latest.json");
    if latest.is_file() {
        let checkpoint = read_json_file(&latest).with_context(|| {
            format!("failed to read progress checkpoint `{}`", latest.display())
        })?;
        let profile = checkpoint["profile"].as_str().with_context(|| {
            format!("progress checkpoint `{}` has no profile", latest.display())
        })?;
        return Ok(Some((profile.to_string(), checkpoint["seed"].as_u64())));
    }
    let progress_log = path.join("logs/progress.jsonl");
    if !progress_log.is_file() {
        return Ok(None);
    }
    let progress = read_optional_progress_log(path)?.with_context(|| {
        format!(
            "progress mirror `{}` has no progress events",
            path.display()
        )
    })?;
    let last = &progress["last"];
    let profile = last["profile"]
        .as_str()
        .with_context(|| format!("progress log `{}` has no profile", progress_log.display()))?;
    Ok(Some((profile.to_string(), last["seed"].as_u64())))
}

fn directory_bytes(path: &std::path::Path) -> Result<u64> {
    if path.is_file() {
        return Ok(std::fs::metadata(path)?.len());
    }
    let mut total = 0;
    for entry in std::fs::read_dir(path)
        .with_context(|| format!("failed to read directory `{}`", path.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            total += directory_bytes(&path)?;
        } else {
            total += entry.metadata()?.len();
        }
    }
    Ok(total)
}

fn ensure_scale_resume_replay_preconditions(
    checkpoint: &serde_json::Value,
    fixture: &std::path::Path,
) -> Result<()> {
    if checkpoint["safe_boundary"].as_bool() != Some(true) {
        bail!(
            "scale fixture resume requires a safe-boundary checkpoint; checkpoint phase is `{}`",
            checkpoint["phase"].as_str().unwrap_or("unknown")
        );
    }
    if fixture.exists() {
        bail!(
            "scale fixture deterministic resume replays from seed and requires output path `{}` to be absent",
            fixture.display()
        );
    }
    Ok(())
}

fn compare_fixtures(left: &std::path::Path, right: &std::path::Path) -> Result<serde_json::Value> {
    let left_manifest = read_manifest(left)?;
    let right_manifest = read_manifest(right)?;
    let left_search = read_optional_report(left, "search-corpus-report.json")?;
    let right_search = read_optional_report(right, "search-corpus-report.json")?;
    let left_distribution = read_optional_report(left, "object-distribution.json")?;
    let right_distribution = read_optional_report(right, "object-distribution.json")?;
    let left_world_plan = read_optional_report(left, "world-plan.json")?;
    let right_world_plan = read_optional_report(right, "world-plan.json")?;
    let left_checkpoint = read_optional_report(left, "checkpoints/completed.json")?;
    let right_checkpoint = read_optional_report(right, "checkpoints/completed.json")?;
    let left_invariant = read_optional_report(left, "invariant-report.json")?;
    let right_invariant = read_optional_report(right, "invariant-report.json")?;
    let left_progress = read_optional_progress_log(left)?;
    let right_progress = read_optional_progress_log(right)?;
    let left_count = left_manifest["object_count"].as_i64().unwrap_or_default();
    let right_count = right_manifest["object_count"].as_i64().unwrap_or_default();
    let left_size = fixture_database_bytes(left)?;
    let right_size = fixture_database_bytes(right)?;
    Ok(serde_json::json!({
        "left": left,
        "right": right,
        "left_profile": left_manifest["profile"],
        "right_profile": right_manifest["profile"],
        "object_count_difference": right_count - left_count,
        "object_type_distribution_difference": distribution_difference(&left_manifest["object_counts_by_type"], &right_manifest["object_counts_by_type"]),
        "status_distribution_difference": distribution_difference(&left_manifest["counts_by_status"], &right_manifest["counts_by_status"]),
        "conflict_distribution_difference": distribution_difference(&left_manifest["counts_by_conflict_type"], &right_manifest["counts_by_conflict_type"]),
        "conflict_behavior_difference": compare_conflict_reports(
            &left_manifest["conflict_report"],
            &right_manifest["conflict_report"],
        ),
        "logical_replay_difference": compare_logical_replay_digests(
            &left_manifest,
            &right_manifest,
        ),
        "database_size_difference": (right_size as i128) - (left_size as i128),
        "generation_time_difference_ms": right_manifest["performance_report"]["generation_ms"].as_i64().unwrap_or_default() - left_manifest["performance_report"]["generation_ms"].as_i64().unwrap_or_default(),
        "search_corpus_difference": compare_optional_reports(&left_search, &right_search),
        "object_distribution_report_difference": compare_optional_object_distribution_reports(
            &left_distribution,
            &right_distribution,
        ),
        "world_plan_difference": compare_optional_world_plans(&left_world_plan, &right_world_plan),
        "checkpoint_difference": compare_optional_checkpoints(&left_checkpoint, &right_checkpoint),
        "invariant_report_difference": compare_optional_invariant_reports(
            &left_invariant,
            &right_invariant,
        ),
        "progress_log_difference": compare_optional_progress_logs(&left_progress, &right_progress),
        "logical_topology_difference": {
            "actors": right_manifest["actor_count"].as_i64().unwrap_or_default() - left_manifest["actor_count"].as_i64().unwrap_or_default(),
            "ledgers": right_manifest["ledger_count"].as_i64().unwrap_or_default() - left_manifest["ledger_count"].as_i64().unwrap_or_default(),
            "replicas": right_manifest["replica_count"].as_i64().unwrap_or_default() - left_manifest["replica_count"].as_i64().unwrap_or_default(),
        }
    }))
}

fn compare_logical_replay_digests(
    left_manifest: &serde_json::Value,
    right_manifest: &serde_json::Value,
) -> serde_json::Value {
    let left = left_manifest["logical_replay_digest"].as_str();
    let right = right_manifest["logical_replay_digest"].as_str();
    serde_json::json!({
        "available": left.is_some() && right.is_some(),
        "changed": left.zip(right).is_some_and(|(left, right)| left != right),
        "left": left,
        "right": right,
    })
}

fn read_manifest(fixture: &std::path::Path) -> Result<serde_json::Value> {
    let manifest_path = fixture.join("manifest.json");
    read_json_file(&manifest_path)
}

fn read_json_file(path: &std::path::Path) -> Result<serde_json::Value> {
    serde_json::from_slice(
        &std::fs::read(path).with_context(|| format!("failed to read `{}`", path.display()))?,
    )
    .with_context(|| format!("failed to parse `{}`", path.display()))
}

fn scale_all_plan_json(
    seed: u64,
    target_objects: usize,
    output_base: PathBuf,
) -> Result<serde_json::Value> {
    let profiles = fact_sim_runner::scale::PROFILES
        .iter()
        .map(|profile| {
            scale_plan_json_for_output(
                profile,
                seed,
                target_objects,
                scale_fixture_output(&output_base, profile, seed),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let ready_count = profiles
        .iter()
        .filter(|plan| {
            plan["storage_preflight"]["sufficient"]
                .as_bool()
                .unwrap_or(false)
        })
        .count();
    Ok(serde_json::json!({
        "seed": seed,
        "effective_target_objects": target_objects,
        "output_base": output_base,
        "profile_count": profiles.len(),
        "storage_ready_profile_count": ready_count,
        "aggregate_storage_preflight": scale_all_preflight_summary(&profiles),
        "profiles": profiles,
    }))
}

fn scale_plan_json_for_output(
    profile: &str,
    seed: u64,
    target_objects: usize,
    output: PathBuf,
) -> Result<serde_json::Value> {
    let config = fact_sim_runner::scale::profile_config(profile, seed)
        .context("load scale fixture profile config")?;
    let budget = fact_sim_runner::scale::object_budget_plan(&config, target_objects);
    let storage_preflight =
        fact_sim_runner::scale::storage_preflight_report(&output, &config, &budget);
    let target_arg = if target_objects == config.target_objects {
        String::new()
    } else {
        format!(" --target-propositions {target_objects}")
    };
    Ok(serde_json::json!({
        "profile": profile,
        "seed": seed,
        "profile_target_objects": config.target_objects,
        "effective_target_objects": target_objects,
        "config": config,
        "object_budget": budget,
        "storage_preflight": storage_preflight,
        "suggested_output": output,
        "commands": {
            "generate": format!(
                "./target/release/fact-sim generate --profile {profile} --seed {seed}{target_arg} --output {}",
                output.display()
            ),
            "verify": format!("./target/release/fact-sim verify {}", output.display()),
            "inspect": format!("./target/release/fact-sim inspect {}", output.display()),
            "resume": format!(
                "./target/release/fact-sim resume {}/checkpoints/completed.json",
                output.display()
            )
        }
    }))
}

fn scale_all_preflight_summary(plans: &[serde_json::Value]) -> serde_json::Value {
    let total_estimated_storage_bytes = plans
        .iter()
        .map(|plan| {
            plan["storage_preflight"]["estimated_storage_bytes"]
                .as_u64()
                .unwrap_or_default()
        })
        .sum::<u64>();
    let available_bytes = plans
        .iter()
        .filter_map(|plan| plan["storage_preflight"]["available_bytes"].as_u64())
        .min();
    let individually_sufficient = plans.iter().all(|plan| {
        plan["storage_preflight"]["sufficient"]
            .as_bool()
            .unwrap_or(false)
    });
    let aggregate_sufficient = individually_sufficient
        && available_bytes.is_none_or(|bytes| bytes >= total_estimated_storage_bytes);
    let warning = if !individually_sufficient {
        Some("one or more profile preflights are insufficient".to_string())
    } else if let Some(bytes) = available_bytes {
        (bytes < total_estimated_storage_bytes).then(|| {
            format!(
                "combined estimated storage {total_estimated_storage_bytes} exceeds available bytes {bytes}"
            )
        })
    } else {
        None
    };
    serde_json::json!({
        "profile_count": plans.len(),
        "total_estimated_storage_bytes": total_estimated_storage_bytes,
        "minimum_available_bytes": available_bytes,
        "individual_profiles_sufficient": individually_sufficient,
        "sufficient": aggregate_sufficient,
        "warning": warning,
    })
}

fn scale_fixture_output(base: &std::path::Path, profile: &str, seed: u64) -> PathBuf {
    base.join(format!("{profile}-seed-{seed}"))
}

fn read_optional_report(
    fixture: &std::path::Path,
    name: &str,
) -> Result<Option<serde_json::Value>> {
    let path = fixture.join(name);
    if !path.exists() {
        return Ok(None);
    }
    serde_json::from_slice(
        &std::fs::read(&path).with_context(|| format!("failed to read `{}`", path.display()))?,
    )
    .with_context(|| format!("failed to parse `{}`", path.display()))
    .map(Some)
}

fn read_optional_progress_log(fixture: &std::path::Path) -> Result<Option<serde_json::Value>> {
    let path = fixture.join("logs").join("progress.jsonl");
    if !path.exists() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read `{}`", path.display()))?;
    let mut events = Vec::new();
    for (index, line) in content.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        events.push(
            serde_json::from_str::<serde_json::Value>(line).with_context(|| {
                format!(
                    "failed to parse progress event {} in `{}`",
                    index + 1,
                    path.display()
                )
            })?,
        );
    }
    let phases = events
        .iter()
        .filter_map(|event| event["phase"].as_str().map(str::to_string))
        .collect::<std::collections::BTreeSet<_>>();
    Ok(Some(serde_json::json!({
        "event_count": events.len(),
        "phases": phases,
        "first": events.first(),
        "last": events.last(),
    })))
}

fn inspect_fixture_json(
    fixture: &std::path::Path,
    manifest: &serde_json::Value,
    artifacts: Vec<serde_json::Value>,
    world_plan: Option<&serde_json::Value>,
    completed_checkpoint: Option<&serde_json::Value>,
) -> serde_json::Value {
    serde_json::json!({
        "fixture": fixture,
        "profile": manifest["profile"],
        "seed": manifest["seed"],
        "target_objects": manifest["target_objects"],
        "object_count": manifest["object_count"],
        "object_counts_by_type": manifest["object_counts_by_type"],
        "scenario_family_counts": manifest["scenario_family_counts"],
        "target_object_overshoot": manifest["target_object_overshoot"],
        "commitment_root": manifest["commitment_root"],
        "logical_replay_digest": manifest["logical_replay_digest"],
        "artifacts": artifacts,
        "databases": manifest["databases"],
        "world_plan_summary": world_plan.map(|plan| serde_json::json!({
            "logical_replay_digest": plan["logical_replay_digest"],
            "planner": plan["planner"],
            "configured_world": plan["configured_world"],
            "configured_distribution": plan["configured_distribution"],
            "expected_object_budget": plan["expected_object_budget"],
            "storage_preflight": plan["storage_preflight"],
        })),
        "completed_checkpoint_summary": completed_checkpoint.map(|checkpoint| serde_json::json!({
            "completed": checkpoint["completed"],
            "safe_boundary": checkpoint["safe_boundary"],
            "logical_replay_digest": checkpoint["logical_replay_digest"],
            "current_scenario_instance": checkpoint["current_scenario_instance"],
            "current_object_count": checkpoint["current_object_count"],
            "simulated_time": checkpoint["simulated_time"],
            "progress": checkpoint["progress"],
        })),
    })
}

fn inspect_artifacts(fixture: &std::path::Path, profile: Option<&str>) -> Vec<serde_json::Value> {
    let mut names = vec!["manifest.json"];
    if profile.is_some_and(fact_sim_runner::scale::is_bulk_proposition_profile) {
        names.extend(scale_bulk_required_sidecars());
    } else if profile.is_some_and(fact_sim_runner::scale::is_scale_profile) {
        names.extend(scale_required_sidecars());
    } else {
        names.push("objects.factbndl");
    }
    names
        .into_iter()
        .map(|name| {
            let path = fixture.join(name);
            serde_json::json!({
                "name": name,
                "path": path,
                "exists": path.exists(),
                "bytes": std::fs::metadata(&path).map(|meta| meta.len()).unwrap_or_default(),
            })
        })
        .collect()
}

fn compare_optional_reports(
    left: &Option<serde_json::Value>,
    right: &Option<serde_json::Value>,
) -> serde_json::Value {
    match (left, right) {
        (Some(left), Some(right)) => serde_json::json!({
            "available": true,
            "indexed_changed": left["indexed"] != right["indexed"],
            "sampled_query_count_difference": right["sampled_query_count"].as_i64().unwrap_or_default()
                - left["sampled_query_count"].as_i64().unwrap_or_default(),
            "sampled_search_query_count_difference": right["sampled_search_query_count"].as_i64().unwrap_or_default()
                - left["sampled_search_query_count"].as_i64().unwrap_or_default(),
            "searchable_object_counts_difference": distribution_difference(
                &left["searchable_object_counts"],
                &right["searchable_object_counts"],
            ),
            "sampled_search_terms_difference": string_array_difference(
                &left["sampled_search_terms"],
                &right["sampled_search_terms"],
            ),
            "search_evidence_changed": {
                "effective_search_sampled": left["effective_search_sampled"] != right["effective_search_sampled"],
                "status_filter_search_sampled": left["status_filter_search_sampled"] != right["status_filter_search_sampled"],
                "bounded_page_size_sampled": left["bounded_page_size_sampled"] != right["bounded_page_size_sampled"],
                "ambiguous_reference_sampled": left["ambiguous_reference_sampled"] != right["ambiguous_reference_sampled"],
            },
            "sampled_search_query_difference": compare_sampled_search_queries(
                &left["sampled_search_queries"],
                &right["sampled_search_queries"],
            ),
            "cli_ux_coverage_difference": string_array_difference(
                &left["cli_ux_coverage"],
                &right["cli_ux_coverage"],
            ),
        }),
        (None, None) => serde_json::json!({
            "available": false,
            "reason": "search-corpus-report.json missing on both fixtures",
        }),
        (Some(_), None) => serde_json::json!({
            "available": false,
            "reason": "search-corpus-report.json missing on right fixture",
        }),
        (None, Some(_)) => serde_json::json!({
            "available": false,
            "reason": "search-corpus-report.json missing on left fixture",
        }),
    }
}

fn compare_sampled_search_queries(
    left: &serde_json::Value,
    right: &serde_json::Value,
) -> serde_json::Value {
    let left_by_key = sampled_search_query_map(left);
    let right_by_key = sampled_search_query_map(right);
    let keys = left_by_key
        .keys()
        .chain(right_by_key.keys())
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let differences = keys
        .into_iter()
        .map(|key| {
            let left = left_by_key.get(&key);
            let right = right_by_key.get(&key);
            (
                key,
                serde_json::json!({
                    "present_left": left.is_some(),
                    "present_right": right.is_some(),
                    "result_count_difference": right.and_then(|value| value["result_count"].as_i64()).unwrap_or_default()
                        - left.and_then(|value| value["result_count"].as_i64()).unwrap_or_default(),
                    "page_size_changed": left.map(|value| &value["page_size"]) != right.map(|value| &value["page_size"]),
                    "bounded_by_page_size_changed": left.map(|value| &value["bounded_by_page_size"])
                        != right.map(|value| &value["bounded_by_page_size"]),
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    serde_json::Value::Object(differences)
}

fn sampled_search_query_map(
    value: &serde_json::Value,
) -> std::collections::BTreeMap<String, serde_json::Value> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .map(|item| {
            let text = item["text"].as_str().unwrap_or_default();
            let status = item["status"].as_str().unwrap_or("*");
            let effective = item["effective"].as_bool().unwrap_or_default();
            let page_size = item["page_size"].as_u64().unwrap_or_default();
            let key = format!("{text}|status={status}|effective={effective}|page_size={page_size}");
            (key, item.clone())
        })
        .collect()
}

fn compare_optional_object_distribution_reports(
    left: &Option<serde_json::Value>,
    right: &Option<serde_json::Value>,
) -> serde_json::Value {
    match (left, right) {
        (Some(left), Some(right)) => serde_json::json!({
            "available": true,
            "revision_depth_difference": numeric_summary_difference(
                &left["revision_depth"],
                &right["revision_depth"],
            ),
            "deliberation_size_difference": numeric_summary_difference(
                &left["deliberation_size"],
                &right["deliberation_size"],
            ),
            "participant_count_difference": numeric_summary_difference(
                &left["participant_count"],
                &right["participant_count"],
            ),
            "object_counts_per_ledger_difference": distribution_difference(
                &left["object_counts_per_ledger"],
                &right["object_counts_per_ledger"],
            ),
            "object_counts_per_simulated_year_difference": distribution_difference(
                &left["object_counts_per_simulated_year"],
                &right["object_counts_per_simulated_year"],
            ),
            "pending_counts_by_kind_difference": distribution_difference(
                &left["pending_counts_by_kind"],
                &right["pending_counts_by_kind"],
            ),
            "reconciliation_counts_by_mode_difference": distribution_difference(
                &left["reconciliation_counts_by_mode"],
                &right["reconciliation_counts_by_mode"],
            ),
            "deep_validation_category_count_difference": distribution_difference(
                &left["deep_validation_sample"]["category_counts"],
                &right["deep_validation_sample"]["category_counts"],
            ),
            "deep_validation_coverage_changed": bool_map_changed(
                &left["deep_validation_sample"]["coverage"],
                &right["deep_validation_sample"]["coverage"],
            ),
        }),
        (None, None) => serde_json::json!({
            "available": false,
            "reason": "object-distribution.json missing on both fixtures",
        }),
        (Some(_), None) => serde_json::json!({
            "available": false,
            "reason": "object-distribution.json missing on right fixture",
        }),
        (None, Some(_)) => serde_json::json!({
            "available": false,
            "reason": "object-distribution.json missing on left fixture",
        }),
    }
}

fn numeric_summary_difference(
    left: &serde_json::Value,
    right: &serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "count": numeric_value_difference(&left["count"], &right["count"]),
        "average": right["average"].as_f64().unwrap_or_default()
            - left["average"].as_f64().unwrap_or_default(),
        "maximum": numeric_value_difference(&left["maximum"], &right["maximum"]),
    })
}

fn bool_map_changed(left: &serde_json::Value, right: &serde_json::Value) -> serde_json::Value {
    let mut keys = std::collections::BTreeSet::new();
    if let Some(object) = left.as_object() {
        keys.extend(object.keys().cloned());
    }
    if let Some(object) = right.as_object() {
        keys.extend(object.keys().cloned());
    }
    serde_json::Value::Object(
        keys.into_iter()
            .map(|key| {
                let changed = left[&key].as_bool().unwrap_or_default()
                    != right[&key].as_bool().unwrap_or_default();
                (key, serde_json::json!(changed))
            })
            .collect(),
    )
}

fn compare_optional_checkpoints(
    left: &Option<serde_json::Value>,
    right: &Option<serde_json::Value>,
) -> serde_json::Value {
    match (left, right) {
        (Some(left), Some(right)) => serde_json::json!({
            "available": true,
            "safe_boundary_changed": left["safe_boundary"] != right["safe_boundary"],
            "completed_changed": left["completed"] != right["completed"],
            "phase_changed": left["phase"] != right["phase"],
            "logical_replay_digest_changed": left["logical_replay_digest"] != right["logical_replay_digest"],
            "current_scenario_instance_difference": numeric_value_difference(
                &left["current_scenario_instance"],
                &right["current_scenario_instance"],
            ),
            "current_object_count_difference": numeric_value_difference(
                &left["current_object_count"],
                &right["current_object_count"],
            ),
            "random_generator_state_changed": left["random_generator_state"] != right["random_generator_state"],
            "world_plan_position_difference": {
                "next_scenario_instance": numeric_value_difference(
                    &left["world_plan_position"]["next_scenario_instance"],
                    &right["world_plan_position"]["next_scenario_instance"],
                ),
                "executed_scenario_instances": numeric_value_difference(
                    &left["world_plan_position"]["executed_scenario_instances"],
                    &right["world_plan_position"]["executed_scenario_instances"],
                ),
            },
            "progress_difference": {
                "elapsed_seconds": right["progress"]["elapsed_seconds"].as_f64().unwrap_or_default()
                    - left["progress"]["elapsed_seconds"].as_f64().unwrap_or_default(),
                "objects_per_second": right["progress"]["objects_per_second"].as_f64().unwrap_or_default()
                    - left["progress"]["objects_per_second"].as_f64().unwrap_or_default(),
                "progress_percent": right["progress"]["progress_percent"].as_f64().unwrap_or_default()
                    - left["progress"]["progress_percent"].as_f64().unwrap_or_default(),
                "database_bytes": numeric_value_difference(
                    &left["progress"]["database_bytes"],
                    &right["progress"]["database_bytes"],
                ),
                "conflict_count": numeric_value_difference(
                    &left["progress"]["conflict_count"],
                    &right["progress"]["conflict_count"],
                ),
                "scenario_failure_count": numeric_value_difference(
                    &left["progress"]["scenario_failure_count"],
                    &right["progress"]["scenario_failure_count"],
                ),
            },
            "partial_report_state_difference": {
                "scenario_family_counts": distribution_difference(
                    &left["partial_report_state"]["scenario_family_counts"],
                    &right["partial_report_state"]["scenario_family_counts"],
                ),
                "scenario_family_object_counts": distribution_difference(
                    &left["partial_report_state"]["scenario_family_object_counts"],
                    &right["partial_report_state"]["scenario_family_object_counts"],
                ),
                "object_counts_by_type": distribution_difference(
                    &left["partial_report_state"]["object_counts_by_type"],
                    &right["partial_report_state"]["object_counts_by_type"],
                ),
                "counts_by_status": distribution_difference(
                    &left["partial_report_state"]["counts_by_status"],
                    &right["partial_report_state"]["counts_by_status"],
                ),
                "counts_by_conflict_type": distribution_difference(
                    &left["partial_report_state"]["counts_by_conflict_type"],
                    &right["partial_report_state"]["counts_by_conflict_type"],
                ),
                "scenario_failure_count": numeric_value_difference(
                    &left["partial_report_state"]["scenario_failure_count"],
                    &right["partial_report_state"]["scenario_failure_count"],
                ),
            },
        }),
        (None, None) => serde_json::json!({
            "available": false,
            "reason": "checkpoints/completed.json missing on both fixtures",
        }),
        (Some(_), None) => serde_json::json!({
            "available": false,
            "reason": "checkpoints/completed.json missing on right fixture",
        }),
        (None, Some(_)) => serde_json::json!({
            "available": false,
            "reason": "checkpoints/completed.json missing on left fixture",
        }),
    }
}

fn compare_optional_invariant_reports(
    left: &Option<serde_json::Value>,
    right: &Option<serde_json::Value>,
) -> serde_json::Value {
    match (left, right) {
        (Some(left), Some(right)) => serde_json::json!({
            "available": true,
            "assertions_changed": left["assertions"] != right["assertions"],
            "failure_context_fields_changed": left["failure_context_contract"]["fields"]
                != right["failure_context_contract"]["fields"],
            "failure_context_complete_changed": left["failure_context_contract"]["complete"]
                != right["failure_context_contract"]["complete"],
            "safeguard_observation_difference": {
                "scenario_failure_count": numeric_value_difference(
                    &left["safeguard_observations"]["scenario_failure_count"],
                    &right["safeguard_observations"]["scenario_failure_count"],
                ),
                "retry_count": numeric_value_difference(
                    &left["safeguard_observations"]["retry_count"],
                    &right["safeguard_observations"]["retry_count"],
                ),
                "database_bytes": numeric_value_difference(
                    &left["safeguard_observations"]["database_bytes"],
                    &right["safeguard_observations"]["database_bytes"],
                ),
                "generation_ms": numeric_value_difference(
                    &left["safeguard_observations"]["generation_ms"],
                    &right["safeguard_observations"]["generation_ms"],
                ),
                "peak_memory_bytes": numeric_value_difference(
                    &left["safeguard_observations"]["peak_memory_bytes"],
                    &right["safeguard_observations"]["peak_memory_bytes"],
                ),
            },
        }),
        (None, None) => serde_json::json!({
            "available": false,
            "reason": "invariant-report.json missing on both fixtures",
        }),
        (Some(_), None) => serde_json::json!({
            "available": false,
            "reason": "invariant-report.json missing on right fixture",
        }),
        (None, Some(_)) => serde_json::json!({
            "available": false,
            "reason": "invariant-report.json missing on left fixture",
        }),
    }
}

fn compare_optional_progress_logs(
    left: &Option<serde_json::Value>,
    right: &Option<serde_json::Value>,
) -> serde_json::Value {
    match (left, right) {
        (Some(left), Some(right)) => serde_json::json!({
            "available": true,
            "event_count_difference": numeric_value_difference(
                &left["event_count"],
                &right["event_count"],
            ),
            "phases_changed": left["phases"] != right["phases"],
            "first_event_changed": left["first"] != right["first"],
            "last_event_difference": {
                "current_scenario_instance": numeric_value_difference(
                    &left["last"]["current_scenario_instance"],
                    &right["last"]["current_scenario_instance"],
                ),
                "current_object_count": numeric_value_difference(
                    &left["last"]["current_object_count"],
                    &right["last"]["current_object_count"],
                ),
                "scenario_failure_count": numeric_value_difference(
                    &left["last"]["progress"]["scenario_failure_count"],
                    &right["last"]["progress"]["scenario_failure_count"],
                ),
                "database_bytes": numeric_value_difference(
                    &left["last"]["progress"]["database_bytes"],
                    &right["last"]["progress"]["database_bytes"],
                ),
                "completed_changed": left["last"]["completed"] != right["last"]["completed"],
                "safe_boundary_changed": left["last"]["safe_boundary"] != right["last"]["safe_boundary"],
            },
        }),
        (None, None) => serde_json::json!({
            "available": false,
            "reason": "logs/progress.jsonl missing on both fixtures",
        }),
        (Some(_), None) => serde_json::json!({
            "available": false,
            "reason": "logs/progress.jsonl missing on right fixture",
        }),
        (None, Some(_)) => serde_json::json!({
            "available": false,
            "reason": "logs/progress.jsonl missing on left fixture",
        }),
    }
}

fn compare_optional_world_plans(
    left: &Option<serde_json::Value>,
    right: &Option<serde_json::Value>,
) -> serde_json::Value {
    match (left, right) {
        (Some(left), Some(right)) => serde_json::json!({
            "available": true,
            "logical_replay_digest_changed": left["logical_replay_digest"] != right["logical_replay_digest"],
            "planner_changed": left["planner"] != right["planner"],
            "configured_world_changed": left["configured_world"] != right["configured_world"],
            "configured_distribution_difference": distribution_difference(
                &left["configured_distribution"],
                &right["configured_distribution"],
            ),
            "configured_safeguards_difference": distribution_difference(
                &left["configured_safeguards"],
                &right["configured_safeguards"],
            ),
            "realized_topology_difference": distribution_difference(
                &left["realized_topology"],
                &right["realized_topology"],
            ),
            "budget_difference": {
                "target_objects": numeric_value_difference(
                    &left["expected_object_budget"]["target_objects"],
                    &right["expected_object_budget"]["target_objects"],
                ),
                "estimated_instances": numeric_value_difference(
                    &left["expected_object_budget"]["estimated_instances"],
                    &right["expected_object_budget"]["estimated_instances"],
                ),
                "estimated_topology_objects": numeric_value_difference(
                    &left["expected_object_budget"]["estimated_topology_objects"],
                    &right["expected_object_budget"]["estimated_topology_objects"],
                ),
                "estimated_objects": numeric_value_difference(
                    &left["expected_object_budget"]["estimated_objects"],
                    &right["expected_object_budget"]["estimated_objects"],
                ),
                "estimated_storage_bytes": numeric_value_difference(
                    &left["expected_object_budget"]["estimated_storage_bytes"],
                    &right["expected_object_budget"]["estimated_storage_bytes"],
                ),
            },
            "storage_preflight_difference": {
                "estimated_storage_bytes": numeric_value_difference(
                    &left["storage_preflight"]["estimated_storage_bytes"],
                    &right["storage_preflight"]["estimated_storage_bytes"],
                ),
                "available_bytes": numeric_value_difference(
                    &left["storage_preflight"]["available_bytes"],
                    &right["storage_preflight"]["available_bytes"],
                ),
                "sufficient_changed": left["storage_preflight"]["sufficient"]
                    != right["storage_preflight"]["sufficient"],
            },
        }),
        (None, None) => serde_json::json!({
            "available": false,
            "reason": "world-plan.json missing on both fixtures",
        }),
        (Some(_), None) => serde_json::json!({
            "available": false,
            "reason": "world-plan.json missing on right fixture",
        }),
        (None, Some(_)) => serde_json::json!({
            "available": false,
            "reason": "world-plan.json missing on left fixture",
        }),
    }
}

fn distribution_difference(
    left: &serde_json::Value,
    right: &serde_json::Value,
) -> serde_json::Value {
    let mut keys = std::collections::BTreeSet::new();
    if let Some(object) = left.as_object() {
        keys.extend(object.keys().cloned());
    }
    if let Some(object) = right.as_object() {
        keys.extend(object.keys().cloned());
    }
    serde_json::Value::Object(
        keys.into_iter()
            .map(|key| {
                let left_value = left[&key].as_i64().unwrap_or_default();
                let right_value = right[&key].as_i64().unwrap_or_default();
                (key, serde_json::json!(right_value - left_value))
            })
            .collect(),
    )
}

fn numeric_value_difference(left: &serde_json::Value, right: &serde_json::Value) -> i128 {
    right.as_i64().unwrap_or_default() as i128 - left.as_i64().unwrap_or_default() as i128
}

fn string_array_difference(
    left: &serde_json::Value,
    right: &serde_json::Value,
) -> serde_json::Value {
    let left = left
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    let right = right
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    serde_json::json!({
        "only_left": left.difference(&right).copied().collect::<Vec<_>>(),
        "only_right": right.difference(&left).copied().collect::<Vec<_>>(),
    })
}

fn compare_conflict_reports(
    left: &serde_json::Value,
    right: &serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "compatible_deliberations_without_conflict_difference": numeric_value_difference(
            &left["compatible_deliberations_without_conflict"],
            &right["compatible_deliberations_without_conflict"],
        ),
        "last_undisputed_ancestor_preserved_changed": left["last_undisputed_ancestor_preserved_as_effective"]
            != right["last_undisputed_ancestor_preserved_as_effective"],
        "arrival_order_selected_winner_changed": left["arrival_order_selected_winner"]
            != right["arrival_order_selected_winner"],
        "sample_conflict_proposition_changed": left["sample_conflict_proposition_id"]
            != right["sample_conflict_proposition_id"],
        "conflict_replicas_difference": string_array_difference(
            &left["conflict_replicas"],
            &right["conflict_replicas"],
        ),
    })
}

fn fixture_database_bytes(fixture: &std::path::Path) -> Result<u64> {
    let manifest = read_manifest(fixture)?;
    let mut total = 0_u64;
    if let Some(databases) = manifest["databases"].as_object() {
        for path in databases.values().filter_map(|value| value.as_str()) {
            let path = resolve_packaged_fixture_path(fixture, std::path::Path::new(path));
            total += std::fs::metadata(path)
                .map(|meta| meta.len())
                .unwrap_or_default();
        }
    } else if let Some(path) = manifest["database"].as_str() {
        let path = resolve_packaged_fixture_path(fixture, std::path::Path::new(path));
        total += std::fs::metadata(path)
            .map(|meta| meta.len())
            .unwrap_or_default();
    }
    Ok(total)
}

fn resolve_packaged_fixture_path(fixture: &std::path::Path, recorded: &std::path::Path) -> PathBuf {
    if recorded.exists() {
        return recorded.to_path_buf();
    }

    let mut candidates = Vec::new();
    if let Some(file_name) = recorded.file_name() {
        candidates.push(fixture.join(file_name));
        if let Some(parent_name) = recorded.parent().and_then(std::path::Path::file_name) {
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

fn fixture_profile(fixture: &std::path::Path) -> Result<Option<String>> {
    let manifest = fixture.join("manifest.json");
    let value: serde_json::Value = serde_json::from_slice(
        &std::fs::read(&manifest)
            .with_context(|| format!("failed to read `{}`", manifest.display()))?,
    )
    .with_context(|| format!("failed to parse `{}`", manifest.display()))?;
    Ok(value["profile"].as_str().map(str::to_owned))
}

fn print_conflict_repair_report(
    fixture: &std::path::Path,
    status: &str,
    report: &fact_sim_runner::conflict_repair::ConflictRepairReport,
    full: bool,
) -> Result<()> {
    let output = conflict_repair_report_json(fixture, status, report, full)?;
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn print_scale_report_allowing_reduced_target(
    fixture: &std::path::Path,
    status: &str,
    full: bool,
) -> Result<()> {
    let output = scale_report_json_allowing_reduced_target(fixture, status, full)?;
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn scale_report_json_allowing_reduced_target(
    fixture: &std::path::Path,
    status: &str,
    full: bool,
) -> Result<serde_json::Value> {
    let manifest = read_manifest(fixture)?;
    let bulk_profile = manifest["profile"]
        .as_str()
        .is_some_and(fact_sim_runner::scale::is_bulk_proposition_profile);
    let target_objects = manifest["target_objects"].as_u64().unwrap_or_default();
    if target_objects >= fact_sim_runner::scale::TARGET_OBJECTS as u64 {
        let report = fact_sim_runner::scale::verify_scale_fixture(fixture)?;
        return conflict_repair_report_json(fixture, status, &report, full);
    }
    if bulk_profile {
        let report = fact_sim_runner::scale::verify_scale_fixture(fixture)?;
        let mut output =
            conflict_repair_report_json(fixture, &format!("{status}-smoke"), &report, full)?;
        output["full_verification_note"] = serde_json::json!(
            "reduced target bulk proposition smoke report; cargo sim verify still enforces the 500,000-proposition gate for full fixtures"
        );
        return Ok(output);
    }

    let mut report: fact_sim_runner::conflict_repair::ConflictRepairReport =
        serde_json::from_value(manifest)
            .with_context(|| format!("failed to parse manifest for `{}`", fixture.display()))?;
    normalize_scale_smoke_report_paths(fixture, &mut report);
    fact_sim_runner::scale::validate_scale_fixture_checkpoint_metadata(fixture)?;
    ensure_scale_smoke_sidecars(fixture, &report)?;
    let mut output =
        conflict_repair_report_json(fixture, &format!("{status}-smoke"), &report, full)?;
    output["full_verification_note"] = serde_json::json!(
        "reduced target smoke report; cargo sim verify still enforces the 500,000-proposition gate"
    );
    Ok(output)
}

fn ensure_scale_smoke_sidecars(
    fixture: &std::path::Path,
    report: &fact_sim_runner::conflict_repair::ConflictRepairReport,
) -> Result<()> {
    for artifact in scale_required_sidecars() {
        let path = fixture.join(artifact);
        if !path.exists() {
            bail!(
                "scale fixture reduced smoke report is missing required sidecar `{}`",
                path.display()
            );
        }
    }
    validate_scale_smoke_sidecar_content(fixture, report)?;
    Ok(())
}

fn normalize_scale_smoke_report_paths(
    fixture: &std::path::Path,
    report: &mut fact_sim_runner::conflict_repair::ConflictRepairReport,
) {
    report.output = fixture.to_path_buf();
    for database in report.databases.values_mut() {
        *database = resolve_packaged_fixture_path(fixture, database);
    }
    report.bundle = resolve_packaged_fixture_path(fixture, &report.bundle);
    for bundle_path in &mut report.bundle_paths {
        *bundle_path = resolve_packaged_fixture_path(fixture, bundle_path);
    }
    for snapshot_path in &mut report.snapshot_paths {
        *snapshot_path = resolve_packaged_fixture_path(fixture, snapshot_path);
    }
}

fn scale_required_sidecars() -> &'static [&'static str] {
    &[
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
    ]
}

fn scale_bulk_required_sidecars() -> &'static [&'static str] {
    &[
        "profile.yaml",
        "timing-report.json",
        "bundles/objects.factbndl",
    ]
}

fn validate_scale_smoke_sidecar_content(
    fixture: &std::path::Path,
    report: &fact_sim_runner::conflict_repair::ConflictRepairReport,
) -> Result<()> {
    let world_plan = read_json_sidecar(fixture, "world-plan.json")?;
    if world_plan["profile"].as_str() != Some(report.profile.as_str())
        || world_plan["seed"].as_u64() != Some(report.seed)
        || world_plan["logical_replay_digest"].as_str()
            != Some(report.logical_replay_digest.as_str())
        || world_plan["target_objects"].as_u64() != Some(report.target_objects as u64)
        || world_plan["expected_object_budget"]["target_objects"].as_u64()
            != Some(report.target_objects as u64)
        || world_plan["scenario_family_counts"]
            != serde_json::to_value(&report.scenario_family_counts)?
        || world_plan["scenario_family_object_counts"]
            != serde_json::to_value(&report.scenario_family_object_counts)?
    {
        bail!("scale fixture reduced smoke report world plan does not match manifest");
    }

    let scenario_report = read_json_sidecar(fixture, "scenario-report.json")?;
    if scenario_report["generated_instances"].as_u64() != Some(report.generated_instances as u64)
        || scenario_report["target_objects"].as_u64() != Some(report.target_objects as u64)
        || scenario_report["scenario_family_counts"]
            != serde_json::to_value(&report.scenario_family_counts)?
        || scenario_report["scenario_family_object_counts"]
            != serde_json::to_value(&report.scenario_family_object_counts)?
        || scenario_report["target_object_overshoot"].as_u64()
            != Some(report.target_object_overshoot as u64)
    {
        bail!("scale fixture reduced smoke report scenario sidecar does not match manifest");
    }

    let object_distribution = read_json_sidecar(fixture, "object-distribution.json")?;
    if object_distribution["object_count"].as_u64() != Some(report.object_count as u64)
        || object_distribution["object_counts_by_type"]
            != serde_json::to_value(&report.object_counts_by_type)?
        || object_distribution["counts_by_status"]
            != serde_json::to_value(&report.counts_by_status)?
        || object_distribution["deep_validation_sample"] != report.deep_validation_sample
    {
        bail!("scale fixture reduced smoke report object distribution does not match manifest");
    }

    let invariant_report = read_json_sidecar(fixture, "invariant-report.json")?;
    if invariant_report["assertions"] != serde_json::to_value(&report.assertion_report)?
        || invariant_report["failure_context_contract"]["complete"].as_bool() != Some(true)
    {
        bail!("scale fixture reduced smoke report invariant sidecar does not match manifest");
    }

    let projection_report = read_json_sidecar(fixture, "projection-report.json")?;
    if projection_report["projection_rebuild_equivalent"].as_bool()
        != Some(report.assertion_report.projection_rebuild_equivalent)
        || projection_report["converged_projections"].as_bool()
            != Some(report.assertion_report.converged_projections)
        || projection_report["canonical_history_preserved"].as_bool()
            != Some(report.repair_report.canonical_history_preserved)
    {
        bail!("scale fixture reduced smoke report projection sidecar does not match manifest");
    }

    let search_corpus = read_json_sidecar(fixture, "search-corpus-report.json")?;
    if search_corpus["indexed"].as_bool() != Some(true)
        || search_corpus["cli_ux_coverage"] != serde_json::to_value(&report.cli_ux_coverage)?
    {
        bail!("scale fixture reduced smoke report search sidecar does not match manifest");
    }

    let timing_report = read_json_sidecar(fixture, "timing-report.json")?;
    if timing_report["performance"]["database_bytes"].as_u64()
        != Some(report.performance_report.database_bytes)
        || timing_report["simulated_duration_seconds"].as_i64()
            != Some(report.simulated_duration_seconds)
    {
        bail!("scale fixture reduced smoke report timing sidecar does not match manifest");
    }

    let commitment_report = read_json_sidecar(fixture, "commitments/object-set.json")?;
    if commitment_report["root"].as_str() != report.commitment_root.as_deref()
        || commitment_report["object_count"].as_u64() != Some(report.object_count as u64)
        || commitment_report["verified"].as_bool() != Some(true)
    {
        bail!("scale fixture reduced smoke report commitment sidecar does not match manifest");
    }

    let snapshot_report = read_json_sidecar(fixture, "snapshots/object-set.json")?;
    if snapshot_report["commitment_root"].as_str() != report.commitment_root.as_deref()
        || snapshot_report["unique_object_count"].as_u64() != Some(report.object_count as u64)
    {
        bail!("scale fixture reduced smoke report snapshot sidecar does not match manifest");
    }

    let bundle_inventory = read_json_sidecar(fixture, "bundles/inventory.json")?;
    if bundle_inventory["bundle_count"]
        .as_u64()
        .unwrap_or_default()
        == 0
    {
        bail!("scale fixture reduced smoke report bundle inventory is empty");
    }

    let progress_log = read_optional_progress_log(fixture)?
        .context("scale fixture reduced smoke report is missing progress events")?;
    if progress_log["event_count"].as_u64().unwrap_or_default() == 0
        || progress_log["last"]["profile"].as_str() != Some(report.profile.as_str())
        || progress_log["last"]["seed"].as_u64() != Some(report.seed)
        || progress_log["last"]["target_objects"].as_u64() != Some(report.target_objects as u64)
        || progress_log["last"]["current_object_count"].as_u64() != Some(report.object_count as u64)
        || progress_log["last"]["completed"].as_bool() != Some(true)
    {
        bail!("scale fixture reduced smoke report progress log does not match manifest");
    }

    validate_scale_smoke_package_layout(fixture, report)?;
    Ok(())
}

fn validate_scale_smoke_package_layout(
    fixture: &std::path::Path,
    report: &fact_sim_runner::conflict_repair::ConflictRepairReport,
) -> Result<()> {
    let ledger_dir = fixture.join("ledgers");
    for database in report.databases.values() {
        if database.parent() != Some(ledger_dir.as_path()) {
            bail!(
                "scale fixture reduced smoke report database `{}` is not under `{}`",
                database.display(),
                ledger_dir.display()
            );
        }
        if !database.exists() {
            bail!(
                "scale fixture reduced smoke report references missing database `{}`",
                database.display()
            );
        }
    }

    let expected_bundle = fixture.join("bundles").join("objects.factbndl");
    if report.bundle != expected_bundle
        || !report
            .bundle_paths
            .iter()
            .any(|bundle| bundle == &expected_bundle)
    {
        bail!(
            "scale fixture reduced smoke report primary object bundle is not packaged as `{}`",
            expected_bundle.display()
        );
    }
    for bundle in &report.bundle_paths {
        if !bundle.exists() {
            bail!(
                "scale fixture reduced smoke report references missing bundle `{}`",
                bundle.display()
            );
        }
    }
    for snapshot in &report.snapshot_paths {
        if !snapshot.exists() {
            bail!(
                "scale fixture reduced smoke report references missing snapshot `{}`",
                snapshot.display()
            );
        }
    }
    Ok(())
}

fn read_json_sidecar(fixture: &std::path::Path, artifact: &str) -> Result<serde_json::Value> {
    let path = fixture.join(artifact);
    serde_json::from_slice(
        &std::fs::read(&path).with_context(|| format!("failed to read `{}`", path.display()))?,
    )
    .with_context(|| format!("failed to parse `{}`", path.display()))
}

fn conflict_repair_report_json(
    fixture: &std::path::Path,
    status: &str,
    report: &fact_sim_runner::conflict_repair::ConflictRepairReport,
    full: bool,
) -> Result<serde_json::Value> {
    let cli_sample_report = if full {
        serde_json::to_value(&report.cli_sample_report)?
    } else {
        serde_json::Value::Array(
            report
                .cli_sample_report
                .iter()
                .map(|receipt| {
                    serde_json::json!({
                        "command": receipt.command,
                        "status": receipt.status,
                        "duration_ms": receipt.duration_ms,
                        "stdout_bytes": receipt.stdout.len(),
                        "stderr_bytes": receipt.stderr.len(),
                        "parsed_json": receipt.parsed_json.is_some(),
                    })
                })
                .collect(),
        )
    };
    let http_sample_report = if full {
        serde_json::to_value(&report.http_sample_report)?
    } else {
        serde_json::Value::Array(
            report
                .http_sample_report
                .iter()
                .map(|receipt| {
                    serde_json::json!({
                        "method": receipt.method,
                        "path": receipt.path,
                        "status": receipt.status,
                        "duration_ms": receipt.duration_ms,
                        "body_bytes": receipt.body_bytes,
                        "parsed_json": receipt.parsed_json.is_some(),
                    })
                })
                .collect(),
        )
    };
    let mut output = serde_json::Map::new();
    output.insert("fixture".into(), serde_json::to_value(fixture)?);
    output.insert("status".into(), serde_json::to_value(status)?);
    output.insert("profile".into(), serde_json::to_value(&report.profile)?);
    output.insert(
        "profile_version".into(),
        serde_json::to_value(report.profile_version)?,
    );
    output.insert("seed".into(), serde_json::to_value(report.seed)?);
    output.insert(
        "scheduler_version".into(),
        serde_json::to_value(&report.scheduler_version)?,
    );
    output.insert(
        "scenario_corpus_version".into(),
        serde_json::to_value(&report.scenario_corpus_version)?,
    );
    output.insert(
        "content_template_version".into(),
        serde_json::to_value(&report.content_template_version)?,
    );
    output.insert(
        "time_distribution_profile".into(),
        serde_json::to_value(&report.time_distribution_profile)?,
    );
    output.insert(
        "started_at".into(),
        serde_json::to_value(report.started_at)?,
    );
    output.insert(
        "simulated_started_at".into(),
        serde_json::to_value(report.simulated_started_at)?,
    );
    output.insert(
        "simulated_ended_at".into(),
        serde_json::to_value(report.simulated_ended_at)?,
    );
    output.insert(
        "simulated_duration_seconds".into(),
        serde_json::to_value(report.simulated_duration_seconds)?,
    );
    output.insert(
        "facts_sdk_revision".into(),
        serde_json::to_value(&report.facts_sdk_revision)?,
    );
    output.insert(
        "facts_cli_revision".into(),
        serde_json::to_value(&report.facts_cli_revision)?,
    );
    output.insert(
        "simulator_revision".into(),
        serde_json::to_value(&report.simulator_revision)?,
    );
    output.insert(
        "generator_version".into(),
        serde_json::to_value(&report.generator_version)?,
    );
    output.insert(
        "generator_source_commit".into(),
        serde_json::to_value(&report.generator_source_commit)?,
    );
    output.insert(
        "rust_toolchain_version".into(),
        serde_json::to_value(&report.rust_toolchain_version)?,
    );
    output.insert(
        "target_objects".into(),
        serde_json::to_value(report.target_objects)?,
    );
    output.insert(
        "object_count".into(),
        serde_json::to_value(report.object_count)?,
    );
    output.insert(
        "object_counts_by_type".into(),
        serde_json::to_value(&report.object_counts_by_type)?,
    );
    output.insert(
        "counts_by_status".into(),
        serde_json::to_value(&report.counts_by_status)?,
    );
    output.insert(
        "counts_by_conflict_type".into(),
        serde_json::to_value(&report.counts_by_conflict_type)?,
    );
    output.insert(
        "actor_count".into(),
        serde_json::to_value(report.actor_count)?,
    );
    output.insert(
        "ledger_count".into(),
        serde_json::to_value(report.ledger_count)?,
    );
    output.insert(
        "replica_count".into(),
        serde_json::to_value(report.replica_count)?,
    );
    output.insert(
        "generated_instances".into(),
        serde_json::to_value(report.generated_instances)?,
    );
    output.insert(
        "scenario_family_counts".into(),
        serde_json::to_value(&report.scenario_family_counts)?,
    );
    output.insert(
        "scenario_family_object_counts".into(),
        serde_json::to_value(&report.scenario_family_object_counts)?,
    );
    output.insert(
        "target_object_overshoot".into(),
        serde_json::to_value(report.target_object_overshoot)?,
    );
    output.insert("bundle".into(), serde_json::to_value(&report.bundle)?);
    output.insert(
        "bundle_paths".into(),
        serde_json::to_value(&report.bundle_paths)?,
    );
    output.insert(
        "snapshot_paths".into(),
        serde_json::to_value(&report.snapshot_paths)?,
    );
    output.insert("databases".into(), serde_json::to_value(&report.databases)?);
    output.insert(
        "commitment_root".into(),
        serde_json::to_value(&report.commitment_root)?,
    );
    output.insert(
        "final_commitment_roots".into(),
        serde_json::to_value(&report.final_commitment_roots)?,
    );
    output.insert(
        "packaged_reports".into(),
        serde_json::json!({
            "manifest": fixture.join("manifest.json"),
            "profile": fixture.join("profile.yaml"),
            "world_plan": fixture.join("world-plan.json"),
            "scenario": fixture.join("scenario-report.json"),
            "object_distribution": fixture.join("object-distribution.json"),
            "invariant": fixture.join("invariant-report.json"),
            "projection": fixture.join("projection-report.json"),
            "search_corpus": fixture.join("search-corpus-report.json"),
            "timing": fixture.join("timing-report.json"),
            "completed_checkpoint": fixture.join("checkpoints/completed.json"),
            "progress_log": fixture.join("logs/progress.jsonl"),
        }),
    );
    output.insert(
        "synchronization_report".into(),
        serde_json::to_value(&report.synchronization_report)?,
    );
    output.insert(
        "retry_report".into(),
        serde_json::to_value(&report.retry_report)?,
    );
    output.insert(
        "repair_report".into(),
        serde_json::to_value(&report.repair_report)?,
    );
    output.insert(
        "reconciliation_counts_by_mode".into(),
        serde_json::to_value(&report.reconciliation_counts_by_mode)?,
    );
    output.insert(
        "coordinator_disposition_counts".into(),
        serde_json::to_value(&report.coordinator_disposition_counts)?,
    );
    output.insert(
        "conflict_report".into(),
        serde_json::to_value(&report.conflict_report)?,
    );
    output.insert(
        "assertion_report".into(),
        serde_json::to_value(&report.assertion_report)?,
    );
    output.insert(
        "deep_validation_sample".into(),
        serde_json::to_value(&report.deep_validation_sample)?,
    );
    output.insert("cli_sample_report".into(), cli_sample_report);
    output.insert(
        "cli_ux_coverage".into(),
        serde_json::to_value(&report.cli_ux_coverage)?,
    );
    output.insert("http_sample_report".into(), http_sample_report);
    output.insert("full".into(), serde_json::to_value(full)?);
    output.insert(
        "unresolved_protocol_behavior".into(),
        serde_json::to_value(&report.unresolved_protocol_behavior)?,
    );
    output.insert(
        "performance_report".into(),
        serde_json::to_value(&report.performance_report)?,
    );
    output.insert(
        "verification_result".into(),
        serde_json::to_value(report.verification_result)?,
    );
    output.insert(
        "logical_replay_digest".into(),
        serde_json::to_value(&report.logical_replay_digest)?,
    );
    Ok(serde_json::Value::Object(output))
}

fn read_scenario(path: &PathBuf) -> Result<fact_sim_dsl::Scenario> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read scenario `{}`", path.display()))?;
    fact_sim_dsl::Scenario::from_yaml_str(&contents)
}

fn run_suite(path: PathBuf) -> Result<()> {
    let mut entries = Vec::new();
    collect_scenarios(&path, &mut entries)?;
    entries.sort();
    let mut scenario_count = 0;
    for path in entries {
        let scenario = read_scenario(&path)?;
        fact_sim_runner::run_scenario(&scenario)
            .with_context(|| format!("scenario `{}` failed", path.display()))?;
        scenario_count += 1;
    }
    println!("suite passed: {} scenarios", scenario_count);
    Ok(())
}

fn collect_scenarios(path: &PathBuf, scenarios: &mut Vec<PathBuf>) -> Result<()> {
    if path.is_file() {
        if path.extension().and_then(|ext| ext.to_str()) == Some("yaml") {
            scenarios.push(path.clone());
        }
        return Ok(());
    }
    let mut entries = std::fs::read_dir(path)
        .with_context(|| format!("failed to read suite `{}`", path.display()))?
        .collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        collect_scenarios(&entry.path(), scenarios)?;
    }
    Ok(())
}

async fn run_fact_cli(args: CliAdapterArgs) -> Result<()> {
    let started = Instant::now();
    let mut child = Command::new(&args.fact_binary)
        .args(&args.args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("failed to spawn `{}`", args.fact_binary.display()))?;

    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    if let Some(mut pipe) = child.stdout.take() {
        pipe.read_to_end(&mut stdout).await?;
    }
    if let Some(mut pipe) = child.stderr.take() {
        pipe.read_to_end(&mut stderr).await?;
    }
    let status = child.wait().await?;
    let parsed_json = serde_json::from_slice::<serde_json::Value>(&stdout).ok();
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "status": status.code(),
            "success": status.success(),
            "stdout": String::from_utf8_lossy(&stdout),
            "stderr": String::from_utf8_lossy(&stderr),
            "parsed_json": parsed_json,
            "duration_ms": started.elapsed().as_millis(),
            "args": args.args,
            "fact_binary": args.fact_binary,
        }))?
    );
    if !status.success() {
        bail!("fact CLI exited with {status}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;
    use std::collections::BTreeMap;

    fn synthetic_scale_report() -> fact_sim_runner::conflict_repair::ConflictRepairReport {
        fact_sim_runner::conflict_repair::ConflictRepairReport {
            profile: "scale-500k-balanced".into(),
            profile_version: 1,
            seed: 42,
            scheduler_version: "deterministic-weighted-family-runner-v1".into(),
            scenario_corpus_version: "scale-scenario-corpus-v1".into(),
            content_template_version: "scale-content-templates-v1".into(),
            time_distribution_profile: "multi-year-bursty-v1".into(),
            started_at: time::OffsetDateTime::parse(
                "2026-01-01T00:00:00Z",
                &time::format_description::well_known::Rfc3339,
            )
            .unwrap(),
            simulated_started_at: time::OffsetDateTime::parse(
                "2020-01-01T00:00:00Z",
                &time::format_description::well_known::Rfc3339,
            )
            .unwrap(),
            simulated_ended_at: time::OffsetDateTime::parse(
                "2025-01-01T00:00:00Z",
                &time::format_description::well_known::Rfc3339,
            )
            .unwrap(),
            simulated_duration_seconds: 157_852_800,
            facts_sdk_revision: "sdk-test".into(),
            facts_cli_revision: "cli-test".into(),
            simulator_revision: "sim-test".into(),
            generator_version: "generator-test".into(),
            generator_source_commit: "commit-test".into(),
            rust_toolchain_version: "rust-test".into(),
            output: "/tmp/fixture".into(),
            databases: BTreeMap::from([(
                "operations_a".into(),
                "/tmp/fixture/ledgers/operations-a.sqlite".into(),
            )]),
            bundle: "/tmp/fixture/bundles/all.factbndl".into(),
            bundle_paths: vec!["/tmp/fixture/bundles/all.factbndl".into()],
            snapshot_paths: vec!["/tmp/fixture/snapshots/object-set.json".into()],
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
            synchronization_report: fact_sim_runner::sync_scale::SynchronizationReport {
                operation_count: 1,
                full_sync_count: 1,
                partial_sync_count: 0,
                duplicate_delivery_idempotent: true,
                missing_dependency_deferred: true,
                delayed_dependency_retry_succeeded: true,
                push_pull_equivalent: true,
                transfer_order_independent: true,
                transfers: Vec::new(),
            },
            retry_report: Vec::new(),
            repair_report: fact_sim_runner::sync_scale::RepairReport {
                projection_repairs: 1,
                partial_sync_repairs: 1,
                semantic_corrections: 1,
                repaired_replicas_converged: true,
                canonical_history_preserved: true,
                repairs: Vec::new(),
            },
            reconciliation_counts_by_mode: BTreeMap::from([("select".into(), 1)]),
            coordinator_disposition_counts: BTreeMap::from([("accepted".into(), 1)]),
            conflict_report: fact_sim_runner::sync_scale::ConflictReport {
                sibling_revision_conflicts: 7,
                incompatible_deliberation_conflicts: 1,
                compatible_deliberations_without_conflict: 1,
                last_undisputed_ancestor_preserved_as_effective: true,
                arrival_order_selected_winner: false,
                sample_conflict_proposition_id: None,
                conflict_replicas: vec!["operations_a".into(), "operations_b".into()],
            },
            assertion_report: fact_sim_runner::sync_scale::AssertionReport {
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
            performance_report: fact_sim_runner::sync_scale::PerformanceReport {
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
            cli_sample_report: vec![fact_sim_runner::CliReceipt {
                command: vec!["--json".into(), "pending".into()],
                status: Some(0),
                stdout: r#"[{"has_pending_revision":true}]"#.into(),
                stderr: String::new(),
                parsed_json: Some(serde_json::json!([{"has_pending_revision": true}])),
                duration_ms: 2,
            }],
            cli_ux_coverage: vec!["json-pending-actionable-state".into()],
            http_sample_report: vec![fact_sim_runner::sync_scale::HttpReceipt {
                method: "GET".into(),
                path: "/facts/ledgers".into(),
                status: 200,
                body_bytes: 2,
                parsed_json: Some(serde_json::json!([])),
                duration_ms: 3,
            }],
            unresolved_protocol_behavior: Vec::new(),
            verification_result: true,
            logical_replay_digest: "digest-test".into(),
        }
    }

    #[test]
    fn scale_report_json_surfaces_scale_manifest_fields() {
        let fixture = std::path::Path::new("/tmp/fixture");
        let report = synthetic_scale_report();
        let output = conflict_repair_report_json(fixture, "reported", &report, false).unwrap();

        assert_eq!(output["profile"], "scale-500k-balanced");
        assert_eq!(output["target_objects"], 500_000);
        assert_eq!(output["object_count"], 1_000_143);
        assert_eq!(output["target_object_overshoot"], 143);
        assert_eq!(output["profile_version"], 1);
        assert_eq!(
            output["scenario_corpus_version"],
            "scale-scenario-corpus-v1"
        );
        assert_eq!(
            output["content_template_version"],
            "scale-content-templates-v1"
        );
        assert_eq!(output["time_distribution_profile"], "multi-year-bursty-v1");
        assert_eq!(output["simulated_duration_seconds"], 157_852_800);
        assert_eq!(output["counts_by_status"]["pending"], 1000);
        assert_eq!(output["counts_by_conflict_type"]["sibling_revision"], 7);
        assert_eq!(output["scenario_family_counts"]["stable-fact"], 100);
        assert_eq!(output["scenario_family_object_counts"]["stable-fact"], 2000);
        assert_eq!(output["commitment_root"], "root-1");
        assert_eq!(
            output["packaged_reports"]["object_distribution"],
            "/tmp/fixture/object-distribution.json"
        );
        assert_eq!(
            output["deep_validation_sample"]["coverage"]["pending_propositions"],
            true
        );
        assert_eq!(output["cli_sample_report"][0]["stdout_bytes"], 31);
        assert_eq!(output["cli_sample_report"][0]["parsed_json"], true);
        assert!(output["cli_sample_report"][0]["stdout"].is_null());
        assert_eq!(output["http_sample_report"][0]["body_bytes"], 2);
        assert_eq!(output["verification_result"], true);
        assert_eq!(output["logical_replay_digest"], "digest-test");

        let resumed =
            conflict_repair_report_json(fixture, "resumed-complete-smoke", &report, false).unwrap();
        assert_eq!(resumed["status"], "resumed-complete-smoke");
        assert_eq!(
            resumed["packaged_reports"]["manifest"],
            "/tmp/fixture/manifest.json"
        );
        assert_eq!(resumed["target_objects"], 500_000);
    }

    #[test]
    fn scale_report_supports_reduced_target_smoke_fixtures() -> Result<()> {
        let fixture = tempfile::tempdir()?;
        std::fs::create_dir(fixture.path().join("checkpoints"))?;
        let mut report = synthetic_scale_report();
        report.target_objects = 100;
        report.object_count = 2_274;
        report.target_object_overshoot = 2_174;
        report.generated_instances = 8;
        report.performance_report.database_bytes = 123_456;
        report.output = fixture.path().to_path_buf();
        report.databases = BTreeMap::from([(
            "operations_a".into(),
            fixture.path().join("ledgers").join("operations-a.sqlite"),
        )]);
        report.bundle = fixture.path().join("bundles").join("objects.factbndl");
        report.bundle_paths = vec![report.bundle.clone()];
        report.snapshot_paths = vec![fixture.path().join("snapshots").join("object-set.json")];
        write_synthetic_scale_manifest(fixture.path(), &report)?;
        std::fs::create_dir_all(fixture.path().join("ledgers"))?;
        std::fs::write(
            fixture.path().join("ledgers").join("operations-a.sqlite"),
            b"database",
        )?;
        std::fs::write(
            fixture.path().join("profile.yaml"),
            fact_sim_runner::scale::profile_yaml_for_target(
                &report.profile,
                report.seed,
                report.target_objects,
            )?,
        )?;
        std::fs::write(
            fixture.path().join("checkpoints/completed.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "safe_boundary": true,
                "completed": true,
                "phase": "completed",
                "fixture": fixture.path(),
                "profile": report.profile,
                "seed": report.seed,
                "logical_replay_digest": report.logical_replay_digest,
                "target_objects": report.target_objects,
                "current_scenario_instance": report.generated_instances,
                "current_object_count": report.object_count,
                "random_generator_state": {
                    "kind": "seed-and-scenario-index-replay",
                    "seed": report.seed,
                    "next_scenario_instance": report.generated_instances,
                },
                "world_plan_position": {
                    "next_scenario_instance": report.generated_instances,
                    "executed_scenario_instances": report.generated_instances,
                },
                "ledger_paths": {},
                "replica_paths": {},
                "scenario_family_counts": report.scenario_family_counts,
                "partial_report_state": {
                    "scenario_family_counts": report.scenario_family_counts,
                    "scenario_family_object_counts": report.scenario_family_object_counts,
                    "object_counts_by_type": report.object_counts_by_type,
                    "counts_by_status": report.counts_by_status,
                    "counts_by_conflict_type": report.counts_by_conflict_type,
                    "scenario_failure_count": 0,
                },
                "progress": {
                    "elapsed_seconds": 1.0,
                    "objects_per_second": 1.0,
                    "progress_percent": 100.0,
                    "database_bytes": report.performance_report.database_bytes,
                    "scenario_failure_count": 0,
                }
            }))?,
        )?;
        for artifact in scale_required_sidecars() {
            let path = fixture.path().join(artifact);
            if path.exists() {
                continue;
            }
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            if *artifact == "bundles/objects.factbndl" {
                std::fs::write(&path, b"bundle")?;
                continue;
            }
            if *artifact == "logs/progress.jsonl" {
                let events = [
                    serde_json::json!({
                        "phase": "initialized",
                        "profile": report.profile,
                        "seed": report.seed,
                        "target_objects": report.target_objects,
                        "current_scenario_instance": 0,
                        "current_object_count": 0,
                        "completed": false,
                    }),
                    serde_json::json!({
                        "phase": "started",
                        "profile": report.profile,
                        "seed": report.seed,
                        "target_objects": report.target_objects,
                        "current_scenario_instance": 0,
                        "current_object_count": 0,
                        "completed": false,
                    }),
                    serde_json::json!({
                        "phase": "completed",
                        "profile": report.profile,
                        "seed": report.seed,
                        "target_objects": report.target_objects,
                        "current_scenario_instance": report.generated_instances,
                        "current_object_count": report.object_count,
                        "completed": true,
                    }),
                ];
                let mut content = String::new();
                for event in events {
                    content.push_str(&serde_json::to_string(&event)?);
                    content.push('\n');
                }
                std::fs::write(&path, content)?;
                continue;
            }
            let value = match *artifact {
                "world-plan.json" => serde_json::json!({
                    "profile": report.profile,
                    "seed": report.seed,
                    "logical_replay_digest": report.logical_replay_digest,
                    "target_objects": report.target_objects,
                    "expected_object_budget": {
                        "target_objects": report.target_objects,
                    },
                    "scenario_family_counts": report.scenario_family_counts,
                    "scenario_family_object_counts": report.scenario_family_object_counts,
                }),
                "scenario-report.json" => serde_json::json!({
                    "generated_instances": report.generated_instances,
                    "target_objects": report.target_objects,
                    "scenario_family_counts": report.scenario_family_counts,
                    "scenario_family_object_counts": report.scenario_family_object_counts,
                    "target_object_overshoot": report.target_object_overshoot,
                }),
                "object-distribution.json" => serde_json::json!({
                    "object_count": report.object_count,
                    "object_counts_by_type": report.object_counts_by_type,
                    "counts_by_status": report.counts_by_status,
                    "deep_validation_sample": report.deep_validation_sample,
                }),
                "invariant-report.json" => serde_json::json!({
                    "assertions": report.assertion_report,
                    "failure_context_contract": {
                        "complete": true,
                    },
                }),
                "projection-report.json" => serde_json::json!({
                    "projection_rebuild_equivalent": report.assertion_report.projection_rebuild_equivalent,
                    "converged_projections": report.assertion_report.converged_projections,
                    "canonical_history_preserved": report.repair_report.canonical_history_preserved,
                }),
                "search-corpus-report.json" => serde_json::json!({
                    "indexed": true,
                    "cli_ux_coverage": report.cli_ux_coverage,
                }),
                "timing-report.json" => serde_json::json!({
                    "performance": report.performance_report,
                    "simulated_duration_seconds": report.simulated_duration_seconds,
                }),
                "bundles/inventory.json" => serde_json::json!({
                    "bundle_count": 1,
                }),
                "commitments/object-set.json" => serde_json::json!({
                    "root": report.commitment_root,
                    "object_count": report.object_count,
                    "verified": true,
                }),
                "snapshots/object-set.json" => serde_json::json!({
                    "commitment_root": report.commitment_root,
                    "unique_object_count": report.object_count,
                }),
                _ => serde_json::json!({}),
            };
            std::fs::write(&path, serde_json::to_vec_pretty(&value)?)?;
        }

        let output = scale_report_json_allowing_reduced_target(fixture.path(), "reported", false)?;
        assert_eq!(output["status"], "reported-smoke");
        assert_eq!(output["target_objects"], 100);
        assert_eq!(output["object_count"], 2_274);
        assert!(
            output["full_verification_note"]
                .as_str()
                .unwrap()
                .contains("500,000-proposition")
        );
        let manifest_path = fixture.path().join("manifest.json");
        assert_eq!(
            output["packaged_reports"]["manifest"].as_str(),
            Some(manifest_path.to_string_lossy().as_ref())
        );
        std::fs::remove_file(fixture.path().join("world-plan.json"))?;
        let error = scale_report_json_allowing_reduced_target(fixture.path(), "reported", false)
            .unwrap_err();
        assert!(error.to_string().contains("missing required sidecar"));
        std::fs::write(fixture.path().join("world-plan.json"), "{}")?;
        let error = scale_report_json_allowing_reduced_target(fixture.path(), "reported", false)
            .unwrap_err();
        assert!(error.to_string().contains("world plan does not match"));
        std::fs::write(
            fixture.path().join("world-plan.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "profile": report.profile,
                "seed": report.seed,
                "logical_replay_digest": report.logical_replay_digest,
                "target_objects": report.target_objects,
                "expected_object_budget": {
                    "target_objects": report.target_objects,
                },
                "scenario_family_counts": report.scenario_family_counts,
                "scenario_family_object_counts": report.scenario_family_object_counts,
            }))?,
        )?;
        let mut moved_report = report.clone();
        moved_report.databases = BTreeMap::from([(
            "operations_a".into(),
            PathBuf::from("/old/location/ledgers/operations-a.sqlite"),
        )]);
        moved_report.bundle = PathBuf::from("/old/location/bundles/objects.factbndl");
        moved_report.bundle_paths = vec![moved_report.bundle.clone()];
        moved_report.snapshot_paths =
            vec![PathBuf::from("/old/location/snapshots/object-set.json")];
        write_synthetic_scale_manifest(fixture.path(), &moved_report)?;
        let output = scale_report_json_allowing_reduced_target(fixture.path(), "reported", false)?;
        assert_eq!(
            output["databases"]["operations_a"],
            fixture
                .path()
                .join("ledgers")
                .join("operations-a.sqlite")
                .to_string_lossy()
                .as_ref()
        );
        assert_eq!(
            output["bundle"],
            fixture
                .path()
                .join("bundles")
                .join("objects.factbndl")
                .to_string_lossy()
                .as_ref()
        );

        let mut root_database_report = report.clone();
        root_database_report.databases = BTreeMap::from([(
            "operations_a".into(),
            fixture.path().join("operations-a.sqlite"),
        )]);
        std::fs::write(fixture.path().join("operations-a.sqlite"), b"database")?;
        write_synthetic_scale_manifest(fixture.path(), &root_database_report)?;
        let error = scale_report_json_allowing_reduced_target(fixture.path(), "reported", false)
            .unwrap_err();
        assert!(error.to_string().contains("is not under"));

        let mut root_bundle_report = report.clone();
        root_bundle_report.bundle = fixture.path().join("objects.factbndl");
        root_bundle_report.bundle_paths = vec![root_bundle_report.bundle.clone()];
        std::fs::write(fixture.path().join("objects.factbndl"), b"bundle")?;
        write_synthetic_scale_manifest(fixture.path(), &root_bundle_report)?;
        let error = scale_report_json_allowing_reduced_target(fixture.path(), "reported", false)
            .unwrap_err();
        assert!(error.to_string().contains("primary object bundle"));

        write_synthetic_scale_manifest(fixture.path(), &report)?;
        std::fs::write(
            fixture.path().join("profile.yaml"),
            fact_sim_runner::scale::profile_yaml_for_target(&report.profile, report.seed, 101)?,
        )?;
        let error = scale_report_json_allowing_reduced_target(fixture.path(), "reported", false)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("packaged profile config does not match")
        );
        Ok(())
    }

    fn write_synthetic_scale_manifest(
        fixture: &std::path::Path,
        report: &fact_sim_runner::conflict_repair::ConflictRepairReport,
    ) -> Result<()> {
        std::fs::write(
            fixture.join("manifest.json"),
            serde_json::to_vec_pretty(report)?,
        )?;
        Ok(())
    }

    #[test]
    fn fixture_database_bytes_resolves_packaged_database_paths_after_move() -> Result<()> {
        let fixture = tempfile::tempdir()?;
        std::fs::create_dir(fixture.path().join("ledgers"))?;
        std::fs::write(
            fixture.path().join("ledgers/operations-a.sqlite"),
            [1_u8; 7],
        )?;
        std::fs::write(
            fixture.path().join("ledgers/engineering.sqlite"),
            [2_u8; 11],
        )?;
        std::fs::write(
            fixture.path().join("manifest.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "databases": {
                    "operations_a": "/stale/original/fixture/ledgers/operations-a.sqlite",
                    "engineering": "/stale/original/fixture/ledgers/engineering.sqlite"
                }
            }))?,
        )?;

        assert_eq!(fixture_database_bytes(fixture.path())?, 18);
        Ok(())
    }

    #[test]
    fn cleanup_helpers_report_candidates_without_touching_targets() -> Result<()> {
        let normal = cleanup_candidates(false);
        assert!(normal.contains(&PathBuf::from("target/fact-sim-runs")));
        assert!(normal.contains(&PathBuf::from("crates/fact-sim-cli/target/fact-sim-runs")));
        assert!(normal.contains(&PathBuf::from(
            "crates/fact-sim-runner/target/fact-sim-runs"
        )));
        assert!(!normal.contains(&PathBuf::from("target")));
        assert!(!normal.contains(&PathBuf::from("crates/fact-sim-cli/target")));
        assert!(!normal.contains(&PathBuf::from("crates/fact-sim-runner/target")));

        let with_target = cleanup_candidates(true);
        assert_eq!(with_target, {
            let mut expected = vec![
                PathBuf::from("crates/fact-sim-cli/target"),
                PathBuf::from("crates/fact-sim-runner/target"),
                PathBuf::from("target"),
            ];
            expected.sort();
            expected
        });
        assert!(with_target.contains(&PathBuf::from("target")));
        assert!(with_target.contains(&PathBuf::from("crates/fact-sim-cli/target")));
        assert!(with_target.contains(&PathBuf::from("crates/fact-sim-runner/target")));

        let temp = tempfile::tempdir()?;
        std::fs::create_dir(temp.path().join("nested"))?;
        std::fs::write(temp.path().join("nested").join("artifact.bin"), b"12345")?;
        assert_eq!(directory_bytes(temp.path())?, 5);
        Ok(())
    }

    #[test]
    fn cleanup_fixture_candidates_require_manifest_profile() -> Result<()> {
        let temp = tempfile::tempdir()?;
        write_fixture_manifest(
            temp.path(),
            "scale-500k-proposition-bulk-seed-42",
            "scale-500k-proposition-bulk",
            42,
        )?;
        std::fs::create_dir(temp.path().join("benchmark-matrix"))?;
        write_fixture_manifest(
            &temp.path().join("benchmark-matrix"),
            "scale-500k-balanced-small-seed-42",
            "scale-500k-balanced",
            42,
        )?;
        write_progress_mirror(
            &temp.path().join("benchmark-matrix"),
            "scale-500k-balanced-medium-seed-42.progress",
            "scale-500k-balanced",
            42,
        )?;
        write_fixture_manifest(temp.path(), "multi-actor-10k", "multi-actor-10k", 42)?;
        std::fs::create_dir(temp.path().join("not-a-fixture"))?;

        let profile_matches =
            cleanup_fixture_candidates_from(temp.path(), Some("scale-500k-balanced"), false)?;
        assert_eq!(profile_matches.len(), 2);
        assert!(profile_matches.iter().all(|candidate| {
            candidate.profile == "scale-500k-balanced" && candidate.seed == Some(42)
        }));
        let profile_match_kinds = profile_matches
            .iter()
            .map(|candidate| candidate.kind.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            profile_match_kinds,
            std::collections::BTreeSet::from(["fixture", "progress"])
        );

        let all_matches = cleanup_fixture_candidates_from(temp.path(), None, true)?;
        assert_eq!(all_matches.len(), 4);
        assert_eq!(all_matches[0].profile, "scale-500k-balanced");
        assert_eq!(all_matches[1].profile, "scale-500k-balanced");
        assert_eq!(all_matches[2].profile, "multi-actor-10k");
        assert_eq!(all_matches[3].profile, "scale-500k-proposition-bulk");
        assert!(cleanup_fixture_candidates_from(temp.path(), Some("x"), true).is_err());
        Ok(())
    }

    #[test]
    fn cleanup_rejects_fixture_profile_without_fixture_mode() {
        assert!(
            cleanup_generated_artifacts(true, false, false, Some("profile".into()), false).is_err()
        );
        assert!(cleanup_generated_artifacts(true, false, true, None, false).is_err());
    }

    fn write_fixture_manifest(
        base: &std::path::Path,
        name: &str,
        profile: &str,
        seed: u64,
    ) -> Result<()> {
        let fixture = base.join(name);
        std::fs::create_dir(&fixture)?;
        std::fs::write(
            fixture.join("manifest.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "profile": profile,
                "seed": seed
            }))?,
        )?;
        Ok(())
    }

    fn write_progress_mirror(
        base: &std::path::Path,
        name: &str,
        profile: &str,
        seed: u64,
    ) -> Result<()> {
        let mirror = base.join(name);
        std::fs::create_dir(&mirror)?;
        std::fs::create_dir(mirror.join("logs"))?;
        std::fs::create_dir(mirror.join("checkpoints"))?;
        let event = serde_json::json!({
            "profile": profile,
            "seed": seed,
            "phase": "completed",
            "safe_boundary": true,
            "completed": false,
            "progress": {
                "progress_percent": 50.0
            }
        });
        std::fs::write(
            mirror.join("logs/progress.jsonl"),
            format!("{}\n", serde_json::to_string(&event)?),
        )?;
        std::fs::write(
            mirror.join("checkpoints/latest.json"),
            serde_json::to_vec_pretty(&event)?,
        )?;
        Ok(())
    }

    #[test]
    fn world_plan_comparison_reports_budget_and_preflight_differences() {
        let left = serde_json::json!({
            "logical_replay_digest": "digest-a",
            "planner": {"kind": "deterministic-weighted-family-runner"},
            "configured_world": {"actors": 500},
            "configured_distribution": {"stable_fact_journeys": 45},
            "configured_safeguards": {"max_database_bytes": 1000},
            "realized_topology": {
                "actors": 500,
                "shared_ledgers": 12,
                "ledgers": 512,
                "replicas": 24
            },
            "expected_object_budget": {
                "target_objects": 100,
                "estimated_instances": 10,
                "estimated_topology_objects": 50,
                "estimated_objects": 120,
                "estimated_storage_bytes": 1200
            },
            "storage_preflight": {
                "estimated_storage_bytes": 1200,
                "available_bytes": 2000,
                "sufficient": true
            }
        });
        let right = serde_json::json!({
            "logical_replay_digest": "digest-b",
            "planner": {"kind": "deterministic-weighted-family-runner"},
            "configured_world": {"actors": 500},
            "configured_distribution": {"stable_fact_journeys": 50},
            "configured_safeguards": {"max_database_bytes": 1500},
            "realized_topology": {
                "actors": 505,
                "shared_ledgers": 12,
                "ledgers": 517,
                "replicas": 30
            },
            "expected_object_budget": {
                "target_objects": 200,
                "estimated_instances": 20,
                "estimated_topology_objects": 65,
                "estimated_objects": 240,
                "estimated_storage_bytes": 2400
            },
            "storage_preflight": {
                "estimated_storage_bytes": 2400,
                "available_bytes": 3000,
                "sufficient": true
            }
        });
        let diff = compare_optional_world_plans(&Some(left), &Some(right));
        assert_eq!(diff["available"], true);
        assert_eq!(diff["logical_replay_digest_changed"], true);
        assert_eq!(
            diff["configured_distribution_difference"]["stable_fact_journeys"],
            5
        );
        assert_eq!(
            diff["configured_safeguards_difference"]["max_database_bytes"],
            500
        );
        assert_eq!(diff["budget_difference"]["target_objects"], 100);
        assert_eq!(diff["budget_difference"]["estimated_instances"], 10);
        assert_eq!(diff["budget_difference"]["estimated_topology_objects"], 15);
        assert_eq!(diff["realized_topology_difference"]["actors"], 5);
        assert_eq!(diff["realized_topology_difference"]["shared_ledgers"], 0);
        assert_eq!(diff["realized_topology_difference"]["ledgers"], 5);
        assert_eq!(diff["realized_topology_difference"]["replicas"], 6);
        assert_eq!(
            diff["storage_preflight_difference"]["estimated_storage_bytes"],
            1200
        );
        assert_eq!(
            diff["storage_preflight_difference"]["available_bytes"],
            1000
        );
        assert_eq!(
            diff["storage_preflight_difference"]["sufficient_changed"],
            false
        );
    }

    #[test]
    fn scale_inspection_includes_required_sidecar_artifacts() {
        let artifacts = inspect_artifacts(
            std::path::Path::new("/tmp/fixture"),
            Some("scale-500k-balanced"),
        );
        let names = artifacts
            .iter()
            .filter_map(|artifact| artifact["name"].as_str())
            .collect::<std::collections::BTreeSet<_>>();
        for required in [
            "manifest.json",
            "bundles/objects.factbndl",
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
            "bundles/inventory.json",
            "commitments/object-set.json",
            "snapshots/object-set.json",
        ] {
            assert!(
                names.contains(required),
                "{required} missing from inspect inventory"
            );
        }
    }

    #[test]
    fn bulk_scale_inspection_uses_minimal_artifact_contract() {
        let artifacts = inspect_artifacts(
            std::path::Path::new("/tmp/fixture"),
            Some("scale-500k-proposition-bulk"),
        );
        let names = artifacts
            .iter()
            .filter_map(|artifact| artifact["name"].as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(names.contains("manifest.json"));
        assert!(names.contains("profile.yaml"));
        assert!(names.contains("timing-report.json"));
        assert!(names.contains("bundles/objects.factbndl"));
        assert!(!names.contains("world-plan.json"));
        assert!(!names.contains("checkpoints/completed.json"));
    }

    #[test]
    fn scale_inspection_surfaces_logical_replay_digest() {
        let fixture = std::path::Path::new("/tmp/fixture");
        let manifest = serde_json::json!({
            "profile": "scale-500k-balanced",
            "seed": 42,
            "target_objects": 500_000,
            "object_count": 1_000_143,
            "object_counts_by_type": {"revision": 500_000},
            "scenario_family_counts": {"stable-fact": 45},
            "target_object_overshoot": 143,
            "commitment_root": "root-1",
            "logical_replay_digest": "digest-1",
            "databases": {"operations_a": "/tmp/fixture/ledgers/operations_a.sqlite"},
        });
        let world_plan = serde_json::json!({
            "logical_replay_digest": "digest-1",
            "planner": {"kind": "deterministic-weighted-family-runner"},
            "configured_world": {"actors": 500},
            "configured_distribution": {"stable_fact_journeys": 45},
            "expected_object_budget": {"target_objects": 500_000},
            "storage_preflight": {"sufficient": true},
        });
        let checkpoint = serde_json::json!({
            "completed": true,
            "safe_boundary": true,
            "logical_replay_digest": "digest-1",
            "current_scenario_instance": 10,
            "current_object_count": 1_000_143,
            "simulated_time": "2025-01-01T00:00:00Z",
            "progress": {"progress_percent": 100.0},
        });

        let output = inspect_fixture_json(
            fixture,
            &manifest,
            vec![serde_json::json!({"name": "manifest.json"})],
            Some(&world_plan),
            Some(&checkpoint),
        );
        assert_eq!(output["logical_replay_digest"], "digest-1");
        assert_eq!(
            output["completed_checkpoint_summary"]["logical_replay_digest"],
            "digest-1"
        );
        assert_eq!(
            output["world_plan_summary"]["logical_replay_digest"],
            "digest-1"
        );
        assert_eq!(
            output["world_plan_summary"]["planner"]["kind"],
            "deterministic-weighted-family-runner"
        );
        assert_eq!(output["completed_checkpoint_summary"]["completed"], true);
    }

    #[test]
    fn operations_registry_exposes_contract() -> Result<()> {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let registry_path = root.join("docs/operations/registry.json");
        let issues_path = root.join("docs/operations/open-issues.json");
        let registry: serde_json::Value = serde_json::from_slice(&std::fs::read(&registry_path)?)?;
        let issues: serde_json::Value = serde_json::from_slice(&std::fs::read(&issues_path)?)?;
        assert_eq!(registry["schema_version"], "facts-operations-registry-v0");
        assert_eq!(
            registry["large_scale_policy"]["large_500k"],
            "manual-opt-in-performance-configuration"
        );
        let operations = registry["operations"].as_array().unwrap();
        let names = operations
            .iter()
            .filter_map(|operation| operation["name"].as_str())
            .collect::<std::collections::BTreeSet<_>>();
        for required in [
            "CreateLedger",
            "CreateActor",
            "RotateKey",
            "RecoverActorKey",
            "RetireActor",
            "InitializeReplica",
            "DeleteLocalLedger",
            "GrantCapability",
            "DelegateAuthority",
            "Propose",
            "Revise",
            "CopyProposition",
            "OpenDeliberation",
            "ExtendDeliberation",
            "InviteParticipant",
            "JoinDeliberation",
            "LeaveDeliberation",
            "AddParticipant",
            "RemoveParticipant",
            "CastDecision",
            "ResolveDecisionConflict",
            "MaterializeSettlement",
            "ArchiveProposition",
            "WithdrawProposition",
            "ReconcileConflict",
            "ImportObjects",
            "Push",
            "Pull",
            "SynchronizeReplicas",
            "RebuildProjections",
            "QueryEffectiveState",
            "SearchEffectiveFacts",
        ] {
            assert!(names.contains(required), "missing operation {required}");
        }
        for operation in operations {
            for field in [
                "name",
                "version",
                "purpose",
                "inputs",
                "preconditions",
                "explicit_context",
                "authorization",
                "validation_steps",
                "creates",
                "projection_effects",
                "effective_state_effects",
                "retry",
                "partial_success",
                "failure_classes",
                "observable_result",
                "mappings",
                "conformance",
            ] {
                assert!(
                    operation.get(field).is_some(),
                    "operation {} missing {field}",
                    operation["name"]
                );
            }
        }
        assert_eq!(issues["schema_version"], "facts-operations-open-issues-v0");
        assert!(
            issues["issues"]
                .as_array()
                .unwrap()
                .iter()
                .any(|issue| issue["id"] == "OPS-007"),
            "read/write CLI gap must remain tracked"
        );
        Ok(())
    }

    #[test]
    fn search_corpus_comparison_reports_structured_differences() {
        let left = serde_json::json!({
            "indexed": true,
            "sampled_query_count": 10,
            "sampled_search_query_count": 2,
            "searchable_object_counts": {"revision": 8},
            "sampled_search_terms": ["base", "scale"],
            "effective_search_sampled": true,
            "status_filter_search_sampled": true,
            "bounded_page_size_sampled": true,
            "ambiguous_reference_sampled": true,
            "sampled_search_queries": [
                {
                    "text": "scale",
                    "status": null,
                    "effective": true,
                    "page_size": 2,
                    "result_count": 2,
                    "bounded_by_page_size": true
                }
            ],
            "cli_ux_coverage": ["search-effective-json"]
        });
        let right = serde_json::json!({
            "indexed": true,
            "sampled_query_count": 12,
            "sampled_search_query_count": 3,
            "searchable_object_counts": {"revision": 13},
            "sampled_search_terms": ["scale", "rare"],
            "effective_search_sampled": true,
            "status_filter_search_sampled": false,
            "bounded_page_size_sampled": true,
            "ambiguous_reference_sampled": true,
            "sampled_search_queries": [
                {
                    "text": "scale",
                    "status": null,
                    "effective": true,
                    "page_size": 2,
                    "result_count": 1,
                    "bounded_by_page_size": true
                }
            ],
            "cli_ux_coverage": ["search-effective-json", "search-page-size-bounded-json"]
        });
        let diff = compare_optional_reports(&Some(left), &Some(right));
        assert_eq!(diff["sampled_query_count_difference"], 2);
        assert_eq!(diff["sampled_search_query_count_difference"], 1);
        assert_eq!(diff["searchable_object_counts_difference"]["revision"], 5);
        assert_eq!(
            diff["sampled_search_terms_difference"]["only_right"],
            serde_json::json!(["rare"])
        );
        assert_eq!(
            diff["sampled_search_terms_difference"]["only_left"],
            serde_json::json!(["base"])
        );
        assert_eq!(
            diff["search_evidence_changed"]["status_filter_search_sampled"],
            true
        );
        assert_eq!(
            diff["sampled_search_query_difference"]["scale|status=*|effective=true|page_size=2"]["result_count_difference"],
            -1
        );
    }

    #[test]
    fn object_distribution_comparison_reports_deep_validation_differences() {
        let left = serde_json::json!({
            "revision_depth": {"count": 10, "average": 2.5, "maximum": 5},
            "deliberation_size": {"count": 3, "average": 4.0, "maximum": 8},
            "participant_count": {"count": 3, "average": 2.0, "maximum": 4},
            "object_counts_per_ledger": {"operations": 100},
            "object_counts_per_simulated_year": {"2026": 100},
            "pending_counts_by_kind": {"decision": 1},
            "reconciliation_counts_by_mode": {"select": 1},
            "deep_validation_sample": {
                "category_counts": {
                    "accepted_propositions": 10,
                    "pending_propositions": 1,
                    "withdrawn_propositions": 0,
                    "reconciliation_revisions": 1
                },
                "coverage": {
                    "accepted_propositions": true,
                    "pending_actions": false,
                    "pending_propositions": true,
                    "withdrawn_propositions": false
                }
            }
        });
        let right = serde_json::json!({
            "revision_depth": {"count": 15, "average": 3.0, "maximum": 9},
            "deliberation_size": {"count": 3, "average": 5.0, "maximum": 9},
            "participant_count": {"count": 5, "average": 3.0, "maximum": 6},
            "object_counts_per_ledger": {"operations": 130, "engineering": 7},
            "object_counts_per_simulated_year": {"2026": 90, "2027": 20},
            "pending_counts_by_kind": {"decision": 0, "review": 2},
            "reconciliation_counts_by_mode": {"select": 2},
            "deep_validation_sample": {
                "category_counts": {
                    "accepted_propositions": 12,
                    "pending_propositions": 4,
                    "withdrawn_propositions": 1,
                    "reconciliation_revisions": 3
                },
                "coverage": {
                    "accepted_propositions": true,
                    "pending_actions": true,
                    "pending_propositions": true,
                    "withdrawn_propositions": true
                }
            }
        });
        let diff = compare_optional_object_distribution_reports(&Some(left), &Some(right));
        assert_eq!(diff["available"], true);
        assert_eq!(diff["revision_depth_difference"]["count"], 5);
        assert_eq!(diff["revision_depth_difference"]["maximum"], 4);
        assert_eq!(
            diff["object_counts_per_ledger_difference"]["engineering"],
            7
        );
        assert_eq!(
            diff["deep_validation_category_count_difference"]["accepted_propositions"],
            2
        );
        assert_eq!(
            diff["deep_validation_category_count_difference"]["pending_propositions"],
            3
        );
        assert_eq!(
            diff["deep_validation_category_count_difference"]["withdrawn_propositions"],
            1
        );
        assert_eq!(
            diff["deep_validation_coverage_changed"]["pending_actions"],
            true
        );
        assert_eq!(
            diff["deep_validation_coverage_changed"]["withdrawn_propositions"],
            true
        );
    }

    #[test]
    fn checkpoint_comparison_reports_replay_and_partial_state_differences() {
        let left = serde_json::json!({
            "safe_boundary": true,
            "completed": true,
            "phase": "completed",
            "logical_replay_digest": "digest-a",
            "current_scenario_instance": 10,
            "current_object_count": 100,
            "random_generator_state": {
                "kind": "seed-and-scenario-index-replay",
                "seed": 42,
                "next_scenario_instance": 10
            },
            "world_plan_position": {
                "next_scenario_instance": 10,
                "executed_scenario_instances": 10
            },
            "progress": {
                "elapsed_seconds": 2.5,
                "objects_per_second": 40.0,
                "progress_percent": 50.0,
                "database_bytes": 1000,
                "conflict_count": 1,
                "scenario_failure_count": 0
            },
            "partial_report_state": {
                "scenario_family_counts": {"stable-fact": 5},
                "scenario_family_object_counts": {"stable-fact": 25},
                "object_counts_by_type": {"revision": 20},
                "counts_by_status": {"accepted": 4},
                "counts_by_conflict_type": {"sibling_revision": 1},
                "scenario_failure_count": 0
            }
        });
        let right = serde_json::json!({
            "safe_boundary": true,
            "completed": true,
            "phase": "completed",
            "logical_replay_digest": "digest-b",
            "current_scenario_instance": 12,
            "current_object_count": 130,
            "random_generator_state": {
                "kind": "seed-and-scenario-index-replay",
                "seed": 42,
                "next_scenario_instance": 12
            },
            "world_plan_position": {
                "next_scenario_instance": 12,
                "executed_scenario_instances": 12
            },
            "progress": {
                "elapsed_seconds": 3.0,
                "objects_per_second": 43.0,
                "progress_percent": 65.0,
                "database_bytes": 1600,
                "conflict_count": 2,
                "scenario_failure_count": 1
            },
            "partial_report_state": {
                "scenario_family_counts": {"stable-fact": 6, "conflict": 1},
                "scenario_family_object_counts": {"stable-fact": 30, "conflict": 16},
                "object_counts_by_type": {"revision": 25},
                "counts_by_status": {"accepted": 6},
                "counts_by_conflict_type": {"sibling_revision": 2},
                "scenario_failure_count": 1
            }
        });
        let diff = compare_optional_checkpoints(&Some(left), &Some(right));
        assert_eq!(diff["available"], true);
        assert_eq!(diff["current_scenario_instance_difference"], 2);
        assert_eq!(diff["current_object_count_difference"], 30);
        assert_eq!(diff["logical_replay_digest_changed"], true);
        assert_eq!(diff["random_generator_state_changed"], true);
        assert_eq!(
            diff["world_plan_position_difference"]["next_scenario_instance"],
            2
        );
        assert_eq!(diff["progress_difference"]["database_bytes"], 600);
        assert_eq!(diff["progress_difference"]["scenario_failure_count"], 1);
        assert_eq!(
            diff["partial_report_state_difference"]["scenario_family_counts"]["conflict"],
            1
        );
        assert_eq!(
            diff["partial_report_state_difference"]["scenario_family_object_counts"]["stable-fact"],
            5
        );
        assert_eq!(
            diff["partial_report_state_difference"]["scenario_family_object_counts"]["conflict"],
            16
        );
        assert_eq!(
            diff["partial_report_state_difference"]["counts_by_status"]["accepted"],
            2
        );
        assert_eq!(
            diff["partial_report_state_difference"]["scenario_failure_count"],
            1
        );
    }

    #[test]
    fn invariant_report_comparison_reports_contract_and_safeguard_differences() {
        let left = serde_json::json!({
            "assertions": {"status": "ok", "passed": 10},
            "failure_context_contract": {
                "fields": ["profile", "seed", "scenario"],
                "complete": true
            },
            "safeguard_observations": {
                "scenario_failure_count": 0,
                "retry_count": 2,
                "database_bytes": 1000,
                "generation_ms": 500,
                "peak_memory_bytes": 4096
            }
        });
        let right = serde_json::json!({
            "assertions": {"status": "ok", "passed": 11},
            "failure_context_contract": {
                "fields": ["profile", "seed", "scenario", "operation"],
                "complete": false
            },
            "safeguard_observations": {
                "scenario_failure_count": 1,
                "retry_count": 5,
                "database_bytes": 1600,
                "generation_ms": 650,
                "peak_memory_bytes": 8192
            }
        });
        let diff = compare_optional_invariant_reports(&Some(left), &Some(right));
        assert_eq!(diff["available"], true);
        assert_eq!(diff["assertions_changed"], true);
        assert_eq!(diff["failure_context_fields_changed"], true);
        assert_eq!(diff["failure_context_complete_changed"], true);
        assert_eq!(
            diff["safeguard_observation_difference"]["scenario_failure_count"],
            1
        );
        assert_eq!(diff["safeguard_observation_difference"]["retry_count"], 3);
        assert_eq!(
            diff["safeguard_observation_difference"]["database_bytes"],
            600
        );
        assert_eq!(
            diff["safeguard_observation_difference"]["generation_ms"],
            150
        );
        assert_eq!(
            diff["safeguard_observation_difference"]["peak_memory_bytes"],
            4096
        );
    }

    #[test]
    fn progress_log_reader_and_comparison_report_event_drift() -> Result<()> {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let temp = std::env::temp_dir().join(format!(
            "fact-sim-cli-progress-log-test-{}-{unique}",
            std::process::id()
        ));
        let left = temp.join("left");
        let right = temp.join("right");
        std::fs::create_dir_all(left.join("logs"))?;
        std::fs::create_dir_all(right.join("logs"))?;
        std::fs::write(
            left.join("logs/progress.jsonl"),
            [
                r#"{"phase":"initialized","current_scenario_instance":0,"current_object_count":0,"progress":{"scenario_failure_count":0,"database_bytes":0},"completed":false,"safe_boundary":true}"#,
                r#"{"phase":"completed","current_scenario_instance":10,"current_object_count":100,"progress":{"scenario_failure_count":0,"database_bytes":1000},"completed":true,"safe_boundary":true}"#,
            ]
            .join("\n"),
        )?;
        std::fs::write(
            right.join("logs/progress.jsonl"),
            [
                r#"{"phase":"initialized","current_scenario_instance":0,"current_object_count":0,"progress":{"scenario_failure_count":0,"database_bytes":0},"completed":false,"safe_boundary":true}"#,
                r#"{"phase":"started","current_scenario_instance":0,"current_object_count":0,"progress":{"scenario_failure_count":0,"database_bytes":0},"completed":false,"safe_boundary":true}"#,
                r#"{"phase":"completed","current_scenario_instance":12,"current_object_count":130,"progress":{"scenario_failure_count":1,"database_bytes":1600},"completed":true,"safe_boundary":false}"#,
            ]
            .join("\n"),
        )?;

        let left_log = read_optional_progress_log(&left)?;
        let right_log = read_optional_progress_log(&right)?;
        assert_eq!(left_log.as_ref().unwrap()["event_count"], 2);
        assert_eq!(right_log.as_ref().unwrap()["event_count"], 3);
        let diff = compare_optional_progress_logs(&left_log, &right_log);
        assert_eq!(diff["available"], true);
        assert_eq!(diff["event_count_difference"], 1);
        assert_eq!(diff["phases_changed"], true);
        assert_eq!(
            diff["last_event_difference"]["current_scenario_instance"],
            2
        );
        assert_eq!(diff["last_event_difference"]["current_object_count"], 30);
        assert_eq!(diff["last_event_difference"]["scenario_failure_count"], 1);
        assert_eq!(diff["last_event_difference"]["database_bytes"], 600);
        assert_eq!(diff["last_event_difference"]["safe_boundary_changed"], true);
        Ok(())
    }

    #[test]
    fn scale_all_outputs_use_profile_seed_directories() {
        assert_eq!(
            scale_fixture_output(std::path::Path::new("fixtures"), "scale-500k-balanced", 42),
            PathBuf::from("fixtures/scale-500k-balanced-seed-42")
        );
        assert_eq!(
            scale_fixture_output(
                std::path::Path::new("/tmp/fact-scale"),
                "scale-500k-sync-heavy",
                7
            ),
            PathBuf::from("/tmp/fact-scale/scale-500k-sync-heavy-seed-7")
        );
    }

    #[test]
    fn scale_plan_commands_reproduce_effective_target_override() {
        let reduced = scale_plan_json_for_output(
            "scale-500k-balanced",
            42,
            100,
            PathBuf::from("fixtures/scale-500k-balanced-seed-42"),
        )
        .unwrap();
        assert_eq!(reduced["effective_target_objects"], 100);
        assert!(
            reduced["commands"]["generate"]
                .as_str()
                .unwrap()
                .starts_with("./target/release/fact-sim")
        );
        assert!(
            reduced["commands"]["generate"]
                .as_str()
                .unwrap()
                .contains("--target-propositions 100")
        );

        let default = scale_plan_json_for_output(
            "scale-500k-balanced",
            42,
            fact_sim_runner::scale::TARGET_OBJECTS,
            PathBuf::from("fixtures/scale-500k-balanced-seed-42"),
        )
        .unwrap();
        assert!(
            !default["commands"]["generate"]
                .as_str()
                .unwrap()
                .contains("--target-propositions")
        );
    }

    #[test]
    fn fixture_target_override_is_visible_in_cli_help() {
        let mut command = Cli::command();
        let generate_help = command
            .find_subcommand_mut("generate")
            .unwrap()
            .render_long_help()
            .to_string();
        assert!(generate_help.contains("--target-propositions <TARGET_PROPOSITIONS>"));
        assert!(generate_help.contains("alias: --target-objects"));

        let mut command = Cli::command();
        let plan_help = command
            .find_subcommand_mut("plan")
            .unwrap()
            .render_long_help()
            .to_string();
        assert!(plan_help.contains("--target-propositions <TARGET_PROPOSITIONS>"));
        assert!(plan_help.contains("alias: --target-objects"));
    }

    #[test]
    fn scale_all_plan_uses_custom_output_base() {
        let plan = scale_all_plan_json(7, 100, PathBuf::from("/tmp/fact-scale")).unwrap();
        assert_eq!(plan["output_base"], "/tmp/fact-scale");
        assert_eq!(
            plan["profiles"][0]["suggested_output"],
            "/tmp/fact-scale/scale-500k-balanced-seed-7"
        );
        assert!(
            plan["profiles"][0]["commands"]["generate"]
                .as_str()
                .unwrap()
                .starts_with("./target/release/fact-sim")
        );
        assert!(
            plan["profiles"][0]["commands"]["generate"]
                .as_str()
                .unwrap()
                .contains("--output /tmp/fact-scale/scale-500k-balanced-seed-7")
        );
    }

    #[test]
    fn scale_single_plan_uses_exact_custom_output() {
        let plan = scale_plan_json_for_output(
            "scale-500k-balanced",
            7,
            100,
            PathBuf::from("/tmp/exact-balanced"),
        )
        .unwrap();
        assert_eq!(plan["suggested_output"], "/tmp/exact-balanced");
        assert!(
            plan["commands"]["generate"]
                .as_str()
                .unwrap()
                .starts_with("./target/release/fact-sim")
        );
        assert!(
            plan["commands"]["generate"]
                .as_str()
                .unwrap()
                .contains("--output /tmp/exact-balanced")
        );
    }

    #[test]
    fn scale_all_preflight_requires_combined_storage_capacity() {
        let plans = vec![
            serde_json::json!({
                "storage_preflight": {
                    "estimated_storage_bytes": 70,
                    "available_bytes": 100,
                    "sufficient": true
                }
            }),
            serde_json::json!({
                "storage_preflight": {
                    "estimated_storage_bytes": 50,
                    "available_bytes": 100,
                    "sufficient": true
                }
            }),
        ];
        let summary = scale_all_preflight_summary(&plans);
        assert_eq!(summary["profile_count"], 2);
        assert_eq!(summary["total_estimated_storage_bytes"], 120);
        assert_eq!(summary["minimum_available_bytes"], 100);
        assert_eq!(summary["individual_profiles_sufficient"], true);
        assert_eq!(summary["sufficient"], false);
        assert!(
            summary["warning"]
                .as_str()
                .unwrap()
                .contains("combined estimated storage")
        );
    }

    #[test]
    fn scale_all_preflight_reports_profile_level_blocks() {
        let plans = vec![serde_json::json!({
            "storage_preflight": {
                "estimated_storage_bytes": 70,
                "available_bytes": 100,
                "sufficient": false
            }
        })];
        let summary = scale_all_preflight_summary(&plans);
        assert_eq!(summary["individual_profiles_sufficient"], false);
        assert_eq!(summary["sufficient"], false);
        assert_eq!(
            summary["warning"],
            "one or more profile preflights are insufficient"
        );
    }

    #[test]
    fn scale_resume_replay_rejects_unsafe_checkpoint() {
        let checkpoint = serde_json::json!({
            "phase": "started",
            "safe_boundary": false,
        });
        let error = ensure_scale_resume_replay_preconditions(
            &checkpoint,
            std::path::Path::new("/tmp/fact-missing-resume-output"),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("requires a safe-boundary checkpoint")
        );
    }

    #[test]
    fn scale_resume_replay_rejects_existing_output_path() {
        let checkpoint = serde_json::json!({
            "phase": "checkpoint",
            "safe_boundary": true,
        });
        let error = ensure_scale_resume_replay_preconditions(
            &checkpoint,
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")),
        )
        .unwrap_err();
        assert!(error.to_string().contains("requires output path"));
    }

    #[test]
    fn status_distribution_difference_reports_manifest_status_counts() {
        let left = serde_json::json!({
            "accepted": 10,
            "pending": 3,
            "withdrawn": 1,
        });
        let right = serde_json::json!({
            "accepted": 12,
            "pending": 1,
            "archived": 4,
        });
        let diff = distribution_difference(&left, &right);
        assert_eq!(diff["accepted"], 2);
        assert_eq!(diff["pending"], -2);
        assert_eq!(diff["withdrawn"], -1);
        assert_eq!(diff["archived"], 4);
    }

    #[test]
    fn logical_replay_digest_comparison_reports_manifest_drift() {
        let same = compare_logical_replay_digests(
            &serde_json::json!({"logical_replay_digest": "abc"}),
            &serde_json::json!({"logical_replay_digest": "abc"}),
        );
        assert_eq!(same["available"], true);
        assert_eq!(same["changed"], false);
        assert_eq!(same["left"], "abc");
        assert_eq!(same["right"], "abc");

        let changed = compare_logical_replay_digests(
            &serde_json::json!({"logical_replay_digest": "abc"}),
            &serde_json::json!({"logical_replay_digest": "def"}),
        );
        assert_eq!(changed["available"], true);
        assert_eq!(changed["changed"], true);

        let missing = compare_logical_replay_digests(
            &serde_json::json!({}),
            &serde_json::json!({"logical_replay_digest": "def"}),
        );
        assert_eq!(missing["available"], false);
        assert_eq!(missing["changed"], false);
    }

    #[test]
    fn conflict_behavior_comparison_reports_nonnumeric_conflict_fields() {
        let left_counts = serde_json::json!({
            "sibling_revision": 2,
            "incompatible_deliberation": 1,
        });
        let right_counts = serde_json::json!({
            "sibling_revision": 5,
            "decision_conflict": 1,
        });
        let count_diff = distribution_difference(&left_counts, &right_counts);
        assert_eq!(count_diff["sibling_revision"], 3);
        assert_eq!(count_diff["incompatible_deliberation"], -1);
        assert_eq!(count_diff["decision_conflict"], 1);

        let left_report = serde_json::json!({
            "compatible_deliberations_without_conflict": 1,
            "last_undisputed_ancestor_preserved_as_effective": true,
            "arrival_order_selected_winner": false,
            "sample_conflict_proposition_id": "left",
            "conflict_replicas": ["operations_a"]
        });
        let right_report = serde_json::json!({
            "compatible_deliberations_without_conflict": 3,
            "last_undisputed_ancestor_preserved_as_effective": false,
            "arrival_order_selected_winner": false,
            "sample_conflict_proposition_id": "right",
            "conflict_replicas": ["operations_a", "operations_b"]
        });
        let behavior_diff = compare_conflict_reports(&left_report, &right_report);
        assert_eq!(
            behavior_diff["compatible_deliberations_without_conflict_difference"],
            2
        );
        assert_eq!(
            behavior_diff["last_undisputed_ancestor_preserved_changed"],
            true
        );
        assert_eq!(
            behavior_diff["arrival_order_selected_winner_changed"],
            false
        );
        assert_eq!(behavior_diff["sample_conflict_proposition_changed"], true);
        assert_eq!(
            behavior_diff["conflict_replicas_difference"]["only_right"],
            serde_json::json!(["operations_b"])
        );
    }
}
