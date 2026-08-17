use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

pub use crate::sync_scale::{
    GenerateOptions, SyncScaleReport as ScaleReport, validate_scale_fixture_checkpoint_metadata,
    verify_sync_scale_fixture,
};

pub const TARGET_OBJECTS: usize = 500_000;
pub const BULK_PROPOSITION_PROFILE: &str = "scale-500k-proposition-bulk";

pub const PROFILES: &[&str] = &[
    "scale-500k-balanced",
    "scale-500k-proposition-heavy",
    "scale-500k-revision-heavy",
    "scale-500k-deliberation-heavy",
    "scale-500k-sync-heavy",
    "scale-500k-conflict-heavy",
    BULK_PROPOSITION_PROFILE,
];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScaleProfileConfig {
    pub version: u32,
    pub name: String,
    pub seed: u64,
    pub target_objects: usize,
    pub world: WorldConfig,
    pub distribution: DistributionConfig,
    pub safeguards: SafeguardsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorldConfig {
    pub actors: usize,
    pub ledgers: usize,
    pub replicas_per_shared_ledger_min: usize,
    pub replicas_per_shared_ledger_max: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DistributionConfig {
    pub stable_fact_journeys: usize,
    pub collaborative_revision_journeys: usize,
    pub rejected_revision_journeys: usize,
    pub participant_lifecycle_journeys: usize,
    pub identity_lifecycle_journeys: usize,
    pub synchronization_journeys: usize,
    pub conflict_journeys: usize,
    pub reconciliation_journeys: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SafeguardsConfig {
    pub max_database_bytes: u64,
    pub max_scenario_failures: usize,
    pub max_retry_count: usize,
    pub max_generation_seconds: u64,
    pub max_memory_mb: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectBudgetPlan {
    pub target_objects: usize,
    pub estimated_topology_objects: usize,
    pub estimated_instances: usize,
    pub estimated_objects: usize,
    pub estimated_storage_bytes: u64,
    pub families: Vec<ScenarioFamilyBudget>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScenarioFamilyBudget {
    pub family: String,
    pub weight: usize,
    pub expected_objects_per_instance: usize,
    pub estimated_instances: usize,
    pub estimated_objects: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoragePreflightReport {
    pub output: PathBuf,
    pub checked_path: PathBuf,
    pub estimated_storage_bytes: u64,
    pub max_database_bytes: u64,
    pub available_bytes: Option<u64>,
    pub sufficient: bool,
    pub warning: Option<String>,
}

pub fn is_scale_profile(profile: &str) -> bool {
    PROFILES.contains(&profile)
}

pub fn is_bulk_proposition_profile(profile: &str) -> bool {
    profile == BULK_PROPOSITION_PROFILE
}

pub fn profile_config(profile: &str, seed: u64) -> Result<ScaleProfileConfig> {
    let distribution = match profile {
        BULK_PROPOSITION_PROFILE => DistributionConfig {
            stable_fact_journeys: 100,
            collaborative_revision_journeys: 0,
            rejected_revision_journeys: 0,
            participant_lifecycle_journeys: 0,
            identity_lifecycle_journeys: 0,
            synchronization_journeys: 0,
            conflict_journeys: 0,
            reconciliation_journeys: 0,
        },
        "scale-500k-balanced" => DistributionConfig {
            stable_fact_journeys: 45,
            collaborative_revision_journeys: 20,
            rejected_revision_journeys: 8,
            participant_lifecycle_journeys: 8,
            identity_lifecycle_journeys: 5,
            synchronization_journeys: 8,
            conflict_journeys: 4,
            reconciliation_journeys: 2,
        },
        "scale-500k-proposition-heavy" => DistributionConfig {
            stable_fact_journeys: 78,
            collaborative_revision_journeys: 8,
            rejected_revision_journeys: 3,
            participant_lifecycle_journeys: 2,
            identity_lifecycle_journeys: 2,
            synchronization_journeys: 4,
            conflict_journeys: 2,
            reconciliation_journeys: 1,
        },
        "scale-500k-revision-heavy" => DistributionConfig {
            stable_fact_journeys: 18,
            collaborative_revision_journeys: 46,
            rejected_revision_journeys: 14,
            participant_lifecycle_journeys: 5,
            identity_lifecycle_journeys: 4,
            synchronization_journeys: 5,
            conflict_journeys: 5,
            reconciliation_journeys: 3,
        },
        "scale-500k-deliberation-heavy" => DistributionConfig {
            stable_fact_journeys: 20,
            collaborative_revision_journeys: 15,
            rejected_revision_journeys: 7,
            participant_lifecycle_journeys: 28,
            identity_lifecycle_journeys: 5,
            synchronization_journeys: 8,
            conflict_journeys: 10,
            reconciliation_journeys: 7,
        },
        "scale-500k-sync-heavy" => DistributionConfig {
            stable_fact_journeys: 22,
            collaborative_revision_journeys: 12,
            rejected_revision_journeys: 6,
            participant_lifecycle_journeys: 5,
            identity_lifecycle_journeys: 8,
            synchronization_journeys: 34,
            conflict_journeys: 8,
            reconciliation_journeys: 5,
        },
        "scale-500k-conflict-heavy" => DistributionConfig {
            stable_fact_journeys: 12,
            collaborative_revision_journeys: 13,
            rejected_revision_journeys: 8,
            participant_lifecycle_journeys: 10,
            identity_lifecycle_journeys: 5,
            synchronization_journeys: 12,
            conflict_journeys: 26,
            reconciliation_journeys: 14,
        },
        other => {
            anyhow::bail!("unknown scale fixture profile `{other}`");
        }
    };
    Ok(ScaleProfileConfig {
        version: 1,
        name: profile.to_string(),
        seed,
        target_objects: TARGET_OBJECTS,
        world: WorldConfig {
            actors: if profile == BULK_PROPOSITION_PROFILE {
                1
            } else {
                12
            },
            ledgers: if profile == BULK_PROPOSITION_PROFILE {
                1
            } else {
                3
            },
            replicas_per_shared_ledger_min: if profile == BULK_PROPOSITION_PROFILE {
                0
            } else {
                2
            },
            replicas_per_shared_ledger_max: if profile == BULK_PROPOSITION_PROFILE {
                0
            } else {
                2
            },
        },
        distribution,
        safeguards: SafeguardsConfig {
            max_database_bytes: 80 * 1024 * 1024 * 1024,
            max_scenario_failures: 0,
            max_retry_count: 10_000,
            max_generation_seconds: 7 * 24 * 60 * 60,
            max_memory_mb: None,
        },
    })
}

pub fn profile_yaml(profile: &str, seed: u64) -> Result<String> {
    let target_objects = profile_config(profile, seed)?.target_objects;
    profile_yaml_for_target(profile, seed, target_objects)
}

pub fn profile_yaml_for_target(profile: &str, seed: u64, target_objects: usize) -> Result<String> {
    let mut config = profile_config(profile, seed)?;
    config.target_objects = target_objects;
    Ok(format!(
        "version: {}\nname: {}\nseed: {}\ntarget_objects: {}\nworld:\n  actors: {}\n  ledgers: {}\n  replicas_per_shared_ledger:\n    min: {}\n    max: {}\ndistribution:\n  stable_fact_journeys: {}\n  collaborative_revision_journeys: {}\n  rejected_revision_journeys: {}\n  participant_lifecycle_journeys: {}\n  identity_lifecycle_journeys: {}\n  synchronization_journeys: {}\n  conflict_journeys: {}\n  reconciliation_journeys: {}\nsafeguards:\n  max_database_bytes: {}\n  max_scenario_failures: {}\n  max_retry_count: {}\n  max_generation_seconds: {}\n  max_memory_mb: {}\n",
        config.version,
        config.name,
        config.seed,
        config.target_objects,
        config.world.actors,
        config.world.ledgers,
        config.world.replicas_per_shared_ledger_min,
        config.world.replicas_per_shared_ledger_max,
        config.distribution.stable_fact_journeys,
        config.distribution.collaborative_revision_journeys,
        config.distribution.rejected_revision_journeys,
        config.distribution.participant_lifecycle_journeys,
        config.distribution.identity_lifecycle_journeys,
        config.distribution.synchronization_journeys,
        config.distribution.conflict_journeys,
        config.distribution.reconciliation_journeys,
        config.safeguards.max_database_bytes,
        config.safeguards.max_scenario_failures,
        config.safeguards.max_retry_count,
        config.safeguards.max_generation_seconds,
        config
            .safeguards
            .max_memory_mb
            .map(|value| value.to_string())
            .unwrap_or_else(|| "null".to_string()),
    ))
}

pub fn scenario_family_for_instance(config: &ScaleProfileConfig, instance: usize) -> &'static str {
    let slot = ((instance as u64 + config.seed) % 100) as usize;
    let weighted = [
        ("stable-fact", config.distribution.stable_fact_journeys),
        (
            "collaborative-revision",
            config.distribution.collaborative_revision_journeys,
        ),
        (
            "rejected-revision",
            config.distribution.rejected_revision_journeys,
        ),
        (
            "participant-lifecycle",
            config.distribution.participant_lifecycle_journeys,
        ),
        (
            "identity-lifecycle",
            config.distribution.identity_lifecycle_journeys,
        ),
        (
            "synchronization",
            config.distribution.synchronization_journeys,
        ),
        ("conflict", config.distribution.conflict_journeys),
        (
            "reconciliation",
            config.distribution.reconciliation_journeys,
        ),
    ];
    let mut cursor = 0;
    for (family, weight) in weighted {
        cursor += weight;
        if slot < cursor {
            return family;
        }
    }
    "stable-fact"
}

pub fn scenario_families_for_profile(profile: &str) -> Result<Vec<&'static str>> {
    let config = profile_config(profile, 0)?;
    let weighted = [
        ("stable-fact", config.distribution.stable_fact_journeys),
        (
            "collaborative-revision",
            config.distribution.collaborative_revision_journeys,
        ),
        (
            "rejected-revision",
            config.distribution.rejected_revision_journeys,
        ),
        (
            "participant-lifecycle",
            config.distribution.participant_lifecycle_journeys,
        ),
        (
            "identity-lifecycle",
            config.distribution.identity_lifecycle_journeys,
        ),
        (
            "synchronization",
            config.distribution.synchronization_journeys,
        ),
        ("conflict", config.distribution.conflict_journeys),
        (
            "reconciliation",
            config.distribution.reconciliation_journeys,
        ),
    ];
    Ok(weighted
        .into_iter()
        .filter_map(|(family, weight)| (weight > 0).then_some(family))
        .collect())
}

pub fn periodic_sync_interval(profile: &str) -> Result<usize> {
    Ok(match profile {
        BULK_PROPOSITION_PROFILE => usize::MAX,
        "scale-500k-sync-heavy" => 250,
        "scale-500k-conflict-heavy" => 500,
        "scale-500k-balanced" => 1_000,
        "scale-500k-proposition-heavy"
        | "scale-500k-revision-heavy"
        | "scale-500k-deliberation-heavy" => 2_500,
        other => {
            anyhow::bail!("unknown scale fixture profile `{other}`");
        }
    })
}

pub fn object_budget_plan(config: &ScaleProfileConfig, target_objects: usize) -> ObjectBudgetPlan {
    if config.name == BULK_PROPOSITION_PROFILE {
        let estimated_topology_objects = estimate_topology_objects(config);
        let expected_objects_per_instance = expected_objects_per_instance("stable-fact");
        let estimated_scenario_objects = target_objects * expected_objects_per_instance;
        let estimated_objects = estimated_topology_objects + estimated_scenario_objects;
        return ObjectBudgetPlan {
            target_objects,
            estimated_topology_objects,
            estimated_instances: target_objects,
            estimated_storage_bytes: estimate_storage_bytes(estimated_objects),
            estimated_objects,
            families: vec![ScenarioFamilyBudget {
                family: "stable-fact".into(),
                weight: 100,
                expected_objects_per_instance,
                estimated_instances: target_objects,
                estimated_objects: estimated_scenario_objects,
            }],
        };
    }
    let weighted = weighted_families(&config.distribution);
    let estimated_topology_objects = estimate_topology_objects(config);
    let expected_per_100 = weighted
        .iter()
        .map(|(family, weight)| expected_objects_per_instance(family) * weight)
        .sum::<usize>();
    let required_family_count = weighted.iter().filter(|(_, weight)| *weight > 0).count();
    let scenario_target_objects = target_objects.saturating_sub(estimated_topology_objects);
    let estimated_instances = scenario_target_objects
        .saturating_mul(100)
        .div_ceil(expected_per_100.max(1))
        .max(required_family_count);
    let families = weighted
        .into_iter()
        .filter(|(_, weight)| *weight > 0)
        .map(|(family, weight)| {
            let estimated_instances = (estimated_instances * weight).div_ceil(100).max(1);
            let expected_objects_per_instance = expected_objects_per_instance(family);
            ScenarioFamilyBudget {
                family: family.to_string(),
                weight,
                expected_objects_per_instance,
                estimated_instances,
                estimated_objects: estimated_instances * expected_objects_per_instance,
            }
        })
        .collect::<Vec<_>>();
    let estimated_scenario_objects = families
        .iter()
        .map(|family| family.estimated_objects)
        .sum::<usize>();
    let estimated_objects = estimated_topology_objects + estimated_scenario_objects;
    ObjectBudgetPlan {
        target_objects,
        estimated_topology_objects,
        estimated_instances,
        estimated_storage_bytes: estimate_storage_bytes(estimated_objects),
        estimated_objects,
        families,
    }
}

fn estimate_topology_objects(config: &ScaleProfileConfig) -> usize {
    let shared_replica_budget =
        config.world.ledgers * config.world.replicas_per_shared_ledger_max.max(1);
    let identity_ledger_objects = config.world.actors * 4;
    let shared_ledger_objects = config.world.ledgers * 8;
    let replica_objects = shared_replica_budget * 4;
    identity_ledger_objects + shared_ledger_objects + replica_objects + 100
}

pub fn storage_preflight_report(
    output: &Path,
    config: &ScaleProfileConfig,
    budget: &ObjectBudgetPlan,
) -> StoragePreflightReport {
    let checked_path = existing_path_for_capacity_check(output);
    let available_bytes = available_bytes(&checked_path);
    let warning = if budget.estimated_storage_bytes > config.safeguards.max_database_bytes {
        Some(format!(
            "estimated storage {} exceeds configured max database bytes {}",
            budget.estimated_storage_bytes, config.safeguards.max_database_bytes
        ))
    } else if let Some(available) = available_bytes {
        (available < budget.estimated_storage_bytes).then(|| {
            format!(
                "estimated storage {} exceeds available bytes {} at {}",
                budget.estimated_storage_bytes,
                available,
                checked_path.display()
            )
        })
    } else {
        None
    };
    StoragePreflightReport {
        output: output.to_path_buf(),
        checked_path,
        estimated_storage_bytes: budget.estimated_storage_bytes,
        max_database_bytes: config.safeguards.max_database_bytes,
        available_bytes,
        sufficient: warning.is_none(),
        warning,
    }
}

fn estimate_storage_bytes(estimated_objects: usize) -> u64 {
    const SCALE_TOPOLOGY_STORAGE_FLOOR: u64 = 512 * 1024 * 1024;
    const ESTIMATED_BYTES_PER_OBJECT: u64 = 12 * 1024;
    SCALE_TOPOLOGY_STORAGE_FLOOR + estimated_objects as u64 * ESTIMATED_BYTES_PER_OBJECT
}

fn existing_path_for_capacity_check(path: &Path) -> PathBuf {
    let mut current = path;
    loop {
        if current.exists() {
            return current.to_path_buf();
        }
        let Some(parent) = current.parent() else {
            return PathBuf::from(".");
        };
        current = parent;
    }
}

fn available_bytes(path: &Path) -> Option<u64> {
    let output = Command::new("df").arg("-Pk").arg(path).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8(output.stdout).ok()?;
    let line = stdout.lines().nth(1)?;
    let available_kib = line.split_whitespace().nth(3)?.parse::<u64>().ok()?;
    Some(available_kib * 1024)
}

fn expected_objects_per_instance(family: &str) -> usize {
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

fn weighted_families(distribution: &DistributionConfig) -> [(&'static str, usize); 8] {
    [
        ("stable-fact", distribution.stable_fact_journeys),
        (
            "collaborative-revision",
            distribution.collaborative_revision_journeys,
        ),
        ("rejected-revision", distribution.rejected_revision_journeys),
        (
            "participant-lifecycle",
            distribution.participant_lifecycle_journeys,
        ),
        (
            "identity-lifecycle",
            distribution.identity_lifecycle_journeys,
        ),
        ("synchronization", distribution.synchronization_journeys),
        ("conflict", distribution.conflict_journeys),
        ("reconciliation", distribution.reconciliation_journeys),
    ]
}

pub fn generate_scale(mut options: GenerateOptions) -> Result<ScaleReport> {
    if is_bulk_proposition_profile(&options.profile) {
        return crate::sync_scale::generate_bulk_proposition_fixture(options);
    }
    let config = profile_config(&options.profile, options.seed)
        .context("load scale fixture profile config")?;
    let target_objects = options.target_objects.unwrap_or(TARGET_OBJECTS);
    let budget = object_budget_plan(&config, target_objects);
    let preflight = storage_preflight_report(&options.output, &config, &budget);
    if !preflight.sufficient {
        anyhow::bail!(
            "scale fixture preflight failed for `{}`: {}",
            options.profile,
            preflight
                .warning
                .as_deref()
                .unwrap_or("insufficient generation capacity")
        );
    }
    options.target_objects = Some(target_objects);
    crate::sync_scale::generate_sync_scale(options)
}

pub fn verify_scale_fixture(fixture: &Path) -> Result<ScaleReport> {
    let manifest_path = fixture.join("manifest.json");
    if manifest_path.exists() {
        let manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(&manifest_path)?)?;
        if manifest["profile"].as_str() == Some(BULK_PROPOSITION_PROFILE) {
            return crate::sync_scale::verify_bulk_proposition_fixture(fixture);
        }
    }
    crate::sync_scale::verify_sync_scale_fixture(fixture)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_scale_profiles_have_versioned_configs() {
        assert!(PROFILES.len() >= 5);
        for profile in PROFILES {
            let config = profile_config(profile, 42).unwrap();
            assert_eq!(config.version, 1);
            assert_eq!(config.target_objects, TARGET_OBJECTS);
            let total = config.distribution.stable_fact_journeys
                + config.distribution.collaborative_revision_journeys
                + config.distribution.rejected_revision_journeys
                + config.distribution.participant_lifecycle_journeys
                + config.distribution.identity_lifecycle_journeys
                + config.distribution.synchronization_journeys
                + config.distribution.conflict_journeys
                + config.distribution.reconciliation_journeys;
            assert_eq!(total, 100, "{profile} distribution must be percentages");
        }
    }

    #[test]
    fn profile_yaml_can_record_effective_target_override() {
        let default_yaml = profile_yaml("scale-500k-balanced", 42).unwrap();
        assert!(default_yaml.contains("target_objects: 500000\n"));

        let reduced_yaml = profile_yaml_for_target("scale-500k-balanced", 42, 100).unwrap();
        assert!(reduced_yaml.contains("target_objects: 100\n"));
        assert!(reduced_yaml.contains("name: scale-500k-balanced\n"));
        assert!(reduced_yaml.contains("seed: 42\n"));
    }

    #[test]
    fn seeded_family_scheduler_honors_profile_distribution() {
        let config = profile_config("scale-500k-balanced", 42).unwrap();
        let mut counts = std::collections::BTreeMap::new();
        for instance in 0..100 {
            *counts
                .entry(scenario_family_for_instance(&config, instance))
                .or_insert(0) += 1;
        }
        assert_eq!(counts["stable-fact"], 45);
        assert_eq!(counts["collaborative-revision"], 20);
        assert_eq!(counts["rejected-revision"], 8);
        assert_eq!(counts["participant-lifecycle"], 8);
        assert_eq!(counts["identity-lifecycle"], 5);
        assert_eq!(counts["synchronization"], 8);
        assert_eq!(counts["conflict"], 4);
        assert_eq!(counts["reconciliation"], 2);
    }

    #[test]
    fn profile_scenario_families_match_configured_scale_families() {
        let families = scenario_families_for_profile("scale-500k-balanced").unwrap();
        assert_eq!(
            families,
            vec![
                "stable-fact",
                "collaborative-revision",
                "rejected-revision",
                "participant-lifecycle",
                "identity-lifecycle",
                "synchronization",
                "conflict",
                "reconciliation",
            ]
        );
    }

    #[test]
    fn periodic_sync_intervals_match_profile_sync_pressure() {
        assert_eq!(
            periodic_sync_interval("scale-500k-sync-heavy").unwrap(),
            250
        );
        assert_eq!(
            periodic_sync_interval("scale-500k-conflict-heavy").unwrap(),
            500
        );
        assert_eq!(
            periodic_sync_interval("scale-500k-balanced").unwrap(),
            1_000
        );
        assert_eq!(
            periodic_sync_interval("scale-500k-proposition-heavy").unwrap(),
            2_500
        );
        assert_eq!(
            periodic_sync_interval("scale-500k-revision-heavy").unwrap(),
            2_500
        );
        assert_eq!(
            periodic_sync_interval("scale-500k-deliberation-heavy").unwrap(),
            2_500
        );
    }

    #[test]
    fn object_budget_plan_estimates_family_work() {
        let config = profile_config("scale-500k-balanced", 42).unwrap();
        let budget = object_budget_plan(&config, TARGET_OBJECTS);
        assert_eq!(budget.target_objects, TARGET_OBJECTS);
        assert!(budget.estimated_instances > 0);
        assert!(budget.estimated_topology_objects > 0);
        assert!(budget.estimated_objects >= TARGET_OBJECTS);
        assert!(budget.estimated_objects > budget.estimated_topology_objects);
        assert!(budget.estimated_storage_bytes >= budget.estimated_objects as u64);
        assert_eq!(budget.families.len(), 8);
        assert!(budget.families.iter().all(|family| {
            family.weight > 0
                && family.expected_objects_per_instance > 0
                && family.estimated_instances > 0
                && family.estimated_objects > 0
        }));
    }

    #[test]
    fn object_budget_includes_fixed_topology_floor_for_small_targets() {
        let config = profile_config("scale-500k-balanced", 42).unwrap();
        let budget = object_budget_plan(&config, 100);
        assert!(budget.estimated_topology_objects > 100);
        assert!(budget.estimated_objects >= budget.estimated_topology_objects);
        assert_eq!(budget.estimated_instances, budget.families.len());
        assert!(
            budget
                .families
                .iter()
                .all(|family| family.estimated_instances >= 1)
        );
    }

    #[test]
    fn storage_preflight_reports_capacity_estimate() {
        let config = profile_config("scale-500k-balanced", 42).unwrap();
        let budget = object_budget_plan(&config, 100);
        let report = storage_preflight_report(Path::new("."), &config, &budget);
        assert_eq!(report.output, PathBuf::from("."));
        assert!(report.estimated_storage_bytes > 0);
        assert_eq!(
            report.max_database_bytes,
            config.safeguards.max_database_bytes
        );
    }
}
