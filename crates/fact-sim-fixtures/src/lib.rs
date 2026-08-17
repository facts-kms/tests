use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FixtureManifest {
    pub name: String,
    pub profile: String,
    pub seed: u64,
    pub object_count: usize,
    pub database_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScaleProfile {
    pub name: String,
    pub intended_workload: String,
}

pub fn initial_scale_profiles() -> Vec<ScaleProfile> {
    vec![
        ScaleProfile {
            name: "multi-actor-10k".to_string(),
            intended_workload:
                "multi-actor fixture synthetic organization corpus with roughly 10,000 objects.".to_string(),
        },
        ScaleProfile {
            name: "sync-100k".to_string(),
            intended_workload:
                "synchronization fixture distributed synchronization corpus profile name retained as a legacy alias."
                    .to_string(),
        },
        ScaleProfile {
            name: "conflict-repair".to_string(),
            intended_workload:
                "conflict and repair fixture deterministic conflict, reconciliation, retry, and repair corpus."
                    .to_string(),
        },
        ScaleProfile {
            name: "scale-500k-balanced".to_string(),
            intended_workload:
                "scale fixture broadly used organization corpus with at least 500,000 canonical propositions."
                    .to_string(),
        },
        ScaleProfile {
            name: "scale-500k-proposition-heavy".to_string(),
            intended_workload:
                "scale fixture large knowledge repository corpus with mostly stable facts."
                    .to_string(),
        },
        ScaleProfile {
            name: "scale-500k-revision-heavy".to_string(),
            intended_workload:
                "scale fixture corpus with frequent revisions, history traversal, and projection rebuild pressure."
                    .to_string(),
        },
        ScaleProfile {
            name: "scale-500k-deliberation-heavy".to_string(),
            intended_workload:
                "scale fixture corpus emphasizing participants, invitations, comments, decisions, and settlements."
                    .to_string(),
        },
        ScaleProfile {
            name: "scale-500k-sync-heavy".to_string(),
            intended_workload:
                "scale fixture distributed corpus emphasizing replicas, bundle exchange, retries, and convergence."
                    .to_string(),
        },
        ScaleProfile {
            name: "scale-500k-conflict-heavy".to_string(),
            intended_workload:
                "scale fixture adversarial valid corpus emphasizing conflicts, reconciliation, and repair."
                    .to_string(),
        },
        ScaleProfile {
            name: "scale-500k-proposition-bulk".to_string(),
            intended_workload:
                "scale fixture single-ledger SQLite sizing corpus with 500,000 accepted propositions."
                    .to_string(),
        },
    ]
}
