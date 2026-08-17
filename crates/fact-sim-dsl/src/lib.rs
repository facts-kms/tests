use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use time::OffsetDateTime;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Scenario {
    pub version: u32,
    pub name: String,
    #[serde(default = "default_seed")]
    pub seed: u64,
    pub clock: ClockConfig,
    #[serde(default)]
    pub actors: Vec<ActorDecl>,
    #[serde(default)]
    pub characters: Vec<CharacterDecl>,
    #[serde(default)]
    pub ledgers: Vec<LedgerDecl>,
    #[serde(default)]
    pub steps: Vec<Step>,
    #[serde(default)]
    pub assertions: Vec<Assertion>,
}

impl Scenario {
    pub fn from_yaml_str(input: &str) -> Result<Self> {
        let scenario: Self =
            serde_yaml::from_str(input).context("failed to parse scenario YAML")?;
        scenario.validate()?;
        Ok(scenario)
    }

    pub fn validate(&self) -> Result<()> {
        let mut character_names = BTreeSet::new();
        let mut character_environments = BTreeMap::<&str, BTreeSet<&str>>::new();
        let mut environment_names = BTreeSet::new();
        for actor in &self.actors {
            if !character_names.insert(actor.name.as_str()) {
                bail!("duplicate character `{}`", actor.name);
            }
            character_environments.insert(actor.name.as_str(), BTreeSet::new());
        }
        for character in &self.characters {
            if !character_names.insert(character.name.as_str()) {
                bail!("duplicate character `{}`", character.name);
            }
            let mut environments = BTreeSet::new();
            for environment in &character.environments {
                if !environments.insert(environment.as_str()) {
                    bail!(
                        "character `{}` declares duplicate environment `{environment}`",
                        character.name
                    );
                }
                if !environment_names.insert(environment.as_str()) {
                    bail!("duplicate environment `{environment}`");
                }
            }
            character_environments.insert(character.name.as_str(), environments);
        }
        let mut ledger_names = BTreeSet::new();
        for ledger in &self.ledgers {
            if !ledger_names.insert(ledger.name.as_str()) {
                bail!("duplicate ledger `{}`", ledger.name);
            }
            if let Some(owner) = &ledger.owner {
                require_declared(&character_names, owner, "ledger owner")?;
            }
        }
        for step in &self.steps {
            validate_step(
                step,
                &character_names,
                &character_environments,
                &ledger_names,
                &environment_names,
            )?;
        }
        Ok(())
    }
}

fn validate_step(
    step: &Step,
    character_names: &BTreeSet<&str>,
    character_environments: &BTreeMap<&str, BTreeSet<&str>>,
    ledger_names: &BTreeSet<&str>,
    environment_names: &BTreeSet<&str>,
) -> Result<()> {
    match step {
        Step::Propose { propose } => {
            require_declared(character_names, &propose.actor, "propose actor")?;
            require_declared(ledger_names, &propose.ledger, "propose ledger")?;
            require_environment(character_environments, &propose.actor, &propose.replica)?;
        }
        Step::Revise { revise } => {
            require_declared(character_names, &revise.actor, "revise actor")?;
            require_environment(character_environments, &revise.actor, &revise.replica)?;
        }
        Step::Derive { derive } => {
            require_declared(character_names, &derive.actor, "derive actor")?;
            require_environment(character_environments, &derive.actor, &derive.replica)?;
            if derive.derived_from.len() < 2 {
                bail!("derive step requires at least two source revisions");
            }
        }
        Step::Accept { accept } => {
            require_declared(character_names, &accept.actor, "accept actor")?;
            require_environment(character_environments, &accept.actor, &accept.replica)?;
        }
        Step::Reject { reject } => {
            require_declared(character_names, &reject.actor, "reject actor")?;
            require_environment(character_environments, &reject.actor, &reject.replica)?;
        }
        Step::Decide { decide } => {
            require_declared(character_names, &decide.actor, "decide actor")?;
            require_environment(character_environments, &decide.actor, &decide.replica)?;
            if !matches!(decide.value.as_str(), "accepted" | "rejected") {
                bail!(
                    "decide step value `{}` must be `accepted` or `rejected`",
                    decide.value
                );
            }
        }
        Step::OpenDeliberation { open_deliberation } => {
            require_declared(
                character_names,
                &open_deliberation.actor,
                "open_deliberation actor",
            )?;
            require_environment(
                character_environments,
                &open_deliberation.actor,
                &open_deliberation.replica,
            )?;
        }
        Step::Settle { settle } => {
            require_declared(character_names, &settle.actor, "settle actor")?;
            require_environment(character_environments, &settle.actor, &settle.replica)?;
        }
        Step::Reconcile { reconcile } => {
            require_declared(character_names, &reconcile.actor, "reconcile actor")?;
            require_environment(character_environments, &reconcile.actor, &reconcile.replica)?;
            if !matches!(
                reconcile.resolution_mode.as_str(),
                "select" | "derive" | "reject-all"
            ) {
                bail!(
                    "reconcile resolution_mode `{}` must be `select`, `derive`, or `reject-all`",
                    reconcile.resolution_mode
                );
            }
            if reconcile.conflicts.is_empty() {
                bail!("reconcile step requires at least one conflict");
            }
        }
        Step::SemanticCorrection {
            semantic_correction,
        } => {
            require_declared(
                character_names,
                &semantic_correction.actor,
                "semantic_correction actor",
            )?;
            require_environment(
                character_environments,
                &semantic_correction.actor,
                &semantic_correction.replica,
            )?;
        }
        Step::RetryMissingDependency {
            retry_missing_dependency,
        } => {
            require_declared(
                environment_names,
                &retry_missing_dependency.from,
                "retry source environment",
            )?;
            require_declared(
                environment_names,
                &retry_missing_dependency.to,
                "retry target environment",
            )?;
            if let Some(ledger) = &retry_missing_dependency.ledger {
                require_declared(ledger_names, ledger, "retry ledger")?;
            }
        }
        Step::InitReplica { init_replica } => {
            require_declared(character_names, &init_replica.actor, "init_replica actor")?;
            require_declared(ledger_names, &init_replica.ledger, "init_replica ledger")?;
            require_environment(
                character_environments,
                &init_replica.actor,
                &Some(init_replica.replica.clone()),
            )?;
        }
        Step::RecordDisposition { record_disposition } => {
            require_declared(
                character_names,
                &record_disposition.coordinator,
                "disposition coordinator",
            )?;
            if !matches!(
                record_disposition.disposition.as_str(),
                "accepted"
                    | "rejected-protocol-invalid"
                    | "rejected-unauthorized"
                    | "rejected-policy"
                    | "rejected-missing-dependency"
                    | "deferred"
                    | "quarantined"
                    | "removed-local"
                    | "unknown"
            ) {
                bail!(
                    "record_disposition disposition `{}` is not a coordinator disposition",
                    record_disposition.disposition
                );
            }
            if let Some(classification) = &record_disposition.classification
                && !matches!(
                    classification.as_str(),
                    "retryable-unchanged" | "requires-new-signed-object"
                )
            {
                bail!(
                    "record_disposition classification `{classification}` is not a failure classification"
                );
            }
        }
        Step::Comment { comment } => {
            require_declared(character_names, &comment.actor, "comment actor")?;
            require_environment(character_environments, &comment.actor, &comment.replica)?;
        }
        Step::Invite { invite } => {
            require_declared(character_names, &invite.actor, "invite actor")?;
            require_environment(character_environments, &invite.actor, &invite.replica)?;
            let Some(participant) = &invite.participant else {
                bail!("invite step requires `participant`");
            };
            require_declared(character_names, participant, "invite participant")?;
        }
        Step::Join { join } => {
            require_declared(character_names, &join.actor, "join actor")?;
            require_environment(character_environments, &join.actor, &join.replica)?;
        }
        Step::InvitationLifecycle {
            invitation_lifecycle,
        } => {
            require_declared(
                character_names,
                &invitation_lifecycle.actor,
                "invitation_lifecycle actor",
            )?;
            require_environment(
                character_environments,
                &invitation_lifecycle.actor,
                &invitation_lifecycle.replica,
            )?;
            if !matches!(
                invitation_lifecycle.operation.as_str(),
                "decline" | "revoke" | "supersede"
            ) {
                bail!(
                    "invitation_lifecycle operation `{}` must be `decline`, `revoke`, or `supersede`",
                    invitation_lifecycle.operation
                );
            }
        }
        Step::Leave { leave } => {
            require_declared(character_names, &leave.actor, "leave actor")?;
            require_environment(character_environments, &leave.actor, &leave.replica)?;
        }
        Step::Archive { archive } => {
            require_optional_character(character_names, &archive.actor, "archive actor")?;
            require_environment(
                character_environments,
                archive.actor.as_deref().unwrap_or_default(),
                &archive.replica,
            )?;
        }
        Step::Withdraw { withdraw } => {
            require_optional_character(character_names, &withdraw.actor, "withdraw actor")?;
            require_environment(
                character_environments,
                withdraw.actor.as_deref().unwrap_or_default(),
                &withdraw.replica,
            )?;
        }
        Step::Grant { grant } => {
            require_declared(character_names, &grant.actor, "grant actor")?;
            require_declared(ledger_names, &grant.ledger, "grant ledger")?;
            if grant.capabilities.is_empty() {
                bail!("grant for `{}` has no capabilities", grant.actor);
            }
        }
        Step::RotateKey { rotate_key } => {
            require_declared(character_names, &rotate_key.actor, "rotate_key actor")?;
        }
        Step::Sync { sync } => {
            require_declared(environment_names, &sync.from, "sync source environment")?;
            require_declared(environment_names, &sync.to, "sync target environment")?;
            if let Some(ledger) = &sync.ledger {
                require_declared(ledger_names, ledger, "sync ledger")?;
            }
        }
        Step::Parallel { parallel } => {
            if parallel.branches.is_empty() {
                bail!("parallel step requires at least one branch");
            }
            for branch in &parallel.branches {
                if let Some(replica) = &branch.replica {
                    require_declared(environment_names, replica, "parallel branch replica")?;
                }
                for nested in &branch.steps {
                    validate_step(
                        nested,
                        character_names,
                        character_environments,
                        ledger_names,
                        environment_names,
                    )?;
                }
            }
        }
        Step::Assert { .. }
        | Step::RebuildProjections { .. }
        | Step::CorruptProjections { .. }
        | Step::CliCheck { .. }
        | Step::AdvanceTime { .. } => {}
    }
    Ok(())
}

fn require_declared(names: &BTreeSet<&str>, value: &str, role: &str) -> Result<()> {
    if names.contains(value) {
        Ok(())
    } else {
        bail!("{role} `{value}` is not declared")
    }
}

fn require_optional_character(
    names: &BTreeSet<&str>,
    value: &Option<String>,
    role: &str,
) -> Result<()> {
    if let Some(value) = value {
        require_declared(names, value, role)
    } else {
        Ok(())
    }
}

fn require_environment(
    environments_by_character: &BTreeMap<&str, BTreeSet<&str>>,
    character: &str,
    environment: &Option<String>,
) -> Result<()> {
    let Some(environment) = environment else {
        return Ok(());
    };
    let Some(environments) = environments_by_character.get(character) else {
        bail!("character `{character}` is not declared");
    };
    if environments.contains(environment.as_str()) {
        Ok(())
    } else {
        bail!("character `{character}` has no environment `{environment}`")
    }
}

fn default_seed() -> u64 {
    0
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClockConfig {
    #[serde(with = "time::serde::rfc3339")]
    pub start: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActorDecl {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CharacterDecl {
    pub name: String,
    #[serde(default)]
    pub environments: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LedgerDecl {
    pub name: String,
    #[serde(default)]
    pub owner: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum Step {
    Propose {
        propose: ProposeStep,
    },
    Revise {
        revise: ReviseStep,
    },
    Derive {
        derive: DeriveStep,
    },
    Accept {
        accept: DecisionStep,
    },
    Reject {
        reject: DecisionStep,
    },
    Decide {
        decide: RawDecisionStep,
    },
    OpenDeliberation {
        open_deliberation: OpenDeliberationStep,
    },
    Settle {
        settle: SettleStep,
    },
    Reconcile {
        reconcile: ReconcileStep,
    },
    SemanticCorrection {
        semantic_correction: SemanticCorrectionStep,
    },
    RetryMissingDependency {
        retry_missing_dependency: RetryMissingDependencyStep,
    },
    InitReplica {
        init_replica: InitReplicaStep,
    },
    RecordDisposition {
        record_disposition: RecordDispositionStep,
    },
    Assert {
        assert: Vec<Assertion>,
    },
    RebuildProjections {
        rebuild_projections: RebuildStep,
    },
    CorruptProjections {
        corrupt_projections: RebuildStep,
    },
    CliCheck {
        cli_check: CliCheckStep,
    },
    Grant {
        grant: GrantStep,
    },
    RotateKey {
        rotate_key: RotateKeyStep,
    },
    Sync {
        sync: SyncStep,
    },
    Parallel {
        parallel: ParallelStep,
    },
    Comment {
        comment: CommentStep,
    },
    Invite {
        invite: ParticipantStep,
    },
    Join {
        join: ParticipantStep,
    },
    InvitationLifecycle {
        invitation_lifecycle: InvitationLifecycleStep,
    },
    Leave {
        leave: ParticipantStep,
    },
    Archive {
        archive: PropositionRefStep,
    },
    Withdraw {
        withdraw: PropositionRefStep,
    },
    AdvanceTime {
        advance_time: AdvanceTimeStep,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProposeStep {
    pub actor: String,
    #[serde(default)]
    pub replica: Option<String>,
    pub ledger: String,
    #[serde(rename = "as")]
    pub symbol: String,
    pub markdown: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReviseStep {
    pub actor: String,
    #[serde(default)]
    pub replica: Option<String>,
    pub proposition: String,
    #[serde(rename = "as")]
    pub symbol: Option<String>,
    pub markdown: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeriveStep {
    pub actor: String,
    #[serde(default)]
    pub replica: Option<String>,
    pub proposition: String,
    pub common_ancestor: String,
    pub derived_from: Vec<String>,
    #[serde(rename = "as")]
    pub symbol: String,
    pub markdown: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DecisionStep {
    pub actor: String,
    #[serde(default)]
    pub replica: Option<String>,
    pub proposition: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawDecisionStep {
    pub actor: String,
    #[serde(default)]
    pub replica: Option<String>,
    pub proposition: String,
    #[serde(default)]
    pub deliberation: Option<String>,
    pub value: String,
    #[serde(default, rename = "as")]
    pub symbol: Option<String>,
    #[serde(default)]
    pub supersedes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpenDeliberationStep {
    pub actor: String,
    #[serde(default)]
    pub replica: Option<String>,
    pub proposition: String,
    pub revision: String,
    #[serde(rename = "as")]
    pub symbol: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SettleStep {
    pub actor: String,
    #[serde(default)]
    pub replica: Option<String>,
    pub proposition: String,
    pub revision: String,
    pub deliberation: String,
    #[serde(default, rename = "as")]
    pub symbol: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconcileStep {
    pub actor: String,
    #[serde(default)]
    pub replica: Option<String>,
    #[serde(rename = "as")]
    pub symbol: String,
    pub affected_proposition: String,
    pub common_ancestor: String,
    pub resolution_mode: String,
    #[serde(default)]
    pub selected_revision: Option<String>,
    #[serde(default)]
    pub result_revision: Option<String>,
    #[serde(default)]
    pub resolved_tips: Vec<String>,
    pub conflicts: Vec<ReconciliationConflictStep>,
    pub markdown: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconciliationConflictStep {
    pub revision: String,
    pub deliberation: String,
    pub settlement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SemanticCorrectionStep {
    pub actor: String,
    #[serde(default)]
    pub replica: Option<String>,
    pub proposition: String,
    pub previous_revision: String,
    #[serde(rename = "as")]
    pub symbol: String,
    pub markdown: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetryMissingDependencyStep {
    pub from: String,
    pub to: String,
    #[serde(default)]
    pub ledger: Option<String>,
    pub object: String,
    #[serde(default)]
    pub missing_dependency_kind: Option<String>,
    #[serde(default, rename = "as")]
    pub symbol: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InitReplicaStep {
    pub actor: String,
    pub replica: String,
    pub ledger: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordDispositionStep {
    pub coordinator: String,
    pub object: String,
    pub disposition: String,
    #[serde(default)]
    pub classification: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(rename = "as")]
    pub symbol: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RebuildStep {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CliCheckStep {
    #[serde(default = "default_cli_binary_env")]
    pub fact_binary_env: String,
    #[serde(default)]
    pub expected_pending_actions: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GrantStep {
    pub ledger: String,
    pub actor: String,
    pub capabilities: Vec<String>,
    #[serde(default = "default_true")]
    pub propagate: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RotateKeyStep {
    pub actor: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncStep {
    pub from: String,
    pub to: String,
    #[serde(default)]
    pub ledger: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParallelStep {
    pub branches: Vec<ParallelBranch>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParallelBranch {
    #[serde(default)]
    pub replica: Option<String>,
    #[serde(default)]
    pub steps: Vec<Step>,
}

fn default_cli_binary_env() -> String {
    "FACT_BINARY".to_string()
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommentStep {
    pub actor: String,
    #[serde(default)]
    pub replica: Option<String>,
    pub proposition: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParticipantStep {
    pub actor: String,
    #[serde(default)]
    pub replica: Option<String>,
    pub proposition: String,
    #[serde(default)]
    pub participant: Option<String>,
    #[serde(default)]
    pub invitation: Option<String>,
    #[serde(default, rename = "as")]
    pub symbol: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InvitationLifecycleStep {
    pub actor: String,
    #[serde(default)]
    pub replica: Option<String>,
    pub invitation: String,
    pub operation: String,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default, rename = "as")]
    pub symbol: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PropositionRefStep {
    pub actor: Option<String>,
    #[serde(default)]
    pub replica: Option<String>,
    pub proposition: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdvanceTimeStep {
    #[serde(default)]
    pub seconds: i64,
    #[serde(default)]
    pub minutes: i64,
    #[serde(default)]
    pub hours: i64,
    #[serde(default)]
    pub days: i64,
}

impl AdvanceTimeStep {
    pub fn duration(&self) -> time::Duration {
        time::Duration::seconds(self.seconds)
            + time::Duration::minutes(self.minutes)
            + time::Duration::hours(self.hours)
            + time::Duration::days(self.days)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum Assertion {
    EffectiveRevision {
        effective_revision: EffectiveRevisionAssertion,
    },
    Status {
        status: StatusAssertion,
    },
    LatestContent {
        latest_content: LatestContentAssertion,
    },
    ObjectCount {
        object_count: ObjectCountAssertion,
    },
    LatestRevision {
        latest_revision: RevisionRefAssertion,
    },
    RevisionStatus {
        revision_status: RevisionStatusAssertion,
    },
    PendingActionCount {
        pending_action_count: PendingActionCountAssertion,
    },
    ProjectionRebuildEquivalent {
        projection_rebuild_equivalent: ProjectionRebuildEquivalentAssertion,
    },
    CanonicalHistoryUnchanged {
        canonical_history_unchanged: CanonicalHistoryUnchangedAssertion,
    },
    Conflict {
        conflict: ConflictAssertion,
    },
    DecisionConflict {
        decision_conflict: DecisionConflictAssertion,
    },
    DeliberationConflict {
        deliberation_conflict: DeliberationConflictAssertion,
    },
    DerivedRevision {
        derived_revision: DerivedRevisionAssertion,
    },
    Reconciliation {
        reconciliation: ReconciliationAssertion,
    },
    ReconciliationConflict {
        reconciliation_conflict: ReconciliationConflictAssertion,
    },
    SemanticCorrection {
        semantic_correction: SemanticCorrectionAssertion,
    },
    Retry {
        retry: RetryAssertion,
    },
    CoordinatorDisposition {
        coordinator_disposition: CoordinatorDispositionAssertion,
    },
    InvitationRace {
        invitation_race: InvitationRaceAssertion,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EffectiveRevisionAssertion {
    pub proposition: String,
    pub equals: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StatusAssertion {
    pub proposition: String,
    pub equals: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LatestContentAssertion {
    pub proposition: String,
    pub contains: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectCountAssertion {
    pub equals: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RevisionRefAssertion {
    pub proposition: String,
    pub equals: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RevisionStatusAssertion {
    pub revision: String,
    pub equals: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingActionCountAssertion {
    pub actor: String,
    pub equals: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProjectionRebuildEquivalentAssertion {
    pub equals: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CanonicalHistoryUnchangedAssertion {
    pub equals: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConflictAssertion {
    pub proposition: String,
    pub conflict_type: String,
    #[serde(default)]
    pub branch_tips: Vec<String>,
    pub last_undisputed_ancestor: String,
    #[serde(default)]
    pub reconciliation_required: bool,
    #[serde(default)]
    pub no_arbitrary_winner: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DecisionConflictAssertion {
    pub proposition: String,
    #[serde(default)]
    pub participant: Option<String>,
    pub equals: bool,
    #[serde(default)]
    pub decision_tips: Vec<String>,
    #[serde(default)]
    pub superseded_by: Option<String>,
    #[serde(default)]
    pub applicable_decision_count: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeliberationConflictAssertion {
    pub proposition: String,
    pub revision: String,
    pub equals: bool,
    #[serde(default)]
    pub deliberations: Vec<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DerivedRevisionAssertion {
    pub revision: String,
    pub parent: String,
    pub derived_from: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconciliationAssertion {
    pub proposition: String,
    pub affected_proposition: String,
    pub common_ancestor: String,
    pub resolution_mode: String,
    #[serde(default)]
    pub selected_revision: Option<String>,
    #[serde(default)]
    pub result_revision: Option<String>,
    #[serde(default)]
    pub conflicts: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconciliationConflictAssertion {
    pub proposition: String,
    pub equals: bool,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SemanticCorrectionAssertion {
    pub correction: String,
    pub previous_revision: String,
    pub corrective_revision: String,
    pub effective_revision: String,
    #[serde(default)]
    pub preserved_history: Option<bool>,
    #[serde(default)]
    pub requires_new_signed_object: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetryAssertion {
    pub retry: String,
    #[serde(default)]
    pub first_disposition: Option<String>,
    #[serde(default)]
    pub retry_disposition: Option<String>,
    #[serde(default)]
    pub classification: Option<String>,
    #[serde(default)]
    pub missing_dependency_kind: Option<String>,
    #[serde(default)]
    pub retryable_unchanged: Option<bool>,
    #[serde(default)]
    pub duplicate_count: Option<usize>,
    #[serde(default)]
    pub converged: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoordinatorDispositionAssertion {
    pub records: Vec<String>,
    pub object: String,
    #[serde(default)]
    pub dispositions: Vec<String>,
    #[serde(default)]
    pub not_dispositions: Vec<String>,
    #[serde(default)]
    pub same_object: Option<bool>,
    #[serde(default)]
    pub different_coordinators: Option<bool>,
    #[serde(default)]
    pub canonical_unchanged: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InvitationRaceAssertion {
    pub invitation: String,
    pub join: String,
    pub lifecycle: String,
    pub operation: String,
    #[serde(default)]
    pub equals: bool,
    #[serde(default)]
    pub enrollment_occurs: Option<bool>,
    #[serde(default)]
    pub new_invitation_required: Option<bool>,
    #[serde(default)]
    pub classification: Option<String>,
}
