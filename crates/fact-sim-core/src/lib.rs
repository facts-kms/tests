use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result, bail};
use rand::RngCore;
use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

pub trait Clock {
    fn now(&self) -> OffsetDateTime;
}

pub trait IdSource {
    fn next_uuid(&mut self) -> Uuid;
}

pub trait RandomSource {
    fn next_u64(&mut self) -> u64;
}

#[derive(Debug, Clone)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

#[derive(Debug, Clone)]
pub struct SimClock {
    current: Arc<RwLock<OffsetDateTime>>,
    transitions: Arc<RwLock<Vec<TimeTransition>>>,
}

impl SimClock {
    pub fn new(start: OffsetDateTime) -> Self {
        Self {
            current: Arc::new(RwLock::new(start)),
            transitions: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn advance(&self, duration: time::Duration) -> Result<()> {
        let mut current = self
            .current
            .write()
            .map_err(|_| anyhow::anyhow!("simulation clock lock poisoned"))?;
        let from = *current;
        let to = from
            .checked_add(duration)
            .context("simulation clock advance overflowed")?;
        *current = to;
        self.transitions
            .write()
            .map_err(|_| anyhow::anyhow!("simulation clock transition lock poisoned"))?
            .push(TimeTransition { from, to });
        Ok(())
    }

    pub fn set(&self, value: OffsetDateTime) -> Result<()> {
        let mut current = self
            .current
            .write()
            .map_err(|_| anyhow::anyhow!("simulation clock lock poisoned"))?;
        let from = *current;
        *current = value;
        self.transitions
            .write()
            .map_err(|_| anyhow::anyhow!("simulation clock transition lock poisoned"))?
            .push(TimeTransition { from, to: value });
        Ok(())
    }

    pub fn transitions(&self) -> Result<Vec<TimeTransition>> {
        Ok(self
            .transitions
            .read()
            .map_err(|_| anyhow::anyhow!("simulation clock transition lock poisoned"))?
            .clone())
    }
}

impl Clock for SimClock {
    fn now(&self) -> OffsetDateTime {
        *self.current.read().expect("simulation clock lock poisoned")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TimeTransition {
    #[serde(with = "time::serde::rfc3339")]
    pub from: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub to: OffsetDateTime,
}

#[derive(Debug, Clone)]
pub struct DeterministicIdSource {
    rng: ChaCha20Rng,
}

impl DeterministicIdSource {
    pub fn from_seed(seed: u64) -> Self {
        Self {
            rng: ChaCha20Rng::seed_from_u64(seed),
        }
    }
}

impl IdSource for DeterministicIdSource {
    fn next_uuid(&mut self) -> Uuid {
        let mut bytes = [0_u8; 16];
        self.rng.fill_bytes(&mut bytes);
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }
}

#[derive(Debug, Clone)]
pub struct DeterministicRandomSource {
    rng: ChaCha20Rng,
}

impl DeterministicRandomSource {
    pub fn from_seed(seed: u64) -> Self {
        Self {
            rng: ChaCha20Rng::seed_from_u64(seed),
        }
    }
}

impl RandomSource for DeterministicRandomSource {
    fn next_u64(&mut self) -> u64 {
        self.rng.next_u64()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Actor {
    pub id: Uuid,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Ledger {
    pub id: Uuid,
    pub name: String,
    pub owner: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Revision {
    pub id: Uuid,
    pub markdown: String,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    pub accepted_by: Vec<String>,
    pub rejected_by: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PropositionStatus {
    Pending,
    Accepted,
    Rejected,
    Withdrawn,
    Archived,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Proposition {
    pub id: Uuid,
    pub name: String,
    pub ledger: String,
    pub author: String,
    pub revisions: Vec<Revision>,
    pub effective_revision: Option<Uuid>,
    pub status: PropositionStatus,
    pub comments: Vec<Comment>,
    pub participants: Vec<String>,
}

impl Proposition {
    pub fn latest_revision(&self) -> Option<&Revision> {
        self.revisions.last()
    }

    pub fn effective_revision(&self) -> Option<&Revision> {
        let id = self.effective_revision?;
        self.revisions.iter().find(|revision| revision.id == id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Comment {
    pub id: Uuid,
    pub actor: String,
    pub message: String,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct ObjectCounts {
    pub actors: usize,
    pub ledgers: usize,
    pub propositions: usize,
    pub revisions: usize,
    pub decisions: usize,
    pub comments: usize,
}

impl ObjectCounts {
    pub fn total(&self) -> usize {
        self.actors
            + self.ledgers
            + self.propositions
            + self.revisions
            + self.decisions
            + self.comments
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum CoordinatorDisposition {
    Accepted,
    RejectedProtocolInvalid,
    RejectedUnauthorized,
    RejectedPolicy,
    RejectedMissingDependency,
    Deferred,
    Quarantined,
    RemovedLocal,
    Unknown,
}

impl CoordinatorDisposition {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::RejectedProtocolInvalid => "rejected-protocol-invalid",
            Self::RejectedUnauthorized => "rejected-unauthorized",
            Self::RejectedPolicy => "rejected-policy",
            Self::RejectedMissingDependency => "rejected-missing-dependency",
            Self::Deferred => "deferred",
            Self::Quarantined => "quarantined",
            Self::RemovedLocal => "removed-local",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum FailureClassification {
    RetryableUnchanged,
    RequiresNewSignedObject,
}

impl FailureClassification {
    pub fn same_signed_object_may_be_retried(&self) -> bool {
        matches!(self, Self::RetryableUnchanged)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum FailureCase {
    MissingDependency,
    TemporarilyUnsupportedProtocolVersion,
    TemporaryCoordinatorUnavailability,
    DeferredProcessing,
    QuarantinePendingReview,
    LocalPolicyRejectionMayChange,
    TimeUncertainty,
    TemporarilyUnavailableKeyOrAttestationEvidence,
    UnknownLedgerNotInitializedLocally,
    RevisionConflict,
    DeliberationConflict,
    DecisionConflict,
    ReconciliationRequirement,
    InvitationRace,
    StaleLineage,
    InvalidParentReference,
    AbsentAuthorizationAtCausalPoint,
    ExhaustedInvitation,
    InvalidCanonicalContent,
    InvalidSignature,
    AttemptedMutationOfExistingSignedObject,
}

impl FailureCase {
    pub fn classification(&self) -> FailureClassification {
        match self {
            Self::MissingDependency
            | Self::TemporarilyUnsupportedProtocolVersion
            | Self::TemporaryCoordinatorUnavailability
            | Self::DeferredProcessing
            | Self::QuarantinePendingReview
            | Self::LocalPolicyRejectionMayChange
            | Self::TimeUncertainty
            | Self::TemporarilyUnavailableKeyOrAttestationEvidence
            | Self::UnknownLedgerNotInitializedLocally => FailureClassification::RetryableUnchanged,
            Self::RevisionConflict
            | Self::DeliberationConflict
            | Self::DecisionConflict
            | Self::ReconciliationRequirement
            | Self::InvitationRace
            | Self::StaleLineage
            | Self::InvalidParentReference
            | Self::AbsentAuthorizationAtCausalPoint
            | Self::ExhaustedInvitation
            | Self::InvalidCanonicalContent
            | Self::InvalidSignature
            | Self::AttemptedMutationOfExistingSignedObject => {
                FailureClassification::RequiresNewSignedObject
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunManifest {
    pub generator_version: String,
    pub facts_sdk_version: String,
    #[serde(default = "default_scheduler_version")]
    pub scheduler_version: String,
    pub seed: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub started_at: OffsetDateTime,
    pub scenario_count: usize,
    pub object_count: usize,
    pub object_counts_by_type: ObjectCounts,
    pub ledger_ids: Vec<Uuid>,
    #[serde(default)]
    pub character_ids: BTreeMap<String, Uuid>,
    #[serde(default)]
    pub character_key_ids: BTreeMap<String, Uuid>,
    #[serde(default)]
    pub character_environments: BTreeMap<String, Vec<String>>,
    pub output_database: Option<String>,
    pub project_git_commit: Option<String>,
    pub facts_git_commit: Option<String>,
    pub rust_version: Option<String>,
    pub scenario_corpus_version: String,
    pub configuration_digest: Option<String>,
    pub final_commitment_root: Option<String>,
    #[serde(default)]
    pub conflict_repair_report: ConflictRepairManifestReport,
}

fn default_scheduler_version() -> String {
    "serial-v0".to_string()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConflictRepairManifestReport {
    pub conflict_counts_by_class: BTreeMap<String, usize>,
    pub reconciliation_counts_by_mode: BTreeMap<String, usize>,
    pub retry_counts_by_disposition: BTreeMap<String, usize>,
    pub repair_counts: BTreeMap<String, usize>,
    pub fixture_locations: BTreeMap<String, String>,
    pub assertion_results: BTreeMap<String, usize>,
}

#[derive(Debug, Clone)]
pub struct SimulationContext {
    pub clock: SimClock,
    pub id_source: DeterministicIdSource,
    pub random_source: DeterministicRandomSource,
    pub actors: BTreeMap<String, Actor>,
    pub ledgers: BTreeMap<String, Ledger>,
    pub propositions: BTreeMap<String, Proposition>,
    pub counts: ObjectCounts,
}

impl SimulationContext {
    pub fn new(seed: u64, start: OffsetDateTime) -> Self {
        Self {
            clock: SimClock::new(start),
            id_source: DeterministicIdSource::from_seed(seed),
            random_source: DeterministicRandomSource::from_seed(seed ^ 0x5eed),
            actors: BTreeMap::new(),
            ledgers: BTreeMap::new(),
            propositions: BTreeMap::new(),
            counts: ObjectCounts::default(),
        }
    }

    pub fn create_actor(&mut self, name: impl Into<String>) -> Result<Uuid> {
        let name = name.into();
        if self.actors.contains_key(&name) {
            bail!("actor `{name}` already exists");
        }
        let id = self.id_source.next_uuid();
        self.actors.insert(name.clone(), Actor { id, name });
        self.counts.actors += 1;
        Ok(id)
    }

    pub fn create_ledger(
        &mut self,
        name: impl Into<String>,
        owner: impl Into<String>,
    ) -> Result<Uuid> {
        let name = name.into();
        let owner = owner.into();
        if !self.actors.contains_key(&owner) {
            bail!("ledger owner `{owner}` is not a declared actor");
        }
        if self.ledgers.contains_key(&name) {
            bail!("ledger `{name}` already exists");
        }
        let id = self.id_source.next_uuid();
        self.ledgers
            .insert(name.clone(), Ledger { id, name, owner });
        self.counts.ledgers += 1;
        Ok(id)
    }

    pub fn propose(
        &mut self,
        actor: impl Into<String>,
        ledger: impl Into<String>,
        symbol: impl Into<String>,
        markdown: impl Into<String>,
    ) -> Result<Uuid> {
        let actor = actor.into();
        let ledger = ledger.into();
        let symbol = symbol.into();
        if !self.actors.contains_key(&actor) {
            bail!("proposal actor `{actor}` is not declared");
        }
        if !self.ledgers.contains_key(&ledger) {
            bail!("proposal ledger `{ledger}` is not declared");
        }
        if self.propositions.contains_key(&symbol) {
            bail!("proposition symbol `{symbol}` already exists");
        }
        let proposition_id = self.id_source.next_uuid();
        let revision_id = self.id_source.next_uuid();
        let revision = Revision {
            id: revision_id,
            markdown: markdown.into(),
            created_at: self.clock.now(),
            accepted_by: Vec::new(),
            rejected_by: Vec::new(),
        };
        self.propositions.insert(
            symbol.clone(),
            Proposition {
                id: proposition_id,
                name: symbol,
                ledger,
                author: actor.clone(),
                revisions: vec![revision],
                effective_revision: None,
                status: PropositionStatus::Pending,
                comments: Vec::new(),
                participants: vec![actor],
            },
        );
        self.counts.propositions += 1;
        self.counts.revisions += 1;
        Ok(proposition_id)
    }

    pub fn revise(
        &mut self,
        actor: impl Into<String>,
        proposition: impl Into<String>,
        markdown: impl Into<String>,
    ) -> Result<Uuid> {
        let actor = actor.into();
        if !self.actors.contains_key(&actor) {
            bail!("revision actor `{actor}` is not declared");
        }
        let proposition = self
            .propositions
            .get_mut(&proposition.into())
            .context("proposition reference is unknown")?;
        let revision_id = self.id_source.next_uuid();
        proposition.revisions.push(Revision {
            id: revision_id,
            markdown: markdown.into(),
            created_at: self.clock.now(),
            accepted_by: Vec::new(),
            rejected_by: Vec::new(),
        });
        proposition.status = PropositionStatus::Pending;
        self.counts.revisions += 1;
        Ok(revision_id)
    }

    pub fn accept(
        &mut self,
        actor: impl Into<String>,
        proposition: impl Into<String>,
    ) -> Result<Uuid> {
        self.decide(actor, proposition, true)
    }

    pub fn reject(
        &mut self,
        actor: impl Into<String>,
        proposition: impl Into<String>,
    ) -> Result<Uuid> {
        self.decide(actor, proposition, false)
    }

    fn decide(
        &mut self,
        actor: impl Into<String>,
        proposition: impl Into<String>,
        accepted: bool,
    ) -> Result<Uuid> {
        let actor = actor.into();
        if !self.actors.contains_key(&actor) {
            bail!("decision actor `{actor}` is not declared");
        }
        let proposition = self
            .propositions
            .get_mut(&proposition.into())
            .context("proposition reference is unknown")?;
        let revision = proposition
            .revisions
            .last_mut()
            .context("proposition has no revision")?;
        let decision_id = self.id_source.next_uuid();
        if accepted {
            if !revision.accepted_by.contains(&actor) {
                revision.accepted_by.push(actor);
            }
            proposition.effective_revision = Some(revision.id);
            proposition.status = PropositionStatus::Accepted;
        } else {
            if !revision.rejected_by.contains(&actor) {
                revision.rejected_by.push(actor);
            }
            proposition.status = PropositionStatus::Rejected;
        }
        self.counts.decisions += 1;
        Ok(decision_id)
    }

    pub fn comment(
        &mut self,
        actor: impl Into<String>,
        proposition: impl Into<String>,
        message: impl Into<String>,
    ) -> Result<Uuid> {
        let actor = actor.into();
        if !self.actors.contains_key(&actor) {
            bail!("comment actor `{actor}` is not declared");
        }
        let proposition = self
            .propositions
            .get_mut(&proposition.into())
            .context("proposition reference is unknown")?;
        let id = self.id_source.next_uuid();
        proposition.comments.push(Comment {
            id,
            actor,
            message: message.into(),
            created_at: self.clock.now(),
        });
        self.counts.comments += 1;
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coordinator_dispositions_serialize_as_protocol_vocabulary() {
        let dispositions = vec![
            CoordinatorDisposition::Accepted,
            CoordinatorDisposition::RejectedProtocolInvalid,
            CoordinatorDisposition::RejectedUnauthorized,
            CoordinatorDisposition::RejectedPolicy,
            CoordinatorDisposition::RejectedMissingDependency,
            CoordinatorDisposition::Deferred,
            CoordinatorDisposition::Quarantined,
            CoordinatorDisposition::RemovedLocal,
            CoordinatorDisposition::Unknown,
        ];
        let encoded = serde_json::to_value(&dispositions).unwrap();
        assert_eq!(
            encoded,
            serde_json::json!([
                "accepted",
                "rejected-protocol-invalid",
                "rejected-unauthorized",
                "rejected-policy",
                "rejected-missing-dependency",
                "deferred",
                "quarantined",
                "removed-local",
                "unknown"
            ])
        );
    }

    #[test]
    fn failure_classification_records_retry_contract() {
        assert!(FailureClassification::RetryableUnchanged.same_signed_object_may_be_retried());
        assert!(
            !FailureClassification::RequiresNewSignedObject.same_signed_object_may_be_retried()
        );
    }

    #[test]
    fn required_failure_cases_are_classified_by_retry_contract() {
        let retryable = [
            FailureCase::MissingDependency,
            FailureCase::TemporarilyUnsupportedProtocolVersion,
            FailureCase::TemporaryCoordinatorUnavailability,
            FailureCase::DeferredProcessing,
            FailureCase::QuarantinePendingReview,
            FailureCase::LocalPolicyRejectionMayChange,
            FailureCase::TimeUncertainty,
            FailureCase::TemporarilyUnavailableKeyOrAttestationEvidence,
            FailureCase::UnknownLedgerNotInitializedLocally,
        ];
        for case in retryable {
            assert_eq!(
                case.classification(),
                FailureClassification::RetryableUnchanged
            );
            assert!(case.classification().same_signed_object_may_be_retried());
        }

        let requires_new_object = [
            FailureCase::RevisionConflict,
            FailureCase::DeliberationConflict,
            FailureCase::DecisionConflict,
            FailureCase::ReconciliationRequirement,
            FailureCase::InvitationRace,
            FailureCase::StaleLineage,
            FailureCase::InvalidParentReference,
            FailureCase::AbsentAuthorizationAtCausalPoint,
            FailureCase::ExhaustedInvitation,
            FailureCase::InvalidCanonicalContent,
            FailureCase::InvalidSignature,
            FailureCase::AttemptedMutationOfExistingSignedObject,
        ];
        for case in requires_new_object {
            assert_eq!(
                case.classification(),
                FailureClassification::RequiresNewSignedObject
            );
            assert!(!case.classification().same_signed_object_may_be_retried());
        }
    }
}
