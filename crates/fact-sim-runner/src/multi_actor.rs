use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use fact_sdk::discussion::{
    create_comment_with_runtime, join_deliberation_with_runtime, leave_deliberation_with_runtime,
};
use fact_sdk::environment::{LedgerEntry, UserEnvironment};
use fact_sdk::identity::{
    create_identity_grant_with_runtime, export_identity, import_identity,
    revoke_identity_grant_with_runtime,
};
use fact_sdk::invitation::create_invitation_with_runtime;
use fact_sdk::lifecycle::{archive_proposition_with_runtime, withdraw_proposition_with_runtime};
use fact_sdk::proposition::{
    DecisionOutcome, ListPropositionStatus, ListPropositionsFilter,
    accept_proposition_with_runtime, create_proposition_with_runtime, list_propositions,
    pending_propositions, reject_proposition_with_runtime, update_proposition_content_with_runtime,
};
use fact_sdk::runtime::DeterministicRuntime;
use fact_sdk::state::rebuild_state;
use fact_sdk::sync::encode_bundle;
use fact_sdk::workflow::{BootstrapLedgerInput, create_ledger_with_runtime};
use fact_sim_core::{Clock, DeterministicRandomSource, RandomSource, SimClock};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{CliReceipt, SdkStateSnapshot, protocol_hashes_for_database};

const TARGET_OBJECTS: usize = 10_000;
const WORKFLOW_OBJECTS: usize = 300;

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiActorReport {
    pub profile: String,
    pub seed: u64,
    pub output: PathBuf,
    pub database: PathBuf,
    pub bundle: PathBuf,
    pub snapshot: Option<PathBuf>,
    pub commitment_root: Option<String>,
    pub object_count: usize,
    pub object_counts_by_type: BTreeMap<String, usize>,
    pub actor_count: usize,
    pub actors: Vec<String>,
    pub generated_instances: usize,
    pub logical_digest: MultiActorDigest,
    pub assertion_report: AssertionReport,
    pub cli_sample_report: Vec<CliReceipt>,
    pub sdk_gaps: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MultiActorDigest {
    pub object_counts_by_type: BTreeMap<String, usize>,
    pub accepted_count: usize,
    pub pending_count: usize,
    pub rejected_count: usize,
    pub archived_count: usize,
    pub withdrawn_count: usize,
    pub revision_count: usize,
    pub comment_count: usize,
    pub invitation_count: usize,
    pub participant_change_count: usize,
    pub delegation_count: usize,
    pub revocation_count: usize,
    pub lifecycle_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssertionReport {
    pub projection_rebuild_equivalent: bool,
    pub deterministic_logical_replay: bool,
    pub deterministic_byte_replay: bool,
    pub search_effective_content: bool,
    pub lifecycle_search_excludes_withdrawn: bool,
    pub cli_samples_checked: usize,
}

#[derive(Debug)]
struct World {
    clock: SimClock,
    runtime: DeterministicRuntime,
    random: DeterministicRandomSource,
    fact_home: PathBuf,
    environment: UserEnvironment,
    actors: BTreeMap<String, ActorEntry>,
    propositions: Vec<GeneratedProposition>,
    comments: usize,
    invitations: usize,
    participant_changes: usize,
    delegations: usize,
    revocations: usize,
    lifecycle: usize,
}

#[derive(Debug, Clone)]
struct ActorEntry {
    entry: LedgerEntry,
    seed: [u8; 32],
}

#[derive(Debug, Clone)]
struct GeneratedProposition {
    proposition_id: Uuid,
    latest_revision_id: Uuid,
    body_keyword: String,
    withdrawn: bool,
    archived: bool,
    accepted: bool,
}

pub fn generate_multi_actor(options: GenerateOptions) -> Result<MultiActorReport> {
    if options.profile != "multi-actor-10k" {
        bail!(
            "unsupported profile `{}`; expected multi-actor-10k",
            options.profile
        );
    }
    if options.output.exists() {
        bail!(
            "output directory `{}` already exists; remove it or choose another path",
            options.output.display()
        );
    }

    let mut world = World::new(options.seed)?;
    world.bootstrap_actors()?;
    world.generate_until(TARGET_OBJECTS)?;

    let before = world.snapshot()?;
    let store = fact_store::Store::open(&world.admin().entry.database)?;
    rebuild_state(&store)?;
    let after = world.snapshot()?;
    let projection_rebuild_equivalent = before == after;
    if !projection_rebuild_equivalent {
        bail!("projection rebuild did not preserve logical state");
    }

    let (deterministic_logical_replay, deterministic_byte_replay) = {
        let mut replay = World::new(options.seed)?;
        replay.bootstrap_actors()?;
        replay.generate_until(TARGET_OBJECTS)?;
        (
            world.digest()? == replay.digest()?,
            protocol_hashes_for_database(&world.admin().entry.database)?
                == protocol_hashes_for_database(&replay.admin().entry.database)?,
        )
    };
    if !deterministic_logical_replay {
        bail!("same seed did not reproduce logical corpus digest");
    }
    if !deterministic_byte_replay {
        bail!("same seed did not reproduce canonical object bytes");
    }

    let search_effective_content =
        world.assert_cli_search_effective_content(options.fact_binary.as_deref())?;
    let lifecycle_search_excludes_withdrawn =
        world.lifecycle_search_excludes_withdrawn(options.fact_binary.as_deref())?;
    let cli_sample_report = world.run_cli_samples(options.fact_binary.as_deref())?;

    fs::create_dir_all(&options.output)?;
    let database = options.output.join("ledger.sqlite");
    fs::copy(&world.admin().entry.database, &database)?;
    let ledger_id = Uuid::parse_str(&world.admin().entry.ledger_id)?;
    let bundle_objects = all_protocol_objects(&world.admin().entry.database)?;
    let bundle_bytes = encode_bundle(ledger_id, &bundle_objects)?;
    let bundle = options.output.join("objects.factbndl");
    fs::write(&bundle, bundle_bytes)?;
    let snapshot = None;
    let commitment_root = fact_sdk::commitment::create_commitment(
        bundle_objects.iter().map(|(hash, _)| *hash).collect(),
    )
    .ok()
    .map(|commitment| commitment.root);

    let digest = world.digest()?;
    let report = MultiActorReport {
        profile: options.profile,
        seed: options.seed,
        output: options.output.clone(),
        database,
        bundle,
        snapshot,
        commitment_root,
        object_count: digest.object_counts_by_type.values().sum(),
        object_counts_by_type: digest.object_counts_by_type.clone(),
        actor_count: world.actors.len(),
        actors: world.actors.keys().cloned().collect(),
        generated_instances: world.propositions.len(),
        logical_digest: digest,
        assertion_report: AssertionReport {
            projection_rebuild_equivalent,
            deterministic_logical_replay,
            deterministic_byte_replay,
            search_effective_content,
            lifecycle_search_excludes_withdrawn,
            cli_samples_checked: cli_sample_report.len(),
        },
        cli_sample_report,
        sdk_gaps: Vec::new(),
    };
    fs::write(
        options.output.join("manifest.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    Ok(report)
}

pub fn verify_multi_actor_fixture(fixture: &Path) -> Result<MultiActorReport> {
    let manifest = fixture.join("manifest.json");
    let report: MultiActorReport = serde_json::from_slice(
        &fs::read(&manifest).with_context(|| format!("failed to read `{}`", manifest.display()))?,
    )
    .with_context(|| format!("failed to parse `{}`", manifest.display()))?;
    if report.profile != "multi-actor-10k" {
        bail!(
            "fixture profile `{}` is not multi-actor-10k",
            report.profile
        );
    }
    if report.seed != 42 {
        bail!("fixture seed is {}, expected 42", report.seed);
    }
    if report.object_count < TARGET_OBJECTS {
        bail!(
            "fixture has {} protocol objects, expected at least {}",
            report.object_count,
            TARGET_OBJECTS
        );
    }
    for path in [&report.database, &report.bundle] {
        if !path.exists() {
            bail!("fixture artifact `{}` is missing", path.display());
        }
    }
    let database_counts = all_protocol_object_counts(&report.database)?;
    let database_object_count: usize = database_counts.values().sum();
    if database_object_count != report.object_count {
        bail!(
            "database object count {} does not match manifest {}",
            database_object_count,
            report.object_count
        );
    }
    let required_types = [
        "authorization_grant",
        "authorization_revocation",
        "decision",
        "deliberation_comment",
        "deliberation_participant_change",
        "participant_invitation",
        "proposition",
        "proposition_lifecycle",
        "revision",
        "settlement",
    ];
    for object_type in required_types {
        if database_counts
            .get(object_type)
            .copied()
            .unwrap_or_default()
            == 0
        {
            bail!("fixture is missing `{object_type}` protocol objects");
        }
    }
    if report.actor_count < 3 {
        bail!(
            "fixture has {} actors, expected at least 3",
            report.actor_count
        );
    }
    if !report.assertion_report.projection_rebuild_equivalent {
        bail!("manifest did not record projection rebuild equivalence");
    }
    if !report.assertion_report.deterministic_logical_replay {
        bail!("manifest did not record deterministic logical replay");
    }
    if !report.assertion_report.deterministic_byte_replay {
        bail!("manifest did not record deterministic byte replay");
    }
    if !report.assertion_report.search_effective_content {
        bail!("manifest did not record effective content search coverage");
    }
    if !report.assertion_report.lifecycle_search_excludes_withdrawn {
        bail!("manifest did not prove lifecycle search excludes withdrawn propositions");
    }
    Ok(report)
}

impl World {
    fn new(seed: u64) -> Result<Self> {
        let workspace = run_workspace("multi-actor-10k", seed)?;
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
            "2026-01-05T09:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )?;
        Ok(Self {
            clock: SimClock::new(start),
            runtime: DeterministicRuntime::new(format!("multi-actor-10k:{seed}"), start),
            random: DeterministicRandomSource::from_seed(seed),
            fact_home,
            environment,
            actors: BTreeMap::new(),
            propositions: Vec::new(),
            comments: 0,
            invitations: 0,
            participant_changes: 0,
            delegations: 0,
            revocations: 0,
            lifecycle: 0,
        })
    }

    fn bootstrap_actors(&mut self) -> Result<()> {
        let alice = self.create_standalone_ledger("default", "alice")?;
        self.environment.set_active("default")?;
        self.actors.insert("alice".to_string(), alice);
        for actor in ["bob", "carol"] {
            let entry = self.create_standalone_ledger(actor, actor)?;
            let exported = export_identity(&entry.entry)?;
            import_identity(&self.admin().entry, &exported.bundle)
                .with_context(|| format!("import identity for `{actor}` into main ledger"))?;
            self.actors.insert(actor.to_string(), entry);
        }
        Ok(())
    }

    fn create_standalone_ledger(
        &mut self,
        ledger_name: &str,
        actor_name: &str,
    ) -> Result<ActorEntry> {
        let seed = self.next_seed();
        let nonce = self.next_nonce();
        let database = self
            .environment
            .ledger_dir
            .join(format!("{ledger_name}.sqlite"));
        let store = fact_store::Store::open(&database)?;
        self.sync_runtime_clock()?;
        let bootstrap = create_ledger_with_runtime(
            &store,
            BootstrapLedgerInput {
                namespace: format!("local.{actor_name}"),
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
            name: ledger_name.to_string(),
            ledger_id: bootstrap.ledger_id,
            database,
            actor_id: bootstrap.actor_id,
            key_id: bootstrap.key_id,
            seed_file,
            read_only: false,
        };
        let mut catalog = self.environment.load()?;
        if ledger_name == "default" {
            catalog.insert(ledger_name.to_string(), entry.clone());
            self.environment.save(&catalog)?;
        }
        Ok(ActorEntry { entry, seed })
    }

    fn generate_until(&mut self, target_objects: usize) -> Result<()> {
        let bob_id = self.actor_uuid("bob")?;
        let carol_id = self.actor_uuid("carol")?;
        self.sync_runtime_clock()?;
        let bob_grant = create_identity_grant_with_runtime(
            &self.admin().entry,
            &self.admin().seed,
            &bob_id.to_string(),
            &["comment".to_string(), "invite".to_string()],
            &self.runtime,
        )
        .context("create identity grant for bob")?;
        self.delegations += 1;
        self.sync_runtime_clock()?;
        let carol_grant = create_identity_grant_with_runtime(
            &self.admin().entry,
            &self.admin().seed,
            &carol_id.to_string(),
            &["comment".to_string()],
            &self.runtime,
        )
        .context("create identity grant for carol")?;
        self.delegations += 1;

        let mut instance = 0usize;
        while self.object_count()? < WORKFLOW_OBJECTS {
            self.generate_instance(instance)?;
            instance += 1;
        }
        self.fill_bootstrap_objects(target_objects)?;
        self.sync_runtime_clock()?;
        revoke_identity_grant_with_runtime(
            &self.admin().entry,
            &self.admin().seed,
            &bob_grant.grant_id.to_string(),
            "corpus coverage",
            &self.runtime,
        )
        .context("revoke identity grant for bob")?;
        self.revocations += 1;
        self.sync_runtime_clock()?;
        revoke_identity_grant_with_runtime(
            &self.admin().entry,
            &self.admin().seed,
            &carol_grant.grant_id.to_string(),
            "corpus coverage",
            &self.runtime,
        )
        .context("revoke identity grant for carol")?;
        self.revocations += 1;
        Ok(())
    }

    fn fill_bootstrap_objects(&mut self, target_objects: usize) -> Result<()> {
        let database = self.admin().entry.database.clone();
        let store = fact_store::Store::open(&database)?;
        let mut index = 0usize;
        while self.object_count()? < target_objects {
            let seed = self.next_seed();
            let nonce = self.next_nonce();
            self.sync_runtime_clock()?;
            create_ledger_with_runtime(
                &store,
                BootstrapLedgerInput {
                    namespace: format!("local.multi_actor.filler.{index:05}"),
                    created_at: sdk_timestamp(self.clock.now()),
                    seed,
                    nonce,
                },
                &self.runtime,
            )
            .with_context(|| format!("create filler ledger bootstrap {index}"))?;
            index += 1;
        }
        Ok(())
    }

    fn generate_instance(&mut self, index: usize) -> Result<()> {
        let family = [
            "deployment-policy",
            "incident-followup",
            "operating-procedure",
            "product-requirement",
            "meeting-conclusion",
            "engineering-decision",
        ][index % 6];
        let keyword = format!("{family}-{index:04}");
        let initial = format!(
            "# {} {}\n\n{} requires owner review and explicit rollout notes.\n",
            title(family),
            index,
            keyword
        );
        let participant_case = index.is_multiple_of(20);
        let decision = match (participant_case, index % 9) {
            (true, _) => None,
            (_, 1) => Some(DecisionOutcome::Rejected),
            _ => Some(DecisionOutcome::Accepted),
        };
        self.sync_runtime_clock()?;
        let created = create_proposition_with_runtime(
            &self.admin().entry,
            &self.admin().seed,
            initial.as_bytes(),
            decision,
            &self.runtime,
        )
        .with_context(|| format!("create proposition instance {index}"))?;
        let mut generated = GeneratedProposition {
            proposition_id: created.proposition_id,
            latest_revision_id: created.revision_id,
            body_keyword: keyword.clone(),
            withdrawn: false,
            archived: false,
            accepted: decision == Some(DecisionOutcome::Accepted),
        };

        if index.is_multiple_of(3) && !participant_case {
            let revised_content = format!(
                "# {} {}\n\n{} now requires two reviewers and rollback evidence.\n",
                title(family),
                index,
                keyword
            );
            self.sync_runtime_clock()?;
            let revised = update_proposition_content_with_runtime(
                &self.admin().entry,
                &self.admin().seed,
                &created.proposition_id.to_string(),
                revised_content.as_bytes(),
                &self.runtime,
            )
            .with_context(|| format!("revise proposition instance {index}"))?;
            generated.latest_revision_id = revised.revision_id;
            if index.is_multiple_of(6) {
                self.sync_runtime_clock()?;
                accept_proposition_with_runtime(
                    &self.admin().entry,
                    &self.admin().seed,
                    Some(&created.proposition_id.to_string()),
                    &self.runtime,
                )
                .with_context(|| format!("accept revised proposition instance {index}"))?;
                generated.accepted = true;
            } else {
                self.sync_runtime_clock()?;
                reject_proposition_with_runtime(
                    &self.admin().entry,
                    &self.admin().seed,
                    Some(&created.proposition_id.to_string()),
                    &self.runtime,
                )
                .with_context(|| format!("reject revised proposition instance {index}"))?;
                generated.accepted = false;
            }
        }

        if index.is_multiple_of(4) {
            self.sync_runtime_clock()?;
            create_comment_with_runtime(
                &self.admin().entry,
                &self.admin().seed,
                &created.proposition_id.to_string(),
                format!("# Follow-up\n\nReview note for {keyword}.\n").as_bytes(),
                &self.runtime,
            )
            .with_context(|| format!("comment on proposition instance {index}"))?;
            self.comments += 1;
        }

        if index.is_multiple_of(20) {
            self.sync_runtime_clock()?;
            let invitation = create_invitation_with_runtime(
                &self.admin().entry,
                &self.admin().seed,
                &created.proposition_id.to_string(),
                &self.actor_uuid("bob")?.to_string(),
                &self.runtime,
            )
            .with_context(|| format!("invite bob for proposition instance {index}"))?;
            self.invitations += 1;
            let bob = self.actor_entry_for_main_ledger("bob")?;
            self.sync_runtime_clock()?;
            join_deliberation_with_runtime(
                &bob.entry,
                &bob.seed,
                &created.proposition_id.to_string(),
                &invitation.invitation_id.to_string(),
                &self.runtime,
            )
            .with_context(|| format!("bob joins proposition instance {index}"))?;
            self.participant_changes += 1;
            self.sync_runtime_clock()?;
            leave_deliberation_with_runtime(
                &bob.entry,
                &bob.seed,
                &created.proposition_id.to_string(),
                &self.runtime,
            )
            .with_context(|| format!("bob leaves proposition instance {index}"))?;
            self.participant_changes += 1;
            self.sync_runtime_clock()?;
            accept_proposition_with_runtime(
                &self.admin().entry,
                &self.admin().seed,
                Some(&created.proposition_id.to_string()),
                &self.runtime,
            )
            .with_context(|| format!("accept participant lifecycle instance {index}"))?;
            generated.accepted = true;
        }

        if index != 0 && index.is_multiple_of(15) {
            self.sync_runtime_clock()?;
            archive_proposition_with_runtime(
                &self.admin().entry,
                &self.admin().seed,
                &created.proposition_id.to_string(),
                "archive sample",
                &self.runtime,
            )?;
            generated.archived = true;
            self.lifecycle += 1;
        }
        if index != 0 && index.is_multiple_of(25) {
            self.sync_runtime_clock()?;
            withdraw_proposition_with_runtime(
                &self.admin().entry,
                &self.admin().seed,
                &created.proposition_id.to_string(),
                "withdrawal sample",
                &self.runtime,
            )?;
            generated.withdrawn = true;
            self.lifecycle += 1;
        }

        if index.is_multiple_of(10) {
            self.clock.advance(time::Duration::minutes(5))?;
        }
        self.propositions.push(generated);
        Ok(())
    }

    fn run_cli_samples(&self, fact_binary: Option<&Path>) -> Result<Vec<CliReceipt>> {
        let fact_binary = fact_binary
            .map(Path::to_path_buf)
            .unwrap_or_else(default_fact_binary_path);
        if !fact_binary.exists() {
            bail!("fact binary `{}` does not exist", fact_binary.display());
        }
        let sample = self
            .propositions
            .iter()
            .find(|item| item.accepted && !item.withdrawn && !item.archived)
            .context("no accepted active proposition available for CLI sample")?;
        let reference = sample.proposition_id.to_string();
        let commands = [
            vec!["list"],
            vec!["--json", "list"],
            vec!["revisions", &reference],
            vec!["--json", "revisions", &reference],
            vec!["echo", &reference],
            vec!["pending"],
            vec!["--json", "pending"],
            vec!["search", &sample.body_keyword],
            vec!["--json", "search", &sample.body_keyword],
            vec!["history", &reference],
            vec!["--json", "history", &reference],
        ];
        let mut receipts = Vec::new();
        for command in commands {
            receipts.push(self.run_fact_command(&fact_binary, &command)?);
        }
        Ok(receipts)
    }

    fn run_fact_command(&self, fact_binary: &Path, args: &[&str]) -> Result<CliReceipt> {
        let started = Instant::now();
        let output = Command::new(fact_binary)
            .args(args)
            .env("FACT_HOME", &self.fact_home)
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
        if !output.status.success() {
            bail!("fact {} failed: {}", args.join(" "), receipt.stderr);
        }
        Ok(receipt)
    }

    fn assert_cli_search_effective_content(&self, fact_binary: Option<&Path>) -> Result<bool> {
        let fact_binary = fact_binary
            .map(Path::to_path_buf)
            .unwrap_or_else(default_fact_binary_path);
        let sample = self
            .propositions
            .iter()
            .find(|item| item.accepted && !item.withdrawn && !item.archived)
            .context("no searchable proposition")?;
        let receipt = self.run_fact_command(
            &fact_binary,
            &[
                "search",
                "--status",
                "accepted",
                "--effective",
                &sample.body_keyword,
            ],
        )?;
        Ok(receipt
            .stdout
            .contains(&sample.proposition_id.to_string()[..12]))
    }

    fn lifecycle_search_excludes_withdrawn(&self, fact_binary: Option<&Path>) -> Result<bool> {
        let fact_binary = fact_binary
            .map(Path::to_path_buf)
            .unwrap_or_else(default_fact_binary_path);
        let Some(sample) = self.propositions.iter().find(|item| item.withdrawn) else {
            return Ok(true);
        };
        let receipt = self.run_fact_command(
            &fact_binary,
            &[
                "search",
                "--status",
                "accepted",
                "--effective",
                &sample.body_keyword,
            ],
        )?;
        Ok(!receipt
            .stdout
            .contains(&sample.proposition_id.to_string()[..12]))
    }

    fn digest(&self) -> Result<MultiActorDigest> {
        let object_counts_by_type = self.object_counts()?;
        let propositions = list_propositions(
            &self.admin().entry,
            ListPropositionsFilter {
                status: None,
                all: true,
            },
        )?;
        let pending = pending_propositions(&self.admin().entry)?;
        let rejected = list_propositions(
            &self.admin().entry,
            ListPropositionsFilter {
                status: Some(ListPropositionStatus::Rejected),
                all: true,
            },
        )?;
        Ok(MultiActorDigest {
            object_counts_by_type,
            accepted_count: propositions
                .iter()
                .filter(|item| item.status == "accepted")
                .count(),
            pending_count: pending.len(),
            rejected_count: rejected.len(),
            archived_count: self
                .propositions
                .iter()
                .filter(|item| item.archived)
                .count(),
            withdrawn_count: self
                .propositions
                .iter()
                .filter(|item| item.withdrawn)
                .count(),
            revision_count: self
                .object_counts()?
                .get("revision")
                .copied()
                .unwrap_or_default(),
            comment_count: self.comments,
            invitation_count: self.invitations,
            participant_change_count: self.participant_changes,
            delegation_count: self.delegations,
            revocation_count: self.revocations,
            lifecycle_count: self.lifecycle,
        })
    }

    fn snapshot(&self) -> Result<SdkStateSnapshot> {
        crate::snapshot_for_entry(&self.admin().entry)
    }

    fn object_count(&self) -> Result<usize> {
        Ok(self.object_counts()?.values().sum())
    }

    fn object_counts(&self) -> Result<BTreeMap<String, usize>> {
        all_protocol_object_counts(&self.admin().entry.database)
    }

    fn actor_entry_for_main_ledger(&self, actor: &str) -> Result<ActorEntry> {
        let actor_entry = self
            .actors
            .get(actor)
            .with_context(|| format!("unknown actor `{actor}`"))?;
        let mut entry = actor_entry.entry.clone();
        entry.name = "default".to_string();
        entry.ledger_id = self.admin().entry.ledger_id.clone();
        entry.database = self.admin().entry.database.clone();
        Ok(ActorEntry {
            entry,
            seed: actor_entry.seed,
        })
    }

    fn actor_uuid(&self, actor: &str) -> Result<Uuid> {
        self.actors
            .get(actor)
            .with_context(|| format!("unknown actor `{actor}`"))?
            .entry
            .actor_id
            .parse()
            .with_context(|| format!("actor `{actor}` id is invalid"))
    }

    fn admin(&self) -> &ActorEntry {
        self.actors.get("alice").expect("alice actor exists")
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

fn title(family: &str) -> String {
    family
        .split('-')
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

fn run_workspace(profile: &str, seed: u64) -> Result<PathBuf> {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
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

fn all_protocol_object_counts(database: &Path) -> Result<BTreeMap<String, usize>> {
    let connection = rusqlite::Connection::open(database)?;
    let mut statement = connection.prepare(
        "SELECT object_type, COUNT(*) FROM protocol_object GROUP BY object_type ORDER BY object_type",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as usize))
    })?;
    let mut counts = BTreeMap::new();
    for row in rows {
        let (object_type, count) = row?;
        counts.insert(object_type, count);
    }
    Ok(counts)
}

fn all_protocol_objects(database: &Path) -> Result<Vec<(fact_core::Hash, Vec<u8>)>> {
    let connection = rusqlite::Connection::open(database)?;
    let mut statement = connection
        .prepare("SELECT content_hash, cose FROM protocol_object ORDER BY content_hash")?;
    let rows = statement.query_map([], |row| {
        let hash_bytes: Vec<u8> = row.get(0)?;
        let cose: Vec<u8> = row.get(1)?;
        Ok((hash_bytes, cose))
    })?;
    let mut objects = Vec::new();
    for row in rows {
        let (hash_bytes, cose) = row?;
        let hash_array: [u8; 32] = hash_bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid content hash length"))?;
        objects.push((fact_core::Hash::from_bytes(hash_array), cose));
    }
    Ok(objects)
}
