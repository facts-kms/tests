use fact_sdk::environment::LedgerEntry;
use fact_sdk::proposition::{
    ContentSelection, DecisionOutcome, accept_proposition, create_proposition,
    read_proposition_content, read_proposition_content_with_selection, update_proposition_content,
};
use fact_sdk::workflow::{BootstrapLedgerInput, create_ledger};

fn sdk_entry() -> (tempfile::TempDir, LedgerEntry, [u8; 32]) {
    let temp = tempfile::tempdir().expect("tempdir");
    let database = temp.path().join("sdk-journey.sqlite");
    let seed = [31; 32];
    let store = fact_store::Store::open(&database).expect("open store");
    let bootstrap = create_ledger(
        &store,
        BootstrapLedgerInput {
            namespace: "local.fact-sim-sdk-journey".into(),
            created_at: "2026-01-05T09:00:00.000Z".into(),
            seed,
            nonce: [17; 16],
        },
    )
    .expect("create ledger");
    let seed_file = temp.path().join("seed");
    (
        temp,
        LedgerEntry {
            name: "sdk-journey".into(),
            ledger_id: bootstrap.ledger_id,
            database,
            actor_id: bootstrap.actor_id,
            key_id: bootstrap.key_id,
            seed_file,
            read_only: false,
        },
        seed,
    )
}

#[test]
fn sdk_keeps_accepted_revision_effective_until_pending_revision_is_accepted() {
    let (_temp, entry, seed) = sdk_entry();
    let created = create_proposition(
        &entry,
        &seed,
        b"# Deployment policy\n\nDeployments require peer review.\n",
        Some(DecisionOutcome::Accepted),
    )
    .expect("create accepted proposition");
    let reference = created.proposition_id.to_string();

    let revised = update_proposition_content(
        &entry,
        &seed,
        &reference,
        b"# Deployment policy\n\nDeployments require peer review and rollback instructions.\n",
    )
    .expect("create pending revision");
    assert_eq!(revised.status, "pending");
    assert_eq!(revised.previous_revision_id, Some(created.revision_id));
    assert_eq!(revised.previous_revision_effective, Some(true));

    let effective_before_accept =
        read_proposition_content(&entry, &reference).expect("read effective content");
    assert_eq!(effective_before_accept.revision_id, created.revision_id);
    assert_eq!(
        effective_before_accept.content,
        b"# Deployment policy\n\nDeployments require peer review.\n"
    );

    let pending =
        read_proposition_content_with_selection(&entry, &reference, ContentSelection::Pending)
            .expect("read pending content");
    assert_eq!(pending.revision_id, revised.revision_id);

    let accepted = accept_proposition(&entry, &seed, Some(&reference)).expect("accept revision");
    assert_eq!(accepted.status, "accepted");

    let effective_after_accept =
        read_proposition_content(&entry, &reference).expect("read accepted content");
    assert_eq!(effective_after_accept.revision_id, revised.revision_id);
    assert_eq!(
        effective_after_accept.content,
        b"# Deployment policy\n\nDeployments require peer review and rollback instructions.\n"
    );
}
