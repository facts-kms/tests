use anyhow::{Context, Result, bail};
use fact_sim_core::{PropositionStatus, SimulationContext};

pub fn assert_status(context: &SimulationContext, proposition: &str, expected: &str) -> Result<()> {
    let actual = &context
        .propositions
        .get(proposition)
        .with_context(|| format!("proposition `{proposition}` does not exist"))?
        .status;
    let expected = match expected {
        "pending" => PropositionStatus::Pending,
        "accepted" => PropositionStatus::Accepted,
        "rejected" => PropositionStatus::Rejected,
        "withdrawn" => PropositionStatus::Withdrawn,
        "archived" => PropositionStatus::Archived,
        other => bail!("unknown expected proposition status `{other}`"),
    };
    if *actual != expected {
        bail!("expected `{proposition}` status {expected:?}, got {actual:?}");
    }
    Ok(())
}

pub fn assert_latest_content_contains(
    context: &SimulationContext,
    proposition: &str,
    needle: &str,
) -> Result<()> {
    let latest = context
        .propositions
        .get(proposition)
        .with_context(|| format!("proposition `{proposition}` does not exist"))?
        .latest_revision()
        .context("proposition has no latest revision")?;
    if !latest.markdown.contains(needle) {
        bail!("latest revision for `{proposition}` does not contain `{needle}`");
    }
    Ok(())
}

pub fn assert_effective_revision_symbol(
    context: &SimulationContext,
    proposition: &str,
    expected_symbol: &str,
) -> Result<()> {
    let proposition_state = context
        .propositions
        .get(proposition)
        .with_context(|| format!("proposition `{proposition}` does not exist"))?;
    let effective = proposition_state
        .effective_revision()
        .context("proposition has no effective revision")?;
    let expected = match expected_symbol {
        name if name == proposition => proposition_state.revisions.first(),
        "latest" => proposition_state.revisions.last(),
        _ => proposition_state.revisions.last(),
    }
    .context("expected revision does not exist")?;
    if effective.id != expected.id {
        bail!(
            "expected effective revision `{expected_symbol}` for `{proposition}`, got {}",
            effective.id
        );
    }
    Ok(())
}

pub fn assert_object_count(context: &SimulationContext, expected: usize) -> Result<()> {
    let actual = context.counts.total();
    if actual != expected {
        bail!("expected {expected} objects, got {actual}");
    }
    Ok(())
}
