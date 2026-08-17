# Fact Tests

Deterministic test, simulation, fixture, and benchmark harness for Facts
protocol journeys.

The project executes scenario files through a small YAML DSL, records deterministic
run manifests, and keeps SDK-scale simulation separate from CLI user-experience
checks. Objects must be created through Facts operations; fixture code must not
insert arbitrary database rows.

## Quick Start

```sh
cargo check-all
cargo test-all
cargo sim suite run scenarios/smoke
```

The default local and CI path is intentionally small: Rust checks, unit tests,
and smoke scenarios. Scenario runs start from empty generated workspaces under
`target/fact-sim-runs/`, create ledgers through `fact-sdk`, verify SDK state,
rebuild projections, and run selected `fact` CLI checks against generated
ledgers.

Generated artifacts are disposable. Use `cargo sim cleanup --dry-run` to inspect
simulator workspaces and `cargo sim cleanup` to remove them. Generated fixtures,
SQLite databases, benchmark matrices, reports, `tmp/`, and Cargo `target/`
directories are ignored by Git; source generators, scenario definitions, tests,
and docs are the versioned assets.

## Configuration

The Facts SDK dependencies are declared once in the workspace manifest. In the
split-repository layout, this workspace expects the Facts SDK checkout to live
beside this repository at `../sdk`.

CLI UX tests resolve the `fact` binary from `FACT_BINARY`; when using the
standard sibling checkout layout, build the CLI repository beside this one at
`../cli`:

```sh
cargo build --manifest-path ../cli/Cargo.toml --bin fact
FACT_BINARY=../cli/target/debug/fact cargo sim cli -- --help
```

## Fixtures and Suites

The vertical slice is one deterministic scenario where an accepted proposition is
revised, the original revision remains effective while the newer revision is
pending, the revision is accepted, and SDK state can be compared with CLI output.

The multi-actor corpus generates a portable small-organization corpus with Alice,
Bob, and Carol, plus propositions, revisions, comments, invitations,
join/leave participant changes, decisions, lifecycle events, and capability
grant/revocation history. Recreate and verify it with:

```sh
cargo sim generate --profile multi-actor-10k --seed 42 --output fixtures/multi-actor-10k
cargo sim verify fixtures/multi-actor-10k
```

The generated fixture contains `ledger.sqlite`, `objects.factbndl`, and
`manifest.json`. The manifest reports object distributions, deterministic replay,
projection rebuild equivalence, sampled CLI receipts, and known SDK/CLI gaps.

The synchronization corpus covers distributed synchronization. Its profile name
is `sync-100k`. It covers multiple ledgers, shared replicas, named
remotes, delayed dependency delivery, duplicate delivery, key lifecycle objects,
capability revocation, and convergence evidence.

```sh
cargo sim verify fixtures/sync-100k
```

The conflict and repair corpus is the conflict, reconciliation, retry, and repair
checkpoint. It is generated with the canonical `conflict-repair`
profile:

```sh
cargo sim generate --profile conflict-repair --seed 42 --output fixtures/conflict-repair
cargo sim verify fixtures/conflict-repair
cargo sim report fixtures/conflict-repair
```

The conflict and repair fixture manifest records replica topology, synchronization transfers,
object counts, conflict counts, reconciliation modes, retry and repair evidence,
projection rebuild checks, performance timings, and unresolved protocol behavior
observed in the current SDK. Existing `sync-100k` fixtures with the
conflict-repair report schema remain verifiable as a legacy alias.

The scale fixture workflow keeps the historical `scale-500k-*` profile names for
compatibility, but 500K is no longer the primary project goal. Full
500,000-proposition fixtures are manual scale and benchmark inputs, not part of
the default test suite:

```sh
cargo sim plan --all --seed 42
cargo sim plan --profile scale-500k-balanced --seed 42
cargo sim generate --profile scale-500k-balanced --seed 42 --output fixtures/scale-500k-balanced-seed-42
cargo sim inspect fixtures/scale-500k-balanced-seed-42
cargo sim verify fixtures/scale-500k-balanced-seed-42
```

Available scale fixture workflow profiles include `scale-500k-balanced`,
`scale-500k-proposition-heavy`, `scale-500k-revision-heavy`,
`scale-500k-deliberation-heavy`, `scale-500k-sync-heavy`, and
`scale-500k-conflict-heavy`. The dedicated `scale-500k-proposition-bulk`
profile is the manual SQLite sizing fixture for the 500K proposition target.
Generated scale fixtures report the actual target, overshoot, object
distribution, executed scenario-family counts, portable bundle inventory,
snapshot inventory, and object-set commitment proofs where applicable.

To reproduce a failure, rerun the same scenario with the seed shown in the
manifest. The current fixture uses `seed: 42`; failures include scenario name,
seed, step number, operation, expected result, actual result, symbolic references,
and protocol IDs.

Scale profiles and benchmarks are documented under `docs/`.

The benchmark suite adds reproducible benchmark workflows. Run these only when
the required generated fixture set is already present, or when intentionally
creating it on a machine with enough disk and time:

```sh
cargo sim benchmark spec
cargo sim benchmark plan --suite full --fixture-base fixtures/benchmark-matrix --report-output reports/benchmarks
cargo sim benchmark baseline --suite full --base fixtures --output reports/benchmarks --require-ready
cargo sim benchmark audit --base fixtures --baseline-summary reports/benchmarks/baseline-summary.json
cargo sim benchmark analyze --baseline-summary reports/benchmarks/baseline-summary.json
cargo sim benchmark budgets --baseline-summary reports/benchmarks/baseline-summary.json
cargo sim benchmark check-budgets --budgets reports/benchmarks/budgets.json --baseline-summary reports/benchmarks/current-summary.json
cargo sim benchmark profile-plan --baseline-summary reports/benchmarks/baseline-summary.json
cargo sim benchmark report reports/benchmarks/read.json
cargo sim benchmark report reports/benchmarks/growth-analysis.json
cargo sim benchmark report reports/benchmarks/budgets.json
cargo sim benchmark report reports/benchmarks/budget-check.json
cargo sim benchmark report reports/benchmarks/profile-plan.json
cargo sim benchmark report reports/benchmarks/comparison.json
cargo sim benchmark compare reports/benchmarks/baseline.json reports/benchmarks/current.json
cargo sim benchmark compare --thresholds reports/benchmarks/thresholds.json reports/benchmarks/baseline.json reports/benchmarks/current.json
cargo sim benchmark compare-matrix reports/benchmarks/baseline-summary.json reports/benchmarks/current-summary.json
cargo sim benchmark fixtures --base fixtures
```

Manual large benchmark runs are available through workflow dispatch in
`.github/workflows/benchmarks.yml` for self-hosted benchmark runners.

The fault suite adds deterministic fault and recovery profiles. The normal fault
suite targets the small and medium levels; the `faults-500k-mixed` profile is
manual opt-in only:

```sh
cargo sim fault spec
cargo sim fault run --profile faults-projection --seed 9001 --output reports/faults/projection-9001.json
cargo sim fault replay reports/faults/projection-9001.json
cargo sim fault recover reports/faults/projection-9001.json
cargo sim fault verify reports/faults/projection-9001.json
cargo sim fault report reports/faults/projection-9001.json
cargo sim fault run --profile faults-500k-mixed --seed 9001 --include-large
```

The CLI UX suite adds deterministic CLI UX validation against the actual `fact`
binary. The normal UX suite targets small and medium scale; sampled 500K UX runs
require `--include-large`:

```sh
cargo sim ux spec
cargo sim ux run --suite smoke --seed 8008 --output reports/ux/smoke.json
cargo sim ux replay reports/ux/smoke.json
cargo sim ux report reports/ux/smoke.json
cargo sim ux compare reports/ux/baseline.json reports/ux/current.json
```

## License

MIT
