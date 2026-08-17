# Interface Mapping Matrix

This matrix maps current interface surfaces to canonical operations. Operation
semantics are authoritative; interface names are adapters.

| Operation | SDK | CLI | HTTP | Simulator |
| --- | --- | --- | --- | --- |
| `CreateLedger` | `workflow::create_ledger` | `fact init`, `fact new`, `fact ledger init` | Provisional | ledger bootstrap helpers |
| `CreateActor` | `identity::create_identity` | `fact as`, `fact identity new` | Provisional | character creation |
| `ImportIdentity` | `identity::import_identity` | `fact identity import` | Provisional | none |
| `BindKey` | identity binding internals, `rotate_identity_key` | `fact identity`, `fact as` | Provisional | key rotation |
| `RotateKey` | `identity::rotate_identity_key` | `fact identity rotate` | Provisional | key rotation |
| `RecoverActorKey` | Open issue | None | None | None |
| `RetireActor` | Open issue | None | None | None |
| `InitializeReplica` | environment clone/import helpers | `fact clone`, `fact from` | `objects:pull` | replica initialization |
| `DeleteLocalLedger` | local environment/catalog helpers | `fact ledger delete` | Local-only | None |
| `GrantCapability` | `identity::create_identity_grant`, `delegation::create_delegation` | `fact as --permission`, `fact permission grant` | Provisional | named-character grant |
| `RevokeCapability` | `identity::revoke_identity_grant`, `delegation::revoke_delegation` | `fact permission revoke`, `fact identity revoke` | Provisional | key rotation/revocation scenarios |
| `DelegateAuthority` | `delegation::create_delegation` | `fact permission grant` | Provisional | named-character grant |
| `Propose` | `proposition::create_proposition` | `fact propose`; `fact write` is missing | Provisional `POST /facts/operations/propose` | `propose`, stable-fact journeys |
| `Revise` | `proposition::update_proposition_content` | `fact revise`, `fact edit` | Provisional | revision journeys |
| `CopyProposition` | provenance support plus `create_proposition` | Provisional | Provisional | None |
| `OpenDeliberation` | discussion/proposition deliberation helpers | `fact deliberation open` | Provisional | open deliberation |
| `ExtendDeliberation` | revision update workflow | `fact revise` | Provisional | collaborative revision |
| `InviteParticipant` | `invitation::create_invitation` | `fact invite`, `fact invitations` | Provisional | participant lifecycle |
| `JoinDeliberation` | `discussion::join_deliberation` | `fact join` | Provisional | participant lifecycle |
| `LeaveDeliberation` | `discussion::leave_deliberation` | `fact leave` | Provisional | participant lifecycle |
| `AddParticipant` | participant change internals | Provisional | Provisional | participant internals |
| `RemoveParticipant` | participant change internals | Provisional | Provisional | participant internals |
| `Comment` | `discussion::create_comment` | `fact comment`, `fact comments` | Provisional | comment journeys |
| `CastDecision` | `accept_proposition`, `reject_proposition`, `decision::create_decision` | `fact accept`, `fact reject` | Provisional | accept/reject journeys |
| `ResolveDecisionConflict` | `decision::create_decision` with supersession | `fact accept`, `fact reject` | Provisional | concurrent participant decisions |
| `MaterializeSettlement` | settlement internals via decision workflows | `fact accept`, `fact reject` | Provisional | settlement projection |
| `ArchiveProposition` | `lifecycle::archive_proposition` | `fact archive` | Provisional | archive journey |
| `WithdrawProposition` | `lifecycle::withdraw_proposition` | `fact withdraw` | Provisional | withdraw journey |
| `ReconcileConflict` | `create_reconciliation_proposition`, `resolve_revision_conflict` | `fact resolve`, `fact reconcile create` | Provisional | reconciliation journeys |
| `ImportObjects` | `sync::push_bundle_to_store`, store import APIs | `fact push DATABASE FILE`, `fact object import`, `fact sync retry` | `objects:push` | bundle import, retry |
| `Push` | bundle export/import helpers | `fact push`, `fact sync push` | `objects:push` | synchronization |
| `Pull` | `write_pull_bundle_from_store_with_options` | `fact pull`, `fact sync pull` | `objects:pull` | synchronization |
| `SynchronizeReplicas` | sync module workflows | `fact push`, `fact pull` | `objects:push`, `objects:pull` | synchronization journeys |
| `RebuildProjections` | `state::rebuild_state` | `fact state rebuild` | Local-only | projection repair |
| `QueryEffectiveState` | list/show/read/history APIs | `fact list`, `show`, `open`, `echo`, `revisions`, `history`; `fact read` is missing | Provisional query routes | inspect/verify/UX |
| `SearchEffectiveFacts` | search APIs | `fact search`, `fact find` | Provisional search route | search corpus |

## Noncanonical Local Operations

`DeleteLocalLedger`, local catalog edits, benchmark artifact cleanup, and
projection-table corruption used in fault tests are implementation operations.
They MUST NOT be interpreted as protocol mutations.
