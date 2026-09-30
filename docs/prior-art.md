# Multi-Table Transactions: Prior Art in the Iceberg Community

Companion to the talk *Bringing Warehouse-Grade Transactions to Open Lakehouses* and to the
[design doc in the Polaris branch](https://github.com/qlong/polaris/blob/lakehouse-transactions/site/content/in-dev/unreleased/multi-table-transaction-design.md),
which formalizes the REST protocol referenced throughout.

This doc summarizes the **previous community efforts** to bring multi-table transactions to the
Apache Iceberg REST catalog — including the stalled and closed ones — and positions the Polaris
implementation on this branch against them. The point is not to claim novelty; it is to show that
every design fork the community argued over, we have had to take a concrete position on, and to make
those positions legible when we take this upstream.

Everything attributed to a thread, issue, or PR below was read from the source (GitHub / the ASF
mailing-list archive), not paraphrased from memory. Links are inline.

> **Note on the design docs.** The **current consolidated proposal** — Singh/Sidhu's
> [*Multi-Table/Statement Transactions in Iceberg*](https://docs.google.com/document/d/1jr4Ah8oceOmo6fwxG_0II4vKDUHUKScb/edit?usp=sharing)
> (Google Doc, publicly viewable) — is available and is the authoritative source for the Option 1 /
> Option 2 framing below. The **older** per-effort design docs (linked from PR #6948 and issue
> #10617) have since been **deleted** and no longer resolve; where a claim could only have come from
> one of those, it is reconstructed from the durable public record (GitHub issue/PR text and the
> mailing-list thread bodies) and the dead links are retained in Sources as historical citation only.

---

## 0. The one distinction that organizes everything

The single most important framing — and the thesis of the talk — comes from **Eduard Tudenhöfner
(nastra)** on issue [#10617](https://github.com/apache/iceberg/issues/10617):

> Iceberg REST today already has a **multi-table *commit*** — one stateless `POST /v1/{prefix}/transactions/commit`
> that atomically swaps a *set of already-prepared* metadata pointers. It has **no isolation
> semantics, no begin, no server-side state.**
>
> A **multi-table *transaction*** is the other thing: a `begin → read at isolation → write → commit`
> lifecycle, with the server holding state across calls.

Almost all the confusion in the prior threads collapses once you hold these two apart. The community
*shipped* the first years ago (`commitTransaction` in the REST spec). The second is what everyone has
been failing to converge on since 2023. **Polaris on this branch implements the second.**

---

## 1. Timeline of the prior efforts

| When | Effort | Driver | Outcome |
|---|---|---|---|
| 2023-02 | PR [#6948](https://github.com/apache/iceberg/pull/6948) — *Core: Add Catalog Transactions API* | nastra | Prototype; **closed 2026-05-29** unmerged |
| 2024-07 | Issue [#10617](https://github.com/apache/iceberg/issues/10617) — *Add Multi-Table Transaction API* | jackye1995 | **Open**, stalled Dec 2024 |
| 2025-06 | Dev-list [DISCUSS] thread `r5otylsrm4txd4oxyv7c6scdwrbolck9` | Maninderjit Singh / Jagdeep Sidhu | Active mid-2025; converged on "opaque commit IDs" direction, no PR yet |
| (earlier) | Dev-list thread `q7vgnfwdxng5q6mq45m0psghzy7553r7` — first TransactionContext discussion | — | Dropped ("chatty protocol") |

The through-line: **three attempts, three different server-state models, none merged.** The debate
was never about *whether* to do it — it was about **how much state the catalog server must hold** and
**what the client must know about the server's internal ordering.**

---

## 2. Effort A — nastra's Catalog Transactions API (PR #6948)

**What it was.** A client-side `Transaction createTransaction(...)` abstraction in Iceberg core, with
the catalog made transaction-aware. (Its design doc — a Google doc linked from the PR — has since
been deleted; the account below comes from the PR text and code, not the doc.)

**Where it landed.** Opened 2023-02-27, **closed unmerged 2026-05-29** at RussellSpitzer's request
after ~2 years dormant. ajantha-bhat offered to pick it up (2025-01-13); it didn't restart.

**The most valuable artifact in it** is jackye1995's 2023-07-03 offline-discussion note on
**isolation and phantom reads**, which every later design (ours included) has to answer:

- Snapshot / serializable isolation does **not** require that all tables in a transaction be pinned
  to a single consistent instant *at transaction-start time*. That is a stronger guarantee than the
  isolation level actually demands.
- **Phantom / non-repeatable reads** only become observable in two cases: (1) a self-join, or (2)
  reading the *same table twice* inside one multi-statement transaction.
- Engines already cache table metadata for the duration of a query, so single-statement reads are
  fine without any server pinning. Multi-statement transactions are exactly where the transaction API
  earns its keep.

**How it relates to ours.** This analysis is the justification for our **`read_point` cutoff** design
(see the design doc's visibility section). We give the transaction one
read-point and let every table resolve against it — precisely the "pin the transaction, not each
table independently" model jackye1995 argued was sufficient. Where we go further than the prototype:
we make the read-point a *server-managed, per-catalog `commit_scn` cutoff* rather than a client-held
snapshot set, which closes the assign-before-visible window the naive design leaves open.

---

## 3. Effort B — the "Add Multi-Table Transaction API" issue (#10617)

**What it was.** jackye1995's 2024 umbrella issue to standardize the transaction lifecycle in the
REST spec, reusing the #6948 design doc (now deleted).

**Where it landed.** Stalled December 2024. nastra (2025-07-03) stated he had no resources to drive it
and pointed the community at the newer dev-list thread (Effort C). Practical scope narrowed to
**REST / Nessie**; making arbitrary catalogs (Hive, Glue, JDBC) transaction-aware was treated as a
non-goal.

**How it relates to ours.** The scoping decision is the same one we made independently:

- **Our transactions are catalog-scoped**, everything under `/v1/{prefix}/…`. Cross-catalog
  transactions are an explicit non-goal.
- **We implement the server, not a generic core abstraction.** The prototype tried to add a
  `Transaction` type to Iceberg *core* so every catalog could opt in. We took the pragmatic REST-first
  path #10617 converged on: implement it once, in the catalog server, behind the REST protocol, and
  let OpenAPI clients drive it. That is exactly what the design doc formalizes.

---

## 4. Effort C — the 2025 dev-list DISCUSS thread (the live one)

Thread: [`[DISCUSS] Multi-statement multi-transaction proposal for Apache REST Catalog`](https://lists.apache.org/thread/r5otylsrm4txd4oxyv7c6scdwrbolck9),
started 2025-06-17 by **Maninderjit Singh**; main interlocutors **Jagdeep Sidhu** and
**Dmitri Bourlatchkov (dimas-b)**. The consolidated-proposal doc it centered on is
[publicly available](https://docs.google.com/document/d/1jr4Ah8oceOmo6fwxG_0II4vKDUHUKScb/edit?usp=sharing)
(the link originally posted to the thread has since died, but the doc itself is still shared) — and this
thread is in any case the most self-contained of the three, because the interesting design argument
(Bourlatchkov's counter-proposal, §4.2) played out *in the email bodies themselves*, which survive in
the archive.

This is the most important prior effort because it's the most recent, it's *server-side aware*, and it
argued out the exact tradeoff we resolved in code.

### 4.1 The proposal (Singh / Sidhu)

Merge two earlier competing approaches into one:

1. **Catalog-authored timestamps** — the catalog stamps every commit with a server-authored time.
2. **Monotonic sequence numbers (CSN)** — a per-catalog commit-sequence-number the client reads and
   carries to order reads/writes across tables.

(Both approaches, and their tradeoff analysis, are detailed in the consolidated proposal doc linked
above.) The proposal deliberately **omitted the server-side implementation** ("to allow
each vendor to choose the best option," Singh 2025-06-19), presenting the client protocol plus a
tradeoff analysis.

### 4.2 Bourlatchkov's pushback and counter-proposal — "opaque commit IDs"

Dmitri Bourlatchkov (2025-06-19, expanded 2025-06-25) made two objections and a counter-design whose
opaque-handle idea **is exactly our read path** (though not our write path — see §5.2):

- **Objection 1 — a client-visible monotonic sequence number has deep implications for catalog
  implementations.** Forcing every catalog to expose a monotonic CSN over the wire constrains the
  storage layer hard.
- **Objection 2 — the proposal under-specifies the server side.** He wanted the server impact
  considered in depth, since the change is two-sided.
- **Counter-design — opaque commit IDs, no `TransactionContext`.** The catalog produces a commit ID
  for every change and exposes it as *an opaque short string*. Clients echo it back to pin reads and
  to declare a "base" for conflict detection; they never learn what's inside it. Crucially: **servers
  keep no in-progress transaction state** — the existing multi-table-commit mechanism is extended to
  also mint and resolve commit IDs.

His worked flow (verbatim shape from the thread):

```
1. Load table A            → server returns metadata + commit ID C1
2. Load table B  (pass C1) → consistent with C1
3. Load table C  (pass C1)
4..6. process / update A, B locally
7. Commit A,B with base=C1, declare C as "read-but-not-changed"
8. Server checks conflicts between C1 and current state (incl. C unchanged)
9. Commit → returns C2   (conflict → returns C3 = current state; client re-reads at C3, retries)
```

### 4.3 Why the community leaned stateless — but kept the stateful option

Two things are easy to conflate here, so keep them apart.

**The older, *eager* `TransactionContext` drew a real critique.** On the older thread
`q7vgnfwdxng5q6mq45m0psghzy7553r7`, Jagdeep Sidhu (2025-06-19/23) argued that an engine which doesn't
yet know a statement will become multi-table would have to register a context with the catalog and
later delete it — adding round trips to *simple reads* ("chatty"), plus complex server-side state.
That critique was aimed at the **eager register-everything** shape.

**But the current proposal doc does not drop the stateful alternative — and its recommendation is not
a settled decision.** The doc presents *both* approaches — Option 1 (stateless CatalogSequenceNumber)
and Option 2 (stateful TransactionContext) — as, in its own words, "viable paths for implementing
multi-table transactional behavior," and then states a *preference* for Option 1 as the simpler,
stateless interface. But the doc's own comment threads show the choice is still open, not closed:
reviewers are still arguing the mechanism (should the ordering value be an exposed sequence number or
an opaque ID? should transaction state live in the engine or the catalog?), the Option-1 preference
itself rests on informal *community-sync feedback* rather than a written resolution, and the thread
explicitly defers the API — "we can talk about Java API changes after we agree on an approach." As
late as **late 2025** the stateful-vs-stateless axis is still being debated in the margins. So the
accurate summary is: the community *leans* stateless on paper, but the decision is **left open,
pending a demonstration** — which is exactly the gap a working PoC is meant to close. Option 2, the
durable server-side model, is a live alternative, not an abandoned one.

(This matters for §5.2: our write path is a **lazy** stateful design — a read opens no transaction at
all — so Sidhu's "chatty" critique of the eager model does not apply to it.)

---

## 5. How Polaris relates: same forks, concrete positions

The Polaris branch is, in effect, one worked answer to every question these threads left open. Side
by side:

| Community question | Prior-effort positions | Polaris on this branch |
|---|---|---|
| Client-side core abstraction, or server-side REST? | #6948 tried core; #10617 narrowed to REST | **Server-side REST**, OpenAPI-drivable, no engine-specific read plugin required (see design doc) |
| Does the client see the internal ordering number? | Singh/Sidhu: yes (CSN over the wire). Bourlatchkov: no (opaque commit ID) | **No — opaque handles.** Client holds `txn_token`, `txn_snapshot_id`, `subtxn_token`, `op_token`; never sees `commit_scn` or `read_point` (see design doc) |
| Must every table be pinned to one instant at begin? | jackye1995: not required by the isolation level | **One `read_point` cutoff** per transaction, resolved per table — matches jackye1995's analysis |
| Durable server-side transaction state, or stateless? | Proposal doc: both viable; **recommends** stateless Option 1 over stateful Option 2 | **Hybrid.** Reads are stateless (opaque handle); *writes* keep durable server state — see §5.2 |
| Conflict model | Bourlatchkov: base commit ID + "read-but-not-changed" set | **Per-op OCC**: merge-and-retry for appends; first-committer-wins (HTTP 409) for destructive ops; lock check in `doCommit` for *all* commits |

### 5.1 Where we agree with the mainstream direction

- **Opaque handles, not value-carrying.** Our `txn_snapshot_id` is exactly Bourlatchkov's "opaque
  commit ID": the client echoes it back to pin reads, and the underlying `read_point` never crosses
  the wire. The design doc's classification test ("would two independent clients need to
  agree on it?") is the same interoperability lens the dev-list thread used to decide CSN-vs-opaque.
- **REST-first, catalog-scoped, no core changes required.** Aligned with #10617's practical scope.
- **The commit-vs-transaction distinction** (nastra) is load-bearing in our design: our
  `commitTransaction` reuses the atomic multi-pointer swap that the *commit* endpoint already had, and
  layers the lifecycle and isolation on top — which is precisely Bourlatchkov's "extend the existing
  multi-table-commit mechanism" prescription.

### 5.2 Where we diverge — and why it's a hybrid, not a copy

The community's leaning as of mid-2025 was **stateless** (opaque handles, no durable per-transaction
server object) — a *preference*, not a settled decision (§4.3). This design is a **hybrid**, and the
two halves have different origins — worth stating plainly, because "we built Option 2" would be wrong
in both directions:

- **Read path — stateless, borrowed.** A consistent read does **not** open a transaction. It uses an
  opaque, server-minted handle carrying a `read_point` cutoff — the client echoes it back and never
  sees the internal ordering number. That is the opaque-handle spirit the community preferred
  (Bourlatchkov's counter-proposal, Option 1's thinking). Reads add no per-transaction server state.
- **Write / coordination path — durable, arrived at independently.** Multi-table *writes* do keep
  server-side state (`transactions`, `subtransactions`, staging, locks, a reaper). This converges with
  Option 2 (TransactionContext) — but it was built clean-room and reconciled afterward, so the
  convergence is **evidence that the boundary is real, not a copy of the proposal.**

**Why the write path needs the state — the one structural reason.** A stateless "carry the base commit
ID" client protocol assumes a *single* client assembling the whole write set locally before commit.
**Fan-out write breaks that assumption:** independent workers — different processes, even different
machines — each stage part of one transaction. That requires a rendezvous every worker can reach, which
a client process is not. The catalog is. So the durable state isn't overhead bolted onto a stateless
design — it is the capability that lets a transaction span independent workers at all. The read path
stays stateless precisely because reads don't need that rendezvous.

The full round-trip accounting, lock-mode matrix, and conflict model are in the
[design doc](https://github.com/qlong/polaris/blob/lakehouse-transactions/site/content/in-dev/unreleased/multi-table-transaction-design.md).

### 5.3 Reconciling with the community

This design was built clean-room — deliberately without reading the prior proposals first, so it
would be an independent take on what cross-engine multi-table transactions actually require. It was
reconciled against the dev-list discussion and the proposal doc afterward. That reconciliation
changed the design in one concrete way: the read path was revised to the **stateless opaque handle**
the community preferred, replacing an earlier per-read server row. The write-side coordination was
kept as-is — it was arrived at independently *and* is load-bearing (it's what fan-out write needs).
So the hybrid isn't an accident of taste: the read half was aligned *toward* the community on
purpose, and the write half converges with Option 2 as independent evidence, not as a copy.

The community *leaned* toward **stateless opaque commit IDs** (a preference, not a closed decision —
§4.3). We are a **stateful** implementation, and §5.2 is why: fan-out write needs a server-side rendezvous. When this goes
upstream, that is the conversation to have head-on — but the reconciliation path is clean, because we
serve the opaque-handle *protocol* the community prefers **without** forcing the client to know we're
stateful behind it. The handle is the contract; the coordinator, the durable state, and the lock
manager sit *behind the handles* and never cross the wire (which is exactly where the design
doc draws the line). A future stateless server could implement the same read protocol differently — it
just couldn't offer distributed fan-out staging, because that capability *is* the state.

---

## Sources

- Issue [apache/iceberg#10617](https://github.com/apache/iceberg/issues/10617) — *Add Multi-Table Transaction API*
- PR [apache/iceberg#6948](https://github.com/apache/iceberg/pull/6948) — *Core: Add Catalog Transactions API* (closed)
- Dev-list [`r5otylsrm4txd4oxyv7c6scdwrbolck9`](https://lists.apache.org/thread/r5otylsrm4txd4oxyv7c6scdwrbolck9) — 2025 DISCUSS thread (10 messages, read in full)
- Dev-list [`q7vgnfwdxng5q6mq45m0psghzy7553r7`](https://lists.apache.org/thread/q7vgnfwdxng5q6mq45m0psghzy7553r7) — earlier *eager* TransactionContext thread (superseded by the current proposal's Option 2)
- Current consolidated proposal (Google Doc, **publicly viewable**) — Singh/Sidhu,
  [*Multi-Table/Statement Transactions in Iceberg*](https://docs.google.com/document/d/1jr4Ah8oceOmo6fwxG_0II4vKDUHUKScb/edit?usp=sharing).
  The authoritative source for the Option 1 / Option 2 framing above.
- Older design docs (Google) — **deleted; links retained as historical citation only, not readable:**
  #6948 design (`docs.google.com/document/d/1UxXifU8iqP_byaW4E2RuKZx1nobxmAvc5urVcWas1B8`),
  the earlier proposal export linked from the dev-list thread (`drive.google.com/open?id=1KVgUJc1WgftHfLz118vMbEE7HV8_pUDk4s-GJFDyAOE`),
  partial metadata loading (`docs.google.com/document/d/1eXnT0ZiFvdm_Zvk6fLGT_UxVWO-HsiqVywqu1Uk8s7E`)
- Ours: the [design doc in the Polaris branch](https://github.com/qlong/polaris/blob/lakehouse-transactions/site/content/in-dev/unreleased/multi-table-transaction-design.md)
