<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Lakehouse Transactions

Multi-table transactions for the open lakehouse — **no torn reads, no partial writes**, without
distributed two-phase commit and without a table-format change.

Strict workloads suffer **torn multi-table reads** and **partial writes**: problems that warehouses
solved long ago and the open lakehouse has quietly left un-solved. Apache Iceberg's single-table
commit already spans engines, and the REST catalog can even apply a bundle of table changes
atomically. What's still missing is a *multi-table transaction* engines can use: atomic writes **and**
one consistent read point across tables. This is a working proof of concept for familiar multi-table, multi-statement `BEGIN` / `COMMIT`, coordinated
by the catalog and demonstrated live across **Apache Spark and Trino** on a single Apache Polaris
catalog.

> [!IMPORTANT]
> **This is an early proof of concept — not production software.** It demonstrates that the approach
> works end to end. Much hard work remains: see [Open frontiers](#open-frontiers).

## Why the catalog

In a lakehouse, a table's current version is a pointer the catalog publishes. A commit is the catalog
swapping that pointer. Data files are immutable; visibility is entirely about which pointer the
catalog advertises.

Only the catalog can flip several table pointers at once. An engine sees the catalog as a remote API
and cannot atomically swap two pointers; two engines racing to write the same table cannot referee
each other. The catalog is already the single authority over table pointers, which makes it the
natural — and really the only — place a multi-table transaction can live.

Because the coordination sits in the catalog rather than in an engine, cross-engine reach is
**inherited from Iceberg rather than bolted on**: a transaction begun by one engine is understood by
any other engine that speaks the same catalog.

## Understand the design

Three short docs, each for a different depth — read as far as you care to:

| Doc | For | What it covers |
|---|---|---|
| [**Explained**](docs/explained.md) | anyone | The *why* — the problem (torn reads, partial writes) and why today's tools fall short. Plain language, no internals. |
| [**How it works**](docs/how-it-works.md) | the technically curious | The *how* — catalog-coordinated `BEGIN`/`COMMIT`, staging, and server-side read resolution. Still high level. |
| [**Prior art**](docs/prior-art.md) | the design conversation | Where this sits against the Iceberg community's earlier multi-table transaction efforts, and where it agrees or diverges. |

For the full design (schemas, REST protocol), see the
[design doc in the Polaris branch](https://github.com/qlong/polaris/blob/lakehouse-transactions/site/content/in-dev/unreleased/multi-table-transaction-design.md).

## Demo

**[Watch the demo on YouTube](https://youtu.be/N8WxLtpk_ys)** — four short demos on one Polaris catalog,
across Spark and Trino:

1. **Isolation levels** — `READ COMMITTED` sees a torn read; `REPEATABLE READ` doesn't.
2. **Atomic two-table write** — uncommitted work is invisible at every isolation level; both tables
   appear together or not at all.
3. **Fan-out read** — Spark and Trino pinned to one shared snapshot: the same instant across two engines.
4. **Fan-out write** — one transaction split across independent workers; nothing is visible until the
   parent commits, then everything at once.

Slides from the Lakehouse Day EU 2026 talk:
[multi-table-transactions-lakehouseday2026.pdf](docs/multi-table-transactions-lakehouseday2026.pdf).

## The code

The implementation lives on branches of real forks of the upstream projects:

| Project | Role | Branch |
|---------|------|--------|
| **Apache Polaris** | Catalog coordinator — transaction verbs, staging, commit | [`qlong/polaris` @ `lakehouse-transactions`](https://github.com/qlong/polaris/tree/lakehouse-transactions) |
| **Trino** | Reader-side isolation — a small, focused patch (read-only transactions) | [`qlong/trino` @ `lakehouse-transactions`](https://github.com/qlong/trino/tree/lakehouse-transactions) |

The Spark client ships as a plugin inside the Polaris branch. It is the existing
`polaris-spark-3.5_2.13` plugin — there is no Spark 4.1-specific build; the 3.5 plugin is compatible
with the Spark 4.1.x runtime across the catalog APIs this work uses, and that is the combination the
demos run on.

> [!NOTE]
> Code is being prepared for publication and the branch links above will go live shortly before the
> talk. Follow the branch links, not the repository roots — a fork's default branch is upstream code
> and will not show this work.

## Try it yourself

Everything needed to reproduce the demos lives in the Polaris branch, under
[`demos/multi-table-transaction/`](https://github.com/qlong/polaris/tree/lakehouse-transactions/demos/multi-table-transaction).
Its [`README.md`](https://github.com/qlong/polaris/tree/lakehouse-transactions/demos/multi-table-transaction#readme)
is the authoritative setup guide — prerequisites, the jars to build, and the exact commands to bring
up PostgreSQL, a Polaris server, and the seeded demo tables. Follow it there rather than a copy here,
so the steps never drift from the scripts.

The heart of the demo is a single observation. In one Spark SQL session:

```sql
BEGIN TRANSACTION;
  INSERT INTO customers VALUES ...;
  INSERT INTO orders    VALUES ...;
COMMIT;
```

While that transaction is open, query either table from another session: neither insert is visible.
After `COMMIT`, both are. That is the whole claim, and it is the fastest way to convince yourself the
mechanism is real. For the full runnable walkthrough — real table names, terminal layout, and the
complete set of demos across Spark and Trino — see
[`DEMOS.md`](https://github.com/qlong/polaris/blob/lakehouse-transactions/demos/multi-table-transaction/DEMOS.md).

### What you need

For the single-engine Spark demo, **Polaris is the only patched component** — Iceberg and Spark are
used **as released, unmodified**. (The cross-engine demos add one more patched component, Trino; see
below.)

| | |
|---|---|
| **Apache Spark 4.1.2** | Stock distribution (`spark-4.1.2-bin-hadoop3-connect`, Scala 2.13), downloaded from Apache. No patch. |
| **Apache Iceberg runtime** | Stock released jar (`iceberg-spark-runtime-4.1_2.13:1.11.0`), downloaded from Maven Central and passed to Spark via `--jars`. No patch. |
| **Polaris** | Built from the branch above. This is the only component with changes. |
| **Docker** | Runs PostgreSQL, which Polaris uses to store transaction state. |

So there is exactly one thing to build from source. Every jar path in `spark-env.sh` is overridable if
you would rather point at builds you already have.

The Iceberg jar is a plain download — grab it into the location `spark-env.sh` expects by default:

```bash
mkdir -p ~/demo-jars
curl -L -o ~/demo-jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.11.0/iceberg-spark-runtime-4.1_2.13-1.11.0.jar
```

The scripts pass it to Spark with `--jars`; you do not copy it into Spark's `jars/` directory.

Trino is optional and only needed for the cross-engine demos; it is the second patched component.

Further demos in that directory go beyond the basic case: reading the same dataset consistently from
Trino while Spark writes, and a distributed transaction in which independent writer processes each
contribute part of one atomic commit. Both need more setup (a Trino build with the client patch) and
are documented alongside the scripts.

## Open frontiers

The design is open and the PoC is early. You do not need to own a frontier — or write any code — to
contribute:

- **Shape the direction** — the design is open. Ideas, prior art, and objections are all welcome.
- **Review the design and the code** — a careful read is a contribution.

And these are concrete, ownable pieces of work:

- **DDL inside transactions** — `CREATE` / `ALTER` / `DROP`, not only DML. The PoC is DML-only today.
- **Snapshot-lifecycle safety** — Iceberg must not expire a snapshot that a live transaction still
  pins. This one spans projects and is as much an Iceberg conversation as a Polaris one.
- **More engines** — beyond Spark and Trino. The protocol is engine-agnostic by design.
- **A setup anyone can run** — the demo currently needs source-built jars. A container image, or a
  path that works against released artifacts, would make this far easier to evaluate.
- **Security and robustness hardening** — authorization on the transaction endpoints is incomplete
  (table write privilege is not yet enforced there), and a read point that has aged out currently
  falls back to the latest data instead of failing. **Do not expose this PoC to untrusted
  principals.** Details are in the design doc's security section.

## Where this is headed

Personal forks are a starting point, not a destination. There are two destinations, and they need
different kinds of help:

- **Apache Polaris — land this as a real, additive catalog extension.** That means code: a design the
  community agrees with, reviewed and merged upstream rather than living in a fork.
- **Apache Iceberg — help the community refine the spec.** This is a *specification conversation, not
  a code drop*. Coordinating transactions across tables raises questions the spec does not answer
  yet, starting with snapshot lifecycle. The right outcome is a spec that supports multi-table
  atomicity cleanly for every catalog, not just this one.

Help getting there is the most valuable contribution.

## Contributing

**Start a discussion.** That is the most useful thing most people can do here, and it does not require
writing any code. Questions, objections, prior art, and "have you considered…" are all genuinely
wanted — an early design is far easier to change than a late one.

| I want to… | Go here |
|---|---|
| Ask how something works | [Q&A discussions](../../discussions/categories/q-a) |
| Propose a direction, raise an objection, point at prior art | [Ideas discussions](../../discussions/categories/ideas) |
| Anything else | [General discussions](../../discussions) |
| Pick up a frontier, or report something broken | [Issues](../../issues) |

Disagreement is welcome and useful. If you think the approach is wrong, the Ideas category is the
right place to say so — that conversation is more valuable now than after the design hardens.

### Contributing code

Code lives in the two forks, not in this repository. Send pull requests there:

- **Polaris** — [`qlong/polaris`](https://github.com/qlong/polaris/tree/lakehouse-transactions)
- **Trino** — [`qlong/trino`](https://github.com/qlong/trino/tree/lakehouse-transactions)

> [!IMPORTANT]
> Target the **`lakehouse-transactions` branch**, not `main`. A fork's `main` is upstream code, so a
> pull request opened against it will show this entire body of work as the diff.

For a small fix, a pull request is the fastest path. For anything larger — a frontier, or a change to
the design — please open a discussion or an issue first. These forks are a staging area rather than
the final destination (see *Where this is headed*), so it is worth agreeing early on where a change
should ultimately land, and it avoids anyone doing substantial work that later has nowhere to go.

### Discussing the upstream direction

Conversation about what a Polaris extension *should* look like belongs with the Polaris community, on
the [Apache Polaris dev mailing list](https://lists.apache.org/list.html?dev@polaris.apache.org).
Decisions about upstream are made there, not here. Discussions in this repository that turn out to be
upstream questions will be pointed that way.

## Project relationship

This is a personal project. It was not initiated by Cloudera; Cloudera is aware of the work and
supports it, and copyright is held by my employer, Cloudera, per standard employment terms.

This repository is **not** an Apache Software Foundation project. It is an independent proof of
concept built *on* the Apache lakehouse stack — Apache Polaris, Apache Iceberg, Apache Spark —
together with Trino, and proposed as an additive catalog extension. Nothing here is an official
deliverable of any of those projects.

Apache®, Apache Polaris™, Apache Iceberg™, and Apache Spark™ are trademarks of the Apache Software
Foundation.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
