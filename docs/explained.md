# Multi-Table Transactions, Explained

*A plain-language introduction. It explains **what** problem multi-table transactions solve and
**why** the existing tools fall short — no schemas, APIs, or internals. Next, [how-it-works.md](how-it-works.md)
covers **how** it works; for the full design (schemas, protocol) see the
[design doc in the Polaris branch](https://github.com/qlong/polaris/blob/lakehouse-transactions/site/content/in-dev/unreleased/multi-table-transaction-design.md).*

---

## 1. What problem are we solving?

A data lakehouse rarely has just one table. Real datasets are **several tables that only make sense
together**. The problem multi-table transactions solve is simple to state:

> **Keeping related tables consistent with each other — both when writing them and when reading
> them.**

Two failure modes show up constantly.

### 1a. Writing related tables together (atomicity)

Consider the most ordinary star schema: an `orders` fact table and a `customers` dimension table. A
nightly job loads a new batch: new customers *and* the orders those customers placed.

```
Load batch B:
   + 3 new customers  (Alice, Bob, Carol)
   + 8 new orders      (all placed by Alice / Bob / Carol)
```

Today these are two independent writes. Whatever the order, there is a **window in between**:

```
                        orders updated?   customers updated?   a reader here sees…
after customers write         no                yes            customers with no orders  (harmless-ish)
after orders write            yes               no             orders referencing customers
                                                                that don't exist yet      ✗ BROKEN
```

A dashboard or a join that runs in that window sees **orders pointing at customers that aren't
there** — broken foreign keys, wrong totals, failed joins. Nothing is corrupted on disk; each table
is individually fine. The dataset is just **internally inconsistent for a moment**, and that moment
is enough to produce a wrong answer or crash a downstream job.

And this is not a rare race that needs something to go wrong. Even when **every write succeeds**, two
separate commits publish at two different instants — so the disagreement window opens on *every* run,
not just when a job crashes. A crash doesn't *cause* the window; it only makes a transient window
**permanent**. That's why "just make the writes reliable" doesn't fix it: reliability closes the
crash case, not the everyday one.

The same shape appears everywhere:

| Scenario | The two (or more) tables that must move together |
|---|---|
| Event sourcing | an `events` log and the `current_state` it drives |
| Compliance | a `patient_record` and its `audit_log` entry |
| Financial ledger | a `debit` on one account and the matching `credit` on another |
| Table swap | promoting several `*_staging` tables into `*_prod` as one release |

In each case, a reader that catches the in-between state sees something that **should never have
existed**: an event with no state change, a record with no audit trail, money that left one account
but hasn't arrived in the other, a half-published release.

**What we want:** the whole batch becomes visible **all at once, or not at all**.

### 1b. Reading related tables together (consistency)

Even if every individual write is correct, **reading** several tables can still tear.

A report joins `orders` and `customers`. It reads them one after another (that's how engines work —
one table at a time). If a writer commits *between* those two reads:

```
Report:
   read orders     ──►  (sees state as of 10:00:00)
        …a writer commits new orders + customers at 10:00:01…
   read customers  ──►  (sees state as of 10:00:02)

   The report now joins orders-as-of-10:00:00 with customers-as-of-10:00:02.
   These two halves never coexisted. The join is against a state that
   never actually existed in the database.  ✗
```

This is a **torn read**. It is not a bug in either table and not a data-file problem — it is purely
about *when* each table's metadata was fetched. It is easy to reproduce and maddening to debug,
because re-running the report usually "fixes" it (the timing no longer lines up) — until it happens
again in production at 3 a.m.

**What we want:** all tables in one query resolve to the **same instant in time**, no matter when
each individual read happens.

### 1c. Why this matters: the cost leaks into everything

These aren't rare edge cases — they are the normal state of any pipeline that touches more than one
table. And because the lakehouse itself doesn't guarantee cross-table consistency, **the burden
shifts onto the people building on top of it.** That shows up as real, recurring cost:

- **Application logic gets more complicated — and stays that way.** Without atomic multi-table
  writes, teams hand-roll workarounds: careful write *ordering* (load dimensions before facts to make
  the window "less bad"), staging tables plus a separate "publish" step, and reconciliation jobs that
  run afterward to detect and repair partial loads. Each is extra code that exists only to paper over
  a gap the platform left open — and none of them fully close it (ordering still leaves a window, and
  it doesn't even compose once you have three or four tables).

- **Readers have to code defensively.** Because a read might catch a torn state, every downstream
  query learns to distrust its own inputs: `LEFT JOIN` instead of `JOIN` to tolerate a missing
  dimension row, filters to skip half-loaded batches, "wait a few minutes for things to settle"
  guards before a dashboard refreshes. Correctness gets scattered across dozens of consumers instead
  of being guaranteed once at the source.

- **Correctness becomes a convention, not a guarantee.** "Orders always reference a real customer" is
  true only by tribal knowledge and discipline — nothing enforces it. New team members, new
  pipelines, or a slightly re-ordered job quietly break the assumption, and no one notices until a
  report is wrong.

- **Bugs are flaky and expensive to chase.** Torn reads and partial loads are timing-dependent: they
  vanish on re-run, pass every test locally, and resurface in production at 3 a.m. Engineers burn days
  reproducing something that isn't a bug in any one table — it's a bug in the *seam between* tables.

- **Trust erodes.** Once a dashboard shows the wrong total even occasionally, people stop trusting it —
  and start adding manual checks, reconciliation reports, and "let me just re-run it" habits. For
  audit and compliance workloads, an observable inconsistency between a record and its audit trail
  isn't just annoying, it can be a regulatory problem.

- **Pipelines lose flexibility.** To keep the window small, teams are pushed to cram related writes
  into a single job on a single engine so they can control ordering — giving up parallelism, fault
  isolation, and the freedom to let independent teams or engines own different tables.

In short: the absence of multi-table transactions doesn't remove the consistency problem — it just
**pushes it out of the platform and into everyone's application code, tests, dashboards, and
on-call rotations**, where it is far more expensive to handle and impossible to handle completely.

---

## 2. Why aren't today's tools enough?

Iceberg already has excellent building blocks. They are just the wrong shape for *cross-table*
consistency.

### 2a. Single-table transactions

Iceberg's atomic commit is **per table**. It guarantees that *one* table moves from a valid state to
another valid state atomically — and it does that very well.

But it has no concept of "these two commits belong together." Two single-table commits are two
independent events, so there is always an observable gap between them (§1a). Single-table atomicity
composed twice is **not** two-table atomicity — it is two atomic steps with a visible seam in the
middle.

> Single-table transactions make each table correct **on its own**. They say nothing about tables
> being correct **relative to each other**.

There is one multi-table tool: Iceberg's REST catalog can commit changes to **several tables in a
single request**, all or nothing. That closes the seam — but only for a writer that can prepare every
change in one place and send it in one shot. It's an atomic *apply*, not a transaction: there's no
`BEGIN`/`COMMIT` you can wrap around several statements, nothing keeps your reads of those tables at
one consistent instant while you prepare the change, and the work can't be split across independent
jobs — one client has to build the whole bundle. SQL engines also don't expose it: an
`INSERT INTO orders …; INSERT INTO customers …` from Spark or Trino still commits table by table.

### 2b. Time travel (`AS OF snapshot / timestamp`)

Iceberg time travel lets you read a table *as of* a past snapshot or timestamp. It's tempting to
think this already solves consistent reads: "just read every table as of the same timestamp."

It doesn't — for a subtle but decisive reason. A consistent multi-table read is really **two
problems**:

1. **Which point of *each* table lines up into one consistent instant?** (e.g. orders@snapshot-42
   goes with customers@snapshot-17)
2. **Read a given table at a given point.**

**Time travel only solves problem 2.** It's a per-table reader: hand it a snapshot for *one* table
and it reads that table there. It has no notion of a cross-table instant, so it cannot tell you
*which* per-table snapshots belong together — that's problem 1, and it's the hard part.

"Read every table `AS OF 10:00:00`" seems to answer problem 1, but a wall-clock timestamp is an
unreliable way to line tables up:

- A snapshot's timestamp is the **writer's** clock at *write* time, which is not the same as *commit*
  time, and different writers have different clocks.
- So the two halves of one logical change (the `orders` write and the `customers` write of the same
  batch) can land on **opposite sides** of your chosen timestamp.

Result: `AS OF <timestamp>` across tables can reproduce the very torn read you were trying to avoid.
Time travel is a great *executor* once you already know the right per-table points; it is not a
*resolver* that finds them.

> Single-table transactions lack cross-table **atomicity**; the multi-table commit is atomic but is a
> one-shot bundle, not a transaction. Time travel lacks cross-table **resolution**. All three gaps are
> about the *relationship between tables* over time, which none of these tools models.

---

## 3. How multi-table transactions solve it

The idea is the one every application developer already knows from relational databases:
**`BEGIN` … `COMMIT`**, extended across tables, with the catalog acting as the coordinator (it is
already the authority that publishes each table's current state, so it is the natural place to move
several tables together).

Two capabilities fall out of this, matching the two problems above.

### 3a. Atomic multi-table write — solves §1a

Inside one transaction you stage changes to as many tables as you like. At `COMMIT`, they all become
visible **together, in a single step**; on failure, **none** of them do.

```
BEGIN
   write new customers
   write new orders
COMMIT      ──►  both tables flip to the new state at the same instant
```

Re-running the "orders count vs. customers count" probe during the commit now only ever sees
`(before, before)` or `(after, after)` — never the broken `(after, before)` in between. The window
from §1a is gone by construction.

### 3b. Consistent multi-table read — solves §1b

A reader takes **one shared read handle** at the start and passes it to every table read. Every read
under that handle resolves to the **same logical instant**, regardless of when each read physically
runs — even across parallel jobs on different machines.

```
take one read handle H
   read orders     with H  ─┐
   read customers  with H  ─┼─►  all resolve to the same instant, even if a
   read products   with H  ─┘    writer commits in the middle
```

The catalog does the hard part (problem 1 from §2b): it *resolves* the handle to the correct point
of every table. The torn read from §1b is impossible, because there is no longer a per-read timing
question — the instant is fixed once, up front.

This handle is cheap and shareable: an orchestrator can take it once and hand it to ten Spark jobs so
they all read the same dataset version, even if some start ten minutes later than others.

### 3c. Isolation levels — pick how strict you need

"Which instant do I see, and does it change under me?" is exactly what **isolation levels** answer.
We use the familiar SQL names, with the same meaning developers already expect:

| Level | What you see | Use it when |
|---|---|---|
| **READ COMMITTED** | Each statement sees the latest committed data *as of that statement*. Two reads in the same session can differ if someone commits in between. | Quick, single-table lookups where "freshest" beats "stable". |
| **REPEATABLE READ** *(a.k.a. snapshot isolation — the default)* | The **whole transaction** sees one frozen instant, pinned at `BEGIN`. Every statement, across every table, sees that same instant — concurrent commits are invisible until you finish. | Multi-table reads, multi-step reports, anything that joins or compares tables. This is what makes §1b go away. |

The mental model is short:

- **READ COMMITTED** = "always the latest, per read." Good for a point check.
- **REPEATABLE READ** = "a stable photograph of the whole dataset for the life of my transaction."
  Good for anything that reads more than one thing and expects them to agree.

A writer using REPEATABLE READ also gets **read-your-own-writes**: within its transaction it sees its
own staged changes, while everyone else keeps seeing the old state until it commits.

(SERIALIZABLE — full protection against even subtle write-skew between concurrent writers — is a
possible future level; snapshot isolation already covers the multi-table use cases above.)

### 3d. Why this is easier to reason about and use

- **It's the model you already know.** `BEGIN`/`COMMIT` and SQL isolation levels are decades-old,
  widely taught, and match how warehouse engines already behave. There is little new to learn.
- **No manual bookkeeping.** You don't thread snapshot IDs through your code or guess timestamps per
  table. You take one handle (or one transaction) and the catalog resolves every table correctly.
- **Simple failure semantics.** A committed transaction is fully visible; an aborted or crashed one
  is fully invisible. There is no partial state to detect, clean up, or reason about — "all or
  nothing" is the whole story.
- **Correctness doesn't depend on timing.** The torn read and the broken-foreign-key window are
  eliminated *by construction*, not by hoping the race doesn't happen. Tests stop being flaky and
  3 a.m. incidents stop being timing-dependent.
- **Engine-neutral.** Because the catalog coordinates, independent engines (Spark, Trino, Flink) and
  independent jobs all observe the same consistent dataset version without having to talk to each
  other.
- **Additive — nothing changes until you use it.** There is no Iceberg table-format change and no
  server switch to flip. The transaction verbs are just an extension the catalog offers; a client
  that never calls them keeps behaving exactly as it does today. You opt in per operation — ask for a
  consistent read, or wrap writes in `BEGIN`/`COMMIT` — so a table written transactionally is still a
  normal Iceberg table any tool can read. (Consistent *reads* work on any engine with no plugin;
  transactional *writes* use the Polaris Spark plugin today.)

---

## In one sentence

**Single-table transactions keep each table correct on its own and time travel reads one table at a
known point; multi-table transactions add the missing piece — keeping related tables correct
*relative to each other* — using the same `BEGIN`/`COMMIT` and isolation-level model developers
already trust.**
