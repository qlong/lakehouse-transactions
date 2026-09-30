# How Multi-Table Transactions Work

*A follow-up to [explained.md](explained.md) (the **why**). This is the **how** — still high level,
still no schemas or code. The goal is that an engineer who has never read a database-internals paper
comes away understanding the key ideas and why they hang together. For the full design, see the
[design doc in the Polaris branch](https://github.com/qlong/polaris/blob/lakehouse-transactions/site/content/in-dev/unreleased/multi-table-transaction-design.md).*

---

## The one decision everything follows from: coordinate in the catalog

Start with a question: *if several tables must change together, who is allowed to change them
together?*

In a lakehouse, a table's "current version" is just a **pointer** the catalog publishes — "table
`orders` currently lives at this metadata file." A commit is the catalog swapping that pointer from
the old file to a new one. The data files themselves are immutable; visibility is entirely about
which pointer the catalog is advertising.

That gives an unavoidable conclusion:

> **Only the authority that owns the table pointers can swap several of them atomically in one step —
> and in a lakehouse, that authority is the catalog.**

Not the engine (Spark/Trino/Flink), because engines see the catalog as a remote API — they can't
atomically swap two pointers, and two engines racing to write the same table can't referee each
other. Not the client library, for the same reason. And a *separate* coordinator service doesn't
escape this: to swap the pointers atomically it must either be granted authority over them — at which
point it simply **is** the catalog under another name — or coordinate with the catalog across two
authorities, which is exactly the distributed two-phase commit we're trying to avoid. So the catalog
isn't a convenient choice; it's the one place a multi-table commit is a single local swap instead of a
distributed handshake.

The REST catalog already proves the point: its multi-table commit endpoint
(`POST /v1/{prefix}/transactions/commit`) swaps several table pointers atomically — for a bundle one
client has prepared in advance. This design extends that same single-step swap to a real transaction:
work that spans statements, reads at one consistent point, and can be split across independent workers.

**Why a service, specifically?** Because a transaction is *state that outlives a single request*:
"transaction 7 is open, it has staged orders but not customers, and it holds a lock on this table."
That state has to be durable and shared, so any node can see it and a crash doesn't lose it. A
catalog service backed by a database is exactly that: a shared, durable, transactional place to keep
transaction state. This also means we get atomicity almost for free (more on that below) instead of
building a distributed commit protocol.

---

## Key idea: borrow the database playbook, adapt it to Iceberg

Databases solved "many things must change together, and readers must see a consistent view" decades
ago. The classic ingredients:

- a **`BEGIN` / `COMMIT`** envelope,
- a way to give each reader a **stable point in time** to read from,
- **isolation levels** to choose how strict that stability is.

We reuse those ideas directly — they're familiar and battle-tested — but adapt them to the way
Iceberg already works. The crucial adaptation is that Iceberg is **copy-on-write and append-only**: a
commit never overwrites data, it writes new files and publishes a new snapshot. That single fact
makes the whole design simpler than a traditional database, as the next two sections show.

---

## How a write works: stage off to the side, then publish once

A multi-table write has two phases. Think of it as **"prepare quietly, then reveal all at once."**

```
BEGIN transaction
   ┌─ stage table A  (write data files + a new metadata file — but don't publish)
   ├─ stage table B
   └─ stage table C
COMMIT
   └─ catalog flips A, B, C to their new versions in one atomic step
```

The first phase does all the slow, heavy work — writing data files and new metadata to object
storage — **without changing any table pointer.** Those staged files are physically present but
**invisible**, because no pointer references them yet. Multiple tables (or multiple apps) can do this
in parallel.

The second phase is tiny and fast: the catalog swaps all the pointers together, inside a single
step, so every reader sees either the old world or the new world — never a mix. That single step is
also where the transaction gets its **`commit_scn`** — its position in the commit order — which is
what readers later compare against (see the read path below).

**Abort is trivial — there's no undo.** Because staged files are only made visible by the pointer
swap, aborting means simply *never doing the swap*. The staged files become orphans that a background
cleanup deletes later. Nothing to roll back, no undo log, no half-applied changes. This is the
copy-on-write payoff: "rollback" is just "never reveal it."

> **Key idea:** visibility is controlled by *one* pointer swap at the end, not by the writes
> themselves. So "all-or-nothing across tables" reduces to "do the swap, or don't."

---

## The write can fan out across independent workers

Because staging is decoupled from the final swap, the staging phase doesn't have to be done by one
process. One transaction can be split into per-table **subtransactions**, and independent workers —
different threads, different processes, even different machines — can each stage their table's slice
**in parallel**:

```
BEGIN transaction T
   subtransaction for orders     → worker 1  ┐  stage in parallel,
   subtransaction for customers  → worker 2  ├  each independent,
   subtransaction for products   → worker 3  ┘  none blocking the others
COMMIT T   → catalog publishes every staged table atomically
```

Two properties fall out of this:

- **Retry stays local.** If one subtransaction fails on a transient error, you re-assign *just that
  subtransaction* — possibly to a different worker — while the others stay staged and untouched.
  Re-run the 3-minute step, not the 3-hour job.
- **It needs the catalog as the rendezvous.** The workers share nothing but the catalog — no common
  process, no shared memory. The open transaction has to live somewhere every worker can reach and
  attach its slice to, and the catalog is the only place that is. This is why the write side keeps
  server-side state: a transaction that spans independent workers can't be held in any one client.

This is the write-side analog of the shareable read handle below — one transaction, fanned out to
many writers, instead of one snapshot, fanned out to many readers.

---

## How a consistent read works: a `TxnSnapshot` plus server-side resolution

For reads, we need every table in a query to resolve to the *same instant*. Three concepts make that
work — and these are the exact terms you'll see in the design doc, so it's worth learning them here.

**1. `commit_scn` — a logical commit order.** Every commit is stamped with its position in a single,
ever-increasing order for the catalog. "SCN" is borrowed from databases (System Change Number); read
it as **a version counter for the whole catalog**, not per table. Commit #100 happened before commit
#101, full stop. This is the logical clock that lets us talk about *an instant* across many tables
without relying on wall-clock time (which, as the previous doc explained, doesn't line tables up
reliably).

**2. `read_point` — a cutoff into that order.** A `read_point` is simply *a captured `commit_scn`
value* that means **"show me everything committed at or before this point."** It's one number: the
bookmark into the commit timeline.

**3. `TxnSnapshot` — the handle a reader holds.** A `TxnSnapshot` is the lightweight, shareable token
a reader acquires; it carries a `read_point` (plus an expiry). It has no locks and no write ability —
it's purely "the instant I'm reading as of." Pass the same `TxnSnapshot` to every table read and they
all resolve to the same instant.

This is a solved idea in a new place. PostgreSQL has `pg_export_snapshot()`: one session exports a
snapshot, others import it and read the same instant. Data warehouses pin an SCN the same way. The
lakehouse simply never had that primitive **across engines** — a handle a Spark job and a Trino query,
in different processes, can share. That is what the `TxnSnapshot` is: a warehouse primitive ported to
the open catalog.

Reading a table is then a lookup the catalog performs: *"for table `orders`, what was the newest
version with `commit_scn` ≤ my `read_point`?"* The catalog keeps a small mapping from
`commit_scn` → the concrete Iceberg version of each table (the design doc calls it
`committed_table_snapshots`) and answers per table.

```
acquire TxnSnapshot  (read_point = N)  ────────────────────────────────┐
   read orders    → newest orders    with commit_scn ≤ N → v42         │  all three
   read customers → newest customers with commit_scn ≤ N → v17         │  agree at
   read products  → newest products  with commit_scn ≤ N → v31         │  instant N
```

> **Key idea:** the reader picks the instant *once* (its `read_point`), and the **catalog resolves
> each table to it** via the `commit_scn` mapping. The engine never has to guess timestamps or thread
> snapshot IDs — the hard part (which per-table version lines up) is done for it, server-side.

This is precisely the gap plain time travel couldn't fill: time travel can *read* a table at a known
version, but it can't *figure out* which per-table versions form one consistent instant. The
`commit_scn` order plus the catalog's mapping is that missing resolver.

> **A note on the two "snapshots".** Careful with the word — it means two different things here. A
> **`TxnSnapshot`** is the transaction-level read handle (a `read_point`). An **Iceberg table
> snapshot** is Iceberg's native per-table point-in-time. The catalog's job is exactly to translate
> the first into the right one of the second, for each table.

---

## Locks: only where they're actually needed

Not every write needs a blocking lock, and **the client — not the server — chooses.** Each
subtransaction declares a mode; the catalog never escalates automatically based on the operation:

- **Default: non-blocking (`ROW_EXCLUSIVE`).** This is the mode you get unless you ask for another. It
  records a lock row but is self-compatible, so concurrent additive writers proceed freely — Iceberg's
  high-throughput ingest is deliberately left untouched. A conflict here isn't fatal: if a concurrent
  commit moved the table's base first, the catalog **merges and retries** at commit — re-applying just
  the new data files on top of the current state — rather than failing.
- **Opt-in pessimistic (`EXCLUSIVE` / `ACCESS_EXCLUSIVE`).** When a client knows an operation is
  destructive (overwrite, rewrite) and wants a guaranteed-clean finalize/retry, it **explicitly
  requests** a pessimistic mode at subtransaction creation. That reserves the table up front so hours
  of work aren't wasted by a conflict at the finish line — but it's the caller's choice, not an
  automatic decision.

(The single-session inline commit path takes no lock at all — it stages everything and relies on the
commit-time conflict check.) Locks are held by the catalog (in its database), acquired in a consistent
order to avoid deadlocks, and — critically — **time-limited**. Which leads to failure handling.

---

## When things go wrong: leases and a reaper

Distributed jobs crash. The design assumes it, and never relies on detecting it perfectly:

- Every transaction has a **lease** (an expiry). If the client vanishes, the transaction can't linger
  forever.
- A background **reaper** periodically aborts expired transactions and releases their locks, so a
  dead job can't block others indefinitely.
- **Safety never depends on the reaper.** Because visibility is the final pointer swap, a crashed
  transaction simply never became visible. The reaper is about *tidying up* (freeing locks, cleaning
  orphaned files), not about correctness.

And because all transaction state lives in the catalog's database, **atomicity comes from the
database, not from us.** That database is a **relational one** (e.g., PostgreSQL), so the final
multi-table publish is just **one local ACID transaction**: if it commits,
every pointer moved; if it rolls back (crash, conflict), none did. There is no fragile distributed
two-phase commit to coordinate across machines — we turn the coordination problem into a single
`COMMIT` and inherit 30+ years of battle-tested transactional durability instead of hand-rolling
consensus.

> **Key idea:** the server process stays stateless and horizontally scalable; the *state* lives in a
> shared database. Any node can handle any request, and the database referees the one moment that
> must be serialized (the commit).

---

## Why no changes to Spark (or Iceberg) are required

This is the part that makes it adoptable. **The engine keeps reading Iceberg tables the way it always
has.**

- The catalog does the resolution and hands back a **standard Iceberg table version** (a metadata
  location / snapshot the engine already knows how to read). From Spark's point of view it's just
  loading a table — it doesn't need to understand transactions, bookmarks, or commit order.
- You don't rewrite queries to thread snapshot IDs or guess `AS OF` timestamps. Application logic and
  SQL stay the same; the consistency is delivered by the catalog underneath.
- The transaction verbs (`BEGIN TRANSACTION`, `COMMIT`, `ROLLBACK`, and the multi-app write flow) are
  provided through the **Polaris catalog plugin and REST API** — configuration and a thin catalog
  layer, not a fork of Spark and not a change to the Iceberg format.

So the feature rides on top of stock engines and the existing Iceberg spec. Teams that don't opt in
keep their exact current behavior; teams that do get multi-table consistency without re-plumbing
their jobs.

> **Key idea:** consistency is resolved **server-side and returned as ordinary Iceberg metadata**, so
> the intelligence lives in the catalog and the engines stay dumb, standard, and unmodified.

---

## Isolation levels, mechanically

The two isolation levels from the previous doc map directly onto the read path above:

| Level | Mechanically, this means… |
|---|---|
| **READ COMMITTED** | Take a **fresh `read_point` for each statement.** Every read sees the latest committed version at that moment; successive reads can differ. |
| **REPEATABLE READ** *(default)* | Capture **one `read_point` at `BEGIN`** (a `TxnSnapshot` pinned for the whole transaction) and reuse it for every statement, across every table — so the view is stable and cross-table consistent until you finish. |

A writer using REPEATABLE READ reads through its own `read_point` *plus* its own staged
(not-yet-published) changes — that's read-your-own-writes: it sees its work in progress while everyone
else still sees the old world.

---

## Key technical concepts, in one place

| Concept | Plain meaning |
|---|---|
| **Catalog as coordinator** | The catalog owns table pointers, so it's the only thing that can move several tables together. Transactions live there. |
| **Stage, then publish** | Writes go to object storage invisibly first; one atomic pointer swap at `COMMIT` reveals them all at once. |
| **Copy-on-write ⇒ no undo** | Aborting just means never publishing. Orphaned files are cleaned up later; there's nothing to roll back. |
| **`commit_scn` (commit order)** | A single catalog-wide, ever-increasing number stamped on each commit. Defines "before/after" without wall-clock time. |
| **`read_point`** | A captured `commit_scn` cutoff — "everything committed at or before this." The one number that defines a consistent instant. |
| **`TxnSnapshot`** | The lightweight, shareable read handle carrying a `read_point`. Passed on every read to pin one consistent instant across tables, jobs, and machines. |
| **`committed_table_snapshots` mapping** | The catalog's index from `commit_scn` → the concrete Iceberg version of each table. It resolves a `read_point` into per-table versions — the piece time travel lacks. |
| **Server-side resolution** | The catalog maps a `read_point` to the right version of each table, so engines read normally and stay unmodified. |
| **Non-blocking default / opt-in pessimistic locks** | Subtransactions default to the non-blocking `ROW_EXCLUSIVE`; a client explicitly requests `EXCLUSIVE`/`ACCESS_EXCLUSIVE` for destructive writes. The server never escalates automatically. |
| **Leases + reaper** | Time-limited transactions and a background cleaner bound the blast radius of a crashed client — without ever being load-bearing for correctness. |
| **Atomicity from the database** | The final commit is one DB transaction; the catalog's backing database provides all-or-nothing for free, so there's no distributed 2PC. |

---

## How it compares

Other systems solve *parts* of this. What's distinct here is the **combination** — warehouse
`BEGIN`/`COMMIT` semantics, over the open REST catalog, usable by any engine:

| | Single-engine warehouse | Nessie (git-like) | This approach |
|---|---|---|---|
| Cross-table atomicity | ✅ *(in-engine)* | ✅ *(branch/merge)* | ✅ *(catalog publish)* |
| Consistent multi-table reads | ✅ *(in-engine)* | ✅ *(pin a branch)* | ✅ *(exportable snapshot)* |
| **Across independent engines** *(no 2PC)* | ❌ | ✅ | ✅ |
| Mental model | SQL transactions | branch / commit / merge | **SQL `BEGIN`/`COMMIT`** |
| Fan work to parallel workers, retry one not all | — | manual | ✅ *(subtransactions)* |
| No data / format change | — | needs Nessie as your catalog | ✅ *(any REST catalog; opt-in extension)* |

The only row all three share is atomicity-plus-reads. A single-engine warehouse gives you transactions
only *inside* that one engine. Nessie has the catalog primitive — atomic multi-table commit and
branch/merge — but adopting it means adopting a Git-style versioned catalog, a different mental model.
This approach puts familiar `BEGIN`/`COMMIT` on the open REST catalog you already run, as an extension
you opt into, with no data or format change.

And the plain REST catalog on its own? Its multi-table commit gives cross-table **atomicity** for a
bundle one client prepares in a single request — but no transaction around it: no `BEGIN`/`COMMIT`
across statements, no consistent read point, no isolation level, and no fan-out to parallel workers.

## In one sentence

**We put transaction coordination where it's the only thing that can work — the catalog — reuse the
proven `BEGIN`/`COMMIT` + isolation-level playbook, lean on Iceberg's copy-on-write model so
"publish" and "abort" are trivial, and resolve consistency server-side into ordinary Iceberg
metadata, so Spark and Iceberg need no changes at all.**
