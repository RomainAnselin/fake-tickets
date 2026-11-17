# 01 — Existing Application Specification (v1, `src/`)

> Scope: reverse-engineered specification of the **current** `fake-tickets`
> application as found in `src/` on 2026-11-11, cross-checked against the
> existing `README.md`, `WIKI.md`, `OPTIMIZATION.md` (now in [`v1/`](v1/)), the
> owner's TTL-enhancement and REPRO2 notes (kept outside the repo) and `test/`. Where the docs and the code disagree, **the code
> wins** and the discrepancy is logged in §7.

>
> **Where v1 lives now:** branch `v1` / tag `v1.0`. The scripts are under `src/`
> there (including `oldrecord.sh`, moved from `test/`, which stays a local workspace).
---

## 1. Purpose

`fake-tickets` is a **data simulator / load generator** for Apache Cassandra,
DataStax Enterprise (DSE) and DataStax Astra DB. It inserts synthetic
"support ticket" rows at a controlled rate so that support engineers can:

- reproduce customer issues (compaction, TTL, tombstones, indexes, LWT, MV),
- observe compaction behaviour over long runs (STCS vs TWCS, SSTable expiry),
- keep a *predictable*, bounded ingestion load on a resource-limited lab cluster.

Anti-patterns (monotonic `int` keys, MVs, large `IN`) are **intentional**.

---

## 2. Runtime & dependencies

| Item | Value |
|---|---|
| Language | Python 3 (no pinned version, no `requirements.txt`) |
| Driver | `cassandra-driver` (DataStax Python driver) |
| Entry point | `python src/insert-astra.py` |
| Wrapper | `forinsert.sh` — shell loop calling the CLI repeatedly with sliding `startval` |
| Packaging | none (no Dockerfile, no package metadata) |

---

## 3. Configuration (`conf/*.ini`)

Plain-text INI file, path passed with `-c`. **Secrets are stored in clear
text** on disk.

| Section | Key | Used? | Meaning |
|---|---|---|---|
| `general` | `isAstra` | yes | `"true"` → Astra, anything else → host list |
| `general` | `dcname` | yes | DC name for `NetworkTopologyStrategy` |
| `general` | `ksname` | yes | Keyspace to create / use |
| `astra` | `scb` | yes | Path to Secure Connect Bundle zip |
| `astra` | `token`, `secret` | yes | Client ID / secret (PlainTextAuthProvider) |
| `hosts` | `host1` | yes | Single contact point |
| `hosts` | `host2` | read, unused | Second contact point (commented out) |
| `hosts` | `isauthenabled` | yes | `"true"` → PlainText auth |
| `hosts` | `username`, `password` | yes | Auth credentials |
| `hosts` | `ssl_ks*`, `ssl_ts*` | **no** | Placeholders, TLS never implemented |

---

## 4. Functional features

### F1 — Multi-target connection (Cassandra / DSE / Astra) ★
- **Astra:** `Cluster(cloud={'secure_connect_bundle': scb}, auth_provider=PlainTextAuthProvider(id, secret))`.
- **Cassandra / DSE:** `Cluster([host1])`, optional `PlainTextAuthProvider`,
  `control_connection_timeout=30` when unauthenticated.
- No TLS, no load-balancing policy, no protocol version pinning, single
  contact point.
- Cassandra and DSE are the same code path; the difference only matters for
  DSE-only features (Solr search index).

### F2 — Connectivity / version check
`cassqueries.simple_test_query`: `SELECT release_version FROM system.local`,
printed to stdout. Runs on every invocation before any workload.

### F3 — Keyspace provisioning
On non-Astra targets: `CREATE KEYSPACE IF NOT EXISTS <ks> WITH replication =
{'class':'NetworkTopologyStrategy','<dc>':'3'}` (RF hard-coded to 3).
Astra keyspaces must pre-exist.

### F4 — Standard write workflow (`fake_tickets_workflow`) — *present but not wired*
- Table `<ks>.<table>(id int PK, ownedby text, ticket int, time timestamp, updatedat timestamp, notes text)`, default STCS.
- Writes ids `[startval, startval+numrec)` at `LOCAL_QUORUM` via paced async (F7).
- Row generator: `ownedby` = random name from a 17-entry list; `ticket` = random `0..100000`;
  `time` = now (ms); `notes` = one of two text templates depending on id parity;
  `updatedat` never written (always null).
- At ids in `{1,10,100,…,10^7}`: flush buffer, read row by id (`QUORUM`) and
  `SELECT count(*) … WHERE id=?`, print elapsed time. → embryonic **read benchmark**.
- Progress print every 10 000 ids.

### F5 — TWCS + TTL workflow (`fake_tickets_twcs_workflow`) — *the one currently wired*
- Table as F4 plus: `default_time_to_live`, TWCS (`1 HOURS`, `MILLISECONDS`
  resolution, `unchecked_tombstone_compaction=true`), `gc_grace_seconds=10800`.
- **Infinite loop** (`while True`) from `startval`; `numrec` is ignored.
- **Overlap injection (REPRO2):** every `target_ops × overlap_interval_min × 60`
  ids, re-write one random id taken from the last `target_ops × overlap_lookback_min × 60`
  ids; event logged to `overlap_<table>.log`. Reproduces cross-window
  partition overwrites in TWCS.

### F6 — Insert variants (helpers, not wired)
- `insert_bind_fake_tickets_ttl` — `USING TTL <n>` per statement.
- `insert_bind_fake_tickets_lwt` — `IF NOT EXISTS` with `LOCAL_SERIAL`.
- `insert_bind_dualid` (cassqueries) — clustering table `(id, id2)` with `USING TTL 86400`.

### F7 — Paced asynchronous ingestion (`insert_paced_async`) ★ (recent)
- Buffer `concurrency × 2` rows, dispatch with `execute_concurrent_with_args(concurrency)`,
  then `sleep(chunk / target_ops)`. `target_ops=0` disables pacing.
- Effective rate = `chunk / (exec_time + sleep)` → always **below** target
  (pacing error grows with latency). Rate is fixed for the life of the process.

### F8 — Index / view helpers (not wired)
- SAI: `CREATE CUSTOM INDEX … USING 'StorageAttachedIndex'` on `ownedby`.
- Solr (DSE Search): `CREATE SEARCH INDEX … WITH COLUMNS ownedby, updatedat, time, notes`.
- Materialized View `mv_fake_tickets` keyed `(ownedby, id)` — **CQL has a syntax error** (stray `(`).
- 53 encrypted Solr cores (`solr_encrypted_tables_create`) — DSP reproduction.

### F9 — Other reproduction helpers (not wired)
- `create_table_onepk_gc600` — single partition, `gc_grace_seconds=600`.
- `dualid_workflow` — `IN` query of 100 partitions benchmark (insert loop commented out).
- `truncate_table`.
- `deletebackups.opscenter_delete` — prints `DELETE` statements for ad-hoc OpsCenter backup rows (week hard-coded `202250`).

### F10 — Run state / resumption
- `-v <start> <count>` explicit range, or `-i <file>` containing `"start count"`.
- With `-i`, the file is advanced to `start+count` **before** the run starts.
- `helper.record()` — another counter file (`record.txt`), unused.
- `helper.cpurange()` — multi-process range splitting, unused.

### F11 — Operational scripts (`test/`)
- `oldrecord.sh` — inserts back-dated rows (`USING TIMESTAMP` in the past + TTL
  anchored to now) to study TWCS behaviour with late data.
- `reporting.sh` — collects `ls`/`sstablemetadata` per node through `ctool`.
- `reproduction/track_sstable.py` — follows an SSTable through compactions in debug logs.

---

## 5. Workflow selection

Workflow selection is done by **editing source code**: commenting / uncommenting
calls at the bottom of `insert-astra.py`. Only `fake_tickets_twcs_workflow` is
active. There is no CLI flag, no menu, no API.

---

## 6. Non-functional characteristics

| Aspect | Current state |
|---|---|
| Lifecycle | Batch process. Finishes after `numrec` (F4) or never (F5). In Kubernetes a finished process ⇒ container exit ⇒ **CrashLoop / restart** and the workload re-runs from the same ids. |
| Concurrency | One workload per process; multiple users = multiple shells. |
| Observability | `print()` to stdout; overlap log file in CWD; no metrics, no latency stats, no error counters. |
| Rate control | Static `target_ops` at call time; pacing inaccurate (F7). |
| Security | Clear-text credentials on disk; no TLS to DB; no auth on the tool itself. |
| Error handling | Write failures printed; connection failures crash the process. |
| Tests | None automated (shell/CQL scripts only). |
| Config | `.ini` file must be present on local disk — awkward in containers. |

---

## 7. Findings: defects & doc/code mismatches

| # | Finding | Where | Impact |
|---|---|---|---|
| D1 | `fake_tickets_twcs_workflow` default TTL is **86 400 s (1 day)** in code; WIKI/README/TTL-ENHANCEMENT state **172 800 s (2 days)** | `faketickets.py` | Data expires twice as fast as documented |
| D2 | `gc_grace_seconds` is **10 800** in code, docs say **3 600** | `faketickets.py` | SSTable drop delayed 2 h vs documented |
| D3 | `numrec` ignored by TWCS workflow (`while True`) | `faketickets.py` | `-v start count` misleading; never ends |
| D4 | With `-i`, incfile advanced **before** the run → crash = skipped range | `insert-astra.py` | Ids never written yet considered done |
| D5 | `id int` → overflow at 2 147 483 647. At 1 k/s ≈ 24.8 days from 0; at 50 k/s ≈ 12 h | schema | Continuous runs eventually fail every bind |
| D6 | Overlap schedule expressed in **ids** derived from `target_ops` — only correct if achieved rate == target (it is always lower, F7) | `faketickets.py` | Overlap cadence drifts from the intended 30 min |
| D7 | Paced async: sleep added *after* execution ⇒ achieved < target | `insert_paced_async` | "1000 ops/s" is really ~1000 × sleep/(sleep+exec) |
| D8 | MV creation CQL invalid (`… IF NOT EXISTS ks.mv (AS select …`) | `create_mv_fake_tickets` | Fails if ever enabled |
| D9 | `helper.read_config` passes format args to `sys.exit` (message never formatted) | `helper.py` | Cosmetic |
| D10 | `truncate_table` uses `print("%s", a, b)` (no formatting) | `cassqueries.py` | Cosmetic |
| D11 | `ssl_*` config keys are never used | `insert-astra.py` | No TLS to cluster |
| D12 | Writes at `LOCAL_QUORUM`, point read at `QUORUM`, count at driver default (`LOCAL_ONE`) — inconsistent CLs | `faketickets.py` | Cross-DC reads in multi-DC clusters; count may miss rows |
| D13 | WIKI §5 still documents `rate_limit_sleep` that no longer exists | `WIKI.md` | Doc drift |
| D14 | `overlap_<table>.log` written in CWD — lost in containers | `faketickets.py` | No audit trail in k8s |

Items previously reported in `OPTIMIZATION.md` (timestamp padding, `selrows`
bug, `cnt =+ 1`) are **already fixed** in the current code.

---

## 8. Features to keep in v2 (requirements carried forward)

| v1 feature | v2 requirement |
|---|---|
| F1 Cassandra / DSE / Astra connectivity | **Keep**, add TLS + multiple contact points + SCB upload |
| F2 version check | Workload **W1 Connectivity** (+ DSE version, DC, peers) |
| F3 keyspace provisioning | Keep, RF/DC configurable, auto-detect DC, skip on Astra |
| F4/F6/F8 inserts + TTL + LWT + SAI/2i/Solr | Workload **W2 Write** with options |
| F4 power-of-10 reads | Workload **W3 Read** (point reads + optional index reads) |
| F5 TWCS + overlap (+ `oldrecord.sh` back-dating) | Workload **W5 TWCS time-series** with wall-clock overlap & back-dated injection |
| F7 paced async | Rewritten as a **live-adjustable** deadline-based rate limiter |
| F10 resume | Server-side "next id" bookkeeping per connection/table |
| — | New **W4 Mixed** read/write, **W6 Datatypes** table |
| F9 OpsCenter cleanup, encrypted Solr cores, `IN` benchmark | Not ported (one-off reproductions); remain in `src/` |
