# 01b — fake-tickets v2: Requirements & Specification

> **Status:** written 2026-11-12 for the record, *after* v2 was built
> (repository root, image `localhost/fake-tickets:2.0.0`). Not yet reviewed by the owner.
> It states **what** v2 must do, **where** each requirement came from and **how**
> it was checked. The *how it is built* lives in the [HLD](02-HLD.md) and
> [LLD](03-LLD.md); v1 is described in [01](01-EXISTING-SPECIFICATIONS.md).
>
> Wording: **must** = required, **should** = expected unless there is a reason
> not to, **may** = optional.

---

## 1. Purpose and scope

fake-tickets is a **data simulator**. It puts a predictable, adjustable load of
realistic rows (and reads) on Cassandra, DSE or Astra so that cluster behaviour,
compaction in particular, can be tested in a homelab with limited resources.

v1 is a set of batch scripts: plain-text config files, workload chosen by editing
code, and a process that exits when the data set is written. Put in Kubernetes as
is, it restarts in a loop. v2 is a **long-running service**: the workload is a
*run* that a user starts, tunes and stops from a browser, and the service stays up.

## 2. Sources

| Ref | Source | Notes |
|---|---|---|
| **S1** | Owner's redesign brief, 2026-11-11 (kept outside the repo) | Redesign brief: Kubernetes-friendly, front end / back end split, login, connection store, workloads, rate slider |
| **S2** | Owner's earlier objectives note (kept outside the repo) | "A predictable amount of data being inserted constantly but not too fast"; TWCS table, 1 h windows, 2-day TTL |
| **S3** | [01 v1 specification](01-EXISTING-SPECIFICATIONS.md) | Features **F1–F11** to carry forward, defects **D1–D14** to fix |
| **S4** | Owner feedback, 2026-11-11 (after the first build) | (a) out-of-order inserts were only a reproduction attempt for one bug; (b) the front end must let you set a **number of records**; (c) add workloads for **compaction-strategy testing** |
| **S5** | Designer decisions where the brief was silent | Logged as **K1–K19** in [HLD §6](02-HLD.md#6-key-design-decisions-log) |

## 3. Users and use cases

**Users:** the homelab owner, plus any other logged-in user. Several users may
run workloads at the same time; each run records its owner.

| # | Use case |
|---|---|
| UC1 | Check that a cluster is reachable and see its version and topology |
| UC2 | Put a slow, steady load on a cluster for days without the pod restarting |
| UC3 | Insert a set number of records (e.g. 5 M) or load for a set time, then stop |
| UC4 | Compare compaction strategies on the same data shape (overwrites, deletes, TTL) |
| UC5 | Watch disk use, tombstones and compactions on the table while a run is going |
| UC6 | Change the rate while a run is going |
| UC7 | Reproduce the REPRO2 TWCS bug with v1's exact table settings |
| UC8 | Resume inserting where the last run on a table stopped |

## 4. Functional requirements

**Status key:**
- ✅ built and verified (how is shown)
- ◐ built, but tested only with mocks or partly
- ⚠ built, but not run against the real system
- ✗ not built

### 4.1 Service and runs

| ID | Requirement | Source | Status |
|---|---|---|---|
| R1 | The service **must** stay up when a workload finishes. A workload runs as a *run* that is started, changed and stopped through an HTTP API. | S1 design objective | ✅ `test_backend_api` |
| R2 | Each run **must** execute in its own OS process (or processes), so several users can run workloads at the same time. | S1 §4 Backend | ✅ `test_backend_api` (real spawned workers) |
| R3 | A run **must** end with a recorded reason: `stopped`, `count_reached`, `duration_reached`, `id_space_exhausted`, or a failure with its error. | S5 (K1) | ✅ unit + integration |
| R4 | The workload catalogue, including each workload's parameters, **must** come from the back end. The front end renders it and has no hard-coded list. | S1 "listed from what lives in the backend" | ✅ |
| R5 | Runs that were active when the back end stopped **must** be marked `interrupted`. The next run on the same connection and table **should** be offered the id where the last one stopped. | S3 F10, D4 | ✅ container restart test |
| R6 | Concurrent runs **must** be capped (`FT_MAX_RUNS`, default 8). Extra requests get HTTP 429. | S2 "finite resources" | ✅ |

### 4.2 Connections and secrets

| ID | Requirement | Source | Status |
|---|---|---|---|
| R10 | Connect to **Cassandra**, **DSE** and **Astra**. Astra uses an uploaded Secure Connect Bundle (zip, checked to contain `config.json`). | S1, S3 F1 | Cassandra ✅ · SCB upload ✅ (UI) · real Astra and DSE connections ⚠ |
| R11 | Connection profiles **must** be stored in SQLite. Secrets (passwords, Astra token or secret, SCB, TLS key) **must** be encrypted with a salted key derivation: AES-256-GCM, with a key from scrypt(master key, per-record salt). Secrets **must not** appear in argv, environment, logs or the back-end database. | S1 "sqlite DB with salting" | ✅ `test_frontend` (on-disk check), `test_backend_api` (no secrets persisted) |
| R12 | When a saved connection is edited, empty secret fields **must** keep the stored value. The browser never gets a secret back. | S5 | ✅ Playwright |
| R13 | TLS from the client to the cluster: CA certificate, hostname verification, optional client certificate. | S3 D11 | ⚠ not run against a TLS-enabled cluster |
| R14 | The keyspace **should** be created when missing, with a configurable replication factor and an auto-detected data centre. This is skipped on Astra. | S3 F3 | ✅ Cassandra |

### 4.3 Access and transport

| ID | Requirement | Source | Status |
|---|---|---|---|
| R20 | Login **must** be optional: `none` (behind an auth proxy), `local` (bootstrap admin) or `ldap` (homelab directory, with an optional group). | S1 UX 1 | local ✅ (browser) · LDAP ◐ (mock directory only) |
| R21 | HTTPS with certificates from the homelab smallstep CA, at configurable file paths. Renewed certificates **must** be picked up without a restart, because step leaves are short-lived. | S1 UX 3 | ◐ verified with a locally generated CA; real step-ca / step-issuer ⚠ |
| R22 | Front end → back end **must** use a shared bearer token, and TLS verified with the same CA when enabled. | S5 | ✅ |
| R23 | Browser hardening: CSRF header on writes, strict CSP, `HttpOnly; SameSite=Strict; Secure` session cookie. | S5 | ✅ `test_frontend` |

### 4.4 Rate

| ID | Requirement | Source | Status |
|---|---|---|---|
| R30 | The user sets a target rate from 1 to 100 000 ops/s, or unlimited, with a slider, a number box and presets (10, 100, 1 k, 5 k, 20 k, unlimited). An estimate of rows and bytes per hour and per day is shown. | S1 nice-to-have | ✅ Playwright |
| R31 | Rate, concurrency and the read share **must** be changeable while a run is going, without restarting it and without a burst. | S1, S3 F7 | ✅ |
| R32 | When the cluster keeps up, the achieved rate **must** be within ±10 % of the target. This fixes v1's sleep-after-execute drift. | S2 "predictable", S3 D7 | ✅ 1.5 k/s against Cassandra; limiter ±1 % from 1 to 100 k/s with a fake clock |
| R33 | Rates beyond one process **must** be reachable by spreading a run over several processes (`processes`, capped by `FT_MAX_PROCESSES_PER_RUN`, default 8). | S5 (HLD §4.3) | ✅ 2 processes |
| R34 | An optional global rate ceiling, `FT_MAX_RATE`. | S2 | ✅ |

### 4.5 How much: number of records or duration (S4 b)

| ID | Requirement | Source | Status |
|---|---|---|---|
| R40 | The user chooses **Until stopped**, **Number of records** or **Duration**. | S4 b | ✅ Playwright |
| R41 | *Number of records* means **rows written**. Reads, deletes and the extra writes the REPRO2 workload schedules (overlap rewrites, back-dated rows) do not count. For the Read workload it is the number of reads, and the label says so. | S4 b, S5 (K16) | ✅ unit (`count_kinds`) + integration (mixed: rows = count) |
| R42 | Records accept `250000`, `250k`, `1.5M`, `2B` and presets from 1 k to 100 M. Durations accept `90s`, `10m`, `1.5h`, `2d` (a bare number means minutes) and presets from 1 min to 7 days. | S4 b | ✅ Playwright |
| R43 | Before start, the UI **should** estimate the time to complete (or the records in the chosen duration) and the raw data volume at the chosen rate. | S4 b | ✅ |
| R44 | While a run is going, its page **must** show a progress bar with an ETA, and the runs table **must** show a progress column. | S4 b | ✅ Playwright |
| R45 | Ids **must** never overflow or wrap. New tables use `bigint`. On an existing `int` table, the run stops cleanly at 2 147 483 647 (`id_space_exhausted`), and a record count that cannot fit is rejected **before** the run starts. | S3 D5, S4 | ✅ integration (bigint ids > 2³¹, `int` stop, pre-check) |

### 4.6 Workloads

| ID | Requirement | Source | Status |
|---|---|---|---|
| R50 | **Connectivity**: release and DSE version, cluster name, data centre, peers, keyspaces, round-trip latency. | S1 workload 1, S3 F2 | ✅ Cassandra |
| R51 | **Write fake tickets** (v1 row shape): TTL none / table default / per row; index none / SAI / 2i / Solr; optional LWT. | S1 workload 2, S3 F4 F6 F8 | ✅ (SAI, 2i, LWT, TTL) · Solr ⚠ |
| R52 | **Read**: by id, by owner (index) or Solr query, with hit and miss counts. | S1 workload 3 | ✅ id, owner · Solr ⚠ |
| R53 | **Mixed** read / write with a read percentage that can change while running. | S1 "mixing reads and writes" | ✅ |
| R54 | **Data types**: a different table with other data types (collections, UDT, tuple, uuid / timeuuid, decimal, varint, inet, date / time, blob, duration) in wide partitions. | S1 workload 4 | ✅ |
| R55 | **Compaction workloads**: overwrite / update-heavy, deletes & tombstones, time-series with TTL, change compaction (ALTER), inspect table. See §5.2. | S4 c | ✅ functional (see §8 for what is not proven) |
| R56 | The out-of-order / back-dated insert workload **must** be kept only as a labelled **bug reproduction** (REPRO2), with v1's exact table options. General TWCS testing uses the time-series workload. | S4 a, S5 (K19) | ✅ integration (`WRITETIME` checks) |
| R57 | v1 one-off scripts (OpsCenter cleanup, 53 encrypted Solr cores, `IN` benchmark) and materialized views are **not** ported. | S5 | — (stay in `src/`) |

### 4.7 Compaction options and table monitoring (S4 c)

| ID | Requirement | Source | Status |
|---|---|---|---|
| R60 | Every workload that creates a table **must** offer the same compaction options: strategy STCS / LCS / UCS / TWCS with that strategy's sub-options, plus `gc_grace_seconds`, `tombstone_threshold` and `unchecked_tombstone_compaction`. The UI shows only the selected strategy's sub-options. | S4 c, S5 (K17) | ✅ integration (option matrix, LCS, UCS `L10,T4`) |
| R61 | If the table already exists, the user chooses **keep** (a warning is shown when the settings differ) or **alter** (ALTER TABLE to the chosen settings). Settings **must never** be silently ignored. | S5 (K17) | ✅ integration |
| R62 | Any user text that reaches DDL **must** be validated: table and keyspace names against `^[A-Za-z][A-Za-z0-9_]{0,47}$`, UCS `scaling_parameters` against a strict pattern, everything else a fixed choice or a number. | S5 | ✅ unit (injection attempts rejected) |
| R63 | The run page **should** show the table's state on the cluster: disk use, largest partition and sstable, tombstones per read, running compactions and those completed since the run started. Each completed compaction becomes a run event. | S4 c, S5 (K18) | ✅ single node (manual flush / compact) · multi-node ⚠ |
| R64 | Where the cluster lacks those system views (Astra, DSE 6.8, Cassandra < 4.1), monitoring **must** switch itself off with one info event and **must not** affect the run. | S5 (K18) | ◐ failure path written, not exercised on those systems |

### 4.8 Observability

| ID | Requirement | Source | Status |
|---|---|---|---|
| R70 | Live stats every second: ok and error rates, latency p50 / p95 / p99 / max, in-flight requests. One hour of history per run. | S5 | ✅ |
| R71 | Prometheus `/metrics`, plus `/healthz` and `/readyz`. | S5 (K12) | ✅ |
| R72 | Run events (overlaps, compactions, warnings) are stored with the run. This replaces v1's `overlap_<table>.log` in the working directory. | S3 D14 | ✅ |

## 5. Workload specifications

Exact defaults as served by the image's catalogue (`GET /api/v1/workloads`).

### 5.1 Parameters shared by continuous workloads

| Parameter | Default | Range | Live |
|---|---|---|---|
| `rate` (ops/s, 0 = unlimited) | 1 000 (`datatypes`: 500) | 0 – 1 000 000, capped by `FT_MAX_RATE` | yes |
| `concurrency` (requests in flight) | 64 | 1 – 4 096 | yes |
| `processes` | 1 | 1 – 16, capped by `FT_MAX_PROCESSES_PER_RUN` (8) | no |
| `count` (records, 0 = no limit) | API 0 · UI 100 000 | 0 – 10¹² | no |
| `duration_s` (0 = no limit) | 0 | up to 1 year | no |
| `monitor_every_s` (0 = off) | 10 | 0 – 3 600 | no |
| `consistency` | `LOCAL_QUORUM` | any standard level | no |
| `keyspace` / `create_keyspace` | connection's default / true | name regex (R62) | no |

### 5.2 Catalogue

| Workload | Group | Kind | Default table | `count` counts | Main parameters (defaults) |
|---|---|---|---|---|---|
| `connectivity` | Data | one-shot | — | — | `pings` 20 |
| `write` | Data | continuous | `fake_tickets` | rows | `ttl_mode` none (`ttl_seconds` 86 400) · `index` none / sai / 2i / solr · `lwt` off · `id_type` bigint · `start_id` −1 = resume |
| `read` | Data | continuous | `fake_tickets` | reads | `mode` by_id / by_owner / solr · `id_min` 0 · `id_max` −1 = from cursor or probe · `limit` 10 |
| `mixed` | Data | continuous | `fake_tickets` | writes | `read_pct` 50 (live) |
| `datatypes` | Data | continuous | `fake_events` | rows | `rows_per_partition` 100 · `collection_size` 5 · `blob_bytes` 256 · `ttl_seconds` 0 |
| `overwrite` | Compaction | continuous | `ft_overwrite` | writes | `keys` 100 000 · `distribution` uniform / hotspot (`hot_ops_pct` 90 on `hot_keys_pct` 1) / sequential passes · `update_mode` full_row / one_column · `read_pct` 0 |
| `tombstones` | Compaction | continuous | `ft_queue` | writes | `partitions` 10 · `delete_kind` row / range / cell / ttl · `delete_lag` 1 000 · `range_every` 100 · `row_ttl_seconds` 60 · `read_pct` 0, `read_limit` 10 |
| `timeseries` | Compaction | continuous | `ft_timeseries` | writes | `sensors` 100 · `bucket_minutes` 60 · `ttl_mode` table_default, `ttl_seconds` 3 600 · `long_ttl_pct` 0 · `late_pct` 0, `late_max_min` 60 · `read_pct` 0 |
| `alter_table` | Compaction | one-shot | `fake_tickets` | — | compaction options (default LCS) · `default_ttl` −1 = unchanged |
| `table_stats` | Compaction | one-shot | `fake_tickets` | — | `history_hours` 24 |
| `twcs` | Bug repro | continuous | `tickets_twcs` | main inserts | v1 options: TWCS `window_hours` 1, `timestamp_resolution` MILLISECONDS, `ttl_seconds` 172 800, `gc_grace_seconds` 10 800 · `overlap_every_min` 30, `overlap_lookback_min` 120 · `backdate_every_min` 0 (off), `backdate_offset_min` 360 |

What each compaction workload is for:

- **overwrite**: rewrites a fixed set of keys, so obsolete versions pile up
  across sstables. Compare space and read amplification between STCS, LCS and UCS.
- **tombstones**: queue pattern. Rows are appended, then deleted `delete_lag`
  rows behind (row, range, cell or TTL). Reads from the head of a partition scan
  the tombstones. A low `gc_grace_seconds` (3 600 by default) lets you watch purging.
  When a run stops on `count`, exactly `delete_lag` rows are left live.
- **timeseries**: sensor data with TWCS and a TTL. Mixed TTLs (`long_ttl_pct`) and
  late writes (`late_pct`) are the two things that stop TWCS from dropping expired
  sstables, and both can be switched on.
- **alter_table**: changes strategy, gc_grace or default TTL on an existing
  table. It can be run while another run is writing to that table.
- **table_stats**: reports a table's options and its current state on the cluster
  (§4.7 R63).

### 5.3 Compaction option defaults

| | Strategy | Sub-option defaults | `gc_grace_seconds` |
|---|---|---|---|
| `write`, `mixed`, `overwrite` | STCS | STCS `min/max_threshold` 4 / 32 · LCS `sstable_size_in_mb` 160, `fanout_size` 10 · UCS `scaling_parameters` T4, `target_sstable_size` 1 024 MiB · TWCS 1 HOURS | 864 000 (10 days) |
| `tombstones` | STCS | as above | 3 600 |
| `timeseries` | TWCS | TWCS 5 MINUTES | 600 |
| `alter_table` | LCS | as above | 864 000 |

All of them use `timestamp_resolution` MICROSECONDS, `tombstone_threshold` 0.2,
`unchecked_tombstone_compaction` false, and `existing_table` keep.

## 6. Non-functional requirements

| ID | Requirement | Source | Status |
|---|---|---|---|
| N1 | **One container image** with two roles (`frontend`, `backend`), built on UBI 9. It runs non-root with a read-only root filesystem and `dumb-init` as PID 1. | S1, homelab convention | ✅ podman |
| N2 | **Kubernetes manifests** (kustomize base plus a homelab overlay: step-issuer certificate, LDAP, ingress). Each role runs as one replica with `Recreate`. | S1 | ◐ renders with `kubectl kustomize`; not applied |
| N3 | **No plain-text config files.** Settings come from environment variables, and any secret may be given as `<NAME>_FILE`. | S1 "conf files in plain text… ludicrous" | ✅ |
| N4 | State lives in SQLite on a **block-storage (RWO) volume, not NFS**, because SQLite locking is unreliable on NFS. | S5 (K9) | — deployment guidance |
| N5 | **Graceful shutdown:** on SIGTERM, all runs are stopped, their stats flushed, and they are marked `interrupted`. | S1 | ✅ container test |
| N6 | **Gentle on a small lab:** the default rate is 1 000 ops/s, with the `FT_MAX_RATE` and `FT_MAX_RUNS` guard rails and one-click stop. | S2 | ✅ |
| N7 | **Browser UI without a JavaScript build toolchain.** It works offline and fits a phone screen (390 px). | S5 (K11) | ✅ Playwright |
| N8 | **Tests:** unit and API tests with no database; integration tests against a live Cassandra; the image's `test` target runs the unit suite against the installed package. | S1 "consider building tests" | ✅ 70 unit, 25 integration |
| N9 | **Compatibility:** Cassandra 4.x / 5.0, DSE, Astra. UCS needs Cassandra 5, and the table monitor needs 4.1 or later. | S1, S3 | Cassandra 5.0.9 ✅ · other versions, DSE and Astra ⚠ |

## 7. Out of scope (for now)

- Running the back end on more than one pod. Runs live in one pod's process tree.
  The path to that is k8s Jobs per run (HLD §8).
- Spreading one run's load across machines.
- Role-based permissions beyond "logged-in user", and OIDC login.
- A key-rotation command for `FT_MASTER_KEY`. Losing the key means re-entering
  stored secrets.
- Porting v1's one-off scripts and materialized views (R57).

## 8. Open questions for the owner

| # | Question | Current choice |
|---|---|---|
| Q1 | REPRO2 / v1 TWCS defaults: The objectives note (S2) says a 2-day TTL, while v1's code uses 1 day. v1's code has `gc_grace_seconds` 10 800, while its docs say 3 600. | TTL 172 800 (S2), gc_grace 10 800 (v1 code); both editable per run (K13) |
| Q2 | v1 creates the TWCS table with `timestamp_resolution = MILLISECONDS`, but drivers write timestamps in microseconds. Is that mismatch part of the bug REPRO2 chases? | The repro keeps MILLISECONDS; every other workload defaults to MICROSECONDS |
| Q3 | Should the UI default to a number of records or to *until stopped*? | 100 000 records. Bug repro workloads, and those without a count, default to until stopped |
| Q4 | Should the table monitor be on by default? | On, every 10 s |
| Q5 | What throughput is needed? The brief (S1) allows measuring insert and read speed later. | Rate accuracy is verified at 1.5 k/s. The maximum per process against the homelab cluster has not been measured |

## 9. Not yet verified

These are built but have not been exercised against the real system:

- DSE: Solr index and `solr_query` reads.
- A real Astra connection with an SCB.
- The real homelab LDAP.
- A real step-ca / step-issuer.
- Client-to-cluster TLS.
- Applying the k8s manifests.
- Running `compose.yaml` end to end.
- The table monitor on a multi-node cluster, DSE or Astra.
- Compaction **behaviour** under LCS, UCS and TWCS. Their options are applied and
  checked, but only STCS was observed compacting.
- TWCS expiry being blocked in `timeseries`. This needs a TTL and real time to pass.

Full verification table: [`README.md` § What has been verified](../README.md#what-has-been-verified-2026-11-11).

## 10. Change log

| Date | Change |
|---|---|
| 2026-11-11 | v2 built from S1–S3: service, connection store, auth, rate engine, workloads `connectivity` … `twcs`, image, k8s manifests, tests |
| 2026-11-11 | Owner feedback S4: "How much" (records or duration, progress and ETA), `count` = records written, shared compaction options, compaction workloads, table monitor, `twcs` relabelled as a bug repro |
| 2026-11-12 | This document written |
| 2026-11-17 | Repository restructured: v1 kept on branch `v1` / tag `v1.0`, v2 moved to the root, v1 notes in `docs/v1/`, gitleaks secret scanning added |
