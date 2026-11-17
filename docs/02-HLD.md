# 02 — High-Level Design: fake-tickets v2 ("Kubernetes-friendly simulator")

> Requirements this design answers: [01b — v2 requirements](01b-V2-REQUIREMENTS.md).

## 1. Problem statement

v1 is a batch script: configuration in clear-text INI files, workload chosen by
editing code, one workload per shell, and the process **exits when it is done**
(or never, for TWCS). Dropped into Kubernetes as-is it restarts endlessly and
replays the same ids. v2 turns it into a **long-running service** where the
workload is a *runtime object* (a "run") that users start, tune, and stop from a
browser, while the service itself stays up.

## 2. Goals / non-goals

| Goals | Non-goals (for now) |
|---|---|
| Long-running, restart-safe service; runs are started/stopped via API | Horizontal scaling of the backend across pods |
| Keep v1's Cassandra / DSE / Astra connectivity (incl. SCB upload) | Porting one-off scripts (OpsCenter cleanup, 53 Solr cores, `IN` bench) |
| Secrets encrypted at rest (SQLite), never in argv / env / logs | Multi-tenant RBAC beyond "logged-in user" |
| Selectable workloads, live-adjustable rate (1 → 50 000+ ops/s, or unlimited) | A JS build tool-chain (UI is dependency-free HTML/JS) |
| Each run isolated in its own OS process; many users concurrently | Distributed load generation (k8s Jobs per run — see §8) |
| Optional login (LDAP / local / none) and HTTPS from the homelab smallstep CA | |
| One container image, tests, k8s manifests | |

## 3. Context

```
                 ┌───────────── homelab ─────────────┐
 Browser ──HTTPS─►  frontend  ──HTTPS + bearer──► backend ──CQL(+TLS)──► Cassandra / DSE
   ▲               │  UI, login, connection store │   run manager           │
   │               │  SQLite (encrypted secrets)  │   worker processes ─────┘
   │               └──────────┬───────────────────┘        └──CQL──► Astra (SCB)
   │                          │ LDAP(S) bind
   │                          ▼
   │                    homelab LDAP
   └── certs issued by smallstep CA (files mounted by cert-manager/step-issuer or step CLI)
```

## 4. Building blocks

One image, two roles selected by the command (`faketickets frontend|backend|run`).

### 4.1 Frontend (UI + connection vault)
- Serves the single-page UI and a JSON API used only by the UI.
- **Login** (optional): `FT_AUTH_MODE = none | local | ldap`. Server-side
  sessions in SQLite, `HttpOnly; Secure; SameSite=Strict` cookie.
- **Connection store**: Cassandra / DSE / Astra profiles in SQLite. Non-secret
  fields in clear; secrets (passwords, Astra secret/token, SCB zip, TLS key) are
  encrypted with AES-256-GCM using a per-record key derived by **scrypt(master
  key, per-record random salt)**. Master key comes from a k8s Secret
  (`FT_MASTER_KEY`). This is the "salting" requirement: secrets must be
  recoverable to connect, so they are *encrypted with salted key derivation*,
  not hashed. Local-user passwords (auth mode `local`) *are* salted-hashed (scrypt).
- **Proxy to backend**: when a user starts a run, the frontend decrypts the
  profile and sends the full connection spec to the backend over HTTPS with a
  shared bearer token. The browser never sees the secrets again after saving.

### 4.2 Backend (workload engine)
- HTTP API: list workloads (with parameter schemas), create run, list/get run
  (live stats + time series), **patch run (live rate / concurrency)**, stop run,
  id cursor ("resume where the last run left off"), Prometheus `/metrics`,
  `/healthz`, `/readyz`.
- **Run manager**: each run is a **separate OS process** (`multiprocessing`
  *spawn* context — the Cassandra driver is not fork-safe). Control (rate
  changes, stop) and telemetry (1 s stats) flow over a `multiprocessing.Pipe`.
  Credentials travel through that pipe, never through argv or environment.
- **Run registry** persisted in the backend's SQLite: on restart, runs that were
  `running` are marked `interrupted`, and their last checkpointed `next_id` is
  used to propose a resume point.
- Cap on concurrent runs (`FT_MAX_RUNS`), graceful shutdown on SIGTERM (stop
  all workers, flush stats), `dumb-init` as PID 1 to reap children.

### 4.3 Worker (inside each run process)
- Builds a `Cluster` from the connection spec (Astra SCB / contact points, TLS,
  auth, DC-aware load balancing), runs one workload, reports stats.
- **Rate engine**: deadline/token based dispatcher on top of `execute_async`
  with an in-flight cap. Replaces v1's "execute chunk then sleep" so achieved
  rate ≈ target, and the target can change at any moment.
- Rates above what one Python process can drive (~5–10 k ops/s) are reached
  with `processes > 1` on a run: the run fans out into N worker processes that
  split the rate and stride the id space.

## 5. Workloads (decisions)

| Id | Workload | Kind | Derived from v1 |
|---|---|---|---|
| `connectivity` | Version / topology check: `release_version`, `dse_version`, cluster, DC, peers, latency | one-shot | F2 |
| `write` | Fake tickets insert: STCS/LCS/TWCS, TTL none / per-row, index none / SAI / 2i / Solr (DSE), optional LWT, count / duration / forever | continuous | F4, F6, F8 |
| `read` | Random point reads over the written id range; optional index lookup by `ownedby`; hit/miss counters | continuous | F4 power-of-10 reads |
| `mixed` | Read/write mix with a % slider | continuous | new |
| `datatypes` | Wide-partition table with collections, UDT, uuid/timeuuid, decimal, varint, inet, date/time, blob, duration | continuous | new ("other data types") |
| `overwrite` | **Compaction:** rewrites a fixed key space (uniform / hot set / sequential passes; whole row or one column) → obsolete versions across sstables; space & read amplification per strategy | continuous | new |
| `tombstones` | **Compaction:** queue pattern — append, then row / range / cell delete or TTL `lag` rows behind; optional head reads that scan tombstones; low gc_grace to watch purging | continuous | new |
| `timeseries` | **Compaction:** sensor time-series, TWCS + TTL; mixed TTLs and late (back-dated) writes — what blocks TWCS from dropping expired sstables | continuous | F5 (generalised) |
| `alter_table` | **Compaction:** switch strategy / gc_grace / default TTL on an existing table (under load) | one-shot | new |
| `table_stats` | **Compaction:** options + per-node disk, largest partition / sstable, tombstones per read, running & recent compactions | one-shot | new |
| `twcs` | **Bug repro (REPRO2):** v1's exact TWCS options, **wall-clock** overlap rewrites and optional **back-dated** writes (v1 `oldrecord.sh`) | continuous | F5, F11 |

All continuous workloads share: `rate` (0 = unlimited), `concurrency`,
`consistency`, stop conditions (`count` = records written, `duration_s`, else
forever), `processes`, and an optional compaction monitor (`monitor_every_s`).
Every workload that creates a table takes the same compaction options (strategy
plus its sub-options, gc_grace, tombstone settings, keep/alter an existing table).

## 6. Key design decisions (log)

| # | Decision | Alternatives | Rationale |
|---|---|---|---|
| K1 | Service never exits when a workload ends; runs are API objects | Job-per-run | Fixes the restart loop; simplest operational model in a homelab |
| K2 | One image, two roles | two images | Single build/version; roles scale separately |
| K3 | Backend = **single replica** (`Recreate`) | shared state across replicas | Runs live in one pod's process tree; a 2nd replica would not see them |
| K4 | Process-per-run via `spawn` | threads, asyncio | GIL isolation, driver fork-safety, kill-ability, multi-user |
| K5 | Rate engine: deadline/token dispatch, live-adjustable | v1 chunk+sleep | Accuracy (D7) and slider requirement |
| K6 | New tables use `id bigint`; existing `int` tables detected from schema; run **stops cleanly** with `id_space_exhausted` | wrap-around | Fixes D5 without silently rewriting data |
| K7 | TWCS overlap triggered by **wall clock**, pool chosen from time→id checkpoints | v1 id arithmetic | Correct under any achieved rate / live slider (D6) |
| K8 | Secrets: AES-GCM + scrypt(master, per-record salt) | hashing (impossible: must decrypt), Vault | Meets "salting" requirement, no external dependency |
| K9 | SQLite on **RWO PVC** (block storage), not NFS | Postgres | SQLite locking is unreliable on NFS; Postgres overkill |
| K10 | TLS from mounted files with **hot reload** | restart on renewal | smallstep leaves are short-lived (24 h default) |
| K11 | UI in plain HTML/JS/CSS served by FastAPI | React/Vite | No node tool-chain needed; tiny image; works offline |
| K12 | Prometheus text `/metrics` on backend | push to stdout only | Fits homelab monitoring stack |
| K13 | TWCS workload defaults: TTL **172 800 s** (owner's objectives note: "2 days TTL"), `gc_grace_seconds` **10 800** (v1 code; docs said 3 600) | v1 code TTL 86 400 / docs gc_grace 3 600 | The objectives note is the stated requirement for TTL; gc_grace has no stated requirement so the running value is kept. Both are per-run parameters — **owner to confirm** |
| K14 | `/metrics` unauthenticated; backend reachable only from frontend/monitoring via NetworkPolicy | bearer on /metrics | Prometheus scraping without secrets; exposes run ids and rates only |
| K15 | Finished runs: last 50 kept in memory (1 h series), older served from SQLite | keep all | Service is long-lived; bounded memory |
| K16 | `count` means **records written** (per-workload `count_kinds`); UI offers Until stopped / Number of records / Duration with estimates and a progress bar | count every request | Users think in rows; reads and deletes in mixed workloads must not consume the budget |
| K17 | Compaction workloads model *data shapes* (overwrites, deletes, TTL / late data) and share one structured compaction option set; existing tables are kept (with a warning) or explicitly ALTERed | one workload per strategy; free-form compaction map | Strategies only differ when there is something to merge; free-form maps would put user text into DDL |
| K18 | Compaction visibility from CQL virtual tables + `compaction_history`, queried per node, best-effort | JMX / nodetool sidecar | Works with only a CQL connection; degrades to "not available" on Astra / older versions |
| K19 | The overlap / back-dated workload is labelled a **bug reproduction** (REPRO2) and keeps v1's exact options (`timestamp_resolution = MILLISECONDS`); general TWCS testing uses `timeseries` (MICROSECONDS default) | make overlaps a core feature | Owner: out-of-order inserts were only a repro attempt for one bug |

## 7. Deployment view (Kubernetes)

```
Namespace fake-tickets
 ├─ Deployment frontend (1 replica, Recreate) ── PVC frontend-data (RWO, SQLite)
 │     └─ Service frontend :8443  ◄── Ingress / LB (TLS passthrough or re-encrypt)
 ├─ Deployment backend  (1 replica, Recreate) ── PVC backend-data (RWO, runs DB, scratch SCB)
 │     └─ Service backend :8444 (ClusterIP only)
 ├─ Secret fake-tickets-secrets  (FT_MASTER_KEY, FT_BACKEND_TOKEN, LDAP bind pw)
 ├─ Certificate (cert-manager + step-issuer) → Secret fake-tickets-tls (tls.crt/tls.key/ca.crt)
 └─ ConfigMap fake-tickets-config (auth mode, LDAP URL/base DN, limits)
```

Security context: non-root UID, read-only root FS, no privilege escalation, all
capabilities dropped; writable paths are the PVC mount and an `emptyDir` `/tmp`.

## 8. Evolution paths
- **Distributed load**: backend could create a k8s `Job` per run (same image,
  `faketickets run` command) instead of a local process — the worker already
  supports headless mode, only the run manager would change.
- **Postgres** store if the frontend needs >1 replica.
- **OIDC** (smallstep / Authelia) as a 4th auth mode.

## 9. Risks
| Risk | Mitigation |
|---|---|
| A runaway run saturates the lab cluster | default rate 1 000 ops/s; `FT_MAX_RATE` ceiling; one-click stop; stop-all on shutdown |
| Master key loss ⇒ stored secrets unrecoverable | documented; key rotation command (`faketickets rotate-key`) — future |
| Pod restart kills runs | runs marked `interrupted`; resume cursor offered |
| Python throughput ceiling | `processes` fan-out per run |
