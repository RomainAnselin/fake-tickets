# 03 — Low-Level Design: fake-tickets v2

Implementation lives at the repository root. This document maps the HLD onto modules,
data structures, protocols and algorithms. Paths are relative to the repository root.

## 1. Package layout

```
faketickets/
  __main__.py          CLI: backend | frontend | run | workloads | genkey | hash-password
  config.py            Settings.from_env(role) — FT_* env vars, *_FILE variants for secrets
  server.py            uvicorn launcher + CertReloader (TLS hot reload)
  models.py            ConnectionSpec (pydantic) — shared frontend → backend → worker
  worker.py            worker_main(): process entry, run_loop(), control/telemetry threads
  engine/
    ratelimit.py       RateLimiter — deadline/token limiter, live set_rate()
    dispatcher.py      Dispatcher — execute_async + live in-flight cap; Op dataclass
    histogram.py       log-bucket latency histogram (mergeable across processes)
    stats.py           Recorder — per-interval counters drained every second
    ids.py             IdAllocator (stride/offset, overflow guard), INT_MAX/BIGINT_MAX
  db/cluster.py        build_cluster / connect (Astra SCB, TLS, auth, DC-aware LBP),
                       ensure_keyspace, local_info, column_type
  workloads/
    base.py            Param schema, Workload contract, RunContext, identifier checks,
                       wait_for_table (multi-process schema sync)
    datagen.py         v1-compatible ticket rows, owner pool, padding
    connectivity.py    W1   tickets.py  W2 write / W3 read / W4 mixed
    twcs.py            W5 (bug repro)   datatypes.py W6       __init__.py  REGISTRY
    kit.py             one import for workload authors (docs/04-ADDING-A-WORKLOAD.md)
    tableopts.py       compaction_params() / table_options_cql() / create_or_reconcile()
    compaction.py      overwrite, tombstones, timeseries, alter_table, table_stats
    monitor.py         CompactionMonitor — system_views + compaction_history, per node
  backend/
    app.py             FastAPI routes, bearer auth, Prometheus text renderer
    manager.py         RunManager / Run / Proc — spawn, aggregate, control, persist
    store.py           RunStore (SQLite: runs, events)
  frontend/
    app.py             FastAPI routes, CSRF guard, security headers, static UI
    store.py           FrontStore (SQLite: connections, users, sessions)
    crypto.py          SecretBox (scrypt + AES-GCM), password hashing, master key
    auth.py            LDAP (ldap3) / local authentication
    backend_client.py  httpx client (bearer, CA-verified TLS, optional client cert)
    static/            index.html, style.css, app.js (no build step, CSP-safe)
tests/                 unit + API tests (no DB) and integration tests (live Cassandra)
deploy/k8s/            kustomize base + overlays/holonet (lab: ArgoCD, ESO, LDAP, Traefik)
Dockerfile, compose.yaml
```

## 2. Workload contract

```python
class Workload:
    name, title, description           # catalogue
    kind = "continuous" | "oneshot"
    category = "Data" | "Compaction" | "Bug repro"   # UI grouping
    uses_ids: bool                      # contributes to the resume cursor
    count_kinds: frozenset | None       # op kinds that count towards `count` (None = all)
    approx_row_bytes: int               # UI data-volume estimate (0 = unknown)
    needs_session: bool = True          # False only for test doubles
    params: list[Param]                 # schema rendered by the UI

    @classmethod validate(raw) -> dict  # coerce, range-check, identifier-check, cross-check
    setup(ctx)                          # DDL (shard 0 only) + prepare statements
    next_op(ctx) -> Op | None           # continuous: one request (None = done)
    tick(ctx, now) -> list[Op]          # ~5 Hz hook for scheduled extra ops (TWCS events)
    run_once(ctx) -> dict               # oneshot
```

`Param(name, type, default, label, help, min, max, choices, group, visible_if, live, pattern)`.
`group ∈ {workload, compaction, rate, stop, advanced}` drives UI placement; `pattern`
is a full-match regex for `str` values that reach DDL (e.g. UCS `scaling_parameters`); `visible_if` hides
dependent fields (`{"ttl_mode": ["per_row"]}`, `{"overlap_every_min": [{"gt": 0}]}`);
`live=True` marks parameters that may be PATCHed while running (rate, concurrency,
`read_pct`).

All workloads share `rate` (0 = unlimited), `concurrency`, `processes`, `count`,
`duration_s`, `monitor_every_s`, and (where relevant) `keyspace`, `table`, `consistency`, `start_id`.

**`count` = records, not requests.** `run_loop` only counts ops whose kind is in
`count_kinds` (`{"write"}` for mixed / overwrite / tombstones / timeseries), so reads
and deletes never use up the budget. The worker publishes `progress.counted`; the
manager sums it over shards and exposes `counted`, `count_target`,
`duration_target` for the UI progress bar / ETA. Id workloads reject
`start_id + count − 1 > id limit` in `setup_ids()` (e.g. 1 M records into an `int`
table that has 500 k ids left fails up front, not half-way).

### Compaction options (`tableopts.py`)
`compaction_params(default, gc_grace_default, twcs_unit, twcs_size)` adds, in the
`compaction` group: strategy (stcs/lcs/ucs/twcs); STCS `min/max_threshold`; LCS
`sstable_size_in_mb`, `fanout_size`; UCS `scaling_parameters` (regex
`^[TLN][0-9]{0,3}(,[TLN][0-9]{0,3}){0,15}$`), `target_sstable_size`; TWCS window
unit/size and `timestamp_resolution`; `gc_grace_seconds`, `tombstone_threshold`,
`unchecked_tombstone_compaction`; and `existing_table = keep | alter`.
`create_or_reconcile()` creates the table with these options; when it already
exists (`CREATE … IF NOT EXISTS` would silently ignore them) it either emits a
warning naming the current strategy (`keep`) or runs `ALTER TABLE … WITH` (`alter`).

### Compaction monitor (`monitor.py`)
On shard 0, when `monitor_every_s > 0`, a thread queries on **every live host**
(`Session.execute(host=…)` — virtual tables are node-local):
`system_views.disk_usage`, `max_partition_size`, `max_sstable_size`,
`tombstones_per_read`, `sstable_tasks` and `system.compaction_history`
(`compacted_at > run start`, `ALLOW FILTERING` on a small local table). Results go to
`progress.cmp_*` (sum of disk; max of sizes / tombstones; running tasks and % done;
completed compactions with bytes in→out) and each new history row becomes a run
event. A view missing on the cluster (Astra, DSE 6.8, Cassandra < 4.1) is disabled
after its first failure with one info event. Caveats: sizes are whole MiB;
`tombstones_per_read` is the node's decaying histogram (last minutes, all clients),
and purgeable tombstones (older than gc_grace) are not counted by Cassandra;
`compaction_history` is matched by table *name*, so a dropped-and-recreated table
shows its predecessor's compactions.
Keyspace / table names are restricted to `^[A-Za-z][A-Za-z0-9_]{0,47}$` because
they are interpolated into DDL.

### Workload specifics

| Workload | Schema | Ops |
|---|---|---|
| `connectivity` | — | `system.local` (+`dse_version`), N × `SELECT now()` latency, topology, keyspaces |
| `write` | `id bigint` (or existing `int`) PK + v1 columns; compaction options above; `default_time_to_live` or `USING TTL`; index sai/2i/solr on `ownedby`; optional `IF NOT EXISTS` | `INSERT` |
| `read` | existing table | `WHERE id=?` (range from cursor or exponential/binary probe), `WHERE ownedby=?`, or `solr_query`; counts hit/miss |
| `mixed` | as write | `read_pct`% point reads over ids written so far, rest inserts |
| `overwrite` | tickets shape, `id bigint`; any strategy | `INSERT` of all columns or `UPDATE notes` on keys drawn uniform / hotspot (x % ops on y % keys) / sequential passes; optional point reads |
| `tombstones` | `((bucket int), seq bigint) payload` | append `seq`; delete `seq − lag` as row / cell (`SET payload = null`) — the delete is sent *before* the write so a count stop leaves exactly `lag` live rows — or range (`seq < x` every N rows per partition), or `USING TTL`; reads `LIMIT n` from the partition head scan the tombstones |
| `timeseries` | `((sensor int, bucket bigint), ts, seq)` DESC, TWCS default (5 MINUTES), `default_time_to_live` | `INSERT … USING [TTL ? AND] TIMESTAMP ?`; `long_ttl_pct` rows get another TTL (0 = never); `late_pct` rows get `ts`/write-time up to `late_max_min` in the past; optional reads of a sensor's current bucket |
| `alter_table` | existing table | one-shot `ALTER TABLE … WITH compaction/gc_grace[/default TTL]`; returns before/after |
| `table_stats` | existing table | one-shot: options + one CompactionMonitor sample over `history_hours` |
| `twcs` (bug repro) | v1's exact options: TWCS window N h, `timestamp_resolution = MILLISECONDS`, `default_time_to_live`, `gc_grace_seconds` | inserts + overlap rewrites + back-dated `USING TIMESTAMP` inserts |
| `datatypes` | `fake_events((device_id uuid), event_time timeuuid)` + 19 typed columns incl. UDT `ft_address`, tuple, set/list/map, duration | `INSERT`, N rows per partition |

## 3. Worker process

```
worker_main(conn, job)
 ├─ control thread    conn.poll/recv → limiter.set_rate / dispatcher.set_concurrency /
 │                     ctx.params.update(live) / stop_event
 ├─ reporter thread   every 1 s: Recorder.drain() → {"type":"stats", ok, err, extra,
 │                     errtypes, hist, hist_max, inflight, progress}
 ├─ main thread       connect → wl.setup → send state → run_once | run_loop → drain
 └─ driver loop       (libev) callbacks → Recorder.ok/err, Dispatcher slot release
```

`job = {run_id, proc_index, proc_count, workload_ref, params, spec, rate, concurrency, log_level}`
— pickled through the spawn pipe, so the connection secrets never appear in
argv, environment or logs. SCB / client key PEMs are written `0600` in a per-run
`mkdtemp` dir (tmpfs in k8s) and removed in `finally`.

**run_loop** (per shard):
```
while not stop:
    if duration reached → "duration_reached";  if issued ≥ count share → "count_reached"
    every 200 ms: submit wl.tick(ctx, now)
    slots = dispatcher.free_slots(wait=50 ms)        # in-flight cap
    n = limiter.acquire(min(slots, 512, remaining))   # rate cap
    for n: op = wl.next_op(ctx) → dispatcher.submit(op)   (IdSpaceExhausted → "id_space_exhausted")
drain in-flight (30 s)
```

**RateLimiter**: permits due = `floor((now − anchor) × rate + 1e-9) − granted`.
`set_rate` re-anchors (no burst on slider moves). If the consumer stalls, debt is
capped to `rate × 0.1 s` (no catch-up storm). `rate = 0` → unlimited. Verified to
±1 % from 1 to 100 000 ops/s with a fake clock, ±10 % on a real clock and against
Cassandra.

**Multi-process runs**: shard *i* of *N* gets `rate/N`, ids `start + i + k·N`,
`count` split with `share()`. Only shard 0 runs DDL; others `wait_for_table`
(metadata refresh **and** a real `SELECT … LIMIT 1`, because `system_schema` can
show a table a few ms before the node serves it — found in testing).

**Id overflow** (`IdAllocator`): limit from the table's actual key type
(`int` → 2 147 483 647). Next id beyond the limit raises `IdSpaceExhausted` → run
ends cleanly with that reason; nothing is wrapped or overwritten.

**TWCS scheduler**: every second `(t, last_id)` is checkpointed (deque pruned to
lookback + 60 s). Overlap at `now ≥ next_overlap`: pool = `[id at first checkpoint
≥ now − lookback, last_id]` (whole run if history is shorter — v1 semantics),
random id rewritten with the v1 `*** OVERLAP REWRITE … ***` marker. Back-dated:
fresh id written `USING TIMESTAMP (now − offset)` with `time` = the same instant.
Next fire = `prev + every`, or `now + every` after a clock jump (no bursts). Every
event goes to the run's event log (replaces v1's `overlap_<table>.log` in CWD).

## 4. Backend

### 4.1 Run manager state machine

```
starting ──state msg──► running ──stop()──► stopping ──all shards done──► stopped
    │                      │                                   (shutdown) ► interrupted
    │                      ├─ all shards done (count/duration/id space) ──► finished
    └──── error / crash ───┴─ any shard failed|crashed (others are stopped) ► failed
backend restart: starting|running|stopping in SQLite ─────────────────────► interrupted
```

* One reader thread per shard (`conn.recv()`); EOF without `done` ⇒ `crashed`
  (exit code reported).
* Rates are computed from each shard's **own** message timestamps
  (`ok / (t − t_prev)`), summed across shards — no jitter from 1 s tick
  misalignment. A 1 s ticker appends `{t, rate, err_rate, rate_<kind>, target,
  inflight, p50, p95, p99, max}` to a 3 600-point ring buffer and checkpoints
  `next_id`/totals to SQLite every 5 s.
* `stop()` sends `stop` to each shard; a reaper escalates to SIGTERM/SIGKILL after
  20 s only for shards that never reported `done`.
* `shutdown()` (lifespan exit on SIGTERM): stop all, join ≤ 25 s, terminate
  stragglers, persist as `interrupted / backend_shutdown`. In the image,
  `dumb-init --single-child` delivers SIGTERM to the server only.

### 4.2 Resume cursor
`runs(connection_ref, target=keyspace.table, next_id)`;
`cursor = MAX(next_id)`. On create, `start_id = -1` → cursor or 0; `id_max = -1`
(read) → cursor − 1 (else the worker probes). `connection_ref` is the frontend
connection's immutable uid, so renaming a connection keeps its history.

### 4.3 SQLite (`$FT_DATA_DIR/backend.db`, WAL)
`runs(id, workload, params JSON, connection_ref, connection_name, target, owner,
label, state, reason, error, result JSON, created_at, started_at, ended_at,
next_id, total_ok, total_err)`, `events(run_id, t, level, msg)`. **No secrets.**

### 4.4 HTTP API (bearer token `FT_BACKEND_TOKEN` when set)

| Method | Path | Body / query | Notes |
|---|---|---|---|
| GET | `/healthz`, `/readyz` | | unauthenticated |
| GET | `/metrics` | | Prometheus text; unauthenticated (no secrets) |
| GET | `/api/v1/workloads` | | catalogue with param schemas |
| POST | `/api/v1/runs` | `{workload, params, connection, connection_ref, connection_name, owner, label}` | 201 run view · 422 validation · 429 `FT_MAX_RUNS` |
| GET | `/api/v1/runs` | `?owner=` | live + persisted |
| GET | `/api/v1/runs/{id}` | `?since=<t>` | incremental series |
| PATCH | `/api/v1/runs/{id}` | `{rate?, concurrency?, params?}` | only `live` params |
| POST | `/api/v1/runs/{id}/stop` | | |
| DELETE | `/api/v1/runs/{id}` | | finished runs only |
| GET | `/api/v1/cursor` | `connection_ref, keyspace, table` | |

## 5. Frontend

### 5.1 Connection vault (`$FT_DATA_DIR/frontend.db`)
`connections(id, uid, name UNIQUE, kind, public JSON, secret BLOB, created_by, …)`.
`public` = non-secret ConnectionSpec fields + `_has` flags + `scb_filename`.
`secret` = `0x01 ‖ salt(16) ‖ nonce(12) ‖ AES-256-GCM(json(secrets), key=scrypt(master, salt, n=2¹⁴,r=8,p=1), aad="fake-tickets/connection/v1/"+uid)`.
Secret fields: `password, astra_client_secret, astra_token, astra_scb_b64, tls_key_pem`.
On update, empty secret fields keep the stored value. The decrypted spec only
exists in memory long enough to POST it to the backend over TLS.

`users(username, password_hash="scrypt$n$r$p$salt$dk")`, `sessions(sha256(token), username, expires_at)`.

### 5.2 Auth
* `none` — owner = `X-Forwarded-User` or `anonymous` (use behind an auth proxy).
* `local` — `FT_ADMIN_USER` / `FT_ADMIN_PASSWORD` bootstrap user.
* `ldap` — template bind (`FT_LDAP_USER_DN_TEMPLATE`) or service-bind search
  (`FT_LDAP_BASE_DN` + filter); RDN/filter escaping; empty passwords refused
  (unauthenticated-bind trap); optional group (`member`/`uniqueMember`/`memberUid`);
  LDAPS / StartTLS verified with `FT_LDAP_CA_FILE`.
* Session cookie `ft_session`: `HttpOnly; SameSite=Strict; Secure` (when TLS).
* CSRF: non-GET `/api/*` requires `X-Requested-With: fake-tickets`.
* Headers: strict CSP (`'self'` only — the UI never uses inline script/style),
  `X-Frame-Options: DENY`, `nosniff`, `no-referrer`, HSTS under TLS.

### 5.3 UI API (same-origin, cookie session)
`GET /api/me`, `POST /api/login|logout`, `GET|POST /api/connections`,
`PUT|DELETE /api/connections/{id}` (multipart: `data` JSON + optional `scb` zip,
validated to contain `config.json`), `GET /api/workloads`, `GET|POST /api/runs`,
`GET|PATCH /api/runs/{id}`, `POST /api/runs/{id}/stop`, `DELETE /api/runs/{id}`,
`GET /api/cursor?connection_id&keyspace&table`. Deep link: `/#run=<id>`.

### 5.4 Rate control UX
Log-scale slider 1 → 100 000 ops/s, numeric box, presets (Trickle 10, Slow &
steady 100, 1k, 5k, 20k, Unlimited), live estimate of rows/h, raw bytes/h
(≈110 B/row, [PERFORMANCE.md](PERFORMANCE.md)) and rows/day, and a hint to raise
`processes` above ~6 k ops/s. The same widget drives live PATCHes on a running
run (committed on slider release).

### 5.5 "How much" UX
A segmented control: **Until stopped / Number of records / Duration** (labels
follow the workload, e.g. "Number of writes", "Number of reads"). Records accept
`250000`, `250k`, `1.5M`, `2B` plus presets 1k → 100M; duration accepts `90s`,
`10m`, `1.5h`, `2d` plus presets 1 min → 7 days. The estimate line combines the
rate, the write share (`read_pct`, one delete per write for row/cell tombstones) and
`approx_row_bytes + payload` into time-to-complete or records-in-duration and raw
volume. The widget writes hidden `count` / `duration_s` inputs. Running runs show a
progress bar with an ETA computed from the observed `counted` rate; the runs table
has a progress column. A "Table on the cluster" tile row shows the `cmp_*` monitor
values. Workload cards are grouped by `category`.

## 6. TLS
`server.serve()` builds uvicorn's SSLContext from `FT_TLS_CERT_FILE/KEY_FILE`
(+`FT_TLS_CA_FILE`, optional `FT_TLS_CLIENT_AUTH` for mTLS) and starts
`CertReloader`: every `FT_TLS_RELOAD_INTERVAL` s it hashes the files and, on
change, calls `load_cert_chain` on the live context (new handshakes get the new
leaf). Unreadable / half-written files keep the current cert. The frontend
verifies the backend with the same CA and presents its own cert.

## 7. Configuration reference

| Variable | Role | Default | Meaning |
|---|---|---|---|
| `FT_HOST` / `FT_PORT` | both | `0.0.0.0` / 8443 fe, 8444 be | listen address |
| `FT_DATA_DIR` | both | `/data` | SQLite + generated master key |
| `FT_LOG_LEVEL` | both | `INFO` | |
| `FT_TLS_CERT_FILE` / `FT_TLS_KEY_FILE` / `FT_TLS_CA_FILE` | both | — | enables HTTPS; CA verifies peers |
| `FT_TLS_CLIENT_AUTH` | both | false | require client certificates |
| `FT_TLS_RELOAD_INTERVAL` | both | 60 | seconds between cert checks |
| `FT_BACKEND_TOKEN` | both | — | shared bearer token |
| `FT_MAX_RUNS` / `FT_MAX_PROCESSES_PER_RUN` / `FT_MAX_RATE` | backend | 8 / 8 / 0 | guard rails |
| `FT_BACKEND_URL` | frontend | `http://127.0.0.1:8444` | |
| `FT_MASTER_KEY` | frontend | generated into `$FT_DATA_DIR/master.key` | ≥16 chars |
| `FT_AUTH_MODE` | frontend | `none` | `none` / `local` / `ldap` |
| `FT_ADMIN_USER` / `FT_ADMIN_PASSWORD` | frontend | — | local mode bootstrap |
| `FT_SESSION_TTL_HOURS` / `FT_COOKIE_SECURE` / `FT_MAX_UPLOAD_MB` | frontend | 12 / auto / 20 | |
| `FT_LDAP_URL`, `FT_LDAP_USER_DN_TEMPLATE`, `FT_LDAP_BASE_DN`, `FT_LDAP_USER_FILTER`, `FT_LDAP_BIND_DN`, `FT_LDAP_BIND_PASSWORD`, `FT_LDAP_REQUIRED_GROUP`, `FT_LDAP_START_TLS`, `FT_LDAP_CA_FILE` | frontend | | LDAP |

Any secret may be supplied as `<NAME>_FILE=/path`.

## 8. Test plan (implemented)

| Suite | What | Needs |
|---|---|---|
| `test_engine.py` | limiter accuracy 1→100k/s, live change, debt cap, real-clock; histogram; ids overflow/stride; dispatcher in-flight cap & error accounting | — |
| `test_workloads.py` | registry/defaults, validation & identifier injection, v1 row shape, ConnectionSpec rules, TWCS 3 h simulation at varying rate (wall-clock cadence, pool ≤ lookback), back-dated timestamps, clock-jump | — |
| `test_backend_api.py` | real spawned workers (DB-less dummy): lifecycle, live PATCH, stop, cursor/resume, duration/count, one-shot, failure, limits (422/429), metrics, restart ⇒ interrupted, shutdown ⇒ interrupted, no secrets persisted | — |
| `test_frontend.py` | SecretBox (salt, AAD, tamper, wrong key), hashing, vault encryption on disk + update semantics, sessions, CSRF, CSP, CRUD + run proxy (mock backend), SCB validation, local login, backend down | — |
| `test_ldap_tls.py` | LDAP template/search bind, group, empty-password, filter injection (ldap3 mock); TLS hot reload incl. half-written key | — |
| `test_compaction.py` | compaction option maps per strategy, UCS pattern injection, STCS cross-check; tombstone op ordering (delete before write, exactly `lag` live), range/TTL kinds; overwrite sequential / hotspot (≈90 %) / sharded passes; `count_kinds` counting in `run_loop`; shard progress merge | — |
| `test_integration.py` | connectivity; write rate ±10 % & row count; read hits; TWCS+TTL+SAI with 2 processes; `int` overflow stop; mixed; datatypes; TWCS overlap/back-dated (WRITETIME); option matrix; count = rows written (mixed); count past `int` limit rejected; bigint ids > 2³¹; overwrite LCS / UCS (row count = keys, options applied, monitor publishes); tombstones row/range/cell/ttl (live rows, gc_grace); timeseries mixed TTL + late writes (TTL null share, WRITETIME); keep-warning → `alter_table` → `existing_table=alter`; `table_stats` | Cassandra |

The `Dockerfile` `test` target runs the non-DB suites against the **installed**
package inside the image.
