> **v1 document (historical).** Describes the original scripts, kept on branch `v1` /
> tag `v1.0`. File references point at that version.

# fake-tickets

> ⚠️ **For testing and reproduction purposes only.**
> These scripts intentionally implement anti-patterns and destructive write
> behaviours to stress-test Apache Cassandra, DataStax Enterprise (DSE), and
> DataStax Astra DB clusters. Do not use in production.

A Python workload generator for Cassandra/DSE/Astra. Inserts synthetic ticket
records at a controlled rate, exercises compaction strategies (STCS, TWCS),
TTL expiry, SAI/Solr indexing, LWT, and Materialized Views. Designed for
support engineering, compaction analysis, and issue reproduction.

---

## Quick start

### 1. Configure

Copy the example config and fill in your cluster details:

```bash
cp conf/conf_dummy.ini conf/conf_myenv.ini
# edit conf/conf_myenv.ini
```

Key fields:

| Field | Description |
|---|---|
| `isAstra` | `true` for Astra DB, `false` for on-premise DSE/Cassandra |
| `ksname` | Target keyspace (created automatically if absent) |
| `dcname` | Datacenter name for `NetworkTopologyStrategy` (on-premise only) |
| `host1` | Seed node IP (on-premise only) |
| `scb` / `token` / `secret` | Astra Secure Connect Bundle path and credentials |

### 2. Run

```bash
# Insert 10 records starting at id=0 into table 'fake_tickets'
python src/insert-astra.py -c conf/conf_myenv.ini -t fake_tickets -v 0 10

# Use an increment file to pick up where the last run left off
python src/insert-astra.py -c conf/conf_myenv.ini -t fake_tickets -i state.txt
```

---

## CLI reference

```
python src/insert-astra.py -c <conf> -t <table> (-v <start> <count> | -i <incfile>)
```

| Flag | Description |
|---|---|
| `-c / --conf` | Path to the `.ini` configuration file |
| `-t / --table` | Table name to insert into |
| `-v / --values` | Explicit start ID and record count, e.g. `-v 0 1000000` |
| `-i / --incfile` | Path to a state file (`start count` on one line); automatically advances after each run |

---

## Workflows

### Standard workflow — `fake_tickets_workflow`

Inserts records into a plain table with `id` as partition key. Performs
single-row reads and counts at power-of-10 milestones (1, 10, 100 …).
Uses paced asynchronous ingestion via `execute_concurrent_with_args`.

Activate in `src/insert-astra.py`:
```python
ft.fake_tickets_workflow(session, ksname, startval, numrec, tblname)
```

### TWCS / TTL workflow — `fake_tickets_twcs_workflow`

Inserts into a table configured with **Time Window Compaction Strategy**
(1-hour windows) and a **default TTL of 2 days**. Runs an infinite loop
from `startval`, advancing the ID counter continuously.

```python
ft.fake_tickets_twcs_workflow(session, ksname, startval, numrec, tblname)
```

Key parameters (all have defaults):

| Parameter | Default | Description |
|---|---|---|
| `ttl_seconds` | `86400` | Row TTL in seconds |
| `concurrency` | `50` | Max in-flight requests per chunk |
| `target_ops` | `1000` | Target writes/second |
| `overlap_interval_min` | `30` | Minutes of ingestion between overlap events |
| `overlap_lookback_min` | `120` | Minutes of history forming the overlap candidate pool |

**Overlap injection:** every `overlap_interval_min` minutes worth of writes
(measured in IDs at `target_ops`), one record from the past
`overlap_lookback_min` minutes of history is rewritten. This reproduces
cross-TWCS-window partition overwrites. All overlap events are timestamped
and appended to `overlap_<tblname>.log`. See [`REPRO2.md`](REPRO2.md) for
the full concurrent two-process reproduction plan.

---

## Insert speed tuning

Writes are dispatched asynchronously via `insert_paced_async`, which batches
rows into chunks and fires them concurrently using `execute_concurrent_with_args`
from the Cassandra driver. Two parameters control the effective throughput:

| Parameter | Where to set | Effect |
|---|---|---|
| `concurrency` | workflow call or default in `faketickets.py` | Number of in-flight requests sent simultaneously per chunk. Higher values increase throughput but add pressure on the coordinator and client thread pool. |
| `target_ops` | workflow call or default in `faketickets.py` | Target writes per second. After each chunk of `concurrency × 2` rows is flushed, the driver sleeps for `chunk_size / target_ops` seconds to smooth the rate. Set to `0` to disable throttling entirely. |

The chunk size is always `concurrency × 2`, so the two parameters interact:
a higher `concurrency` means larger chunks and coarser pacing steps, while a
lower `target_ops` introduces longer inter-chunk sleeps.

**Typical starting points:**

```python
# ~200 writes/sec — light load, resource-constrained node
ft.fake_tickets_twcs_workflow(..., concurrency=50, target_ops=200)

# ~1 000 writes/sec — default TWCS target
ft.fake_tickets_twcs_workflow(..., concurrency=50, target_ops=1000)

# Uncapped — maximum throughput, no sleep between chunks
ft.fake_tickets_twcs_workflow(..., concurrency=100, target_ops=0)
```

Note that `target_ops` also drives the overlap injection schedule: the
interval and lookback pool are both expressed as `target_ops × minutes × 60`
IDs, so changing the rate automatically rescales the overlap timing.

---

## Running two processes concurrently (REPRO2)

To reproduce cross-window compaction artifacts with two concurrent ingestion
processes, launch each with a non-overlapping `startval`. Each process
independently fires overlap rewrites into its own past history on a
rate-derived schedule.

```bash
# Process A — IDs from 0
python src/insert-astra.py -c conf/conf_myenv.ini -t ads_twcs -v 0 999999999

# Process B — IDs from 10 000 000 (clear of A's 2-hour pool at 1k/s)
python src/insert-astra.py -c conf/conf_myenv.ini -t ads_twcs -v 10000000 999999999
```

See [`REPRO2.md`](REPRO2.md) for the full rationale, arithmetic, and scenario
variants (30-minute and 1h30m delay).

---

## Source layout

```
src/
  insert-astra.py     CLI entry point, connection builder, keyspace provisioning
  faketickets.py      Table schemas, insert/read helpers, TWCS workflow
  helper.py           Config parser, ms timestamps, CPU range utilities
  cassqueries.py      Supplementary queries: dual-ID, IN benchmarks, Solr tables
  deletebackups.py    OpsCenter backup metadata cleanup helper
conf/
  conf_dummy.ini      Configuration template
test/
  reproduction/
    track_sstable.py  Follow an SSTable through successive compactions in debug logs
    TRACKER.md        Manual compaction tracking example and context
  PERFORMANCE.md      Performance notes
  twcs_analysis/
    ANALYSIS.md       TWCS compaction behaviour analysis
```

---

## Further reading

| Document | Content |
|---|---|
| [`WIKI.md`](WIKI.md) | Full architecture, module breakdown, execution flow, paced async ingestion |
| [`TTL-ENHANCEMENT.md`](TTL-ENHANCEMENT.md) | TWCS + TTL table design proposal and CQL schema |
| [`OPTIMIZATION.md`](OPTIMIZATION.md) | Code review findings and implemented optimizations |
| [`REPRO2.md`](REPRO2.md) | Concurrent two-process reproduction plan with overlap injection design |
