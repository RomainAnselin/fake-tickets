> **v1 document (historical).** Describes the original scripts, kept on branch `v1` /
> tag `v1.0`. File references point at that version.

# Fake Tickets - Architecture & Application Wiki

## 1. Overview & Purpose
The `fake-tickets` application is a Python-based utility developed for DataStax Enterprise (DSE), Apache Cassandra, and DataStax Astra DB environments. Its primary purpose is to:
- Generate and ingest synthetic workload data (tickets, logs, ad-hoc metadata) into Cassandra/Astra clusters.
- Test and benchmark various storage, indexing, and compaction mechanics (e.g., SAI, Solr search indexes, Materialized Views, LWT, TWCS, TTL).
- Simulate both recommended data-access patterns and common anti-patterns (e.g., large `IN` queries, full scans, single partition hotspots) to evaluate cluster stability under constrained resource conditions.

---

## 2. Architecture & Component Diagram

```
+-------------------------------------------------------------+
|                      Configuration                          |
|             (conf_dummy.ini / conf_<env>.ini)               |
|   - isAstra (true/false)                                    |
|   - Keyspace & Datacenter names                             |
|   - Astra SCB + Tokens OR Host IPs + Auth credentials       |
+------------------------------+------------------------------+
                               |
                               v
+-------------------------------------------------------------+
|                     CLI Entry Point                         |
|                   (insert-astra.py)                         |
|   - CLI argument parser (-c conf, -t table, -v/-i records)  |
|   - State tracking via offset increment files               |
|   - Cluster & Session initialization                        |
|   - Keyspace provisioning (NetworkTopologyStrategy)         |
+------------------------------+------------------------------+
                               |
                               v
+-------------------------------------------------------------+
|                    Workload Modules                         |
|                                                             |
|  +---------------------+   +---------------------+          |
|  |   faketickets.py    |   |   cassqueries.py    |          |
|  | - Table creation    |   | - System info query |          |
|  | - Prepared inserts  |   | - Solr encrypted    |          |
|  | - LWT / TTL inserts |   | - Dual-ID / IN test |          |
|  | - Reads & Counts    |   | - Table truncation  |          |
|  | - SAI / Solr Index  |   +---------------------+          |
|  | - TWCS/TTL workflow |                                    |
|  +---------------------+                                    |
|                                                             |
|  +---------------------+   +---------------------+          |
|  |      helper.py      |   |  deletebackups.py   |          |
|  | - Config parser     |   | - OpsCenter backup  |          |
|  | - Timestamps (ms)   |   |   metadata cleanup  |          |
|  | - CPU / range calc  |   +---------------------+          |
|  | - Offset recorder   |                                    |
|  +---------------------+                                    |
+------------------------------+------------------------------+
                               |
                               v
+-------------------------------------------------------------+
|                   Target Database Cluster                   |
|   - Apache Cassandra / DataStax Enterprise (DSE) / Astra DB |
|   - Keyspaces, Tables (STCS, LCS, TWCS), Secondary/SAI/Solr |
+-------------------------------------------------------------+
```

---

## 3. Module Breakdown

### `insert-astra.py` (Main Orchestrator)
- **Role**: Primary command-line driver and connection manager.
- **Connection Strategy**:
  - **Astra DB**: Authenticates via Cloud Secure Connect Bundle (`.zip`) and Client ID/Secret PlainText credentials.
  - **On-Premise / DSE / OSS Cassandra**: Connects via host IP list with optional PlainText authentication.
- **Workflow Execution**: Automatically provisions the keyspace if missing (using `NetworkTopologyStrategy` with DC replication factor 3) and delegates execution to workload routines (e.g., `faketickets.fake_tickets_workflow`, `faketickets.fake_tickets_twcs_workflow`).
- **CLI Modes**: Accepts explicit start/count ranges (`-v start count`) or stateful incremental files (`-i incfile`).

### `faketickets.py` (Workload & Schema Management)
- **Schema**: Standard `fake_tickets` schema with `id` as partition key, owner, ticket ID, timestamp, updated timestamp, and notes.
- **Write Path**:
  - Uses `PreparedStatement` with `LOCAL_QUORUM` consistency.
  - Generates pseudo-random ticket IDs, alternating payload strings, and random assignee names from a predefined team list.
  - **Paced async ingestion** via `insert_paced_async()` (see [Section 6](#6-paced-asynchronous-ingestion)): rows are buffered into chunks and dispatched with `execute_concurrent_with_args`, replacing the former per-row blocking `session.execute` call.
- **Read Path & Benchmarking**:
  - Intermittently issues read and count queries at powers of 10 iteration intervals (1, 10, 100, 1,000, 10,000...).
  - Before each power-of-10 read, any buffered writes are flushed to ensure the target row is visible.
  - Calculates and logs execution durations.
- **Indexing Options**: Includes creation helpers for SAI (`StorageAttachedIndex`), Solr Search Indexes, and Materialized Views.
- **TWCS / TTL Workflow**: See [Section 5](#5-twcs--ttl-workflow) for the time-series ingestion path added in the TTL enhancement.

### `helper.py` (Utilities & Helpers)
- **Configuration loading**: Validates `.ini` file existence and parses key-value pairs.
- **Timestamp utilities**: Provides millisecond epoch timestamp generation and formatted timestamp converters.
- **State management**: Functions to persist and read sequential counters (`record.txt`).
- **Multiprocessing sizing**: Logic to calculate per-CPU batch partitioning based on core counts.

### `cassqueries.py` (Specialized Test Scenarios)
- Provides diagnostic queries (e.g., `system.local` release checks).
- Solr encrypted table generation tests.
- Single-partition clustering and GC grace test scenarios (`onepk_gc600`).
- Multi-key partition and `IN` query benchmarking (`dualid_workflow`).

### `deletebackups.py` (OpsCenter Utilities)
- Generates CQL deletion commands for cleaning up ad-hoc OpsCenter backup report metadata from `"OpsCenter".backup_reports`.

---

## 4. Execution Workflow

```
1. User executes CLI:
   python src/insert-astra.py -c conf/conf_dummy.ini -t fake_tickets -v 0 1000

2. helper.read_config reads database connection settings.

3. Connection established:
   - If Astra -> Cluster(cloud=cloud_config, auth_provider=...)
   - If DSE/Cassandra -> Cluster([host1], auth_provider=...)

4. Keyspace verified:
   CREATE KEYSPACE IF NOT EXISTS <ksname> WITH replication = {'class': 'NetworkTopologyStrategy', '<dc>': '3'}

5. Table created & statements prepared:
   faketickets.create_table_fake_tickets()
   faketickets.insert_bind_fake_tickets()

6. Ingestion loop:
   - Generates synthetic payload.
   - Executes bound insert at LOCAL_QUORUM.
   - Logs progress every 10k records.
   - Executes single-row read & count verification at power-of-10 milestones.
```

To run the **TWCS/TTL workflow** instead, uncomment the corresponding line in `insert-astra.py`:

```python
# Replace the default call:
ft.fake_tickets_workflow(session, ksname, startval, numrec, tblname)

# With the TWCS variant (in src/insert-astra.py):
ft.fake_tickets_twcs_workflow(session, ksname, startval, numrec, tblname)
```

---

## 5. TWCS / TTL Workflow

### Overview

The TWCS/TTL feature adds a dedicated time-series ingestion path designed for resource-constrained environments with continuous data ingestion. Data is written to a table configured with **Time Window Compaction Strategy (TWCS)** and a **default TTL of 2 days (172800 seconds)**, allowing Cassandra to automatically expire and reclaim disk space without explicit deletes.

### Table Configuration

| Parameter | Value |
|---|---|
| Compaction strategy | `TimeWindowCompactionStrategy` |
| Compaction window size | `1 HOURS` |
| Timestamp resolution | `MILLISECONDS` |
| Unchecked tombstone compaction | `true` |
| Default TTL | `172800` seconds (2 days) |
| `gc_grace_seconds` | `3600` (1 hour) |

The reduced `gc_grace_seconds` accelerates disk reclamation: an entire SSTable is eligible for drop once `max_timestamp + TTL + gc_grace_seconds < current_time`, avoiding expensive compaction passes.

### CQL Schema

```sql
CREATE TABLE IF NOT EXISTS {ksname}.{tblname} (
    id int,
    ownedby text,
    ticket int,
    time timestamp,
    updatedat timestamp,
    notes text,
    PRIMARY KEY (id)
)
WITH default_time_to_live = 172800
AND compaction = {
    'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy',
    'compaction_window_size': '1',
    'compaction_window_unit': 'HOURS',
    'timestamp_resolution': 'MILLISECONDS',
    'unchecked_tombstone_compaction': 'true'
}
AND gc_grace_seconds = 3600;
```

### New Functions in `faketickets.py`

#### `create_table_fake_tickets_twcs(session, ksname, tblname, ttl_seconds=172800, window_size=1, window_unit='HOURS')`
Issues a `CREATE TABLE IF NOT EXISTS` with the TWCS compaction options and default TTL. All parameters are configurable at call time to allow experimentation with different window sizes or TTL durations.

#### `insert_bind_fake_tickets_twcs(session, ksname, tblname, ttl_seconds=None)`
Prepares a `LOCAL_QUORUM` insert statement. When `ttl_seconds` is `None` (default), the statement defers to the table-level `default_time_to_live`. When an explicit integer is passed, the statement appends `USING TTL <value>` to override the default on a per-row basis.

#### `fake_tickets_twcs_workflow(session, ksname, startval, numrec, tblname, ttl_seconds=172800, rate_limit_sleep=0.01)`
Full ingestion loop that:
1. Provisions the TWCS table via `create_table_fake_tickets_twcs`.
2. Prepares the insert statement via `insert_bind_fake_tickets_twcs` (table-level TTL).
3. Iterates from `startval` to `startval + numrec`, generating synthetic payloads matching the standard workflow pattern.
4. Applies a configurable `rate_limit_sleep` (`time.sleep`) between writes to pace ingestion on low-resource nodes.
5. Logs progress every 1,000 records.

### Operational Recommendations

1. **Uniform TTL**: Do not override the TTL with variable per-row values. SSTable-level expiration (and the associated disk reclamation) only works when all rows in an SSTable expire within the same window.
2. **No partial overwrites**: Out-of-order writes or updates with different timestamps create overlapping TWCS windows across SSTables, preventing clean SSTable deletion.
3. **Pacing control**: Throughput is governed by the `concurrency` and `target_ops` parameters passed to `fake_tickets_twcs_workflow` (defaults: `concurrency=50`, `target_ops=200`). See [Section 6](#6-paced-asynchronous-ingestion) for details.

---

## 6. Paced Asynchronous Ingestion

### Overview

Both `fake_tickets_workflow` and `fake_tickets_twcs_workflow` now use **paced asynchronous ingestion** (Optimization Proposal 1) instead of a blocking per-row `session.execute` call. The shared helper is `insert_paced_async()`.

### How it Works

Row argument tuples are accumulated in a local buffer inside each workflow loop. Once the buffer reaches `chunk_size = concurrency * 2` entries, `insert_paced_async` is called to flush the chunk:

1. `execute_concurrent_with_args` dispatches up to `concurrency` writes in parallel.
2. All results are inspected; any failure is printed immediately.
3. A `time.sleep(chunk_size / target_ops)` pause follows each chunk to pace the overall write rate.

At power-of-10 read milestones (standard workflow) the buffer is flushed eagerly so the target row is guaranteed to be visible before the read is issued. Any rows remaining at the end of the loop are flushed in a final call.

### Parameters

| Parameter | Default | Effect |
|---|---|---|
| `concurrency` | `50` | Maximum simultaneous in-flight requests per chunk |
| `target_ops` | `200` | Target writes/sec (smooth `time.sleep` inter-chunk). Set `0` to disable throttling. |

### Calling Example

```python
# Standard workflow — 100 concurrent requests, uncapped rate
ft.fake_tickets_workflow(session, ksname, startval, numrec, tblname, concurrency=100, target_ops=0)

# TWCS workflow — default 50 concurrency, 200 ops/sec ceiling
ft.fake_tickets_twcs_workflow(session, ksname, startval, numrec, tblname)
```
