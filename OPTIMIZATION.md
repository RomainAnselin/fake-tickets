# Code Review, Workflow Gaps, and Optimization Proposals

## 1. Executive Summary
This document provides a comprehensive code review of the `fake-tickets` repository, identifies architectural and implementation gaps, and details concrete optimization proposals to improve performance, resource predictability, code quality, and maintainability.

---

## 2. Gaps and Defects Identified

### 2.1. Critical Bugs & Runtime Errors
1. **Timestamp Formatting Bug in `helper.fromts()` ([`helper.py:37-44`](helper.py:37))**:
   - `time.strftime('%Y-%m-%d %H:%M:%S:{}'.format(myts%1000), time.gmtime(myts/1000.0))` does not zero-pad milliseconds. A remainder of `79` ms formats as `:79` instead of `:079`.
   - *Fix*: Use `datetime.fromtimestamp(myts / 1000.0, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]` or format integer millisecond with `{:03d}`.

2. **Unbound Variable / Logic Crash in Read Loop ([`faketickets.py:160-161`](faketickets.py:160))**:
   - Inside `fake_tickets_workflow()`, when `i in power10`, `cntrows = session.execute(tickselcnt_bind)` is executed, but the subsequent print iterates over `selrows` using `row.count`:
     ```python
     for row in selrows:
         print("Number of rows: " + str(row.count))
     ```
   - `selrows` contains columns (`id`, `ownedby`, `ticket`, `time`, `notes`), NOT `count`. This raises an `AttributeError` at runtime on `power10` iterations (e.g., at `i=1`).
   - *Fix*: Iterate over `cntrows` or call `cntrows.one()[0]`.

3. **Missing Imports and Broken References**:
   - [`insert-astra.py:13`](insert-astra.py:13): Imports `astrasets` which is not present in the repository, potentially breaking executions where imports are strictly resolved if un-commented.
   - [`faketickets.py:12`](faketickets.py:12): Unused import `timedelta`.
   - [`cassqueries.py:122`](cassqueries.py:122): Typo `cnt =+ 1` (assigns positive 1) instead of `cnt += 1` (increment).

4. **Resource Leaks & Unreachable Code in File Operations ([`insert-astra.py:46`](insert-astra.py:46))**:
   - In `readrecords(incf)`: `file.close()` appears *after* `return startval, numrec`, making it unreachable. Furthermore, the `with open()` context manager already automatically closes the file, making explicit `.close()` redundant.
   - In `writerecords(incf)`: Uses `open()` without `with` context or try/finally handling.

---

### 2.2. Performance & Concurrency Gaps
1. **Synchronous Single-Threaded Write Loop (Blocking I/O)** ✅ *Fixed — Proposal 1*:
   - In [`faketickets.py:132`](faketickets.py:132), `session.execute(tickins_bind)` blocked on every single insert.
   - For a network RTT of 2–10ms, max throughput was throttled to 100–500 writes/second sequentially.
   - *Implemented*: `insert_paced_async()` uses `execute_concurrent_with_args` to dispatch chunks of `concurrency * 2` writes concurrently; both `fake_tickets_workflow` and `fake_tickets_twcs_workflow` now buffer row tuples and flush via this helper.

2. **No Backpressure or Rate Limiting** ✅ *Fixed — Proposal 1*:
   - The loop ran unthrottled in pure synchronous mode, creating uneven CPU spikes.
   - *Implemented*: `insert_paced_async()` accepts a `target_ops` parameter (default 200 ops/sec). A `time.sleep` inter-chunk delay is calculated as `chunk_size / target_ops` to produce a smooth, steady-state ingestion rate. Pass `target_ops=0` to disable throttling entirely.

3. **Excessive stdout Logging** ✅ *Fixed — Proposal 1*:
   - `print(str(tickins_bind))` on every single insert introduced substantial I/O overhead.
   - *Implemented*: The per-record print is removed; progress is logged every 10,000 records only.

---

### 2.3. Schema & Cassandra Anti-Patterns (Wanted Support/Testing Behaviour)
> **Note**: As part of support engineering, breaking flows and specific anti-patterns are intentionally designed into this test harness to reproduce customer issues and evaluate cluster resilience.
1. **Bare `int` as Unique Monotonic ID (Wanted Behaviour)**:
   - Sequentially increasing integer keys (`0, 1, 2, ...`) are intentionally retained to easily track record counts, monitor progress, and test specific partition ranges.
2. **Materialized Views (`create_mv_fake_tickets`) (Wanted Behaviour)**:
   - MVs are purposefully included to test write amplification, MV synchronization latency, and view stability under workload in DSE/Cassandra.
3. **Missing Default TTL / Tombstone Control**:
   - Standard tables omit TTL to test unbounded table growth and compaction behavior, while time-series testing is handled via dedicated TWCS tables (see [`TTL-ENHANCEMENT.md`](TTL-ENHANCEMENT.md:1)).

---

## 3. Concrete Optimization Proposals

### Proposal 1: Implement Paced Asynchronous Ingestion (Controlled Concurrency) ✅ *Implemented*

> **Status**: Implemented in [`src/faketickets.py`](src/faketickets.py).

The blocking synchronous insert loop has been replaced with `insert_paced_async()`, a shared helper that dispatches chunks of inserts concurrently via `execute_concurrent_with_args` and throttles throughput to a configurable target OPS:

```python
from cassandra.concurrent import execute_concurrent_with_args

def insert_paced_async(session, prepared_stmt, arg_tuples, concurrency=50, target_ops=200):
    """
    Flush a list of bound-argument tuples using execute_concurrent_with_args
    with controlled concurrency and smooth rate pacing.
    """
    chunk_size = concurrency * 2
    sleep_delay = chunk_size / target_ops if target_ops > 0 else 0
    arg_list = list(arg_tuples)

    for offset in range(0, len(arg_list), chunk_size):
        chunk = arg_list[offset:offset + chunk_size]
        results = execute_concurrent_with_args(session, prepared_stmt, chunk, concurrency=concurrency)
        for success, result in results:
            if not success:
                print(f"Write failure: {result}")
        if sleep_delay > 0:
            time.sleep(sleep_delay)
```

Both `fake_tickets_workflow` and `fake_tickets_twcs_workflow` now accept `concurrency` and `target_ops` keyword arguments and accumulate row argument tuples into a local buffer, flushing it through `insert_paced_async` once it reaches `chunk_size`. Any remaining rows are flushed after the loop. The former `rate_limit_sleep` parameter in the TWCS workflow has been superseded by `target_ops`.

**Tuning knobs:**

| Parameter | Default | Effect |
|---|---|---|
| `concurrency` | `50` | Max simultaneous in-flight requests per chunk |
| `target_ops` | `200` | Target writes/sec (smooth `time.sleep` inter-chunk). Set `0` to disable. |

---

### Proposal 2: Robust State Tracking & Persistence
Refactor `readrecords` and `writerecords` to use atomic JSON/YAML state files to prevent corrupted state upon abrupt termination (e.g., SIGINT / Ctrl+C).

```python
import json
import os

def load_state(state_file):
    if os.path.exists(state_file):
        with open(state_file, 'r') as f:
            return json.load(f)
    return {"last_id": 0, "batch_size": 1000}

def save_state(state_file, state):
    temp_file = state_file + ".tmp"
    with open(temp_file, 'w') as f:
        json.dump(state, f)
    os.replace(temp_file, state_file)  # Atomic write on POSIX
```

---

### Proposal 3: Correct Millisecond Timestamp Formatting in `helper.py`
Replace the problematic formatting function with a standards-compliant UTC ISO-8601 millisecond formatter:

```python
from datetime import datetime, timezone

def fromts(myts):
    """Accurately converts millisecond epoch timestamp to ISO format with 3-digit zero-padded ms."""
    dt = datetime.fromtimestamp(myts / 1000.0, tz=timezone.utc)
    return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
```

---

### Proposal 4: Modular CLI with Subcommands
Refactor [`insert-astra.py`](insert-astra.py:1) to support clear subcommands:
- `ingest`: Standard linear ingestion.
- `twcs`: Time-window time-to-live ingestion.
- `bench`: Read/write power-of-10 benchmark.
- `clean`: Housekeeping and truncation.

This eliminates commented-out code blocks and improves script maintainability.
