# 04 — Adding a table and a workload

A workload is **one Python class in one file** plus **one line in the registry**.
The backend lists it, the UI builds its form from the parameters you declare,
and the engine handles the rest: rate, concurrency, worker processes, stop
conditions, stats, progress, the compaction monitor and the run history.

This guide builds a complete example, `user_events`: one partition per user,
newest events first, with optional reads of a user's latest events. Every code
block below is the exact code that was tested.

---

## 1. What you write, and what you get for free

| You write | The engine does |
|---|---|
| The table's `CREATE TABLE` | Creates the keyspace (with the connection's RF), appends the compaction options the user picked, and handles "table already exists" (keep with a warning, or ALTER) |
| A list of `Param`s | Validates and range-checks them, shows them in the UI and the API, and lets `live` ones change while the run is going |
| `setup()`: prepare statements | Runs DDL on worker 0 only; the other workers wait until the table is queryable |
| `next_op()`: return the next request | Rate limiting, the in-flight cap, error counting (errors are counted, not retried), latency histograms, and the stop on count or duration |

## 2. The workload file

Create `faketickets/workloads/user_events.py`:

```python
# faketickets/workloads/user_events.py
"""Example workload: user activity events, one partition per user, newest first."""

from __future__ import annotations

import random

from .kit import *  # noqa: F403  (Workload, Param, Op, helpers: see kit.py)


class UserEventsWorkload(Workload):
    # --- catalogue: how it appears in the UI -----------------------------
    name = "user_events"                  # API id, unique, never change it once runs exist
    title = "User events (example)"
    description = "Appends events to one partition per user; optional reads of a user's latest events."
    category = "Data"                     # UI group; a new name simply creates a new group
    count_kinds = frozenset({"write"})    # "Number of records" counts writes, not reads
    approx_row_bytes = 120                # for the UI's data-volume estimate

    # --- parameters: the UI form is built from this list -----------------
    params = target_params("ft_user_events") + [
        Param("users", "int", 10_000, "Users", "number of partitions", 1, 10**9),
        Param("event_types", "str", "login,view,purchase", "Event types", "comma-separated",
              pattern=r"^[a-z_]{1,20}(,[a-z_]{1,20}){0,19}$"),
        Param("read_pct", "int", 10, "Read percentage", "reads of a user's latest events", 0, 100, live=True),
        Param("read_limit", "int", 20, "Events per read", "", 1, 1000, visible_if={"read_pct": [{"gt": 0}]}),
        Param("ttl_seconds", "int", 0, "TTL (s)", "table default_time_to_live; 0 = keep forever", 0, 630_720_000),
        consistency_param(),
    ] + compaction_params("stcs") + rate_params(1000) + monitor_params() + stop_params()

    @classmethod
    def check(cls, params):               # cross-field validation, runs before the run starts
        check_compaction(params)

    # --- runs once per worker process, before the first request ----------
    def setup(self, ctx: RunContext) -> None:
        ks, t = ctx.keyspace, ctx.table
        create_table(
            ctx,
            f"CREATE TABLE IF NOT EXISTS {ks}.{t} ("
            " user_id bigint, event_time timeuuid, event_type text, payload text,"
            " PRIMARY KEY ((user_id), event_time))"
            " WITH CLUSTERING ORDER BY (event_time DESC)",
            default_ttl=ctx.params["ttl_seconds"] or None,
        )
        self.insert = prepare(ctx, f"INSERT INTO {ks}.{t} (user_id, event_time, event_type, payload) VALUES (?, now(), ?, ?)")
        self.latest = prepare(ctx, f"SELECT event_time, event_type FROM {ks}.{t} WHERE user_id = ? LIMIT ?")
        self.types = ctx.params["event_types"].split(",")
        self.rng = random.Random()
        self.on_rows = count_hits(ctx)

    # --- called for every request; keep it fast and non-blocking ---------
    def next_op(self, ctx: RunContext) -> Op:
        p = ctx.params                    # read params here so live changes apply at once
        user = self.rng.randrange(p["users"])
        if self.rng.random() * 100 < p["read_pct"]:
            return Op(self.latest, (user, p["read_limit"]), "read", self.on_rows)
        return Op(self.insert, (user, self.rng.choice(self.types), padding(self.rng, 64)), "write")
```

## 3. Register it

In `faketickets/workloads/__init__.py`, import the class and add it to the
tuple. The order of the tuple is the order in the UI.

```python
# faketickets/workloads/__init__.py (excerpt)
from .user_events import UserEventsWorkload
#   ...
        AlterTableWorkload, TableStatsWorkload, TwcsWorkload, UserEventsWorkload,
```

That's all. Rebuild the image (or restart `faketickets backend` in a venv) and
the workload appears in the UI under its category, with its own form,
"Table & compaction" box, rate control and "How much" box.

Check it without the UI:

```bash
faketickets workloads | grep user_events
faketickets run user_events -c conn.json -p users=100 -p count=10000
```

## 4. The building blocks

Everything comes from `faketickets/workloads/kit.py` (`from .kit import *`).

### Parameters

`Param(name, type, default, label, help, min, max, choices=…, group=…, visible_if=…, live=…, pattern=…)`

| Field | Meaning |
|---|---|
| `type` | `int`, `float`, `bool`, `str` or `choice` (with `choices=[...]`) |
| `min` / `max` | Range check for numbers; the API returns 422 outside it |
| `group` | Where the UI shows it: `workload` (default), `compaction`, `rate`, `stop`, `advanced` (collapsed) |
| `visible_if` | Show only when another field matches: `{"mode": ["a", "b"]}` or `{"read_pct": [{"gt": 0}]}` |
| `live` | Can be changed while the run is going (UI shows it on the run page; `PATCH /runs/{id}`) |
| `pattern` | Full-match regex for `str` values. **Required for any string that ends up inside CQL text** |

Ready-made groups:

| Helper | Adds |
|---|---|
| `target_params("default_table")` | `keyspace`, `table`, `create_keyspace` (names are checked against `^[A-Za-z][A-Za-z0-9_]{0,47}$`) |
| `compaction_params("stcs")` | Strategy and its sub-options, `gc_grace_seconds`, tombstone options, keep/alter. Optional: `gc_grace_default=`, `twcs_unit=`, `twcs_size=` |
| `rate_params(1000)` | `rate` (live), `concurrency` (live), `processes` |
| `stop_params()` | `count` and `duration_s`; labels can be changed, e.g. `stop_params("Number of reads")` |
| `monitor_params()` | `monitor_every_s` for the compaction monitor |
| `consistency_param()` | `consistency`, applied by `prepare()` |

### The table

`create_table(ctx, "CREATE TABLE IF NOT EXISTS ks.t (...) [WITH CLUSTERING ORDER BY (...)]", default_ttl=None)`

- Don't add `compaction = …` or `gc_grace_seconds` yourself: they come from the
  user's choices in the "Table & compaction" box.
- Keyspace and table names come from `ctx.keyspace` / `ctx.table` and were
  validated. Any other value you put into DDL must be a number, a `choice`, or a
  `str` with a strict `pattern`.
- If the table already exists, its shape is not checked. If your workload needs
  particular columns, check them in `setup()` and raise `RuntimeError` with a
  clear message (see `OverwriteWorkload.setup` for an example).

### Requests

`next_op()` returns `Op(statement, args, kind, on_rows=None)`:

- `kind` is a free label (`"write"`, `"read"`, `"delete"`, …). The run page shows
  a rate per kind, and `count_kinds` decides which kinds count towards
  "Number of records" (`None` = every op).
- `on_rows`, for reads: `count_hits(ctx)` counts hits and misses in the run stats.
- Return `None` to end the run (reason `completed`), e.g. after a fixed data set.
- Never wait or sleep in `next_op()`: the engine calls it at the target rate.

### Other things the context gives you

| `ctx.` | Use |
|---|---|
| `params` | Validated parameters (live ones update in place) |
| `proc_index`, `proc_count` | This worker's number and the number of workers, to split keys between them |
| `progress["key"] = value` | Shown as a tile on the run page (e.g. `pass`, `deletes`) |
| `emit(level, message)` | Adds a line to the run's event log (`info`, `warning`, `event`) |
| `recorder.bump(name, n)` | Extra counters in the run stats |

### Less common

- **One-shot** workloads (a report, an ALTER): set `kind = "oneshot"` and
  implement `run_once(ctx) -> dict` instead of `next_op()`. The dict is shown as
  the run's result. See `TableStatsWorkload` in `compaction.py`.
- **Resumable ids** (start where the last run stopped): set `uses_ids = True`
  and use `setup_ids()` like `tickets.py` and `TombstoneWorkload`.
- **Scheduled extra requests** (every N minutes): `tick(ctx, now)` is called
  about 5 times a second and may return a list of `Op`s. See `twcs.py`.

## 5. Tests

### Without a database

Create `tests/test_user_events.py`. It replaces `create_table` and the session
with stand-ins, so it runs in a second with no Cassandra:

```python
# tests/test_user_events.py
from collections import Counter
from types import SimpleNamespace

import pytest

from faketickets import workloads
from faketickets.engine.stats import Recorder
from faketickets.workloads import user_events
from faketickets.workloads.base import RunContext


class StubSession:
    def prepare(self, cql):
        return SimpleNamespace(cql=cql, consistency_level=None)


def make(monkeypatch, **params):
    monkeypatch.setattr(user_events, "create_table", lambda *a, **k: None)
    cls = workloads.get("user_events")
    ctx = RunContext(session=StubSession(), spec=SimpleNamespace(keyspace="ks"), params=cls.validate(params),
                     recorder=Recorder(), emit=lambda *a: None)
    wl = cls()
    wl.setup(ctx)
    return wl, ctx


def test_registered_with_defaults():
    d = workloads.get("user_events").describe()
    assert d["count_kinds"] == ["write"] and d["category"] == "Data"


def test_rejects_cql_in_event_types():
    with pytest.raises(ValueError):
        workloads.get("user_events").validate({"event_types": "login'); DROP TABLE x; --"})


def test_read_share_and_live_change(monkeypatch):
    wl, ctx = make(monkeypatch, users=50, read_pct=0)
    ops = [wl.next_op(ctx) for _ in range(1000)]
    assert {o.kind for o in ops} == {"write"}
    assert all(0 <= o.args[0] < 50 for o in ops)
    ctx.params["read_pct"] = 30           # what a live PATCH does
    kinds = Counter(wl.next_op(ctx).kind for _ in range(10_000))
    assert 2500 < kinds["read"] < 3500
```

Run it with `pytest tests/test_user_events.py`.

### Against Cassandra

Add to `tests/test_integration.py`. It uses that file's `run()` helper, which
starts real worker processes:

```python
# tests/test_integration.py (append)
def test_user_events(session):
    t = "ue_" + uuid.uuid4().hex[:8]
    msgs = run("user_events", {"table": t, "users": 20, "read_pct": 30, "rate": 500, "count": 400, "compaction": "lcs"})
    assert not errors(msgs), errors(msgs)
    assert session.execute(f"SELECT count(*) FROM {KS}.{t}").one()[0] == 400   # count = writes only
    assert total_ok(msgs, "read") > 0
    session.cluster.refresh_table_metadata(KS, t)
    assert "Leveled" in session.cluster.metadata.keyspaces[KS].tables[t].options["compaction"]["class"]
```

Run it against a test node (cap its memory on a small machine):

```bash
podman run -d --name ft-cass -p 19042:9042 --memory 1600m -e MAX_HEAP_SIZE=640M -e HEAP_NEWSIZE=128M cassandra:5.0
FT_IT_CONTACT_POINTS=127.0.0.1:19042 pytest -m integration -k user_events
```

> Defining workloads in the UI, without Python, is planned as a separate project on
> NoSQLBench: [ideas/workload-studio-nosqlbench.md](ideas/workload-studio-nosqlbench.md).

## 6. Checklist

- [ ] `name` is unique and stable (it's stored with every run).
- [ ] Every string that reaches CQL text has a `pattern`, or is a `choice`.
- [ ] `count_kinds` is set if the workload also reads or deletes.
- [ ] `approx_row_bytes` is roughly right, so the UI estimate is useful.
- [ ] `next_op()` doesn't block, and reads `ctx.params` for live parameters.
- [ ] With `processes > 1`, workers don't collide when they shouldn't (split keys
      with `proc_index` / `proc_count`).
- [ ] A unit test and an integration test, as above.
