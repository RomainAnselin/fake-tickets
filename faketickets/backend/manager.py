"""Run manager: spawns one OS process per run shard, aggregates telemetry."""

from __future__ import annotations

import logging
import multiprocessing as mp
import secrets
import threading
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from typing import Any, Optional

from .. import workloads
from ..config import Settings
from ..engine.histogram import Histogram
from ..worker import worker_main
from .store import RunStore

log = logging.getLogger(__name__)

SERIES_LEN = 3600  # 1 h of 1 s points
STOP_GRACE_S = 20.0
CHECKPOINT_EVERY_S = 5.0
FINAL_STATES = ("finished", "stopped", "failed", "interrupted")
KEEP_FINISHED_IN_MEMORY = 50


class RunError(Exception):
    def __init__(self, status: int, msg: str) -> None:
        super().__init__(msg)
        self.status = status


@dataclass
class Proc:
    index: int
    process: Any
    conn: Any
    done: bool = False
    reason: Optional[str] = None
    last_t: Optional[float] = None
    rate_ok: float = 0.0
    rate_err: float = 0.0
    rate_by_kind: dict[str, float] = field(default_factory=dict)
    inflight: int = 0
    progress: dict[str, Any] = field(default_factory=dict)


@dataclass
class Run:
    id: str
    workload: str
    params: dict[str, Any]
    target: Optional[str]
    connection_ref: Optional[str]
    connection_name: Optional[str]
    owner: Optional[str]
    label: Optional[str]
    rate: float
    concurrency: int
    created_at: float
    state: str = "starting"
    reason: Optional[str] = None
    error: Optional[str] = None
    result: Any = None
    started_at: Optional[float] = None
    ended_at: Optional[float] = None
    stop_requested: bool = False
    procs: list[Proc] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)
    total_ok: int = 0
    total_err: int = 0
    by_kind_ok: Counter = field(default_factory=Counter)
    by_kind_err: Counter = field(default_factory=Counter)
    extra: Counter = field(default_factory=Counter)
    errtypes: Counter = field(default_factory=Counter)
    last_error: Optional[str] = None
    hist_total: Histogram = field(default_factory=Histogram)
    hist_interval: Histogram = field(default_factory=Histogram)
    last_latency: dict[str, float] = field(default_factory=dict)
    series: deque = field(default_factory=lambda: deque(maxlen=SERIES_LEN))
    events: deque = field(default_factory=lambda: deque(maxlen=200))
    last_checkpoint: float = 0.0

    @property
    def active(self) -> bool:
        return self.state not in FINAL_STATES

    @property
    def next_id(self) -> Optional[int]:
        vals = [p.progress.get("next_id") for p in self.procs if p.progress.get("next_id") is not None]
        return max(vals) if vals else None

    @property
    def counted(self) -> int:
        """Ops that count towards ``count`` (records written, for most workloads), all shards."""
        return sum(int(p.progress.get("counted", 0)) for p in self.procs)


PROGRESS_SUM = frozenset({"deletes", "late", "overlaps", "backdated"})  # per-shard counters
PROGRESS_MAX = frozenset({"pass"})


def merge_progress(procs: list[Proc]) -> dict[str, Any]:
    """Merge shard progress: known counters are summed, ``pass`` takes the max, anything else
    (``cmp_*`` monitor values, live knobs) is taken from the lowest shard that reports it."""
    out: dict[str, Any] = {}
    for p in procs:  # ordered by shard index
        for k, v in p.progress.items():
            if k in ("next_id", "counted"):
                continue
            if k not in out:
                out[k] = v
            elif k in PROGRESS_SUM:
                out[k] += v
            elif k in PROGRESS_MAX:
                out[k] = max(out[k], v)
    return out


class RunManager:
    def __init__(self, settings: Settings, store: RunStore) -> None:
        self.settings = settings
        self.store = store
        self.runs: dict[str, Run] = {}
        self._lock = threading.Lock()
        self._mp = mp.get_context("spawn")
        self._stop = threading.Event()
        self._shutting_down = False
        n = store.mark_interrupted()
        if n:
            log.warning("marked %d run(s) from a previous backend process as interrupted", n)
        self._ticker = threading.Thread(target=self._tick_loop, name="run-ticker", daemon=True)
        self._ticker.start()

    # ------------------------------------------------------------------ start
    def start(self, req: dict[str, Any]) -> Run:
        cls = workloads.get(req["workload"])
        try:
            params = cls.validate(req.get("params") or {})
        except ValueError as exc:
            raise RunError(422, str(exc)) from None
        procs = int(params.get("processes", 1))
        if procs > self.settings.max_processes_per_run:
            raise RunError(422, f"processes > FT_MAX_PROCESSES_PER_RUN ({self.settings.max_processes_per_run})")
        rate = float(params.get("rate", 0))
        if "rate" in params and self.settings.max_rate and (rate == 0 or rate > self.settings.max_rate):
            raise RunError(422, f"rate must be between 1 and FT_MAX_RATE ({self.settings.max_rate})")
        with self._lock:
            active = sum(1 for r in self.runs.values() if r.active)
            if active >= self.settings.max_runs:
                raise RunError(429, f"too many active runs ({active}/{self.settings.max_runs})")

        spec = req.get("connection") or {}
        conn_ref = req.get("connection_ref")
        target = None
        if "table" in params:
            target = f"{params.get('keyspace') or spec.get('keyspace') or 'fake_tickets'}.{params['table']}"
        # Resume cursor: start_id / id_max = -1 mean "continue from what previous runs wrote".
        cursor = self.store.cursor(conn_ref, target) if (conn_ref and target) else None
        if params.get("start_id", 0) == -1:
            params["start_id"] = cursor or 0
        if params.get("id_max", 0) == -1 and cursor:
            params["id_max"] = cursor - 1

        run = Run(
            id=secrets.token_hex(6),
            workload=cls.name,
            params=params,
            target=target,
            connection_ref=conn_ref,
            connection_name=req.get("connection_name"),
            owner=req.get("owner"),
            label=(req.get("label") or "")[:200] or None,
            rate=rate,
            concurrency=int(params.get("concurrency", 64)),
            created_at=time.time(),
        )
        self.store.upsert(
            run.id, workload=run.workload, params=params, connection_ref=conn_ref, connection_name=run.connection_name,
            target=target, owner=run.owner, label=run.label, state=run.state, created_at=run.created_at,
            next_id=params.get("start_id") if cls.uses_ids else None,
        )
        ref = f"{cls.__module__}:{cls.__qualname__}"
        n = procs if cls.kind == "continuous" else 1
        with self._lock:
            self.runs[run.id] = run
        for i in range(n):
            parent, child = self._mp.Pipe()
            job = {
                "run_id": run.id,
                "proc_index": i,
                "proc_count": n,
                "workload_ref": ref,
                "params": params,
                "spec": spec,
                "rate": rate,
                "concurrency": run.concurrency,
                "log_level": self.settings.log_level,
            }
            p = self._mp.Process(target=worker_main, args=(child, job), name=f"ft-run-{run.id}-{i}", daemon=True)
            p.start()
            child.close()
            proc = Proc(index=i, process=p, conn=parent)
            run.procs.append(proc)
            threading.Thread(target=self._reader, args=(run, proc), name=f"reader-{run.id}-{i}", daemon=True).start()
        where = f" on {target}" if target else ""
        via = f" via {req['connection_summary']}" if req.get("connection_summary") else ""
        self._event(run, "info", f"started {cls.name} with {n} process(es){where}{via}")
        return run

    # ---------------------------------------------------------- telemetry in
    def _reader(self, run: Run, proc: Proc) -> None:
        while True:
            try:
                msg = proc.conn.recv()
            except (EOFError, OSError):
                break
            try:
                self._on_message(run, proc, msg)
            except Exception:  # never let a bad message kill the reader
                log.exception("bad message from run %s", run.id)
            if msg.get("type") == "done":
                break
        proc.process.join(timeout=10)
        if not proc.done:
            code = proc.process.exitcode
            self._on_message(run, proc, {"type": "error", "error": f"worker process died (exit code {code})"})
            self._on_message(run, proc, {"type": "done", "reason": "crashed"})
        try:
            proc.conn.close()
        except Exception:
            pass

    def _on_message(self, run: Run, proc: Proc, msg: dict[str, Any]) -> None:
        kind = msg.get("type")
        if kind == "stats":
            t = msg["t"]
            ok, err = msg.get("ok", {}), msg.get("err", {})
            n_ok, n_err = sum(ok.values()), sum(err.values())
            with run.lock:
                if proc.last_t is not None and t > proc.last_t:
                    dt = t - proc.last_t
                    proc.rate_ok = n_ok / dt
                    proc.rate_err = n_err / dt
                    proc.rate_by_kind = {k: v / dt for k, v in ok.items()}
                proc.last_t = t
                proc.inflight = msg.get("inflight", 0)
                if msg.get("progress"):
                    proc.progress.update(msg["progress"])
                run.total_ok += n_ok
                run.total_err += n_err
                run.by_kind_ok.update(ok)
                run.by_kind_err.update(err)
                run.extra.update(msg.get("extra", {}))
                run.errtypes.update(msg.get("errtypes", {}))
                if msg.get("last_error"):
                    run.last_error = msg["last_error"]
                hist = msg.get("hist") or {}
                run.hist_total.merge_counts(hist, msg.get("hist_max", 0.0))
                run.hist_interval.merge_counts(hist, msg.get("hist_max", 0.0))
        elif kind == "state":
            with run.lock:
                if msg.get("progress"):
                    proc.progress.update(msg["progress"])
                if run.state == "starting":
                    run.state = "running"
                    run.started_at = time.time()
            self.store.upsert(run.id, state=run.state, started_at=run.started_at)
        elif kind == "event":
            self._event(run, msg.get("level", "info"), msg.get("msg", ""), msg.get("t"))
        elif kind == "result":
            run.result = msg.get("data")
            self.store.upsert(run.id, result=run.result)
        elif kind == "error":
            run.error = msg.get("error")
            self._event(run, "error", run.error or "error")
        elif kind == "done":
            proc.done = True
            proc.reason = msg.get("reason")
            proc.rate_ok = proc.rate_err = 0.0
            proc.rate_by_kind = {}
            if all(p.done for p in run.procs):
                self._finalize(run)
            elif proc.reason in ("failed", "crashed") and not run.stop_requested:
                # One shard failed: bring the others down too.
                self.stop(run.id, reason=f"shard {proc.index} {proc.reason}")

    def _finalize(self, run: Run) -> None:
        reasons = [p.reason for p in run.procs]
        if any(r in ("failed", "crashed") for r in reasons):
            state = "failed"
        elif self._shutting_down:
            state = "interrupted"
        elif run.stop_requested:
            state = "stopped"
        else:
            state = "finished"
        reason = next((r for r in reasons if r not in ("stopped",)), reasons[0] if reasons else None)
        with run.lock:
            run.state = state
            run.reason = run.reason or reason
            run.ended_at = time.time()
        self._checkpoint(run)
        self._event(run, "info", f"run {state} ({run.reason}); ok={run.total_ok} err={run.total_err}")
        self._evict()

    def _evict(self) -> None:
        """Keep only the most recent finished runs in memory (their 1 h series); older ones
        are still served from SQLite (summary + events) — the service is meant to stay up."""
        with self._lock:
            done = sorted((r for r in self.runs.values() if not r.active), key=lambda r: r.ended_at or 0, reverse=True)
            for r in done[KEEP_FINISHED_IN_MEMORY:]:
                self.runs.pop(r.id, None)

    def _event(self, run: Run, level: str, msg: str, t: Optional[float] = None) -> None:
        t = t or time.time()
        run.events.append({"t": t, "level": level, "msg": msg})
        self.store.add_event(run.id, t, level, msg)
        log.info("run %s [%s] %s", run.id, level, msg)

    def _checkpoint(self, run: Run) -> None:
        run.last_checkpoint = time.time()
        self.store.upsert(
            run.id, state=run.state, reason=run.reason, error=run.error, ended_at=run.ended_at,
            next_id=run.next_id, total_ok=run.total_ok, total_err=run.total_err,
            params=run.params,
        )

    # ------------------------------------------------------------- 1 s ticker
    def _tick_loop(self) -> None:
        while not self._stop.wait(1.0):
            now = time.time()
            with self._lock:
                runs = [r for r in self.runs.values() if r.active]
            for run in runs:
                with run.lock:
                    if run.hist_interval.total:
                        run.last_latency = run.hist_interval.summary_ms()
                        run.hist_interval.reset()
                    point = {
                        "t": round(now, 3),
                        "rate": round(sum(p.rate_ok for p in run.procs), 1),
                        "err_rate": round(sum(p.rate_err for p in run.procs), 1),
                        "inflight": sum(p.inflight for p in run.procs),
                        "target": run.rate,
                        **{f"rate_{k}": round(sum(p.rate_by_kind.get(k, 0.0) for p in run.procs), 1)
                           for k in {k for p in run.procs for k in p.rate_by_kind}},
                        **run.last_latency,
                    }
                    run.series.append(point)
                if now - run.last_checkpoint >= CHECKPOINT_EVERY_S:
                    self._checkpoint(run)

    # ------------------------------------------------------------- controls
    def get(self, run_id: str) -> Run:
        run = self.runs.get(run_id)
        if run is None:
            raise RunError(404, "run not found (or not in this backend's memory)")
        return run

    def update(self, run_id: str, *, rate: Optional[float] = None, concurrency: Optional[int] = None,
               params: Optional[dict[str, Any]] = None) -> Run:
        run = self.get(run_id)
        if not run.active:
            raise RunError(409, f"run is {run.state}")
        cls = workloads.get(run.workload)
        live = {p.name: p for p in cls.params if p.live}
        msg: dict[str, Any] = {"type": "set"}
        changes = []
        if rate is not None:
            rate = live["rate"].coerce(rate) if "rate" in live else rate
            if self.settings.max_rate and (rate == 0 or rate > self.settings.max_rate):
                raise RunError(422, f"rate must be between 1 and FT_MAX_RATE ({self.settings.max_rate})")
            run.rate = run.params["rate"] = msg["rate"] = rate
            changes.append(f"rate={rate:g}")
        if concurrency is not None:
            concurrency = live["concurrency"].coerce(concurrency) if "concurrency" in live else concurrency
            run.concurrency = run.params["concurrency"] = msg["concurrency"] = concurrency
            changes.append(f"concurrency={concurrency}")
        if params:
            bad = [k for k in params if k not in live or k in ("rate", "concurrency")]
            if bad:
                raise RunError(422, f"not live-adjustable: {', '.join(bad)}")
            coerced = {k: live[k].coerce(v) for k, v in params.items()}
            run.params.update(coerced)
            msg["params"] = coerced
            changes += [f"{k}={v}" for k, v in coerced.items()]
        for p in run.procs:
            if not p.done:
                try:
                    p.conn.send(msg)
                except (OSError, BrokenPipeError):
                    pass
        if changes:
            self._event(run, "info", "updated " + ", ".join(changes))
        return run

    def stop(self, run_id: str, reason: str = "stopped by user", *, reap: bool = True) -> Run:
        run = self.get(run_id)
        if not run.active:
            return run
        run.stop_requested = True
        if run.state != "stopping":
            run.state = "stopping"
            run.reason = reason
            self._event(run, "info", f"stopping: {reason}")
        for p in run.procs:
            if not p.done:
                try:
                    p.conn.send({"type": "stop"})
                except (OSError, BrokenPipeError):
                    pass
        if reap:  # shutdown() joins the processes itself; never join one process from two threads
            threading.Thread(target=self._reap, args=(run,), daemon=True).start()
        return run

    def _reap(self, run: Run) -> None:
        """Escalate only for shards that never reported 'done' (their reader thread owns the join)."""
        deadline = time.time() + STOP_GRACE_S
        while time.time() < deadline and not all(p.done for p in run.procs):
            time.sleep(0.2)
        for p in run.procs:
            if not p.done and p.process.is_alive():
                log.warning("run %s shard %d ignored stop; terminating", run.id, p.index)
                p.process.terminate()
                p.process.join(5)
                if p.process.is_alive():
                    p.process.kill()

    def delete(self, run_id: str) -> None:
        run = self.runs.get(run_id)
        if run and run.active:
            raise RunError(409, "stop the run before deleting it")
        with self._lock:
            self.runs.pop(run_id, None)
        self.store.delete(run_id)

    def shutdown(self, timeout: float = 25.0) -> None:
        """SIGTERM path: stop every run, wait, then terminate stragglers."""
        self._shutting_down = True
        active = [r for r in self.runs.values() if r.active]
        for r in active:
            self.stop(r.id, reason="backend_shutdown", reap=False)
        deadline = time.time() + timeout
        for r in active:
            for p in r.procs:
                p.process.join(timeout=max(0.1, deadline - time.time()))
                if p.process.is_alive():
                    p.process.terminate()
        for r in active:
            for p in r.procs:
                p.process.join(2)
            if r.active:
                r.state, r.reason, r.ended_at = "interrupted", "backend_shutdown", time.time()
            self._checkpoint(r)
        self._stop.set()

    # ------------------------------------------------------------- views
    def view(self, run: Run, *, series_since: Optional[float] = None, detail: bool = True) -> dict[str, Any]:
        with run.lock:
            d: dict[str, Any] = {
                "id": run.id,
                "workload": run.workload,
                "state": run.state,
                "reason": run.reason,
                "error": run.error,
                "owner": run.owner,
                "label": run.label,
                "target": run.target,
                "connection_ref": run.connection_ref,
                "connection_name": run.connection_name,
                "created_at": run.created_at,
                "started_at": run.started_at,
                "ended_at": run.ended_at,
                "rate_target": run.rate,
                "concurrency": run.concurrency,
                "processes": len(run.procs),
                "rate": run.series[-1]["rate"] if run.series and run.active else 0.0,
                "total_ok": run.total_ok,
                "total_err": run.total_err,
                "next_id": run.next_id,
                "counted": run.counted,
                "count_target": int(run.params.get("count") or 0),
                "duration_target": float(run.params.get("duration_s") or 0),
                "latency_ms": run.hist_total.summary_ms(),
                "last_error": run.last_error,
            }
            if detail:
                series = list(run.series)
                if series_since is not None:
                    series = [p for p in series if p["t"] > series_since]
                d.update(
                    params=run.params,
                    result=run.result,
                    ops_ok=dict(run.by_kind_ok),
                    ops_err=dict(run.by_kind_err),
                    counters=dict(run.extra),
                    error_types=dict(run.errtypes),
                    progress=merge_progress(run.procs),
                    series=series,
                    events=list(run.events)[-100:],
                )
        return d

    def list_views(self, owner: Optional[str] = None) -> list[dict[str, Any]]:
        live = {r.id: self.view(r, detail=False) for r in list(self.runs.values()) if not owner or r.owner == owner}
        out = list(live.values())
        for rec in self.store.list(owner=owner):
            if rec["id"] not in live:
                rec["rate"] = 0.0
                rec["processes"] = None
                out.append(rec)
        out.sort(key=lambda r: r["created_at"], reverse=True)
        return out
