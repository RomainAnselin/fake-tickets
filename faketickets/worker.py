"""Worker process: runs ONE workload (or one shard of it) and talks to the
run manager over a ``multiprocessing`` pipe.

Parent → worker messages: ``{"type": "set", "rate": .., "concurrency": .., "params": {..}}``,
``{"type": "stop"}``.
Worker → parent messages: ``state``, ``stats`` (every second, interval deltas),
``event``, ``result`` (one-shot workloads), ``error``, ``done``.

Secrets arrive inside the job dict through the pipe; nothing sensitive is put
in argv or the environment, and the job is never logged.
"""

from __future__ import annotations

import importlib
import logging
import shutil
import signal
import tempfile
import threading
import time
import traceback
from typing import Any

from .engine.dispatcher import Dispatcher
from .engine.ids import IdSpaceExhausted
from .engine.ratelimit import RateLimiter
from .engine.stats import Recorder
from .workloads.base import RunContext, Workload

log = logging.getLogger("faketickets.worker")

STATS_INTERVAL_S = 1.0
TICK_INTERVAL_S = 0.2
MAX_BATCH = 512


def import_ref(ref: str) -> type[Workload]:
    mod, _, name = ref.partition(":")
    obj: Any = importlib.import_module(mod)
    for part in name.split("."):
        obj = getattr(obj, part)
    return obj


def share(total: int, index: int, count: int) -> int:
    """Split ``total`` over ``count`` processes (0 stays 0 = unlimited)."""
    if total <= 0:
        return 0
    return total // count + (1 if index < total % count else 0)


class _Sender:
    def __init__(self, conn: Any) -> None:
        self._conn = conn
        self._lock = threading.Lock()
        self.broken = False

    def __call__(self, msg: dict) -> None:
        if self.broken:
            return
        with self._lock:
            try:
                self._conn.send(msg)
            except (BrokenPipeError, EOFError, OSError):
                self.broken = True


def run_loop(wl: Workload, ctx: RunContext, disp: Dispatcher, limiter: RateLimiter, count: int, duration_s: float) -> str:
    """Dispatch ops until a stop condition; ``count`` applies to ops whose kind is in ``count_kinds``."""
    t0 = time.monotonic()
    counted = type(wl).count_kinds
    issued = 0
    next_tick = 0.0
    try:
        while not ctx.stop_event.is_set():
            mono = time.monotonic()
            if duration_s and mono - t0 >= duration_s:
                return "duration_reached"
            if mono >= next_tick:
                for op in wl.tick(ctx, ctx.clock()):
                    disp.submit(op)
                next_tick = mono + TICK_INTERVAL_S
                ctx.progress["counted"] = issued
            if count and issued >= count:
                return "count_reached"
            slots = disp.free_slots(0.05)
            if not slots:
                continue
            want = min(slots, MAX_BATCH, (count - issued) if count else MAX_BATCH)
            n = limiter.acquire(want, max_wait=0.05)
            for _ in range(n):
                try:
                    op = wl.next_op(ctx)
                except IdSpaceExhausted as exc:
                    ctx.emit("warning", str(exc))
                    return "id_space_exhausted"
                if op is None:
                    return "completed"
                disp.submit(op)
                if counted is None or op.kind in counted:
                    issued += 1
        return "stopped"
    finally:
        ctx.progress["counted"] = issued


def worker_main(conn: Any, job: dict) -> None:
    """Entry point of a spawned worker process."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # the manager decides when we stop
    logging.basicConfig(level=job.get("log_level", "INFO"), format="%(asctime)s %(levelname)s [run %(run)s] %(message)s")
    run_tag = f"{job['run_id']}/{job['proc_index']}"
    logging.getLogger().handlers[0].addFilter(lambda r: setattr(r, "run", run_tag) or True)

    send = _Sender(conn)
    stop_event = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
    recorder = Recorder()
    workdir = tempfile.mkdtemp(prefix="ft-run-")
    cluster = None
    reason = "failed"
    ctx: RunContext | None = None
    disp: Dispatcher | None = None
    limiter = RateLimiter(job.get("rate", 0) / job["proc_count"])

    def emit(level: str, msg: str) -> None:
        getattr(log, "warning" if level == "warning" else "info")(msg)
        send({"type": "event", "level": level, "msg": msg, "t": time.time(), "proc": job["proc_index"]})

    def control() -> None:
        while not stop_event.is_set():
            try:
                if not conn.poll(0.2):
                    continue
                msg = conn.recv()
            except (EOFError, OSError):
                stop_event.set()  # manager went away
                return
            if msg.get("type") == "stop":
                stop_event.set()
            elif msg.get("type") == "set":
                if msg.get("rate") is not None:
                    limiter.set_rate(float(msg["rate"]) / job["proc_count"])
                if msg.get("concurrency") is not None and disp is not None:
                    disp.set_concurrency(int(msg["concurrency"]))
                if msg.get("params") and ctx is not None:
                    ctx.params.update(msg["params"])

    def reporter() -> None:
        while not stop_event.wait(STATS_INTERVAL_S):
            report()

    def report() -> None:
        snap = recorder.drain()
        snap.update(
            type="stats",
            t=time.time(),
            proc=job["proc_index"],
            inflight=disp.inflight if disp else 0,
            progress=dict(ctx.progress) if ctx else {},
        )
        send(snap)

    threading.Thread(target=control, name="control", daemon=True).start()
    try:
        cls = import_ref(job["workload_ref"])
        wl = cls()
        session = None
        spec = None
        if cls.needs_session:
            from .db.cluster import connect
            from .models import ConnectionSpec

            spec = ConnectionSpec(**job["spec"])
            cluster, session = connect(spec, workdir)
        ctx = RunContext(
            session=session,
            spec=spec,
            params=dict(job["params"]),
            recorder=recorder,
            emit=emit,
            proc_index=job["proc_index"],
            proc_count=job["proc_count"],
            stop_event=stop_event,
        )
        wl.setup(ctx)
        send({"type": "state", "state": "running", "proc": job["proc_index"], "progress": dict(ctx.progress)})
        threading.Thread(target=reporter, name="reporter", daemon=True).start()
        if cls.kind == "oneshot":
            result = wl.run_once(ctx)
            send({"type": "result", "data": result, "proc": job["proc_index"]})
            reason = "completed"
        else:
            disp = Dispatcher(ctx.session, recorder, job.get("concurrency", 64))  # workloads may swap the session (tests)
            monitor = None
            every = int(job["params"].get("monitor_every_s") or 0)
            if every and cls.needs_session and job["proc_index"] == 0:
                from .workloads.monitor import CompactionMonitor

                monitor = CompactionMonitor(ctx.session, ctx.keyspace, ctx.table, emit)
                monitor.start(every, stop_event, ctx.progress)
            reason = run_loop(
                wl,
                ctx,
                disp,
                limiter,
                share(int(job["params"].get("count", 0)), job["proc_index"], job["proc_count"]),
                float(job["params"].get("duration_s", 0)),
            )
            if not disp.drain(30):
                emit("warning", "timed out waiting for in-flight requests")
            if monitor is not None:
                monitor.publish(ctx.progress)  # final sample
    except Exception as exc:  # report, never crash silently
        log.error("run failed: %s", exc)
        send({"type": "error", "error": f"{type(exc).__name__}: {exc}"[:2000], "trace": traceback.format_exc()[-4000:], "proc": job["proc_index"]})
        reason = "failed"
    finally:
        stop_event.set()
        report()
        send({"type": "done", "reason": reason, "proc": job["proc_index"]})
        if cluster is not None:
            try:
                cluster.shutdown()
            except Exception:
                pass
        shutil.rmtree(workdir, ignore_errors=True)
        try:
            conn.close()
        except Exception:
            pass
