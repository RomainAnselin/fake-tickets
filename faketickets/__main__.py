"""Command line: ``faketickets backend | frontend | run | genkey | hash-password``."""

from __future__ import annotations

import argparse
import json
import logging
import multiprocessing as mp
import secrets
import signal
import sys
import time

from . import __version__


def _logging(level: str) -> None:
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    # The driver warns on every protocol downgrade during negotiation; that's noise here.
    logging.getLogger("cassandra.cluster").addFilter(lambda r: "Downgrading core protocol" not in r.getMessage())


def cmd_server(role: str) -> None:
    from .config import Settings
    from .server import serve

    settings = Settings.from_env(role)
    _logging(settings.log_level)
    if role == "backend":
        from .backend.app import create_app
    else:
        from .frontend.app import create_app
    serve(create_app(settings), settings)


def cmd_run(args: argparse.Namespace) -> int:
    """Headless run (no HTTP): handy for a k8s Job or a quick terminal test."""
    from . import workloads
    from .models import ConnectionSpec
    from .worker import worker_main

    _logging("INFO")
    with open(args.connection, "r", encoding="utf-8") as fh:
        spec = ConnectionSpec(**json.load(fh))
    cls = workloads.get(args.workload)
    raw = dict(kv.split("=", 1) for kv in args.param)
    params = cls.validate(raw)
    if params.get("start_id") == -1:
        params["start_id"] = 0
    ctx = mp.get_context("spawn")
    parent, child = ctx.Pipe()
    job = {
        "run_id": "cli", "proc_index": 0, "proc_count": 1,
        "workload_ref": f"{cls.__module__}:{cls.__qualname__}", "params": params,
        "spec": spec.model_dump(), "rate": params.get("rate", 0), "concurrency": params.get("concurrency", 64),
    }
    p = ctx.Process(target=worker_main, args=(child, job), daemon=True)
    p.start()
    child.close()
    stop = lambda *_: parent.send({"type": "stop"})  # noqa: E731
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    rc, last_t = 0, None
    while True:
        try:
            msg = parent.recv()
        except EOFError:
            break
        kind = msg["type"]
        if kind == "stats":
            n = sum(msg["ok"].values())
            dt = (msg["t"] - last_t) if last_t else 1.0
            last_t = msg["t"]
            print(json.dumps({"t": time.strftime("%H:%M:%S"), "ops_s": round(n / dt, 1), "ok": msg["ok"], "err": msg["err"],
                              **msg["extra"], **msg["progress"], "last_error": msg["last_error"]}), flush=True)
        elif kind == "result":
            print(json.dumps(msg["data"], indent=2, default=str), flush=True)
        elif kind in ("event", "error"):
            print(f"[{kind}] {msg.get('msg') or msg.get('error')}", file=sys.stderr, flush=True)
            rc = 1 if kind == "error" else rc
        elif kind == "done":
            print(f"done: {msg['reason']}", file=sys.stderr)
            break
    p.join(30)
    return rc


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="faketickets", description=f"fake-tickets v{__version__}")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("backend", help="workload engine API (port 8444)")
    sub.add_parser("frontend", help="web UI + connection store (port 8443)")
    r = sub.add_parser("run", help="run one workload headless and print stats")
    r.add_argument("workload")
    r.add_argument("-c", "--connection", required=True, help="JSON file with a connection spec")
    r.add_argument("-p", "--param", action="append", default=[], help="name=value (repeatable)")
    sub.add_parser("workloads", help="print the workload catalogue as JSON")
    sub.add_parser("genkey", help="print a random master key for FT_MASTER_KEY / FT_BACKEND_TOKEN")
    hp = sub.add_parser("hash-password", help="hash a local-user password (reads stdin)")
    hp.add_argument("--stdin", action="store_true", default=True)
    args = ap.parse_args(argv)

    if args.cmd in ("backend", "frontend"):
        cmd_server(args.cmd)
        return 0
    if args.cmd == "run":
        return cmd_run(args)
    if args.cmd == "workloads":
        from . import workloads

        print(json.dumps([w.describe() for w in workloads.REGISTRY.values()], indent=2))
        return 0
    if args.cmd == "genkey":
        print(secrets.token_urlsafe(32))
        return 0
    if args.cmd == "hash-password":
        from .frontend.crypto import hash_password

        print(hash_password(sys.stdin.readline().rstrip("\n")))
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
