"""Synthetic ticket payloads (same shape as v1)."""

from __future__ import annotations

import random
import string
import time

# Same owner pool as v1 (src/faketickets.py).
OWNERS = [
    "Romain", "Ryan", "Bettina", "Navaneetha", "Rachan", "Ivan", "Alberto", "Peter", "Pav",
    "Cordell", "Frez", "Adjete", "Paco", "Pranjal", "Yuva", "Anjali", "Suraj",
]

_PAD_SRC = "".join(random.Random(42).choices(string.ascii_letters + string.digits + " ", k=1 << 17))


def now_ms() -> int:
    return int(time.time() * 1000)


def fmt_ms(ms: int) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ms / 1000.0)) + f".{ms % 1000:03d}"


def padding(rng: random.Random, n: int) -> str:
    """Pseudo-random filler (not trivially compressible) of ``n`` chars."""
    if n <= 0:
        return ""
    start = rng.randrange(0, len(_PAD_SRC) - n) if n < len(_PAD_SRC) else 0
    return _PAD_SRC[start:start + n]


def ticket_row(i: int, rng: random.Random, ts_ms: int | None = None, payload_bytes: int = 0, tag: str = "") -> tuple:
    """Return ``(id, ownedby, ticket, time, notes)`` for record ``i``."""
    ts = ts_ms if ts_ms is not None else now_ms()
    owner = rng.choice(OWNERS)
    ticket = rng.randint(0, 100_000)
    if i % 2 == 0:
        notes = f"{tag}Python writing data with value {i} for {owner} at {fmt_ms(ts)}"
    else:
        notes = f"{tag}{fmt_ms(ts)} Odd entries have a different data string for record {i} for {owner}"
    if payload_bytes:
        notes = notes + " " + padding(rng, payload_bytes)
    return (i, owner, ticket, ts, notes)
