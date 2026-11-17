"""Secrets at rest.

* Connection secrets must be *recoverable* (we need them to connect), so they
  are encrypted, not hashed: AES-256-GCM with a per-record key derived as
  ``scrypt(master_key, salt=random 16 bytes)``. Each record has its own salt
  and nonce; the record uid is bound as associated data so a blob cannot be
  swapped onto another record.
* Local-user passwords are one-way: ``scrypt`` with a per-user random salt.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
from typing import Any

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

VERSION = 1
SALT_LEN = 16
NONCE_LEN = 12
# scrypt cost: ~30-60 ms per derivation — only paid when a run starts / a secret is saved.
N, R, P = 2**14, 8, 1


def _derive(master: bytes, salt: bytes) -> bytes:
    return hashlib.scrypt(master, salt=salt, n=N, r=R, p=P, dklen=32, maxmem=64 * 1024 * 1024)


class SecretBox:
    def __init__(self, master_key: str) -> None:
        if not master_key or len(master_key) < 16:
            raise ValueError("FT_MASTER_KEY must be at least 16 characters (use `faketickets genkey`)")
        self._master = master_key.encode()

    def seal(self, data: dict[str, Any], aad: str) -> bytes:
        salt = os.urandom(SALT_LEN)
        nonce = os.urandom(NONCE_LEN)
        ct = AESGCM(_derive(self._master, salt)).encrypt(nonce, json.dumps(data).encode(), aad.encode())
        return bytes([VERSION]) + salt + nonce + ct

    def open(self, blob: bytes, aad: str) -> dict[str, Any]:
        if not blob or blob[0] != VERSION:
            raise ValueError("unknown secret blob version")
        salt = blob[1:1 + SALT_LEN]
        nonce = blob[1 + SALT_LEN:1 + SALT_LEN + NONCE_LEN]
        ct = blob[1 + SALT_LEN + NONCE_LEN:]
        pt = AESGCM(_derive(self._master, salt)).decrypt(nonce, ct, aad.encode())
        return json.loads(pt)


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(SALT_LEN)
    dk = hashlib.scrypt(password.encode(), salt=salt, n=N, r=R, p=P, dklen=32)
    return "scrypt$%d$%d$%d$%s$%s" % (N, R, P, base64.b64encode(salt).decode(), base64.b64encode(dk).decode())


def verify_password(password: str, stored: str) -> bool:
    try:
        algo, n, r, p, salt_b64, dk_b64 = stored.split("$")
        if algo != "scrypt":
            return False
        dk = hashlib.scrypt(password.encode(), salt=base64.b64decode(salt_b64), n=int(n), r=int(r), p=int(p), dklen=32)
        return hmac.compare_digest(dk, base64.b64decode(dk_b64))
    except Exception:
        return False


def load_or_create_master_key(configured: str | None, data_dir: str) -> tuple[str, bool]:
    """Return (key, generated). Without FT_MASTER_KEY a key file is created in
    the data dir — fine for a laptop, but in k8s pass a Secret so that losing
    the PVC is the only way to lose the key."""
    if configured:
        return configured, False
    path = os.path.join(data_dir, "master.key")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read().strip(), False
    os.makedirs(data_dir, exist_ok=True)
    key = secrets.token_urlsafe(32)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as fh:
        fh.write(key)
    return key, True
