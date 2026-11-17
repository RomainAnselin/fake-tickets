"""Build a driver ``Cluster`` from a ConnectionSpec (Cassandra / DSE / Astra)."""

from __future__ import annotations

import base64
import logging
import os
import ssl
import tempfile
from typing import Any

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, Session
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy

from ..models import ConnectionSpec

log = logging.getLogger(__name__)


def _write_secret_file(workdir: str, name: str, data: bytes) -> str:
    path = os.path.join(workdir, name)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as fh:
        fh.write(data)
    return path


def build_ssl_context(spec: ConnectionSpec, workdir: str) -> ssl.SSLContext:
    ctx = ssl.create_default_context(cadata=spec.tls_ca_pem) if spec.tls_ca_pem else ssl.create_default_context()
    ctx.check_hostname = bool(spec.tls_verify_hostname)
    if not spec.tls_verify_hostname and not spec.tls_ca_pem:
        # No CA and no hostname check: encrypt only (lab clusters with self-signed certs).
        ctx.verify_mode = ssl.CERT_NONE
    if spec.tls_cert_pem and spec.tls_key_pem:
        cert = _write_secret_file(workdir, "client.crt", spec.tls_cert_pem.encode())
        key = _write_secret_file(workdir, "client.key", spec.tls_key_pem.encode())
        ctx.load_cert_chain(cert, key)
    return ctx


def build_cluster(spec: ConnectionSpec, workdir: str | None = None) -> Cluster:
    """Create (but do not connect) a Cluster. ``workdir`` holds SCB / key files."""
    workdir = workdir or tempfile.mkdtemp(prefix="ft-conn-")
    profile = ExecutionProfile(
        request_timeout=spec.request_timeout,
        load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc=spec.local_dc or None)),
    )
    kwargs: dict[str, Any] = {"connect_timeout": spec.connect_timeout}
    if spec.protocol_version:
        kwargs["protocol_version"] = spec.protocol_version

    if spec.kind == "astra":
        scb = _write_secret_file(workdir, "scb.zip", base64.b64decode(spec.astra_scb_b64 or ""))
        if spec.astra_token:
            auth = PlainTextAuthProvider("token", spec.astra_token)
        else:
            auth = PlainTextAuthProvider(spec.astra_client_id, spec.astra_client_secret)
        kwargs.setdefault("protocol_version", 4)
        return Cluster(
            cloud={"secure_connect_bundle": scb, "connect_timeout": spec.connect_timeout},
            auth_provider=auth,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
            **kwargs,
        )

    if spec.username:
        kwargs["auth_provider"] = PlainTextAuthProvider(spec.username, spec.password or "")
    if spec.tls_enabled:
        kwargs["ssl_context"] = build_ssl_context(spec, workdir)
    return Cluster(
        contact_points=spec.contact_points,
        port=spec.port,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        control_connection_timeout=max(10.0, spec.connect_timeout),
        **kwargs,
    )


def connect(spec: ConnectionSpec, workdir: str | None = None) -> tuple[Cluster, Session]:
    cluster = build_cluster(spec, workdir)
    try:
        session = cluster.connect()
    except Exception:
        cluster.shutdown()
        raise
    return cluster, session


def local_info(session: Session) -> dict[str, Any]:
    row = session.execute(
        "SELECT release_version, cluster_name, data_center, rack, cql_version, partitioner, host_id "
        "FROM system.local"
    ).one()
    info = {k: (str(v) if v is not None else None) for k, v in row._asdict().items()} if row else {}
    try:
        dse = session.execute("SELECT dse_version FROM system.local").one()
        info["dse_version"] = dse.dse_version if dse else None
    except Exception:
        info["dse_version"] = None
    return info


def ensure_keyspace(session: Session, spec: ConnectionSpec, keyspace: str, *, create: bool = True, dc: str | None = None) -> None:
    cluster = session.cluster
    if keyspace in cluster.metadata.keyspaces:
        return
    if spec.kind == "astra":
        raise RuntimeError(f"keyspace '{keyspace}' does not exist; create it in the Astra console first")
    if not create:
        raise RuntimeError(f"keyspace '{keyspace}' does not exist and create_keyspace is off")
    dc = dc or spec.local_dc or local_info(session).get("data_center") or "datacenter1"
    rf = spec.replication_factor
    # Never ask for more replicas than live nodes in the DC (single-node labs).
    nodes = sum(1 for h in cluster.metadata.all_hosts() if h.datacenter == dc) or 1
    if rf > nodes:
        log.warning("replication_factor %s > %s node(s) in %s; using %s", rf, nodes, dc, nodes)
        rf = nodes
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = "
        f"{{'class': 'NetworkTopologyStrategy', '{dc}': {rf}}}"
    )
    cluster.refresh_keyspace_metadata(keyspace)


def column_type(session: Session, keyspace: str, table: str, column: str) -> str | None:
    cluster = session.cluster
    try:
        cluster.refresh_table_metadata(keyspace, table)
    except Exception:
        pass
    ks = cluster.metadata.keyspaces.get(keyspace)
    tbl = ks.tables.get(table) if ks else None
    col = tbl.columns.get(column) if tbl else None
    return col.cql_type if col else None
