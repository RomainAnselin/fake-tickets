"""LDAP auth against an in-memory ldap3 mock directory, and TLS cert hot-reload."""

import datetime as dt
import ssl

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from ldap3 import MOCK_SYNC, OFFLINE_SLAPD_2_4, Connection, Server

from faketickets.config import Settings, TLSSettings
from faketickets.frontend.auth import AuthError, ldap_authenticate
from faketickets.server import CertReloader

BASE = "dc=home,dc=lab"


@pytest.fixture
def directory():
    server = Server("mock", get_info=OFFLINE_SLAPD_2_4)
    seed = Connection(server, user="cn=admin," + BASE, password="adminpw", client_strategy=MOCK_SYNC)
    seed.strategy.add_entry("cn=admin," + BASE, {"userPassword": "adminpw", "objectClass": "person", "sn": "a"})
    seed.strategy.add_entry(f"uid=alice,ou=people,{BASE}", {"uid": "alice", "userPassword": "alicepw", "objectClass": "inetOrgPerson", "sn": "A", "cn": "Alice"})
    seed.strategy.add_entry(f"uid=bob,ou=people,{BASE}", {"uid": "bob", "userPassword": "bobpw", "objectClass": "inetOrgPerson", "sn": "B", "cn": "Bob"})
    seed.strategy.add_entry(f"cn=simulators,ou=groups,{BASE}", {"objectClass": "groupOfNames", "cn": "simulators", "member": [f"uid=alice,ou=people,{BASE}"]})
    seed.bind()
    # ldap3's mock strategy keeps entries per Server object: reuse it for the code under test.
    return server


def _patch_mock(monkeypatch):
    import ldap3

    real = ldap3.Connection

    def mocked(server, *a, **kw):
        kw["client_strategy"] = MOCK_SYNC
        return real(server, *a, **kw)

    monkeypatch.setattr(ldap3, "Connection", mocked)


def test_ldap_template_bind_and_group(directory, monkeypatch):
    _patch_mock(monkeypatch)
    s = Settings(ldap_user_dn_template="uid={username},ou=people," + BASE, ldap_required_group=f"cn=simulators,ou=groups,{BASE}")
    assert ldap_authenticate(s, "alice", "alicepw", server=directory).startswith("uid=alice")
    with pytest.raises(AuthError, match="invalid"):
        ldap_authenticate(s, "alice", "wrong", server=directory)
    with pytest.raises(AuthError, match="group"):
        ldap_authenticate(s, "bob", "bobpw", server=directory)
    with pytest.raises(AuthError, match="required"):
        ldap_authenticate(s, "alice", "", server=directory)  # never allow unauthenticated binds


def test_ldap_search_then_bind(directory, monkeypatch):
    _patch_mock(monkeypatch)
    s = Settings(ldap_base_dn=BASE, ldap_bind_dn="cn=admin," + BASE, ldap_bind_password="adminpw")
    assert ldap_authenticate(s, "bob", "bobpw", server=directory) == f"uid=bob,ou=people,{BASE}"
    with pytest.raises(AuthError):
        ldap_authenticate(s, "nobody", "x", server=directory)
    with pytest.raises(AuthError):
        ldap_authenticate(s, "*", "bobpw", server=directory)  # filter injection is escaped


def _write_cert(path_crt, path_key, cn):
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])
    now = dt.datetime.now(dt.timezone.utc)
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
            .serial_number(x509.random_serial_number()).not_valid_before(now).not_valid_after(now + dt.timedelta(days=1))
            .sign(key, hashes.SHA256()))
    path_crt.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    path_key.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))


def test_cert_reloader(tmp_path):
    crt, key = tmp_path / "tls.crt", tmp_path / "tls.key"
    _write_cert(crt, key, "leaf-1")
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(crt, key)
    r = CertReloader(ctx, TLSSettings(cert_file=str(crt), key_file=str(key)))
    assert r.check() is False  # unchanged
    _write_cert(crt, key, "leaf-2")  # renewal
    assert r.check() is True
    key.write_text("garbage")  # half-written rotation must not break the listener
    assert r.check() is False
