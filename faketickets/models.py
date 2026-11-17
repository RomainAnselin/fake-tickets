"""Shared pydantic models (connection spec travels frontend → backend → worker)."""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

Kind = Literal["cassandra", "dse", "astra"]

SECRET_FIELDS = ("password", "astra_client_secret", "astra_token", "astra_scb_b64", "tls_key_pem")


class ConnectionSpec(BaseModel):
    """Everything a worker needs to open a session. Contains secrets: never log it."""

    kind: Kind = "cassandra"
    # Cassandra / DSE
    contact_points: list[str] = Field(default_factory=list)
    port: int = Field(9042, ge=1, le=65535)
    local_dc: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    tls_enabled: bool = False
    tls_ca_pem: Optional[str] = None
    tls_cert_pem: Optional[str] = None
    tls_key_pem: Optional[str] = None
    tls_verify_hostname: bool = True
    # Astra
    astra_scb_b64: Optional[str] = None
    astra_client_id: Optional[str] = None
    astra_client_secret: Optional[str] = None
    astra_token: Optional[str] = None
    # Common
    keyspace: str = "fake_tickets"
    replication_factor: int = Field(3, ge=1, le=12)
    protocol_version: Optional[int] = Field(None, ge=3, le=6)
    connect_timeout: float = Field(10.0, gt=0, le=120)
    request_timeout: float = Field(20.0, gt=0, le=600)

    @field_validator("contact_points", mode="before")
    @classmethod
    def _split_points(cls, v):
        if isinstance(v, str):
            v = [p.strip() for p in v.replace(";", ",").split(",")]
        return [p for p in (v or []) if p]

    @field_validator("keyspace")
    @classmethod
    def _ks(cls, v: str) -> str:
        from .workloads.base import check_identifier

        return check_identifier(v, "keyspace")

    @model_validator(mode="after")
    def _check(self):
        if self.kind == "astra":
            if not self.astra_scb_b64:
                raise ValueError("Astra connections need a Secure Connect Bundle")
            if not (self.astra_token or (self.astra_client_id and self.astra_client_secret)):
                raise ValueError("Astra connections need a token or a client id + secret")
        elif not self.contact_points:
            raise ValueError("at least one contact point is required")
        return self

    def redacted(self) -> dict:
        d = self.model_dump()
        for f in SECRET_FIELDS:
            if d.get(f):
                d[f] = "***"
        return d

    def summary(self) -> str:
        if self.kind == "astra":
            return "astra (secure connect bundle)"
        return f"{self.kind} {','.join(self.contact_points)}:{self.port}"
