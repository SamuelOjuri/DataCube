"""Analyst-only configuration. Never load the repository .env or ETL settings."""
import os
from urllib.parse import urlsplit
import re
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from psycopg.conninfo import conninfo_to_dict
from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator


DEFAULT_MODEL_TIMEOUT_SECONDS = 90
DEFAULT_WORKFLOW_TIMEOUT_SECONDS = 600


class Settings(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    read_dsn: SecretStr
    state_dsn: SecretStr
    environment: str = "production"
    analyst_enabled: bool = True
    pilot_only: bool = False
    pilot_subjects: list[UUID] = Field(default_factory=list, max_length=100)
    disabled_metrics: list[str] = Field(default_factory=list, max_length=32)
    telemetry_token: SecretStr | None = None
    model_input_usd_per_million: float | None = Field(default=None, ge=0, le=1000, allow_inf_nan=False)
    model_output_usd_per_million: float | None = Field(default=None, ge=0, le=1000, allow_inf_nan=False)
    cors_origins: list[str] = Field(default_factory=list)
    read_pool_size: int = Field(default=2, ge=1, le=20)
    state_pool_size: int = Field(default=2, ge=1, le=20)
    replica_count: int = Field(default=1, ge=1, le=20)
    deployment_overlap: int = Field(default=1, ge=1, le=2)
    connection_budget: int = Field(default=4, ge=2, le=200)
    pool_timeout_seconds: float = Field(default=3, gt=0, le=30)
    statement_timeout_ms: int = Field(default=5000, ge=100, le=30000)
    requests_per_minute: int = Field(default=60, ge=1, le=600)
    max_body_bytes: int = Field(default=32768, ge=1024, le=131072)
    auth_provider: str = "disabled"
    monday_client_id: str | None = None
    monday_client_secret: SecretStr | None = None
    monday_account_id: str | None = None
    monday_redirect_uri: str | None = None
    monday_read_token: SecretStr | None = None
    monday_read_api_version: str = Field(default="2026-07", pattern=r"^20[0-9]{2}-(01|04|07|10)$")
    monday_read_timeout_seconds: float = Field(default=10, ge=1, le=30)
    monday_read_max_bytes: int = Field(default=262144, ge=4096, le=524288)
    monday_read_concurrency: int = Field(default=2, ge=1, le=4)
    auth_frontend_url: str | None = None
    session_seconds: int = Field(default=900, ge=60, le=900)
    auth_requests_per_minute: int = Field(default=120, ge=10, le=600)
    business_timezone: str | None = "Europe/London"
    metric_evaluation_enabled: bool = False
    metric_max_rows: int = Field(default=1000, ge=1, le=1000)
    metric_max_bytes: int = Field(default=900000, ge=16384, le=900000)
    metric_concurrency: int = Field(default=2, ge=1, le=20)
    metric_timeout_seconds: float = Field(default=15, ge=1, le=60)
    workflow_enabled: bool = False
    gemini_api_key: SecretStr | None = None
    workflow_concurrency: int = Field(default=2, ge=1, le=8)
    workflow_timeout_seconds: float = Field(default=DEFAULT_WORKFLOW_TIMEOUT_SECONDS, ge=1, le=600)
    workflow_max_model_calls: int = Field(default=8, ge=3, le=12)
    model_timeout_seconds: float = Field(default=DEFAULT_MODEL_TIMEOUT_SECONDS, ge=1, le=90)

    @model_validator(mode="after")
    def validate_boundary(self):
        if self.environment not in {"production", "staging", "test"}:
            raise ValueError("environment must be production, staging or test")
        if self.telemetry_token and len(self.telemetry_token.get_secret_value()) < 32:
            raise ValueError("Telemetry requires an independent token of at least 32 characters")
        if (self.model_input_usd_per_million is None) != (self.model_output_usd_per_million is None):
            raise ValueError("Configure both reviewed model prices or neither")
        from .semantic import load_catalogue
        if not set(self.disabled_metrics) <= {m.id for m in load_catalogue().metrics}:
            raise ValueError("Unknown disabled metric")
        if self.workflow_enabled and (not self.gemini_api_key or not self.gemini_api_key.get_secret_value().strip()):
            raise ValueError("Workflow requires an independent Gemini API key")
        if (self.read_pool_size + self.state_pool_size) * self.replica_count * self.deployment_overlap > self.connection_budget:
            raise ValueError("Analyst pools across replicas exceed the reserved connection budget")
        targets = []
        for secret, role in ((self.read_dsn, "bi_analyst_reader"), (self.state_dsn, "bi_analyst_state")):
            try:
                info = conninfo_to_dict(secret.get_secret_value())
            except Exception:
                raise ValueError("Invalid analyst database connection configuration") from None
            # Supabase's shared session pooler routes role.project_ref to the role.
            username = info.get("user", "")
            pooler_user = (info.get("host", "").endswith(".pooler.supabase.com")
                           and username.startswith(role + ".") and len(username) > len(role) + 1)
            if (username != role and not pooler_user) or not info.get("host") or not info.get("dbname"):
                raise ValueError("Use explicit analyst role, host and database connection parameters")
            if any(k in info for k in ("service", "options", "hostaddr")) or "," in info["host"]:
                raise ValueError("Connection indirection and session options are not supported")
            local_test = self.environment == "test" and info["host"] in {"127.0.0.1", "localhost", "::1"}
            if not local_test and info.get("sslmode") != "verify-full":
                raise ValueError("Remote connections require sslmode=verify-full and a trusted CA")
            if info.get("port", "5432") != "5432" and not local_test:
                raise ValueError("Use a direct or session-pooler connection on port 5432")
            targets.append((info["host"], info.get("port", "5432"), info["dbname"]))
        if targets[0] != targets[1]:
            raise ValueError("Analyst pools must target the same database")
        if self.business_timezone is not None:
            try:
                ZoneInfo(self.business_timezone)
            except (ZoneInfoNotFoundError, ValueError):
                raise ValueError("Use an explicit IANA business timezone") from None
        if self.metric_evaluation_enabled and (
            self.environment != "test" or targets[0][0] not in {"127.0.0.1", "localhost", "::1"}
            or self.business_timezone is None
        ):
            raise ValueError("Uncertified metric evaluation requires a loopback test database and explicit timezone")
        if self.metric_concurrency > self.read_pool_size:
            raise ValueError("Metric concurrency must fit the analytical pool")
        for origin in self.cors_origins:
            url = urlsplit(origin)
            local = self.environment == "test" and url.hostname in {"localhost", "127.0.0.1"}
            if (url.scheme != "https" and not (local and url.scheme == "http")) or not url.netloc:
                raise ValueError("CORS origins must be explicit HTTPS origins")
            if url.path or url.query or url.fragment or url.username or url.password or "*" in origin:
                raise ValueError("CORS origins must not contain paths, credentials or wildcards")
        if self.auth_provider not in {"disabled", "monday"}:
            raise ValueError("auth_provider must be disabled or monday")
        if self.monday_read_token is not None:
            token = self.monday_read_token.get_secret_value()
            if not token.strip() or len(token) > 16384 or any(c.isspace() for c in token):
                raise ValueError("Monday source reads require a valid server token")
            if not re.fullmatch(r"[1-9][0-9]{0,29}", self.monday_account_id or ""):
                raise ValueError("Monday source reads require the approved account ID")
        if self.auth_provider == "monday":
            if not self.monday_client_id or not self.monday_client_secret or not self.monday_client_secret.get_secret_value().strip():
                raise ValueError("Monday client credentials are required")
            if not re.fullmatch(r"[1-9][0-9]{0,29}", self.monday_account_id or ""):
                raise ValueError("An approved Monday account ID is required")
            for target in (self.monday_redirect_uri, self.auth_frontend_url):
                url = urlsplit(target or "")
                local = self.environment == "test" and url.hostname in {"localhost", "127.0.0.1"}
                if (not url.netloc or (url.scheme != "https" and not (local and url.scheme == "http"))
                        or url.username or url.password or url.query or url.fragment
                        or "*" in (target or "") or any(c.isspace() for c in (target or ""))):
                    raise ValueError("Authentication redirects require fixed HTTPS URLs")
            if urlsplit(self.monday_redirect_uri).path != "/auth/callback":
                raise ValueError("Monday redirect path must be /auth/callback")
            front = urlsplit(self.auth_frontend_url)
            if f"{front.scheme}://{front.netloc}" not in self.cors_origins:
                raise ValueError("The authentication frontend must have an explicit CORS origin")
        return self

    @classmethod
    def from_env(cls):
        values = {}
        for name in cls.model_fields:
            value = os.environ.get("BI_ANALYST_" + name.upper())
            if value is not None:
                values[name] = [s.strip() for s in value.split(",") if s.strip()] if name in {"cors_origins", "pilot_subjects", "disabled_metrics"} else value
        return cls.model_validate(values)
