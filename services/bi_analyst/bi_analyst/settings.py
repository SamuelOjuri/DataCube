"""Analyst-only configuration. Never load the repository .env or ETL settings."""
import os
from urllib.parse import urlsplit
import re

from psycopg.conninfo import conninfo_to_dict
from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator


class Settings(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    read_dsn: SecretStr
    state_dsn: SecretStr
    environment: str = "production"
    cors_origins: list[str] = Field(default_factory=list)
    read_pool_size: int = Field(default=2, ge=1, le=20)
    state_pool_size: int = Field(default=2, ge=1, le=20)
    replica_count: int = Field(default=1, ge=1, le=20)
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
    auth_frontend_url: str | None = None
    session_seconds: int = Field(default=900, ge=60, le=900)
    auth_requests_per_minute: int = Field(default=120, ge=10, le=600)

    @model_validator(mode="after")
    def validate_boundary(self):
        if self.environment not in {"production", "staging", "test"}:
            raise ValueError("environment must be production, staging or test")
        if (self.read_pool_size + self.state_pool_size) * self.replica_count > self.connection_budget:
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
        for origin in self.cors_origins:
            url = urlsplit(origin)
            local = self.environment == "test" and url.hostname in {"localhost", "127.0.0.1"}
            if (url.scheme != "https" and not (local and url.scheme == "http")) or not url.netloc:
                raise ValueError("CORS origins must be explicit HTTPS origins")
            if url.path or url.query or url.fragment or url.username or url.password or "*" in origin:
                raise ValueError("CORS origins must not contain paths, credentials or wildcards")
        if self.auth_provider not in {"disabled", "monday"}:
            raise ValueError("auth_provider must be disabled or monday")
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
                values[name] = [s.strip() for s in value.split(",") if s.strip()] if name == "cors_origins" else value
        return cls.model_validate(values)
