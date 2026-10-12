"""Database-backed OAuth attempts and opaque sessions; no provider tokens stored."""
from contextlib import asynccontextmanager
import hashlib

from fastapi import HTTPException


def digest(value: str) -> str:
    return hashlib.sha256(value.encode("ascii")).hexdigest()


class AuthStore:
    def __init__(self, database):
        self.db = database

    async def budget(self):
        async with self.db.transaction() as conn:
            row = await (await conn.execute("""INSERT INTO analyst_state.auth_rate_limit
                (id,window_start,requests) VALUES (1,date_trunc('minute',clock_timestamp()),1)
                ON CONFLICT(id) DO UPDATE SET window_start=EXCLUDED.window_start,
                requests=CASE WHEN auth_rate_limit.window_start=EXCLUDED.window_start
                    THEN least(auth_rate_limit.requests+1,1000000) ELSE 1 END RETURNING requests""")).fetchone()
        if row["requests"] > self.db.settings.auth_requests_per_minute:
            raise HTTPException(429, "rate_limited", headers={"Retry-After": "60"})

    @asynccontextmanager
    async def scoped(self, key, value, *, subject=None):
        async with self.db.transaction(subject=subject) as conn:
            await conn.execute("SELECT set_config(%s,%s,true)", ("bi_analyst." + key, value))
            yield conn

    async def begin(self, state, nonce, verifier, challenge):
        async with self.scoped("oauth_state", digest(state)) as conn:
            await conn.execute("""INSERT INTO analyst_state.oauth_attempts
                (state_hash,nonce_hash,provider_verifier,client_challenge,expires_at)
                VALUES (%s,%s,%s,%s,clock_timestamp()+interval '5 minutes')""",
                (digest(state), digest(nonce), verifier, challenge))

    async def claim_callback(self, state, nonce):
        async with self.scoped("oauth_state", digest(state)) as conn:
            row = await (await conn.execute("""UPDATE analyst_state.oauth_attempts SET phase='exchanging'
                WHERE state_hash=%s AND nonce_hash=%s AND phase='pending' AND expires_at>clock_timestamp()
                RETURNING provider_verifier""", (digest(state), digest(nonce)))).fetchone()
        if not row:
            raise HTTPException(400, "invalid_login")
        return row["provider_verifier"]

    async def lookup(self, account, user):
        async with self.scoped("external_identity", f"monday:{account}:{user}") as conn:
            row = await (await conn.execute("""SELECT subject FROM analyst_state.external_identities
                WHERE provider='monday' AND account_id=%s AND user_id=%s""", (account, user))).fetchone()
        if not row:
            raise HTTPException(403, "access_denied")
        return row["subject"]

    async def complete(self, state, principal):
        async with self.scoped("oauth_state", digest(state)) as conn:
            await conn.execute("""UPDATE analyst_state.oauth_attempts
                SET phase='complete',provider_verifier=NULL,subject=%s,permissions_version=%s,
                    expires_at=clock_timestamp()+interval '60 seconds'
                WHERE state_hash=%s AND phase='exchanging'""",
                (principal.subject, principal.permissions_version, digest(state)))

    async def exchange(self, state, challenge, token, ttl, *, idle=900):
        async with self.scoped("oauth_state", digest(state)) as conn:
            row = await (await conn.execute("""UPDATE analyst_state.oauth_attempts SET phase='used'
                WHERE state_hash=%s AND client_challenge=%s AND phase='complete'
                    AND expires_at>clock_timestamp()
                RETURNING subject,permissions_version""", (digest(state), challenge))).fetchone()
            if not row:
                raise HTTPException(400, "invalid_login")
            await conn.execute("SELECT set_config('bi_analyst.subject',%s,true)", (str(row["subject"]),))
            grant = await (await conn.execute("""SELECT 1 FROM analyst_state.principals
                WHERE subject=%s AND enabled AND company_wide AND permissions_version=%s""",
                (row["subject"], row["permissions_version"]))).fetchone()
            if not grant:
                raise HTTPException(403, "access_denied")
            if self.db.settings.pilot_only and row["subject"] not in self.db.settings.pilot_subjects:
                raise HTTPException(403, "access_denied")
            await conn.execute("SELECT set_config('bi_analyst.session_hash',%s,true)", (digest(token),))
            if self.db.schema_version == 8:
                session = await (await conn.execute("""INSERT INTO analyst_state.sessions
                    (token_hash,owner_id,permissions_version,expires_at,last_active_at,idle_timeout_seconds)
                    VALUES (%s,%s,%s,statement_timestamp()+make_interval(secs=>%s),statement_timestamp(),%s)
                    RETURNING expires_at""",
                    (digest(token), row["subject"], row["permissions_version"], ttl, idle))).fetchone()
            else:
                if ttl > 900:
                    raise HTTPException(503, "session_schema_required")
                session = await (await conn.execute("""INSERT INTO analyst_state.sessions
                (token_hash,owner_id,permissions_version,expires_at)
                VALUES (%s,%s,%s,statement_timestamp()+make_interval(secs=>%s)) RETURNING expires_at""",
                (digest(token), row["subject"], row["permissions_version"], ttl))).fetchone()
        return row["subject"], session["expires_at"]

    @staticmethod
    def check(row):
        reason = None
        if not row:
            reason = "authentication_required"
        elif row["revoked_at"] is not None:
            reason = "session_revoked"
        elif row["expires_at"] <= row["database_now"]:
            reason = "session_absolute_expired"
        elif row.get("idle_expires_at", row["expires_at"]) <= row["database_now"]:
            reason = "session_idle_expired"
        if reason:
            raise HTTPException(401, reason)
        return row

    async def validate_session(self, token):
        async with self.scoped("session_hash", digest(token)) as conn:
            columns = "*, last_active_at+make_interval(secs=>idle_timeout_seconds) AS idle_expires_at" if self.db.schema_version == 8 else "*"
            row = await (await conn.execute(f"SELECT {columns},clock_timestamp() AS database_now FROM analyst_state.sessions WHERE token_hash=%s",
                                           (digest(token),))).fetchone()
        return self.check(row)

    async def session(self, token):
        return await self.validate_session(token)

    async def record_activity(self, token):
        if self.db.schema_version != 8:
            raise HTTPException(400, "activity_unavailable")
        async with self.scoped("session_hash", digest(token)) as conn:
            row = await (await conn.execute("""SELECT *,last_active_at+make_interval(secs=>idle_timeout_seconds) AS idle_expires_at
                FROM analyst_state.sessions WHERE token_hash=%s FOR UPDATE""", (digest(token),))).fetchone()
            # Time is obtained AFTER the lock, including any wait for logout/activity.
            now = await (await conn.execute("SELECT clock_timestamp() AS now")).fetchone()
            if row:
                row["database_now"] = now["now"]
            self.check(row)
            subject = row["owner_id"]
            if self.db.settings.pilot_only and subject not in self.db.settings.pilot_subjects:
                raise HTTPException(403, "access_denied")
            if not self.db.settings.analyst_enabled:
                raise HTTPException(503, "analyst_disabled")
            await conn.execute("SELECT set_config('bi_analyst.subject',%s,true)", (str(subject),))
            grant = await (await conn.execute("SELECT enabled,company_wide,permissions_version FROM analyst_state.principals WHERE subject=%s", (subject,))).fetchone()
            if not grant or not grant["enabled"] or not grant["company_wide"]:
                raise HTTPException(403, "access_denied")
            if grant["permissions_version"] != row["permissions_version"]:
                raise HTTPException(403, "permissions_changed")
            updated = await (await conn.execute("""UPDATE analyst_state.sessions SET last_active_at=%s
                WHERE token_hash=%s AND revoked_at IS NULL AND expires_at>clock_timestamp()
                  AND last_active_at+make_interval(secs=>idle_timeout_seconds)>clock_timestamp()
                  AND EXISTS(SELECT FROM analyst_state.principals p WHERE p.subject=owner_id
                    AND p.enabled AND p.company_wide AND p.permissions_version=sessions.permissions_version)
                RETURNING *,last_active_at+make_interval(secs=>idle_timeout_seconds) AS idle_expires_at,
                  clock_timestamp() AS database_now""", (now["now"], digest(token)))).fetchone()
            if not updated:
                clock = await (await conn.execute("SELECT clock_timestamp() AS now")).fetchone()
                row["database_now"] = clock["now"]
                self.check(row)
                raise HTTPException(403, "permissions_changed")
        return updated

    async def revoke(self, token):
        async with self.scoped("session_hash", digest(token)) as conn:
            await conn.execute("""UPDATE analyst_state.sessions SET revoked_at=clock_timestamp()
                WHERE token_hash=%s AND revoked_at IS NULL""", (digest(token),))
