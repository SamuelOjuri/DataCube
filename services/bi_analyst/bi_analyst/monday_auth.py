"""Identity-only Monday OAuth 2.1 adapter. Never use ETL credentials or JWT claims."""
import asyncio
import json
import re

from fastapi import HTTPException
import httpx

AUTHORIZE_URL = "https://auth.monday.com/oauth2/authorize"
TOKEN_URL = "https://auth.monday.com/oauth_ms/oauth/token"
REVOKE_URL = "https://auth.monday.com/oauth_ms/oauth/revoke"
API_URL = "https://api.monday.com/v2"
ME_QUERY = "query { me { id account { id } enabled is_guest is_pending is_verified } }"


class MondayOAuth:
    def __init__(self, settings, client):
        self.settings, self.client = settings, client

    async def post(self, url, *, payload, headers=None):
        try:
            # Bound the whole operation as well as HTTPX's per-operation timeouts.
            async with asyncio.timeout(12):
                async with self.client.stream("POST", url, json=payload, headers=headers) as response:
                    response.raise_for_status()
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        body.extend(chunk)
                        if len(body) > 65536:
                            raise ValueError("oversized response")
                    data = json.loads(body)
                    if not isinstance(data, dict):
                        raise ValueError("invalid response")
                    return data
        except (httpx.HTTPError, TimeoutError, ValueError):
            raise HTTPException(503, "identity_provider_unavailable") from None

    def credentials(self):
        return {"client_id": self.settings.monday_client_id,
                "client_secret": self.settings.monday_client_secret.get_secret_value()}

    async def identify(self, code, verifier):
        tokens = await self.post(TOKEN_URL, payload={**self.credentials(),
            "grant_type": "authorization_code", "code": code, "code_verifier": verifier,
            "redirect_uri": self.settings.monday_redirect_uri})
        # Revoke every returned usable token, including on scope/identity denial.
        try:
            access = tokens.get("access_token")
            refresh = tokens.get("refresh_token")
            token_type = tokens.get("token_type")
            if (not isinstance(access, str) or not access or len(access) > 16384
                    or not isinstance(refresh, str) or not refresh or len(refresh) > 16384
                    or not isinstance(token_type, str) or token_type.lower() != "bearer"
                    or tokens.get("scope") != "me:read"):
                raise HTTPException(503, "identity_provider_unavailable")
            data = await self.post(API_URL, payload={"query": ME_QUERY},
                                   headers={"Authorization": access, "API-Version": "2026-04"})
            result = data.get("data")
            me = result.get("me") if isinstance(result, dict) else None
            if data.get("errors") or not isinstance(me, dict):
                raise HTTPException(503, "identity_provider_unavailable")
            account_data = me.get("account")
            account = str(account_data.get("id", "")) if isinstance(account_data, dict) else ""
            user = str(me.get("id", ""))
            if (account != self.settings.monday_account_id or not re.fullmatch(r"[1-9][0-9]{0,29}", user)
                    or me.get("enabled") is not True or me.get("is_guest") is not False
                    or me.get("is_pending") is not False or me.get("is_verified") is not True):
                raise HTTPException(403, "access_denied")
            return account, user
        finally:
            failed = False
            for kind in ("access_token", "refresh_token"):
                token = tokens.get(kind)
                if isinstance(token, str) and token and len(token) <= 16384:
                    try:
                        response = await self.post(REVOKE_URL, payload={**self.credentials(),
                            "token": token, "token_type_hint": kind})
                        failed |= response.get("success") is not True
                    except HTTPException:
                        failed = True
            if failed:
                raise HTTPException(503, "identity_provider_unavailable")
