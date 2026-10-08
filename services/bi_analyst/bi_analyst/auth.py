"""Browser-bound OAuth and single-use, verifier-bound frontend handoff."""
import base64
import hashlib
import re
import secrets
from urllib.parse import urlencode, urlsplit

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import RedirectResponse, Response
from pydantic import BaseModel, ConfigDict, Field

from .identity import bearer
from .monday_auth import AUTHORIZE_URL

TOKEN_PATTERN = r"^[A-Za-z0-9_-]{43}$"


def challenge(verifier):
    return base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii")).digest()).rstrip(b"=").decode("ascii")


class Exchange(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    code: str = Field(pattern=TOKEN_PATTERN)
    verifier: str = Field(pattern=TOKEN_PATTERN)


def routes(principal):
    router = APIRouter(prefix="/auth")

    async def configured(request: Request):
        if request.app.state.settings.auth_provider != "monday":
            raise HTTPException(503, "identity_not_configured")

    def cookie_name(settings):
        return "__Host-datacube_oauth" if urlsplit(settings.monday_redirect_uri).scheme == "https" else "datacube_oauth_test"

    @router.get("/login", dependencies=[Depends(configured)])
    async def login(request: Request, client_challenge: str = Query(pattern=TOKEN_PATTERN)):
        await request.app.state.auth_store.budget()
        config = request.app.state.settings
        state, nonce, verifier = (secrets.token_urlsafe(32) for _ in range(3))
        await request.app.state.auth_store.begin(state, nonce, verifier, client_challenge)
        response = RedirectResponse(AUTHORIZE_URL + "?" + urlencode({
            "client_id": config.monday_client_id, "redirect_uri": config.monday_redirect_uri,
            "scope": "me:read", "response_type": "code", "state": state,
            "code_challenge": challenge(verifier), "code_challenge_method": "S256"}), status_code=303)
        response.set_cookie(cookie_name(config), nonce, max_age=300, httponly=True,
                            secure=urlsplit(config.monday_redirect_uri).scheme == "https", samesite="lax", path="/")
        return response

    @router.get("/callback", dependencies=[Depends(configured)])
    async def callback(request: Request):
        config = request.app.state.settings
        response = RedirectResponse(config.auth_frontend_url + "#error=sign_in_failed", status_code=303)
        try:
            await request.app.state.auth_store.budget()
            params = request.query_params
            if any(len(params.getlist(key)) > 1 for key in ("state", "code", "error", "status")):
                raise HTTPException(400, "invalid_login")
            state = params.get("state", "")
            nonce = request.cookies.get(cookie_name(config), "")
            if not re.fullmatch(TOKEN_PATTERN, state) or not re.fullmatch(TOKEN_PATTERN, nonce):
                raise HTTPException(400, "invalid_login")
            verifier = await request.app.state.auth_store.claim_callback(state, nonce)
            # Successful callbacks can omit status; reject explicit failure statuses.
            code = params.get("code", "")
            if params.get("error") or params.get("status") not in (None, "success") or not code or len(code) > 4096:
                raise HTTPException(400, "invalid_login")
            account, user = await request.app.state.monday.identify(code, verifier)
            subject = await request.app.state.auth_store.lookup(account, user)
            request.state.subject = subject
            actor = await request.app.state.store.authorize(subject)
            await request.app.state.auth_store.complete(state, actor)
            response = RedirectResponse(config.auth_frontend_url + "#code=" + state, status_code=303)
        except HTTPException as error:
            # Neither provider responses nor request parameters go to the frontend/logs.
            request.state.auth_outcome = error.detail
            request.state.audit_status = error.status_code
        response.delete_cookie(cookie_name(config), path="/", httponly=True,
                               secure=urlsplit(config.monday_redirect_uri).scheme == "https", samesite="lax")
        return response

    @router.post("/exchange", dependencies=[Depends(configured)])
    async def exchange(body: Exchange, request: Request):
        config = request.app.state.settings
        front = urlsplit(config.auth_frontend_url)
        if request.headers.get("origin") != f"{front.scheme}://{front.netloc}":
            raise HTTPException(403, "origin_denied")
        await request.app.state.auth_store.budget()
        token = secrets.token_urlsafe(32)
        subject, expires = await request.app.state.auth_store.exchange(body.code, challenge(body.verifier), token, config.session_seconds)
        request.state.subject = subject
        return {"access_token": token, "token_type": "Bearer", "expires_at": expires, "subject": subject}

    @router.get("/session")
    async def session(request: Request, actor=Depends(principal)):
        row = await request.app.state.auth_store.session(bearer(request))
        return {"subject": actor.subject, "permissions_version": actor.permissions_version, "expires_at": row["expires_at"]}

    @router.post("/logout", dependencies=[Depends(configured)])
    async def logout(request: Request):
        token = bearer(request)
        # Revocation remains usable after DataCube access has been withdrawn.
        try:
            row = await request.app.state.auth_store.session(token)
            request.state.subject = row["owner_id"]
        except HTTPException:
            pass
        await request.app.state.auth_store.revoke(token)
        return Response(status_code=204)

    return router
