# Session management on the existing production URLs

The owner's current hosting URLs are retained:

```dotenv
BI_ANALYST_MONDAY_REDIRECT_URI=https://datacube-bi-analyst.onrender.com/auth/callback
BI_ANALYST_AUTH_FRONTEND_URL=https://datacube-bi-analyst.netlify.app/auth/callback
BI_ANALYST_CORS_ORIGINS=https://datacube-bi-analyst.netlify.app
```

Build the frontend with `VITE_API_ORIGIN=https://datacube-bi-analyst.onrender.com`.
There is no custom-domain, DNS, gateway or Monday callback change. This implementation
supersedes the cookie transport recommendation in the original session plan.
Application authentication remains an explicit `Authorization: Bearer` header;
API fetches omit cookies. The existing five-minute OAuth nonce cookie is retained
only for the top-level Monday login/callback flow.

The compatibility default remains `BI_ANALYST_SESSION_TRANSPORT=bearer` and
`BI_ANALYST_SESSION_SECONDS=900`. Cookie mode is removed. Schema 7 remains supported
with the original fifteen-minute, memory-only bearer response. Longer issuance fails
closed before migration 010. Existing sessions keep their original expiry.

1. Deploy the compatibility API with `bearer` / `900` settings using the existing
   Render service dashboard. The example Blueprint does not update the deployed
   service and should not reconnect the retired staging Blueprint.
2. Apply the revised `20261012_010_analyst_session_management.sql` in one administrator
   transaction against the explicitly selected database after migrations 001-009.
   Verify schema 8, the packaged `permissions_sessions.sql` audit and readiness under
   both restricted runtime roles. Historical migrations and source privileges are unchanged.
3. Deploy the updated frontend to the existing Netlify project. Check sign-in and
   reload at the existing URLs while issuance remains 900 seconds. Schema-8 exchange
   preserves `access_token`, `expires_in`, `expires_at` and `subject` for compatibility.
4. After the new frontend is deployed, enable the longer policy on the same Render service:

```dotenv
BI_ANALYST_SESSION_TRANSPORT=bearer
BI_ANALYST_SESSION_SECONDS=28800
BI_ANALYST_SESSION_IDLE_SECONDS=1800
BI_ANALYST_SESSION_WARNING_SECONDS=120
```

PostgreSQL enforces a fixed eight-hour absolute limit and a thirty-minute inactivity
limit. Only explicit visible workspace interaction or "Stay signed in" reports
activity; reload, visibility checks, polling and stream heartbeats do not renew it.
Expired or revoked sessions cannot be revived. Logout remains usable after access
withdrawal, and stream revalidation disconnects the viewer without cancelling durable work.

Schema-8 credentials are stored only in the frontend tab's `sessionStorage`, bound to
the configured API origin. Reload restores nothing until the API verifies the token,
subject, permissions and both deadlines. New independent tabs sign in separately;
logout notifications make signed-in tabs revoke their own sessions and clear their UI.
An origin-local non-secret logout version also invalidates saved tokens in tabs that
missed the notification; their next restore revokes the old token before showing any UI.
Credentials are never placed in URLs, localStorage, drafts or cross-tab messages.
`sessionStorage` is readable by application JavaScript, unlike an HttpOnly cookie;
the existing restrictive script/connect CSP and fixed API destination remain in place.
Browser tab restoration may preserve sessionStorage, so server deadlines remain authoritative.

Logout immediately removes the normal restoration token. If revocation fails, retain
only a pending-revocation credential in that tab's sessionStorage and a non-secret
origin-local sign-out marker. Reload blocks restoration and retries revocation before
showing the workspace. A fresh login also revokes the previous/pending tab credential.
The error message states that server sign-out was not confirmed.

Expiry preserves bounded, subject-scoped drafts and uncertain submission metadata,
including the original idempotency key. Explicit logout clears drafts. Restore only
after the same subject is verified; protected results and provider credentials are
not persisted. Recover uncertain outcomes from durable conversation state and retry
only on deliberate user action with the original key.

For offboarding, disable the DataCube principal, increment its permissions version
and revoke its sessions using the existing administrator procedure. Monday-only
disablement can otherwise take up to eight hours from issuance, plus the pending
handoff window, to take effect. Activity does not contact Monday. Another Monday
authorization flow does not guarantee a fresh password/MFA challenge.

Rollback keeps this compatibility API/frontend and schema 8, sets bearer issuance
back to 900 seconds, and revokes already-issued extended sessions as administrator.
Do not downgrade the database or use schema-7-only binaries on schema 8.

Before production policy activation, run a real Monday pilot sign-in and verify
reload, idle/absolute warnings, multiple tabs, logout, access withdrawal and streaming.
The automated HTTPS browser fixture maps the actual hosting names to loopback on
test ports, exercises different browser sites with third-party-cookie phaseout,
and never contacts production. It does not attest to live Monday configuration.
Run the restricted PostgreSQL suite, frontend Node/build tests, workspace Playwright
suite and `npm run test:browser:https`.
