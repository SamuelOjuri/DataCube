# Phase 3: Monday sign-in for the approved pilot

Implemented 8 October 2026. This closes the authentication **code** gap in Phase 3.
It supersedes the earlier request to defer all authentication. It does not apply a
hosted migration, register a Monday app, grant anyone access or deploy either service.

**Deployment update, 9 October 2026:** follow the
[single-production-deployment plan](bi-analyst-phase7.md). Following deletion of
the staging instances, create one new production Render API and one new production
Netlify project, with one Monday identity app. Do not reuse staging URLs or replace
unrelated existing sites. Historical verification below is not a production
readiness claim.

## Assessment of the supplied analysis

The recommendation is appropriate for a pilot whose members already use Monday.
The implementation uses Monday for identity and DataCube for authorization.
Company-wide reporting requires an individually approved DataCube principal;
Monday board permissions do not carry over to PostgreSQL reporting. Guests,
disabled, pending, unverified, foreign-account and unprovisioned users are denied.
Viewer status alone does not grant or deny DataCube access: the explicit
company-wide grant is authoritative after those eligibility checks.

Monday's current [OAuth 2.1 guide](https://developer.monday.com/apps/docs/migrating-to-the-new-oauth-flow)
documents mandatory S256 PKCE, the new token/revocation endpoints and a per-app-version
enablement toggle. Its [me reference](https://developer.monday.com/api-reference/reference/me)
documents `me:read` and the account and user eligibility fields. This is an OAuth
identity lookup, not an OpenID Connect ID-token validator. The backend obtains
identity through `me`; it does not trust browser-supplied user IDs or decode a
provider token as identity proof.

The deliberate V1 simplification is **reauthentication instead of retained refresh
tokens**. Provider credentials exist only during the backend callback. Both access
and refresh tokens are revoked after the identity lookup, including on rejection;
a failed revocation prevents DataCube session issuance. No Monday token is stored
in the database or returned to the browser. A provider outage can prevent sign-in.
There is no durable revocation retry for a provider outage: any token whose
revocation could not be confirmed is discarded locally. It has not been issued to
the frontend and can no longer be used by this service. Use a separate app from ETL.

## Flow and security boundary

1. The React client creates a random handoff verifier, stores it in tab-local
   `sessionStorage` for the pending login only, and navigates to `/auth/login`
   with its S256 challenge. The backend generates separate Monday PKCE material.
2. A five-minute database attempt binds a random OAuth state to the frontend
   challenge and an HttpOnly, Secure, SameSite=Lax host-only browser nonce cookie.
   The callback must match both state and cookie. PostgreSQL atomically claims the
   attempt before exchanging the code; callback replay cannot call Monday again.
3. The backend exchanges the code at the new Monday endpoint, requesting only
   `me:read`, checks `me`, revokes the provider tokens, looks up the approved
   `(provider, account_id, user_id)` mapping and checks the DataCube grant.
4. A fixed configured frontend URL receives a single-use handoff code in the
   fragment. This is the attempt's random state, **not a session token**. Redemption
   requires the independent frontend verifier and exact configured Origin. The
   client removes the fragment before exchange. No caller-supplied return URL is
   accepted. The completed handoff expires after 60 seconds.
5. `/auth/exchange` atomically redeems the attempt and rechecks the current grant
   and permission version. It returns a random 256-bit opaque DataCube bearer in
   the response body, the absolute `expires_at`, and `expires_in` (the configured
   lifetime at issuance, in seconds, at most 900). Only its SHA-256 digest is stored in PostgreSQL. Browser
   session tokens remain in memory; requests use Authorization headers and omit
   cross-origin cookies. Reloading the page requires another Monday sign-in.
6. Each protected request checks session expiry/revocation, current DataCube
   access, the session permission version, and existing owner/RLS restrictions.
   Session state and pre-authentication rate budgets are shared across replicas.

Sessions have an absolute lifetime of 15 minutes (configurable down to one minute,
never above 15), with no sliding renewal. The browser uses `expires_in` and a
monotonic timer, subtracting the entire exchange round-trip before accepting the
session. It checks that same deadline before requests; `expires_at` is metadata,
not a comparison against the computer's wall clock. PostgreSQL remains authoritative
for expiry and revocation. Small clock differences cannot reject a valid session
or extend its lifetime. Monday eligibility is checked at every
sign-in. A Monday-only disable/removal may take the remaining session lifetime,
plus at most the 60-second pending handoff, to take effect; it is not an immediate
DataCube logout. Disable the DataCube principal and increment its permission
version for immediate denial on subsequent requests. Existing in-flight responses
cannot be recalled. The frontend clears its session at expiry and on 401/403, and
aborts outstanding requests at logout. Future streaming integration must keep the
stream's cancellation handle until its body is finished, and reauthorize reconnects.

Logout revokes the current session, including when its DataCube grant has been
withdrawn. It is idempotent. It does not sign the person out of Monday itself.
If network logout fails, the frontend still clears local state and reports that
server revocation was not confirmed. Other sessions are revoked through offline
administration or a permission-version change.

| Endpoint | Contract |
|---|---|
| `GET /auth/login?client_challenge=…` | Top-level browser navigation; creates state/PKCE, sets callback-binding cookie, redirects to Monday |
| `GET /auth/callback` | Fixed Monday redirect URI; validates state/cookie and exchanges code server-side |
| `POST /auth/exchange` | JSON `code`, `verifier`; exact frontend Origin; single-use session issuance |
| `GET /auth/session` | Bearer required; returns internal subject, permission version and expiry |
| `POST /auth/logout` | Bearer required; revokes current session |

Requests use no-store responses and no-referrer policy. Logs contain route templates,
request IDs and safe outcome codes, not callback parameters, bearer tokens or
provider responses. Known subjects retain durable auditing, including grant denials;
unknown identities are logged without inventing a user. Audit failure prevents
release of authenticated responses. Keep Uvicorn access logs disabled, and configure
hosting/proxy logs to redact OAuth callback query parameters. CORS is explicit and
does not allow credentials; it is not the authorization boundary.

## Database migration and provisioning

Migration [005](../src/database/migrations/20261008_005_analyst_auth.sql) adds four
private tables: `external_identities`, `oauth_attempts`, `sessions`, `auth_rate_limit`.
They are owned by the existing NOLOGIN migrator and use forced RLS. The state runtime
can read an exact identity mapping but cannot provision users or alter grants.
It can create sessions and update only their revocation timestamp, not extend their
lifetime or change their owner/version. The analyst reader cannot read auth state.
No additional runtime role, SECURITY DEFINER function, operational write permission
or shared PUBLIC/ETL/reporting privilege is introduced.

The existing TEST-installed migration 003 and its audit remain byte-for-byte intact.
005 has a separate matching [versioned audit](../services/bi_analyst/bi_analyst/permissions_auth.sql),
and advances only `analyst_state.schema_version` from 3 to 5. Runtime code supports
state version 3 with authentication disabled and version 5 with either setting.
Monday authentication refuses readiness/startup without version 5. The Phase 2
semantic migration 004 is independent and remains part of the reporting rollout.

For the explicitly approved deployment database, first check which migrations are
installed. Apply 005 only if missing, as the existing platform administrator,
after 003, in a single transaction. A database already on schema 7 must not rerun
this earlier migration:

```text
psql --single-transaction --set ON_ERROR_STOP=1 --file src/database/migrations/20261008_005_analyst_auth.sql
```

Supply the target securely and explicitly; never inherit the root ETL `.env` or
choose production implicitly. The Supabase SQL editor equivalent is the whole
file inside `BEGIN; … COMMIT;`. No hosted database was migrated in this change.

Provision each approved person using verified Monday IDs and a stable internal
UUID. The following illustrates the parameters, not a list of approved users:

```sql
BEGIN;
SET LOCAL ROLE bi_analyst_migrator;
INSERT INTO analyst_state.principals(subject,enabled,company_wide,permissions_version)
VALUES ('<internal UUID>',true,true,1);
INSERT INTO analyst_state.external_identities(provider,account_id,user_id,subject)
VALUES ('monday','<approved account ID>','<verified Monday user ID>','<same internal UUID>');
COMMIT;
```

Use an existing principal UUID when connecting an existing user. Never link identities
by email, reset permission versions, or reuse a former user's mapping. Microsoft or
another provider can be added later without changing conversation owners, but that
requires an explicit extension of the provider constraint and adapter.

Immediate DataCube revocation, in an administrator/migrator transaction:

```sql
UPDATE analyst_state.principals
SET enabled=false, permissions_version=permissions_version+1
WHERE subject='<internal UUID>';
UPDATE analyst_state.sessions SET revoked_at=clock_timestamp()
WHERE owner_id='<same internal UUID>' AND revoked_at IS NULL;
```

Increment the permission version on **every** grant change, including re-enablement.
The runtime has no auth-state DELETE privilege. Schedule daily bounded maintenance
under the offline administrator/migrator role; expired rows never authorize access:

```sql
DELETE FROM analyst_state.oauth_attempts WHERE state_hash IN
 (SELECT state_hash FROM analyst_state.oauth_attempts
  WHERE expires_at<now()-interval '1 day' ORDER BY expires_at LIMIT 10000);
DELETE FROM analyst_state.sessions WHERE token_hash IN
 (SELECT token_hash FROM analyst_state.sessions
  WHERE expires_at<now()-interval '7 days' ORDER BY expires_at LIMIT 10000);
```

Repeat bounded batches as necessary. Keep durable audit retention separate; these
commands do not delete histories or audit events. The shared pre-auth budget defaults
to 120 login/callback/exchange requests per minute (up to 40 full sign-ins). It bounds
database/provider load without trusting spoofable forwarded IP headers; it is also
a shared availability limit. Edge abuse controls and retention scheduling remain
deployment tasks. There is no startup worker or automatic maintenance job.

## Monday, Render and Netlify setup

1. Register a dedicated identity-only Monday app. Set its sole scope to `me:read`,
   configure the exact backend `/auth/callback` redirect, enable **New OAuth Flow**
   for the intended version, and arrange app installation/approval in the approved
   account. Test the draft using Monday's **Active for me**, then promote it to live.
   Use the final production URLs; this plan does not need a second identity app.
2. Review the approved production database's migration inventory and provision the
   explicitly approved pilot. Apply only missing migrations using the Phase 7
   sequence. Retain restricted runtime DSNs, TLS and NOLOGIN owner roles; do not
   substitute TEST credentials for production credentials.
3. Configure the independent [backend environment](../services/bi_analyst/env.example):
   `BI_ANALYST_MONDAY_CLIENT_ID`, secret, approved account ID, fixed backend callback,
   fixed frontend callback and explicit CORS origin. Set `BI_ANALYST_AUTH_PROVIDER=monday`
   only after these steps and the Phase 7 pilot/model configuration. The new Render
   service is created directly through **New > Web Service**; manage its settings
   in the dashboard, without importing or reconnecting the old Blueprints.
4. Set `VITE_API_ORIGIN` for the `web/` build on the new Netlify production
   project. It is the only public frontend
   configuration; no Supabase key, client secret, database credential or Monday
   token belongs there. Use Node 22.12+ or 24 and `npm ci`, then `npm run build`.
   The root [Netlify configuration](../netlify.toml) includes SPA callback routing,
   no-store and no-referrer; the build generates CSP with the exact API origin.
   Disable deploy previews and branch deploys for this route. Do not point
   `STAGING_API_ORIGIN` at production or allow preview origins through CORS.
5. Verify approved sign-in, explicit denial cases, cancelled consent, refresh/reload,
   logout, session expiry and two-user conversation isolation through the actual
   Netlify/Render/Monday path. Confirm provider revocation succeeds for both token
   types, all eligibility fields are available with `me:read`, and the intended
   app version uses the new endpoint. Record evidence before pilot release.

The verification recorded below used synthetic loopback tests, not live Monday
acceptance. Record real sign-in evidence through the single-deployment pilot
checklist. Authentication does not certify metric definitions; the analytical
workspace is documented in [Phase 6](bi-analyst-phase6.md).

Rollback uses `BI_ANALYST_AUTH_PROVIDER=disabled` on the new code, which denies all
data endpoints. Do not roll back to the older binary after 005: its version-3 audit
will reject the new auth tables. Keep the additive state and audit history intact.

## Troubleshooting a successful exchange followed by a sign-in error

In Render's sanitised `http_request` logs, check the route, status and
`auth_outcome`. A callback's HTTP 303 alone does not prove success: failures also
redirect. A callback with no failure outcome followed by `/auth/exchange` HTTP 200
shows that the backend issued a DataCube session. Check frontend session handling
before changing Monday scopes, grants or CSP.

The earlier frontend compared `expires_at` against the computer's clock and
rejected any apparent lifetime over 900 seconds. Even a computer a few seconds
behind the server could therefore reject a valid 15-minute session. The current
duration-based contract avoids this. Deploy the updated Render backend **before**
the updated Netlify frontend: older clients ignore the added `expires_in` field,
but the updated frontend requires it. Then reload the frontend and begin a fresh
login; callback handoffs cannot be reused. No database migration is needed.

Verify `/auth/exchange` HTTP 200, `/auth/session` HTTP 200, an approved user's
workspace, and logout. Do not share response tokens, callback query strings,
cookies or full network exports when collecting diagnostics.

## Verification

Local results on 8 October 2026: **216 full analyst regression tests passed**;
after the final malformed-provider-response hardening, **51 authentication and
configuration tests passed** (including two added malformed-response cases).
All **6 client tests** passed. The frontend production build and version 0.3.1
Python wheel build passed; the wheel contains the authentication modules and
version-5 permission audit. `git diff --check` passed.

The authentication integration tests use a disposable loopback PostgreSQL database,
the real 003/005 migrations, actual restricted logins, and mocked Monday HTTPS.
They do not override the identity dependency. Coverage includes state/cookie binding,
PKCE, callback and concurrent handoff replay, fixed origin, denied identities/grants,
revocation, expiry, permission versions, restart/reconnect, cross-user ownership,
provider failures, shared rate limits and privilege drift. Existing source ACLs are
snapshotted before/after 005. The historical version-3 regression tests still run.

The Node client tests cover verifier storage, callback URL cleanup, unsolicited
callbacks, expiry/denial cleanup, logout cancellation, failed network revocation
and reconnection headers. TypeScript checking and the Vite production build pass.
In-app browser automation failed during initialization even after reset, so visual
browser inspection is not claimed. Live Monday acceptance is also not claimed.

```powershell
$env:BI_ANALYST_TEST_DSN = 'host=127.0.0.1 port=55439 dbname=postgres user=postgres connect_timeout=15 sslmode=disable gssencmode=disable'
& .\report.venv\Scripts\python.exe -m pytest services/bi_analyst/tests -q -p no:cacheprovider --basetemp outputs/pytest_auth_check
# From web/:
npm test
npm run build
```
