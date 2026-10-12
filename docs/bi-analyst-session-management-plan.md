# DataCube Analyst session-management implementation plan

Proposed 12 October 2026. This document describes implementation work; the current
15-minute authentication behaviour remains in place. It does not change application
code, apply a database migration, configure domains, or deploy either service.

## 1. Recommended implementation

Keep the existing opaque, database-backed DataCube sessions. Move the browser's
session credential into an API-host-only HttpOnly cookie, add a server-enforced
inactivity deadline, and restore a valid session when the page loads.

For this repository, a separate short-lived access token and refresh-token service
is unnecessary. Every protected request already consults PostgreSQL and the current
DataCube principal. Quiet extension means updating the session's activity timestamp
within its fixed maximum lifetime. Generate a new credential on every successful
Monday sign-in, including sign-in after expiry.

Periodic credential rotation can be added separately if required. It is not required
to fix the current interruptions, and it needs a stable session identifier, concurrent
request handling, and a way to keep existing streams valid. Do not implement rotation
by creating a new eight-hour session every fifteen minutes: that would remove the
maximum-lifetime guarantee.

| Policy | Initial value | Behaviour |
|---|---|---|
| Absolute lifetime | 28,800 seconds / 8 hours | Fixed when issued; activity cannot move it. |
| Inactivity lifetime | 1,800 seconds / 30 minutes | Measured from the last activity accepted by the server. |
| Expiry warning | 120 seconds | Warn before the earlier of the idle and absolute deadlines. |
| Activity reporting | At most once per 60 seconds while interacting | Report actual interaction; do not run an unconditional heartbeat. |
| Reload | Restore the existing valid session | Reload and session checks do not reset inactivity. |
| Explicit logout | Revoke the session and clear the cookie | Other tabs using that cookie also lose access. |
| Permission withdrawal | Deny subsequent requests and stream checks | Preserve current principal and permission-version checks. |

The durations are proposed policy choices for the internal reporting application.
The distinction between overall and inactivity timeouts follows
[NIST's session-management guidance](https://pages.nist.gov/800-63-4/sp800-63b/session/).
[OWASP](https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html#session-expiration)
also distinguishes optional credential renewal and describes 4–8-hour absolute
lifetimes for office applications. Eight hours is a cap between sign-ins, not a
promise that an inactive user remains signed in all day.

## 2. Where the current behaviour comes from

| File | Current responsibility | Required change |
|---|---|---|
| `services/bi_analyst/bi_analyst/settings.py` | `session_seconds` defaults to 900 and is capped at 900 | Add explicit transport and idle-policy settings; validate separate limits. |
| `services/bi_analyst/bi_analyst/auth.py` | PKCE login, provider callback, handoff exchange, session inspection, logout | Issue/clear a session cookie; add activity endpoint and CSRF validation. |
| `services/bi_analyst/bi_analyst/auth_store.py` | Stores token digests and checks fixed expiry | Store activity policy; distinguish validation from activity updates. |
| `services/bi_analyst/bi_analyst/identity.py` | Extracts bearer and loads its session | Resolve cookie credentials in cookie mode; preserve expiry and version information. |
| `services/bi_analyst/bi_analyst/api.py` | Principal checks, CORS, auditing and repeated stream authorization | Enable controlled credentialed CORS; enforce request origin/CSRF; preserve stream validation. |
| `services/bi_analyst/bi_analyst/database.py` | Schema/readiness checks and runtime privilege auditing | Support the new schema and exact additional activity-column privilege. |
| `src/database/migrations/20261008_005_analyst_auth.sql` | Immutable session fields, RLS, 15-minute database constraint | Preserve this historical migration; add a new migration. |
| `web/src/auth.mjs` | In-memory bearer, fixed timeout, clear on every 401/403 | Cookie requests, bootstrap, activity reporting, reason-aware expiry. |
| `web/src/main.tsx` | Shows sign-in when the in-memory session disappears | Restore sessions before rendering; distinguish checking, authenticated and signed-out states. |
| `web/src/Workspace.tsx` | Local question/reply state and durable conversation navigation | Preserve unsent input across expiry/reload without resubmitting uncertain requests. |
| `web/src/api.ts` | API errors and stream recovery | Keep structured auth reasons through the request wrapper. |
| `web/src/types.ts` | Small session/auth type definitions | Add session deadlines, warning state, restore and activity methods. |
| `web/build-policy.mjs`, `web/vite.config.ts`, `netlify.toml` | Explicit API origin, generated CSP and SPA routing | Keep the exact origin policy; point it at the chosen cookie-compatible API host. |

The existing authentication design is documented in [bi-analyst-auth.md](bi-analyst-auth.md).
The deployment model remains the single production frontend/API described in
[bi-analyst-phase7.md](bi-analyst-phase7.md).

## 3. Establish cookie-compatible routing first

The observed frontend is `https://datacube-bi-analyst.netlify.app`. The deployment
runbook uses a Render API, ordinarily on a separate `onrender.com` site. Verify the
actual deployed API origin before making changes. These hosting domains cannot
share a parent-domain cookie.

The recommended deployment is two HTTPS custom hosts under a company-controlled
registrable domain, for example:

```text
Frontend: https://analyst.example.com       -> existing Netlify project
API:      https://analyst-api.example.com   -> existing Render service
```

These names are placeholders. Both hosts must belong to the same registrable domain
and use HTTPS. They are different origins but the same site. Set the session cookie
on the API host with no `Domain` attribute; frontend JavaScript does not need to read
it. Browser requests to the API use `credentials: 'include'`.

Credentialed cross-origin requests require an explicit permitted origin and
`Access-Control-Allow-Credentials`; third-party cookie restrictions still apply to
cross-site requests. See [MDN's CORS guidance](https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/CORS).
Using sibling custom hosts avoids depending on third-party-cookie acceptance for
the production session.

Update the Monday registered callback, `BI_ANALYST_MONDAY_REDIRECT_URI`,
`BI_ANALYST_AUTH_FRONTEND_URL`, `BI_ANALYST_CORS_ORIGINS` and Netlify's
`VITE_API_ORIGIN` together. Keep the callback on the API host. Use exact origins,
and exclude preview sites and the retired frontend origin from production CORS.

If a company-controlled domain is unavailable, use a deliberately configured
same-origin gateway instead. That is a separate routing task requiring validation
of cookie headers, OAuth callbacks, streamed responses and caching, plus an adapted
read-request origin policy: same-origin GET requests can omit the Origin header.
Continue requiring exact Origin/CSRF validation for unsafe methods. A generic
Netlify rewrite is not sufficient evidence that the application's SSE behaviour
will work. [Netlify documents proxy rewrites](https://docs.netlify.com/manage/routing/redirects/rewrites-proxies/);
their suitability for this application must be tested. Do not ship a cookie-only
frontend on the current cross-site hosts and assume `SameSite=None` resolves browser
privacy restrictions.

## 4. Configuration contract

Reuse `BI_ANALYST_SESSION_SECONDS` as the immutable absolute lifetime to avoid
introducing two competing duration variables. Add:

```dotenv
BI_ANALYST_SESSION_TRANSPORT=cookie
BI_ANALYST_SESSION_SECONDS=28800
BI_ANALYST_SESSION_IDLE_SECONDS=1800
BI_ANALYST_SESSION_WARNING_SECONDS=120
```

In `Settings`, use transport values `bearer` and `cookie`. Keep the compatibility
release's default transport `bearer`, with its existing 900-second default.
Cookie rollout explicitly sets all four values. Validate:

- Bearer mode: absolute lifetime 60–900 seconds.
- Cookie mode: absolute lifetime 60–28,800 seconds; requires the new schema.
- Idle lifetime: 60–1,800 seconds and no greater than the absolute lifetime in cookie mode.
- Cookie-mode warning: positive and shorter than both configured lifetimes. Do not
  impose this new warning validation on existing short-lived bearer configurations.
- Production cookie mode requires HTTPS and one exact frontend origin matching the
  frontend callback origin. Local HTTP exceptions remain restricted to test mode.

The server returns the policy to the frontend. Do not add public `VITE_` timeout
settings: `web/build-policy.mjs` intentionally permits only `VITE_API_ORIGIN`.
Store the issued policy on the session row, so later settings changes cannot
silently lengthen an existing session. Apply urgent policy tightening by revoking
existing sessions as part of that policy change.

Update `services/bi_analyst/env.example` and deployment documentation. The production
runbook currently uses dashboard-managed Render settings; updating `render.yaml`
alone does not change the deployed service. Keep that example consistent if it is
maintained, without reconnecting the old staging Blueprint.

## 5. Database migration and privilege boundary

Add the next migration, provisionally
`src/database/migrations/20261012_010_analyst_session_management.sql`, after verifying
the next available number. The repository currently contains migrations 001–009.
Migration number and state schema version are different: 008 and 009 retain state
version 7. This proposal advances state version 7 to 8.

Keep the existing `analyst_state.sessions` table and token-hash primary key. Add:

| Column | Meaning |
|---|---|
| `transport` | `bearer` or `cookie`; prevents using a cookie session through legacy bearer authentication. |
| `last_active_at` | Server timestamp of the last accepted activity. |
| `idle_timeout_seconds` | Issued inactivity policy, bounded at 1,800 seconds. |

Keep `created_at` as issuance time and `expires_at` as immutable absolute expiry.
Backfill existing rows as `transport='bearer'`, `last_active_at=created_at` and a
bounded legacy idle value. Preserve their original `expires_at`: the migration
must not upgrade old 15-minute sessions into eight-hour sessions.

Replace the old unnamed lifetime constraint with named constraints that enforce:

```text
expires_at > created_at
bearer session: expires_at <= created_at + 15 minutes
cookie session: expires_at <= created_at + 8 hours
60 <= idle_timeout_seconds <= 1800
created_at <= last_active_at < expires_at
```

Inspect and verify the existing constraint definition before dropping it; fail on
unexpected schema drift. Retain current ownership, forced RLS, token-hash scope,
principal ownership checks and grants to the restricted state role.

The runtime gains only `UPDATE(last_active_at)` in addition to its existing
`UPDATE(revoked_at)`. It must still lack table-wide UPDATE and updates to
`owner_id`, `token_hash`, `permissions_version`, `created_at`, `expires_at`,
`transport` and `idle_timeout_seconds`. Do not add DELETE, broad grants, or a
SECURITY DEFINER renewal function.

Add `permissions_sessions.sql` as the schema-8 effective privilege audit, based on
the complete schema-7 presentation audit. Embed exactly the same audit in migration
010. Update `Database.ready()` and `verify_permissions()` to select it for schema 8
and retain the precise column-level drift checks. Include the new audit in
`services/bi_analyst/pyproject.toml` package data.

Also update the schema gates in:

- `operations/preflight.py` and `operations/qualify.py`;
- `operations/release.py`, including the reported release/schema contract;
- `operations/maintenance.py`, which currently explicitly requires schema 7;
- related deployment evidence and migration/audit tests.

The maintenance table list stays unchanged. Existing cleanup by absolute
`expires_at` remains correct, though an idle-expired session may remain stored until
absolute expiry. Earlier cleanup of idle-expired/revoked rows is optional. Expiry
checks must work without the maintenance job.

## 6. Authoritative validation and activity updates

Separate the two operations in `AuthStore`:

```text
validate_session(token, expected_transport)   -> read-only expiry/revocation check
record_activity(token, expected_transport)   -> conditional update of a live session
```

A cookie session is usable only when all of the following hold:

```text
revoked_at IS NULL
expires_at > database_now
last_active_at + idle_timeout_seconds > database_now
transport = 'cookie'
principal is enabled, company_wide and permitted by the pilot policy
principal.permissions_version = session.permissions_version
```

Keep request-level grant checks in `api.py:principal` and the checks already made
inside `Store.scoped()`. For ordinary requests, avoid redundantly loading the same
session twice, as `/auth/session` currently does. Streaming checks must bypass any
request-level identity cache because access can change after stream creation.

Implement activity under the existing token-hash RLS scope. Lock the scoped session
row, obtain database time after acquiring the lock, and recheck expiry and the
current principal before updating `last_active_at`. Keep an expiry predicate on
the UPDATE itself, too. Concurrent activity and logout must serialize on the row;
an expired or revoked session must never be revived by a late activity request.
Never accept a browser-provided activity timestamp or requested expiry.

Use the existing per-principal rate budget for authenticated activity/session
requests; do not consume the service-wide pre-authentication budget for every
activity report. Server throttling still applies even when the frontend batches
reports. A denied or failed activity request must not move the frontend's accepted
deadline.

Return stable reason codes, including:

```text
authentication_required
session_idle_expired
session_absolute_expired
session_revoked
permissions_changed
access_denied
csrf_failed
origin_denied
```

Validation may read a scoped expired row to classify the failure, but must not
return user information from it. Record safe reason codes in `request.state` for
the existing sanitised request logs; do not log credentials or CSRF tokens.

## 7. Cookie, CSRF and API contracts

Issue a fresh random 256-bit session credential and store only its SHA-256 digest,
as the current flow already does. Set:

```http
Set-Cookie: __Host-datacube_session=<opaque credential>; Path=/; Secure; HttpOnly; SameSite=Lax
```

Use a browser-session cookie without `Expires` or `Max-Age` for the initial policy.
It survives reloads. Browser session restoration may retain it after browser close,
so neither tab close nor browser close is a reliable server-side logout boundary.
The database remains authoritative. The cookie's host-only and prefix behaviour is
documented in [MDN's Set-Cookie reference](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Set-Cookie).

Keep the existing separate `__Host-datacube_oauth` nonce cookie, Monday PKCE,
five-minute attempt, single-use handoff, 60-second handoff lifetime and callback
fragment cleanup. Cookie issuance happens only after successful `/auth/exchange`.
For a successful new login, replace the browser credential and revoke the previous
cookie session held by that browser. Do not revoke other devices implicitly.

Add a small `csrf.py` helper. Require the exact configured frontend `Origin` on
cookie API fetches and a session-bound `X-CSRF-Token` on every authenticated unsafe
method, including logout and activity. One implementable token is
`HMAC-SHA256(key=raw_session_credential, message='datacube:csrf:v1')`: return the
derived value to the permitted frontend, keep it only in memory, and validate with
`hmac.compare_digest`. The raw session credential never appears in JSON. Test the
derivation and session binding; use no client-selected key or session identifier.

`/auth/exchange` is a bootstrap exception: it requires the existing exact Origin,
valid handoff and verifier instead of a session CSRF token. `/auth/login` and
`/auth/callback` are top-level navigation exceptions using their existing nonce/state
boundary. Health and operational telemetry routes retain their separate contracts.
Allow CORS preflight without a session and ensure it never changes activity.

For the recommended sibling-origin deployment, reject missing, `null`, retired or
foreign origins on cookie API fetches. Require JSON for mutation bodies. CORS should
allow the one frontend origin, `GET`/`POST`, `Content-Type` and
`X-CSRF-Token`, with `allow_credentials=True`. Keep `Authorization` during the
bounded legacy transition and for independently authenticated operational endpoints
where applicable. CORS is not a substitute for server-side Origin/CSRF checks.
See [OWASP's CSRF guidance](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html).

| Endpoint | Proposed cookie-mode contract | Counts as activity? |
|---|---|---|
| `POST /auth/exchange` | Set cookie; return session metadata and CSRF token; no bearer in JSON | Starts a new session. |
| `GET /auth/session` | Validate cookie and grant; return metadata and CSRF token | No. |
| `POST /auth/activity` | Validate cookie, Origin, CSRF, grant and both deadlines; update activity | Yes. |
| `GET /auth/logout-context` | Exact permitted Origin; return only the cookie-bound CSRF value for logout recovery, without an active-principal dependency | No. |
| `POST /auth/logout` | Revoke the cookie session; clear cookie using matching attributes; idempotent | No. |
| `/v1/*` | Cookie plus current ownership/permission checks; CSRF for unsafe methods | No automatic renewal. UI reports activity separately. |

Session metadata should contain `subject`, `permissions_version`,
`absolute_expires_at`, `idle_expires_at`, `absolute_expires_in_ms`,
`idle_expires_in_ms`, `idle_timeout_seconds`, `warning_seconds` and `csrf_token`.
Calculate durations using database time, floor remaining milliseconds, and clamp
them to the issued bounds. Return `Cache-Control: no-store` on all auth responses.

Cookie-mode credential extraction must reject ambiguous duplicate session cookies
and requests supplying both a cookie session and application bearer. Legacy bearer
validation must require `transport='bearer'`; it must never accept an eight-hour
cookie session. Do not apply browser-session rules to `/ops/metrics`' independent
telemetry credential.

Keep logout usable when the principal has been disabled or its permissions changed.
It needs credential/Origin/CSRF validation, not the normal active-principal
dependency. Clear invalid or expired cookies through server responses. For an
already missing cookie, return an idempotent sign-out result without leaking state.
Preserve the existing fail-closed auditing boundary when attaching cookie headers.

## 8. Frontend restoration, activity and warnings

Refactor `createAuth()` in `web/src/auth.mjs` to expose `restore()` and `activity()`
alongside login, finish, request and logout. All cookie-mode auth and data fetches
use `credentials: 'include'`; remove browser-generated Authorization headers.
Continue enforcing the fixed API origin and supported request paths.

On page startup:

1. Finish a pending OAuth handoff if one exists; remove its fragment first.
2. Otherwise call `/auth/session` to restore the cookie session.
3. Show a loading/checking state until that completes.
4. A 401 means sign-in is needed. A 503 or network failure means restoration is
   unavailable; preserve recoverable work and allow retry.
5. Load the workspace only after the server confirms the subject and grant.

Replace the unconditional 900-second frontend cap/timer with separate idle and
absolute deadlines derived from returned durations. As in the current code,
subtract the full request round-trip and use `performance.now()` for the local
display schedule. Do not treat the computer's wall clock as authorization proof.

Revalidate on `pageshow`, return to a visible tab and recovery after sleep/offline.
Timer suspension and platform sleep behaviour make a local timer insufficient.
Revalidation does not extend inactivity. A user interaction on return can send a
separate activity request, but it cannot revive a session the server has expired.

Report keyboard/input, pointer activation and throttled scrolling in the visible
workspace. Exclude mouse movement, passive visibility changes, polling, SSE
heartbeats, model progress, reconnects and timer ticks. Send activity promptly on
the leading interaction, then coalesce continued interaction to at most once per
minute. Send immediately for “Stay signed in” and before a deliberate submission
when a deadline is close. Do not send scheduled reports while the user is idle.

These events are a usability signal, not proof against an attacker who already
possesses the session credential. The server's fixed eight-hour cap still applies.
Only an acknowledged activity response moves the displayed idle deadline.

Add `web/src/SessionWarning.tsx` with an accessible dialog/banner and countdown:

- Idle warning: “Your session will end in two minutes because of inactivity.”
  “Stay signed in” calls `/auth/activity`.
- Absolute warning: explain that another Monday sign-in is required. Its action
  preserves the route/draft and starts login; it cannot extend the old session.
- If renewal fails, show the failure and keep the authoritative deadline.

At expiry, abort outstanding data requests using the existing controller tracking,
remove protected results from view, and show the specific sign-in message.
Distinguish session/access denial from `csrf_failed` and `origin_denied`: the current
“clear on any 403” rule would otherwise turn a request/configuration problem into a
misleading logout. Never automatically replay failed mutation requests after login.

## 9. Cross-tab behaviour and logout failures

The host-only API cookie is shared by tabs for that API host. Use `BroadcastChannel`
to notify tabs of sign-out, successful activity and identity changes. Messages may
contain status notifications, but no credentials, CSRF token or protected results.
Receiving tabs recheck the server for authoritative metadata. Avoid ping-pong
broadcasts and do not broadcast periodically when nothing changes.

If one tab logs out, clear protected UI in all tabs immediately. The backend
revocation rejects subsequent requests everywhere even if a tab missed the message.
A different-subject login must abort old requests and discard/hide the old subject's
drafts and results before rendering the new workspace.

Cookie logout failure needs special handling: JavaScript cannot delete an HttpOnly
cookie. Merely clearing React state would allow a reload to restore that cookie.
Retain a non-secret, origin-local “logout pending” marker and notify other tabs.
While the marker exists, block normal restoration and data access. Retry server
logout on reconnect; if the in-memory CSRF value was lost on reload, obtain it
privately through `/auth/logout-context` and immediately revoke, without displaying
the workspace. This narrowly scoped endpoint must also work after permission
withdrawal, return no subject/data/session deadlines, and never renew the session.
For an absent cookie it can simply confirm that no browser session remains.
Clear the
marker only after confirmed server logout, confirmed server expiry, or a deliberate
successful new sign-in that replaces/revokes the old browser session.

This marker records a local UX decision and is not a substitute for revocation.
The failure message must state that server sign-out was not confirmed; do not retain
the existing promise that the server credential expires within fifteen minutes.

## 10. Preserve work safely

Submitted conversations and runs are already durable; use the existing conversation
URL and `watchRun()` recovery. Expiring browser access should disconnect the viewer,
not mark a durable backend job cancelled just because a tab lost authentication.
Workers must retain their existing principal/version checks and finite workflow
budgets.

Persist only unsent question/reply text and the minimum recovery metadata in
tab-local `sessionStorage`, keyed by verified subject plus conversation ID. Use
`Workspace.tsx` input handlers so the latest text is saved before an auth-triggered
unmount. Bound stored size and lifetime, and discard malformed/stale entries.
Restore only after a confirmed same-subject sign-in. Clear drafts on confirmed
submission and explicit logout; hide/remove another subject's drafts on an account
change. Document that drafts are browser-resident company text; do not persist query
results, exports, provider credentials or session secrets.

The workspace already tracks uncertain submission/resume outcomes and idempotency
keys. Retain that state when preserving an unconfirmed operation. After sign-in,
check saved run/conversation state first. If the user retries, preserve the existing
idempotency key; a new key could duplicate a question the server already accepted.
Do not equate “unsent draft” with “request failed after sending”.

## 11. SSE and long-running requests

`api.py:events` already revalidates the session inside the streaming loop. Preserve
that check, applying both deadlines without recording activity. The request's
cookie credential remains available during the stream; check it against live
database state rather than assuming the initial handshake stays valid.

Keep the existing stream heartbeat and reconnection logic in `web/src/api.ts`.
At expiry/revocation, the server terminates access and the next authenticated state
request supplies the specific reason. No more protected events should be released
after a failed validation; an already in-flight response cannot be recalled.
Multi-tab activity may extend the same session's idle deadline, but must not move
its absolute deadline.

Periodic token rotation is deliberately outside the first implementation. If later
introduced, bind streams to a stable revocable session record and use a narrowly
bounded credential handover mechanism. Otherwise rotating the cookie would strand
streams still validating the old request credential.

## 12. Monday identity and offboarding

Keep `MondayOAuth.identify()`'s current identity lookup and revocation of both
provider access and refresh tokens. A DataCube activity request must not call Monday
or retain its credentials. No new provider-token storage is needed for the proposed
cookie session.

At the eight-hour cap, run a new Monday authorization flow, repeat eligibility and
DataCube grant checks, and issue a new DataCube session. An existing Monday SSO
session may make that navigation quick. The current OAuth flow does not establish
that a password/MFA challenge occurred recently; do not describe it as guaranteed
fresh authentication. A requirement for fresh MFA needs separately verified provider
capabilities or an identity provider with the required controls.

Longer local sessions increase the delay for a Monday-only user disablement. The
current code checks Monday eligibility only at login. For the initial rollout,
DataCube principal revocation must be part of the offboarding process: disable the
principal, increment its permission version and revoke its sessions using the
existing administrator procedure. That gives immediate denial on subsequent
requests/stream checks.

Record the remaining Monday-only revocation window explicitly: up to eight hours
from session issuance, with the existing pending-handoff window also considered.
If this is unacceptable, implement a separately verified identity-status sync or
supported event integration before enabling the longer policy. Do not claim the
current webhook/ETL flow automatically revokes Analyst sessions or silently reuse
ETL/provider credentials for that purpose.

## 13. Verification required for implementation

Extend the existing restricted-PostgreSQL auth tests, Node auth tests and Playwright
workspace tests. This is authentication behaviour, so meaningful integration tests
are required when code is implemented.

| Area | Required evidence |
|---|---|
| Migration | Existing bearer expiries unchanged; state version 8; matching embedded/package audit; no source-table privilege changes. |
| Privileges | Runtime can touch only scoped activity/revocation fields; cannot change absolute expiry, owner, credential or policy; reader cannot read auth state. |
| Expiry | Idle and absolute expiry enforced by PostgreSQL; activity moves idle only; expired/revoked sessions cannot be revived. |
| Concurrency | Activity versus logout/expiry; duplicate activity; multiple tabs; state consistent across restart and replicas. |
| Cookie | Secure/HttpOnly/host-only flags; no credential in JSON; reload works; cookie session cannot be used as legacy bearer. |
| CSRF/CORS | Deny foreign, null/missing and retired origins; missing/wrong/session-mismatched token; JSON form bypass; valid credentialed preflight. |
| OAuth | Keep nonce/state binding, PKCE, handoff single use, fragment cleanup, provider rejection/revocation and two-user isolation. |
| Activity | Visible user interaction extends idle; polling, restore, hidden tab, stream heartbeats and background model work do not. |
| UX | Warning actions work; absolute warning requires login; clock skew, sleep/wake, failed restore and offline recovery handled. |
| Logout | Server revocation; cookie deletion; cross-tab clearing; permission-withdrawn logout; logout-context exposes no user data; pending logout survives reload without restoring data. |
| Drafts | Same-subject recovery, cross-user isolation, explicit logout cleanup, uncertain requests retain their idempotency keys. |
| Streams | Active stream stops at idle/absolute expiry or principal revocation; reconnect succeeds only with a valid session. |
| Operations | Schema-8 readiness, qualification/release metadata, packaged audit and maintenance preview/apply work. |

Use fake browser timers and administrator-controlled timestamps in the disposable
database rather than waiting eight hours in tests. Include an actual HTTPS browser
test with sibling test hosts/certificates for cookie/CORS behaviour; mocked fetches
alone cannot demonstrate browser cookie acceptance. Keep existing synthetic OAuth
fixtures, then perform a real pilot sign-in through the production domain path.

Implementation checks, using the repository's existing tooling:

```powershell
# Repository root; use the established disposable loopback test database.
& .\report.venv\Scripts\python.exe -m pytest services/bi_analyst/tests -q -p no:cacheprovider --basetemp outputs/pytest_session_management

# From web/:
npm test
npm run build
npm run test:browser

# Repository root:
git diff --check
```

These are future verification steps, not results claimed for this documentation.

## 14. Delivery and rollback sequence

1. Implement a compatibility backend that understands schemas 7 and 8. Keep bearer
   issuance capped at 900 seconds and do not query new columns on schema 7. Complete
   the new auth/CSRF logic and tests before changing production policy.
2. Configure and verify the custom frontend/API hosts, TLS and Monday callback using
   the existing services. Verify the legacy sign-in path on the new origins first.
3. Deploy the compatibility backend before migration 010. Its readiness/audit
   selection must recognize both schemas so the migration does not strand the API.
4. Apply migration 010 as the existing administrator, transactionally, against the
   explicitly selected production database. Run the new permission/readiness checks.
5. Coordinate the cookie-aware frontend deployment with Render's transport/policy
   switch. A short sign-in cutover is acceptable; do not leave a bearer-only frontend
   receiving a cookie-only exchange response. Require a fresh sign-in for the new
   policy; leave existing bearer sessions at their original expiry.
6. During the bounded transition, accept existing bearer sessions only when their
   row transport is bearer and original fifteen-minute lifetime is still valid.
   Stop issuing them at cookie cutover and remove legacy acceptance after that window.
7. Verify approved sign-in, page reload, session warning, inactivity, logout,
   cross-tab identity change, streaming and deprovisioning with the pilot. Review
   sanitised failure reasons before expanding usage.

Rollback to the compatibility backend/frontend and explicit bearer/900-second
settings, not to an older schema-7-only binary. Stop cookie acceptance and revoke
cookie sessions in the administrator procedure. Preserve schema 8 and audit
history; do not downgrade the database or rerun historical migrations. Old binaries
can fail readiness on schema 8 and do not understand the transport distinction.
Include the maintenance executable in the compatible rollback set.

## 15. Implementation work packages

| Work package | Deliverable |
|---|---|
| Routing and identity policy | Verified domain/callback configuration; explicit offboarding/revocation policy. |
| Backend and schema | Migration 010, schema-8 audit, settings, cookie/CSRF/session/activity APIs, operations compatibility. |
| Frontend session lifecycle | Restore, typed deadlines/errors, activity batching, warnings, cross-tab notifications and pending logout. |
| Workspace recovery | Subject-scoped drafts and uncertain-operation recovery using current idempotency semantics. |
| Acceptance and cutover | Restricted-role regression evidence, real HTTPS browser checks, pilot rollout and tested compatible rollback. |

The acceptance outcome is concrete: an active user can work beyond fifteen minutes;
reload preserves a valid login; unattended sessions expire after thirty minutes;
activity never extends the eight-hour cap; access withdrawal remains enforceable;
and re-signing in returns the same user to recoverable work without duplicate runs.
