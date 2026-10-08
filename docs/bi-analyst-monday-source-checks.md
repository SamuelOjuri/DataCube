# Read-only Monday source checks

Implemented in analyst package **0.4.1**, following the owner's 8 October 2026
request to let the analyst consult Monday when a source value is in doubt,
including `metric_not_certified` cases.

The existing Phase 1 acceptance now enables the exact accepted catalogue at
runtime. `semantic/acceptance.json` binds that decision to catalogue 1.1.0's
serialized fingerprint. The sealed `catalogue.json` is unchanged. API discovery
shows effective `certification: owner_accepted` alongside the original
`recorded_certification: pending_phase1`; numerical results record their
`certification_basis`. Changed definitions do not inherit that fingerprint.
The default business timezone is the already approved `Europe/London`.

For source uncertainty, the callable `SourceCheckService.check` and authenticated
`POST /v1/runs/{run_id}/source-check` provide live observations for exact item IDs.
They work for an owned registered or completed run, including when metric
execution is blocked. An actual `metric_not_certified` response now includes a
`source_check` capability with the concrete URL, configuration availability and
allowed boards/columns. `GET /v1/metrics` also exposes this capability. The caller
must identify the relevant records; an aggregate question alone is not used to
guess IDs or scan the CRM. Phase 5 can call this tool when interpreting doubts.

Example request (replace the example item ID with a known hidden-item ID):

```json
{
  "metric_id": "order_parent_value",
  "metric_version": "1.0.0",
  "population": "reportable",
  "reason": "source_discrepancy",
  "board": "hidden_items",
  "item_ids": ["1234567890"],
  "column_ids": ["formula_mkncjq9", "numbers98__1", "numbers3__1"]
}
```

Other reasons are `metric_not_certified`, `missing_source` and
`freshness_unknown`. The response identifies the requested scope, live observation
time, API version, query fingerprint, metric acceptance status, column definitions
and source values. Missing items/columns and unavailable formula displays are
explicitly reported. Zero, blank and decimal text remain distinct. The tool does
not complete the run, replace its stored result or persist a new metric result.

The source reader uses two packaged queries: first account/item-board metadata,
then the permitted column definitions and values. There is no caller-supplied
GraphQL, URL, credential, mutation or automatic recursive link traversal. Limits
are 20 exact item IDs, five columns, two concurrent lookups per process, a
10-second total network deadline and 256 KiB per provider response by default.
It does not retry provider errors or follow redirects. Request rate limits,
audit, run ownership, permission versions and cancellation remain enforced;
database connections are released while waiting on Monday.

The board allowlist is the app's projects (`1825117125`), subitems (`1825117144`)
and hidden items (`1825138260`). Financial/date/stage column allowlists are
packaged in `monday_source.py`. Board membership and account identity are checked
on each read. Formula and mirror display strings remain evidence, not numbers to
sum. Mirror links outside these boards are omitted and flagged. CRM text is
labelled untrusted. No cleanup or writes to Monday are introduced.

Configure the deployed analyst service with:

```dotenv
BI_ANALYST_MONDAY_READ_TOKEN=<dedicated server-side source-read token>
BI_ANALYST_MONDAY_ACCOUNT_ID=<approved Monday account ID>
BI_ANALYST_MONDAY_READ_API_VERSION=2026-07
```

The token should have `boards:read` and `me:read`, with access to the approved
boards. It is separate from the sign-in app's identity-only OAuth tokens and is
never loaded implicitly from ETL's `MONDAY_API_KEY` or the repository `.env`.
The read capability becomes available when this token and account ID are
configured. Without them it returns `monday_source_not_configured`; accepted
database metric execution does not depend on a Monday credential.

Provider permission failures, malformed/partial GraphQL responses, version
mismatches and oversized responses yield sanitized errors. Live evidence covers
only the requested IDs: it cannot establish a historical population total,
database freshness or certification of changed metric definitions. These are
evidence limits, not conditions for reopening the closed Phase 1 gate.

API implementation references: Monday's [item queries](https://developer.monday.com/api-reference/reference/items),
[mirror values](https://developer.monday.com/api-reference/reference/mirror),
[formula displays and limitations](https://developer.monday.com/api-reference/reference/formula)
and [version headers](https://developer.monday.com/api-reference/docs/api-versioning).
The API version is pinned to `2026-07`; no current-version alias is used.

Validation on 8 October 2026: **320 analyst tests passed**, including real
restricted PostgreSQL logins, accepted runtime execution, uncertified-metric
source checks, cross-user denial, mutation/input rejection, account/board checks,
redirect refusal, provider errors, precision preservation, cancellation and
response/time/concurrency limits. The final metadata/provider-shape changes passed
**91 focused regression tests**. Monday HTTPS responses were mocked; no live
CRM request or hosted deployment was performed.
