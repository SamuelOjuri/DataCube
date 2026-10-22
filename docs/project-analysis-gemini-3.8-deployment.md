# Project analysis: Gemini 3.8 Flash deployment

This release changes the project analysis default to `gemini-3.8-flash`. Deploy
the code, root `requirements.txt`, and environment settings together. This is the
DataCube analysis API/webhook/worker service, not the separate `services/bi_analyst`
application. The BI analyst's model settings and dependency lock are unchanged.
No database migration is required.

Conversion probability and gestation remain numeric baseline calculations. The
LLM supplies explanations and can adjust the final rating by -5 to +5 points,
clamped to 1–100. Normal downstream automation can publish successful results to
Monday. Failed LLM requests now return an explicit failure and leave the previous
stored analysis intact; they no longer store fallback text as successful reasoning.
Explicit numeric-only requests retain their existing behavior.

## 1. Prepare the release

Record the previous release artifact/revision, dependency lock or installed package
versions, and environment configuration in your normal secret/configuration store.
Retain a database backup or export of the affected latest analyses before any
historical rerun. Do not print credentials into deployment logs.

Build a fresh release environment using the same Python version as production
(the repository's pinned NumPy requires Python 3.11 or later). From the repository
root, with that environment activated:

```text
python -m pip install -r requirements.txt
python -m pip check
python -m pytest tests/test_project_llm_provider.py tests/test_analysis_service.py tests/test_numeric_analyzer.py -q
```

The Google SDK is pinned to `google-genai==1.56.0`, which supports the required
thinking-level settings. Its authentication dependency is pinned to
`google-auth==2.45.0`. Test in the production build environment as well as locally;
do not upgrade packages in a running process.

Local verification on Python 3.11 completed with 58 passing tests and a clean
`pip check`. These tests use a mock HTTP transport and do not call Gemini,
Supabase, or Monday.
Avoid running the entire legacy `tests/` directory against production credentials:
some historical files exercise live integrations or perform work on import.

## 2. Set the project analysis environment

Configure the following in the host's environment/secret manager (or the root
`.env` for local development). Keep the existing `GEMINI_API_KEY` secret and verify
that its Google project has Gemini 3.8 access, billing, and sufficient quotas.

```dotenv
GEMINI_MODEL=gemini-3.8-flash
GEMINI_THINKING_LEVEL=low
GEMINI_MAX_OUTPUT_TOKENS=4096
GEMINI_TIMEOUT_SECONDS=90
```

Check for an existing `GEMINI_MODEL` override in every deployment. Exported process
variables take precedence over values loaded from `.env`. Do not put these in
`services/bi_analyst/.env`; the predictive analysis service uses the root settings.
Neither `BI_ANALYST_GEMINI_API_KEY` nor `OPENAI_MODEL` selects this model.

The output cap covers thinking plus final JSON. Low thinking is the starting
configuration for this short explanation task. If qualification encounters
`model_output_budget`, inspect token usage and evaluate a higher cap before
deploying it. `minimal` is not supported. Sampling overrides such as temperature
are omitted as required by the [Gemini 3.8 migration guide](https://ai.google.dev/gemini-api/docs/generate-content/whats-new-gemini-3.8).

Transient transport errors and HTTP 408/429/500/502/503/504 receive at most two
additional attempts, with 1- and 2-second backoff. Truncated responses also have
at most two additional attempts within the same shared attempt limit. The SDK's
nested retries are disabled. The timeout is per HTTP attempt, not a total project
deadline: allow roughly 273 seconds plus overhead per project at these defaults.
Ensure host shutdown/drain and request timeouts account for this; prefer the
existing durable workers for long-running work.

Optional cost estimates require both prices:

```dotenv
GEMINI_INPUT_USD_PER_MILLION=0.75
GEMINI_OUTPUT_USD_PER_MILLION=3.75
```

These are Google's published introductory standard text rates through
31 December 2026. The published rates from 1 January 2027 are $1.50 input and
$7.50 output per million tokens. Verify the applicable rates and billing tier
at deployment using [Google's pricing guidance](https://ai.google.dev/gemini-api/docs/generate-content/whats-new-gemini-3.8#pricing).
Change the configured rates when changing models or billing tiers. Estimates use
reported input, output, and thinking tokens across all returned attempts. They
exclude unreported provider usage and are not a billing reconciliation. Without
both prices, estimated cost is `null`, not zero. Comparison overrides to a different
model also leave cost unconfigured to avoid applying the wrong model's rates.

## 3. Run a synthetic production-credential check before starting writers

In a one-off instance of the new release, with production's model credentials and
settings but without starting the API, worker, or scheduler, run:

```text
python -m scripts.check_project_analysis_llm
```

This makes paid API calls using three synthetic projects: a supported segment,
a sparse segment, and a rating near the upper bound. It does not import database
clients, fetch real customer data, store analyses, or send Monday updates. It
prints JSON and exits 1 if initialization, an API request, parsing, or a numeric
invariant fails. Each case has the retry limit described above.

When the old model remains accessible, an optional comparison sends identical
inputs to both models:

```text
python -m scripts.check_project_analysis_llm --compare-model gemini-2.5-flash
```

Require all cases to pass. Inspect the written reasoning manually: figures must
match the supplied baselines, sparse samples must receive suitable caveats, and
pipeline-stage labels must not be used as evidence. Review rating adjustments,
token counts, attempts, and latency against your operational budget. A passing
synthetic check verifies access and contracts; it does not establish model quality
on the full project population. Complete a representative staging evaluation
before bulk production reruns. No live benchmark was run as part of this code change.

## 4. Roll out and observe

Drain existing analysis work using the host's normal procedure, then replace and
restart every process that imports `src.config` or creates an `AnalysisService`:
the analysis API, webhook processes, durable queue workers, and scheduled jobs.
Model settings are read at import and an analyzer may be cached for the process
lifetime. Ensure old and new workers are not consuming work concurrently during
the transition; retain queued events and jobs for replay.

Start with a small monitored cohort through the existing workflow. Confirm:

- Logs show `LLM Analyzer initialized with model: gemini-3.8-flash` and successful
  completions with token counts and attempts.
- Successful stored rows carry `llm_model=gemini-3.8-flash` and
  `analysis_version=v1.1-llm`.
- No sustained timeout, rate-limit, invalid-output, output-budget, or configuration
  errors occur. Investigate before increasing throughput.
- Successful ratings and narratives appear correctly in Monday after the ordinary
  downstream sync. Failed analysis jobs do not enqueue fallback text for publishing.

Read-only verification, using your normal authorized database console:

```sql
SELECT project_id, analysis_timestamp, llm_model, analysis_version,
       expected_conversion_rate, expected_gestation_days, rating_score,
       processing_time_ms
FROM public.analysis_results
ORDER BY analysis_timestamp DESC
LIMIT 20;
```

`analyze_and_store` returns `llm_metadata` containing the attempted model, token
breakdown, attempt count, timing, and any stable error code. The analysis HTTP
route returns 503 for model failures and 404 for a missing project. Logs use stable
failure codes rather than provider response bodies. Per-call usage metadata is
returned and logged; it is not added to the database schema.

## 5. Historical analyses

The change applies to future executions. Existing results are retained, and the
standard backfill skips projects with existing reasoning without checking the
model version. Do not force a full historical rerun as part of deployment.

If historical regeneration is required, define and review its scope, cost, and
Monday publication consequences first. The existing backfill supports a bounded
rerun, for example:

```text
python -m scripts.backfill_analysis_with_llm --cutoff 2026-10-01 --limit 10 --force --throttle 1
```

That example writes analyses for up to ten projects created on/after the cutoff;
it is not an exact-ID selection. A scheduled Monday sync can subsequently publish
them. Use a staging database first, select a suitable cutoff/limit for your cohort,
and coordinate the publication window. Never substitute
`backfill_llm_analysis_for_monday` for the read-only smoke check: that script writes
both stored analyses and Monday updates.

## 6. Rollback

Stop new analysis consumption and drain in-flight work. Restore the previous
release artifact and its dependency/environment configuration, then restart all
analysis processes. Verify the old model is still accessible before relying on it
as a fallback. Merely changing `.env` does not update cached analyzers, and the new
thinking-level configuration must not be sent to Gemini 2.5.

Run the appropriate synthetic access check for the selected release/model before
resuming consumption. Preserve queued jobs; failures under the new release leave
prior stored results intact. Successful Gemini 3.8 rows and any completed Monday
updates remain after code rollback. If those outputs must also be reverted, use
the reviewed pre-rollout backup or a targeted reanalysis and controlled resync;
do not delete analysis history or run an unrestricted forced backfill.
