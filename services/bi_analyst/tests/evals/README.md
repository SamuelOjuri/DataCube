# Phase 1 BI evaluation dataset

This tooling builds a versioned reference dataset in the **TEST Supabase project**
configured in the repository `.env`. It is independent of the future analyst API
and never imports DataCube's `src` package, starts workers, or contacts Monday.

The question pack contains 70 scenarios: 40 metric questions (eight per core
metric), ten two-turn follow-ups, ten ambiguous questions, and ten synthetic edge
cases. Fifty separately maintained SQL queries provide the numerical answer key.
An ambiguous question has a clarification contract, not a fabricated SQL result.
Follow-ups record reference results for both turns. Development and holdout cases
are labelled; keep the holdout questions and all answer keys out of model prompts.

## Run

Use a Python environment with `requirements.txt` from this directory. The commands
below use the repository's existing reporting environment; no dependency on the
ETL application's requirements or startup code is required.

```powershell
& .\report.venv\Scripts\python.exe services/bi_analyst/tests/evals/manage.py inspect
& .\report.venv\Scripts\python.exe -m pytest services/bi_analyst/tests/evals/test_dataset.py -q
& .\report.venv\Scripts\python.exe services/bi_analyst/tests/evals/manage.py freeze --dataset bi_eval_20261007_v1 --as-of 2026-10-07
& .\report.venv\Scripts\python.exe services/bi_analyst/tests/evals/manage.py verify --dataset bi_eval_20261007_v1
& .\report.venv\Scripts\python.exe services/bi_analyst/tests/evals/manage.py probe --dataset bi_eval_20261007_v1
& .\report.venv\Scripts\python.exe services/bi_analyst/tests/evals/manage.py report --dataset bi_eval_20261007_v1
```

`freeze` is a one-time creation command. It rejects an existing dataset or reader
role and never overwrites a prior version. For a future capture, choose a new
`bi_eval_YYYYMMDD_vN` name and that day's date. The capture day must match the
database day in Europe/London so copied date-dependent views and reference SQL
use the same date. Verification continues using the stored date indefinitely.
`probe` tests actual permission denials and mutation guards using savepoints and
zero-row UPDATE statements; it does not change business rows.

Required `.env` entries are `TEST_SUPABASE_NAME`, `TEST_SUPABASE_URL`, and
`TEST_SUPABASE_DB_URL`. Use the TEST project's direct or Session pooler connection
string. The script checks that the URL and database connection refer to the same
test project and rejects the production URL/connection. It does not fall back to
production credentials. Connection errors suppress credentials.

## Storage and access

For dataset `bi_eval_20261007_v1`:

| Location | Contents |
|---|---|
| `bi_eval_20261007_v1` schema | Frozen source tables, reporting populations, copied baseline views, fixed date context, and separate synthetic fixture tables |
| `bi_eval_20261007_v1_key.artifacts` | Manifest, source inventory, questions, independent SQL, expected typed results, diagnostics, freshness evidence, query plans, and validation |
| `bi_eval_20261007_v1_reader` role | NOLOGIN role with SELECT access to the evaluation schema; no answer-key or source-project access |
| `outputs/bi_analyst_evals/bi_eval_20261007_v1/` | Git-ignored manifest, question definitions, validation and aggregate review reports; financial answer rows remain in the restricted database schema |

Existing `public` tables are read only during creation. New snapshot tables are
created in one repeatable-read transaction. They have no dependency on mutable
source views, no copied operational triggers, and no live scheduling. An explicit
mutation-rejection trigger protects every frozen table. Table row counts and
SHA-256 fingerprints allow later integrity checks. Owner-level DDL can still
alter objects; those accounts must remain restricted.

Dataset tables have RLS enabled and a read policy for the dedicated reader.
PUBLIC, `anon`, `authenticated`, and `service_role` access is revoked on the new
schemas and their objects. The reader is a test capability, not application
authentication. The privileged preparation account can SET ROLE to run all
reference queries under that capability. Do not use the preparation credentials
in the future analyst service. Phase 3 still implements user ownership and scope.

Inspect the answer key as an authorised database administrator:

```sql
SELECT name FROM bi_eval_20261007_v1_key.artifacts ORDER BY name;
SELECT payload FROM bi_eval_20261007_v1_key.artifacts WHERE name = 'questions';
SELECT payload -> 'enquiry_last_month'
FROM bi_eval_20261007_v1_key.artifacts WHERE name = 'expected';
SELECT payload FROM bi_eval_20261007_v1_key.artifacts WHERE name = 'diagnostics';
```

Do not put these financial results in source control or send answer keys to an
agent being evaluated. The snapshot contains copied business data, not anonymised
production data. Local reports inherit workspace access controls.

## What the results establish

- Reference SQL can execute reproducibly on the copied data through the restricted
  reader and return the recorded typed results.
- Python Decimal/count calculations independently cross-check the three parent
  totals, both inclusive conversion cohorts, and both gestation means.
- Ten hand-calculated synthetic cases cover extra charges, blanks versus zero,
  signed invoices, exact enquiry reasons, aggregated conversion counts, positive
  gestation, join amplification, repeated source links, completed months and a
  zero denominator. Synthetic tables are separate from all business totals.
- Diagnostics record reporting exclusions, archive coverage, relationship gaps,
  stored-versus-child reconciliation, copied freshness evidence, and view parity.

These checks do **not** certify current Monday source values, Power BI results,
deployed Render flags, organisational permissions, currency/tax presentation,
fiscal calendar, or the future agent's conversational behaviour. Business cases
remain `pending_business_and_source_review`. Financial outputs are recorded-data
reference answers, not an assertion that unresolved company totals are correct.

## Metric details requiring careful review

1. Parent Order Value, hidden complete-input order subtotal, and bookings are
   separate measures. A missing material/charge input is counted as incomplete;
   the numeric subset is never labelled as the full hidden-board total. Typed
   blank-versus-missing source evidence still requires reconciliation.
2. Project invoice totals, child mirrors, hidden invoices, and monthly revenue
   use different grains. Negative values remain in signed base totals.
3. The restored monthly revenue view omits the parent-status/reportable-population
   filters specified by the plan. The reference pack implements the **plan**
   definition and separately preserves the deployed view and its differences.
   No source view is repaired by this tooling.
4. Conversion rates are fractions rounded to three decimal places, as in the
   existing SQL. Percent presentation multiplies that ratio by 100. Counts are
   aggregated before division. Existing cohorts have a lower date bound only;
   any future-dated records are reported rather than silently excluded.
5. Actual gestation uses the stored value. Historical averages and percentiles
   exclude nonpositive values; date-difference checks are diagnostic, not a
   replacement for the established source/fallback rules.
6. Recorded category/type strings are grouped as stored (with blank normalisation).
   Multi-value account/product attribution is not invented. Unknown currency,
   fiscal periods, entities, and current-period definitions require clarification.

An independent reviewer should approve each metric's scope, review the diagnostics,
record the source backup timestamp, and compare Power BI using the same frozen
data and filters. Corrections require a reviewed new dataset/answer-key version;
never regenerate expected results automatically to make a failing test pass.
