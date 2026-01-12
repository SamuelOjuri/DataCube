---
name: Webhook Analysis Automation Plan
overview: ""
todos:
  - id: b9e38a9d-3d6a-489a-ba97-8a370d90e843
    content: Confirm mirrors and numeric fields feeding AnalysisService are populated via sync + Supabase checks.
    status: pending
  - id: a2b8d285-ae42-463e-b2d8-d65023d6cb0b
    content: Adjust webhook handler column triggers and logging for reliable AnalysisService execution.
    status: pending
  - id: 0afae6cb-2650-49a1-b428-f19fe95f05df
    content: Configure env vars and deploy FastAPI webhook server with challenge/HMAC validation.
    status: pending
  - id: 53fb9095-0ecd-4a4d-af45-9c8a80d55025
    content: Use integration token to create Monday webhooks and verify challenge + logging.
    status: pending
  - id: 770bf883-0802-4f17-a3e0-25be2b6909c4
    content: Execute sample board events and confirm analysis writes to Supabase and Monday mirrors.
    status: pending
---

# Webhook Analysis Automation Plan

## 1. Validate Data Foundations

- Inspect `src/database/sync_service.py` + `scripts/rehydrate_sync.py` to ensure mirror fields (`mirror_12__1`, `mirror875__1`, `formula_mkqa31kh`) populate `subitems.account`, `subitems.product_type`, `subitems.new_enquiry_value`.
- Run lightweight Supabase queries (see `app_data/implementation_update.md`) to confirm no null mirrors or missing numeric casts that would undermine scoring inputs.

## 2. Harden Analysis Trigger Logic

- Review `src/webhooks/webhook_server.py` handlers to confirm watched columns cover all fields feeding `AnalysisService` (consider adding `probability`/value-band mirrors if analysis requires them).
- Add defensive logging + error handling so `AnalysisService().analyze_and_store` failures surface in `webhook_events`.

## 3. Deploy and Secure Webhook Listener

- Configure environment variables (`MONDAY_WEBHOOK_URL`, `WEBHOOK_SECRET`, Supabase keys) and start FastAPI app (`src/webhooks/webhook_server.py`).
- Verify HMAC/JWT signature handling; ensure challenge responses meet Monday spec ([monday.com webhooks docs](https://developer.monday.com/api-reference/reference/webhooks)).

## 4. Provision Monday Webhooks via API

- Use integration OAuth token with `scripts/setup_webhooks.py` to register `create_item`, `change_column_value`, `delete_item` for boards `1825117125`, `1825117144`, `1825138260`.
- Confirm Monday sends `challenge` POST; log acceptance in webhook server; validate entries in `webhook_events`.

## 5. End-to-End Verification

- Trigger sample board updates; observe FastAPI background processing + retries, Supabase upserts, and new rows in `analysis_results`.
- Cross-check Monday numeric mirror columns (`PARENT_COLUMN_*` in `src/config.py`) if syncing scores back to the board.

## 6. Optional Monday Workflow Hook

- If UI-triggered reruns are desired, follow `monday-api-mcp_create_workflow_instructions` to publish a board-hosted workflow, using remote options to configure trigger/action blocks; have FastAPI watch the resulting flag column.

## Implementation Todos

- validate-mirror-data: Confirm mirrors + numeric fields are populated for analysis inputs.
- review-webhook-handlers: Ensure column triggers + error logging in `webhook_server.py` support analysis.
- deploy-secure-webhook: Configure environment + run FastAPI listener with proper challenge handling.
- provision-board-webhooks: Register Monday webhooks using integration token and verify challenge handshake.
- run-end-to-end-test: Simulate board updates and confirm analysis results propagate.