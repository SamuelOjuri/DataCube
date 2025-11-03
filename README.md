# DataCube Operations

## Webhook Listener (FastAPI) Setup

1. Duplicate `env.example` to `.env` (or ensure the variables are present in your environment) and provide:
   - `MONDAY_WEBHOOK_URL` pointing to `https://<host>/webhooks/monday`
   - `WEBHOOK_SECRET` matching the signing secret configured in your Monday integration
   - Supabase credentials (`SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `SUPABASE_KEY`)
2. Install dependencies inside the existing virtualenv or via `pip install -r requirements.txt`.
3. Launch the listener:

   ```bash
   uvicorn src.webhooks.webhook_server:app --host 0.0.0.0 --port ${WEBHOOK_PORT:-8000}
   ```

4. When Monday issues the webhook `challenge`, the server echoes it (per [Monday webhook verification](https://developer.monday.com/api-reference/reference/webhooks)). Ensure ports/firewalls allow the inbound request.
5. Review logs for `processed_with_warnings` entries—these indicate the webhook executed but the downstream analysis raised an exception. Details are written to the `webhook_events` table.

## Data Quality Checks

Run `python scripts/data_quality_checks.py` to see mirroring coverage and numeric null counts before relying on webhook-triggered analysis. The script requires Supabase service credentials in your environment.

