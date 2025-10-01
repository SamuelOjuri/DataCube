### What you have
- Initial sync into Supabase is working. Tables `projects`, `subitems`, `hidden_items` are populated.
- Schema includes `analysis_results` and analytics views to support predictions.

### Immediate data QA (5–10 minutes)
- **Check mirrors/empties**
  - Subitems are missing `account`/`product_type` despite being mirrors; confirm counts:
```sql
-- Missing mirrors on subitems
SELECT COUNT(*) FROM subitems WHERE COALESCE(account, '') = '' OR COALESCE(product_type, '') = '';

-- Projects with zero/NULL gestation (should be >0 in many historical cases)
SELECT COUNT(*) FROM projects WHERE COALESCE(gestation_period, 0) = 0;

-- Verify conversion view spans data
SELECT * FROM conversion_metrics LIMIT 5;
```
- **Type sanity for numerics**
```sql
-- Ensure numeric casts are safe
SELECT
  COUNT(*) FILTER (WHERE new_enquiry_value IS NULL) AS null_new_enq,
  COUNT(*) FILTER (WHERE quote_amount IS NULL) AS null_quote
FROM projects p
LEFT JOIN subitems s ON p.monday_id = s.parent_monday_id;
```

### Fix mirror resolution gaps (subitems)
Your sample shows empty `subitems.account`/`product_type`. Ensure we hydrate these from mirrors before transform. Minimal edit: when transforming subitems, fall back to Monday mirror display values if normalized fields are missing.

Add a safe extractor in `src/database/sync_service.py` and use it in `_transform_for_subitems_table`:
```python
def _get_display(item: dict, col_id: str) -> str:
    # item['column_values'] from Monday has entries with id == col_id
    for cv in item.get('column_values', []):
        if cv.get('id') == col_id:
            return (cv.get('display_value') or cv.get('text') or '').strip()
    return ''

def _transform_for_subitems_table(self, subitems: List[Dict]) -> List[Dict]:
    transformed = []
    for item in subitems:
        try:
            account = item.get('account') or _get_display(item, 'mirror_12__1')
            product_type = item.get('product_type') or _get_display(item, 'mirror875__1')
            new_enq_text = item.get('new_enquiry_value') or _get_display(item, 'formula_mkqa31kh')
            subitem = {
                'monday_id': item.get('id'),
                'parent_monday_id': item.get('parent_id'),
                'item_name': item.get('name', ''),
                'account': account,
                'product_type': product_type,
                'new_enquiry_value': self._parse_numeric_value(new_enq_text),
                'last_synced_at': datetime.now().isoformat()
            }
            transformed.append({k: v for k, v in subitem.items() if v is not None})
        except Exception:
            continue
    return transformed
```

Then run a targeted rehydrate for subitems to backfill those fields.

### Implement the analysis pipeline (numeric baseline now)
- **Goal**: For each project, compute
  - expected_gestation_days (from similar historical projects)
  - expected_conversion_rate (win_rate)
  - rating_score (1–10; matches your DB constraint)
  - store to `analysis_results` with light reasoning JSON

Use the existing `conversion_metrics` materialized view and simple fallbacks.

Create `src/services/analysis_service.py`:
```python
from typing import Dict, Any
from datetime import datetime
from ..database.supabase_client import SupabaseClient

class AnalysisService:
    def __init__(self):
        self.db = SupabaseClient()

    def _cluster_key(self, p: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'account': p.get('account') or None,
            'type': p.get('type') or None,
            'category': p.get('category') or None,
            'product_type': p.get('product_type') or None
        }

    def _get_cluster_metrics(self, key: Dict[str, Any]) -> Dict[str, Any]:
        # Query materialized view; loosen filters progressively if empty
        q = self.db.client.table('conversion_metrics').select('*')
        for f in ['account','type','category','product_type']:
            v = key.get(f)
            if v: q = q.eq(f, v)
        res = q.limit(1).execute().data
        if res: return res[0]
        # fallback: relax to type+category
        q = self.db.client.table('conversion_metrics').select('*')
        for f in ['type','category']:
            v = key.get(f)
            if v: q = q.eq(f, v)
        res = q.limit(1).execute().data
        return res[0] if res else {}

    def analyze_project(self, project: Dict[str, Any]) -> Dict[str, Any]:
        key = self._cluster_key(project)
        cm = self._get_cluster_metrics(key)
        win_rate = float(cm.get('win_rate') or 0.25)          # 0..1
        median_gestation = int(cm.get('median_gestation') or 90)

        # Normalize factors
        gest_norm = max(0.0, min(1.0, (median_gestation / 365.0)))
        value = float(project.get('new_enquiry_value') or 0.0)
        value_norm = max(0.0, min(1.0, value / 100000.0))     # simple cap at 100k

        # Score blend (tune later): win rate drives most
        raw_score = 0.7*win_rate + 0.2*(1.0-gest_norm) + 0.1*value_norm
        rating_1_10 = max(1, min(10, round(1 + 9 * raw_score)))

        return {
            'expected_gestation_days': median_gestation,
            'gestation_confidence': 0.7 if cm else 0.4,
            'expected_conversion_rate': round(win_rate, 3),
            'conversion_confidence': 0.7 if cm else 0.4,
            'rating_score': rating_1_10,
            'reasoning': {
                'cluster': key,
                'metrics_used': {
                    'win_rate': win_rate,
                    'median_gestation': median_gestation,
                    'value_norm': value_norm
                }
            },
            'llm_model': 'numeric-baseline',
            'analysis_version': 'v0.1',
            'processing_time_ms': 0
        }

    def analyze_and_store(self, monday_id: str) -> Dict[str, Any]:
        proj = self.db.client.table('projects').select('*').eq('monday_id', monday_id).single().execute().data
        if not proj:
            return {'success': False, 'error': 'project not found'}
        result = self.analyze_project(proj)
        self.db.store_analysis_result(monday_id, result)
        return {'success': True, 'result': result}
```

Optional: layer an LLM explanation later using `src/core/llm_analyzer.py` to turn the numeric reasoning into narrative.

### Add an API endpoint to request or refresh analysis
Create `src/api/routes/analysis.py`:
```python
from fastapi import APIRouter, HTTPException
from ...services.analysis_service import AnalysisService

router = APIRouter(prefix="/analysis", tags=["analysis"])
svc = AnalysisService()

@router.post("/{monday_id}/run")
def run_analysis(monday_id: str):
    out = svc.analyze_and_store(monday_id)
    if not out.get('success'):
        raise HTTPException(status_code=404, detail=out.get('error'))
    return out
```
Wire it in `src/api/app.py`.

### Backfill analyses and schedule
- **One-off backfill** for all recent projects:
```python
# scripts/backfill_analysis.py
from src.services.analysis_service import AnalysisService
from src.database.supabase_client import SupabaseClient

svc = AnalysisService()
db = SupabaseClient()
rows = db.client.table('projects').select('monday_id').gte('date_created', '2023-01-01').execute().data
for r in rows:
    try: svc.analyze_and_store(r['monday_id'])
    except Exception: pass
print(f"Backfilled {len(rows)} projects")
```
- **Schedule**: run nightly plus after each sync:
```
0 2 * * * python scripts_new/backfill_analysis.py
```

### Trigger analysis on relevant updates (webhooks)
In `src/webhooks/webhook_server.py`, after minimal update for project fields that affect analysis (e.g., `status4__1`, `dropdown__1`, `dropdown7__1`, `lookup_mkq010zk`, `lookup_mkt3xgz1`, `lookup_mkqanpbe`), enqueue a background analysis:
```python
from ..services.analysis_service import AnalysisService
analysis_svc = AnalysisService()

# inside handle_column_changed_minimal, after DB update succeeds:
if board_id == "1825117125" and column_id in {"status4__1","dropdown__1","dropdown7__1","lookup_mkq010zk","lookup_mkt3xgz1","lookup_mkqanpbe"}:
    try: analysis_svc.analyze_and_store(item_id)
    except Exception: pass
```

### Refresh analytics objects
- Run the SQL in `src/database/schema/schema.sql` (functions/views) if not already deployed.
- Periodically refresh the materialized view:
```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY conversion_metrics;
```
Cron example:
```
*/30 * * * * psql "$SUPABASE_CONN" -c "REFRESH MATERIALIZED VIEW CONCURRENTLY conversion_metrics;"
```

### Monitoring
- Expose a quick health/report endpoint that includes recent `analysis_results` rows count and `webhook_events` processing status using the patterns in `implementation_guide.md`.

### Order of execution
1) Patch subitems mirror transform and rehydrate subitems.  
2) Deploy analysis service and API route; run a small backfill (e.g., last 90 days).  
3) Hook webhook-triggered analysis.  
4) Schedule periodic conversion_metrics refresh and analysis backfill.  
5) Add dashboards for sync health and analysis coverage.

