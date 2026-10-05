"""Read-only evidence capture for two staged lifecycle deferrals; no database."""
import json
from pathlib import Path
import sys
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
from scripts import monday_review_cleanup as cleanup
from src.services import monday_lifecycle_activity as activity

load_dotenv(ROOT / '.env')
out = ROOT / 'outputs/monday_lifecycle/review31_diagnostics_20261005'
out.mkdir(parents=True, exist_ok=True)
monday = cleanup.CleanupMondayClient()
try:
    for item_id in ('2125557809', '2686607939'):
        row = next(r for r in cleanup.targets() if r['item_id'] == item_id)
        request = cleanup.recovery_request(row)
        before = activity.current_membership(monday, request)
        until = datetime.now(timezone.utc).isoformat()
        history = activity.read_history(monday, request, until)
        after = activity.current_membership(monday, request)
        record = dict(request=request, history=history, before=before, after=after)
        (out / (item_id + '.json')).write_text(json.dumps(record, indent=2), encoding='utf-8')
        try:
            activity.require_deletion(request, history)
            reason = None
        except ValueError as exc:
            reason = str(exc)
        print(json.dumps({'item_id':item_id, 'reason':reason, 'metadata_equal':before == after}), flush=True)
finally:
    monday.session.close()
