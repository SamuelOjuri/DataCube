import asyncio
import logging
import sys
import time
from pathlib import Path
from datetime import date, timedelta
import math

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.services.analysis_service import AnalysisService
from src.database.supabase_client import SupabaseClient

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s', datefmt='%H:%M:%S')

svc = AnalysisService()
db = SupabaseClient()

PAGE = 1000
cutoff = (date.today() - timedelta(days=5*365)).isoformat()  # ~5y
ok = 0
errs = 0
start = 0
start_time = time.time()

# Get first page and total
res = db.client.table('projects')\
    .select('monday_id', count='exact')\
    .gte('date_created', cutoff)\
    .order('monday_id')\
    .range(0, PAGE - 1)\
    .execute()
total = res.count or 0
pages_total = math.ceil(total / PAGE) if total else 0
rows = res.data or []

logging.info(f"Backfill start | cutoff={cutoff} | total={total} | pages={pages_total}")

failed_ids = []

def log_progress(processed: int):
    elapsed = max(1e-6, time.time() - start_time)
    rate = processed / elapsed
    remain = max(0, total - processed)
    eta = int(remain / rate) if rate > 0 else 0
    pct = (processed / total * 100) if total else 100.0
    logging.info(f"processed {processed}/{total} ({pct:.1f}%) | ok={ok} err={errs} | {rate:.1f}/s | ETA {eta//60}m{eta%60:02d}s")

# Process first page
for r in rows:
    try:
        if svc.analyze_and_store(r['monday_id']).get('success'):
            ok += 1
        else:
            errs += 1
            failed_ids.append(str(r['monday_id']))
    except Exception:
        errs += 1
        failed_ids.append(str(r['monday_id']))
processed = min(PAGE, total)
log_progress(processed)

# Remaining pages
start = PAGE
page_idx = 1
while start < total:
    res = db.client.table('projects')\
        .select('monday_id')\
        .gte('date_created', cutoff)\
        .order('monday_id')\
        .range(start, min(start + PAGE - 1, total - 1))\
        .execute()
    rows = res.data or []
    if not rows:
        break
    for r in rows:
        try:
            if svc.analyze_and_store(r['monday_id']).get('success'):
                ok += 1
            else:
                errs += 1
                failed_ids.append(str(r['monday_id']))
        except Exception:
            errs += 1
            failed_ids.append(str(r['monday_id']))
    page_idx += 1
    processed = min(start + len(rows), total)
    log_progress(processed)
    start += PAGE

print(f"Backfilled {ok}/{total} projects (errors={errs})")
if failed_ids:
    print(f"Failed IDs ({len(failed_ids)}): {', '.join(failed_ids[:50])}" + (" ..." if len(failed_ids) > 50 else ""))