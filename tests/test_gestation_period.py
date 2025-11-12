# scripts_new/test_gestation_period.py
import os
import sys
import argparse
import json
from pathlib import Path
from typing import Dict, List, Optional

# Ensure project root is on sys.path so we can import src.*
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.core.monday_client import MondayClient
from src.config import HIDDEN_ITEMS_BOARD_ID, HIDDEN_ITEMS_COLUMNS

def _extract_date_value(item: Dict, column_id: str) -> Optional[str]:
    # Matches current logic in src/core/data_processor.py
    column_values = item.get('column_values', [])
    for col in column_values:
        if col.get('id') == column_id:
            text = col.get('text')
            if text and text.strip():
                return text.strip()
    return None

def _calculate_gestation_period(design_dates: List[str], invoice_dates: List[str]) -> Optional[int]:
    # Matches current logic in src/core/data_processor.py
    if not design_dates:
        return None
    if not invoice_dates:
        return 0
    try:
        from datetime import datetime
        earliest_design = min(design_dates)
        earliest_invoice = min(invoice_dates)
        design_dt = datetime.strptime(earliest_design, '%Y-%m-%d')
        invoice_dt = datetime.strptime(earliest_invoice, '%Y-%m-%d')
        days_diff = (invoice_dt - design_dt).days
        if days_diff > 500000 or days_diff < 0:
            return 0
        return days_diff
    except (ValueError, TypeError):
        return None

def fetch_all_hidden_items(client: MondayClient, column_ids: List[str]) -> List[Dict]:
    items: List[Dict] = []
    cursor = None
    while True:
        page = client.get_items_page(
            board_id=HIDDEN_ITEMS_BOARD_ID,
            column_ids=column_ids,
            cursor=cursor
        )
        batch = page.get("items", []) or []
        items.extend(batch)
        cursor = page.get("next_cursor")
        if not cursor or not batch:
            break
    return items

def main():
    parser = argparse.ArgumentParser(description="Compute gestation period for a parent item by monday_id")
    parser.add_argument("-i", "--monday-id", required=True, help="Parent item monday_id")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    args = parser.parse_args()

    parent_id = str(args.monday_id)

    # Ensure API key present (monday client will also check)
    if not os.getenv("MONDAY_API_KEY"):
        print("ERROR: MONDAY_API_KEY not set in environment (e.g., .env).")
        sys.exit(1)

    client = MondayClient()

    # 1) Fetch subitems for the given parent
    subitems = client.get_subitems_for_parents(
        parent_ids=[parent_id],
        column_ids=[]  # no subitem columns needed; names are returned by default
    )

    # 2) Fetch hidden items with the two date columns used in current logic
    design_col = HIDDEN_ITEMS_COLUMNS['date_design_completed']  # 'date__1'
    invoice_col = HIDDEN_ITEMS_COLUMNS['invoice_date']          # 'date42__1'
    hidden_items = fetch_all_hidden_items(client, [design_col, invoice_col])

    # 3) Build lookup for hidden items by name (matches current logic)
    hidden_by_name: Dict[str, Dict] = {}
    for hi in hidden_items:
        name = hi.get('name', '') or ''
        hidden_by_name[name] = hi

    # 4) Collect dates based on subitem names
    design_dates: List[str] = []
    invoice_dates: List[str] = []

    for s in subitems:
        subitem_name = s.get('name', '') or ''
        hidden_item = hidden_by_name.get(subitem_name)
        if not hidden_item:
            continue
        d = _extract_date_value(hidden_item, design_col)
        if d:
            design_dates.append(d)
        inv = _extract_date_value(hidden_item, invoice_col)
        if inv:
            invoice_dates.append(inv)

    # 5) Compute gestation period using current logic
    gp = _calculate_gestation_period(design_dates, invoice_dates)

    payload = {
        "parent_monday_id": parent_id,
        "subitems_count": len(subitems),
        "design_dates": sorted(design_dates) if design_dates else [],
        "invoice_dates": sorted(invoice_dates) if invoice_dates else [],
        "gestation_period_days": gp
    }

    if args.verbose:
        print(json.dumps(payload, indent=2))
    else:
        print(gp if gp is not None else "None")

if __name__ == "__main__":
    main()
