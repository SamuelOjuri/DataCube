"""Backfill project dates and gestation from stored subitems.

Uses the same rules as the sync (src/services/project_rollups.py):
first/last design date, first/last invoice date and earliest order date come
from each project's live subitems (children recorded as archived or deleted
are ignored), a stored value is never replaced by a missing one, and gestation
is derived from the resulting dates. Archived or deleted projects are skipped.
Projects whose first invoice is dated before their first design are listed for
a date check in Monday; their gestation is left blank. No Monday API calls are made.

Usage (from the repository root):
    python -m scripts.backfill_project_derived_dates                 # read-only preview
    python -m scripts.backfill_project_derived_dates --report out.csv
    python -m scripts.backfill_project_derived_dates --apply         # asks for confirmation

Options:
    --project-id ID   limit to one project (repeatable)
    --batch-size N    projects per transaction when applying (default 500)
    --yes             skip the confirmation prompt when applying
"""
import argparse
import csv
import os
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402
from psycopg.conninfo import conninfo_to_dict  # noqa: E402

from src.services import monday_lifecycle as life  # noqa: E402
from src.services import project_rollups as rollups  # noqa: E402

load_dotenv(ROOT / ".env")


def target_label():
    info = conninfo_to_dict(os.environ.get("SUPABASE_DB_URL", ""))
    return f"{info.get('host', '?')}:{info.get('port', '5432')}/{info.get('dbname', '?')} as {info.get('user', '?')}"


def candidate_ids(connection, only):
    if only:
        return sorted(set(only))
    rows = connection.execute("SELECT DISTINCT parent_monday_id AS monday_id FROM public.subitems "
                              "WHERE parent_monday_id IS NOT NULL").fetchall()
    return sorted(r["monday_id"] for r in rows)


def read_preview(connection, ids):
    with connection.transaction():
        connection.execute("SET TRANSACTION READ ONLY")
        connection.execute("SET LOCAL statement_timeout='120s'")
        return rollups.preview(connection, ids)


def summarise(rows):
    filled, corrected = Counter(), Counter()
    for row in rows:
        for field in rollups.DERIVED_FIELDS:
            before, after = row[f"before_{field}"], row[f"after_{field}"]
            if before == after:
                continue
            (filled if before is None else corrected)[field] += 1
    print(f"Projects that would change: {len(rows)}")
    print(f"  {'field':<22}{'filled (was empty)':>20}{'corrected':>12}")
    for field in rollups.DERIVED_FIELDS:
        print(f"  {field:<22}{filled[field]:>20}{corrected[field]:>12}")


def write_report(rows, path):
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["monday_id", "field", "before", "after"])
        for row in rows:
            for field in rollups.DERIVED_FIELDS:
                before, after = row[f"before_{field}"], row[f"after_{field}"]
                if before != after:
                    writer.writerow([row["monday_id"], field, before, after])
    print(f"Wrote per-field changes to {path}")


def report_invoice_before_design(connection, only):
    with connection.transaction():
        connection.execute("SET TRANSACTION READ ONLY")
        rows = rollups.invoice_before_design(connection, only or None)
    print(f"Projects whose first invoice is dated before their first design: {len(rows)}"
          + (" (gestation is left blank; check these dates in Monday)" if rows else ""))
    for row in rows:
        print(f"  {row['item_name']} ({row['monday_id']}): first design {row['first_date_designed']}, "
              f"first invoice {row['first_date_invoiced']}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="write the changes (default is a read-only preview)")
    parser.add_argument("--yes", action="store_true", help="skip the confirmation prompt when applying")
    parser.add_argument("--project-id", action="append", default=[], help="limit to this project ID (repeatable)")
    parser.add_argument("--batch-size", type=int, default=rollups.BATCH_SIZE)
    parser.add_argument("--report", help="write a CSV of before/after values for every change")
    args = parser.parse_args(argv)
    if not os.environ.get("SUPABASE_DB_URL"):
        print("SUPABASE_DB_URL is not set (checked the environment and .env).", file=sys.stderr)
        return 2
    if args.batch_size < 1:
        parser.error("--batch-size must be at least 1")

    print(f"Target database: {target_label()}")
    with life.connect() as connection:
        ids = candidate_ids(connection, args.project_id)
        print(f"Projects with stored subitems to check: {len(ids)}")
        rows = read_preview(connection, ids)
        summarise(rows)
        for row in rows[:5]:
            changes = {f: (str(row[f"before_{f}"]), str(row[f"after_{f}"])) for f in rollups.DERIVED_FIELDS
                       if row[f"before_{f}"] != row[f"after_{f}"]}
            print(f"  e.g. {row['monday_id']}: {changes}")
        if args.report:
            write_report(rows, args.report)
        if not args.apply:
            report_invoice_before_design(connection, args.project_id)
            print("Read-only preview: nothing was written. Re-run with --apply to write these changes.")
            return 0
        if not rows:
            report_invoice_before_design(connection, args.project_id)
            print("Nothing to apply.")
            return 0
        if not args.yes:
            answer = input(f"Write these changes to {target_label()}? Type 'yes' to continue: ")
            if answer.strip().lower() != "yes":
                print("Cancelled; nothing was written.")
                return 1

        pending = [row["monday_id"] for row in rows]
        changed = 0
        for number, batch in enumerate(rollups.batches(pending, args.batch_size), start=1):
            changed += len(rollups.apply(connection, batch, batch_size=args.batch_size))
            print(f"  batch {number}: {changed}/{len(pending)} projects updated", flush=True)
        remaining = read_preview(connection, pending)
        print(f"Applied: {changed} projects updated. Still differing after the run: {len(remaining)}"
              + (" (live sync may have changed subitems meanwhile; re-run to pick them up)" if remaining else ""))
        report_invoice_before_design(connection, args.project_id)
        return 0


if __name__ == "__main__":
    sys.exit(main())
