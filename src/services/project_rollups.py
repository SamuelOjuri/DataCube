"""Project dates and gestation derived from stored subitems.

In archive mode the parent's own date mirrors are blank through the Monday API,
and archive handling no longer builds project values from stored children. This
step is the single writer of these fields in archive mode. It runs after the
subitems are saved and:

- ignores children recorded as archived or deleted (unverified children count,
  as they hold most of the history);
- skips projects recorded as archived or deleted, whose rows are frozen;
- never replaces a stored value with a missing one: a field only changes when a
  live child supplies a value;
- derives gestation from the resulting dates: designed but not invoiced is 0,
  otherwise the days from first design to first invoice. Projects without a
  design date keep their stored gestation.
"""
from psycopg.rows import dict_row

from . import monday_lifecycle as life

DERIVED_FIELDS = ('first_date_designed', 'last_date_designed', 'first_date_invoiced',
                  'last_date_invoiced', 'date_order_received', 'gestation_period')
BATCH_SIZE = 500

_DERIVED = """
WITH children AS (
    SELECT s.parent_monday_id AS monday_id,
           min(s.date_design_completed) AS first_date_designed,
           max(s.date_design_completed) AS last_date_designed,
           min(s.invoice_date) AS first_date_invoiced,
           max(s.invoice_date) AS last_date_invoiced,
           min(s.date_order_received) AS date_order_received
    FROM public.subitems s
    LEFT JOIN public.monday_item_lifecycle l ON l.table_name = 'subitems' AND l.monday_id = s.monday_id
    WHERE s.parent_monday_id = ANY(%(ids)s)
      AND COALESCE(l.monday_state, 'active') = 'active' AND NOT COALESCE(l.blocked, false)
    GROUP BY s.parent_monday_id
),
dates AS (
    SELECT p.monday_id, p.gestation_period AS stored_gestation,
           COALESCE(c.first_date_designed, p.first_date_designed) AS first_date_designed,
           COALESCE(c.last_date_designed, p.last_date_designed) AS last_date_designed,
           COALESCE(c.first_date_invoiced, p.first_date_invoiced) AS first_date_invoiced,
           COALESCE(c.last_date_invoiced, p.last_date_invoiced) AS last_date_invoiced,
           COALESCE(c.date_order_received, p.date_order_received) AS date_order_received
    FROM children c
    JOIN public.projects p ON p.monday_id = c.monday_id
    LEFT JOIN public.monday_item_lifecycle pl ON pl.table_name = 'projects' AND pl.monday_id = p.monday_id
    WHERE COALESCE(pl.monday_state, 'active') = 'active' AND NOT COALESCE(pl.blocked, false)
),
derived AS (
    SELECT monday_id, first_date_designed, last_date_designed, first_date_invoiced,
           last_date_invoiced, date_order_received,
           CASE WHEN first_date_designed IS NULL THEN stored_gestation
                WHEN first_date_invoiced IS NULL THEN 0
                ELSE GREATEST(first_date_invoiced - first_date_designed, 0) END AS gestation_period
    FROM dates
)
"""
_CHANGED = (f"({', '.join('p.' + f for f in DERIVED_FIELDS)}) IS DISTINCT FROM "
            f"({', '.join('d.' + f for f in DERIVED_FIELDS)})")
UPDATE_SQL = (_DERIVED + "UPDATE public.projects p SET "
              + ', '.join(f'{f} = d.{f}' for f in DERIVED_FIELDS)
              + f" FROM derived d WHERE p.monday_id = d.monday_id AND {_CHANGED} RETURNING p.monday_id")
PREVIEW_SQL = (_DERIVED + "SELECT p.monday_id, "
               + ', '.join(f'p.{f} AS before_{f}, d.{f} AS after_{f}' for f in DERIVED_FIELDS)
               + f" FROM derived d JOIN public.projects p ON p.monday_id = d.monday_id WHERE {_CHANGED}"
               " ORDER BY p.monday_id")


def batches(project_ids, size=BATCH_SIZE):
    ids = sorted({str(i).strip() for i in project_ids if str(i or '').strip()})
    for start in range(0, len(ids), size):
        yield ids[start:start + size]


def preview(connection, project_ids):
    """Rows whose derived values differ from the stored ones: before_* and after_* per field."""
    rows = []
    for batch in batches(project_ids):
        with connection.cursor(row_factory=dict_row) as cursor:
            rows.extend(cursor.execute(PREVIEW_SQL, {'ids': batch}).fetchall())
    return rows


def apply(connection, project_ids, *, batch_size=BATCH_SIZE):
    """Write derived values, one short transaction per batch; returns the changed project IDs."""
    changed = []
    for batch in batches(project_ids, batch_size):
        def write(batch=batch):
            with connection.transaction():
                connection.execute("SET LOCAL lock_timeout='750ms'")
                connection.execute("SET LOCAL statement_timeout='30s'")
                with connection.cursor(row_factory=dict_row) as cursor:
                    return [r['monday_id'] for r in cursor.execute(UPDATE_SQL, {'ids': batch}).fetchall()]
        changed.extend(life.retry_lock_conflicts(connection, write))
    return changed


def refresh_projects(project_ids):
    """Sync entry point with its own connection; returns the number of projects changed."""
    with life.connect() as connection:
        return len(apply(connection, project_ids))
