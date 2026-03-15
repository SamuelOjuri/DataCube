import time
from typing import Any

from src.database.supabase_client import SupabaseClient

BATCH_SIZE = 1000
OLDER_THAN_DAYS = 30
SLEEP_SECONDS = 0.2
MAX_CALLS = 500


def parse_deleted_count(raw):
    if raw is None:
        return 0
    if isinstance(raw, (int, float)):
        return int(raw)

    if isinstance(raw, list):
        if not raw:
            return 0
        first = raw[0]
        if isinstance(first, dict):
            return int(
                first.get("cleanup_old_webhook_events_batch")
                or first.get("cleanup_old_webhook_events")
                or next(iter(first.values()), 0)
                or 0
            )
        return int(first or 0)

    if isinstance(raw, dict):
        return int(
            raw.get("cleanup_old_webhook_events_batch")
            or raw.get("cleanup_old_webhook_events")
            or next(iter(raw.values()), 0)
            or 0
        )

    return int(raw)


def main() -> None:
    client = SupabaseClient().client
    total_deleted = 0

    for call_no in range(1, MAX_CALLS + 1):
        resp = client.rpc(
            "cleanup_old_webhook_events_batch",
            {
                "p_batch_size": BATCH_SIZE,
                "p_older_than_days": OLDER_THAN_DAYS,
            },
        ).execute()

        deleted = parse_deleted_count(resp.data)
        total_deleted += deleted

        print(
            f"call={call_no} deleted={deleted} total_deleted={total_deleted} raw={resp.data!r}"
        )

        if deleted == 0:
            print("Done: no more eligible rows.")
            break

        time.sleep(SLEEP_SECONDS)
    else:
        print(f"Stopped at MAX_CALLS={MAX_CALLS}. Run again if needed.")


if __name__ == "__main__":
    main()