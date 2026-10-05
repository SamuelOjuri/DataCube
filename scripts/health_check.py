"""Independent worker watchdog. Exit 0 healthy, 1 alerts, 2 check/configuration failure."""
import argparse
import json
import os
import time
from dotenv import load_dotenv
from src.services import worker_store as store
from src.services.worker_monitor import operational_status, record_alerts


def check(expected_services, record=False):
    with store.connect() as connection:
        report = operational_status(connection, expected_services=expected_services)
        if record:
            record_alerts(connection, report)
        return report


def main(argv=None):
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--expected-service', action='append', default=[])
    parser.add_argument('--watch', action='store_true', help='Run on an independent monitoring host')
    parser.add_argument('--record', action='store_true', help='Persist active/resolved alerts')
    parser.add_argument('--notify', action='store_true', help='Send to configured WORKER_ALERT_WEBHOOK_URL')
    args = parser.parse_args(argv)
    expected = args.expected_service or [s.strip() for s in os.getenv('WORKER_EXPECTED_SERVICES', 'api,webhook').split(',') if s.strip()]
    notified = {}
    while True:
        try:
            report = check(expected, args.record)
            code = 0 if report['healthy'] else 1
        except Exception as exc:
            report = {'healthy': False, 'alerts': [{'key': 'watchdog:database', 'issue': 'monitor_unavailable', 'error_type': type(exc).__name__}]}
            code = 2
        print(json.dumps(report, default=str), flush=True)
        if args.notify:
            from src.services.notification_service import send_worker_alerts
            pending = [a for a in report['alerts'] if time.monotonic() - notified.get(a['key'], float('-inf')) >= 900]
            if pending:
                try:
                    send_worker_alerts(pending)
                    notified.update({a['key']: time.monotonic() for a in pending})
                except Exception as exc:
                    print(json.dumps({'notification_error': type(exc).__name__}), flush=True)
                    code = 2
            notified = {k: v for k, v in notified.items() if k in {a['key'] for a in report['alerts']}}
        if not args.watch:
            return code
        time.sleep(30)


if __name__ == '__main__':
    raise SystemExit(main())
