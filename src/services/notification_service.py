"""Optional alert delivery, invoked only by health_check --notify."""
import os
from urllib.parse import urlsplit
import requests


def send_worker_alerts(alerts):
    url = os.getenv('WORKER_ALERT_WEBHOOK_URL', '')
    parsed = urlsplit(url)
    if parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError('Configure an HTTPS WORKER_ALERT_WEBHOOK_URL without user information')
    text = 'DataCube worker alerts: ' + '; '.join(a['issue'] + ' [' + a['key'] + ']' for a in alerts)
    response = requests.post(url, json={'text': text}, timeout=5, allow_redirects=False)
    if not 200 <= response.status_code < 300:
        raise RuntimeError('Alert delivery rejected')
