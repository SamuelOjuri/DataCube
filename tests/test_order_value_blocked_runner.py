"""No-service PowerShell orchestration checks for reviewed blocked-project runs."""
import json
from pathlib import Path
import shutil
import subprocess

import pytest

POWERSHELL = shutil.which('powershell.exe')
pytestmark = pytest.mark.skipif(not POWERSHELL, reason='Windows PowerShell required')
ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('step,allow,apply_exit,verify_exit,tamper,calls,success', [
    ('Verify', False, 0, 0, False, ['verify'], True),
    ('ApplyAndVerify', False, 0, 0, False, [], False),
    ('ApplyAndVerify', True, 0, 0, False, ['apply', 'verify'], True),
    ('ApplyAndVerify', True, 1, 2, False, ['apply', 'verify'], False),
    ('ApplyAndVerify', True, 0, 0, True, [], False),
])
def test_blocked_runner(tmp_path, step, allow, apply_exit, verify_exit, tamper, calls, success):
    run = tmp_path / 'repair-run'
    run.mkdir()
    manifest = {'run_id': 'offline-example', 'sha256': 'test-hash', 'scopes': 1,
                'deferred_scopes': 0, 'projects': 2, 'changes': 12, 'mode': 'repair'}
    summary = {'source_of_truth': 'Monday CRM', 'excluded_subitems': {'valid': True}, 'runs': {'repair': dict(manifest)}}
    if tamper: manifest['sha256'] = 'different'
    (run / 'manifest.json').write_text(json.dumps(manifest))
    (tmp_path / 'summary.json').write_text(json.dumps(summary))
    fake = tmp_path / 'fake_worker.ps1'
    fake.write_text('''
$taskMode = $args[2]
$taskRun = $args[([array]::IndexOf($args, '--run-dir') + 1)]
Add-Content -LiteralPath (Join-Path (Split-Path -Parent $taskRun) 'calls.txt') -Value $taskMode
if ($taskMode -eq 'apply') {
    if ($args -notcontains '--all-pending' -or $args -notcontains '--allow-repair-fields') { throw 'Missing flags' }
    $global:LASTEXITCODE = APPLY_EXIT
} else { $global:LASTEXITCODE = VERIFY_EXIT }
'''.replace('APPLY_EXIT', str(apply_exit)).replace('VERIFY_EXIT', str(verify_exit)), encoding='utf-8')
    command = [POWERSHELL, '-NoProfile', '-NonInteractive', '-File',
               str(ROOT / 'scripts/run_order_value_blocked_updates.ps1'),
               '-ReviewDir', str(tmp_path), '-Step', step, '-PythonPath', str(fake)]
    if allow: command.append('-AllowRepairFields')
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=30)
    assert (result.returncode == 0) is success, result.stdout + result.stderr
    path = tmp_path / 'calls.txt'
    assert (path.read_text().splitlines() if path.exists() else []) == calls
