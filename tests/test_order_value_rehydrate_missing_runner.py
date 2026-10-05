"""PowerShell acknowledgement, failure reporting and mandatory verification."""
import json
from pathlib import Path
import shutil
import subprocess

import pytest

POWERSHELL = shutil.which('powershell.exe')
pytestmark = pytest.mark.skipif(not POWERSHELL, reason='Windows PowerShell required')
ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('step,allow,apply_exit,verify_exit,workflow,expected,success', [
    ('Stage', False, 0, 0, 'missing-subitems-insert-only-v1', ['stage'], True),
    ('Stage', False, 0, 1, 'missing-subitems-insert-only-v1', ['stage'], False),
    ('Verify', False, 0, 0, 'missing-subitems-insert-only-v1', ['verify'], True),
    ('ApplyAndVerify', False, 0, 0, 'missing-subitems-insert-only-v1', [], False),
    ('ApplyAndVerify', True, 0, 0, 'missing-subitems-insert-only-v1', ['apply', 'verify'], True),
    ('ApplyAndVerify', True, 1, 2, 'missing-subitems-insert-only-v1', ['apply', 'verify'], False),
    ('ApplyAndVerify', True, 2, 2, 'missing-subitems-insert-only-v1', ['apply', 'verify'], False),
    ('ApplyAndVerify', True, 0, 2, 'missing-subitems-insert-only-v1', ['apply', 'verify'], False),
    ('ApplyAndVerify', True, 0, 0, 'exact-link-rehydration-v1', [], False),
])
def test_runner(tmp_path, step, allow, apply_exit, verify_exit, workflow, expected, success):
    run = tmp_path / 'run'
    run.mkdir()
    (run/'manifest.json').write_text(json.dumps({'workflow': workflow, 'run_id': 'offline-example'}))
    fake = tmp_path/'fake_worker.ps1'
    fake.write_text('''
$taskMode = $args[2]
$taskRun = $args[([array]::IndexOf($args, '--run-dir') + 1)]
Add-Content -LiteralPath (Join-Path (Split-Path -Parent $taskRun) 'calls.txt') -Value $taskMode
if ($taskMode -eq 'stage' -and $args -notcontains '--targets-file') { throw 'Missing exact targets' }
if ($taskMode -eq 'apply') {
    if ($args -notcontains '--allow-rehydration' -or $args -notcontains 'offline-example') { throw 'Missing acknowledgement' }
    $global:LASTEXITCODE = APPLY_EXIT
} else { $global:LASTEXITCODE = VERIFY_EXIT }
'''.replace('APPLY_EXIT', str(apply_exit)).replace('VERIFY_EXIT', str(verify_exit)), encoding='utf-8')
    command = [POWERSHELL, '-NoProfile', '-NonInteractive', '-File',
               str(ROOT/'scripts/run_order_value_missing_rehydration.ps1'), '-RunDir', str(run),
               '-Step', step, '-PythonPath', str(fake)]
    if allow: command.append('-AllowRehydration')
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=30)
    assert (result.returncode == 0) is success, result.stdout + result.stderr
    calls = tmp_path/'calls.txt'
    assert (calls.read_text().splitlines() if calls.exists() else []) == expected
