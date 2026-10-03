"""PowerShell sequencing without live service calls."""
import json
from pathlib import Path
import shutil
import subprocess

import pytest

POWERSHELL = shutil.which('powershell.exe')
pytestmark = pytest.mark.skipif(not POWERSHELL, reason='Windows PowerShell required')
ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('step,allow,apply_exit,verify_exit,deferred,expected,success', [
    ('Stage', False, 0, 0, 0, ['stage'], True),
    ('Verify', False, 0, 0, 0, ['verify'], True),
    ('ApplyAndVerify', False, 0, 0, 0, [], False),
    ('ApplyAndVerify', True, 0, 0, 0, ['apply', 'verify'], True),
    ('ApplyAndVerify', True, 1, 2, 0, ['apply', 'verify'], False),
    ('ApplyAndVerify', True, 0, 0, 1, [], False),
])
def test_runner(tmp_path, step, allow, apply_exit, verify_exit, deferred, expected, success):
    run = tmp_path / 'run'
    run.mkdir()
    (run / 'manifest.json').write_text(json.dumps({'workflow': 'exact-link-rehydration-v1',
        'run_id': 'offline-example', 'scopes': 1, 'deferred_scopes': deferred}))
    fake = tmp_path / 'fake_worker.ps1'
    fake.write_text('''
$taskMode = $args[2]
$taskRun = $args[([array]::IndexOf($args, '--run-dir') + 1)]
Add-Content -LiteralPath (Join-Path (Split-Path -Parent $taskRun) 'calls.txt') -Value $taskMode
if ($taskMode -eq 'apply') {
    if ($args -notcontains '--allow-rehydration' -or $args -notcontains 'offline-example') { throw 'Missing acknowledgement' }
    $global:LASTEXITCODE = APPLY_EXIT
} else { $global:LASTEXITCODE = VERIFY_EXIT }
'''.replace('APPLY_EXIT', str(apply_exit)).replace('VERIFY_EXIT', str(verify_exit)), encoding='utf-8')
    command = [POWERSHELL, '-NoProfile', '-NonInteractive', '-File',
               str(ROOT / 'scripts/run_order_value_rehydration.ps1'), '-RunDir', str(run),
               '-Step', step, '-PythonPath', str(fake)]
    if allow: command.append('-AllowRehydration')
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=30)
    assert (result.returncode == 0) is success, result.stdout + result.stderr
    calls = tmp_path / 'calls.txt'
    assert (calls.read_text().splitlines() if calls.exists() else []) == expected
