"""Operator sequencing without external services."""
import json
from pathlib import Path
import shutil
import subprocess

import pytest

POWERSHELL = shutil.which('powershell.exe')
pytestmark = pytest.mark.skipif(not POWERSHELL, reason='Windows PowerShell required')
ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('step,apply_exit,verify_exit,expected,success', [
    ('Stage', 0, 0, ['stage'], True),
    ('Stage', 0, 2, ['stage'], True),
    ('Stage', 0, 1, ['stage'], False),
    ('Verify', 0, 0, ['verify'], True),
    ('ApplyAndVerify', 0, 0, ['apply', 'verify'], True),
    ('ApplyAndVerify', 2, 2, ['apply', 'verify'], True),
    ('ApplyAndVerify', 1, 2, ['apply', 'verify'], False),
])
def test_runner_always_verifies_and_reports_partial_status(tmp_path, step, apply_exit, verify_exit, expected, success):
    run = tmp_path / 'run'
    run.mkdir()
    (run / 'manifest.json').write_text(json.dumps({'workflow': 'monday-field-comparison-v1', 'run_id': 'reviewed-id'}))
    fake = tmp_path / 'fake_worker.ps1'
    fake.write_text('''
$taskMode = $args[2]
$taskRun = $args[([array]::IndexOf($args, '--run-dir') + 1)]
Add-Content -LiteralPath (Join-Path (Split-Path -Parent $taskRun) 'calls.txt') -Value $taskMode
if ($taskMode -eq 'apply') {
    if ($args -notcontains '--confirm-run-id' -or $args -notcontains 'reviewed-id') { throw 'Missing reviewed run ID' }
    $global:LASTEXITCODE = APPLY_EXIT
} else { $global:LASTEXITCODE = VERIFY_EXIT }
'''.replace('APPLY_EXIT', str(apply_exit)).replace('VERIFY_EXIT', str(verify_exit)), encoding='utf-8')
    result = subprocess.run([POWERSHELL, '-NoProfile', '-NonInteractive', '-File',
        str(ROOT / 'scripts/run_order_value_monday_compare.ps1'), '-RunDir', str(run),
        '-Step', step, '-PythonPath', str(fake)], cwd=ROOT, text=True, capture_output=True, timeout=30)
    assert (result.returncode == 0) is success, result.stdout + result.stderr
    assert (tmp_path / 'calls.txt').read_text().splitlines() == expected
    if verify_exit == 2 and step != 'Stage' and success:
        assert 'does not mean complete reconciliation' in result.stdout
