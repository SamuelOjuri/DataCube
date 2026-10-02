"""Exercise the PowerShell orchestration using a fake worker, never live services."""
import json
from pathlib import Path
import shutil
import subprocess

import pytest


POWERSHELL = shutil.which('powershell.exe')
pytestmark = pytest.mark.skipif(not POWERSHELL, reason='PowerShell runner is Windows-specific')
ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('step,stage_exit,apply_exit,verify_exit,bad_selection,expected_calls,success', [
    ('All', 0, 0, 0, False, ['stage', 'apply', 'verify'], True),
    ('All', 1, 0, 0, False, ['stage'], False),
    ('All', 2, 0, 0, False, ['stage'], False),
    ('All', 0, 0, 0, True, ['stage'], False),
    ('All', 0, 2, 2, False, ['stage', 'apply', 'verify'], False),
    ('All', 0, 1, 0, False, ['stage', 'apply', 'verify'], True),
    ('Stage', 0, 0, 0, False, ['stage'], True),
    ('ApplyAndVerify', 0, 0, 0, False, ['apply', 'verify'], True),
    ('Verify', 0, 0, 0, False, ['verify'], True),
])
def test_runner_gates_apply_and_always_verifies_attempted_apply(
        tmp_path, step, stage_exit, apply_exit, verify_exit, bad_selection, expected_calls, success):
    run_dir = tmp_path / 'run'
    run_dir.mkdir()
    ids = sorted(line for line in (ROOT / 'docs/order-value-recovery-48-projects.txt').read_text().splitlines()
                 if line and not line.startswith('#'))
    if bad_selection:
        ids[-1] = '999'
    manifest = {'workflow': 'scoped-reads-v1', 'mode': 'orders', 'monday_source_refreshed': True,
                'deferred_scopes': 0, 'selected_projects': 48, 'projects': 48, 'scopes': 2,
                'changes': 502, 'run_id': 'fake-run-for-offline-test'}
    plan = {'deferred': [], 'scopes': [{'scope': {'projects': ids[:25]}}, {'scope': {'projects': ids[25:]}}]}
    (run_dir / 'manifest.json').write_text(json.dumps(manifest))
    (run_dir / 'scopes.json').write_text(json.dumps(plan))
    fake = tmp_path / 'fake_python.ps1'
    fake.write_text('''
$taskRunIndex = [array]::IndexOf($args, '--run-dir')
$taskRunPath = $args[$taskRunIndex + 1]
if ($args[1] -eq 'scripts.order_value_scopes_refresh') { $taskMode = 'stage' }
else { $taskMode = $args[2] }
Add-Content -LiteralPath (Join-Path (Split-Path -Parent $taskRunPath) 'calls.txt') -Value $taskMode
switch ($taskMode) {
    'stage' { $global:LASTEXITCODE = STAGE_EXIT }
    'apply' {
        if ($args -notcontains '--all-pending' -or $args -notcontains '--allow-partial') { throw 'Missing apply flags' }
        $global:LASTEXITCODE = APPLY_EXIT
    }
    'verify' { $global:LASTEXITCODE = VERIFY_EXIT }
    default { throw 'Unexpected worker invocation' }
}
'''.replace('STAGE_EXIT', str(stage_exit)).replace('APPLY_EXIT', str(apply_exit))
       .replace('VERIFY_EXIT', str(verify_exit)), encoding='utf-8')
    result = subprocess.run([POWERSHELL, '-NoProfile', '-NonInteractive', '-File',
                             str(ROOT / 'scripts/run_order_value_recovery_48.ps1'),
                             '-RunDir', str(run_dir), '-Step', step, '-PythonPath', str(fake)],
                            cwd=ROOT, capture_output=True, text=True, timeout=30)
    assert (result.returncode == 0) is success, result.stdout + result.stderr
    assert (tmp_path / 'calls.txt').read_text().splitlines() == expected_calls
