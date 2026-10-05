[CmdletBinding()]
param(
    [ValidateSet('Stage', 'ApplyAndVerify', 'Verify')]
    [string]$Step = 'Stage',
    [Parameter(Mandatory = $true)]
    [string]$RunDir,
    [string]$Report = 'outputs/order_value_backfill/blocked_review_20261002/projects.csv',
    [string]$PythonPath
)

$ErrorActionPreference = 'Stop'
$taskRoot = Split-Path -Parent $PSScriptRoot
$taskPython = if ($PythonPath) { $PythonPath } else { Join-Path $taskRoot 'report.venv/Scripts/python.exe' }
if (-not (Test-Path -LiteralPath $taskPython)) { throw "Python not found: $taskPython" }
Push-Location -LiteralPath $taskRoot
try {
    if ($Step -eq 'Stage') {
        & $taskPython -m scripts.order_value_monday_compare stage --report $Report --run-dir $RunDir
        $stageExit = $LASTEXITCODE
        if ($stageExit -notin @(0, 2)) {
            throw 'Read-only staging failed; this Stage command made no Supabase changes. Preserve any output, do not apply, and retry Stage in a new run directory.'
        }
        if ($stageExit -eq 2) {
            Write-Warning 'Comparison completed with unresolved fields or deferred scopes. See unresolved.csv and comparison.json.'
        }
        Write-Host "Review $RunDir/manifest.json, changes.csv, projects.csv and unresolved.csv before ApplyAndVerify."
    }
    else {
        $manifest = Get-Content -LiteralPath (Join-Path $RunDir 'manifest.json') -Raw | ConvertFrom-Json
        if ($manifest.workflow -ne 'monday-field-comparison-v1') { throw 'Wrong workflow for this runner.' }
        $applyExit = 0
        $verifyExit = 1
        if ($Step -eq 'ApplyAndVerify') {
            try {
                & $taskPython -m scripts.order_value_monday_compare apply --run-dir $RunDir --confirm-run-id $manifest.run_id
                $applyExit = $LASTEXITCODE
            }
            finally {
                & $taskPython -m scripts.order_value_monday_compare verify --run-dir $RunDir
                $verifyExit = $LASTEXITCODE
            }
        }
        else {
            & $taskPython -m scripts.order_value_monday_compare verify --run-dir $RunDir
            $verifyExit = $LASTEXITCODE
        }
        Write-Host "Apply exit: $applyExit; verify exit: $verifyExit"
        if ($applyExit -notin @(0, 2) -or $verifyExit -notin @(0, 2)) {
            throw 'Run stopped. Earlier scopes may have committed; preserve this run and inspect its receipts.'
        }
        if ($applyExit -eq 2 -or $verifyExit -eq 2) {
            Write-Warning 'Run has unresolved work. Inspect the latest verify-*.json: staged_changes_successful, counts and remaining_uncommitted. Exit 2 does not mean complete reconciliation.'
        }
        else {
            Write-Host 'Mapped comparison fields verified. This does not certify every Monday or Supabase column.'
        }
    }
}
finally { Pop-Location }
