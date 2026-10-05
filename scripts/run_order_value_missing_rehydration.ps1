[CmdletBinding()]
param(
    [ValidateSet('Stage', 'ApplyAndVerify', 'Verify')]
    [string]$Step = 'Stage',
    [Parameter(Mandatory = $true)]
    [string]$RunDir,
    [string]$TargetsFile = 'scripts/order_value_missing_subitems_2.json',
    [string]$PythonPath,
    [switch]$AllowRehydration
)

$ErrorActionPreference = 'Stop'
$taskRoot = Split-Path -Parent $PSScriptRoot
$taskPython = if ($PythonPath) { $PythonPath } else { Join-Path $taskRoot 'report.venv/Scripts/python.exe' }
if (-not (Test-Path -LiteralPath $taskPython)) { throw "Python not found: $taskPython" }
Push-Location -LiteralPath $taskRoot
try {
    if ($Step -eq 'Stage') {
        & $taskPython -m scripts.order_value_rehydrate_missing stage --targets-file $TargetsFile --run-dir $RunDir
        if ($LASTEXITCODE -ne 0) { throw 'Read-only staging failed. No Supabase changes were made. Do not apply this run.' }
        Write-Host "Review $RunDir/manifest.json, changes.csv and inserts.json before ApplyAndVerify."
    }
    else {
        $manifest = Get-Content -LiteralPath (Join-Path $RunDir 'manifest.json') -Raw | ConvertFrom-Json
        if ($manifest.workflow -ne 'missing-subitems-insert-only-v1') { throw 'Wrong workflow for this runner.' }
        $applyExit = 0
        $verifyExit = 1
        if ($Step -eq 'ApplyAndVerify') {
            if (-not $AllowRehydration) { throw 'Use -AllowRehydration after reviewing the missing-row insert plan.' }
            try {
                & $taskPython -m scripts.order_value_rehydrate_missing apply --run-dir $RunDir --confirm-run-id $manifest.run_id --allow-rehydration
                $applyExit = $LASTEXITCODE
            }
            finally {
                & $taskPython -m scripts.order_value_rehydrate_missing verify --run-dir $RunDir
                $verifyExit = $LASTEXITCODE
            }
        }
        else {
            & $taskPython -m scripts.order_value_rehydrate_missing verify --run-dir $RunDir
            $verifyExit = $LASTEXITCODE
        }
        Write-Host "Apply exit: $applyExit; verify exit: $verifyExit"
        if ($applyExit -ne 0 -or $verifyExit -ne 0) {
            throw 'Insertion run needs review. Preserve receipts and inspect the latest apply/verify summaries before retrying.'
        }
        Write-Host 'Selected subitems verified. Existing project totals were preserved.'
    }
}
finally { Pop-Location }
