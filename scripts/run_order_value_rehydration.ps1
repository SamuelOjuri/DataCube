[CmdletBinding()]
param(
    [ValidateSet('Stage', 'ApplyAndVerify', 'Verify')]
    [string]$Step = 'Stage',
    [Parameter(Mandatory = $true)]
    [string]$RunDir,
    [string]$ProjectsFile = 'scripts/order_value_rehydrate_projects_27.txt',
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
        & $taskPython -m scripts.order_value_rehydrate stage --projects-file $ProjectsFile --run-dir $RunDir
        if ($LASTEXITCODE -ne 0) { throw 'Staging is incomplete. Review deferred scopes; do not apply this run.' }
        Write-Host "Review $RunDir/manifest.json and $RunDir/changes.csv before ApplyAndVerify."
    }
    else {
        $manifest = Get-Content -LiteralPath (Join-Path $RunDir 'manifest.json') -Raw | ConvertFrom-Json
        if ($manifest.workflow -ne 'exact-link-rehydration-v1') { throw 'Wrong workflow for this runner.' }
        if ($manifest.scopes -lt 1) { throw 'No staged scopes.' }
        if ($Step -eq 'ApplyAndVerify') {
            if (-not $AllowRehydration) {
                throw 'Use -AllowRehydration after reviewing inserts, repair fields, partial commits and brief table write locks.'
            }
            if ($manifest.deferred_scopes -gt 0) { throw 'Resolve staging deferrals before applying this run.' }
            $applyExit = 1
            $verifyExit = 1
            try {
                & $taskPython -m scripts.order_value_rehydrate apply --run-dir $RunDir --confirm-run-id $manifest.run_id --allow-rehydration
                $applyExit = $LASTEXITCODE
            }
            finally {
                & $taskPython -m scripts.order_value_rehydrate verify --run-dir $RunDir
                $verifyExit = $LASTEXITCODE
                Write-Host "Rehydration: apply exit $applyExit; verification exit $verifyExit"
            }
            if ($applyExit -ne 0 -or $verifyExit -ne 0) {
                throw 'Run incomplete. Preserve this directory and inspect the receipts before retrying or restaging.'
            }
        }
        else {
            & $taskPython -m scripts.order_value_rehydrate verify --run-dir $RunDir
            if ($LASTEXITCODE -ne 0) { throw 'Verification incomplete. Review the saved report.' }
        }
        Write-Host 'All selected rehydration scopes passed verification.'
    }
}
finally { Pop-Location }
