[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$ReviewDir,
    [ValidateSet('ApplyAndVerify', 'Verify')]
    [string]$Step = 'Verify',
    [switch]$AllowRepairFields,
    [string]$PythonPath = '.\report.venv\Scripts\python.exe'
)

$ErrorActionPreference = 'Stop'
Push-Location -LiteralPath (Split-Path -Parent $PSScriptRoot)
try {
    $taskSummary = Get-Content -LiteralPath (Join-Path $ReviewDir 'summary.json') -Raw | ConvertFrom-Json
    if ($taskSummary.source_of_truth -ne 'Monday CRM' -or $taskSummary.excluded_subitems.valid -ne $true) {
        throw 'The reassessment does not certify the required source and exception checks.'
    }
    $taskRuns = @()
    foreach ($taskMode in @('orders', 'repair')) {
        $taskReviewed = $taskSummary.runs.$taskMode
        if ($null -eq $taskReviewed) { continue }
        $taskRunDir = Join-Path $ReviewDir ($taskMode + '-run')
        $taskManifest = Get-Content -LiteralPath (Join-Path $taskRunDir 'manifest.json') -Raw | ConvertFrom-Json
        if ($taskManifest.run_id -ne $taskReviewed.run_id -or $taskManifest.sha256 -ne $taskReviewed.sha256 -or
            $taskManifest.mode -ne $taskMode -or $taskManifest.scopes -lt 1 -or $taskManifest.deferred_scopes -ne 0) {
            throw 'A staged run differs from the reassessment summary. Apply was not started.'
        }
        $taskRuns += [pscustomobject]@{ Mode = $taskMode; Directory = $taskRunDir; Manifest = $taskManifest }
    }
    if ($taskRuns.Count -eq 0) { throw 'No eligible corrections were staged in this review.' }
    if ($Step -eq 'ApplyAndVerify' -and @($taskRuns | Where-Object { $_.Mode -eq 'repair' }).Count -gt 0 -and
        -not $AllowRepairFields) {
        throw 'Repair refreshes links and their related financial/date fields. Review changes.csv, then supply -AllowRepairFields.'
    }
    $taskFailed = $false
    foreach ($taskRun in $taskRuns) {
        Write-Host "Processing $($taskRun.Mode): $($taskRun.Manifest.projects) projects, $($taskRun.Manifest.changes) field changes"
        $taskApplyExit = 0
        $taskVerifyExit = -1
        try {
            if ($Step -eq 'ApplyAndVerify') {
                $taskArguments = @('-m', 'scripts.order_value_scopes_targeted', 'apply', '--run-dir', $taskRun.Directory,
                    '--confirm-run-id', $taskRun.Manifest.run_id, '--allow-partial', '--all-pending')
                if ($taskRun.Mode -eq 'repair') { $taskArguments += '--allow-repair-fields' }
                & $PythonPath @taskArguments
                $taskApplyExit = $LASTEXITCODE
            }
        }
        finally {
            & $PythonPath -m scripts.order_value_scopes_targeted verify --run-dir $taskRun.Directory
            $taskVerifyExit = $LASTEXITCODE
            Write-Host "$($taskRun.Mode): apply exit $taskApplyExit; verification exit $taskVerifyExit"
        }
        if ($taskVerifyExit -ne 0) { $taskFailed = $true }
    }
    if ($taskFailed) {
        throw 'Some staged scopes remain unverified. Review their receipts and keep the same run directories for safe resume.'
    }
    Write-Host 'All staged eligible corrections passed verification.'
    Write-Host "Cases excluded from automatic correction remain listed in $(Join-Path $ReviewDir 'unresolved-projects.csv')."
}
finally {
    Pop-Location
}
