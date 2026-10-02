[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$RunDir,

    [ValidateSet('Stage', 'ApplyAndVerify', 'Verify', 'All')]
    [string]$Step = 'Stage',

    [string]$PythonPath = '.\report.venv\Scripts\python.exe'
)

$ErrorActionPreference = 'Stop'
$taskRepo = Split-Path -Parent $PSScriptRoot
Push-Location -LiteralPath $taskRepo
try {
    $taskSelection = 'docs/order-value-recovery-48-projects.txt'
    $taskExpectedIds = @(Get-Content -LiteralPath $taskSelection |
        ForEach-Object { $_.Trim() } |
        Where-Object { $_ -and -not $_.StartsWith('#') } |
        Sort-Object)
    if ($taskExpectedIds.Count -ne 48 -or @($taskExpectedIds | Select-Object -Unique).Count -ne 48) {
        throw 'The recovery selection must contain exactly 48 unique project IDs.'
    }

    if ($Step -in @('Stage', 'All')) {
        & $PythonPath -m scripts.order_value_scopes_refresh `
            --previous-run outputs/order_value_backfill/targeted_orders_20261002_092604 `
            --projects-file $taskSelection `
            --run-dir $RunDir
        $taskStageExit = $LASTEXITCODE
        if ($taskStageExit -ne 0) {
            throw "Staging returned $taskStageExit. No apply was started. Review the staged deferrals or error."
        }
    }

    $taskManifest = Get-Content -LiteralPath (Join-Path $RunDir 'manifest.json') -Raw | ConvertFrom-Json
    $taskPlan = Get-Content -LiteralPath (Join-Path $RunDir 'scopes.json') -Raw | ConvertFrom-Json
    $taskStagedIds = @($taskPlan.scopes | ForEach-Object { $_.scope.projects } | Sort-Object)
    if ($taskManifest.workflow -ne 'scoped-reads-v1' -or $taskManifest.mode -ne 'orders' -or
        $taskManifest.monday_source_refreshed -ne $true -or $taskManifest.deferred_scopes -ne 0 -or
        $taskManifest.selected_projects -ne 48 -or $taskManifest.projects -ne 48 -or
        $taskStagedIds.Count -ne 48 -or @($taskPlan.deferred).Count -ne 0 -or
        ($taskStagedIds -join ',') -cne ($taskExpectedIds -join ',')) {
        throw 'This is not a complete, refreshed order-only plan for the selected 48 projects. Apply was not started.'
    }
    $taskManifest | Select-Object run_id, projects, scopes, deferred_scopes, changes | Format-List
    Write-Host "Recovery directory: $RunDir"

    if ($Step -in @('ApplyAndVerify', 'All')) {
        $taskApplyExit = -1
        $taskVerifyExit = -1
        try {
            & $PythonPath -m scripts.order_value_scopes_targeted apply `
                --run-dir $RunDir --confirm-run-id $taskManifest.run_id --allow-partial --all-pending
            $taskApplyExit = $LASTEXITCODE
        }
        finally {
            # Verify even after a deferred scope or interrupted/uncertain apply.
            & $PythonPath -m scripts.order_value_scopes_targeted verify --run-dir $RunDir
            $taskVerifyExit = $LASTEXITCODE
            Write-Host "Apply exit: $taskApplyExit; verification exit: $taskVerifyExit"
        }
        if ($taskVerifyExit -ne 0) {
            throw 'Recovery is not fully verified. Review the receipts; retain this directory for diagnosis or safe resume.'
        }
        if ($taskApplyExit -ne 0) {
            Write-Warning 'Apply reported an error, but the subsequent verification confirmed all selected scopes.'
        }
        Write-Host 'All 48 selected projects passed verification.'
    }
    elseif ($Step -eq 'Verify') {
        & $PythonPath -m scripts.order_value_scopes_targeted verify --run-dir $RunDir
        if ($LASTEXITCODE -ne 0) {
            throw 'Recovery is not fully verified. Review the verification receipt.'
        }
        Write-Host 'All 48 selected projects passed verification.'
    }
    else {
        Write-Host "Staging completed. Review $(Join-Path $RunDir 'changes.csv') before ApplyAndVerify."
    }
}
finally {
    Pop-Location
}
