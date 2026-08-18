param(
    [Parameter(Mandatory = $true)]
    [ValidateRange(1, 65535)]
    [int]$Port,

    [Parameter(Mandatory = $true)]
    [string]$Catalog,

    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$SupabaseServer,

    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$DatabaseName,

    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$PythonScriptPath,

    [string]$AcceptanceExportPath,

    [datetime]$ExpectedHistoryStart = [datetime]"2022-01-01",

    [datetime]$ExpectedHistoryEnd = [datetime]::MinValue,

    [ValidateRange(1, 120)]
    [int]$ExpectedForecastMonths = 15,

    [switch]$PreviewM
)

$ErrorActionPreference = "Stop"

function ConvertTo-MTextLiteral {
    param(
        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
        [string]$Value
    )

    $escaped = $Value.Replace("#", "#(0023)").Replace('"', '""')
    $escaped = $escaped.Replace("`r`n", "`n").Replace("`r", "`n")
    return '"' + $escaped.Replace("`n", "#(lf)") + '"'
}

function Get-NormalizedFieldName {
    param([Parameter(Mandatory = $true)][string]$Name)

    return $Name -replace '^\[|\]$', ''
}

function Invoke-AdomdSingleRow {
    param(
        [Parameter(Mandatory = $true)][object]$Connection,
        [Parameter(Mandatory = $true)][string]$Query
    )

    $command = $Connection.CreateCommand()
    $command.CommandTimeout = 120
    $command.CommandText = $Query
    $reader = $command.ExecuteReader()
    try {
        if (-not $reader.Read()) {
            throw "The ADOMD verification query returned no rows."
        }

        $values = [ordered]@{}
        for ($index = 0; $index -lt $reader.FieldCount; $index++) {
            $name = Get-NormalizedFieldName -Name $reader.GetName($index)
            $values[$name] = $reader.GetValue($index)
        }
        return [PSCustomObject]$values
    }
    finally {
        $reader.Dispose()
        $command.Dispose()
    }
}

function Export-AdomdRows {
    param(
        [Parameter(Mandatory = $true)][object]$Connection,
        [Parameter(Mandatory = $true)][string]$Query,
        [Parameter(Mandatory = $true)][string]$Path
    )

    $command = $Connection.CreateCommand()
    $command.CommandTimeout = 120
    $command.CommandText = $Query
    $reader = $command.ExecuteReader()
    try {
        $rows = [System.Collections.Generic.List[object]]::new()
        while ($reader.Read()) {
            $record = [ordered]@{}
            for ($index = 0; $index -lt $reader.FieldCount; $index++) {
                $name = Get-NormalizedFieldName -Name $reader.GetName($index)
                $value = $reader.GetValue($index)
                if ($value -is [System.DBNull]) {
                    $record[$name] = $null
                }
                elseif ($value -is [datetime]) {
                    $record[$name] = $value.ToString(
                        "yyyy-MM-dd",
                        [System.Globalization.CultureInfo]::InvariantCulture
                    )
                }
                elseif ($value -is [System.IFormattable]) {
                    $record[$name] = $value.ToString(
                        $null,
                        [System.Globalization.CultureInfo]::InvariantCulture
                    )
                }
                else {
                    $record[$name] = $value
                }
            }
            $rows.Add([PSCustomObject]$record)
        }
    }
    finally {
        $reader.Dispose()
        $command.Dispose()
    }

    $resolvedPath = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath(
        $Path
    )
    $parent = Split-Path -Parent $resolvedPath
    if ($parent) {
        [void][System.IO.Directory]::CreateDirectory($parent)
    }
    $rows | Export-Csv -LiteralPath $resolvedPath -NoTypeInformation -Encoding UTF8
    Write-Output "PBIX acceptance rows written to: $resolvedPath"
}

function Assert-Equal {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][object]$Actual,
        [Parameter(Mandatory = $true)][object]$Expected
    )

    if ($Actual -ne $Expected) {
        throw "$Name failed: actual=$Actual, expected=$Expected"
    }
}

function Assert-MonthEqual {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][object]$Actual,
        [Parameter(Mandatory = $true)][datetime]$Expected
    )

    $actualMonth = ([datetime]$Actual).Date
    if ($actualMonth -ne $Expected.Date) {
        throw "$Name failed: actual=$($actualMonth.ToString('yyyy-MM-dd')), expected=$($Expected.ToString('yyyy-MM-dd'))"
    }
}

$resolvedPythonScriptPath = (Resolve-Path -LiteralPath $PythonScriptPath).Path
$pythonSource = [System.IO.File]::ReadAllText($resolvedPythonScriptPath)
if ([string]::IsNullOrWhiteSpace($pythonSource)) {
    throw "The embedded Python script is empty: $resolvedPythonScriptPath"
}

$tableName = "vw_weighted_enquiry_value_monthly_oct2027"
$allocationTableName = "ProjectLeafAllocation"
$modelSummaryTableName = "SegmentModelSummary"
$serverLiteral = ConvertTo-MTextLiteral -Value $SupabaseServer
$databaseLiteral = ConvertTo-MTextLiteral -Value $DatabaseName
$pythonLiteral = ConvertTo-MTextLiteral -Value $pythonSource
$sqlQueryLiteral = ConvertTo-MTextLiteral -Value @"
SELECT
    month_start,
    product_segment,
    category_segment,
    actual_weighted_enquiry_value
FROM public.vw_weighted_enquiry_leaf_monthly_v1
ORDER BY month_start, product_segment, category_segment
"@
$mExpression = @(
    "let",
    "    Source = PostgreSQL.Database(",
    "        $serverLiteral,",
    "        $databaseLiteral,",
    "        [CreateNavigationProperties=false, Query=$sqlQueryLiteral]",
    "    ),",
    "    SelectedInput = Table.SelectColumns(",
    "        Source,",
    "        {",
    "            `"month_start`",",
    "            `"product_segment`",",
    "            `"category_segment`",",
    "            `"actual_weighted_enquiry_value`"",
    "        },",
    "        MissingField.Error",
    "    ),",
    "    TypedInput = Table.TransformColumnTypes(",
    "        SelectedInput,",
    "        {",
    "            {`"month_start`", type date},",
    "            {`"product_segment`", type text},",
    "            {`"category_segment`", type text},",
    "            {`"actual_weighted_enquiry_value`", type number}",
    "        },",
    "        `"en-GB`"",
    "    ),",
    "    PythonInput = Table.TransformColumns(",
    "        TypedInput,",
    "        {",
    "            {",
    "                `"month_start`",",
    "                each Date.ToText(_, [Format=`"yyyy-MM-dd`", Culture=`"en-GB`"]),",
    "                type text",
    "            }",
    "        }",
    "    ),",
    "    PythonResult = Python.Execute($pythonLiteral, [dataset = PythonInput]),",
    "    ForecastRows = Table.SelectRows(PythonResult, each [Name] = `"forecast_report`"),",
    "    ForecastReport =",
    "        if Table.RowCount(ForecastRows) = 1 then",
    "            ForecastRows{0}[Value]",
    "        else",
    "            error Error.Record(",
    "                `"Missing Python result`",",
    "                `"Python.Execute did not return exactly one forecast_report table.`",",
    "                [AvailableNames = Text.Combine(List.Transform(PythonResult[Name], Text.From), `", `")]",
    "            ),",
    "    SelectedOutput = Table.SelectColumns(",
    "        ForecastReport,",
    "        {",
    "            `"product_segment`",",
    "            `"category_segment`",",
    "            `"month_start`",",
    "            `"actual_weighted_enquiry_value`",",
    "            `"forecast_weighted_enquiry_value`",",
    "            `"xgboost_forecast`",",
    "            `"seasonal_forecast`",",
    "            `"series_type`",",
    "            `"model`",",
    "            `"forecast_horizon_months`",",
    "            `"history_end`"",
    "        },",
    "        MissingField.Error",
    "    ),",
    "    Changed = Table.TransformColumnTypes(",
    "        SelectedOutput,",
    "        {",
    "            {`"product_segment`", type text},",
    "            {`"category_segment`", type text},",
    "            {`"month_start`", type date},",
    "            {`"actual_weighted_enquiry_value`", type number},",
    "            {`"forecast_weighted_enquiry_value`", type number},",
    "            {`"xgboost_forecast`", type number},",
    "            {`"seasonal_forecast`", type number},",
    "            {`"series_type`", type text},",
    "            {`"model`", type text},",
    "            {`"forecast_horizon_months`", Int64.Type},",
    "            {`"history_end`", type date}",
    "        },",
    "        `"en-GB`"",
    "    )",
    "in",
    "    Changed"
)

$allocationSqlQueryLiteral = ConvertTo-MTextLiteral -Value @"
SELECT
    project_id,
    enquiry_month,
    product_segment,
    category_segment,
    allocation_share,
    project_weighted_enquiry_value,
    allocated_weighted_enquiry_value,
    product_allocation_method,
    category_allocation_method,
    product_mapping_status,
    category_mapping_status,
    subitem_source_value_total
FROM public.vw_weighted_enquiry_project_leaf_allocation_v1
ORDER BY enquiry_month, project_id, product_segment, category_segment
"@
$allocationMExpression = @(
    "let",
    "    Source = PostgreSQL.Database(",
    "        $serverLiteral,",
    "        $databaseLiteral,",
    "        [CreateNavigationProperties=false, Query=$allocationSqlQueryLiteral]",
    "    ),",
    "    Selected = Table.SelectColumns(",
    "        Source,",
    "        {",
    "            `"project_id`",",
    "            `"enquiry_month`",",
    "            `"product_segment`",",
    "            `"category_segment`",",
    "            `"allocation_share`",",
    "            `"project_weighted_enquiry_value`",",
    "            `"allocated_weighted_enquiry_value`",",
    "            `"product_allocation_method`",",
    "            `"category_allocation_method`",",
    "            `"product_mapping_status`",",
    "            `"category_mapping_status`",",
    "            `"subitem_source_value_total`"",
    "        },",
    "        MissingField.Error",
    "    ),",
    "    Changed = Table.TransformColumnTypes(",
    "        Selected,",
    "        {",
    "            {`"project_id`", Int64.Type},",
    "            {`"enquiry_month`", type date},",
    "            {`"product_segment`", type text},",
    "            {`"category_segment`", type text},",
    "            {`"allocation_share`", type number},",
    "            {`"project_weighted_enquiry_value`", type number},",
    "            {`"allocated_weighted_enquiry_value`", type number},",
    "            {`"product_allocation_method`", type text},",
    "            {`"category_allocation_method`", type text},",
    "            {`"product_mapping_status`", type text},",
    "            {`"category_mapping_status`", type text},",
    "            {`"subitem_source_value_total`", type number}",
    "        },",
    "        `"en-GB`"",
    "    )",
    "in",
    "    Changed"
)

if ($PreviewM) {
    $mExpression -join [Environment]::NewLine
    ""
    "// ProjectLeafAllocation"
    $allocationMExpression -join [Environment]::NewLine
    return
}

$powerBiBinPath = "C:\Program Files\Microsoft Power BI Desktop\bin"
$adomdPath = Join-Path $powerBiBinPath "Microsoft.PowerBI.AdomdClient.dll"
$tabularCorePath = Join-Path $powerBiBinPath "Microsoft.AnalysisServices.Server.Core.dll"
$tabularPath = Join-Path $powerBiBinPath "Microsoft.AnalysisServices.Server.Tabular.dll"

foreach ($assemblyPath in @($adomdPath, $tabularCorePath, $tabularPath)) {
    if (-not (Test-Path -LiteralPath $assemblyPath)) {
        throw "Required Power BI assembly was not found at $assemblyPath"
    }
}

Add-Type -Path $adomdPath
Add-Type -Path $tabularCorePath
Add-Type -Path $tabularPath

$columns = @(
    [ordered]@{ name = "product_segment"; dataType = "string"; sourceColumn = "product_segment" },
    [ordered]@{ name = "category_segment"; dataType = "string"; sourceColumn = "category_segment" },
    [ordered]@{ name = "month_start"; dataType = "dateTime"; formatString = "dd/MM/yyyy"; sourceColumn = "month_start" },
    [ordered]@{ name = "actual_weighted_enquiry_value"; dataType = "double"; sourceColumn = "actual_weighted_enquiry_value"; summarizeBy = "sum" },
    [ordered]@{ name = "forecast_weighted_enquiry_value"; dataType = "double"; sourceColumn = "forecast_weighted_enquiry_value"; summarizeBy = "sum" },
    [ordered]@{ name = "xgboost_forecast"; dataType = "double"; sourceColumn = "xgboost_forecast"; summarizeBy = "sum" },
    [ordered]@{ name = "seasonal_forecast"; dataType = "double"; sourceColumn = "seasonal_forecast"; summarizeBy = "sum" },
    [ordered]@{ name = "series_type"; dataType = "string"; sourceColumn = "series_type" },
    [ordered]@{ name = "model"; dataType = "string"; sourceColumn = "model" },
    [ordered]@{ name = "forecast_horizon_months"; dataType = "int64"; formatString = "0"; sourceColumn = "forecast_horizon_months"; summarizeBy = "sum" },
    [ordered]@{ name = "history_end"; dataType = "dateTime"; formatString = "dd/MM/yyyy"; sourceColumn = "history_end" }
)

$currencyFormat = '"£"#,0.00;-"£"#,0.00;"£"#,0.00'
$measures = @(
    [ordered]@{
        name = "Actual Weighted Enquiry Value - oct2027"
        expression = "CALCULATE(SUM('$tableName'[actual_weighted_enquiry_value]), '$tableName'[series_type] = `"Actual`")"
        formatString = $currencyFormat
    },
    [ordered]@{
        name = "Forecast Weighted Enquiry Value - oct2027"
        expression = "CALCULATE(SUM('$tableName'[forecast_weighted_enquiry_value]), '$tableName'[series_type] IN { `"Bridge`", `"Forecast`" })"
        formatString = $currencyFormat
    },
    [ordered]@{
        name = "XGBoost Forecast Component"
        expression = "CALCULATE(SUM('$tableName'[xgboost_forecast]), '$tableName'[series_type] IN { `"Bridge`", `"Forecast`" })"
        formatString = $currencyFormat
    },
    [ordered]@{
        name = "Seasonal Forecast Component"
        expression = "CALCULATE(SUM('$tableName'[seasonal_forecast]), '$tableName'[series_type] IN { `"Bridge`", `"Forecast`" })"
        formatString = $currencyFormat
    }
)

function Update-ForecastTableMetadata {
    param(
        [Parameter(Mandatory = $true)][int]$Port,
        [Parameter(Mandatory = $true)][string]$Catalog,
        [Parameter(Mandatory = $true)][string]$TableName,
        [Parameter(Mandatory = $true)][object[]]$Measures,
        [Parameter(Mandatory = $true)][string[]]$MExpression
    )

    $server = [Microsoft.AnalysisServices.Tabular.Server]::new()
    try {
        $server.Connect("localhost:$Port")
        $database = $server.Databases[$Catalog]
        if ($null -eq $database) {
            throw "The TOM catalog was not found: $Catalog"
        }
        $table = $database.Model.Tables[$TableName]
        if ($null -eq $table) {
            throw "The TOM forecast table was not found: $TableName"
        }

        $nonContractColumn = $table.Columns["fallback_reason"]
        if ($null -ne $nonContractColumn) {
            [void]$table.Columns.Remove($nonContractColumn)
        }

        foreach ($definition in $Measures) {
            $measure = $table.Measures[$definition.name]
            if ($null -eq $measure) {
                $measure = [Microsoft.AnalysisServices.Tabular.Measure]::new()
                $measure.Name = $definition.name
                $table.Measures.Add($measure)
            }
            $measure.Expression = [string]$definition.expression
            $measure.FormatString = [string]$definition.formatString
        }

        $partition = $table.Partitions[$TableName]
        if ($null -eq $partition) {
            throw "The TOM forecast partition was not found: $TableName"
        }
        $partitionSource = [Microsoft.AnalysisServices.Tabular.MPartitionSource]::new()
        $partitionSource.Expression = $MExpression -join [Environment]::NewLine
        $partition.Source = $partitionSource

        [void]$database.Model.SaveChanges()
        Write-Output "Forecast table metadata updated without replacing date variations."
    }
    finally {
        $server.Disconnect()
    }
}

$allocationColumns = @(
    [ordered]@{ name = "project_id"; dataType = "int64"; formatString = "0"; sourceColumn = "project_id"; summarizeBy = "none" },
    [ordered]@{ name = "enquiry_month"; dataType = "dateTime"; formatString = "dd/MM/yyyy"; sourceColumn = "enquiry_month" },
    [ordered]@{ name = "product_segment"; dataType = "string"; sourceColumn = "product_segment" },
    [ordered]@{ name = "category_segment"; dataType = "string"; sourceColumn = "category_segment" },
    [ordered]@{ name = "allocation_share"; dataType = "double"; formatString = "0.00%"; sourceColumn = "allocation_share"; summarizeBy = "sum" },
    [ordered]@{ name = "project_weighted_enquiry_value"; dataType = "double"; formatString = $currencyFormat; sourceColumn = "project_weighted_enquiry_value"; summarizeBy = "sum" },
    [ordered]@{ name = "allocated_weighted_enquiry_value"; dataType = "double"; formatString = $currencyFormat; sourceColumn = "allocated_weighted_enquiry_value"; summarizeBy = "sum" },
    [ordered]@{ name = "product_allocation_method"; dataType = "string"; sourceColumn = "product_allocation_method" },
    [ordered]@{ name = "category_allocation_method"; dataType = "string"; sourceColumn = "category_allocation_method" },
    [ordered]@{ name = "product_mapping_status"; dataType = "string"; sourceColumn = "product_mapping_status" },
    [ordered]@{ name = "category_mapping_status"; dataType = "string"; sourceColumn = "category_mapping_status" },
    [ordered]@{ name = "subitem_source_value_total"; dataType = "double"; formatString = $currencyFormat; sourceColumn = "subitem_source_value_total"; summarizeBy = "sum" }
)

$allocationMeasures = @(
    [ordered]@{
        name = "Allocated Weighted Value"
        expression = "SUM('$allocationTableName'[allocated_weighted_enquiry_value])"
        formatString = $currencyFormat
    },
    [ordered]@{
        name = "Distinct Project Count"
        expression = "DISTINCTCOUNT('$allocationTableName'[project_id])"
        formatString = "0"
    },
    [ordered]@{
        name = "Source Weighted Value"
        expression = "SUMX(VALUES('$allocationTableName'[project_id]), CALCULATE(MAX('$allocationTableName'[project_weighted_enquiry_value])))"
        formatString = $currencyFormat
    },
    [ordered]@{
        name = "Allocation Reconciliation Delta"
        expression = @"
VAR SelectedProjects = VALUES('$allocationTableName'[project_id])
VAR AllocatedForProjects =
    CALCULATE(
        [Allocated Weighted Value],
        REMOVEFILTERS('$allocationTableName'),
        TREATAS(SelectedProjects, '$allocationTableName'[project_id])
    )
VAR SourceForProjects =
    CALCULATE(
        [Source Weighted Value],
        REMOVEFILTERS('$allocationTableName'),
        TREATAS(SelectedProjects, '$allocationTableName'[project_id])
    )
RETURN
    AllocatedForProjects - SourceForProjects
"@
        formatString = $currencyFormat
    },
    [ordered]@{
        name = "Allocation % of Selected Total"
        expression = "DIVIDE([Allocated Weighted Value], CALCULATE([Allocated Weighted Value], ALLSELECTED('$allocationTableName'[product_segment], '$allocationTableName'[category_segment])))"
        formatString = "0.00%"
    }
)

$createOrReplaceAllocation = [ordered]@{
    createOrReplace = [ordered]@{
        object = [ordered]@{ database = $Catalog; table = $allocationTableName }
        table = [ordered]@{
            name = $allocationTableName
            columns = $allocationColumns
            measures = $allocationMeasures
            partitions = @(
                [ordered]@{
                    name = $allocationTableName
                    mode = "import"
                    source = [ordered]@{ type = "m"; expression = $allocationMExpression }
                }
            )
        }
    }
}

$modelSummaryExpression = @"
VAR LeafKeys =
    SUMMARIZE(
        FILTER('$tableName', '$tableName'[series_type] = "Forecast"),
        '$tableName'[product_segment],
        '$tableName'[category_segment]
    )
RETURN
    GENERATE(
        LeafKeys,
        VAR ProductSegment = '$tableName'[product_segment]
        VAR CategorySegment = '$tableName'[category_segment]
        VAR ActualRows =
            FILTER(
                '$tableName',
                '$tableName'[product_segment] = ProductSegment
                    && '$tableName'[category_segment] = CategorySegment
                    && '$tableName'[series_type] = "Actual"
            )
        VAR ForecastRows =
            FILTER(
                '$tableName',
                '$tableName'[product_segment] = ProductSegment
                    && '$tableName'[category_segment] = CategorySegment
                    && '$tableName'[series_type] = "Forecast"
            )
        VAR ModelUsed = MAXX(ForecastRows, '$tableName'[model])
        VAR NonzeroMonths =
            COUNTROWS(
                FILTER(ActualRows, '$tableName'[actual_weighted_enquiry_value] > 0)
            )
        RETURN
            ROW(
                "model", ModelUsed,
                "fallback_reason",
                    IF(
                        ModelUsed = "seasonal_average_fallback_sparse",
                        "nonzero_months=" & FORMAT(NonzeroMonths, "0"),
                        ""
                    ),
                "history_start", MINX(ActualRows, '$tableName'[month_start]),
                "history_end", MAXX(ActualRows, '$tableName'[month_start]),
                "history_months", COUNTROWS(ActualRows),
                "nonzero_months", NonzeroMonths,
                "history_total", SUMX(ActualRows, '$tableName'[actual_weighted_enquiry_value]),
                "forecast_horizon_months", COUNTROWS(ForecastRows),
                "forecast_start", MINX(ForecastRows, '$tableName'[month_start]),
                "forecast_end", MAXX(ForecastRows, '$tableName'[month_start]),
                "xgb_weight", 0.75,
                "seasonal_weight", 0.25
            )
    )
"@

$modelSummaryColumns = @(
    [ordered]@{ name = "product_segment"; dataType = "string"; sourceColumn = "product_segment" },
    [ordered]@{ name = "category_segment"; dataType = "string"; sourceColumn = "category_segment" },
    [ordered]@{ name = "model"; dataType = "string"; sourceColumn = "model" },
    [ordered]@{ name = "fallback_reason"; dataType = "string"; sourceColumn = "fallback_reason" },
    [ordered]@{ name = "history_start"; dataType = "dateTime"; formatString = "dd/MM/yyyy"; sourceColumn = "history_start" },
    [ordered]@{ name = "history_end"; dataType = "dateTime"; formatString = "dd/MM/yyyy"; sourceColumn = "history_end" },
    [ordered]@{ name = "history_months"; dataType = "int64"; formatString = "0"; sourceColumn = "history_months"; summarizeBy = "sum" },
    [ordered]@{ name = "nonzero_months"; dataType = "int64"; formatString = "0"; sourceColumn = "nonzero_months"; summarizeBy = "sum" },
    [ordered]@{ name = "history_total"; dataType = "double"; formatString = $currencyFormat; sourceColumn = "history_total"; summarizeBy = "sum" },
    [ordered]@{ name = "forecast_horizon_months"; dataType = "int64"; formatString = "0"; sourceColumn = "forecast_horizon_months"; summarizeBy = "sum" },
    [ordered]@{ name = "forecast_start"; dataType = "dateTime"; formatString = "dd/MM/yyyy"; sourceColumn = "forecast_start" },
    [ordered]@{ name = "forecast_end"; dataType = "dateTime"; formatString = "dd/MM/yyyy"; sourceColumn = "forecast_end" },
    [ordered]@{ name = "xgb_weight"; dataType = "double"; formatString = "0%"; sourceColumn = "xgb_weight"; summarizeBy = "sum" },
    [ordered]@{ name = "seasonal_weight"; dataType = "double"; formatString = "0%"; sourceColumn = "seasonal_weight"; summarizeBy = "sum" }
)

$modelSummaryMeasures = @(
    [ordered]@{
        name = "Leaf Count"
        expression = "COUNTROWS('$modelSummaryTableName')"
        formatString = "0"
    },
    [ordered]@{
        name = "Fallback Leaf Count"
        expression = "CALCULATE(COUNTROWS('$modelSummaryTableName'), '$modelSummaryTableName'[model] = `"seasonal_average_fallback_sparse`")"
        formatString = "0"
    }
)

$createOrReplaceModelSummary = [ordered]@{
    createOrReplace = [ordered]@{
        object = [ordered]@{ database = $Catalog; table = $modelSummaryTableName }
        table = [ordered]@{
            name = $modelSummaryTableName
            columns = $modelSummaryColumns
            measures = $modelSummaryMeasures
            partitions = @(
                [ordered]@{
                    name = $modelSummaryTableName
                    mode = "import"
                    source = [ordered]@{
                        type = "calculated"
                        expression = $modelSummaryExpression
                    }
                }
            )
        }
    }
}

$refreshImports = [ordered]@{
    refresh = [ordered]@{
        type = "full"
        objects = @(
            [ordered]@{ database = $Catalog; table = $tableName },
            [ordered]@{ database = $Catalog; table = $allocationTableName }
        )
    }
}

$recalculateModel = [ordered]@{
    refresh = [ordered]@{
        type = "calculate"
        objects = @([ordered]@{ database = $Catalog })
    }
}

$connection = [Microsoft.AnalysisServices.AdomdClient.AdomdConnection]::new(
    "Data Source=localhost:$Port;Initial Catalog=$Catalog;"
)

try {
    Update-ForecastTableMetadata `
        -Port $Port `
        -Catalog $Catalog `
        -TableName $tableName `
        -Measures $measures `
        -MExpression $mExpression

    Write-Output "Opening local Analysis Services connection on port $Port."
    $connection.Open()
    Write-Output "Local Analysis Services connection opened."

    $operations = @(
        [ordered]@{
            Name = "Replace allocation audit table"
            Payload = $createOrReplaceAllocation
        },
        [ordered]@{
            Name = "Refresh imported audit tables"
            Payload = $refreshImports
        },
        [ordered]@{
            Name = "Replace model summary table"
            Payload = $createOrReplaceModelSummary
        },
        [ordered]@{
            Name = "Recalculate audit model"
            Payload = $recalculateModel
        }
    )
    foreach ($operation in $operations) {
        Write-Output "$($operation.Name) started."
        $command = $connection.CreateCommand()
        $command.CommandTimeout = 600
        $command.CommandText = $operation.Payload | ConvertTo-Json -Depth 20 -Compress
        try {
            [void]$command.ExecuteNonQuery()
        }
        finally {
            $command.Dispose()
        }
        Write-Output "$($operation.Name) completed."
    }

    $verificationQuery = @"
EVALUATE
VAR ActualRows = FILTER('$tableName', '$tableName'[series_type] = "Actual")
VAR BridgeRows = FILTER('$tableName', '$tableName'[series_type] = "Bridge")
VAR ForecastRows = FILTER('$tableName', '$tableName'[series_type] = "Forecast")
RETURN
ROW(
    "RowCount", COUNTROWS('$tableName'),
    "ProductSegmentCount", DISTINCTCOUNT('$tableName'[product_segment]),
    "CategorySegmentCount", DISTINCTCOUNT('$tableName'[category_segment]),
    "ActualRowCount", COUNTROWS(ActualRows),
    "BridgeRowCount", COUNTROWS(BridgeRows),
    "ForecastRowCount", COUNTROWS(ForecastRows),
    "ActualMonthCount", COUNTROWS(SUMMARIZE(ActualRows, '$tableName'[month_start])),
    "BridgeMonthCount", COUNTROWS(SUMMARIZE(BridgeRows, '$tableName'[month_start])),
    "ForecastMonthCount", COUNTROWS(SUMMARIZE(ForecastRows, '$tableName'[month_start])),
    "ActualLeafKeyCount", COUNTROWS(SUMMARIZE(ActualRows, '$tableName'[month_start], '$tableName'[product_segment], '$tableName'[category_segment])),
    "BridgeLeafKeyCount", COUNTROWS(SUMMARIZE(BridgeRows, '$tableName'[month_start], '$tableName'[product_segment], '$tableName'[category_segment])),
    "ForecastLeafKeyCount", COUNTROWS(SUMMARIZE(ForecastRows, '$tableName'[month_start], '$tableName'[product_segment], '$tableName'[category_segment])),
    "ActualTotal", SUM('$tableName'[actual_weighted_enquiry_value]),
    "ForecastTotal", SUM('$tableName'[forecast_weighted_enquiry_value]),
    "FirstActualMonth", MINX(ActualRows, '$tableName'[month_start]),
    "HistoryEnd", MAXX(ActualRows, '$tableName'[month_start]),
    "FirstBridgeMonth", MINX(BridgeRows, '$tableName'[month_start]),
    "LastBridgeMonth", MAXX(BridgeRows, '$tableName'[month_start]),
    "FirstForecastMonth", MINX(ForecastRows, '$tableName'[month_start]),
    "LastForecastMonth", MAXX(ForecastRows, '$tableName'[month_start]),
    "MinForecastHorizon", MIN('$tableName'[forecast_horizon_months]),
    "MaxForecastHorizon", MAX('$tableName'[forecast_horizon_months])
)
"@

    $verification = Invoke-AdomdSingleRow -Connection $connection -Query $verificationQuery
    $historyMonths = [int]$verification.ActualMonthCount
    $expectedActualRows = 8 * $historyMonths
    $expectedForecastRows = 8 * $ExpectedForecastMonths
    $expectedRowCount = $expectedActualRows + 8 + $expectedForecastRows

    Assert-Equal -Name "product segment count" -Actual ([int]$verification.ProductSegmentCount) -Expected 2
    Assert-Equal -Name "category segment count" -Actual ([int]$verification.CategorySegmentCount) -Expected 4
    Assert-Equal -Name "actual row count" -Actual ([int]$verification.ActualRowCount) -Expected $expectedActualRows
    Assert-Equal -Name "bridge row count" -Actual ([int]$verification.BridgeRowCount) -Expected 8
    Assert-Equal -Name "forecast row count" -Actual ([int]$verification.ForecastRowCount) -Expected $expectedForecastRows
    Assert-Equal -Name "total row count" -Actual ([int]$verification.RowCount) -Expected $expectedRowCount
    Assert-Equal -Name "bridge month count" -Actual ([int]$verification.BridgeMonthCount) -Expected 1
    Assert-Equal -Name "forecast month count" -Actual ([int]$verification.ForecastMonthCount) -Expected $ExpectedForecastMonths
    Assert-Equal -Name "actual leaf key uniqueness" -Actual ([int]$verification.ActualLeafKeyCount) -Expected $expectedActualRows
    Assert-Equal -Name "bridge leaf key uniqueness" -Actual ([int]$verification.BridgeLeafKeyCount) -Expected 8
    Assert-Equal -Name "forecast leaf key uniqueness" -Actual ([int]$verification.ForecastLeafKeyCount) -Expected $expectedForecastRows
    Assert-Equal -Name "minimum forecast horizon" -Actual ([int]$verification.MinForecastHorizon) -Expected $ExpectedForecastMonths
    Assert-Equal -Name "maximum forecast horizon" -Actual ([int]$verification.MaxForecastHorizon) -Expected $ExpectedForecastMonths

    $historyEnd = ([datetime]$verification.HistoryEnd).Date
    Assert-MonthEqual -Name "history start" -Actual $verification.FirstActualMonth -Expected $ExpectedHistoryStart
    if ($ExpectedHistoryEnd -ne [datetime]::MinValue) {
        Assert-MonthEqual -Name "history end" -Actual $historyEnd -Expected $ExpectedHistoryEnd
    }
    Assert-MonthEqual -Name "first bridge month" -Actual $verification.FirstBridgeMonth -Expected $historyEnd
    Assert-MonthEqual -Name "last bridge month" -Actual $verification.LastBridgeMonth -Expected $historyEnd
    Assert-MonthEqual -Name "first forecast month" -Actual $verification.FirstForecastMonth -Expected $historyEnd.AddMonths(1)
    Assert-MonthEqual -Name "last forecast month" -Actual $verification.LastForecastMonth -Expected $historyEnd.AddMonths($ExpectedForecastMonths)

    $reconciliationQuery = @"
EVALUATE
VAR MonthlyTotals =
    SUMMARIZE(
        '$tableName',
        '$tableName'[month_start],
        '$tableName'[series_type],
        "OverallValue", COALESCE(SUM('$tableName'[actual_weighted_enquiry_value]), 0) + COALESCE(SUM('$tableName'[forecast_weighted_enquiry_value]), 0),
        "ProductValue", SUMX(VALUES('$tableName'[product_segment]), CALCULATE(COALESCE(SUM('$tableName'[actual_weighted_enquiry_value]), 0) + COALESCE(SUM('$tableName'[forecast_weighted_enquiry_value]), 0))),
        "CategoryValue", SUMX(VALUES('$tableName'[category_segment]), CALCULATE(COALESCE(SUM('$tableName'[actual_weighted_enquiry_value]), 0) + COALESCE(SUM('$tableName'[forecast_weighted_enquiry_value]), 0))),
        "LeafValue", SUMX(SUMMARIZE('$tableName', '$tableName'[product_segment], '$tableName'[category_segment]), CALCULATE(COALESCE(SUM('$tableName'[actual_weighted_enquiry_value]), 0) + COALESCE(SUM('$tableName'[forecast_weighted_enquiry_value]), 0)))
    )
RETURN
ROW(
    "MaxProductCategoryDelta", MAXX(MonthlyTotals, ABS([ProductValue] - [CategoryValue])),
    "MaxOverallLeafDelta", MAXX(MonthlyTotals, ABS([OverallValue] - [LeafValue]))
)
"@

    $reconciliation = Invoke-AdomdSingleRow -Connection $connection -Query $reconciliationQuery
    if ([math]::Abs([double]$reconciliation.MaxProductCategoryDelta) -gt 0.01) {
        throw "Monthly product/category reconciliation exceeded GBP 0.01: $($reconciliation.MaxProductCategoryDelta)"
    }
    if ([math]::Abs([double]$reconciliation.MaxOverallLeafDelta) -gt 0.01) {
        throw "Monthly overall/leaf reconciliation exceeded GBP 0.01: $($reconciliation.MaxOverallLeafDelta)"
    }

    $allocationVerificationQuery = @"
EVALUATE
ROW(
    "RowCount", COUNTROWS('$allocationTableName'),
    "ProjectCount", DISTINCTCOUNT('$allocationTableName'[project_id]),
    "ProductSegmentCount", DISTINCTCOUNT('$allocationTableName'[product_segment]),
    "CategorySegmentCount", DISTINCTCOUNT('$allocationTableName'[category_segment]),
    "SourceTotal", SUMX(VALUES('$allocationTableName'[project_id]), CALCULATE(MAX('$allocationTableName'[project_weighted_enquiry_value]))),
    "AllocatedTotal", SUM('$allocationTableName'[allocated_weighted_enquiry_value]),
    "ReconciliationDelta", [Allocation Reconciliation Delta]
)
"@
    $allocationVerification = Invoke-AdomdSingleRow -Connection $connection -Query $allocationVerificationQuery
    if ([int]$allocationVerification.RowCount -le 0 -or [int]$allocationVerification.ProjectCount -le 0) {
        throw "Project allocation audit table is empty."
    }
    Assert-Equal -Name "allocation product segment count" -Actual ([int]$allocationVerification.ProductSegmentCount) -Expected 2
    Assert-Equal -Name "allocation category segment count" -Actual ([int]$allocationVerification.CategorySegmentCount) -Expected 4
    if ([math]::Abs([double]$allocationVerification.ReconciliationDelta) -gt 0.01) {
        throw "Project allocation reconciliation exceeded GBP 0.01: $($allocationVerification.ReconciliationDelta)"
    }

    $modelSummaryVerificationQuery = @"
EVALUATE
ROW(
    "LeafCount", COUNTROWS('$modelSummaryTableName'),
    "FallbackLeafCount", [Fallback Leaf Count],
    "ProductSegmentCount", DISTINCTCOUNT('$modelSummaryTableName'[product_segment]),
    "CategorySegmentCount", DISTINCTCOUNT('$modelSummaryTableName'[category_segment]),
    "MinForecastHorizon", MIN('$modelSummaryTableName'[forecast_horizon_months]),
    "MaxForecastHorizon", MAX('$modelSummaryTableName'[forecast_horizon_months])
)
"@
    $modelSummaryVerification = Invoke-AdomdSingleRow -Connection $connection -Query $modelSummaryVerificationQuery
    Assert-Equal -Name "model summary leaf count" -Actual ([int]$modelSummaryVerification.LeafCount) -Expected 8
    Assert-Equal -Name "model summary fallback leaf count" -Actual ([int]$modelSummaryVerification.FallbackLeafCount) -Expected 1
    Assert-Equal -Name "model summary product segment count" -Actual ([int]$modelSummaryVerification.ProductSegmentCount) -Expected 2
    Assert-Equal -Name "model summary category segment count" -Actual ([int]$modelSummaryVerification.CategorySegmentCount) -Expected 4
    Assert-Equal -Name "model summary minimum forecast horizon" -Actual ([int]$modelSummaryVerification.MinForecastHorizon) -Expected $ExpectedForecastMonths
    Assert-Equal -Name "model summary maximum forecast horizon" -Actual ([int]$modelSummaryVerification.MaxForecastHorizon) -Expected $ExpectedForecastMonths

    Write-Output "PBIX acceptance checks passed."
    $verification | Format-List
    $reconciliation | Format-List
    $allocationVerification | Format-List
    $modelSummaryVerification | Format-List

    if ($AcceptanceExportPath) {
        $exportQuery = @"
EVALUATE
SELECTCOLUMNS(
    '$tableName',
    "product_segment", '$tableName'[product_segment],
    "category_segment", '$tableName'[category_segment],
    "month_start", '$tableName'[month_start],
    "actual_weighted_enquiry_value", '$tableName'[actual_weighted_enquiry_value],
    "forecast_weighted_enquiry_value", '$tableName'[forecast_weighted_enquiry_value],
    "xgboost_forecast", '$tableName'[xgboost_forecast],
    "seasonal_forecast", '$tableName'[seasonal_forecast],
    "series_type", '$tableName'[series_type],
    "model", '$tableName'[model],
    "forecast_horizon_months", '$tableName'[forecast_horizon_months],
    "history_end", '$tableName'[history_end]
)
ORDER BY [month_start], [product_segment], [category_segment], [series_type]
"@
        Export-AdomdRows -Connection $connection -Query $exportQuery -Path $AcceptanceExportPath
    }
}
finally {
    if ($null -ne $connection) {
        $connection.Dispose()
    }
}
