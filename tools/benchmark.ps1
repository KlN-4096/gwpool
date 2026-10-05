param(
    [Parameter(Mandatory)][string]$Snapshot,
    [Parameter(Mandatory)][string]$OutputDirectory,
    [Parameter(Mandatory)][ValidateSet('gateways', 'single', 'batch3')][string]$Scenario,
    [ValidateRange(1, 32)][int]$Rounds = 32,
    [switch]$Baseline
)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$Snapshot = (Resolve-Path -LiteralPath $Snapshot).Path
[void](New-Item -ItemType Directory -Force -Path $OutputDirectory)
$OutputDirectory = (Resolve-Path -LiteralPath $OutputDirectory).Path

function Get-SourceManifest {
    $files = @(& git -C $root ls-files --cached --others --exclude-standard) |
        Where-Object { $_ -match '\.(go|html|json|tsv|ps1)$|(^|/)go\.(mod|sum)$' -and $_ -notmatch '^docs/' } |
        Sort-Object -Unique
    if ($LASTEXITCODE -ne 0) { throw 'Cannot enumerate benchmark sources' }
    $hashes = [ordered]@{}
    foreach ($file in $files) {
        $hashes[$file] = (Get-FileHash -LiteralPath (Join-Path $root $file) -Algorithm SHA256).Hash.ToLowerInvariant()
    }
    $lines = @($hashes.GetEnumerator() | ForEach-Object { "$($_.Key)`t$($_.Value)" }) -join "`n"
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $digest = ([BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes($lines)))).Replace('-', '').ToLowerInvariant()
    } finally { $sha.Dispose() }
    $head = (& git -C $root rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0) { throw 'Cannot resolve benchmark Git HEAD' }
    return [ordered]@{ git_head = $head; source_tree_sha256 = $digest; files = $hashes }
}

$before = Get-SourceManifest
$before | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $OutputDirectory "source-$Scenario.json") -Encoding utf8
$env:GWPOOL_PERF_SNAPSHOT = $Snapshot
$env:GWPOOL_PERF_SCENARIO = $Scenario
$env:GWPOOL_PERF_ROUNDS = [string]$Rounds
$env:GWPOOL_PERF_OUTPUT = Join-Path $OutputDirectory "perf-$Scenario.json"
$env:GWPOOL_PERF_BASELINE = if ($Baseline) { '1' } else { '0' }
$env:GWPOOL_PERF_GIT_HEAD = $before.git_head
$env:GWPOOL_PERF_SOURCE_SHA256 = $before.source_tree_sha256
Push-Location $root
try {
    & go test -p 2 -timeout 60s -count=1 -run '^TestProductionSnapshotPerformance$' ./internal/httpapi
    $testExit = $LASTEXITCODE
} finally { Pop-Location }
$after = Get-SourceManifest
if ($after.source_tree_sha256 -ne $before.source_tree_sha256) {
    throw 'Sources changed during benchmark; reject this measurement'
}
if ($testExit -ne 0) { throw "Benchmark failed: exit $testExit" }
$result = Get-Content -LiteralPath $env:GWPOOL_PERF_OUTPUT -Raw | ConvertFrom-Json
$result | Select-Object scenario, samples, p50_ms, p95_ms, p99_ms, qps, cpu_seconds,
    process_write_bytes, process_write_operations, performance_gate_passed, source_tree_sha256 |
    ConvertTo-Json -Compress
