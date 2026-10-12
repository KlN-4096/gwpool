param(
    [Parameter(Mandatory = $true)][string]$Baseline,
    [Parameter(Mandatory = $true)][string]$Output
)
$ErrorActionPreference = 'Stop'
$repository = Split-Path -Parent $PSScriptRoot
$go = 'C:/Users/Administrator/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.27.2.windows-amd64/bin/go.exe'
$files = @(
    'internal/config/regions.go', 'internal/config/regions_test.go',
    'internal/contact/stats.go', 'internal/contact/priority.go', 'internal/contact/priority_test.go',
    'internal/httpapi/api.go', 'internal/httpapi/catalog_cache.go', 'internal/httpapi/catalog_cache_test.go',
    'internal/httpapi/contact.go', 'internal/httpapi/cooldown.go', 'internal/httpapi/cooldown_test.go',
    'internal/httpapi/gateways.go', 'internal/httpapi/public_access_test.go',
    'internal/httpapi/report_batch.go', 'internal/httpapi/report_batch_test.go',
    'internal/httpapi/page_samples_test.go',
    'internal/routing/model.go',
    'internal/sched/inventory.go', 'internal/sched/routes.go', 'internal/sched/routing.go',
    'internal/sched/sched.go', 'internal/sched/warm.go', 'internal/sched/route_cache.go', 'internal/sched/route_cache_test.go',
    'internal/store/contact.go', 'internal/store/cooldown.go', 'internal/store/inventory.go',
    'internal/store/journal.go', 'internal/store/routing.go', 'internal/store/store.go', 'internal/store/store_test.go',
    'internal/store/routing_model.go', 'internal/store/routing_model_test.go',
    'internal/store/checkpoint_loop_test.go', 'internal/store/gateway_read_test.go',
    'internal/store/report_retention_test.go', 'internal/store/samples_snapshot_test.go',
    'internal/store/page_samples.go', 'internal/store/page_samples_test.go'
)
$fixture = 'internal/httpapi/performance_linux_test.go'
$hashes = [ordered]@{}
foreach ($path in ($files + $fixture | Sort-Object)) {
    $hashes[$path] = (Get-FileHash -LiteralPath (Join-Path $repository $path) -Algorithm SHA256).Hash.ToLowerInvariant()
}
$baselineManifest = @(Get-ChildItem -LiteralPath $Baseline -Recurse -File |
    Where-Object { $_.Extension -eq '.go' -or $_.Extension -eq '.html' -or $_.Name -in @('go.mod', 'go.sum') } |
    Sort-Object FullName | ForEach-Object {
        "$([IO.Path]::GetRelativePath($Baseline, $_.FullName).Replace('\', '/')) $((Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant())"
    })
$inputText = ($baselineManifest -join "`n") + "`n" + (($hashes.GetEnumerator() | ForEach-Object { "$($_.Key) $($_.Value)" }) -join "`n")
$inputHash = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes($inputText))).ToLowerInvariant()
$root = Join-Path $Output $inputHash.Substring(0, 12)
if (Test-Path -LiteralPath (Join-Path $root 'build.json')) {
    $receipt = Get-Content -Raw -LiteralPath (Join-Path $root 'build.json') | ConvertFrom-Json
    foreach ($name in @('a', 'b')) {
        if ((Get-FileHash -LiteralPath (Join-Path $root "$name.test") -Algorithm SHA256).Hash.ToLowerInvariant() -ne $receipt.binaries.$name) {
            throw 'Existing benchmark binary hash differs'
        }
    }
    Write-Output $root
    exit 0
}
if (Test-Path -LiteralPath $root) { throw 'Incomplete build exists; inspect instead of overwriting' }
New-Item -ItemType Directory -Path $root -Force | Out-Null
$environment = @{}
foreach ($name in @('GOOS', 'GOARCH', 'CGO_ENABLED', 'GOWORK', 'GOFLAGS', 'GOENV', 'GOTOOLCHAIN')) {
    $environment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}
$binaries = [ordered]@{}
$sourceHashes = [ordered]@{}
try {
    $env:GOOS = 'linux'; $env:GOARCH = 'amd64'; $env:CGO_ENABLED = '0'
    $env:GOWORK = 'off'; $env:GOFLAGS = ''; $env:GOENV = 'off'; $env:GOTOOLCHAIN = 'local'
    foreach ($name in @('a', 'b')) {
        $source = Join-Path $root "$name-source"
        Copy-Item -LiteralPath $Baseline -Destination $source -Recurse
        $overlay = @($fixture)
        if ($name -eq 'b') { $overlay += $files }
        foreach ($path in $overlay) {
            Copy-Item -LiteralPath (Join-Path $repository $path) -Destination (Join-Path $source $path)
        }
        $sourceManifest = @(Get-ChildItem -LiteralPath $source -Recurse -File |
            Where-Object { $_.Extension -eq '.go' -or $_.Extension -eq '.html' -or $_.Name -in @('go.mod', 'go.sum') } |
            Sort-Object FullName | ForEach-Object {
                $relative = [IO.Path]::GetRelativePath($source, $_.FullName).Replace('\', '/')
                "$relative $((Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant())"
            })
        $sourceHash = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes(($sourceManifest -join "`n")))).ToLowerInvariant()
        $sourceHashes[$name] = $sourceHash
        $sourceManifest | Set-Content -LiteralPath (Join-Path $root "$name-source-sha256.txt")
        Push-Location $source
        try {
            & $go test -c -p 2 -trimpath -o (Join-Path $root "$name.test") ./internal/httpapi
            if ($LASTEXITCODE -ne 0) { throw "Native cross-build $name failed" }
        } finally { Pop-Location }
        $binaries[$name] = (Get-FileHash -LiteralPath (Join-Path $root "$name.test") -Algorithm SHA256).Hash.ToLowerInvariant()
    }
} finally {
    foreach ($entry in $environment.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
    }
}
foreach ($path in ($files + $fixture)) {
    if ((Get-FileHash -LiteralPath (Join-Path $repository $path) -Algorithm SHA256).Hash.ToLowerInvariant() -ne $hashes[$path]) {
        throw "Source changed during build: $path"
    }
}
[ordered]@{input_sha256=$inputHash; baseline=$Baseline; go=(& $go version).Trim();
    binaries=$binaries; source_hashes=$sourceHashes; files=$hashes; fixture_sha256=$hashes[$fixture]} |
    ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $root 'build.json') -Encoding utf8
Write-Output $root
