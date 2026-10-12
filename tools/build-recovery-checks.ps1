param([Parameter(Mandatory = $true)][string]$Output)
$ErrorActionPreference = 'Stop'
$go = 'C:/Users/Administrator/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.27.2.windows-amd64/bin/go.exe'
$saved = @{}
foreach ($name in @('GOOS', 'GOARCH', 'CGO_ENABLED', 'GOTOOLCHAIN')) {
    $saved[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}
Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    $env:GOOS = 'linux'; $env:GOARCH = 'amd64'; $env:CGO_ENABLED = '0'; $env:GOTOOLCHAIN = 'local'
    & $go test -c -p 2 -trimpath -o $Output ./internal/store
    if ($LASTEXITCODE -ne 0) { throw 'Native cross-build of recovery checks failed' }
    Get-FileHash -LiteralPath $Output -Algorithm SHA256
} finally {
    Pop-Location
    foreach ($entry in $saved.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
    }
}
