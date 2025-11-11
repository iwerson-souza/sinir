Param(
    [ValidateSet('ref-load','stakeholder','mtr')]
    [string]$Cmd = 'ref-load',

    [ValidateSet('Debug','Release')]
    [string]$Configuration = 'Release',

    [switch]$NoBuild
)

$ErrorActionPreference = 'Stop'

try {
    if (-not (Get-Command dotnet -ErrorAction SilentlyContinue)) {
        throw 'dotnet SDK not found. Install .NET 8 SDK and retry.'
    }

    $scriptDir = Split-Path -Parent $PSCommandPath
    Push-Location $scriptDir

    $proj = Join-Path $scriptDir 'Resilead.Integration.Local.csproj'
    if (-not (Test-Path $proj)) {
        throw "Project not found: $proj"
    }

    $tfm = 'net8.0'
    $binDir = Join-Path $scriptDir "bin\$Configuration\$tfm"
    $dll = Join-Path $binDir 'Resilead.Integration.Local.dll'

    if (-not $NoBuild) {
        Write-Host 'Restoring packages...' -ForegroundColor Cyan
        dotnet restore $proj

        Write-Host "Building ($Configuration)..." -ForegroundColor Cyan
        dotnet build $proj -c $Configuration --nologo
    } else {
        Write-Host 'NoBuild is set: skipping restore/build' -ForegroundColor Yellow
    }

    if (-not (Test-Path $dll)) {
        throw "Binary not found: $dll. Run without -NoBuild to compile."
    }

    Write-Host "Running: $Cmd" -ForegroundColor Green
    dotnet $dll $Cmd
}
finally {
    Pop-Location | Out-Null
}

