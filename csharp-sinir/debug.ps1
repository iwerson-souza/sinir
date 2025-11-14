Param(
    [ValidateSet('run','setup','process','address')]
    [string]$Mode = 'run',

    [switch]$NoBuild
)

$ErrorActionPreference = 'Stop'

try {
    if (-not (Get-Command dotnet -ErrorAction SilentlyContinue)) {
        throw 'dotnet SDK not found. Install .NET 8 SDK and retry.'
    }

    $scriptDir = Split-Path -Parent $PSCommandPath
    Push-Location $scriptDir

    $proj = Join-Path $scriptDir 'Sinir.Integration.Local.csproj'
    if (-not (Test-Path $proj)) {
        throw "Project not found: $proj"
    }

    $configuration = 'Debug'
    $tfm = 'net8.0'
    $binDir = Join-Path $scriptDir 'bin'
    $binDir = Join-Path $binDir $configuration
    $binDir = Join-Path $binDir $tfm
    $dll = Join-Path $binDir 'Sinir.Integration.Local.dll'

    if (-not $NoBuild) {
        Write-Host 'Restoring packages...' -ForegroundColor Cyan
        dotnet restore $proj

        Write-Host "Building ($configuration)..." -ForegroundColor Cyan
        dotnet build $proj -c $configuration --nologo
    } else {
        Write-Host 'NoBuild is set: skipping restore/build' -ForegroundColor Yellow
    }

    if (-not (Test-Path $dll)) {
        throw "Binary not found: $dll. Run without -NoBuild to compile."
    }

    dotnet $dll $Mode
}
finally {
    Pop-Location | Out-Null
}
