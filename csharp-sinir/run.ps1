Param(
    [ValidateSet('run','setup','process')]
    [string]$Mode = 'run',

    [ValidateSet('Debug','Release')]
    [string]$Configuration = 'Release',

    [switch]$NoBuild
)

$ErrorActionPreference = 'Stop'

try {
    if (-not (Get-Command dotnet -ErrorAction SilentlyContinue)) {
        throw 'dotnet SDK não encontrado. Instale o .NET 8 SDK e tente novamente.'
    }

    $scriptDir = Split-Path -Parent $PSCommandPath
    Push-Location $scriptDir

    $proj = Join-Path $scriptDir 'Sinir.Integration.Local.csproj'
    if (-not (Test-Path $proj)) {
        throw "Projeto não encontrado: $proj"
    }

    $tfm = 'net8.0'
    $binDir = Join-Path $scriptDir "bin\$Configuration\$tfm"
    $dll = Join-Path $binDir 'Sinir.Integration.Local.dll'

    if (-not $NoBuild) {
        Write-Host "Restaurando pacotes..." -ForegroundColor Cyan
        dotnet restore $proj

        Write-Host "Compilando ($Configuration)..." -ForegroundColor Cyan
        dotnet build $proj -c $Configuration --nologo
    } else {
        Write-Host "NoBuild ativo: pulando restore/build" -ForegroundColor Yellow
    }

    if (-not (Test-Path $dll)) {
        throw "Binário não encontrado: $dll. Execute sem -NoBuild para compilar."
    }

    # Define diretório de saída dos MTRs na raiz da solução
    $solutionDir = Split-Path -Parent $scriptDir
    $env:SINIR_SOLUTION_ROOT = $solutionDir
    $env:SINIR_MTRS_DIR = Join-Path $solutionDir 'mtrs'
    New-Item -ItemType Directory -Path $env:SINIR_MTRS_DIR -Force | Out-Null
    Write-Host "MTRS directory: $env:SINIR_MTRS_DIR" -ForegroundColor Cyan

    Write-Host "Executando: $Mode" -ForegroundColor Green
    dotnet $dll $Mode
}
catch {
    Write-Error $_
    exit 1
}
finally {
    Pop-Location | Out-Null
}
