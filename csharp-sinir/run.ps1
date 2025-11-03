Param(
    [ValidateSet('run','setup','process')]
    [string]$Mode = 'run',

    [ValidateSet('Debug','Release')]
    [string]$Configuration = 'Release',

    [int]$SetupInstances = 1,

    [int]$Instances = 1,

    [switch]$NoBuild,

    [ValidateSet('disk','memory')]
    [string]$MtrMode = 'disk'
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

    # Define diretório de saída dos MTRs na raiz da solução e modo de processamento
    $solutionDir = Split-Path -Parent $scriptDir
    $env:SINIR_SOLUTION_ROOT = $solutionDir
    $env:SINIR_PROCESS_MODE = $MtrMode
    if ($MtrMode -eq 'disk') {
        $env:SINIR_MTRS_DIR = Join-Path $solutionDir 'mtrs'
        New-Item -ItemType Directory -Path $env:SINIR_MTRS_DIR -Force | Out-Null
        Write-Host "MTRS directory: $env:SINIR_MTRS_DIR" -ForegroundColor Cyan
    } else {
        # Clear dir var to avoid accidental use in memory mode
        $env:SINIR_MTRS_DIR = $null
        Write-Host "MTR processing mode: memory (no disk writes)" -ForegroundColor Cyan
    }

    # Orquestração multi-instância (Setup único + N Process)
    if ($Mode -eq 'run' -and $SetupInstances -gt 1) {
        # Setup orquestrado com múltiplas instâncias
        Write-Host "Iniciando $SetupInstances instância(s) do setup em paralelo..." -ForegroundColor Green
        $setupProcs = @()
        for ($i = 1; $i -le $SetupInstances; $i++) {
            $p = Start-Process -FilePath 'dotnet' -ArgumentList @($dll, 'setup') -PassThru -NoNewWindow
            $setupProcs += $p
            Write-Host ("[Setup #{0}] PID={1} iniciado" -f $i, $p.Id) -ForegroundColor Cyan
        }
        Wait-Process -Id ($setupProcs | ForEach-Object Id)
        $failed = $false
        foreach ($p in $setupProcs) {
            $code = $null
            try { $p.Refresh() | Out-Null; $code = $p.ExitCode } catch { $code = $null }
            if ($null -eq $code) { $code = 0 }
            if ($code -ne 0) {
                Write-Host ("[Setup PID={0}] finalizou com erro (ExitCode={1})" -f $p.Id, $code) -ForegroundColor Red
                $failed = $true
            } else {
                Write-Host ("[Setup PID={0}] finalizado com sucesso (ExitCode=0)" -f $p.Id) -ForegroundColor Green
            }
        }
        if ($failed) { throw "Uma ou mais instâncias de setup falharam." }

        # Processamento após setup paralelo
        if ($Instances -gt 1) {
            Write-Host "Iniciando $Instances instância(s) do process em paralelo..." -ForegroundColor Green
            $procs = @()
            for ($i = 1; $i -le $Instances; $i++) {
                $p = Start-Process -FilePath 'dotnet' -ArgumentList @($dll, 'process') -PassThru -NoNewWindow
                $procs += $p
                Write-Host ("[Worker #{0}] PID={1} iniciado" -f $i, $p.Id) -ForegroundColor Cyan
            }
            Wait-Process -Id ($procs | ForEach-Object Id)
            $failed = $false
            foreach ($p in $procs) {
                $code = $null
                try { $p.Refresh() | Out-Null; $code = $p.ExitCode } catch { $code = $null }
                if ($null -eq $code) { $code = 0 }
                if ($code -ne 0) {
                    Write-Host ("[Worker PID={0}] finalizou com erro (ExitCode={1})" -f $p.Id, $code) -ForegroundColor Red
                    $failed = $true
                } else {
                    Write-Host ("[Worker PID={0}] finalizado com sucesso (ExitCode=0)" -f $p.Id) -ForegroundColor Green
                }
            }
            if ($failed) { throw "Uma ou mais instâncias falharam." }
        } else {
            Write-Host "Executando process (1 instância)..." -ForegroundColor Green
            dotnet $dll process
        }
    } elseif ($Mode -eq 'run' -and $Instances -gt 1) {
        Write-Host "Executando setup (única vez)..." -ForegroundColor Green
        dotnet $dll setup

        Write-Host "Iniciando $Instances instância(s) do process em paralelo..." -ForegroundColor Green
        $procs = @()
        for ($i = 1; $i -le $Instances; $i++) {
            $p = Start-Process -FilePath 'dotnet' -ArgumentList @($dll, 'process') -PassThru -NoNewWindow
            $procs += $p
            Write-Host ("[Worker #{0}] PID={1} iniciado" -f $i, $p.Id) -ForegroundColor Cyan
        }

        # Aguarda finalização
        Wait-Process -Id ($procs | ForEach-Object Id)

        # Verifica códigos de saída
        $failed = $false
        foreach ($p in $procs) {
            $code = $null
            try { $p.Refresh() | Out-Null; $code = $p.ExitCode } catch { $code = $null }
            if ($null -eq $code) { $code = 0 } # Em alguns ambientes ExitCode pode vir nulo; assume sucesso
            if ($code -ne 0) {
                Write-Host ("[Worker PID={0}] finalizou com erro (ExitCode={1})" -f $p.Id, $code) -ForegroundColor Red
                $failed = $true
            } else {
                Write-Host ("[Worker PID={0}] finalizado com sucesso (ExitCode=0)" -f $p.Id) -ForegroundColor Green
            }
        }
        if ($failed) { throw "Uma ou mais instâncias falharam." }
    }
    elseif ($Mode -eq 'setup' -and $SetupInstances -gt 1) {
        Write-Host "Iniciando $SetupInstances instância(s) do setup em paralelo..." -ForegroundColor Green
        $setupProcs = @()
        for ($i = 1; $i -le $SetupInstances; $i++) {
            $p = Start-Process -FilePath 'dotnet' -ArgumentList @($dll, 'setup') -PassThru -NoNewWindow
            $setupProcs += $p
            Write-Host ("[Setup #{0}] PID={1} iniciado" -f $i, $p.Id) -ForegroundColor Cyan
        }
        Wait-Process -Id ($setupProcs | ForEach-Object Id)
        $failed = $false
        foreach ($p in $setupProcs) {
            $code = $null
            try { $p.Refresh() | Out-Null; $code = $p.ExitCode } catch { $code = $null }
            if ($null -eq $code) { $code = 0 }
            if ($code -ne 0) {
                Write-Host ("[Setup PID={0}] finalizou com erro (ExitCode={1})" -f $p.Id, $code) -ForegroundColor Red
                $failed = $true
            } else {
                Write-Host ("[Setup PID={0}] finalizado com sucesso (ExitCode=0)" -f $p.Id) -ForegroundColor Green
            }
        }
        if ($failed) { throw "Uma ou mais instâncias de setup falharam." }
    }
    elseif ($Mode -eq 'process' -and $Instances -gt 1) {
        Write-Host "Iniciando $Instances instância(s) do process em paralelo..." -ForegroundColor Green
        $procs = @()
        for ($i = 1; $i -le $Instances; $i++) {
            $p = Start-Process -FilePath 'dotnet' -ArgumentList @($dll, 'process') -PassThru -NoNewWindow
            $procs += $p
            Write-Host ("[Worker #{0}] PID={1} iniciado" -f $i, $p.Id) -ForegroundColor Cyan
        }
        Wait-Process -Id ($procs | ForEach-Object Id)
        $failed = $false
        foreach ($p in $procs) {
            $code = $null
            try { $p.Refresh() | Out-Null; $code = $p.ExitCode } catch { $code = $null }
            if ($null -eq $code) { $code = 0 }
            if ($code -ne 0) {
                Write-Host ("[Worker PID={0}] finalizou com erro (ExitCode={1})" -f $p.Id, $code) -ForegroundColor Red
                $failed = $true
            } else {
                Write-Host ("[Worker PID={0}] finalizado com sucesso (ExitCode=0)" -f $p.Id) -ForegroundColor Green
            }
        }
        if ($failed) { throw "Uma ou mais instâncias falharam." }
    }
    else {
        Write-Host "Executando: $Mode" -ForegroundColor Green
        dotnet $dll $Mode
    }
}
catch {
    Write-Error $_
    exit 1
}
finally {
    Pop-Location | Out-Null
}
