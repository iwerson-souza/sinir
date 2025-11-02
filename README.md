# SINIR Local Integration

Ferramenta em .NET para coletar relatórios analíticos de MTR (Manifesto de Transporte de Resíduos) do SINIR, parsear arquivos XLSX e persistir em MySQL para análises.

## Pré‑requisitos

- .NET SDK 8.0+
- MySQL 5.7+ (ou compatível)
- Configuração em `csharp-sinir/appsettings.json` com `ConnectionStrings.MySql`

Banco e tabelas podem ser criados a partir de `csharp-sinir/ddl.sql`.

## Estrutura

- App C#: `csharp-sinir/` (entrypoint: `Program.cs`)
- Script de orquestração: `csharp-sinir/run.ps1`
- MTRs baixados: `mtrs/` (configurável via `SINIR_MTRS_DIR`)

## Configuração rápida

1) Ajuste `csharp-sinir/appsettings.json` (conexão MySQL, paralelismo etc.)
2) Opcional: rode o DDL inicial do banco: `csharp-sinir/ddl.sql`

## Uso básico

- Compilar e executar pipeline sequencial (setup → process):
  - `csharp-sinir/run.ps1 -Mode run`
- Apenas preparar URLs/loads (setup):
  - `csharp-sinir/run.ps1 -Mode setup`
- Apenas processar loads pendentes:
  - `csharp-sinir/run.ps1 -Mode process`

## Paralelismo e orquestração

O script `run.ps1` suporta paralelizar tanto o Setup quanto o Process.
A coordenação é segura pois:
- Setup insere cargas com `INSERT IGNORE` em `mtr_load` (idempotente)
- Process faz claim atômico por URL (`status` + `locked_by`), evitando colisões

Parâmetros principais:
- `-SetupInstances`  Número de instâncias paralelas para o Setup (padrão 1)
- `-Instances`       Número de instâncias paralelas para o Process (padrão 1)
- `-Configuration`    `Debug` ou `Release` (padrão `Release`)
- `-NoBuild`          Pula restore/build se já compilado

### Exemplos

- Setup em 3 instâncias e Process em 5 instâncias, em uma única chamada:
  - `csharp-sinir/run.ps1 -Mode run -SetupInstances 3 -Instances 5`

- Apenas Setup em 4 instâncias em paralelo:
  - `csharp-sinir/run.ps1 -Mode setup -SetupInstances 4`

- Apenas Process em 6 instâncias em paralelo:
  - `csharp-sinir/run.ps1 -Mode process -Instances 6`

- Reexecutar rapidamente sem compilar:
  - `csharp-sinir/run.ps1 -Mode run -SetupInstances 2 -Instances 4 -NoBuild`

## Observações

- Cada instância do Process também usa paralelismo interno configurável por `Processing.MaxDegreeOfParallelism` em `csharp-sinir/appsettings.json`. Throughput total ≈ `SetupInstances/Instances × MaxDegreeOfParallelism`.
- Logs de múltiplos processos são intercalados no terminal; o script imprime o `PID` de cada worker.
- O script trata `ExitCode` nulo de processos filhos como sucesso (para evitar falsos negativos em alguns ambientes). Se um processo realmente falhar, o ExitCode será diferente de 0 e o script reportará erro.

## Variáveis de ambiente

- `SINIR_SOLUTION_ROOT`  Raiz da solução (auto-definida por `run.ps1`)
- `SINIR_MTRS_DIR`       Diretório onde os XLSX são salvos (auto-definido para `<raiz>/mtrs`)

## Caminhos úteis

- Script: `csharp-sinir/run.ps1`
- Config: `csharp-sinir/appsettings.json`
- DDL: `csharp-sinir/ddl.sql`
- Entrada: `csharp-sinir/Program.cs`
