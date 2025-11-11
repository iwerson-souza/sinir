Resilead Integration Console

Overview
- Console app to normalize SINIR data into the `resilead` schema.
- Commands: `ref-load`, `stakeholder`, `mtr`.

Prerequisites
- .NET 8 SDK installed (`dotnet --version`).
- MySQL server reachable with a user that can read `sinir` and write to `resilead`.

Quick Start
- Configure connection string in `csharp-resilead/appsettings.json` under `ConnectionStrings:MySql`.
- Seed reference data: `csharp-resilead/run.ps1 -Cmd ref-load`.
- Normalize stakeholders: `csharp-resilead/run.ps1 -Cmd stakeholder`.
- Normalize MTRs: `csharp-resilead/run.ps1 -Cmd mtr`.

Commands
- `ref-load`
  - Inserts canonical reference data from `csharp-resilead/data/*.json` into `resilead` (`INSERT IGNORE`).
  - Files: `situacao.json`, `tipoManifesto.json`, `tratamento.json`, `unidade.json`, `classe.json`, `residuos.json`.
- `stakeholder`
  - Reads batches of missing entities from `sinir.stakeholder` and upserts into `resilead.entidade` only.
  - Enriches PJ with BrasilAPI CNPJ; PF has no external call.
  - Does not touch `tipo_entidade`, `entidade_motorista`, `entidade_veiculo`, or `entidade_responsavel`.
- `mtr`
  - Reads batches from `sinir.mtr` and normalizes into `resilead.registro` and related tables.
  - Ensures reference rows exist (insert-if-missing). Does not create entities; uses those from the stakeholder process.
  - Inserts `tipo_entidade` only if missing (no updates).
  - Inserts `entidade_responsavel` (dedupe by `id_entidade + tipo + nome`).
  - Inserts `entidade_veiculo` (insert-ignore by `(id_entidade, placa)`).
  - Inserts `entidade_motorista`; for TRANSPORTADORA PF, sets `proprio=true` when similarity(nome motorista, nome entidade) ≥ 0.80; PJ → `NULL`.
  - Upserts `registro` by `numero_mtr`, updating only: `tipo_manifesto`, `data_emissao`, `data_recebimento`, `situacao`, `justificativa`, `tratamento`, `numero_cdf`.
  - Inserts `registro_residuo` referencing `codigo_residuo`; resolves unidade by sigla/descrição with fallback to `residuo.codigo_unidade_padrao`.
  - On success, writes snapshot to `sinir.mtr_history` and deletes from `sinir.mtr` atomically.

Configuration
- File: `csharp-resilead/appsettings.json`.
- Sections:
  - `ConnectionStrings:MySql`: MySQL connection string.
  - `RefLoad:DataDir`: folder for `data/*.json` (default `data`).
  - `Stakeholder:BatchSize` (default 1), `Stakeholder:Drain` (default false).
  - `Mtr:BatchSize` (default 1), `Mtr:Drain` (default false).
  - `Processing:UserAgent`, `Processing:Http` (timeouts, connections).

Data and DDL
- Reference data: `csharp-resilead/data/`.
- Database schema: `csharp-resilead/ddl.sql` (uses natural PK for `residuo.codigo_residuo`).

Normalization Rules (Highlights)
- Trim strings and compare case-insensitively for lookups.
- `tratamento`: if trimmed empty, treated as NULL.
- `residuo.codigo_residuo`: digits from left of description or `resCodigoIbama`; `perigoso=true` when contains `(*)`.

Running
- Script: `csharp-resilead/run.ps1`.
- Examples:
  - `csharp-resilead/run.ps1 -Cmd ref-load`
  - `csharp-resilead/run.ps1 -Cmd stakeholder`
  - `csharp-resilead/run.ps1 -Cmd mtr`

Troubleshooting
- Ensure MySQL user has permissions on schemas `sinir` (read) and `resilead` (write/create).
- If BrasilAPI is unavailable, PJ enrichment is skipped gracefully.

