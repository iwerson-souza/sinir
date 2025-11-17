using System.Text.Json;
using MySql.Data.MySqlClient;
using Resilead.Integration.Local.Configuration;

namespace Resilead.Integration.Local.Infrastructure;

internal sealed class StakeholderEtl
{
    private readonly AppConfig _cfg;
    private readonly Db _db;
    private readonly BrasilApiClient _br;
    public StakeholderEtl(AppConfig cfg)
    {
        _cfg = cfg;
        _db = new Db(cfg.ConnectionString);
        _br = new BrasilApiClient(cfg);
    }

    public async Task RunAsync()
    {
        Console.WriteLine($"[{DateTime.Now:O}] [stakeholder] Starting. BatchSize={_cfg.Stakeholder.BatchSize}, Drain={_cfg.Stakeholder.Drain}, DOP={_cfg.Processing.MaxDegreeOfParallelism}");
        var processed = 0;
        var pf = 0; var pj = 0; var apiHits = 0; var inserted = 0; var updated = 0; var errors = 0;
        do
        {
            // Use global processing batch size if larger
            var batchSize = Math.Max(_cfg.Stakeholder.BatchSize, _cfg.Processing.BatchSize);
            var batch = await ReadSourceBatchAsync(batchSize);
            if (batch.Count == 0)
            {
                if (processed == 0) Console.WriteLine($"[{DateTime.Now:O}] [stakeholder] No pending stakeholders to normalize.");
                break;
            }
            Console.WriteLine($"[{DateTime.Now:O}] [stakeholder] Fetched {batch.Count} record(s); processing in parallel...");

            var dop = Math.Max(1, _cfg.Processing.MaxDegreeOfParallelism);
            var lockObj = new object();
            var totalInBatch = batch.Count;
            var processedInBatch = 0;

            await Parallel.ForEachAsync(batch, new ParallelOptions { MaxDegreeOfParallelism = dop }, async (s, ct) =>
            {
                try
                {
                    if (processed > 0 && processed % 20 == 0)
                    {
                        //sleep for 2 seconds to avoid hitting API rate limits
                        Console.WriteLine($"[{DateTime.Now:O}] [stakeholder] Processed {processed} records so far, pausing briefly to avoid API rate limits...");
                        await Task.Delay(3000, ct);
                    }

                    var res = await UpsertEntidadeAsync(s);
                    lock (lockObj)
                    {
                        processed++;
                        if (res.IsPf) pf++; else pj++;
                        if (res.ApiHit) apiHits++;
                        if (res.Inserted) inserted++; else updated++;
                    }
                    var pi = Interlocked.Increment(ref processedInBatch);
                    Console.WriteLine($"[{DateTime.Now:O}] [stakeholder] {pi}/{totalInBatch} {(res.IsPf ? "PF" : "PJ")} {(res.ApiHit ? "+API" : "-API")} {(res.Inserted ? "insert" : "update")} - {s.CpfCnpj} {s.Nome}");
                }
                catch (Exception ex)
                {
                    lock (lockObj) { errors++; }
                    var pi = Interlocked.Increment(ref processedInBatch);
                    Console.WriteLine($"[{DateTime.Now:O}] [stakeholder] {pi}/{totalInBatch} ERROR {s.CpfCnpj} {s.Nome}: {ex.Message}");
                }
            });

        } while (_cfg.Stakeholder.Drain);

        Console.WriteLine($"[{DateTime.Now:O}] [stakeholder] Completed. ok={processed - errors}, errors={errors}, pf={pf}, pj={pj}, apiHits={apiHits}, inserted={inserted}, updated={updated}.");

        await ProcessUnidadesAsync();
    }

    private async Task<List<StakeholderRow>> ReadSourceBatchAsync(int limit)
    {
        // Select stakeholders not yet present in resilead.entidade (by cpf_cnpj)
        const string sql = @"SELECT s.cpf_cnpj, s.nome
                             FROM sinir.stakeholder s
                             LEFT JOIN resilead.entidade e ON e.cpf_cnpj = s.cpf_cnpj
                             WHERE e.cpf_cnpj IS NULL
                             AND s.cpf_cnpj NOT IN ('10', '', '00000000000', '00000000000000')
                             AND s.cpf_cnpj NOT IN ('01236126702402','02814351117000','10174421019292','11749421115111','22905370001000','43354257000154','56386341000010','62142872000141','63609006000180','66724151000109','66860195000158','68727042000162','70460950000184','71256334000179','75046914000192','75845150000103','76228726000148','77313348000163','78410109000194','79943599000157','80515304000120','80523826000174','82787414000177','84004832000176','84004832000176','85672330000186','86331645000122','86588800000190','86952202000159','89192989000196','89313668000100','89570771000128','90284883000100','90284883000100','91853045000164','92091515000162','92107277000136','93274990000137','93700439000108','94902842000182','98095498000118','98442444000181','99940992000102', '98204585000166')
                             ORDER BY s.cpf_cnpj
                             LIMIT @limit";
        var list = new List<StakeholderRow>();
        using var conn = await _db.OpenAsync();
        using var cmd = new MySqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@limit", limit);
        using var rdr = await cmd.ExecuteReaderAsync();
        while (await rdr.ReadAsync())
        {
            list.Add(new StakeholderRow
            {
                CpfCnpj = rdr.GetString(0),
                Nome = rdr.GetString(1)
            });
        }
        return list;
    }

    private async Task<EntidadeUpsertResult> UpsertEntidadeAsync(StakeholderRow s)
    {
        var cpfCnpj = Normalization.OnlyDigits(s.CpfCnpj);
        var nome = Normalization.Clean(s.Nome);
        var tipoPessoa = cpfCnpj.Length == 11 ? 'F' : 'J';
        var tipoPessoaStr = tipoPessoa == 'F' ? "F" : "J";

        string? uf = null, municipio = null, cep = null, logradouro = null, numero = null, complemento = null, bairro = null,
                porte = null, cnaeDesc = null;
        int? codIbge = null; long? cnae = null; DateTime? inicioAtiv = null;
        double? latitude = null, longitude = null;

        var apiHit = false;
        if (tipoPessoa == 'J')
        {
            var cnpj = cpfCnpj;
            var dto = await _br.TryGetCnpjAsync(cnpj, CancellationToken.None);
            if (dto is null)
            {
                // avoid hammering API on repeated failures
                await Task.Delay(5000);

                // For PJ, BrasilAPI data is mandatory. Do not persist; signal error to caller.
                throw new InvalidOperationException("BrasilAPI CNPJ required for PJ but unavailable or failed.");

            }
            apiHit = true;
            uf = NullIfEmpty(Normalization.Clean(dto.uf));
            municipio = NullIfEmpty(Normalization.Clean(dto.municipio));
            cep = NullIfEmpty(Normalization.Clean(dto.cep));
            logradouro = NullIfEmpty(Normalization.Clean(dto.logradouro));
            numero = NullIfEmpty(Normalization.Clean(dto.numero));
            complemento = NullIfEmpty(Normalization.Clean(dto.complemento));
            bairro = NullIfEmpty(Normalization.Clean(dto.bairro));
            porte = NullIfEmpty(Normalization.Clean(dto.porte));
            cnae = dto.cnae_fiscal;
            cnaeDesc = NullIfEmpty(Normalization.Clean(dto.cnae_fiscal_descricao));
            codIbge = dto.codigo_municipio_ibge;
            if (DateTime.TryParse(dto.data_inicio_atividade, out var dt)) inicioAtiv = dt;
            // Prefer API-provided names if present
            if (!string.IsNullOrWhiteSpace(dto.razao_social))
            {
                var rz = Normalization.Clean(dto.razao_social);
                if (!string.IsNullOrWhiteSpace(rz)) nome = rz;
            }
        }

        // Geocode latitude/longitude via CEP v2 when we have a valid CEP
        // lat/long not so prercise, but better than nothing
        // if (!string.IsNullOrWhiteSpace(cep))
        // {
        //     var cepDto = await _br.TryGetCepAsync(cep!, CancellationToken.None);
        //     if (cepDto?.location?.coordinates is { } coo)
        //     {
        //         if (double.TryParse((coo.latitude ?? string.Empty).Replace(',', '.'), System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var lat))
        //             latitude = lat;
        //         if (double.TryParse((coo.longitude ?? string.Empty).Replace(',', '.'), System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var lon))
        //             longitude = lon;
        //     }
        // }

        const string sql = @"INSERT INTO resilead.entidade
                                (cpf_cnpj, cpf_cnpj_hash, nome_razao_social, nome_fantasia, tipo_pessoa,
                                 uf, municipio, codigo_municipio_ibge, cep, logradouro, numero, complemento, bairro,
                                 latitude, longitude, porte, data_inicio_atividade, cnae_principal, cnae_principal_descricao)
                              VALUES
                                (@cpf, NULL, @nome, NULL, @tp, @uf, @mun, @ibge, @cep, @log, @num, @comp, @bairro,
                                 @lat, @lon, @porte, @inicio, @cnae, @cnaed)
                              ON DUPLICATE KEY UPDATE
                                nome_razao_social=VALUES(nome_razao_social),
                                uf=VALUES(uf), municipio=VALUES(municipio), codigo_municipio_ibge=VALUES(codigo_municipio_ibge),
                                cep=VALUES(cep), logradouro=VALUES(logradouro), numero=VALUES(numero), complemento=VALUES(complemento), bairro=VALUES(bairro),
                                latitude=VALUES(latitude), longitude=VALUES(longitude),
                                porte=VALUES(porte), data_inicio_atividade=VALUES(data_inicio_atividade), cnae_principal=VALUES(cnae_principal), cnae_principal_descricao=VALUES(cnae_principal_descricao)";

        using var conn = await _db.OpenAsync();
        using var cmd = new MySqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@cpf", cpfCnpj);
        cmd.Parameters.AddWithValue("@nome", nome);
        cmd.Parameters.AddWithValue("@tp", tipoPessoaStr);
        cmd.Parameters.AddWithValue("@uf", (object?)uf ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@mun", (object?)municipio ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@ibge", (object?)codIbge ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@cep", (object?)cep ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@log", (object?)logradouro ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@num", (object?)numero ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@comp", (object?)complemento ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@bairro", (object?)bairro ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@lat", (object?)latitude ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@lon", (object?)longitude ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@porte", (object?)porte ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@inicio", (object?)inicioAtiv ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@cnae", (object?)cnae ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@cnaed", (object?)cnaeDesc ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync();
        var inserted = cmd.LastInsertedId > 0;
        return new EntidadeUpsertResult { IsPf = tipoPessoa == 'F', ApiHit = apiHit, Inserted = inserted };
    }

    private async Task ProcessUnidadesAsync()
    {
        Console.WriteLine($"[{DateTime.Now:O}] [stakeholder:unit] Starting unidade normalization...");
        var processed = 0; var inserted = 0; var updated = 0; var skipped = 0; var errors = 0;
        do
        {
            var batchSize = Math.Max(_cfg.Stakeholder.BatchSize, _cfg.Processing.BatchSize);
            var batch = await ReadUnidadeBatchAsync(batchSize);
            if (batch.Count == 0)
            {
                if (processed == 0)
                {
                    Console.WriteLine($"[{DateTime.Now:O}] [stakeholder:unit] No pending unidades to normalize.");
                }
                break;
            }

            Console.WriteLine($"[{DateTime.Now:O}] [stakeholder:unit] Fetched {batch.Count} unidade(s).");
            var dop = Math.Max(1, _cfg.Processing.MaxDegreeOfParallelism);
            var lockObj = new object();
            var totalInBatch = batch.Count;
            var processedInBatch = 0;

            await Parallel.ForEachAsync(batch, new ParallelOptions { MaxDegreeOfParallelism = dop }, async (u, ct) =>
            {
                try
                {
                    var res = await UpsertEntidadeUnidadeAsync(u);
                    lock (lockObj)
                    {
                        processed++;
                        if (res.Inserted) inserted++;
                        else if (res.Updated) updated++;
                        else skipped++;
                    }
                    var pi = Interlocked.Increment(ref processedInBatch);
                    var action = res.Inserted ? "insert" : res.Updated ? "update" : "skip";
                    Console.WriteLine($"[{DateTime.Now:O}] [stakeholder:unit] {pi}/{totalInBatch} {action} - {u.CpfCnpj} {u.Unidade}");
                }
                catch (Exception ex)
                {
                    lock (lockObj) { errors++; }
                    var pi = Interlocked.Increment(ref processedInBatch);
                    Console.WriteLine($"[{DateTime.Now:O}] [stakeholder:unit] {pi}/{totalInBatch} ERROR {u.CpfCnpj} {u.Unidade}: {ex.Message}");
                }
            });

        } while (_cfg.Stakeholder.Drain);

        Console.WriteLine($"[{DateTime.Now:O}] [stakeholder:unit] Completed. ok={processed - errors}, errors={errors}, inserted={inserted}, updated={updated}, skipped={skipped}.");
    }

    private async Task<List<UnidadeRow>> ReadUnidadeBatchAsync(int limit)
    {
        const string sql = @"
            SELECT s.unidade, s.cpf_cnpj, s.endereco
            FROM sinir.stakeholder s
            INNER JOIN resilead.entidade e ON e.cpf_cnpj = s.cpf_cnpj
            LEFT JOIN resilead.entidade_unidade u ON u.unidade = s.unidade AND u.id_entidade = e.id_entidade
            WHERE (
                    u.id_unidade IS NULL
                    OR COALESCE(NULLIF(TRIM(u.endereco), ''), '') <> COALESCE(NULLIF(TRIM(s.endereco), ''), '')
                  )
            ORDER BY s.unidade
            LIMIT @limit";

        var list = new List<UnidadeRow>();
        using var conn = await _db.OpenAsync();
        using var cmd = new MySqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@limit", limit);
        using var rdr = await cmd.ExecuteReaderAsync();
        while (await rdr.ReadAsync())
        {
            list.Add(new UnidadeRow
            {
                Unidade = rdr.GetString(0),
                CpfCnpj = rdr.GetString(1),
                Endereco = rdr.IsDBNull(2) ? null : rdr.GetString(2)
            });
        }
        return list;
    }

    private async Task<UnidadeUpsertResult> UpsertEntidadeUnidadeAsync(UnidadeRow row)
    {
        var cpf = Normalization.OnlyDigits(row.CpfCnpj);
        var unidade = Normalization.Clean(row.Unidade);
        if (string.IsNullOrWhiteSpace(cpf) || string.IsNullOrWhiteSpace(unidade))
        {
            return new UnidadeUpsertResult { Skipped = true };
        }

        unidade = unidade.Length > 32 ? unidade[..32] : unidade;
        var endereco = NormalizeAddress(row.Endereco);

        using var conn = await _db.OpenAsync();
        long? idEntidade = null;
        const string sel = "SELECT id_entidade FROM resilead.entidade WHERE cpf_cnpj=@cpf LIMIT 1";
        using (var cmd = new MySqlCommand(sel, conn))
        {
            cmd.Parameters.AddWithValue("@cpf", cpf);
            var obj = await cmd.ExecuteScalarAsync();
            if (obj != null && obj != DBNull.Value)
            {
                idEntidade = Convert.ToInt64(obj);
            }
        }

        if (idEntidade is null)
        {
            return new UnidadeUpsertResult { Skipped = true };
        }

        const string updSql = "UPDATE resilead.entidade_unidade SET endereco=@endereco WHERE unidade=@unidade AND id_entidade=@entidade";
        using (var upd = new MySqlCommand(updSql, conn))
        {
            upd.Parameters.AddWithValue("@endereco", endereco);
            upd.Parameters.AddWithValue("@unidade", unidade);
            upd.Parameters.AddWithValue("@entidade", idEntidade.Value);
            var rows = await upd.ExecuteNonQueryAsync();
            if (rows > 0)
            {
                return new UnidadeUpsertResult { Updated = true };
            }
        }

        const string insSql = @"INSERT INTO resilead.entidade_unidade (id_entidade, unidade, endereco)
                                 VALUES (@entidade, @unidade, @endereco)";
        using (var ins = new MySqlCommand(insSql, conn))
        {
            ins.Parameters.AddWithValue("@entidade", idEntidade.Value);
            ins.Parameters.AddWithValue("@unidade", unidade);
            ins.Parameters.AddWithValue("@endereco", endereco);
            await ins.ExecuteNonQueryAsync();
            return new UnidadeUpsertResult { Inserted = true };
        }
    }

    private static string? NormalizeAddress(string? endereco)
    {
        if (string.IsNullOrWhiteSpace(endereco)) return null;
        var cleaned = Normalization.Clean(endereco);
        if (cleaned.Length == 0) return null;
        return cleaned.Length > 500 ? cleaned[..500] : cleaned;
    }

    private static string? NullIfEmpty(string? s)
    {
        return string.IsNullOrWhiteSpace(s) ? null : s.Trim();
    }

    private sealed class StakeholderRow
    {
        public string CpfCnpj { get; set; } = string.Empty;
        public string Nome { get; set; } = string.Empty;
    }

    private sealed class UnidadeRow
    {
        public string Unidade { get; set; } = string.Empty;
        public string CpfCnpj { get; set; } = string.Empty;
        public string Endereco { get; set; } = string.Empty;
    }

    private sealed class EntidadeUpsertResult
    {
        public bool IsPf { get; init; }
        public bool ApiHit { get; init; }
        public bool Inserted { get; init; }
    }

    private sealed class UnidadeUpsertResult
    {
        public bool Inserted { get; init; }
        public bool Updated { get; init; }
        public bool Skipped { get; init; }
    }
}
