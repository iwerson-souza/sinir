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
        Console.WriteLine($"[stakeholder] Starting. BatchSize={_cfg.Stakeholder.BatchSize}, Drain={_cfg.Stakeholder.Drain}");
        var processed = 0;
        var pf = 0; var pj = 0; var apiHits = 0; var inserted = 0; var updated = 0; var errors = 0;
        do
        {
            var batch = await ReadSourceBatchAsync(_cfg.Stakeholder.BatchSize);
            if (batch.Count == 0)
            {
                if (processed == 0) Console.WriteLine("[stakeholder] No pending stakeholders to normalize.");
                break;
            }
            Console.WriteLine($"[stakeholder] Fetched {batch.Count} record(s) to process...");
            foreach (var s in batch)
            {
                try
                {
                    var res = await UpsertEntidadeAsync(s);
                    processed++;
                    if (res.IsPf) pf++; else pj++;
                    if (res.ApiHit) apiHits++;
                    if (res.Inserted) inserted++; else updated++;
                    Console.WriteLine($"[stakeholder] {(res.IsPf ? "PF" : "PJ")} {(res.ApiHit ? "+API" : "-API")} {(res.Inserted ? "insert" : "update")} - {s.CpfCnpj} {s.Nome}");
                }
                catch (Exception ex)
                {
                    errors++;
                    Console.WriteLine($"[stakeholder] ERROR {s.CpfCnpj} {s.Nome}: {ex.Message}");
                }
            }

        } while (_cfg.Stakeholder.Drain);

        Console.WriteLine($"[stakeholder] Completed. ok={processed-errors}, errors={errors}, pf={pf}, pj={pj}, apiHits={apiHits}, inserted={inserted}, updated={updated}.");
    }

    private async Task<List<StakeholderRow>> ReadSourceBatchAsync(int limit)
    {
        // Select stakeholders not yet present in resilead.entidade (by cpf_cnpj)
        const string sql = @"SELECT s.cpf_cnpj, s.nome
                             FROM sinir.stakeholder s
                             LEFT JOIN resilead.entidade e ON e.cpf_cnpj = s.cpf_cnpj
                             WHERE e.cpf_cnpj IS NULL
                             AND s.cpf_cnpj NOT IN ('10', '', '00000000000', '00000000000000')
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
            if (dto is not null)
            {
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

    private static string? NullIfEmpty(string? s)
    {
        return string.IsNullOrWhiteSpace(s) ? null : s.Trim();
    }

    private sealed class StakeholderRow
    {
        public string CpfCnpj { get; set; } = string.Empty;
        public string Nome { get; set; } = string.Empty;
    }

    private sealed class EntidadeUpsertResult
    {
        public bool IsPf { get; init; }
        public bool ApiHit { get; init; }
        public bool Inserted { get; init; }
    }
}
