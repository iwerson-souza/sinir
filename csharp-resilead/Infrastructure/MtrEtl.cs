using System.Globalization;
using System.Text.Json;
using MySql.Data.MySqlClient;
using Resilead.Integration.Local.Configuration;

namespace Resilead.Integration.Local.Infrastructure;

internal sealed class MtrEtl
{
    private static readonly CultureInfo PtBrCulture = CultureInfo.GetCultureInfo("pt-BR");
    private static readonly string[] SupportedDateFormats = new[]
    {
        "dd/MM/yyyy",
        "d/M/yyyy",
        "dd/MM/yyyy HH:mm:ss",
        "d/M/yyyy HH:mm:ss",
        "yyyy-MM-dd",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-ddTHH:mm:ss",
        "yyyy-MM-ddTHH:mm:ssZ"
    };

    private readonly AppConfig _cfg;
    private readonly Db _db;
    public MtrEtl(AppConfig cfg)
    {
        _cfg = cfg;
        _db = new Db(cfg.ConnectionString);
    }

    public async Task RunAsync()
    {
        Console.WriteLine($"[{DateTime.Now:O}] [mtr] Starting. BatchSize={_cfg.Mtr.BatchSize}, Drain={_cfg.Mtr.Drain}");
        var processed = 0; var errors = 0; var rounds = 0;
        do
        {
            var batch = await ReadMtrBatchAsync(_cfg.Mtr.BatchSize);
            if (batch.Count == 0)
            {
                if (processed == 0) Console.WriteLine($"[{DateTime.Now:O}] [mtr] No pending MTRs to normalize.");
                break;
            }
            rounds++;
            Console.WriteLine($"[{DateTime.Now:O}] [mtr] Round {rounds}: fetched {batch.Count} record(s).");
            var totalInBatch = batch.Count; var pi = 0;
            foreach (var m in batch)
            {
                try
                {
                    var ok = await NormalizeOneAsync(m);
                    if (ok)
                    {
                        await MoveToHistoryAndDeleteAsync(m);
                        processed++;
                    }
                }
                catch (Exception ex)
                {
                    errors++;
                    Console.WriteLine($"[{DateTime.Now:O}] [mtr] ERROR {m.Numero}: {ex.Message}");
                    try
                    {
                        await MoveToErrorBucketAsync(m, ex);
                    }
                    catch (Exception errorBucketEx)
                    {
                        Console.WriteLine($"[{DateTime.Now:O}] [mtr] FAILED to persist error for {m.Numero}: {errorBucketEx.Message}");
                    }
                }
                finally
                {
                    pi++;
                    if (pi % 10 == 0 || pi == totalInBatch)
                    {
                        Console.WriteLine($"[{DateTime.Now:O}] [mtr] Progress {pi}/{totalInBatch} this round; total processed={processed}, errors={errors}.");
                    }
                }
            }
        } while (_cfg.Mtr.Drain);

        Console.WriteLine($"[{DateTime.Now:O}] [mtr] Completed. ok={processed}, errors={errors}, rounds={rounds}.");
    }

    private async Task<List<MtrRow>> ReadMtrBatchAsync(int limit)
    {
        const string sql = @"SELECT numero, tipo_manifesto, responsavel_emissao, tem_mtr_complementar, numero_mtr_provisorio,
                                     data_emissao, data_recebimento, situacao, responsavel_recebimento, justificativa, tratamento,
                                     numero_cdf, residuos, residuos_codigo, residuos_classe, gerador, transportador, destinador,
                                     gerador_cpf_cnpj, transportador_cpf_cnpj, destinador_cpf_cnpj, created_by, created_dt
                              FROM sinir.mtr
                              ORDER BY numero
                              LIMIT @limit";
        var list = new List<MtrRow>();
        using var conn = await _db.OpenAsync();
        using var cmd = new MySqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@limit", limit);
        using var rdr = await cmd.ExecuteReaderAsync();
        while (await rdr.ReadAsync())
        {
            var r = new MtrRow
            {
                Numero = rdr.GetString(0),
                TipoManifesto = rdr.GetString(1),
                ResponsavelEmissao = rdr.GetString(2),
                TemMtrComplementar = rdr.IsDBNull(3) ? null : rdr.GetString(3),
                NumeroMtrProvisorio = rdr.IsDBNull(4) ? null : rdr.GetString(4),
                DataEmissao = rdr.GetString(5),
                DataRecebimento = rdr.IsDBNull(6) ? null : rdr.GetString(6),
                Situacao = rdr.GetString(7),
                ResponsavelRecebimento = rdr.IsDBNull(8) ? null : rdr.GetString(8),
                Justificativa = rdr.IsDBNull(9) ? null : rdr.GetString(9),
                Tratamento = rdr.GetString(10),
                NumeroCdf = rdr.IsDBNull(11) ? null : rdr.GetString(11),
                Residuos = rdr.GetString(12),
                ResiduosCodigo = rdr.GetString(13),
                ResiduosClasse = rdr.GetString(14),
                Gerador = rdr.GetString(15),
                Transportador = rdr.GetString(16),
                Destinador = rdr.GetString(17),
                GeradorCpfCnpj = rdr.GetString(18),
                TransportadorCpfCnpj = rdr.GetString(19),
                DestinadorCpfCnpj = rdr.GetString(20),
                CreatedBy = rdr.GetString(21),
                CreatedDt = rdr.GetDateTime(22)
            };
            list.Add(r);
        }
        return list;
    }

    private async Task<bool> NormalizeOneAsync(MtrRow m)
    {
        using var conn = await _db.OpenAsync();
        using var tx = await conn.BeginTransactionAsync();
        try
        {
            // Ensure reference data (insert if missing)
            var tipoManifestoId = await EnsureTipoManifestoAsync(conn, tx, Normalization.Clean(m.TipoManifesto));
            var situacaoId = await EnsureSituacaoAsync(conn, tx, Normalization.Clean(m.Situacao));
            var tratamentoId = await EnsureTratamentoAsync(conn, tx, Normalization.Clean(m.Tratamento)); // may return null if blank

            // Resolve entidades (must exist from Stakeholder process)
            var idGerador = await GetEntidadeIdByCpfCnpjAsync(conn, tx, m.GeradorCpfCnpj);
            var idTransportador = await GetEntidadeIdByCpfCnpjAsync(conn, tx, m.TransportadorCpfCnpj);
            var idDestinador = await GetEntidadeIdByCpfCnpjAsync(conn, tx, m.DestinadorCpfCnpj);
            if (idGerador is null || idTransportador is null || idDestinador is null)
                throw new InvalidOperationException("Missing entidade(s) for MTR. Run stakeholder ETL first.");

            // Insert-only tipo_entidade relations if missing
            await EnsureTipoEntidadeAsync(conn, tx, idGerador.Value, "GERADOR");
            await EnsureTipoEntidadeAsync(conn, tx, idTransportador.Value, "TRANSPORTADOR");
            await EnsureTipoEntidadeAsync(conn, tx, idDestinador.Value, "DESTINADOR");

            // Responsáveis (insert-if-missing, dedupe by id_entidade + tipo + nome normalized)
            var idRespEmissao = await EnsureResponsavelAsync(conn, tx, idGerador.Value, "EMISSAO", m.ResponsavelEmissao);
            long? idRespReceb = null;
            if (!string.IsNullOrWhiteSpace(m.ResponsavelRecebimento))
            {
                idRespReceb = await EnsureResponsavelAsync(conn, tx, idDestinador.Value, "RECEBIMENTO", m.ResponsavelRecebimento!);
            }

            // Upsert registro (update limited subset on reprocess)
            await UpsertRegistroAsync(conn, tx, m, tipoManifestoId, situacaoId, tratamentoId, idGerador.Value, idTransportador.Value, idDestinador.Value, idRespEmissao, idRespReceb);

            // Motorista/Veículo (from transportador JSON)
            await EnsureMotoristaVeiculoAsync(conn, tx, idTransportador.Value, m.Transportador, idTransportador.Value);

            // Resíduos
            await InsertRegistroResiduosAsync(conn, tx, m);

            await tx.CommitAsync();
            return true;
        }
        catch (Exception ex)
        {
            await tx.RollbackAsync();
            Console.WriteLine($"[mtr] Failed for {m.Numero}: {ex.Message}");
            return false;
        }
    }

    private static async Task<long?> GetEntidadeIdByCpfCnpjAsync(MySqlConnection conn, MySqlTransaction tx, string cpfCnpj)
    {
        const string sql = "SELECT id_entidade FROM resilead.entidade WHERE cpf_cnpj=@c";
        using var cmd = new MySqlCommand(sql, conn, tx);
        cmd.Parameters.AddWithValue("@c", Normalization.OnlyDigits(cpfCnpj));
        var obj = await cmd.ExecuteScalarAsync();
        return obj is null || obj == DBNull.Value ? null : Convert.ToInt64(obj);
    }

    private static async Task<int> EnsureTipoManifestoAsync(MySqlConnection conn, MySqlTransaction tx, string descricao)
    {
        const string ins = "INSERT IGNORE INTO resilead.tipo_manifesto(descricao) VALUES (@d)";
        using (var ic = new MySqlCommand(ins, conn, tx)) { ic.Parameters.AddWithValue("@d", descricao); await ic.ExecuteNonQueryAsync(); }
        const string sel = "SELECT id_tipo_manifesto FROM resilead.tipo_manifesto WHERE descricao=@d";
        using var sc = new MySqlCommand(sel, conn, tx); sc.Parameters.AddWithValue("@d", descricao);
        return Convert.ToInt32(await sc.ExecuteScalarAsync());
    }

    private static async Task<int> EnsureSituacaoAsync(MySqlConnection conn, MySqlTransaction tx, string descricao)
    {
        const string ins = "INSERT IGNORE INTO resilead.situacao(descricao) VALUES (@d)";
        using (var ic = new MySqlCommand(ins, conn, tx)) { ic.Parameters.AddWithValue("@d", descricao); await ic.ExecuteNonQueryAsync(); }
        const string sel = "SELECT id_situacao FROM resilead.situacao WHERE descricao=@d";
        using var sc = new MySqlCommand(sel, conn, tx); sc.Parameters.AddWithValue("@d", descricao);
        return Convert.ToInt32(await sc.ExecuteScalarAsync());
    }

    private static async Task<int?> EnsureTratamentoAsync(MySqlConnection conn, MySqlTransaction tx, string descricao)
    {
        if (string.IsNullOrWhiteSpace(descricao)) return null;
        descricao = descricao.Trim();
        if (descricao.Length == 0) return null;
        const string ins = "INSERT IGNORE INTO resilead.tratamento(descricao) VALUES (@d)";
        using (var ic = new MySqlCommand(ins, conn, tx)) { ic.Parameters.AddWithValue("@d", descricao); await ic.ExecuteNonQueryAsync(); }
        const string sel = "SELECT id_tratamento FROM resilead.tratamento WHERE descricao=@d";
        using var sc = new MySqlCommand(sel, conn, tx); sc.Parameters.AddWithValue("@d", descricao);
        return Convert.ToInt32(await sc.ExecuteScalarAsync());
    }

    private static async Task EnsureTipoEntidadeAsync(MySqlConnection conn, MySqlTransaction tx, long idEntidade, string tipo)
    {
        const string ins = @"INSERT IGNORE INTO resilead.tipo_entidade(id_entidade, tipo) VALUES (@e, @t)";
        using var cmd = new MySqlCommand(ins, conn, tx);
        cmd.Parameters.AddWithValue("@e", idEntidade);
        cmd.Parameters.AddWithValue("@t", tipo);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<long> EnsureResponsavelAsync(MySqlConnection conn, MySqlTransaction tx, long idEntidade, string tipo, string nome)
    {
        var normNome = Normalization.NormalizeName(nome);
        const string sel = @"SELECT id_responsavel FROM resilead.entidade_responsavel
                             WHERE id_entidade=@e AND tipo_responsavel=@t AND UPPER(TRIM(nome))=UPPER(TRIM(@n))";
        using (var sc = new MySqlCommand(sel, conn, tx))
        {
            sc.Parameters.AddWithValue("@e", idEntidade);
            sc.Parameters.AddWithValue("@t", tipo);
            sc.Parameters.AddWithValue("@n", normNome);
            var obj = await sc.ExecuteScalarAsync();
            if (obj is not null && obj != DBNull.Value) return Convert.ToInt64(obj);
        }
        const string ins = @"INSERT INTO resilead.entidade_responsavel(id_entidade, nome, tipo_responsavel)
                             VALUES (@e, @n, @t)";
        using (var ic = new MySqlCommand(ins, conn, tx))
        {
            ic.Parameters.AddWithValue("@e", idEntidade);
            ic.Parameters.AddWithValue("@n", nome);
            ic.Parameters.AddWithValue("@t", tipo);
            await ic.ExecuteNonQueryAsync();
            return ic.LastInsertedId;
        }
    }

    private static DateTime? ParseDate(string? s)
    {
        if (string.IsNullOrWhiteSpace(s)) return null;
        var value = s.Trim();
        if (DateTime.TryParseExact(value, SupportedDateFormats, PtBrCulture, DateTimeStyles.AssumeLocal, out var dt))
        {
            return dt;
        }
        if (DateTime.TryParse(value, PtBrCulture, DateTimeStyles.AssumeLocal, out dt)) return dt;
        if (DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out dt)) return dt;
        return null;
    }

    private static async Task UpsertRegistroAsync(MySqlConnection conn, MySqlTransaction tx, MtrRow m, int idTipoManifesto, int idSituacao, int? idTratamento,
        long idGerador, long idTransportador, long idDestinador, long idRespEmissao, long? idRespReceb)
    {
        const string ins = @"INSERT INTO resilead.registro
            (numero_mtr, id_tipo_manifesto, id_gerador, id_transportador, id_destinador, id_entidade_resp_emissao, id_entidade_resp_recebimento,
             id_situacao, id_tratamento, numero_cdf, justificativa, data_emissao, data_recebimento)
          VALUES
            (@num, @tm, @g, @t, @d, @re, @rr, @sit, @trat, @cdf, @just, @de, @dr)
          ON DUPLICATE KEY UPDATE
            id_tipo_manifesto=VALUES(id_tipo_manifesto),
            data_emissao=VALUES(data_emissao),
            data_recebimento=VALUES(data_recebimento),
            id_situacao=VALUES(id_situacao),
            justificativa=VALUES(justificativa),
            id_tratamento=VALUES(id_tratamento),
            numero_cdf=VALUES(numero_cdf)";
        using var cmd = new MySqlCommand(ins, conn, tx);
        cmd.Parameters.AddWithValue("@num", m.Numero);
        cmd.Parameters.AddWithValue("@tm", idTipoManifesto);
        cmd.Parameters.AddWithValue("@g", idGerador);
        cmd.Parameters.AddWithValue("@t", idTransportador);
        cmd.Parameters.AddWithValue("@d", idDestinador);
        cmd.Parameters.AddWithValue("@re", idRespEmissao);
        cmd.Parameters.AddWithValue("@rr", (object?)idRespReceb ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@sit", idSituacao);
        cmd.Parameters.AddWithValue("@trat", (object?)idTratamento ?? DBNull.Value);
        var numeroCdf = Normalization.CleanOrNull(m.NumeroCdf);
        cmd.Parameters.AddWithValue("@cdf", (object?)numeroCdf ?? DBNull.Value);
        var justificativa = Normalization.CleanOrNull(m.Justificativa);
        cmd.Parameters.AddWithValue("@just", (object?)justificativa ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@de", (object?)ParseDate(m.DataEmissao) ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@dr", (object?)ParseDate(m.DataRecebimento) ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task EnsureMotoristaVeiculoAsync(MySqlConnection conn, MySqlTransaction tx, long idTransportador, string transportadorJson, long idEntidadeTransportador)
    {
        using var doc = JsonDocument.Parse(transportadorJson);
        var root = doc.RootElement;
        var nomeEntidade = root.TryGetProperty("Nome", out var ne) ? ne.GetString() ?? string.Empty : string.Empty;
        var motoristaNome = root.TryGetProperty("Motorista", out var mo) ? mo.GetString() : null;
        var placa = root.TryGetProperty("PlacaVeiculo", out var pl) ? pl.GetString() : null;

        // Veículo
        if (!string.IsNullOrWhiteSpace(placa))
        {
            const string iv = "INSERT IGNORE INTO resilead.entidade_veiculo(id_entidade, placa_veiculo) VALUES (@e, @p)";
            using var vc = new MySqlCommand(iv, conn, tx);
            vc.Parameters.AddWithValue("@e", idEntidadeTransportador);
            vc.Parameters.AddWithValue("@p", placa!.Trim().ToUpperInvariant());
            await vc.ExecuteNonQueryAsync();
        }

        // Motorista
        if (!string.IsNullOrWhiteSpace(motoristaNome))
        {
            // proprio: only for PF transportadora; if not PF, set NULL
            bool? proprio = null;
            var tp = await GetTipoPessoaAsync(conn, tx, idEntidadeTransportador);
            if (tp == 'F')
            {
                var sim = Normalization.Similarity(motoristaNome!, nomeEntidade);
                proprio = sim >= 0.80;
            }

            const string sel = @"SELECT id_motorista FROM resilead.entidade_motorista WHERE id_entidade=@e AND UPPER(TRIM(nome))=UPPER(TRIM(@n))";
            using var sc = new MySqlCommand(sel, conn, tx);
            sc.Parameters.AddWithValue("@e", idEntidadeTransportador);
            sc.Parameters.AddWithValue("@n", Normalization.NormalizeName(motoristaNome!));
            var exists = await sc.ExecuteScalarAsync();
            if (exists is null || exists == DBNull.Value)
            {
                const string ins = @"INSERT INTO resilead.entidade_motorista(id_entidade, nome, proprio)
                                     VALUES (@e, @n, @p)";
                using var ic = new MySqlCommand(ins, conn, tx);
                ic.Parameters.AddWithValue("@e", idEntidadeTransportador);
                ic.Parameters.AddWithValue("@n", motoristaNome);
                ic.Parameters.AddWithValue("@p", (object?)proprio ?? DBNull.Value);
                await ic.ExecuteNonQueryAsync();
            }
        }
    }

    private static async Task<char?> GetTipoPessoaAsync(MySqlConnection conn, MySqlTransaction tx, long idEntidade)
    {
        const string sql = "SELECT tipo_pessoa FROM resilead.entidade WHERE id_entidade=@e";
        using var cmd = new MySqlCommand(sql, conn, tx);
        cmd.Parameters.AddWithValue("@e", idEntidade);
        var obj = await cmd.ExecuteScalarAsync();
        return obj is null || obj == DBNull.Value ? null : Convert.ToChar(obj);
    }

    private async Task InsertRegistroResiduosAsync(MySqlConnection conn, MySqlTransaction tx, MtrRow m)
    {
        // Find registro id
        const string selReg = "SELECT id_registro FROM resilead.registro WHERE numero_mtr=@n";
        long idRegistro;
        using (var rc = new MySqlCommand(selReg, conn, tx)) { rc.Parameters.AddWithValue("@n", m.Numero); idRegistro = Convert.ToInt64(await rc.ExecuteScalarAsync()); }

        // Parse residuos array
        using var doc = JsonDocument.Parse(m.Residuos);
        foreach (var el in doc.RootElement.EnumerateArray())
        {
            var desc = el.TryGetProperty("Descricao", out var dEl) ? dEl.GetString() : null;
            var classe = el.TryGetProperty("Classe", out var cEl) ? cEl.GetString() : null;
            var un = el.TryGetProperty("Unidade", out var uEl) ? uEl.GetString() : null;
            var qi = el.TryGetProperty("QuantidadeIndicada", out var qiEl) ? qiEl.GetDouble() : 0.0;
            double? qr = null;
            if (el.TryGetProperty("QuantidadeRecebida", out var qrEl) && qrEl.ValueKind != JsonValueKind.Null)
            {
                if (qrEl.TryGetDouble(out var d)) qr = d;
            }

            var codigoResiduo = Normalization.DeriveResiduoCodigo(desc);
            if (string.IsNullOrEmpty(codigoResiduo)) continue;

            // ensure residuo exists (insert ignore)
            const string insRes = "INSERT IGNORE INTO resilead.residuo(codigo_residuo, descricao, perigoso, codigo_unidade_padrao) VALUES (@c,@d,@p,NULL)";
            using (var ir = new MySqlCommand(insRes, conn, tx))
            {
                var perig = Normalization.HasDangerousMark(desc) || Normalization.HasDangerousMark(codigoResiduo);
                ir.Parameters.AddWithValue("@c", codigoResiduo);
                ir.Parameters.AddWithValue("@d", (object?)Normalization.Clean(desc) ?? string.Empty);
                ir.Parameters.AddWithValue("@p", perig);
                await ir.ExecuteNonQueryAsync();
            }

            // classe code
            int? codigoClasse = await TryResolveClasseAsync(conn, tx, classe);
            // unidade code
            int? codigoUnidade = await TryResolveUnidadeAsync(conn, tx, un);

            // if unidade is null, fallback to residuo default if set
            if (codigoUnidade is null)
            {
                const string selU = "SELECT codigo_unidade_padrao FROM resilead.residuo WHERE codigo_residuo=@c";
                using var su = new MySqlCommand(selU, conn, tx);
                su.Parameters.AddWithValue("@c", codigoResiduo);
                var obj = await su.ExecuteScalarAsync();
                if (obj is not null && obj != DBNull.Value) codigoUnidade = Convert.ToInt32(obj);
            }

            const string ins = @"INSERT INTO resilead.registro_residuo
                                 (id_registro, codigo_residuo, codigo_classe, codigo_unidade, quantidade_indicada, quantidade_recebida)
                                 VALUES (@r, @c, @cl, @u, @qi, @qr)";
            using var cmd = new MySqlCommand(ins, conn, tx);
            cmd.Parameters.AddWithValue("@r", idRegistro);
            cmd.Parameters.AddWithValue("@c", codigoResiduo);
            cmd.Parameters.AddWithValue("@cl", (object?)codigoClasse ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@u", (object?)codigoUnidade ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@qi", qi);
            cmd.Parameters.AddWithValue("@qr", (object?)qr ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private static async Task<int?> TryResolveClasseAsync(MySqlConnection conn, MySqlTransaction tx, string? classe)
    {
        if (string.IsNullOrWhiteSpace(classe)) return null;
        const string sel = "SELECT codigo_classe FROM resilead.classe WHERE UPPER(TRIM(descricao))=UPPER(TRIM(@d))";
        using var sc = new MySqlCommand(sel, conn, tx);
        sc.Parameters.AddWithValue("@d", Normalization.Clean(classe));
        var obj = await sc.ExecuteScalarAsync();
        return obj is null || obj == DBNull.Value ? null : Convert.ToInt32(obj);
    }

    private static async Task<int?> TryResolveUnidadeAsync(MySqlConnection conn, MySqlTransaction tx, string? unidade)
    {
        if (string.IsNullOrWhiteSpace(unidade)) return null;
        // Try by sigla, then by descricao
        const string bySigla = "SELECT codigo_unidade FROM resilead.unidade WHERE UPPER(TRIM(sigla))=UPPER(TRIM(@s))";
        using (var cs = new MySqlCommand(bySigla, conn, tx))
        {
            cs.Parameters.AddWithValue("@s", Normalization.Clean(unidade));
            var obj = await cs.ExecuteScalarAsync();
            if (obj is not null && obj != DBNull.Value) return Convert.ToInt32(obj);
        }
        const string byDesc = "SELECT codigo_unidade FROM resilead.unidade WHERE UPPER(TRIM(descricao))=UPPER(TRIM(@d))";
        using var cd = new MySqlCommand(byDesc, conn, tx);
        cd.Parameters.AddWithValue("@d", Normalization.Clean(unidade));
        var obj2 = await cd.ExecuteScalarAsync();
        return obj2 is null || obj2 == DBNull.Value ? null : Convert.ToInt32(obj2);
    }

    private async Task MoveToHistoryAndDeleteAsync(MtrRow m)
    {
        using var conn = await _db.OpenAsync();
        using var tx = await conn.BeginTransactionAsync();
        try
        {
            const string ins = @"INSERT INTO sinir.mtr_history
                (numero, tipo_manifesto, responsavel_emissao, tem_mtr_complementar, numero_mtr_provisorio,
                 data_emissao, data_recebimento, situacao, responsavel_recebimento, justificativa, tratamento,
                 numero_cdf, residuos, residuos_codigo, residuos_classe, gerador, transportador, destinador,
                 gerador_cpf_cnpj, transportador_cpf_cnpj, destinador_cpf_cnpj, cpfs_cnpjs, created_by, created_dt)
              VALUES
                (@numero, @tipo, @re, @temc, @mtrp, @de, @dr, @sit, @rr, @just, @trat, @cdf,
                 @res, @res_cod, @res_cls, @ger, @transp, @dest, @g_cpf, @t_cpf, @d_cpf, @cpfs, @cb, @cd)";

            using (var cmd = new MySqlCommand(ins, conn, tx))
            {
                cmd.Parameters.AddWithValue("@numero", m.Numero);
                cmd.Parameters.AddWithValue("@tipo", Normalization.Clean(m.TipoManifesto));
                cmd.Parameters.AddWithValue("@re", Normalization.Clean(m.ResponsavelEmissao));
                cmd.Parameters.AddWithValue("@temc", (object?)Normalization.CleanOrNull(m.TemMtrComplementar) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@mtrp", (object?)Normalization.CleanOrNull(m.NumeroMtrProvisorio) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@de", Normalization.Clean(m.DataEmissao));
                cmd.Parameters.AddWithValue("@dr", (object?)Normalization.CleanOrNull(m.DataRecebimento) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@sit", Normalization.Clean(m.Situacao));
                cmd.Parameters.AddWithValue("@rr", (object?)Normalization.CleanOrNull(m.ResponsavelRecebimento) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@just", (object?)Normalization.CleanOrNull(m.Justificativa) ?? DBNull.Value);
                var trat = Normalization.Clean(m.Tratamento); cmd.Parameters.AddWithValue("@trat", trat.Length == 0 ? DBNull.Value : trat);
                cmd.Parameters.AddWithValue("@cdf", (object?)Normalization.CleanOrNull(m.NumeroCdf) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@res", m.Residuos);
                cmd.Parameters.AddWithValue("@res_cod", m.ResiduosCodigo);
                cmd.Parameters.AddWithValue("@res_cls", m.ResiduosClasse);
                cmd.Parameters.AddWithValue("@ger", m.Gerador);
                cmd.Parameters.AddWithValue("@transp", m.Transportador);
                cmd.Parameters.AddWithValue("@dest", m.Destinador);
                cmd.Parameters.AddWithValue("@g_cpf", Normalization.OnlyDigits(m.GeradorCpfCnpj));
                cmd.Parameters.AddWithValue("@t_cpf", Normalization.OnlyDigits(m.TransportadorCpfCnpj));
                cmd.Parameters.AddWithValue("@d_cpf", Normalization.OnlyDigits(m.DestinadorCpfCnpj));
                var cpfs = string.Join(',', new[] { m.GeradorCpfCnpj, m.TransportadorCpfCnpj, m.DestinadorCpfCnpj }.Select(Normalization.OnlyDigits));
                cmd.Parameters.AddWithValue("@cpfs", cpfs);
                cmd.Parameters.AddWithValue("@cb", m.CreatedBy);
                cmd.Parameters.AddWithValue("@cd", m.CreatedDt);
                await cmd.ExecuteNonQueryAsync();
            }

            const string del = "DELETE FROM sinir.mtr WHERE numero=@numero";
            using (var dc = new MySqlCommand(del, conn, tx))
            {
                dc.Parameters.AddWithValue("@numero", m.Numero);
                await dc.ExecuteNonQueryAsync();
            }

            await tx.CommitAsync();
        }
        catch
        {
            await tx.RollbackAsync();
            throw;
        }
    }

    private async Task MoveToErrorBucketAsync(MtrRow m, Exception ex)
    {
        using var conn = await _db.OpenAsync();
        using var tx = await conn.BeginTransactionAsync();
        try
        {
            const string ins = @"INSERT INTO sinir.mtr_normalize_error
                (numero, tipo_manifesto, responsavel_emissao, tem_mtr_complementar, numero_mtr_provisorio,
                 data_emissao, data_recebimento, situacao, responsavel_recebimento, justificativa, tratamento,
                 numero_cdf, residuos, residuos_codigo, residuos_classe, gerador, transportador, destinador,
                 gerador_cpf_cnpj, transportador_cpf_cnpj, destinador_cpf_cnpj, cpfs_cnpjs, created_by, created_dt,
                 error_description)
              VALUES
                (@numero, @tipo, @re, @temc, @mtrp, @de, @dr, @sit, @rr, @just, @trat, @cdf,
                 @res, @res_cod, @res_cls, @ger, @transp, @dest, @g_cpf, @t_cpf, @d_cpf, @cpfs, @cb, @cd, @err)";

            var errorDescription = ex.Message ?? "Unknown error";
            if (errorDescription.Length > 2048) errorDescription = errorDescription[..2048];
            using (var cmd = new MySqlCommand(ins, conn, tx))
            {
                cmd.Parameters.AddWithValue("@numero", m.Numero);
                cmd.Parameters.AddWithValue("@tipo", Normalization.Clean(m.TipoManifesto));
                cmd.Parameters.AddWithValue("@re", Normalization.Clean(m.ResponsavelEmissao));
                cmd.Parameters.AddWithValue("@temc", (object?)Normalization.CleanOrNull(m.TemMtrComplementar) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@mtrp", (object?)Normalization.CleanOrNull(m.NumeroMtrProvisorio) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@de", Normalization.Clean(m.DataEmissao));
                cmd.Parameters.AddWithValue("@dr", (object?)Normalization.CleanOrNull(m.DataRecebimento) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@sit", Normalization.Clean(m.Situacao));
                cmd.Parameters.AddWithValue("@rr", (object?)Normalization.CleanOrNull(m.ResponsavelRecebimento) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@just", (object?)Normalization.CleanOrNull(m.Justificativa) ?? DBNull.Value);
                var trat = Normalization.Clean(m.Tratamento);
                cmd.Parameters.AddWithValue("@trat", trat.Length == 0 ? DBNull.Value : trat);
                cmd.Parameters.AddWithValue("@cdf", (object?)Normalization.CleanOrNull(m.NumeroCdf) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@res", m.Residuos);
                cmd.Parameters.AddWithValue("@res_cod", m.ResiduosCodigo);
                cmd.Parameters.AddWithValue("@res_cls", m.ResiduosClasse);
                cmd.Parameters.AddWithValue("@ger", m.Gerador);
                cmd.Parameters.AddWithValue("@transp", m.Transportador);
                cmd.Parameters.AddWithValue("@dest", m.Destinador);
                cmd.Parameters.AddWithValue("@g_cpf", Normalization.OnlyDigits(m.GeradorCpfCnpj));
                cmd.Parameters.AddWithValue("@t_cpf", Normalization.OnlyDigits(m.TransportadorCpfCnpj));
                cmd.Parameters.AddWithValue("@d_cpf", Normalization.OnlyDigits(m.DestinadorCpfCnpj));
                var cpfs = string.Join(',', new[] { m.GeradorCpfCnpj, m.TransportadorCpfCnpj, m.DestinadorCpfCnpj }.Select(Normalization.OnlyDigits));
                cmd.Parameters.AddWithValue("@cpfs", cpfs);
                cmd.Parameters.AddWithValue("@cb", m.CreatedBy);
                cmd.Parameters.AddWithValue("@cd", m.CreatedDt);
                cmd.Parameters.AddWithValue("@err", errorDescription);
                await cmd.ExecuteNonQueryAsync();
            }

            const string del = "DELETE FROM sinir.mtr WHERE numero=@numero";
            using (var dc = new MySqlCommand(del, conn, tx))
            {
                dc.Parameters.AddWithValue("@numero", m.Numero);
                await dc.ExecuteNonQueryAsync();
            }

            await tx.CommitAsync();
        }
        catch
        {
            await tx.RollbackAsync();
            throw;
        }
    }

    private sealed class MtrRow
    {
        public string Numero { get; set; } = string.Empty;
        public string TipoManifesto { get; set; } = string.Empty;
        public string ResponsavelEmissao { get; set; } = string.Empty;
        public string? TemMtrComplementar { get; set; }
        public string? NumeroMtrProvisorio { get; set; }
        public string DataEmissao { get; set; } = string.Empty;
        public string? DataRecebimento { get; set; }
        public string Situacao { get; set; } = string.Empty;
        public string? ResponsavelRecebimento { get; set; }
        public string? Justificativa { get; set; }
        public string Tratamento { get; set; } = string.Empty;
        public string? NumeroCdf { get; set; }
        public string Residuos { get; set; } = string.Empty; // JSON
        public string ResiduosCodigo { get; set; } = string.Empty;
        public string ResiduosClasse { get; set; } = string.Empty;
        public string Gerador { get; set; } = string.Empty; // JSON
        public string Transportador { get; set; } = string.Empty; // JSON (includes Motorista, PlacaVeiculo)
        public string Destinador { get; set; } = string.Empty; // JSON
        public string GeradorCpfCnpj { get; set; } = string.Empty;
        public string TransportadorCpfCnpj { get; set; } = string.Empty;
        public string DestinadorCpfCnpj { get; set; } = string.Empty;
        public string CreatedBy { get; set; } = "system";
        public DateTime CreatedDt { get; set; } = DateTime.Now;
    }
}
