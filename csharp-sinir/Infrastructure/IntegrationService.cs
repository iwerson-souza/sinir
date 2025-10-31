using System.Text.Json;
using MySql.Data.MySqlClient;
using Sinir.Integration.Local.Domain;

namespace Sinir.Integration.Local.Infrastructure;

internal sealed class IntegrationService
{
    private readonly string _connString;
    public IntegrationService(string connString) => _connString = connString;

    private async Task<MySqlConnection> OpenAsync()
    {
        var c = new MySqlConnection(_connString);
        await c.OpenAsync();
        return c;
    }

    public async Task<List<Stakeholder>> ListStakeholdersAsync()
    {
        const string sql = "SELECT unidade, cpf_cnpj, nome, data_inicial, data_final FROM stakeholder";
        using var conn = await OpenAsync();
        using var cmd = new MySqlCommand(sql, conn);
        using var rdr = await cmd.ExecuteReaderAsync();
        var list = new List<Stakeholder>();
        while (await rdr.ReadAsync())
        {
            list.Add(new Stakeholder
            {
                Unidade = rdr.GetString(0),
                CpfCnpj = rdr.GetString(1),
                Nome = rdr.GetString(2),
                DataInicial = rdr.IsDBNull(3) ? (DateTime?)null : rdr.GetDateTime(3),
                DataFinal = rdr.IsDBNull(4) ? (DateTime?)null : rdr.GetDateTime(4)
            });
        }
        return list;
    }

    public async Task UpsertMtrLoadsAsync(IEnumerable<MtrLoad> loads)
    {
        const string sql = @"INSERT IGNORE INTO mtr_load (url, unidade, status, created_by, created_dt)
                             VALUES (@url, @unidade, 'PENDING', @created_by, @created_dt)";
        using (var conn = await OpenAsync())
        {
            foreach (var l in loads)
            {
                using var cmd = new MySqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@url", l.Url);
                cmd.Parameters.AddWithValue("@unidade", l.Unidade);
                cmd.Parameters.AddWithValue("@created_by", l.CreatedBy);
                cmd.Parameters.AddWithValue("@created_dt", l.CreatedDt);
                await cmd.ExecuteNonQueryAsync();
            }
        }
    }

    public async Task UpdateStakeholderRangeAsync(string unidade, string cpfCnpj, DateTime start, DateTime end)
    {
        const string sql = @"UPDATE stakeholder
                             SET data_inicial=@di, data_final=@df, last_modified_by='system', last_modified_dt=UTC_TIMESTAMP()
                             WHERE unidade=@unidade AND cpf_cnpj=@cpf";
        using var conn = await OpenAsync();
        using var cmd = new MySqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@di", start);
        cmd.Parameters.AddWithValue("@df", end);
        cmd.Parameters.AddWithValue("@unidade", unidade);
        cmd.Parameters.AddWithValue("@cpf", cpfCnpj);
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task<List<MtrLoad>> ListPendingMtrLoadsAsync(int limit)
    {
        const string sql = @"SELECT url, unidade FROM mtr_load WHERE status='PENDING' ORDER BY created_dt LIMIT @limit";
        using var conn = await OpenAsync();
        using var cmd = new MySqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@limit", limit);
        using var rdr = await cmd.ExecuteReaderAsync();
        var list = new List<MtrLoad>();
        while (await rdr.ReadAsync())
        {
            list.Add(new MtrLoad { Url = rdr.GetString(0), Unidade = rdr.GetString(1) });
        }
        return list;
    }

    public async Task<bool> TryClaimMtrLoadAsync(string url, string workerId)
    {
        const string sql = @"UPDATE mtr_load SET status='PROCESSING', locked_by=@worker, locked_at=UTC_TIMESTAMP()
                             WHERE url=@url AND status='PENDING'";
        using var conn = await OpenAsync();
        using var cmd = new MySqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@url", url);
        cmd.Parameters.AddWithValue("@worker", workerId);
        var rows = await cmd.ExecuteNonQueryAsync();
        return rows == 1;
    }

    public async Task DeleteMtrLoadAsync(string url)
    {
        const string sql = "DELETE FROM mtr_load WHERE url=@url";
        using var conn = await OpenAsync();
        using var cmd = new MySqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@url", url);
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task FailMtrLoadAsync(string url, Exception ex)
    {
        const string sql = @"UPDATE mtr_load SET status='ERROR', last_error=@err WHERE url=@url";
        using var conn = await OpenAsync();
        using var cmd = new MySqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@url", url);
        cmd.Parameters.AddWithValue("@err", ex.Message);
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task MarkErrorAsync(string source, string? reference, Exception ex, object? extra = null)
    {
        const string sql = @"INSERT INTO error (source, reference, message, stack, created_dt, extra)
                             VALUES (@source, @reference, @message, @stack, UTC_TIMESTAMP(), @extra)";
        using var conn = await OpenAsync();
        using var cmd = new MySqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@source", source);
        cmd.Parameters.AddWithValue("@reference", reference ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@message", ex.Message);
        cmd.Parameters.AddWithValue("@stack", ex.StackTrace ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@extra", extra != null ? JsonSerializer.Serialize(extra) : (object)DBNull.Value);
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task InsertStakeholdersIgnoreAsync(List<Stakeholder> stakeholders, string user)
    {
        const string sql = @"INSERT IGNORE INTO stakeholder (unidade, cpf_cnpj, nome, data_inicial, data_final, created_by, created_dt)
                             VALUES (@unidade, @cpf, @nome, NULL, NULL, @user, UTC_TIMESTAMP())";
        using (var conn = await OpenAsync())
        {
            foreach (var s in stakeholders)
            {
                using var cmd = new MySqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@unidade", s.Unidade);
                cmd.Parameters.AddWithValue("@cpf", s.CpfCnpj);
                cmd.Parameters.AddWithValue("@nome", s.Nome);
                cmd.Parameters.AddWithValue("@user", user);
                await cmd.ExecuteNonQueryAsync();
            }
        }
    }

    public async Task UpsertMtrsAsync(List<MtrRecord> mtrs, string user)
    {
        const string sql = @"INSERT INTO mtr
            (numero, tipo_manifesto, responsavel_emissao, tem_mtr_complementar, numero_mtr_provisorio,
             data_emissao, data_recebimento, situacao, responsavel_recebimento, justificativa, tratamento,
             numero_cdf, residuos, residuos_codigo, residuos_classe, gerador, transportador, destinador,
             gerador_cpf_cnpj, transportador_cpf_cnpj, destinador_cpf_cnpj, cpfs_cnpjs, created_by, created_dt)
            VALUES
            (@numero, @tipo_manifesto, @responsavel_emissao, @tem_mtr_complementar, @numero_mtr_provisorio,
             @data_emissao, @data_recebimento, @situacao, @responsavel_recebimento, @justificativa, @tratamento,
             @numero_cdf, @residuos, @residuos_codigo, @residuos_classe, @gerador, @transportador, @destinador,
             @gerador_cpf, @transportador_cpf, @destinador_cpf, @cpfs, @user, UTC_TIMESTAMP())
            ON DUPLICATE KEY UPDATE
             tipo_manifesto=VALUES(tipo_manifesto),
             responsavel_emissao=VALUES(responsavel_emissao),
             tem_mtr_complementar=VALUES(tem_mtr_complementar),
             numero_mtr_provisorio=VALUES(numero_mtr_provisorio),
             data_emissao=VALUES(data_emissao),
             data_recebimento=VALUES(data_recebimento),
             situacao=VALUES(situacao),
             responsavel_recebimento=VALUES(responsavel_recebimento),
             justificativa=VALUES(justificativa),
             tratamento=VALUES(tratamento),
             numero_cdf=VALUES(numero_cdf),
             residuos=VALUES(residuos),
             residuos_codigo=VALUES(residuos_codigo),
             residuos_classe=VALUES(residuos_classe),
             gerador=VALUES(gerador),
             transportador=VALUES(transportador),
             destinador=VALUES(destinador),
             gerador_cpf_cnpj=VALUES(gerador_cpf_cnpj),
             transportador_cpf_cnpj=VALUES(transportador_cpf_cnpj),
             destinador_cpf_cnpj=VALUES(destinador_cpf_cnpj),
             cpfs_cnpjs=VALUES(cpfs_cnpjs)";

        using (var conn = await OpenAsync())
        {
            foreach (var m in mtrs)
            {
                var residuosCodigo = string.Join("|", m.Residuos.Select(r => (r.Descricao ?? "").Split('-').FirstOrDefault() ?? ""));
                var residuosClasse = string.Join("|", m.Residuos.Select(r => r.Classe ?? ""));
                var cpfs = string.Join("|", new[] { m.Gerador.CpfCnpj, m.Transportador.CpfCnpj, m.Destinador.CpfCnpj });

                using var cmd = new MySqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@numero", m.Numero);
                cmd.Parameters.AddWithValue("@tipo_manifesto", m.TipoManifesto);
                cmd.Parameters.AddWithValue("@responsavel_emissao", m.ResponsavelEmissao);
                cmd.Parameters.AddWithValue("@tem_mtr_complementar", (object?)m.TemMTRComplementar ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@numero_mtr_provisorio", (object?)m.NumeroMtrProvisorio ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@data_emissao", m.DataEmissao);
                cmd.Parameters.AddWithValue("@data_recebimento", m.DataRecebimento ?? "01/01/1900");
                cmd.Parameters.AddWithValue("@situacao", m.Situacao);
                cmd.Parameters.AddWithValue("@responsavel_recebimento", (object?)m.ResponsavelRecebimento ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@justificativa", (object?)m.Justificativa ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@tratamento", m.Tratamento);
                cmd.Parameters.AddWithValue("@numero_cdf", (object?)m.NumeroCdf ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@residuos", JsonSerializer.Serialize(m.Residuos));
                cmd.Parameters.AddWithValue("@residuos_codigo", residuosCodigo);
                cmd.Parameters.AddWithValue("@residuos_classe", residuosClasse);
                cmd.Parameters.AddWithValue("@gerador", JsonSerializer.Serialize(m.Gerador));
                cmd.Parameters.AddWithValue("@transportador", JsonSerializer.Serialize(m.Transportador));
                cmd.Parameters.AddWithValue("@destinador", JsonSerializer.Serialize(m.Destinador));
                cmd.Parameters.AddWithValue("@gerador_cpf", m.Gerador.CpfCnpj);
                cmd.Parameters.AddWithValue("@transportador_cpf", m.Transportador.CpfCnpj);
                cmd.Parameters.AddWithValue("@destinador_cpf", m.Destinador.CpfCnpj);
                cmd.Parameters.AddWithValue("@cpfs", cpfs);
                cmd.Parameters.AddWithValue("@user", user);
                await cmd.ExecuteNonQueryAsync();
            }
        }
    }
}

