using System.Text.Json;
using MySql.Data.MySqlClient;
using Resilead.Integration.Local.Configuration;

namespace Resilead.Integration.Local.Infrastructure;

internal sealed class RefDataLoader
{
    private readonly AppConfig _cfg;
    private readonly Db _db;
    public RefDataLoader(AppConfig cfg)
    {
        _cfg = cfg;
        _db = new Db(cfg.ConnectionString);
    }

    public async Task RunAsync()
    {
        var baseDir = Path.IsPathRooted(_cfg.DataDir) ? _cfg.DataDir : Path.Combine(AppContext.BaseDirectory, _cfg.DataDir);
        await EnsureSituacaoAsync(baseDir);
        await EnsureTipoManifestoAsync(baseDir);
        await EnsureTratamentoAsync(baseDir);
        await EnsureUnidadeAsync(baseDir);
        await EnsureClasseAsync(baseDir);
        await EnsureResiduoAsync(baseDir);
        Console.WriteLine("[ref-load] Completed.");
    }

    private async Task EnsureSituacaoAsync(string baseDir)
    {
        var path = Path.Combine(baseDir, "situacao.json");
        if (!File.Exists(path)) return;
        var arr = JsonSerializer.Deserialize<string[]>(await File.ReadAllTextAsync(path)) ?? Array.Empty<string>();
        const string sql = "INSERT IGNORE INTO resilead.situacao(descricao) VALUES (@d)";
        using var conn = await _db.OpenAsync();
        foreach (var raw in arr)
        {
            var d = Normalization.Clean(raw);
            if (d.Length == 0) continue;
            using var cmd = new MySqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@d", d);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task EnsureTipoManifestoAsync(string baseDir)
    {
        var path = Path.Combine(baseDir, "tipoManifesto.json");
        if (!File.Exists(path)) return;
        var arr = JsonSerializer.Deserialize<string[]>(await File.ReadAllTextAsync(path)) ?? Array.Empty<string>();
        const string sql = "INSERT IGNORE INTO resilead.tipo_manifesto(descricao) VALUES (@d)";
        using var conn = await _db.OpenAsync();
        foreach (var raw in arr)
        {
            var d = Normalization.Clean(raw);
            if (d.Length == 0) continue;
            using var cmd = new MySqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@d", d);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task EnsureTratamentoAsync(string baseDir)
    {
        var path = Path.Combine(baseDir, "tratamento.json");
        if (!File.Exists(path)) return;
        var arr = JsonSerializer.Deserialize<string[]>(await File.ReadAllTextAsync(path)) ?? Array.Empty<string>();
        const string sql = "INSERT IGNORE INTO resilead.tratamento(descricao) VALUES (@d)";
        using var conn = await _db.OpenAsync();
        foreach (var raw in arr)
        {
            var d = Normalization.Clean(raw);
            if (d.Length == 0) d = string.Empty; // will be inserted as '' only if not filtered; but caller specified: trim("") -> NULL elsewhere
            if (d.Length == 0) continue;
            using var cmd = new MySqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@d", d);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task EnsureUnidadeAsync(string baseDir)
    {
        var path = Path.Combine(baseDir, "unidade.json");
        if (!File.Exists(path)) return;
        using var doc = JsonDocument.Parse(await File.ReadAllTextAsync(path));
        const string sql = "INSERT IGNORE INTO resilead.unidade(codigo_unidade, descricao, sigla) VALUES (@c, @d, @s)";
        using var conn = await _db.OpenAsync();
        foreach (var el in doc.RootElement.EnumerateArray())
        {
            var c = el.GetProperty("uniCodigo").GetInt32();
            var d = Normalization.Clean(el.GetProperty("uniDescricao").GetString());
            var s = Normalization.Clean(el.GetProperty("uniSigla").GetString());
            using var cmd = new MySqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@c", c);
            cmd.Parameters.AddWithValue("@d", d);
            cmd.Parameters.AddWithValue("@s", s);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task EnsureClasseAsync(string baseDir)
    {
        var path = Path.Combine(baseDir, "classe.json");
        if (!File.Exists(path)) return;
        using var doc = JsonDocument.Parse(await File.ReadAllTextAsync(path));
        const string sql = "INSERT IGNORE INTO resilead.classe(codigo_classe, descricao, resolucao) VALUES (@c, @d, @r)";
        using var conn = await _db.OpenAsync();
        foreach (var el in doc.RootElement.EnumerateArray())
        {
            var c = el.GetProperty("claCodigo").GetInt32();
            var d = Normalization.Clean(el.GetProperty("claDescricao").GetString());
            var r = Normalization.Clean(el.GetProperty("claResolucao").GetString());
            using var cmd = new MySqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@c", c);
            cmd.Parameters.AddWithValue("@d", d);
            cmd.Parameters.AddWithValue("@r", r);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task EnsureResiduoAsync(string baseDir)
    {
        var path = Path.Combine(baseDir, "residuos.json");
        if (!File.Exists(path)) return;
        using var doc = JsonDocument.Parse(await File.ReadAllTextAsync(path));
        const string sql = "INSERT IGNORE INTO resilead.residuo(codigo_residuo, descricao, perigoso, codigo_unidade_padrao) VALUES (@c, @d, @p, @u)";
        using var conn = await _db.OpenAsync();

        // Load unidades once into memory for fast lookup (sigla and descricao, lower/trim)
        var siglaToCodigo = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var descToCodigo = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        const string qUn = "SELECT codigo_unidade, sigla, descricao FROM resilead.unidade";
        using (var qu = new MySqlCommand(qUn, conn))
        using (var rdr = await qu.ExecuteReaderAsync())
        {
            while (await rdr.ReadAsync())
            {
                var cod = rdr.GetInt32(0);
                var sigla = (rdr.IsDBNull(1) ? string.Empty : rdr.GetString(1)).Trim().ToLowerInvariant();
                var desc = (rdr.IsDBNull(2) ? string.Empty : rdr.GetString(2)).Trim().ToLowerInvariant();
                if (sigla.Length > 0 && !siglaToCodigo.ContainsKey(sigla)) siglaToCodigo[sigla] = cod;
                if (desc.Length > 0 && !descToCodigo.ContainsKey(desc)) descToCodigo[desc] = cod;
            }
        }

        foreach (var el in doc.RootElement.EnumerateArray())
        {
            var code = Normalization.Clean(el.GetProperty("codigo_residuo").GetString());
            if (code.Length == 0) continue;
            var desc = Normalization.Clean(el.GetProperty("descricao").GetString());
            var perigosoVal = el.GetProperty("perigoso").GetInt32();
            var perigoso = perigosoVal != 0;
            var uniSigla = el.TryGetProperty("unidade_medida_sigla", out var uProp) ? Normalization.Clean(uProp.GetString()) : null;

            int? codigoUnidade = null;
            if (!string.IsNullOrWhiteSpace(uniSigla))
            {
                var key = uniSigla!.Trim().ToLowerInvariant();
                if (siglaToCodigo.TryGetValue(key, out var cu)) codigoUnidade = cu;
                else if (descToCodigo.TryGetValue(key, out var cud)) codigoUnidade = cud; // if provided as full descricao
            }

            using var cmd = new MySqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@c", code);
            cmd.Parameters.AddWithValue("@d", desc);
            cmd.Parameters.AddWithValue("@p", perigoso);
            cmd.Parameters.AddWithValue("@u", (object?)codigoUnidade ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
