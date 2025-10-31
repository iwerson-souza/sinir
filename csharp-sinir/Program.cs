using System.Buffers.Text;
using System.Data;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text.Json;
using ClosedXML.Excel;
using MySql.Data.MySqlClient;

namespace Sinir.Integration.Local;

internal class Program
{
    private static async Task<int> Main(string[] args)
    {
        Console.OutputEncoding = System.Text.Encoding.UTF8;
        var config = AppConfig.Load();
        var mtrsRoot = Paths.GetMtrsRoot();
        Console.WriteLine($"[{DateTime.UtcNow:O}] Using MTRS directory: {mtrsRoot}");
        Directory.CreateDirectory(mtrsRoot);

        var cmd = args.FirstOrDefault()?.ToLowerInvariant() ?? "run";
        switch (cmd)
        {
            case "setup":
                await SetupAsync(config);
                break;
            case "process":
                await ProcessBatchAsync(config, mtrsRoot);
                break;
            case "run":
            default:
                await SetupAsync(config);
                await ProcessBatchAsync(config, mtrsRoot);
                break;
        }

        return 0;
    }

    private static async Task SetupAsync(AppConfig config)
    {
        var svc = new IntegrationService(config.ConnectionString);

        // Strategy: for all stakeholders, (re)generate MtrLoads from last data_final or default
        var stakeholders = await svc.ListStakeholdersAsync();
        foreach (var sh in stakeholders)
        {
            var strat = SinirStrategy.BuildStrategy(sh.Unidade, sh.DataFinal);
            var urls = strat.Setup.SelectMany(x => x.Urls);
            await svc.UpsertMtrLoadsAsync(urls.Select(u => new MtrLoad
            {
                Url = u,
                Unidade = sh.Unidade,
                CreatedBy = "system",
                CreatedDt = DateTime.UtcNow
            }));

            // Update stakeholder date range
            await svc.UpdateStakeholderRangeAsync(sh.Unidade, sh.CpfCnpj, strat.Summary.StartDate, strat.Summary.FinalDate);
        }

        Console.WriteLine($"Setup complete. Generated/ensured loads for {stakeholders.Count} stakeholder(s).");
    }

    private static async Task ProcessBatchAsync(AppConfig config, string mtrsRoot)
    {
        var svc = new IntegrationService(config.ConnectionString);

        // Take a batch of PENDING loads
        Console.WriteLine($"[{DateTime.UtcNow:O}] Fetching up to {config.BatchSize} pending loads from DB...");
        var batch = await svc.ListPendingMtrLoadsAsync(config.BatchSize);
        if (batch.Count == 0)
        {
            Console.WriteLine($"[{DateTime.UtcNow:O}] No pending loads found.");
            return;
        }

        Console.WriteLine($"[{DateTime.UtcNow:O}] Processing {batch.Count} load(s) with DOP={config.MaxDegreeOfParallelism}...");
        foreach (var b in batch.Take(5))
        {
            Console.WriteLine($"[{DateTime.UtcNow:O}] Pending URL: {b.Url}");
        }

        var handler = new SocketsHttpHandler
        {
            MaxConnectionsPerServer = config.MaxConnectionsPerServer,
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
            PooledConnectionLifetime = TimeSpan.FromMinutes(10),
            UseCookies = false,
        };
        using var http = new HttpClient(handler)
        {
            Timeout = Timeout.InfiniteTimeSpan,
            DefaultRequestVersion = config.UseHttp2 ? HttpVersion.Version20 : HttpVersion.Version11,
        };
        http.DefaultRequestHeaders.UserAgent.Clear();
        http.DefaultRequestHeaders.UserAgent.ParseAdd(config.UserAgent);

        var semaphore = new SemaphoreSlim(config.MaxDegreeOfParallelism, config.MaxDegreeOfParallelism);
        var tasks = new List<Task>();
        var workerId = $"{Environment.MachineName}:{Environment.ProcessId}";
        var started = 0;
        var claimedCount = 0;
        var completed = 0;

        foreach (var load in batch)
        {
            await semaphore.WaitAsync();
            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    var localStart = Interlocked.Increment(ref started);
                    Console.WriteLine($"[{DateTime.UtcNow:O}] [#{localStart}] Claiming: {load.Url}");
                    // Claim atomically to avoid double-processing
                    var claimed = await svc.TryClaimMtrLoadAsync(load.Url, workerId);
                    if (!claimed)
                    {
                        Console.WriteLine($"[{DateTime.UtcNow:O}] [#{localStart}] Skipped (already claimed): {load.Url}");
                        return; // another worker got it
                    }

                    var localClaim = Interlocked.Increment(ref claimedCount);
                    Console.WriteLine($"[{DateTime.UtcNow:O}] [#{localStart}] Claimed OK by {workerId}. Processing...");

                    await ProcessSingleAsync(svc, http, load, localStart, mtrsRoot, TimeSpan.FromSeconds(config.RequestTimeoutSeconds));
                    Console.WriteLine($"[{DateTime.UtcNow:O}] [#{localStart}] Deleting load: {load.Url}");
                    await svc.DeleteMtrLoadAsync(load.Url);
                    var done = Interlocked.Increment(ref completed);
                    if (done % 5 == 0 || done == batch.Count)
                    {
                        Console.WriteLine($"[{DateTime.UtcNow:O}] Progress: {done}/{batch.Count} completed.");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{DateTime.UtcNow:O}] ERROR while processing {load.Url}: {ex.Message}");
                    await svc.MarkErrorAsync("processor", load.Url, ex);
                    await svc.FailMtrLoadAsync(load.Url, ex);
                }
                finally
                {
                    semaphore.Release();
                }
            }));
        }

        await Task.WhenAll(tasks);
        Console.WriteLine($"[{DateTime.UtcNow:O}] Processing completed.");
    }

    private static async Task ProcessSingleAsync(IntegrationService svc, HttpClient http, MtrLoad load, int seq, string mtrsRoot, TimeSpan perRequestTimeout)
    {
        Console.WriteLine($"[{DateTime.UtcNow:O}] [{seq}] HTTP GET → {load.Url}");
        var sw = System.Diagnostics.Stopwatch.StartNew();
        using var cts = new CancellationTokenSource(perRequestTimeout);
        HttpResponseMessage response;
        try
        {
            response = await http.GetAsync(load.Url, HttpCompletionOption.ResponseHeadersRead, cts.Token);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine($"[{DateTime.UtcNow:O}] [{seq}] HTTP timeout after {perRequestTimeout.TotalSeconds:F0}s → {load.Url}");
            throw;
        }
        response.EnsureSuccessStatusCode();
        var contentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
        var data = await response.Content.ReadAsByteArrayAsync();
        sw.Stop();
        Console.WriteLine($"[{DateTime.UtcNow:O}] [{seq}] HTTP {((int)response.StatusCode)} in {sw.Elapsed.TotalSeconds:F1}s, {data.Length:N0} bytes");

        var parts = load.Url.Split('/');
        var dataInicial = parts[9].Split('-').Reverse().ToArray();
        var dataFinal = parts[10].Split('-').Reverse().ToArray();
        var filename = $"{load.Unidade}_{string.Join("-", dataInicial)}_{string.Join("-", dataFinal)}_{parts[8]}.xlsx";

        var destDir = Path.Combine(mtrsRoot, load.Unidade);
        Directory.CreateDirectory(destDir);
        var destPath = Path.Combine(destDir, filename);
        await File.WriteAllBytesAsync(destPath, data);
        Console.WriteLine($"[{DateTime.UtcNow:O}] [{seq}] Saved file: {destPath}");

        // Parse XLSX
        Console.WriteLine($"[{DateTime.UtcNow:O}] [{seq}] Parsing XLSX...");
        var parseSw = System.Diagnostics.Stopwatch.StartNew();
        List<MtrRecord> mtrs;
        try
        {
            mtrs = ExcelParser.ParseMTRs(destPath);
        }
        catch (Exception ex)
        {
            // Some files are empty or malformed; treat as no MTRs instead of error
            Console.WriteLine($"[{DateTime.UtcNow:O}] [{seq}] XLSX parse issue: {ex.Message}. Treating as 0 MTR(s).");
            mtrs = new List<MtrRecord>();
        }
        parseSw.Stop();
        Console.WriteLine($"[{DateTime.UtcNow:O}] [{seq}] Parsed {mtrs.Count} MTR(s) in {parseSw.Elapsed.TotalSeconds:F1}s");
        if (mtrs.Count == 0)
        {
            Console.WriteLine($"[{DateTime.UtcNow:O}] [{seq}] No MTRs found for {load.Url}");
            return;
        }

        // Upsert MTRs
        Console.WriteLine($"[{DateTime.UtcNow:O}] [{seq}] Upserting {mtrs.Count} MTR(s) into DB...");
        await svc.UpsertMtrsAsync(mtrs, "system");
        Console.WriteLine($"[{DateTime.UtcNow:O}] [{seq}] Upsert completed");

        // Build distinct stakeholders from records (excluding source unidade)
        var distinct = new Dictionary<string, Stakeholder>();
        foreach (var m in mtrs)
        {
            var g = new Stakeholder { Unidade = m.Gerador.Unidade, CpfCnpj = m.Gerador.CpfCnpj, Nome = m.Gerador.Nome };
            var t = new Stakeholder { Unidade = m.Transportador.Unidade, CpfCnpj = m.Transportador.CpfCnpj, Nome = m.Transportador.Nome };
            var d = new Stakeholder { Unidade = m.Destinador.Unidade, CpfCnpj = m.Destinador.CpfCnpj, Nome = m.Destinador.Nome };
            foreach (var s in new[] { g, t, d })
            {
                if (s.Unidade == load.Unidade) continue;
                var key = $"{s.Unidade}|{s.CpfCnpj}|{s.Nome}";
                if (!distinct.ContainsKey(key)) distinct[key] = s;
            }
        }

        // Insert ignore stakeholders
        Console.WriteLine($"[{DateTime.UtcNow:O}] [{seq}] Distinct stakeholders to insert: {distinct.Count}");
        if (distinct.Count > 0)
        {
            await svc.InsertStakeholdersIgnoreAsync(distinct.Values.ToList(), "system");
            Console.WriteLine($"[{DateTime.UtcNow:O}] [{seq}] Stakeholders insert-ignore completed");
        }
    }
}

internal sealed class AppConfig
{
    public string ConnectionString { get; init; } = "";
    public int MaxDegreeOfParallelism { get; init; } = 10;
    public int BatchSize { get; init; } = 100;
    public string UserAgent { get; init; } = "SinirLocal/1.0";
    public int MaxConnectionsPerServer { get; init; } = 10;
    public int RequestTimeoutSeconds { get; init; } = 180;
    public bool UseHttp2 { get; init; } = false;

    public static AppConfig Load()
    {
        var jsonPath = Path.Combine(AppContext.BaseDirectory, "appsettings.json");
        if (!File.Exists(jsonPath))
        {
            Console.Error.WriteLine($"Configuration file not found: {jsonPath}");
            throw new FileNotFoundException("appsettings.json not found", jsonPath);
        }
        var json = File.ReadAllText(jsonPath);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        var cs = root.GetProperty("ConnectionStrings").GetProperty("MySql").GetString()!;
        var proc = root.GetProperty("Processing");
        return new AppConfig
        {
            ConnectionString = cs,
            MaxDegreeOfParallelism = proc.GetProperty("MaxDegreeOfParallelism").GetInt32(),
            BatchSize = proc.GetProperty("BatchSize").GetInt32(),
            UserAgent = proc.GetProperty("UserAgent").GetString() ?? "SinirLocal/1.0",
            MaxConnectionsPerServer = proc.TryGetProperty("Http", out var http) && http.TryGetProperty("MaxConnectionsPerServer", out var mc)
                ? mc.GetInt32() : 10,
            RequestTimeoutSeconds = proc.TryGetProperty("Http", out http) && http.TryGetProperty("RequestTimeoutSeconds", out var rts)
                ? rts.GetInt32() : 180,
            UseHttp2 = proc.TryGetProperty("Http", out http) && http.TryGetProperty("UseHttp2", out var h2)
                ? h2.GetBoolean() : false
        };
    }
}

internal static class Paths
{
    public static string? TryFindSolutionRoot()
    {
        // Prefer environment variable set by scripts/CI
        var envRoot = Environment.GetEnvironmentVariable("SINIR_SOLUTION_ROOT");
        if (!string.IsNullOrWhiteSpace(envRoot)) return envRoot;

        // Heuristic 1: climb up and look for the project under 'csharp-sinir'
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        for (int i = 0; i < 6 && dir != null; i++)
        {
            var probe = Path.Combine(dir.FullName, "csharp-sinir", "Sinir.Integration.Local.csproj");
            if (File.Exists(probe)) return dir.FullName; // dir is the solution root
            dir = dir.Parent;
        }

        // Heuristic 2: relative to bin -> project -> solution
        var bin = new DirectoryInfo(AppContext.BaseDirectory);
        var projectDir = bin.Parent?.Parent?.Parent; // bin/Config/netX -> up to project folder
        var solutionDir = projectDir?.Parent;        // one more up
        return solutionDir?.FullName;
    }

    public static string GetMtrsRoot()
    {
        var env = Environment.GetEnvironmentVariable("SINIR_MTRS_DIR");
        if (!string.IsNullOrWhiteSpace(env)) return env!;
        var sol = TryFindSolutionRoot();
        var root = sol ?? new DirectoryInfo(AppContext.BaseDirectory).FullName;
        return Path.Combine(root, "mtrs");
    }
}

//

internal static class SinirStrategy
{
    private static readonly string BaseUrl = "https://mtr.sinir.gov.br/api/mtr/pesquisaManifestoRelatorioMtrAnalitico";
    private static readonly string[] Templates = new[]
    {
        "/{ID}/18/8/{START_DATE}/{END_DATE}/5/0/9/0",
        "/{ID}/18/5/{START_DATE}/{END_DATE}/8/0/9/0",
        "/{ID}/18/9/{START_DATE}/{END_DATE}/8/0/5/0"
    };

    public static StrategyResult BuildStrategy(string unidade, DateTime? lastEndDate)
    {
        var startDate = lastEndDate.HasValue
            ? new DateTime(lastEndDate.Value.Year, lastEndDate.Value.Month, lastEndDate.Value.Day).AddDays(1)
            : new DateTime(2020, 1, 1);
        startDate = startDate.Date;

        var now = DateTime.UtcNow.Date;
        var endDate = now.AddDays(-1);

        var periods = GetMonthlyPeriods(startDate, endDate);
        var format = new Func<DateTime, string>(d => $"{d:dd-MM-yyyy}");

        var setups = new List<StrategySetup>();
        foreach (var p in periods)
        {
            var urls = Templates.Select(t =>
                (BaseUrl + t)
                    .Replace("{ID}", unidade)
                    .Replace("{START_DATE}", format(p.StartDate))
                    .Replace("{END_DATE}", format(p.EndDate))
            ).ToList();
            setups.Add(new StrategySetup { StartDate = p.StartDate, EndDate = p.EndDate, Urls = urls });
        }

        return new StrategyResult
        {
            Summary = new StrategySummary { StartDate = startDate, FinalDate = endDate },
            Setup = setups
        };
    }

    private static List<(DateTime StartDate, DateTime EndDate)> GetMonthlyPeriods(DateTime startDate, DateTime endDate)
    {
        var periods = new List<(DateTime, DateTime)>();
        var year = startDate.Year;
        var month = startDate.Month;
        var endYear = endDate.Year;
        var endMonth = endDate.Month;
        while (year < endYear || (year == endYear && month <= endMonth))
        {
            var isStartMonth = year == startDate.Year && month == startDate.Month;
            var firstDay = new DateTime(year, month, isStartMonth ? startDate.Day : 1);
            var lastDay = new DateTime(year, month, DateTime.DaysInMonth(year, month));
            if (year == endYear && month == endMonth && lastDay > endDate)
            {
                lastDay = endDate;
            }
            periods.Add((firstDay, lastDay));
            if (month == 12) { month = 1; year++; } else { month++; }
        }
        return periods;
    }
}

internal record StrategyResult
{
    public StrategySummary Summary { get; init; } = new();
    public List<StrategySetup> Setup { get; init; } = new();
}

internal record StrategySummary
{
    public DateTime StartDate { get; init; }
    public DateTime FinalDate { get; init; }
}

internal record StrategySetup
{
    public DateTime StartDate { get; init; }
    public DateTime EndDate { get; init; }
    public List<string> Urls { get; init; } = new();
}

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

internal sealed class ExcelParser
{
    // Mapping helpers to handle possible header variants
    private static readonly Dictionary<string, string[]> HeaderMap = new(StringComparer.OrdinalIgnoreCase)
    {
        ["NumeroMtr"] = new [] { "Nº MTR", "NA� MTR", "Nº MTR", "Nº Mtr" },
        ["TipoManifesto"] = new [] { "Tipo Manifesto" },
        ["ResponsavelEmissao"] = new [] { "Responsável Emissão", "ResponsA�vel EmissA�o" },
        ["TemMtrComplementar"] = new [] { "Tem MTR Complementar" },
        ["NumeroMtrProvisorio"] = new [] { "MTR Provisório Nº", "MTR ProvisA3rio NA�" },
        ["DataEmissao"] = new [] { "Data de Emissão", "Data de EmissA�o" },
        ["DataRecebimento"] = new [] { "Data de Recebimento" },
        ["Situacao"] = new [] { "Situação", "SituaAA�o", "Situacao" },
        ["ResponsavelRecebimento"] = new [] { "Responsável Recebimento", "ResponsA�vel Recebimento" },
        ["Justificativa"] = new [] { "Justificativa" },
        ["Tratamento"] = new [] { "Tratamento" },
        ["NumeroCdf"] = new [] { "CDF Nº", "CDF NA�" },
        ["Residuo_CodigoInterno"] = new [] { "Cód Interno", "CA3d Interno" },
        ["Residuo_Descricao"] = new [] { "Resíduo Cód/Descrição", "ResA-duo CA3d/DescriAA�o" },
        ["Residuo_DescricaoInterna"] = new [] { "Descr. interna" },
        ["Residuo_Classe"] = new [] { "Classe" },
        ["Residuo_Unidade"] = new [] { "Unidade" },
        ["Residuo_QtdIndicada"] = new [] { "Quantidade indicada" },
        ["Residuo_QtdRecebida"] = new [] { "Quantidade recebida" },
        ["Gerador_Unidade"] = new [] { "Gerador (Unidade)" },
        ["Gerador_CpfCnpj"] = new [] { "Gerador (CNPJ/CPF)" },
        ["Gerador_Nome"] = new [] { "Gerador (Nome)" },
        ["Gerador_Obs"] = new [] { "Observação Gerador", "ObservaAA�o Gerador" },
        ["Transportador_Unidade"] = new [] { "Transportador (Unidade)" },
        ["Transportador_CpfCnpj"] = new [] { "Transportador (CNPJ/CPF)" },
        ["Transportador_Nome"] = new [] { "Transportador (Nome)" },
        ["Transportador_Motorista"] = new [] { "Nome Motorista" },
        ["Transportador_Placa"] = new [] { "Placa Veículo", "Placa VeA-culo" },
        ["Destinador_Unidade"] = new [] { "Destinador (Unidade)" },
        ["Destinador_CpfCnpj"] = new [] { "Destinador (CNPJ/CPF)" },
        ["Destinador_Nome"] = new [] { "Destinador (Nome)" },
        ["Destinador_Obs"] = new [] { "Observação Destinador", "ObservaAA�o Destinador" },
    };

    public static List<MtrRecord> ParseMTRs(string filePath)
    {
        using var wb = new XLWorkbook(filePath);
        if (!wb.Worksheets.Any()) return new();
        var ws = wb.Worksheets.First();
        var used = ws.RangeUsed();
        if (used == null) return new();
        var header = ws.Row(1);
        var lastHeaderCell = header.LastCellUsed();
        if (lastHeaderCell == null) return new();
        var headers = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (int c = 1; c <= lastHeaderCell.Address.ColumnNumber; c++)
        {
            var txt = header.Cell(c).GetString().Trim();
            if (!string.IsNullOrEmpty(txt)) headers[txt] = c;
        }
        if (headers.Count == 0) return new();

        var lastRow = ws.LastRowUsed();
        if (lastRow == null || lastRow.RowNumber() < 2) return new();

        int Col(string key)
        {
            foreach (var cand in HeaderMap[key])
                if (headers.TryGetValue(cand, out var idx)) return idx;
            return -1;
        }

        var rows = new List<MtrRecord?>();
        var indexByNumero = new Dictionary<string, int>();
        for (int r = 2; r <= lastRow.RowNumber(); r++)
        {
            string Cell(int col) => col > 0 ? ws.Cell(r, col).GetString().Trim() : string.Empty;

            var residuo = new Residuo
            {
                CodigoInterno = Nullify(Cell(Col("Residuo_CodigoInterno"))),
                Descricao = Cell(Col("Residuo_Descricao")),
                DescricaoInterna = Nullify(Cell(Col("Residuo_DescricaoInterna"))),
                Classe = Cell(Col("Residuo_Classe")),
                Unidade = Cell(Col("Residuo_Unidade")),
                QuantidadeIndicada = ParseNullableDouble(Cell(Col("Residuo_QtdIndicada"))) ?? 0d,
                QuantidadeRecebida = ParseNullableDouble(Cell(Col("Residuo_QtdRecebida")))
            };

            var gerador = new Pessoa { Unidade = Cell(Col("Gerador_Unidade")), CpfCnpj = Cell(Col("Gerador_CpfCnpj")), Nome = Cell(Col("Gerador_Nome")), Observacao = Nullify(Cell(Col("Gerador_Obs"))) };
            var transportador = new PessoaComVeiculo { Unidade = Cell(Col("Transportador_Unidade")), CpfCnpj = Cell(Col("Transportador_CpfCnpj")), Nome = Cell(Col("Transportador_Nome")), Motorista = Nullify(Cell(Col("Transportador_Motorista"))), PlacaVeiculo = Nullify(Cell(Col("Transportador_Placa"))) };
            var destinador = new Pessoa { Unidade = Cell(Col("Destinador_Unidade")), CpfCnpj = Cell(Col("Destinador_CpfCnpj")), Nome = Cell(Col("Destinador_Nome")), Observacao = Nullify(Cell(Col("Destinador_Obs"))) };

            var numero = Cell(Col("NumeroMtr"));
            if (string.IsNullOrWhiteSpace(numero)) continue;

            if (!indexByNumero.TryGetValue(numero, out var idx))
            {
                var rec = new MtrRecord
                {
                    Numero = numero,
                    TipoManifesto = Cell(Col("TipoManifesto")),
                    ResponsavelEmissao = Cell(Col("ResponsavelEmissao")),
                    TemMTRComplementar = Nullify(Cell(Col("TemMtrComplementar"))),
                    NumeroMtrProvisorio = Nullify(Cell(Col("NumeroMtrProvisorio"))),
                    DataEmissao = Cell(Col("DataEmissao")),
                    DataRecebimento = Nullify(Cell(Col("DataRecebimento"))),
                    Situacao = Cell(Col("Situacao")),
                    ResponsavelRecebimento = Nullify(Cell(Col("ResponsavelRecebimento"))),
                    Justificativa = Nullify(Cell(Col("Justificativa"))),
                    Tratamento = Cell(Col("Tratamento")),
                    NumeroCdf = Nullify(Cell(Col("NumeroCdf"))),
                    Residuos = new List<Residuo> { residuo },
                    Gerador = gerador,
                    Transportador = transportador,
                    Destinador = destinador
                };
                rows.Add(rec);
                indexByNumero[numero] = rows.Count - 1;
            }
            else
            {
                (rows[idx]!.Residuos).Add(residuo);
            }
        }

        return rows.Where(x => x != null).Select(x => x!).ToList();

        static string? Nullify(string s) => string.IsNullOrWhiteSpace(s) ? null : s;
        static double? ParseNullableDouble(string s) => double.TryParse(s.Replace(',', '.'), System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var d) ? d : (double?)null;
    }
}

// Models
internal sealed class Stakeholder
{
    public string Unidade { get; set; } = string.Empty;
    public string CpfCnpj { get; set; } = string.Empty;
    public string Nome { get; set; } = string.Empty;
    public DateTime? DataInicial { get; set; }
    public DateTime? DataFinal { get; set; }
}

internal sealed class MtrLoad
{
    public string Url { get; set; } = string.Empty;
    public string Unidade { get; set; } = string.Empty;
    public string CreatedBy { get; set; } = string.Empty;
    public DateTime CreatedDt { get; set; }
}

internal sealed class MtrRecord
{
    public string Numero { get; set; } = string.Empty;
    public string TipoManifesto { get; set; } = string.Empty;
    public string ResponsavelEmissao { get; set; } = string.Empty;
    public string? TemMTRComplementar { get; set; }
    public string? NumeroMtrProvisorio { get; set; }
    public string DataEmissao { get; set; } = string.Empty;
    public string? DataRecebimento { get; set; }
    public string Situacao { get; set; } = string.Empty;
    public string? ResponsavelRecebimento { get; set; }
    public string? Justificativa { get; set; }
    public string Tratamento { get; set; } = string.Empty;
    public string? NumeroCdf { get; set; }
    public List<Residuo> Residuos { get; set; } = new();
    public Pessoa Gerador { get; set; } = new();
    public PessoaComVeiculo Transportador { get; set; } = new();
    public Pessoa Destinador { get; set; } = new();
}

internal class Pessoa
{
    public string Unidade { get; set; } = string.Empty;
    public string CpfCnpj { get; set; } = string.Empty;
    public string Nome { get; set; } = string.Empty;
    public string? Observacao { get; set; }
}

internal sealed class PessoaComVeiculo : Pessoa
{
    public string? Motorista { get; set; }
    public string? PlacaVeiculo { get; set; }
}

internal sealed class Residuo
{
    public string? CodigoInterno { get; set; }
    public string? Descricao { get; set; }
    public string? DescricaoInterna { get; set; }
    public string? Classe { get; set; }
    public string? Unidade { get; set; }
    public double QuantidadeIndicada { get; set; }
    public double? QuantidadeRecebida { get; set; }
}
