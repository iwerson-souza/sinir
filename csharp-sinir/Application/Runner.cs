using System.Net;
using System.Net.Http;
using System.Threading;
using Sinir.Integration.Local.Configuration;
using Sinir.Integration.Local.Domain;
using Sinir.Integration.Local.Infrastructure;
using Sinir.Integration.Local.Parsing;
using Sinir.Integration.Local.Strategy;

namespace Sinir.Integration.Local.Application;

internal static class Runner
{
    public static async Task SetupAsync(AppConfig config)
    {
        var svc = new IntegrationService(config.ConnectionString);

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
                CreatedDt = DateTime.Now
            }));

            await svc.UpdateStakeholderRangeAsync(sh.Unidade, sh.CpfCnpj, strat.Summary.StartDate, strat.Summary.FinalDate);
        }

        Console.WriteLine($"Setup complete. Generated/ensured loads for {stakeholders.Count} stakeholder(s).");
    }

    public static async Task ProcessBatchAsync(AppConfig config, string mtrsRoot)
    {
        var svc = new IntegrationService(config.ConnectionString);

        // Console.WriteLine($"[{DateTime.Now:O}] Fetching up to {config.BatchSize} pending loads from DB...");
        var batch = await svc.ListPendingMtrLoadsAsync(config.BatchSize);
        if (batch.Count == 0)
        {
            // Console.WriteLine($"[{DateTime.Now:O}] No pending loads found.");
            return;
        }

        // Console.WriteLine($"[{DateTime.Now:O}] Processing {batch.Count} load(s) with DOP={config.MaxDegreeOfParallelism}...");
        foreach (var b in batch.Take(5))
        {
            // Console.WriteLine($"[{DateTime.Now:O}] Pending URL: {b.Url}");
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
                    // Console.WriteLine($"[{DateTime.Now:O}] [#{localStart}] Claiming: {load.Url}");
                    var claimed = await svc.TryClaimMtrLoadAsync(load.Url, workerId);
                    if (!claimed)
                    {
                        // Console.WriteLine($"[{DateTime.Now:O}] [#{localStart}] Skipped (already claimed): {load.Url}");
                        return;
                    }

                    var localClaim = Interlocked.Increment(ref claimedCount);
                    // Console.WriteLine($"[{DateTime.Now:O}] [#{localStart}] Claimed OK by {workerId}. Processing...");

                    await ProcessSingleAsync(svc, http, load, localStart, mtrsRoot, TimeSpan.FromSeconds(config.RequestTimeoutSeconds));
                    // Console.WriteLine($"[{DateTime.Now:O}] [#{localStart}] Deleting load: {load.Url}");
                    await svc.DeleteMtrLoadAsync(load.Url);
                    var done = Interlocked.Increment(ref completed);
                    if (done % 5 == 0 || done == batch.Count)
                    {
                        Console.WriteLine($"[{DateTime.Now:O}] Progress: {done}/{batch.Count} completed.");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{DateTime.Now:O}] ERROR while processing {load.Url}: {ex.Message}");
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
        Console.WriteLine($"[{DateTime.Now:O}] Processing completed.");
    }

    private static async Task ProcessSingleAsync(IntegrationService svc, HttpClient http, MtrLoad load, int seq, string mtrsRoot, TimeSpan perRequestTimeout)
    {
        // Console.WriteLine($"[{DateTime.Now:O}] [{seq}] HTTP GET → {load.Url}");
        var sw = System.Diagnostics.Stopwatch.StartNew();
        using var cts = new CancellationTokenSource(perRequestTimeout);
        HttpResponseMessage response;
        try
        {
            response = await http.GetAsync(load.Url, HttpCompletionOption.ResponseHeadersRead, cts.Token);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine($"[{DateTime.Now:O}] [{seq}] HTTP timeout after {perRequestTimeout.TotalSeconds:F0}s → {load.Url}");
            throw;
        }
        response.EnsureSuccessStatusCode();
        var data = await response.Content.ReadAsByteArrayAsync();
        sw.Stop();
        // Console.WriteLine($"[{DateTime.Now:O}] [{seq}] HTTP {((int)response.StatusCode)} in {sw.Elapsed.TotalSeconds:F1}s, {data.Length:N0} bytes");

        var parts = load.Url.Split('/');
        var dataInicial = parts[9].Split('-').Reverse().ToArray();
        var dataFinal = parts[10].Split('-').Reverse().ToArray();
        var filename = $"{load.Unidade}_{string.Join("-", dataInicial)}_{string.Join("-", dataFinal)}_{parts[8]}.xlsx";

        var destDir = Path.Combine(mtrsRoot, load.Unidade);
        Directory.CreateDirectory(destDir);
        var destPath = Path.Combine(destDir, filename);
        await File.WriteAllBytesAsync(destPath, data);
        // Console.WriteLine($"[{DateTime.Now:O}] [{seq}] Saved file: {destPath}");

        // Console.WriteLine($"[{DateTime.Now:O}] [{seq}] Parsing XLSX...");
        var parseSw = System.Diagnostics.Stopwatch.StartNew();
        List<MtrRecord> mtrs;
        try
        {
            mtrs = ExcelParser.ParseMTRs(destPath);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[{DateTime.Now:O}] [{seq}] XLSX parse issue: {ex.Message}. Treating as 0 MTR(s).");
            mtrs = new List<MtrRecord>();
        }
        parseSw.Stop();
        // Console.WriteLine($"[{DateTime.Now:O}] [{seq}] Parsed {mtrs.Count} MTR(s) in {parseSw.Elapsed.TotalSeconds:F1}s");
        if (mtrs.Count == 0)
        {
            Console.WriteLine($"[{DateTime.Now:O}] [{seq}] No MTRs found for {load.Url}");
            return;
        }

        // Console.WriteLine($"[{DateTime.Now:O}] [{seq}] Upserting {mtrs.Count} MTR(s) into DB...");
        await svc.UpsertMtrsAsync(mtrs, "system");
        Console.WriteLine($"[{DateTime.Now:O}] [{seq}] Upsert completed");

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

        // Console.WriteLine($"[{DateTime.Now:O}] [{seq}] Distinct stakeholders to insert: {distinct.Count}");
        if (distinct.Count > 0)
        {
            await svc.InsertStakeholdersIgnoreAsync(distinct.Values.ToList(), "system");
            Console.WriteLine($"[{DateTime.Now:O}] [{seq}] Stakeholders insert-ignore completed");
        }
    }
}
