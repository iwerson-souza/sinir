using System.Net;
using System.Net.Http;
using System.Threading;
using Sinir.Integration.Local.Configuration;
using Sinir.Integration.Local.Infrastructure;
using Sinir.Integration.Local.Parsing;
using Sinir.Integration.Local.Strategy;

namespace Sinir.Integration.Local.Application;

internal static class DiscoveryRunner
{
    private static readonly DateTime DiscoveryStartDate = new(2026, 4, 1);
    private static readonly DateTime DiscoveryEndDate = new(2026, 4, 30);

    public static async Task RunAsync(AppConfig config)
    {
        var svc = new IntegrationService(config.ConnectionString);
        var units = LoadCandidateUnits()
            .Select(x => x.Trim())
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct()
            .OrderBy(ParseUnitNumber)
            .ToList();

        if (units.Count == 0)
        {
            Console.WriteLine("[Discovery] No candidate units found.");
            return;
        }

        var lastProcessed = await svc.GetMaxStakeholderDiscoveryUnitAsync();
        var lastProcessedNumber = ParseUnitNumber(lastProcessed);
        var pending = units
            .Where(x => ParseUnitNumber(x) > lastProcessedNumber)
            .ToList();

        Console.WriteLine($"[Discovery] Candidate units loaded: {units.Count}.");
        Console.WriteLine($"[Discovery] Last processed unit in DB: {(string.IsNullOrWhiteSpace(lastProcessed) ? "<none>" : lastProcessed)}.");
        Console.WriteLine($"[Discovery] Units pending from CSV: {pending.Count}.");

        if (pending.Count == 0)
        {
            Console.WriteLine("[Discovery] Nothing to do.");
            return;
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

        var processed = 0;
        foreach (var unidade in pending)
        {
            processed++;
            Console.WriteLine($"[Discovery] {processed}/{pending.Count} testing unidade {unidade}...");
            var result = await DiscoverUnitAsync(http, unidade, TimeSpan.FromSeconds(config.RequestTimeoutSeconds));

            await svc.UpsertStakeholderDiscoveryAsync(
                unidade,
                DiscoveryStartDate,
                DiscoveryEndDate,
                result.Tested,
                result.HasData,
                "system");

            Console.WriteLine(
                $"[Discovery] Unidade {unidade} finished. tested={result.Tested}, has_data={result.HasData}, urls_ok={result.SuccessfulUrls}/3.");
        }

        Console.WriteLine("[Discovery] Completed.");
    }

    private static async Task<DiscoveryResult> DiscoverUnitAsync(HttpClient http, string unidade, TimeSpan perRequestTimeout)
    {
        var urls = SinirStrategy.BuildUrls(unidade, DiscoveryStartDate, DiscoveryEndDate);
        var successfulUrls = 0;
        var hasData = false;

        foreach (var url in urls)
        {
            Console.WriteLine($"[Discovery] GET {url}");
            using var cts = new CancellationTokenSource(perRequestTimeout);
            try
            {
                using var response = await http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, cts.Token);
                response.EnsureSuccessStatusCode();
                var data = await response.Content.ReadAsByteArrayAsync();
                successfulUrls++;

                List<Domain.MtrRecord> mtrs;
                try
                {
                    mtrs = ExcelParser.ParseMTRs(data);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Discovery] Parse issue for unidade {unidade}: {ex.Message}. Treating as empty report.");
                    mtrs = new List<Domain.MtrRecord>();
                }

                if (mtrs.Count > 0)
                {
                    hasData = true;
                    Console.WriteLine($"[Discovery] Unidade {unidade} returned {mtrs.Count} MTR(s).");
                }
                else
                {
                    Console.WriteLine($"[Discovery] Unidade {unidade} returned no data for this URL.");
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine($"[Discovery] Timeout after {perRequestTimeout.TotalSeconds:F0}s for unidade {unidade}: {url}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Discovery] Request failed for unidade {unidade}: {ex.Message}");
            }
        }

        return new DiscoveryResult
        {
            Tested = successfulUrls == urls.Count,
            HasData = hasData,
            SuccessfulUrls = successfulUrls
        };
    }

    private static List<string> LoadCandidateUnits()
    {
        var solutionRoot = Paths.TryFindSolutionRoot()
            ?? throw new DirectoryNotFoundException("Solution root not found.");
        var csvPath = Path.Combine(solutionRoot, "csharp-sinir", "Data", "unidades_possiveis.csv");
        if (!File.Exists(csvPath))
        {
            throw new FileNotFoundException("Discovery CSV not found.", csvPath);
        }

        return File.ReadAllLines(csvPath)
            .Select(x => x.Trim())
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .ToList();
    }

    private static long ParseUnitNumber(string? value)
    {
        return long.TryParse(value, out var number) ? number : long.MinValue;
    }

    private sealed class DiscoveryResult
    {
        public bool Tested { get; init; }
        public bool HasData { get; init; }
        public int SuccessfulUrls { get; init; }
    }
}
