using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using Sinir.Integration.Local.Configuration;
using Sinir.Integration.Local.Domain;
using Sinir.Integration.Local.Infrastructure;

namespace Sinir.Integration.Local.Application;

internal static class AddressProcessor
{
    private const string PartnerEndpointTemplate = "https://mtr.sinir.gov.br/api/mtr/consultaParceiro/J/{0}";
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public static async Task RunAsync(AppConfig config)
    {
        var svc = new IntegrationService(config.ConnectionString);
        var handler = new SocketsHttpHandler
        {
            MaxConnectionsPerServer = config.MaxConnectionsPerServer,
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
            PooledConnectionLifetime = TimeSpan.FromMinutes(5),
            UseCookies = false
        };

        using var http = new HttpClient(handler)
        {
            Timeout = Timeout.InfiniteTimeSpan,
            DefaultRequestVersion = config.UseHttp2 ? HttpVersion.Version20 : HttpVersion.Version11
        };
        http.DefaultRequestHeaders.UserAgent.Clear();
        http.DefaultRequestHeaders.UserAgent.ParseAdd(config.UserAgent);

        var totalPersisted = 0;
        var rounds = 0;
        while (true)
        {
            var cnpjs = await svc.ListDistinctCnpjsMissingAddressAsync(config.BatchSize);
            if (cnpjs.Count == 0)
            {
                if (rounds == 0)
                {
                    Console.WriteLine("[address] Nenhum CNPJ pendente de endereço.");
                }
                else
                {
                    Console.WriteLine($"[address] Fila drenada após {rounds} rodada(s).");
                }
                break;
            }

            rounds++;
            Console.WriteLine($"[address] Rodada {rounds}: processando {cnpjs.Count} CNPJ(s) com DOP={config.MaxDegreeOfParallelism}.");
            var resolved = await ResolveAddressesAsync(
                cnpjs,
                http,
                TimeSpan.FromSeconds(config.RequestTimeoutSeconds),
                config.MaxDegreeOfParallelism);
            if (resolved.Count == 0)
            {
                Console.WriteLine("[address] Nenhum endereço encontrado nesta rodada. Encerrando para evitar loop infinito.");
                break;
            }

            await svc.UpsertStakeholderAddressesAsync(resolved, "address-mode");
            totalPersisted += resolved.Count;
            Console.WriteLine($"[address] Rodada {rounds} concluída. Registros aplicados: {resolved.Count}. Total acumulado: {totalPersisted}.");
        }

        Console.WriteLine($"[address] Finalizado. Registros com endereço persistido: {totalPersisted}.");
    }

    private static async Task<List<Stakeholder>> ResolveAddressesAsync(
        IReadOnlyCollection<string> cnpjs,
        HttpClient http,
        TimeSpan timeoutPerRequest,
        int maxDegreeOfParallelism)
    {
        var map = new ConcurrentDictionary<string, Stakeholder>();
        if (maxDegreeOfParallelism < 1) maxDegreeOfParallelism = Environment.ProcessorCount;
        var semaphore = new SemaphoreSlim(maxDegreeOfParallelism, maxDegreeOfParallelism);
        var tasks = new List<Task>();

        foreach (var rawCnpj in cnpjs)
        {
            var cnpj = OnlyDigits(rawCnpj);
            if (cnpj.Length != 14)
            {
                continue;
            }

            await semaphore.WaitAsync();
            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    var stakeholders = await FetchPartnersAsync(http, cnpj, timeoutPerRequest);
                    foreach (var s in stakeholders)
                    {
                        var key = $"{s.Unidade}|{s.CpfCnpj}";
                        map[key] = s;
                    }
                    if (stakeholders.Count == 0)
                    {
                        Console.WriteLine($"[address] Nenhum parceiro retornado para {cnpj}.");
                    }
                    else
                    {
                        Console.WriteLine($"[address] {cnpj}: {stakeholders.Count} endereço(s) obtido(s).");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[address] Erro ao consultar {cnpj}: {ex.Message}");
                }
                finally
                {
                    semaphore.Release();
                }
            }));
        }

        await Task.WhenAll(tasks);
        return map.Values.ToList();
    }

    private static async Task<List<Stakeholder>> FetchPartnersAsync(HttpClient http, string cnpj, TimeSpan timeout)
    {
        var url = string.Format(PartnerEndpointTemplate, cnpj);
        using var cts = new CancellationTokenSource(timeout);
        using var response = await http.GetAsync(url, cts.Token);
        response.EnsureSuccessStatusCode();
        await using var stream = await response.Content.ReadAsStreamAsync(cts.Token);
        var payload = await JsonSerializer.DeserializeAsync<PartnerResponse>(stream, JsonOptions, cts.Token);
        if (payload == null)
        {
            return new List<Stakeholder>();
        }

        if (payload.Erro)
        {
            var msg = payload.Mensagem ?? "Erro desconhecido";
            throw new InvalidOperationException($"API retornou erro para {cnpj}: {msg}");
        }

        var parceiros = payload.ObjetoResposta ?? new List<PartnerDto>();
        var grouped = parceiros
            .Where(p => p.ParCodigo.HasValue)
            .GroupBy(p => p.ParCodigo!.Value)
            .Select(g => g.First());

        var list = new List<Stakeholder>();
        foreach (var partner in grouped)
        {
            var partnerCnpj = OnlyDigits(partner.JurCnpj);
            if (partnerCnpj.Length != 14) continue;

            var endereco = partner.PaeEndereco?.Trim();
            if (string.IsNullOrEmpty(endereco)) continue;

            var unidade = partner.ParCodigo!.Value.ToString();
            var nome = string.IsNullOrWhiteSpace(partner.ParDescricao) ? "SINIR PARCEIRO" : partner.ParDescricao!.Trim();

            list.Add(new Stakeholder
            {
                Unidade = unidade,
                CpfCnpj = partnerCnpj,
                Nome = nome,
                Endereco = endereco
            });
        }

        return list;
    }

    private static string OnlyDigits(string? input)
    {
        if (string.IsNullOrEmpty(input)) return string.Empty;
        Span<char> buffer = stackalloc char[input.Length];
        var idx = 0;
        foreach (var ch in input)
        {
            if (char.IsDigit(ch))
            {
                buffer[idx++] = ch;
            }
        }
        return new string(buffer[..idx]);
    }

    private sealed class PartnerResponse
    {
        [JsonPropertyName("mensagem")]
        public string? Mensagem { get; set; }

        [JsonPropertyName("objetoResposta")]
        public List<PartnerDto>? ObjetoResposta { get; set; }

        [JsonPropertyName("erro")]
        public bool Erro { get; set; }
    }

    private sealed class PartnerDto
    {
        [JsonPropertyName("parCodigo")]
        public long? ParCodigo { get; set; }

        [JsonPropertyName("parDescricao")]
        public string? ParDescricao { get; set; }

        [JsonPropertyName("jurCnpj")]
        public string? JurCnpj { get; set; }

        [JsonPropertyName("paeEndereco")]
        public string? PaeEndereco { get; set; }
    }
}
