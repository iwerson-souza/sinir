using System.Net.Http.Headers;
using System.Text.Json;
using Resilead.Integration.Local.Configuration;

namespace Resilead.Integration.Local.Infrastructure;

internal sealed class BrasilApiClient
{
    private readonly HttpClient _http;
    public BrasilApiClient(AppConfig cfg)
    {
        var handler = new SocketsHttpHandler
        {
            PooledConnectionLifetime = TimeSpan.FromMinutes(2),
            MaxConnectionsPerServer = cfg.Processing.Http.MaxConnectionsPerServer,
            AutomaticDecompression = System.Net.DecompressionMethods.GZip | System.Net.DecompressionMethods.Deflate
        };
        _http = new HttpClient(handler)
        {
            Timeout = TimeSpan.FromSeconds(cfg.Processing.Http.RequestTimeoutSeconds)
        };
        // _http.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue(cfg.Processing.UserAgent, ""));
    }

    public async Task<BrasilApiCnpj?> TryGetCnpjAsync(string cnpj, CancellationToken ct)
    {
        try
        {
            var url = $"https://brasilapi.com.br/api/cnpj/v1/{cnpj}";
            using var resp = await _http.GetAsync(url, ct);
            if (!resp.IsSuccessStatusCode) return null;
            var json = await resp.Content.ReadAsStringAsync(ct);
            return JsonSerializer.Deserialize<BrasilApiCnpj>(json);
        }
        catch
        {
            return null;
        }
    }

    public async Task<BrasilApiCepV2?> TryGetCepAsync(string cep, CancellationToken ct)
    {
        try
        {
            var url = $"https://brasilapi.com.br/api/cep/v2/{cep}";
            using var resp = await _http.GetAsync(url, ct);
            if (!resp.IsSuccessStatusCode) return null;
            var json = await resp.Content.ReadAsStringAsync(ct);
            return JsonSerializer.Deserialize<BrasilApiCepV2>(json);
        }
        catch
        {
            return null;
        }
    }
}

internal sealed class BrasilApiCnpj
{
    public string? uf { get; set; }
    public string? cep { get; set; }
    public string? cnpj { get; set; }
    public string? municipio { get; set; }
    public string? logradouro { get; set; }
    public string? numero { get; set; }
    public string? complemento { get; set; }
    public string? bairro { get; set; }
    public string? porte { get; set; }
    public long? cnae_fiscal { get; set; }
    public string? cnae_fiscal_descricao { get; set; }
    public int? codigo_municipio_ibge { get; set; }
    public string? data_inicio_atividade { get; set; }
    public string? razao_social { get; set; }
    public string? nome_fantasia { get; set; }
}

internal sealed class BrasilApiCepV2
{
    public string? cep { get; set; }
    public string? state { get; set; }
    public string? city { get; set; }
    public string? neighborhood { get; set; }
    public string? street { get; set; }
    public BrasilApiLocation? location { get; set; }
}

internal sealed class BrasilApiLocation
{
    public string? type { get; set; }
    public BrasilApiCoordinates? coordinates { get; set; }
}

internal sealed class BrasilApiCoordinates
{
    public string? longitude { get; set; }
    public string? latitude { get; set; }
}
