using System.Text.Json;

namespace Sinir.Integration.Local.Configuration;

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

