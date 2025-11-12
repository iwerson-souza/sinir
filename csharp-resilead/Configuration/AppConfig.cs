using System.Text.Json;

namespace Resilead.Integration.Local.Configuration;

internal sealed class AppConfig
{
    public string ConnectionString { get; init; } = string.Empty;
    public string DataDir { get; init; } = "data";
    public StakeholderSection Stakeholder { get; init; } = new();
    public MtrSection Mtr { get; init; } = new();
    public ProcessingSection Processing { get; init; } = new();

    public sealed class StakeholderSection
    {
        public int BatchSize { get; init; } = 1;
        public bool Drain { get; init; } = false;
    }

    public sealed class MtrSection
    {
        public int BatchSize { get; init; } = 1;
        public bool Drain { get; init; } = false;
    }

    public sealed class ProcessingSection
    {
        public int MaxDegreeOfParallelism { get; init; } = 10;
        public int BatchSize { get; init; } = 100;
        public string UserAgent { get; init; } = "ResileadLocal/1.0";
        public HttpSection Http { get; init; } = new();
        public sealed class HttpSection
        {
            public int RequestTimeoutSeconds { get; init; } = 30;
            public int MaxConnectionsPerServer { get; init; } = 8;
            public bool UseHttp2 { get; init; } = false;
        }
    }

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
        var processing = root.GetProperty("Processing");
        var dataDir = root.GetProperty("RefLoad").GetProperty("DataDir").GetString() ?? "data";
        var stakeholder = root.GetProperty("Stakeholder");
        var mtr = root.GetProperty("Mtr");

        return new AppConfig
        {
            ConnectionString = cs,
            DataDir = dataDir,
            Stakeholder = new StakeholderSection
            {
                BatchSize = stakeholder.GetProperty("BatchSize").GetInt32(),
                Drain = stakeholder.GetProperty("Drain").GetBoolean()
            },
            Mtr = new MtrSection
            {
                BatchSize = mtr.GetProperty("BatchSize").GetInt32(),
                Drain = mtr.GetProperty("Drain").GetBoolean()
            },
            Processing = new ProcessingSection
            {
                UserAgent = processing.GetProperty("UserAgent").GetString() ?? "ResileadLocal/1.0",
                Http = new ProcessingSection.HttpSection
                {
                    RequestTimeoutSeconds = processing.GetProperty("Http").GetProperty("RequestTimeoutSeconds").GetInt32(),
                    MaxConnectionsPerServer = processing.GetProperty("Http").GetProperty("MaxConnectionsPerServer").GetInt32(),
                    UseHttp2 = processing.GetProperty("Http").GetProperty("UseHttp2").GetBoolean()
                },
                MaxDegreeOfParallelism = processing.TryGetProperty("MaxDegreeOfParallelism", out var dop) ? dop.GetInt32() : 10,
                BatchSize = processing.TryGetProperty("BatchSize", out var bs) ? bs.GetInt32() : 100
            }
        };
    }
}
