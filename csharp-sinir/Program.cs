using Sinir.Integration.Local.Application;
using Sinir.Integration.Local.Configuration;
using Sinir.Integration.Local.Infrastructure;

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
                await Runner.SetupAsync(config);
                break;
            case "process":
                await Runner.ProcessBatchAsync(config, mtrsRoot);
                break;
            case "run":
            default:
                await Runner.SetupAsync(config);
                await Runner.ProcessBatchAsync(config, mtrsRoot);
                break;
        }

        return 0;
    }
}

