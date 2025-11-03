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
        var mode = (Environment.GetEnvironmentVariable("SINIR_PROCESS_MODE") ?? "disk").ToLowerInvariant();
        var saveToDisk = mode != "memory";
        // Console.WriteLine($"[{DateTime.Now:O}] Processing mode: {(saveToDisk ? "disk" : "memory")}");
        if (saveToDisk)
        {
            // Console.WriteLine($"[{DateTime.Now:O}] Using MTRS directory: {mtrsRoot}");
            Directory.CreateDirectory(mtrsRoot);
        }

        var cmd = args.FirstOrDefault()?.ToLowerInvariant() ?? "run";
        switch (cmd)
        {
            case "setup":
                await Runner.SetupAsync(config);
                break;
            case "process":
                await Runner.ProcessUntilEmptyAsync(config, mtrsRoot, saveToDisk);
                break;
            case "run":
            default:
                await Runner.SetupAsync(config);
                await Runner.ProcessUntilEmptyAsync(config, mtrsRoot, saveToDisk);
                break;
        }

        return 0;
    }
}
