using Resilead.Integration.Local.Configuration;
using Resilead.Integration.Local.Infrastructure;

namespace Resilead.Integration.Local;

internal class Program
{
    private static async Task<int> Main(string[] args)
    {
        Console.OutputEncoding = System.Text.Encoding.UTF8;
        var config = AppConfig.Load();
        var cmd = args.FirstOrDefault()?.ToLowerInvariant() ?? "help";

        switch (cmd)
        {
            case "ref-load":
                await new RefDataLoader(config).RunAsync();
                break;
            case "stakeholder":
                await new StakeholderEtl(config).RunAsync();
                break;
            case "mtr":
                await new MtrEtl(config).RunAsync();
                break;
            default:
                Console.WriteLine("Usage: csharp-resilead <ref-load|stakeholder|mtr>");
                break;
        }

        return 0;
    }
}

