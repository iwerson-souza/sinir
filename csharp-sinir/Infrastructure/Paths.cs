namespace Sinir.Integration.Local.Infrastructure;

internal static class Paths
{
    public static string? TryFindSolutionRoot()
    {
        var envRoot = Environment.GetEnvironmentVariable("SINIR_SOLUTION_ROOT");
        if (!string.IsNullOrWhiteSpace(envRoot)) return envRoot;

        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        for (int i = 0; i < 6 && dir != null; i++)
        {
            var probe = Path.Combine(dir.FullName, "csharp-sinir", "Sinir.Integration.Local.csproj");
            if (File.Exists(probe)) return dir.FullName;
            dir = dir.Parent!;
        }

        var bin = new DirectoryInfo(AppContext.BaseDirectory);
        var projectDir = bin.Parent?.Parent?.Parent;
        var solutionDir = projectDir?.Parent;
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

