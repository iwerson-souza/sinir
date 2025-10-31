namespace Sinir.Integration.Local.Strategy;

internal static class SinirStrategy
{
    private static readonly string BaseUrl = "https://mtr.sinir.gov.br/api/mtr/pesquisaManifestoRelatorioMtrAnalitico";
    private static readonly string[] Templates = new[]
    {
        "/{ID}/18/8/{START_DATE}/{END_DATE}/5/0/9/0",
        "/{ID}/18/5/{START_DATE}/{END_DATE}/8/0/9/0",
        "/{ID}/18/9/{START_DATE}/{END_DATE}/8/0/5/0"
    };

    public static StrategyResult BuildStrategy(string unidade, DateTime? lastEndDate)
    {
        var startDate = lastEndDate.HasValue
            ? new DateTime(lastEndDate.Value.Year, lastEndDate.Value.Month, lastEndDate.Value.Day).AddDays(1)
            : new DateTime(2020, 1, 1);
        startDate = startDate.Date;

        var now = DateTime.UtcNow.Date;
        var endDate = now.AddDays(-1);

        var periods = GetMonthlyPeriods(startDate, endDate);
        var format = new Func<DateTime, string>(d => $"{d:dd-MM-yyyy}");

        var setups = new List<StrategySetup>();
        foreach (var p in periods)
        {
            var urls = Templates.Select(t =>
                (BaseUrl + t)
                    .Replace("{ID}", unidade)
                    .Replace("{START_DATE}", format(p.StartDate))
                    .Replace("{END_DATE}", format(p.EndDate))
            ).ToList();
            setups.Add(new StrategySetup { StartDate = p.StartDate, EndDate = p.EndDate, Urls = urls });
        }

        return new StrategyResult
        {
            Summary = new StrategySummary { StartDate = startDate, FinalDate = endDate },
            Setup = setups
        };
    }

    private static List<(DateTime StartDate, DateTime EndDate)> GetMonthlyPeriods(DateTime startDate, DateTime endDate)
    {
        var periods = new List<(DateTime, DateTime)>();
        var year = startDate.Year;
        var month = startDate.Month;
        var endYear = endDate.Year;
        var endMonth = endDate.Month;
        while (year < endYear || (year == endYear && month <= endMonth))
        {
            var isStartMonth = year == startDate.Year && month == startDate.Month;
            var firstDay = new DateTime(year, month, isStartMonth ? startDate.Day : 1);
            var lastDay = new DateTime(year, month, DateTime.DaysInMonth(year, month));
            if (year == endYear && month == endMonth && lastDay > endDate)
            {
                lastDay = endDate;
            }
            periods.Add((firstDay, lastDay));
            if (month == 12) { month = 1; year++; } else { month++; }
        }
        return periods;
    }
}

internal record StrategyResult
{
    public StrategySummary Summary { get; init; } = new();
    public List<StrategySetup> Setup { get; init; } = new();
}

internal record StrategySummary
{
    public DateTime StartDate { get; init; }
    public DateTime FinalDate { get; init; }
}

internal record StrategySetup
{
    public DateTime StartDate { get; init; }
    public DateTime EndDate { get; init; }
    public List<string> Urls { get; init; } = new();
}

