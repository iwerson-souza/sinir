using System.Text.RegularExpressions;

namespace Resilead.Integration.Local.Infrastructure;

internal static class Normalization
{
    private static readonly Regex NonDigits = new("[^0-9]", RegexOptions.Compiled);
    private static readonly Regex DangerousMark = new(@"\(\*\)", RegexOptions.Compiled);

    public static string Clean(string? s) => (s ?? string.Empty).Trim();

    public static string? CleanOrNull(string? s)
    {
        var cleaned = Clean(s);
        return cleaned.Length == 0 ? null : cleaned;
    }

    public static string OnlyDigits(string? s) => NonDigits.Replace(s ?? string.Empty, "");

    public static bool HasDangerousMark(string? s) => DangerousMark.IsMatch(s ?? string.Empty);

    public static string DeriveResiduoCodigo(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return string.Empty;
        var left = raw.Split('-', 2)[0];
        var onlyDigits = OnlyDigits(left);
        return onlyDigits?.Length == 0 ? left : onlyDigits;
    }

    public static double Similarity(string a, string b)
    {
        a = NormalizeName(a);
        b = NormalizeName(b);
        if (a.Length == 0 && b.Length == 0) return 1.0;
        if (a.Length == 0 || b.Length == 0) return 0.0;
        var dist = LevenshteinDistance(a, b);
        var maxLen = Math.Max(a.Length, b.Length);
        return 1.0 - (double)dist / maxLen;
    }

    public static string NormalizeName(string s)
    {
        s = (s ?? string.Empty).Trim().ToUpperInvariant();
        s = Regex.Replace(s, @"\s+", " ");
        return s;
    }

    private static int LevenshteinDistance(string s, string t)
    {
        var n = s.Length; var m = t.Length;
        var d = new int[n + 1, m + 1];
        for (var i = 0; i <= n; i++) d[i, 0] = i;
        for (var j = 0; j <= m; j++) d[0, j] = j;
        for (var i = 1; i <= n; i++)
        {
            for (var j = 1; j <= m; j++)
            {
                var cost = s[i - 1] == t[j - 1] ? 0 : 1;
                d[i, j] = Math.Min(
                    Math.Min(d[i - 1, j] + 1, d[i, j - 1] + 1),
                    d[i - 1, j - 1] + cost);
            }
        }
        return d[n, m];
    }
}
