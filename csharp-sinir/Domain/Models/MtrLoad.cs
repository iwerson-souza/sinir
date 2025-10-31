namespace Sinir.Integration.Local.Domain;

internal sealed class MtrLoad
{
    public string Url { get; set; } = string.Empty;
    public string Unidade { get; set; } = string.Empty;
    public string CreatedBy { get; set; } = string.Empty;
    public DateTime CreatedDt { get; set; }
}

