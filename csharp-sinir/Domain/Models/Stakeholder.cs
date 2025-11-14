namespace Sinir.Integration.Local.Domain;

internal sealed class Stakeholder
{
    public string Unidade { get; set; } = string.Empty;
    public string CpfCnpj { get; set; } = string.Empty;
    public string Nome { get; set; } = string.Empty;
    public string? Endereco { get; set; }
    public bool EnderecoVerificado { get; set; }
    public DateTime? DataInicial { get; set; }
    public DateTime? DataFinal { get; set; }
}
