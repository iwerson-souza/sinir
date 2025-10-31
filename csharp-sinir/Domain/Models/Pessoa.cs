namespace Sinir.Integration.Local.Domain;

internal class Pessoa
{
    public string Unidade { get; set; } = string.Empty;
    public string CpfCnpj { get; set; } = string.Empty;
    public string Nome { get; set; } = string.Empty;
    public string? Observacao { get; set; }
}

internal sealed class PessoaComVeiculo : Pessoa
{
    public string? Motorista { get; set; }
    public string? PlacaVeiculo { get; set; }
}

