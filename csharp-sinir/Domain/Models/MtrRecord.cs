namespace Sinir.Integration.Local.Domain;

internal sealed class MtrRecord
{
    public string Numero { get; set; } = string.Empty;
    public string TipoManifesto { get; set; } = string.Empty;
    public string ResponsavelEmissao { get; set; } = string.Empty;
    public string? TemMTRComplementar { get; set; }
    public string? NumeroMtrProvisorio { get; set; }
    public string DataEmissao { get; set; } = string.Empty;
    public string? DataRecebimento { get; set; }
    public string Situacao { get; set; } = string.Empty;
    public string? ResponsavelRecebimento { get; set; }
    public string? Justificativa { get; set; }
    public string Tratamento { get; set; } = string.Empty;
    public string? NumeroCdf { get; set; }
    public List<Residuo> Residuos { get; set; } = new();
    public Pessoa Gerador { get; set; } = new();
    public PessoaComVeiculo Transportador { get; set; } = new();
    public Pessoa Destinador { get; set; } = new();
}

