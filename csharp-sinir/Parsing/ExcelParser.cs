using ClosedXML.Excel;
using Sinir.Integration.Local.Domain;

namespace Sinir.Integration.Local.Parsing;

internal static class ExcelParser
{
    public static List<MtrRecord> ParseMTRs(string filePath)
    {
        using var wb = new XLWorkbook(filePath);
        var ws = wb.Worksheets
            .OrderByDescending(s => (s.LastRowUsed()?.RowNumber() ?? 0) * (s.LastColumnUsed()?.ColumnNumber() ?? 0))
            .First();
        var chosenRows = ws.LastRowUsed()?.RowNumber() ?? 0;
        var chosenCols = ws.LastColumnUsed()?.ColumnNumber() ?? 0;
        Console.WriteLine($"[ExcelParser] Using sheet '{ws.Name}' with {chosenRows} rows and {chosenCols} cols");
        var rows = new List<MtrRecord>();
        var indexByNumero = new Dictionary<string, int>();

        // Try to detect the header row by scanning the first N rows
        int lastRow = ws.LastRowUsed()?.RowNumber() ?? 0;
        int lastCol = ws.LastColumnUsed()?.ColumnNumber() ?? 0;
        int headerRowIndex = 1;
        int bestScore = -1;
        var expectedHints = new[]
        {
            "numero", "mtr", "tipo", "emissao", "emissão", "situacao", "situação",
            "gerador", "transportador", "destinador", "cpf", "cnpj", "residuo", "resíduo"
        };
        for (int r = 1; r <= Math.Min(20, lastRow); r++)
        {
            int score = 0;
            for (int c = 1; c <= lastCol; c++)
            {
                var val = ws.Cell(r, c).GetString()?.Trim() ?? string.Empty;
                if (string.IsNullOrEmpty(val)) continue;
                var nv = Normalize(val);
                if (expectedHints.Any(h => nv.Contains(Normalize(h)))) score++;
            }
            if (score > bestScore)
            {
                bestScore = score;
                headerRowIndex = r;
            }
        }

        var header = ws.Row(headerRowIndex);
        var cols = new Dictionary<string, int>();
        for (int c = 1; c <= lastCol; c++)
        {
            var key = header.Cell(c).GetString().Trim();
            if (!string.IsNullOrWhiteSpace(key) && !cols.ContainsKey(key))
                cols[key] = c;
        }
        Console.WriteLine($"[ExcelParser] Header row chosen: {headerRowIndex}. Columns detected: {cols.Count}");
        try
        {
            Console.WriteLine("[ExcelParser] Header columns: " + string.Join(", ", cols.Keys.Take(40)) + (cols.Count > 40 ? ", ..." : string.Empty));
        }
        catch { }

        static string Normalize(string s)
        {
            var formD = s.Normalize(System.Text.NormalizationForm.FormD);
            var filtered = new System.Text.StringBuilder();
            foreach (var ch in formD)
            {
                var uc = System.Globalization.CharUnicodeInfo.GetUnicodeCategory(ch);
                if (uc != System.Globalization.UnicodeCategory.NonSpacingMark && char.IsLetterOrDigit(ch))
                    filtered.Append(char.ToLowerInvariant(ch));
            }
            return filtered.ToString();
        }

        int Col(params string[] names)
        {
            // exact
            foreach (var n in names)
            {
                if (cols.TryGetValue(n, out var idx)) return idx;
                var kv = cols.FirstOrDefault(k => string.Equals(k.Key, n, StringComparison.OrdinalIgnoreCase));
                if (kv.Value > 0) return kv.Value;
            }
            // fuzzy by normalized contains
            var normalizedCols = cols.ToDictionary(k => Normalize(k.Key), v => v.Value);
            foreach (var n in names)
            {
                var nn = Normalize(n);
                var match = normalizedCols.FirstOrDefault(k => k.Key.Contains(nn));
                if (match.Value > 0) return match.Value;
            }
            return -1;
        }

        // Pre-resolve key columns to aid diagnostics
        int colNumero = Col("Nº MTR", "N° MTR", "NumeroMtr", "Numero MTR", "Número MTR", "Numero do Manifesto", "Número do Manifesto", "Numero", "Número");
        int colTipo = Col("TipoManifesto", "Tipo de Manifesto", "Tipo");
        int colDataEmissao = Col("DataEmissao", "Data de Emissao", "Data de Emissão");
        int colSituacao = Col("Situacao", "Situação");
        Console.WriteLine($"[ExcelParser] Cols -> Numero:{colNumero} Tipo:{colTipo} Emissao:{colDataEmissao} Situacao:{colSituacao}");

        var dataStartRow = headerRowIndex + 1;
        int samples = 0;
        for (int r = dataStartRow; r <= Math.Min(dataStartRow + 5, lastRow); r++)
        {
            string S(int idx) => idx <= 0 ? string.Empty : ws.Cell(r, idx).GetString();
            var smp = S(colNumero);
            if (!string.IsNullOrWhiteSpace(smp) && samples < 3)
            {
                Console.WriteLine($"[ExcelParser] Sample numero at row {r}: '{smp}'");
                samples++;
            }
        }

        for (int curRow = dataStartRow; curRow <= lastRow; curRow++)
        {
            string Cell(int index) => index <= 0 ? string.Empty : ws.Cell(curRow, index).GetString();
            var residuo = new Residuo
            {
                CodigoInterno = Nullify(Cell(Col("Residuo_CodigoInterno", "CodigoInterno", "Codigo Interno", "Cód Interno"))),
                Descricao = Nullify(Cell(Col("Resíduo Cód/Descrição", "Residuo_Descricao", "Descricao", "Descrição"))),
                DescricaoInterna = Nullify(Cell(Col("Descr. interna", "Residuo_DescricaoInterna", "DescricaoInterna", "Descrição Interna"))),
                Classe = Nullify(Cell(Col("Residuo_Classe", "Classe"))),
                Unidade = Cell(Col("Residuo_Unidade", "Unidade")),
                QuantidadeIndicada = ParseNullableDouble(Cell(Col("Residuo_QtdIndicada", "QtdIndicada", "Quantidade Indicada"))) ?? 0d,
                QuantidadeRecebida = ParseNullableDouble(Cell(Col("Residuo_QtdRecebida", "QtdRecebida", "Quantidade Recebida")))
            };

            var gerador = new Pessoa { Unidade = Cell(Col("Gerador (Unidade)", "Gerador_Unidade", "Gerador_UnidadeFederativa", "Gerador UF", "Gerador Unidade")), CpfCnpj = Cell(Col("Gerador (CNPJ/CPF)", "Gerador_CpfCnpj", "Gerador_Cpf", "Gerador Cpf/Cnpj")), Nome = Cell(Col("Gerador (Nome)", "Gerador_Nome", "Gerador Nome")), Observacao = Nullify(Cell(Col("Observação Gerador", "Gerador_Obs", "Gerador Observacao", "Gerador Observação"))) };
            var transportador = new PessoaComVeiculo { Unidade = Cell(Col("Transportador (Unidade)", "Transportador_Unidade", "Transportador UF", "Transportador Unidade")), CpfCnpj = Cell(Col("Transportador (CNPJ/CPF)", "Transportador_CpfCnpj", "Transportador Cpf/Cnpj")), Nome = Cell(Col("Transportador (Nome)", "Transportador_Nome", "Transportador Nome")), Motorista = Nullify(Cell(Col("Nome Motorista", "Transportador_Motorista", "Motorista"))), PlacaVeiculo = Nullify(Cell(Col("Placa Veículo", "Transportador_Placa", "Placa"))) };
            var destinador = new Pessoa { Unidade = Cell(Col("Destinador (Unidade)", "Destinador_Unidade", "Destinador UF", "Destinador Unidade")), CpfCnpj = Cell(Col("Destinador (CNPJ/CPF)", "Destinador_CpfCnpj", "Destinador Cpf/Cnpj")), Nome = Cell(Col("Destinador (Nome)", "Destinador_Nome", "Destinador Nome")), Observacao = Nullify(Cell(Col("Observação Destinador", "Destinador_Obs", "Destinador Observacao", "Destinador Observação"))) };

            var numero = Cell(colNumero);
            if (string.IsNullOrWhiteSpace(numero)) continue;

            if (!indexByNumero.TryGetValue(numero, out var idx))
            {
                var rec = new MtrRecord
                {
                    Numero = numero,
                    TipoManifesto = Cell(Col("TipoManifesto", "Tipo de Manifesto", "Tipo")),
                    ResponsavelEmissao = Cell(Col("Responsável Emissão", "ResponsavelEmissao", "Responsavel Emissao", "Responsável pela Emissão")),
                    TemMTRComplementar = Nullify(Cell(Col("Tem MTR Complementar", "TemMtrComplementar", "Tem MTR Complementar"))),
                    NumeroMtrProvisorio = Nullify(Cell(Col("MTR Provisório Nº", "NumeroMtrProvisorio", "Numero MTR Provisorio", "Número MTR Provisório"))),
                    DataEmissao = Cell(Col("DataEmissao", "Data de Emissao", "Data de Emissão")),
                    DataRecebimento = Nullify(Cell(Col("DataRecebimento", "Data de Recebimento"))),
                    Situacao = Cell(Col("Situacao", "Situação")),
                    ResponsavelRecebimento = Nullify(Cell(Col("Responsável Recebimento", "ResponsavelRecebimento", "Responsavel Recebimento", "Responsável pelo Recebimento"))),
                    Justificativa = Nullify(Cell(Col("Justificativa"))),
                    Tratamento = Cell(Col("Tratamento")),
                    NumeroCdf = Nullify(Cell(Col("CDF Nº", "NumeroCdf", "Numero CDF", "Número CDF"))),
                    Residuos = new List<Residuo> { residuo },
                    Gerador = gerador,
                    Transportador = transportador,
                    Destinador = destinador
                };
                rows.Add(rec);
                indexByNumero[numero] = rows.Count - 1;
            }
            else
            {
                (rows[idx]!.Residuos).Add(residuo);
            }
        }

        return rows.Where(x => x != null).Select(x => x!).ToList();

        static string? Nullify(string s) => string.IsNullOrWhiteSpace(s) ? null : s;
        static double? ParseNullableDouble(string s) => double.TryParse(s.Replace(',', '.'), System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var d) ? d : (double?)null;
    }
}
