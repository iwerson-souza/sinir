using Sinir.Integration.Local.Application;
using Sinir.Integration.Local.Configuration;
using Sinir.Integration.Local.Infrastructure;

namespace Sinir.Integration.Local;

internal class Program

/*
    TODO: o mesmo CNPJ pode ter mais de um parceiro associado (unidade). ?Usar o paeEndereco para diferenciar?
    GET: https://mtr.sinir.gov.br/api/mtr/consultaParceiro/J/00000208000100
    response:
    {
        "mensagem": null,
        "objetoResposta": [
            {
                "parCodigo": 359141,
                "parDescricao": "BRB - Banco de Brasília SA",
                "jurCnpj": "00000208000100",
                "fisCpf": "null",
                "paeEndereco": "SIG Quadra 08, lotes 2327 a 2337  Arquivo Central do BRB, 70610480, Zona Industrial, Brasília/DF",
                "cnp": null
            },
            {
                "parCodigo": 407329,
                "parDescricao": "Banco de Brasília S.A.",
                "jurCnpj": "00000208000100",
                "fisCpf": "null",
                "paeEndereco": "SIG Quadra 08, Lotes 2327 a 2337 - Arquivo Central do BRB, 70610480, Zona Industrial, Brasília/DF",
                "cnp": null
            },
            {
                "parCodigo": 407340,
                "parDescricao": "Banco de Brasília S.A.",
                "jurCnpj": "00000208000100",
                "fisCpf": "null",
                "paeEndereco": "SAUN Quadra 05, C, 70040250, Setor de Autarquias Norte, Brasília/DF",
                "cnp": null
            },
            {
                "parCodigo": 407341,
                "parDescricao": "Banco de Brasília S.A.",
                "jurCnpj": "00000208000100",
                "fisCpf": "null",
                "paeEndereco": "SAUN Quadra 05, C, 70040250, Setor de Autarquias Norte, Brasília/DF",
                "cnp": null
            },
            {
                "parCodigo": 407344,
                "parDescricao": "Banco de Brasília S.A.",
                "jurCnpj": "00000208000100",
                "fisCpf": "null",
                "paeEndereco": "SAUN Quadra 05, C, 70040250, Setor de Autarquias Norte, Brasília/DF",
                "cnp": null
            }
        ],
        "totalRecords": 0,
        "erro": false
    }    
    
*/
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
                await Runner.SetupAsync(config);
                await Runner.ProcessUntilEmptyAsync(config, mtrsRoot, saveToDisk);
                break;
            default:
                break;
        }

        return 0;
    }
}
