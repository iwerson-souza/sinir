import { AWSUtils, Config } from 'brasil-residuos-lib-aws-utils';
import { axios } from 'brasil-residuos-lib-http';
import { Logger, LoggerConfig } from 'brasil-residuos-lib-utils';
import ExcelJS from 'exceljs';

import { IntegrationService } from '../../../services';
import { Stakeholder } from '../../../services/integration';

// utils
const logger = new Logger({ component: 'mtr-processor-function' } as LoggerConfig);

// aws-utils
const awsConfig: Config = {
    region: process.env.AWS_REGION,
};
const awsUtils = new AWSUtils(awsConfig.region!);
const dynamodbClient = awsUtils.buildDynamoDB();
const s3Client = awsUtils.buildS3();

// services
const integrationService = new IntegrationService(dynamodbClient);

// TODO: move to a models lib
interface MTR {
    numero: string;
    tipoManifesto: string;
    responsavelEmissao: string;
    temMTRComplementar: string | null;
    numeroMtrProvisorio: string | null;
    dataEmissao: string;
    dataRecebimento: string | null;
    situacao: string;
    responsavelRecebimento: string | null;
    justificativa: string | null;
    tratamento: string;
    numeroCdf: string | null;
    residuos: {
        codigoInterno: string | null;
        descricao: string;
        descricaoInterna: string | null;
        classe: string;
        unidade: string;
        quantidadeIndicada: number;
        quantidadeRecebida: number | null;
    }[];
    gerador: {
        unidade: string;
        cpfCnpj: string;
        nome: string;
        observacao: string | null;
    };
    transportador: {
        unidade: string;
        cpfCnpj: string;
        nome: string;
        motorista: string | null;
        placaVeiculo: string | null;
    };
    destinador: {
        unidade: string;
        cpfCnpj: string;
        nome: string;
        observacao: string | null;
    };
}

const parseMTR = async (file: File): Promise<MTR[]> => {
    const buffer = await file.arrayBuffer();
    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.load(buffer);

    const worksheet = workbook.worksheets[0];
    const headerRow = worksheet.getRow(1);
    const headers: Record<string, number> = {};

    if (headerRow.cellCount === 0) {
        return [];
    }

    headerRow.eachCell((cell, colNumber) => {
        headers[cell.text.trim()] = colNumber;
    });

    const getCellValue = (row: ExcelJS.Row, column: string, makeEmptyIf?: string): string => {
        const value = row.getCell(headers[column])?.text?.trim() || '';
        if (makeEmptyIf && value === makeEmptyIf) {
            return '';
        }
        return value;
    };

    const rows: MTR[] = [];
    const dupes = new Map<string, number[]>();

    worksheet.eachRow((row, rowNumber) => {
        if (rowNumber === 1) return; // skip header

        const residuo = {
            codigoInterno: getCellValue(row, 'Cód Interno') || null,
            descricao: getCellValue(row, 'Resíduo Cód/Descrição'),
            descricaoInterna: getCellValue(row, 'Descr. interna') || null,
            classe: getCellValue(row, 'Classe'),
            unidade: getCellValue(row, 'Unidade'),
            quantidadeIndicada: Number(getCellValue(row, 'Quantidade indicada')),
            quantidadeRecebida: getCellValue(row, 'Quantidade recebida') !== '' ? Number(getCellValue(row, 'Quantidade recebida')) : null,
        };
        const gerador = {
            unidade: getCellValue(row, 'Gerador (Unidade)'),
            cpfCnpj: getCellValue(row, 'Gerador (CNPJ/CPF)'),
            nome: getCellValue(row, 'Gerador (Nome)'),
            observacao: getCellValue(row, 'Observação Gerador') || null,
        };
        const transportador = {
            unidade: getCellValue(row, 'Transportador (Unidade)'),
            cpfCnpj: getCellValue(row, 'Transportador (CNPJ/CPF)'),
            nome: getCellValue(row, 'Transportador (Nome)'),
            motorista: getCellValue(row, 'Nome Motorista') || null,
            placaVeiculo: getCellValue(row, 'Placa Veículo') || null,
        };
        const destinador = {
            unidade: getCellValue(row, 'Destinador (Unidade)'),
            cpfCnpj: getCellValue(row, 'Destinador (CNPJ/CPF)'),
            nome: getCellValue(row, 'Destinador (Nome)'),
            observacao: getCellValue(row, 'Observação Destinador') || null,
        };

        const rowData: MTR = {
            numero: getCellValue(row, 'Nº MTR'),
            tipoManifesto: getCellValue(row, 'Tipo Manifesto'),
            responsavelEmissao: getCellValue(row, 'Responsável Emissão'),
            temMTRComplementar: getCellValue(row, 'Tem MTR Complementar') || null,
            numeroMtrProvisorio: getCellValue(row, 'MTR Provisório Nº', '0') || null,
            dataEmissao: getCellValue(row, 'Data de Emissão'),
            dataRecebimento: getCellValue(row, 'Data de Recebimento') || null,
            situacao: getCellValue(row, 'Situação'),
            responsavelRecebimento: getCellValue(row, 'Responsável Recebimento') || null,
            justificativa: getCellValue(row, 'Justificativa') || null,
            tratamento: getCellValue(row, 'Tratamento'),
            numeroCdf: getCellValue(row, 'CDF Nº') || null,

            residuos: [residuo],
            gerador,
            transportador,
            destinador,
        };

        dupes.set(rowData.numero, [rowNumber - 2].concat(dupes.get(rowData.numero) ?? []));
        rows.push(rowData);
    });

    dupes.forEach(values => {
        if (values.length > 1) {
            const targetIndex = values[0];
            for (let index = 1; index < values.length; index++) {
                rows[targetIndex].residuos.push(JSON.parse(JSON.stringify(rows[values[index]].residuos[0])));
                rows[values[index]] = null as any;
            }
        }
    });

    return rows;
};

// TODO: move to a models lib
export const handler = async (event: any): Promise<any> => {
    process.on('unhandledRejection', (reason, promise) => {
        console.error('Unhandled Rejection at:', promise, 'reason:', reason);
    });

    process.on('uncaughtException', err => {
        console.error('Uncaught Exception:', err);
    });

    let unidade: string | undefined;
    let url: string | undefined = undefined;

    try {
        logger.log('Start.', event);

        // env variables
        const dynamodbStakeholderTable = process.env.DYNAMODB_STAKEHOLDER_TABLE!;
        const dynamodbMtrTable = process.env.DYNAMODB_MTR_TABLE!;
        const dynamodbMtrLoadTable = process.env.DYNAMODB_MTR_LOAD_TABLE!;
        const s3IntegrationDomainBucket = process.env.S3_INTEGRATION_DOMAIN_BUCKET!;
        const s3BucketMtrPrefix = process.env.S3_BUCKET_MTR_PREFIX!;

        // parse event payload
        ({ unidade, url } = event);

        // call SINIR
        const response = await axios.get(url!, {
            headers: {
                // accept: 'application/json, text/plain, */*',
                // 'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7,es;q=0.6',
                // 'content-type': 'application/json;charset=UTF-8',
                // 'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
                // 'sec-ch-ua-mobile': '?0',
                // 'sec-ch-ua-platform': '"Windows"',
                // 'sec-fetch-dest': 'empty',
                // 'sec-fetch-mode': 'cors',
                // 'sec-fetch-site': 'same-origin',
                // Referer: 'https://mtr.sinir.gov.br/navegacao/relatorioMtr/1',
                // 'Referrer-Policy': 'strict-origin-when-cross-origin',
            },
            responseType: 'arraybuffer',
            timeout: 9 * 60 * 1000,
        });

        // create a blob
        const contentType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
        const blob = new Blob([response.data], { type: contentType });

        const urlSplit = url!.split('/');
        const dataInicial = urlSplit[9].split('-').slice().reverse().join('-');
        const dataFinal = urlSplit[10].split('-').slice().reverse().join('-');
        const filename = `${unidade}_${dataInicial}_${dataFinal}_${urlSplit[8]}.xlsx`;
        const file = new File([blob], filename, { type: blob.type });

        // parse MTRs
        const mtrs = await parseMTR(file);
        let distinctStakeholders: Stakeholder[] = [];

        if (mtrs.length > 0) {
            // upsert MTRs
            await integrationService.upsertMtrs(dynamodbMtrTable, mtrs, 'system');

            // build distinct stakeholders
            const stakeholders: Stakeholder[] = [];

            for (const mtr of mtrs) {
                const { gerador, transportador, destinador } = mtr;
                stakeholders.push(
                    { unidade: gerador.unidade, cpfCnpj: gerador.cpfCnpj, nome: gerador.nome, audit: {} as never },
                    { unidade: transportador.unidade, cpfCnpj: transportador.cpfCnpj, nome: transportador.nome, audit: {} as never },
                    { unidade: destinador.unidade, cpfCnpj: destinador.cpfCnpj, nome: destinador.nome, audit: {} as never },
                );
            }

            // remove dupes
            const stakeholderSet = new Set<string>();
            distinctStakeholders = stakeholders
                .filter(ent => {
                    const key = `${ent.unidade}|${ent.cpfCnpj}|${ent.nome}`;
                    if (stakeholderSet.has(key)) return false;
                    stakeholderSet.add(key);
                    return true;
                })
                .filter(x => x.unidade !== unidade);

            // store file in S3
            await s3Client.putObject(
                s3IntegrationDomainBucket,
                `${s3BucketMtrPrefix}/${unidade}/${filename}`,
                Buffer.from(response.data),
                contentType,
            );

            // isnert stakeholders
            console.log('stakeholders', JSON.stringify(distinctStakeholders, null, 2));
            await integrationService.insertStakeholders(dynamodbStakeholderTable, distinctStakeholders, 'system', logger);
        } else {
            logger.warn(`No MTRs found`, event);
        }

        // delete mtr load
        await integrationService.deleteMtrLoad(dynamodbMtrLoadTable, url!);

        logger.log('Success.');
    } catch (error: unknown) {
        logger.log('Error.', error);

        // Axios or AWS complex error
        const isEmptyError = error && Object.keys(error).length === 0;
        if (isEmptyError) {
            logger.warn(`Axios/AWS complex error. It is very likely that the file has not been downloaded.`, event);
        }

        return { url, stakeholders: [], mtrs: 0, error };
    }
};
