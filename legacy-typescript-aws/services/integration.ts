import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import DynamoDB from 'brasil-residuos-lib-aws-utils/dist/resources/dynamodb';
import { Audit } from 'brasil-residuos-lib-models/dist/global/audit.type';
import { Logger } from 'brasil-residuos-lib-utils';

// TODO: move to a models lib
export type Stakeholder = {
    unidade: string;
    cpfCnpj: string;
    nome: string;
    dataInicial?: Date;
    dataFinal?: Date;
    audit: Audit;
};

export type MtrLoad = {
    url: string;
    unidade: string;
    audit: Audit;
};

class IntegrationService {
    private readonly dynamodbClient: DynamoDB;

    constructor(dynamodbClient: DynamoDB) {
        this.dynamodbClient = dynamodbClient;
    }

    // future use
    private sweepStakeholder(stakeholder: Stakeholder): Stakeholder {
        return stakeholder;
    }

    private readonly chunkArray = <T>(arr: T[], size: number): T[][] => {
        const result: T[][] = [];
        for (let i = 0; i < arr.length; i += size) {
            result.push(arr.slice(i, i + size));
        }
        return result;
    };

    async upsertMtrs(table: string, mtrs: any[], actionBy: string): Promise<void> {
        const now = new Date().toISOString();

        const writeRequests = mtrs.map(mtr => ({
            PutRequest: {
                Item: {
                    numero: { S: mtr.numero },
                    tipoManifesto: { S: mtr.tipoManifesto },
                    responsavelEmissao: { S: mtr.responsavelEmissao },
                    temMTRComplementar: mtr.temMTRComplementar ? { S: mtr.temMTRComplementar } : { NULL: true },
                    numeroMtrProvisorio: mtr.numeroMtrProvisorio ? { S: mtr.numeroMtrProvisorio } : { NULL: true },
                    dataEmissao: { S: mtr.dataEmissao },
                    dataRecebimento: { S: mtr.dataRecebimento ?? '01/01/1900' },
                    situacao: { S: mtr.situacao },
                    responsavelRecebimento: mtr.responsavelRecebimento ? { S: mtr.responsavelRecebimento } : { NULL: true },
                    justificativa: mtr.justificativa ? { S: mtr.justificativa } : { NULL: true },
                    tratamento: { S: mtr.tratamento },
                    numeroCdf: mtr.numeroCdf ? { S: mtr.numeroCdf } : { NULL: true },
                    residuos: {
                        L: mtr.residuos.map((residuo: any) => ({
                            M: {
                                codigoInterno: residuo.codigoInterno ? { S: residuo.codigoInterno } : { NULL: true },
                                descricao: { S: residuo.descricao },
                                descricaoInterna: residuo.descricaoInterna ? { S: residuo.descricaoInterna } : { NULL: true },
                                classe: { S: residuo.classe },
                                unidade: { S: residuo.unidade },
                                quantidadeIndicada: { N: residuo.quantidadeIndicada.toString() },
                                quantidadeRecebida:
                                    residuo.quantidadeRecebida != null ? { N: residuo.quantidadeRecebida.toString() } : { NULL: true },
                            },
                        })),
                    },
                    'residuos.codigo': { S: mtr.residuos.map((x: any) => x.descricao.split('-')[0]).join('|') },
                    'residuos.classe': { S: mtr.residuos.map((x: any) => x.classe).join('|') },
                    'gerador.cpfCnpj': { S: mtr.gerador.cpfCnpj },
                    'transportador.cpfCnpj': { S: mtr.transportador.cpfCnpj },
                    'destinador.cpfCnpj': { S: mtr.destinador.cpfCnpj },
                    gerador: {
                        M: {
                            unidade: { S: mtr.gerador.unidade },
                            cpfCnpj: { S: mtr.gerador.cpfCnpj },
                            nome: { S: mtr.gerador.nome },
                            observacao: mtr.gerador.observacao ? { S: mtr.gerador.observacao } : { NULL: true },
                        },
                    },
                    transportador: {
                        M: {
                            unidade: { S: mtr.transportador.unidade },
                            cpfCnpj: { S: mtr.transportador.cpfCnpj },
                            nome: { S: mtr.transportador.nome },
                            motorista: mtr.transportador.motorista ? { S: mtr.transportador.motorista } : { NULL: true },
                            placaVeiculo: mtr.transportador.placaVeiculo ? { S: mtr.transportador.placaVeiculo } : { NULL: true },
                        },
                    },
                    destinador: {
                        M: {
                            unidade: { S: mtr.destinador.unidade },
                            cpfCnpj: { S: mtr.destinador.cpfCnpj },
                            nome: { S: mtr.destinador.nome },
                            observacao: mtr.destinador.observacao ? { S: mtr.destinador.observacao } : { NULL: true },
                        },
                    },
                    cpfsCnpjs: { S: `${mtr.gerador.cpfCnpj}|${mtr.transportador.cpfCnpj}|${mtr.destinador.cpfCnpj}` },
                    audit: {
                        M: {
                            createdBy: { S: actionBy },
                            createdDt: { S: now },
                        },
                    },
                },
            },
        }));

        const chunks = this.chunkArray(writeRequests, 25);
        for (const chunk of chunks) {
            const params = {
                RequestItems: {
                    [table]: chunk,
                },
            };

            await this.dynamodbClient.batchWrite(params);
        }
    }

    async insertMtrLoad(table: string, mtrLoad: MtrLoad, actionBy: string): Promise<void> {
        const now = new Date().toISOString();
        const dynamoDBParams = {
            TableName: table,
            Key: {
                url: { S: mtrLoad.url },
            },
            UpdateExpression: 'set #unidade = :unidade, #audit = :audit',
            ExpressionAttributeNames: {
                '#unidade': 'unidade',
                '#audit': 'audit',
            },
            ExpressionAttributeValues: {
                ':unidade': { S: mtrLoad.unidade },
                ':audit': { M: { createdBy: { S: actionBy }, createdDt: { S: now } } },
            },
        };

        await this.dynamodbClient.upsert(dynamoDBParams);
    }

    /*
        initial data example:{
        "unidade": "97653",
        "url": "https://mtr.sinir.gov.br/api/mtr/pesquisaManifestoRelatorioMtrAnalitico/97653/18/5/02-10-2025/31-10-2025/8/0/9/0"
        }
    */
    async listMtrLoads(table: string, maxItems: number): Promise<MtrLoad[]> {
        const dynamoDBParams = {
            TableName: table,
            Limit: maxItems,
        };

        const data = await this.dynamodbClient.scan(dynamoDBParams);
        return (
            data?.Items?.map((x: any) => {
                const y = this.dynamodbClient.nativeUnmarshall(x) as MtrLoad;
                return { unidade: y.unidade, url: y.url, audit: {} } as MtrLoad;
            }) ?? []
        );
    }

    async upsertMtrLoads(table: string, mtrLoads: MtrLoad[], actionBy: string): Promise<void> {
        const now = new Date().toISOString();

        const writeRequests = mtrLoads.map(mtrLoad => ({
            PutRequest: {
                Item: {
                    url: { S: mtrLoad.url },
                    unidade: { S: mtrLoad.unidade },
                    audit: {
                        M: {
                            createdBy: { S: actionBy },
                            createdDt: { S: now },
                        },
                    },
                },
            },
        }));

        const chunks = this.chunkArray(writeRequests, 25);
        for (const chunk of chunks) {
            const params = {
                RequestItems: {
                    [table]: chunk,
                },
            } as any;

            await this.dynamodbClient.batchWrite(params);
        }
    }

    async deleteMtrLoad(table: string, url: string): Promise<void> {
        const dynamoDBParams = {
            TableName: table,
            Key: {
                url: { S: url },
            },
        };

        await this.dynamodbClient.remove(dynamoDBParams);
    }

    async insertStakeholders(table: string, stakeholders: Stakeholder[], actionBy: string, logger: Logger) {
        const now = new Date().toISOString();
        let currentUnidade;

        for (const stakeholder of stakeholders) {
            currentUnidade = stakeholder.unidade;

            const dynamoDBParams = {
                TableName: table,
                Item: {
                    unidade: { S: stakeholder.unidade },
                    cpfCnpj: { S: stakeholder.cpfCnpj },
                    nome: { S: stakeholder.nome },
                    dataInicial: stakeholder.dataInicial ? { S: stakeholder.dataInicial } : { NULL: true },
                    dataFinal: stakeholder.dataFinal ? { S: stakeholder.dataFinal } : { NULL: true },
                    audit: {
                        M: {
                            createdBy: { S: actionBy },
                            createdDt: { S: now },
                        },
                    },
                },
                ConditionExpression: 'attribute_not_exists(unidade) AND attribute_not_exists(cpfCnpj)',
            } as any;

            try {
                await this.dynamodbClient.insert(dynamoDBParams);
            } catch (error: any) {
                if (error instanceof ConditionalCheckFailedException || error?.name === 'ConditionalCheckFailedException') {
                    logger.warn('ConditionalCheckFailedException successfully handled', currentUnidade);
                    continue;
                } else {
                    throw error;
                }
            }
        }
    }

    async updateStakeholders(table: string, stakeholders: Stakeholder[], actionBy: string): Promise<void> {
        const now = new Date().toISOString();

        const writeRequests = stakeholders.map(stakeholder => ({
            PutRequest: {
                Item: {
                    unidade: { S: stakeholder.unidade },
                    cpfCnpj: { S: stakeholder.cpfCnpj },
                    nome: { S: stakeholder.nome },
                    dataInicial: { S: stakeholder.dataInicial!.toISOString().split('T')[0] },
                    dataFinal: { S: stakeholder.dataFinal!.toISOString().split('T')[0] },
                    audit: {
                        M: {
                            createdBy: { S: stakeholder.audit.createdBy },
                            createdDt: { S: stakeholder.audit.createdDt },
                            lastModifiedBy: { S: actionBy },
                            lastModifiedDt: { S: now },
                        },
                    },
                },
            },
        }));

        const chunks = this.chunkArray(writeRequests, 25);
        for (const chunk of chunks) {
            const params = {
                RequestItems: {
                    [table]: chunk,
                },
            } as any;

            await this.dynamodbClient.batchWrite(params);
        }
    }

    async listStakeholders(table: string, sweepData: boolean): Promise<Stakeholder[]> {
        const dynamoDBParams = {
            TableName: table,
        } as any;

        const data = await this.dynamodbClient.fullScan(dynamoDBParams);
        return data?.map((x: any) => {
            const y = this.dynamodbClient.nativeUnmarshall(x) as Stakeholder;
            return sweepData ? this.sweepStakeholder(y) : y;
        });
    }

    async searchStakeholders(table: string, dataFinal: Date, sweepData: boolean): Promise<Stakeholder[]> {
        const dynamoDBParams = {
            TableName: table,
            FilterExpression: '#dataFinal < :cutoff OR attribute_not_exists(#dataFinal)',
            ExpressionAttributeNames: {
                '#dataFinal': 'dataFinal',
            },
            ExpressionAttributeValues: {
                ':cutoff': { S: dataFinal.toISOString().split('T')[0] },
            },
        } as any;

        const data = await this.dynamodbClient.fullScan(dynamoDBParams);
        return data?.map((x: any) => {
            const y = this.dynamodbClient.nativeUnmarshall(x) as Stakeholder;
            return sweepData ? this.sweepStakeholder(y) : y;
        });
    }
}

export default IntegrationService;
