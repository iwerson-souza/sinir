import { Callback, Context, DynamoDBStreamEvent } from 'aws-lambda';
import { AWSUtils, Config } from 'brasil-residuos-lib-aws-utils';
import { Logger, LoggerConfig } from 'brasil-residuos-lib-utils';

import { IntegrationService } from '../../../services';
import { Stakeholder } from '../../../services/integration';
import { buildStrategy } from '../../../services/sinir';

// utils
const logger = new Logger({ component: 'stakeholder-stream-function' } as LoggerConfig);

// aws-utils
const awsConfig: Config = {
    region: process.env.AWS_REGION,
};
const awsUtils = new AWSUtils(awsConfig.region!);
const dynamodbClient = awsUtils.buildDynamoDB();

// services
const integrationService = new IntegrationService(dynamodbClient);

export const handler = async (event: DynamoDBStreamEvent, context: Context, callback: Callback): Promise<void> => {
    try {
        logger.log('Start.', { event });

        // env variables
        const dynamodbStakeholderTable = process.env.DYNAMODB_STAKEHOLDER_TABLE!;
        const dynamodbMtrLoadTable = process.env.DYNAMODB_MTR_LOAD_TABLE!;

        // filter records
        const streamRecords = event.Records.filter(x => x.eventName === 'INSERT');

        // generate mtr loads and update stakeholders
        for (const record of streamRecords) {
            const stakeholder = dynamodbClient.nativeUnmarshall(record.dynamodb!.NewImage as never) as Stakeholder;

            const strategy = buildStrategy(stakeholder.unidade);
            const mtrLoads = strategy.setup
                .map(x => x.urls)
                .flat()
                .map(y => ({ unidade: stakeholder.unidade, url: y, audit: {} as never }));

            await integrationService.upsertMtrLoads(dynamodbMtrLoadTable, mtrLoads, 'system');

            await integrationService.updateStakeholders(
                dynamodbStakeholderTable,
                [{ ...stakeholder, dataInicial: strategy.summary.startDate, dataFinal: strategy.summary.finalDate }],
                'system',
            );
        }

        logger.log('Success.');
        callback(null, 'Success.');
    } catch (error: unknown) {
        const message = error instanceof Error ? error?.message : 'Unknown Error';
        logger.error(message, error);
        logger.log('Error.');
        callback(null, 'Error. (non-retriable)');
    }
};
