/* eslint-disable @typescript-eslint/no-non-null-assertion */
import { AWSUtils, Config } from 'brasil-residuos-lib-aws-utils';
import { Logger, LoggerConfig } from 'brasil-residuos-lib-utils';

import { IntegrationService } from '../../../services';

// utils
const logger = new Logger({ component: 'mtr-setup-function' } as LoggerConfig);

// aws-utils
const awsConfig: Config = {
    region: process.env.AWS_REGION,
};
const awsUtils = new AWSUtils(awsConfig.region!);
const dynamodbClient = awsUtils.buildDynamoDB();

// TODO: still needs to have a daily job to list stakehoders and update the end date
// TODO: move to a models lib
export const handler = async (): Promise<any> => {
    try {
        logger.log('Start.');

        // env variables
        const dynamodbStakeholderTable = process.env.DYNAMODB_STAKEHOLDER_TABLE!;
        const dynamodbMtrLoadTable = process.env.DYNAMODB_MTR_LOAD_TABLE!;

        // services
        const integrationService = new IntegrationService(dynamodbClient);

        // list mtr loads
        const items = await integrationService.listMtrLoads(dynamodbMtrLoadTable, 100);

        logger.log('Success.');
        return { items };
    } catch (error: unknown) {
        logger.log('Error.', error);
        return error;
    }
};
