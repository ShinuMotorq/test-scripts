import appConfig from '../common/configs';
import * as Constants from '../common/constants';
import { SnowflakeSetupServiceConfig } from '../objects/config-schemas/snowflakes-setup-service-config';
import Service from './base-service';
import logger from "../common/logger"
import SnowflakeClient from '../clients/snowflakes-client';
import Errors from '../common/errors';
import BaseErrorHandler from '../clients/error-handler';
import SnowflakeSetupManager from '../managers/snowflakes-setup-manager';


class SnowflakeSetupService extends Service {

    private snowflakesSetupManager: SnowflakeSetupManager;
    private serviceConfig: SnowflakeSetupServiceConfig;
    private scope = "SnowflakeSetupService"

    constructor() {
        super();
        this.serviceConfig = this.fetchServiceConfig()
        this.snowflakesSetupManager = new SnowflakeSetupManager(this.serviceConfig);
    }

    /**
     * @returns service specific config
     */
    fetchServiceConfig(): SnowflakeSetupServiceConfig {
        return require('../../configs/snowflake-setup-config.json')
    }

    /**
     * @override
     * Service level override for init()
     */
    async init() {
        logger.info(this.scope, "~~~~~~ Snowflake setup started ~~~~~~")
        try {
            await this.validateAndCreateSchema();
            let stageName = await this.validateAndCreateStage();
            await this.snowflakesSetupManager.createAndAlterFileformat(Constants.avroFileFormatName, stageName)
            await this.setupReplayTables()
        } catch (err: any) {
            logger.error(this.scope, err.message);
            logger.error(this.scope, "xxxxxxxx Snowflake setup failed xxxxxxxx")
        } finally {
            // -- TearDown scripts here --
            
            // Uncomment below line only for testing
            // await this.sfSyncSetupManager.dropSchema(this.serviceConfig.schemaName)
            await SnowflakeClient.terminateOpenConnection()
        }
        logger.info(this.scope, "~~~~~~ Snowflake setup ends here ~~~~~~")
    }

    /**
     * 1. Gets all existing schemas
     * 2. Verifies if schema to be created already exist
     * 3. Creates new schema and uses it for further operations
     */
    async validateAndCreateSchema() {
        logger.info(this.scope, "Verifying if schema already exists...")
        let schemaName = this.serviceConfig.schemaName //appConfig.snowflakeSchema
        if (schemaName) {
            let schemas = await this.snowflakesSetupManager.getAllSchemas();
            if (schemas.includes(schemaName)) {
                throw new BaseErrorHandler(Errors.SchemaAlreadyExists, { schemaName });
            } else {
                await this.snowflakesSetupManager.createSchema(schemaName);
                await this.snowflakesSetupManager.useSchema(schemaName);
            }
        } else {
            throw new BaseErrorHandler(Errors.ConfigNotFound, { config: 'snowflakeSchema' });
        }
    }

    /**
     * 1. Gets all existing stages
     * 2. Verifies if stage to be created already exist
     * 3. Creates new stage
     */
    async validateAndCreateStage() {
        let stageName = this.serviceConfig.stageName // appConfig.snowflakeStage;
        if (stageName) {
            let stages = await this.snowflakesSetupManager.getAllStages();
            if (stages.includes(stageName)) {
                throw new BaseErrorHandler(Errors.StageAlreadyExists, { stageName });
            } else {
                await this.snowflakesSetupManager.createStage(
                    stageName,
                    appConfig.blobStorage?.containerConfig.containerUrl,
                    appConfig.blobStorage?.containerConfig.containerToken)
                await this.snowflakesSetupManager.verifyStage(stageName)
            }
        } else {
            throw new BaseErrorHandler(Errors.ConfigNotFound, { config: 'snowflakeStage' });
        }
        return stageName;
    }

    /**
     * 1. Creates the Telemetry, Events, Trips replay table
     * 2. Creates neccessary functions
     */
    async setupReplayTables() {
        await this.snowflakesSetupManager.createReplayTables();
        await this.snowflakesSetupManager.createNeccessaryFunctions();
    }
}

export default SnowflakeSetupService;