import appConfig from '../common/configs';
import * as Constants from '../common/constants';
import { SnowflakeSyncSetupManager } from '../managers/snowflakes-sync-setup-manager'
import { SnowflakeSyncServiceConfig } from '../objects/sfsync-service-config';
import Service from './base-service';
import logger from "../common/logger"
import SnowflakeClient from '../clients/snowflakes-client';
import Errors from '../common/errors';
import BaseErrorHandler from '../clients/error-handler';


class SnowflakeSyncService extends Service {

    private sfSyncSetupManager: SnowflakeSyncSetupManager;
    private serviceConfig: SnowflakeSyncServiceConfig;
    private scope = "SnowflakeSyncSetupService"

    constructor() {
        super();
        this.serviceConfig = this.fetchServiceConfig()
        this.sfSyncSetupManager = new SnowflakeSyncSetupManager(this.serviceConfig);
    }

    /**
     * @returns service specific config
     */
    fetchServiceConfig(): SnowflakeSyncServiceConfig {
        return require('../../configs/snowflake-sync-config.json')
    }

    /**
     * @override
     * Service level override for init()
     */
    async init() {
        logger.info(this.scope, "~~~~~~ Snowflake sync setup started ~~~~~~")
        try {
            await this.validateAndCreateSchema();
            let stageName = await this.validateAndCreateStage();
            await this.sfSyncSetupManager.createAndAlterFileformat(Constants.avroFileFormatName, stageName)
            await this.setupReplayTables()
        } catch (err: any) {
            logger.error(this.scope, err.message);
            logger.error(this.scope, "xxxxxxxx Snowflake sync setup failed xxxxxxxx")
        } finally {
            // -- TearDown scripts here --
            
            // Uncomment below line only for testing
            // await this.sfSyncSetupManager.dropSchema(this.serviceConfig.schema_name)
            await SnowflakeClient.terminateOpenConnection()
        }
        logger.info(this.scope, "~~~~~~ Snowflake sync setup ends here ~~~~~~")
    }

    /**
     * 1. Gets all existing schemas
     * 2. Verifies if schema to be created already exist
     * 3. Creates new schema and uses it for further operations
     */
    async validateAndCreateSchema() {
        logger.info(this.scope, "Verifying if schema already exists...")
        let schemaName = this.serviceConfig.schema_name //appConfig.snowflakeSchema
        if (schemaName) {
            let schemas = await this.sfSyncSetupManager.getAllSchemas();
            if (schemas.includes(schemaName)) {
                throw new BaseErrorHandler(Errors.SchemaAlreadyExists, { schemaName });
            } else {
                await this.sfSyncSetupManager.createSchema(schemaName);
                await this.sfSyncSetupManager.useSchema(schemaName);
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
        let stageName = this.serviceConfig.stage_name // appConfig.snowflakeStage;
        if (stageName) {
            let stages = await this.sfSyncSetupManager.getAllStages();
            if (stages.includes(stageName)) {
                throw new BaseErrorHandler(Errors.StageAlreadyExists, { stageName });
            } else {
                await this.sfSyncSetupManager.createStage(
                    stageName,
                    appConfig.blobStorage?.containerConfig.containerUrl,
                    appConfig.blobStorage?.containerConfig.containerToken)
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
        await this.sfSyncSetupManager.createReplayTables();
        await this.sfSyncSetupManager.createNeccessaryFunctions();
    }
}

export default SnowflakeSyncService;