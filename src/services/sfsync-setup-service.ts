import SnowflakeClient from '../clients/snowflakes-client';
import appConfig from '../common/configs';
import * as Constants from '../common/constants';
import { SnowflakeSyncSetupManager } from '../managers/sfsync-setup-manager'
import { SnowflakeSyncServiceConfig } from '../objects/sfsync-service-config';


class SnowflakeSyncSetupService extends Service {

    private sfSyncSetupManager: SnowflakeSyncSetupManager;
    private serviceConfig : SnowflakeSyncServiceConfig;

    constructor() {
        super();
        this.serviceConfig = this.fetchServiceConfig()
        this.sfSyncSetupManager = new SnowflakeSyncSetupManager(this.serviceConfig);        
    }

    fetchServiceConfig(): SnowflakeSyncServiceConfig {
        return require('../../configs/snowflake-sync-config.json')
    }

    async init() {
        await this.validateAndCreateSchema();
        let stageName = await this.validateAndCreateStage();
        await this.sfSyncSetupManager.createAndAlterFileformat(Constants.avroFileFormatName, stageName)
    }

    async validateAndCreateSchema() {
        let schemaName = appConfig.snowflakeSchema
        if (schemaName) {
            let schemas = await this.sfSyncSetupManager.getAllSchemas();
            if (schemas.includes(schemaName)) {
                throw Error(`Schema : ${schemaName} already exists!`)
            } else {
                await this.sfSyncSetupManager.createSchema(schemaName);
                await this.sfSyncSetupManager.useSchema(schemaName);
            }
        } else {
            throw Error("Missing config 'snowflakeSchema'");
        }
    }

    async validateAndCreateStage() {
        let stageName = appConfig.snowflakeStage;
        if (stageName) {
            let stages = await this.sfSyncSetupManager.getAllStages();
            if (stages.includes(stageName)) {
                throw Error(`Stage : ${stageName} already exists!`)
            } else {
                await this.sfSyncSetupManager.createStage(
                    stageName,
                    appConfig.blobStorage?.containerConfig.containerUrl,
                    appConfig.blobStorage?.containerConfig.containerToken)
            }
        } else {
            throw Error("Missing config 'snowflakeStage'");
        }
        return stageName;
    }

    async setupReplayTables() {
        await this.sfSyncSetupManager.createReplayTables();
        await this.sfSyncSetupManager.createNeccessaryFunctions();
    }
}

export default SnowflakeSyncSetupService;