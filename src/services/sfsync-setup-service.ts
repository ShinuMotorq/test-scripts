import SnowflakeClient from '../clients/snowflakes-client';
import appConfig from '../common/configs';
import { SnowflakeSyncSetupManager } from '../managers/sfsync-setup-manager'


class SnowflakeSyncSetupService extends Service {

    private sfSyncSetupManager: SnowflakeSyncSetupManager;

    constructor() {
        super();
        this.sfSyncSetupManager = new SnowflakeSyncSetupManager();
    }

    async init() {
        await this.validateAndCreateSchema();
        await this.validateAndCreateStage();
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

    }
}

export default SnowflakeSyncSetupService;