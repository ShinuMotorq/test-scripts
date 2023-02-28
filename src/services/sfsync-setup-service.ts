import SnowflakeClient from '../clients/snowflakes-client';
import appConfig from '../common/configs';
import { SnowflakeSyncSetupManager } from '../managers/sfsync-setup-manager'


class SnowflakeSyncSetupService extends Service {

    private sfSyncSetupManager: SnowflakeSyncSetupManager;

    constructor() {
        super();
        this.sfSyncSetupManager = new SnowflakeSyncSetupManager();
    }

    init() {
        this.checkAndValidateSchema();
    }

    async checkAndValidateSchema() {
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

    async checkAndValidateStage() {
        await this.sfSyncSetupManager.createStage(
            appConfig.snowflakeStage,
            appConfig.blobStorage?.containerConfig.containerUrl,
            appConfig.blobStorage?.containerConfig.containerToken)
        
    }
}

export default SnowflakeSyncSetupService;