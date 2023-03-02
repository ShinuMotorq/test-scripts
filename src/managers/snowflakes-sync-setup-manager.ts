import * as SnowflakeSyncSetupQueries from "../queries/sf-setup-queries"
import * as format from 'string-template';
import SnowflakeClient from "../clients/snowflakes-client";
import SnowflakeSyncSetupDataAccess from "../data-access/sfsync-setup-data-access";
import { SnowflakeSyncServiceConfig } from "../objects/sfsync-service-config";
import logger from "../common/logger";

class SnowflakeSyncSetupManager {
    /**
     * @todo : decouple DB calls to DAO class with abstraction over different DB
     */
    private sfClient: SnowflakeClient | null = null
    private sfSyncSetupDataAccess: SnowflakeSyncSetupDataAccess | null = null
    private serviceConfig: SnowflakeSyncServiceConfig
    private scope = 'SnowflakeSyncSetupManager'

    constructor(serviceConfig: SnowflakeSyncServiceConfig) {
        this.serviceConfig = serviceConfig;
        this.sfSyncSetupDataAccess = new SnowflakeSyncSetupDataAccess()
    }

    async getAllSchemas(): Promise<string[]> {
        let rows: any = await this.sfSyncSetupDataAccess?.showSchemas();
        logger.info(this.scope, `Snowflakes response : Found ${rows.length} schemas`)
        return rows.map(row => row.name);
    }

    async createSchema(schemaName: string | undefined) {
        let rows: any = await this.sfSyncSetupDataAccess?.createSchema(schemaName);
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
    }

    async useSchema(schemaName: string | undefined) {
        let rows: any = await this.sfSyncSetupDataAccess?.useSchema(schemaName);
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
    }

    async getAllStages(): Promise<string[]> {
        let rows: any = await this.sfSyncSetupDataAccess?.showStages();
        logger.info(this.scope, `Snowflakes response : Found ${rows.length} stages`)
        return rows.map(row => row.name);
    }

    async createStage(stageName, containerUrl, containerToken) {
        let rows: any = await this.sfSyncSetupDataAccess?.createStage(stageName, containerUrl, containerToken);
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
    }

    async createAndAlterFileformat(avroFileFormatName, stageName) {
        let rows: any = await this.sfSyncSetupDataAccess?.createFileFormat(avroFileFormatName)
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
        rows = await this.sfSyncSetupDataAccess?.alterStageFileFormat(
            avroFileFormatName,
            stageName
        )
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
    }

    async createReplayTables() {
        for (let statement of SnowflakeSyncSetupQueries.CreateReplayTables) {
            let rows: any = await this.sfSyncSetupDataAccess?.runPreparedStatement(format.default(statement, {
                prodDB: this.serviceConfig.prod_environment,
                prodSchema: this.serviceConfig.prod_schema
            }))
            logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
        }
    }

    async createNeccessaryFunctions() {
        for (let statement of SnowflakeSyncSetupQueries.CreateFunctions) {
            let rows: any = await this.sfSyncSetupDataAccess?.runPreparedStatement(statement)
            logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
        }
    }

    async dropSchema(schemaName: string | undefined) {
        let schemas: Array<string> = await this.getAllSchemas()
        if(schemas.includes(schemaName!)) {
            let rows: any = await this.sfSyncSetupDataAccess?.dropSchema(schemaName);
            logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
        }        
    }
}

export { SnowflakeSyncSetupManager };