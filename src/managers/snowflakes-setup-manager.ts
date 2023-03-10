import * as SnowflakeSyncSetupQueries from "../queries/snowflakes-setup-queries"
import * as format from 'string-template';
import SnowflakeClient from "../clients/snowflakes-client";
import SnowflakeSetupDataAccessor from "../data-access/snowflakes-setup-data-accessor";
import { SnowflakeSetupServiceConfig } from "../objects/config-schemas/snowflakes-setup-service-config";
import logger from "../common/logger";
import Manager from './base-manager';

class SnowflakeSetupManager extends Manager{
    /**
     * @todo : decouple DB calls to DAO class with abstraction over different DB
     */    
    private snowflakesSetupDataAccess: SnowflakeSetupDataAccessor | null = null
    private serviceConfig: SnowflakeSetupServiceConfig
    private scope = 'SnowflakeSetupManager'

    constructor(serviceConfig: SnowflakeSetupServiceConfig) {
        super()
        this.serviceConfig = serviceConfig;
        this.snowflakesSetupDataAccess = new SnowflakeSetupDataAccessor()
    }

    async getAllSchemas(): Promise<string[]> {
        let rows: any = await this.snowflakesSetupDataAccess?.showSchemas();
        logger.info(this.scope, `Snowflakes response : Found ${rows.length} schemas`)
        return rows.map(row => row.name);
    }

    async createSchema(schemaName: string | undefined) {
        let rows: any = await this.snowflakesSetupDataAccess?.createSchema(schemaName);
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
    }

    async useSchema(schemaName: string | undefined) {
        let rows: any = await this.snowflakesSetupDataAccess?.useSchema(schemaName);
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
    }

    async getAllStages(): Promise<string[]> {
        let rows: any = await this.snowflakesSetupDataAccess?.showStages();
        logger.info(this.scope, `Snowflakes response : Found ${rows.length} stages`)
        return rows.map(row => row.name);
    }

    async createStage(stageName, containerUrl, containerToken) {
        let rows: any = await this.snowflakesSetupDataAccess?.createStage(stageName, containerUrl, containerToken);
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
    }

    async verifyStage(stageName) {
        let rows: any = await this.snowflakesSetupDataAccess?.verifyStage(stageName);
        logger.info(this.scope, `Snowflakes response : Found ${rows.length} files`)
    }

    async createAndAlterFileformat(avroFileFormatName, stageName) {
        let rows: any = await this.snowflakesSetupDataAccess?.createFileFormat(avroFileFormatName)
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
        rows = await this.snowflakesSetupDataAccess?.alterStageFileFormat(
            avroFileFormatName,
            stageName
        )
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
    }

    async createReplayTables() {
        for (let statement of SnowflakeSyncSetupQueries.CreateReplayTables) {
            let rows: any = await this.snowflakesSetupDataAccess?.runPreparedStatement(format.default(statement, {
                prodDB: this.serviceConfig.prodEnvironment,
                prodSchema: this.serviceConfig.prodSchema
            }))
            logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
        }
    }

    async createNeccessaryFunctions() {
        for (let statement of SnowflakeSyncSetupQueries.CreateFunctions) {
            let rows: any = await this.snowflakesSetupDataAccess?.runPreparedStatement(statement)
            logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
        }
    }

    async dropSchema(schemaName: string | undefined) {
        let schemas: Array<string> = await this.getAllSchemas()
        if(schemas.includes(schemaName!)) {
            let rows: any = await this.snowflakesSetupDataAccess?.dropSchema(schemaName);
            logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
        }        
    }
}

export default SnowflakeSetupManager;