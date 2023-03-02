import SnowflakeClient from "../clients/snowflakes-client";
import * as SnowflakeSyncSetupQueries from "../queries/sf-setup-queries"
import format from 'string-template'
import DataAccess from "./base-data-access";
import logger from '../common/logger';
import Errors from "../common/errors";
import BaseErrorHandler from "../clients/error-handler";


class SnowflakeSyncSetupDataAccess extends DataAccess {

    private sfClient: SnowflakeClient | null = null

    constructor() {
        super()
    }

    async runPreparedStatement(query) {
        try {
            this.sfClient = await SnowflakeClient.getInstance();
            return await this.sfClient.runStatement(query);
        } catch (err) {
            throw new BaseErrorHandler(Errors.SnowflakeQueryFailed, { query, err })
        }
    }

    async showSchemas(): Promise<any> {
        return this.runPreparedStatement(SnowflakeSyncSetupQueries.ShowSchemas)
    }

    async createSchema(schemaName: string | undefined): Promise<any> {
        let query = format(SnowflakeSyncSetupQueries.CreateSchema, { schemaName })
        return this.runPreparedStatement(query)
    }

    async useSchema(schemaName: string | undefined) {
        let query = format(SnowflakeSyncSetupQueries.UseSchema, { schemaName })
        return this.runPreparedStatement(query)
    }

    async showStages() {
        return this.runPreparedStatement(SnowflakeSyncSetupQueries.ShowStages)
    }

    async createStage(stageName, containerUrl, containerToken) {
        let query = format(SnowflakeSyncSetupQueries.CreateStage, {
            stageName,
            containerUrl,
            containerToken,
        })
        return this.runPreparedStatement(query);
    }

    async createFileFormat(avroFileFormatName) {
        let query = format(SnowflakeSyncSetupQueries.CreateFileFormat, { avroFileFormatName })
        return this.runPreparedStatement(query)
    }

    async alterStageFileFormat(avroFileFormatName, stageName) {
        let query = format(SnowflakeSyncSetupQueries.AlterStageFileFormat, {
            avroFileFormatName,
            stageName
        })
        return this.runPreparedStatement(query)
    }

    async dropSchema(schemaName: string | undefined): Promise<any> {
        let query = format(SnowflakeSyncSetupQueries.DropSchema, { schemaName })
        return this.runPreparedStatement(query)
    }

}

export default SnowflakeSyncSetupDataAccess;