import SnowflakeClient from "../clients/snowflakes-client";
import * as SnowflakeSetupQueries from "../queries/snowflakes-setup-queries"
import format from 'string-template'
import DataAccess from "./base-data-access";
import logger from '../common/logger';
import Errors from "../common/errors";
import BaseErrorHandler from "../clients/error-handler";


class SnowflakeSetupDataAccess extends DataAccess {

    private sfClient: SnowflakeClient | null = null

    constructor() {
        super()
    }

    async runPreparedStatement(query) {
        try {
            this.sfClient = await SnowflakeClient.getInstance();
            return await this.sfClient.runStatement(query);
        } catch (err: any) {
            throw new BaseErrorHandler(Errors.SnowflakeQueryFailed, {
                query: query,
                errorMessage: err.message
            })
        }
    }

    async showSchemas(): Promise<any> {
        return this.runPreparedStatement(SnowflakeSetupQueries.ShowSchemas)
    }

    async createSchema(schemaName: string | undefined): Promise<any> {
        let query = format(SnowflakeSetupQueries.CreateSchema, { schemaName })
        return this.runPreparedStatement(query)
    }

    async useSchema(schemaName: string | undefined) {
        let query = format(SnowflakeSetupQueries.UseSchema, { schemaName })
        return this.runPreparedStatement(query)
    }

    async showStages() {
        return this.runPreparedStatement(SnowflakeSetupQueries.ShowStages)
    }

    async createStage(stageName, containerUrl, containerToken) {
        let query = format(SnowflakeSetupQueries.CreateStage, {
            stageName,
            containerUrl,
            containerToken,
        })
        return this.runPreparedStatement(query);
    }

    async verifyStage(stageName) {
        let query = format(SnowflakeSetupQueries.VerifyStage, { stageName })
        return this.runPreparedStatement(query);
    }

    async createFileFormat(avroFileFormatName) {
        let query = format(SnowflakeSetupQueries.CreateFileFormat, { avroFileFormatName })
        return this.runPreparedStatement(query)
    }

    async alterStageFileFormat(avroFileFormatName, stageName) {
        let query = format(SnowflakeSetupQueries.AlterStageFileFormat, {
            avroFileFormatName,
            stageName
        })
        return this.runPreparedStatement(query)
    }

    async dropSchema(schemaName: string | undefined): Promise<any> {
        let query = format(SnowflakeSetupQueries.DropSchema, { schemaName })
        return this.runPreparedStatement(query)
    }

}

export default SnowflakeSetupDataAccess;