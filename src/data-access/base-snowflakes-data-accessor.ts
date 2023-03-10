import BaseErrorHandler from "../clients/error-handler";
import SnowflakeClient from "../clients/snowflakes-client";
import Errors from "../common/errors";
import DataAccessor from "./base-data-accessor";
import format from 'string-template'
import * as SnowflakeCommonQueries from "../queries/snowflakes-common-queries"

class BaseSnowflakesDataAccessor extends DataAccessor {

    protected snowflakesClient: SnowflakeClient | null = null

    constructor() {
        super()
    }

    async runPreparedStatement(query) {
        try {
            this.snowflakesClient = await SnowflakeClient.getInstance();
            return await this.snowflakesClient.runStatement(query);
        } catch (err: any) {
            throw new BaseErrorHandler(Errors.SnowflakeQueryFailed, {
                query: query,
                errorMessage: err.message
            })
        }
    }

    async useSchema(schemaName: string | undefined) {
        let query = format(SnowflakeCommonQueries.UseSchema, { schemaName })
        return await this.runPreparedStatement(query)
    }


    async showSchemas(): Promise<any> {
        return await this.runPreparedStatement(SnowflakeCommonQueries.ShowSchemas)
    }

    async createSchema(schemaName: string | undefined): Promise<any> {
        let query = format(SnowflakeCommonQueries.CreateSchema, { schemaName })
        return await this.runPreparedStatement(query)
    }

    async showStages() {
        return await this.runPreparedStatement(SnowflakeCommonQueries.ShowStages)
    }

    async dropSchema(schemaName: string | undefined): Promise<any> {
        let query = format(SnowflakeCommonQueries.DropSchema, { schemaName })
        return await this.runPreparedStatement(query)
    }
}

export default BaseSnowflakesDataAccessor