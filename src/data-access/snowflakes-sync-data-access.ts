import BaseErrorHandler from "../clients/error-handler";
import SnowflakeClient from "../clients/snowflakes-client";
import * as SnowflakeSyncQueries from "../queries/snowflakes-sync-queries"
import Errors from "../common/errors";
import DataAccess from "./base-data-access";
import format from 'string-template'

class SnowflakeSyncDataAccess extends DataAccess {
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

    async useSchema(schemaName: string | undefined) {
        let query = format(SnowflakeSyncQueries.UseSchema, { schemaName })
        return this.runPreparedStatement(query)
    }

    async loadFeedDataFromStage(dbName: string | undefined, schemaName: string | undefined, stageName: string | undefined) {
        let query = format(SnowflakeSyncQueries.LoadFeedData, { dbName, schemaName, stageName })
        return this.runPreparedStatement(query)
    }

    async createDedupTempFromFeed() {
        return this.runPreparedStatement(SnowflakeSyncQueries.CreateDedupTempFromFeed);
    }

    async removeRowNum() {
        return this.runPreparedStatement(SnowflakeSyncQueries.RemoveRowNum);
    }

    async createInitLoadDedupedTable() {
        return this.runPreparedStatement(SnowflakeSyncQueries.CreateInitLoadDedupedTable);
    }

    async loadInitLoadDedupedTable() {
        return this.runPreparedStatement(SnowflakeSyncQueries.LoadInitLoadDedupedTable);
    }

    async dropDedupTempTable() {
        return this.runPreparedStatement(SnowflakeSyncQueries.DropDedupTempTable);
    }

    async createDedupTempTableTelemetry() {
        return this.runPreparedStatement(SnowflakeSyncQueries.CreateDedupTempTableTelemetry);
    }

    async insertTelemetryToDedupTemp() {
        return this.runPreparedStatement(SnowflakeSyncQueries.InsertTelemetryToDedupTemp);
    }

    async mergeTelemetryInternal() {
        return this.runPreparedStatement(SnowflakeSyncQueries.MergeTelemetryInternal);
    }

    async createDedupTempForEvents() {
        return this.runPreparedStatement(SnowflakeSyncQueries.CreateDedupTempForEvents);
    }

    async insertEventToDedupTemp() {
        return this.runPreparedStatement(SnowflakeSyncQueries.InsertEventToDedupTemp);
    }

    async mergeEvents() {
        return this.runPreparedStatement(SnowflakeSyncQueries.MergeEvents);
    }

    async createDedupTempForTrips() {
        return this.runPreparedStatement(SnowflakeSyncQueries.CreateDedupTempForTrips);
    }

    async insertTripToDedupTemp() {
        return this.runPreparedStatement(SnowflakeSyncQueries.InsertTripToDedupTemp);
    }

    async mergeTrips() {
        return this.runPreparedStatement(SnowflakeSyncQueries.MergeTrips);
    }

    async deleteInitLoadV3() {
        return this.runPreparedStatement(SnowflakeSyncQueries.DeleteInitLoadV3);
    }

}

export default SnowflakeSyncDataAccess;