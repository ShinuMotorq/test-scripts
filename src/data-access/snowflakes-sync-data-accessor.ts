import * as SnowflakeSyncQueries from "../queries/snowflakes-sync-queries";
import format from 'string-template';
import BaseSnowflakesDataAccessor from "./base-snowflakes-data-accessor";

class SnowflakeSyncDataAccessor extends BaseSnowflakesDataAccessor {
    constructor() {
        super()
    }

    async loadFeedDataFromStage(dbName: string | undefined, schemaName: string | undefined, stageName: string | undefined) {
        let query = format(SnowflakeSyncQueries.LoadFeedData, { dbName, schemaName, stageName })
        return await this.runPreparedStatement(query)
    }

    async createDedupTempFromFeed() {
        return await this.runPreparedStatement(SnowflakeSyncQueries.CreateDedupTempFromFeed);
    }

    async removeRowNum() {
        return await this.runPreparedStatement(SnowflakeSyncQueries.RemoveRowNum);
    }

    async createInitLoadDedupedTable() {
        return await this.runPreparedStatement(SnowflakeSyncQueries.CreateInitLoadDedupedTable);
    }

    async loadInitLoadDedupedTable() {
        return await this.runPreparedStatement(SnowflakeSyncQueries.LoadInitLoadDedupedTable);
    }

    async dropDedupTempTable() {
        return await this.runPreparedStatement(SnowflakeSyncQueries.DropDedupTempTable);
    }

    async createDedupTempTableTelemetry() {
        return await this.runPreparedStatement(SnowflakeSyncQueries.CreateDedupTempTableTelemetry);
    }

    async insertTelemetryToDedupTemp() {
        return await this.runPreparedStatement(SnowflakeSyncQueries.InsertTelemetryToDedupTemp);
    }

    async mergeTelemetryInternal() {
        return await this.runPreparedStatement(SnowflakeSyncQueries.MergeTelemetryInternal);
    }

    async createDedupTempForEvents() {
        return await this.runPreparedStatement(SnowflakeSyncQueries.CreateDedupTempForEvents);
    }

    async insertEventToDedupTemp() {
        return await this.runPreparedStatement(SnowflakeSyncQueries.InsertEventToDedupTemp);
    }

    async mergeEvents() {
        return await this.runPreparedStatement(SnowflakeSyncQueries.MergeEvents);
    }

    async createDedupTempForTrips() {
        return await this.runPreparedStatement(SnowflakeSyncQueries.CreateDedupTempForTrips);
    }

    async insertTripToDedupTemp() {
        return await this.runPreparedStatement(SnowflakeSyncQueries.InsertTripToDedupTemp);
    }

    async mergeTrips() {
        return await this.runPreparedStatement(SnowflakeSyncQueries.MergeTrips);
    }

    async deleteInitLoadV3() {
        return await this.runPreparedStatement(SnowflakeSyncQueries.DeleteInitLoadV3);
    }

}

export default SnowflakeSyncDataAccessor;