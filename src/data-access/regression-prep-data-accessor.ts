import SnowflakeClient from "../clients/snowflakes-client";
import BaseSnowflakesDataAccessor from "./base-snowflakes-data-accessor";
import format from 'string-template';
import * as RegressionPrepQueries from '../queries/regression-prep-queries';

class RegressionPrepDataAccessor extends BaseSnowflakesDataAccessor {

    private sfClient: SnowflakeClient | null = null

    constructor() {
        super()
    }

    async createVehiclesTable(prodEnvironment: string, prodSchema: string) {
        let query = format(RegressionPrepQueries.CreateVehiclesTable, {
            prodEnvironment, 
            prodSchema
        })
        return await this.runPreparedStatement(query);
    }

    async loadVehiclesTable(prodEnvironment: string, prodSchema: string, datasource: string) {
        let query = format(RegressionPrepQueries.LoadVehiclesData, {
            prodEnvironment, 
            prodSchema, 
            datasource
        })
        return await this.runPreparedStatement(query);
    }

    async createProdTelemetryTable(prodTelemetryTable: string, prodEnvironment: string, prodSchema: string) {
        let query = format(RegressionPrepQueries.CreateProdTelemetryTable, {
            prodTelemetryTable, 
            prodEnvironment, 
            prodSchema
        })
        return await this.runPreparedStatement(query);
    }

    async loadProdTelemetryTable(prodTelemetryTable: string, prodEnvironment: string, prodSchema: string, datasource: string, startTimestamp: string, endTimestamp: string) {
        let query = format(RegressionPrepQueries.LoadProdTelemetryData, {
            prodTelemetryTable, 
            prodEnvironment, 
            prodSchema, 
            datasource, 
            startTimestamp, 
            endTimestamp,
        })
        return await this.runPreparedStatement(query);
    }

    async createProdEventsTable(prodEventTable: string, prodEnvironment: string, prodSchema: string) {
        let query = format(RegressionPrepQueries.CreateProdEventTable, {
            prodEventTable,
            prodEnvironment,
            prodSchema
        })
        return await this.runPreparedStatement(query);
    }

    async loadProdEventsTable(prodEventTable: string, prodEnvironment: string, prodSchema: string, datasource: string, startTimestamp: string, endTimestamp: string) {
        let query = format(RegressionPrepQueries.LoadProdEventTable, {
            prodEventTable,
            prodEnvironment,
            prodSchema,
            datasource,
            startTimestamp,
            endTimestamp,
        })
        return await this.runPreparedStatement(query);
    }

    async createProdTripsTable(prodTripsTable: string, prodEnvironment: string, prodSchema: string) {
        let query = format(RegressionPrepQueries.CreateProdTripTable, {
            prodTripsTable,
            prodEnvironment,
            prodSchema
        })
        return await this.runPreparedStatement(query);
    }

    async loadProdTripsTable(prodTripsTable: string, prodEnvironment: string, prodSchema: string, datasource: string, startTimestamp: string, endTimestamp: string) {
        let query = format(RegressionPrepQueries.LoadProdTripTable, {
            prodTripsTable,
            prodEnvironment,
            prodSchema,
            datasource,
            startTimestamp,
            endTimestamp,
        })
        return await this.runPreparedStatement(query);
    }
}

export default RegressionPrepDataAccessor;