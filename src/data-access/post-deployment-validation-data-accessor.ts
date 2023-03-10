import BaseSnowflakesDataAccessor from "./base-snowflakes-data-accessor";
import format from 'string-template';
import * as PostDeploymentValidationQueries from '../queries/post-deployment-validations-queries';


class PostDeploymentValidationDataAccessor extends BaseSnowflakesDataAccessor {

    constructor() {
        super();
    }

    async getTelemetryDataVolume(dbName: string | undefined, schemaName: string, startTimestamp: string, endTimestamp: string) {
        let query = format(PostDeploymentValidationQueries.TelemetryVolumeCheck, {
            prodEnvironment: dbName,
            prodSchema: schemaName,
            startTimestamp: startTimestamp,
            endTimestamp: endTimestamp
        })
        return await this.runPreparedStatement(query);
    }

    async getTelemetryDataVolumePerVID(dbName: string | undefined, schemaName: string, startTimestamp: string, endTimestamp: string) {
        let query = format(PostDeploymentValidationQueries.TelemetryPerVinVolumeCheck, {
            prodEnvironment: dbName,
            prodSchema: schemaName,
            startTimestamp: startTimestamp,
            endTimestamp: endTimestamp
        })
        return await this.runPreparedStatement(query);
    }

    async getEventsDataVolume(dbName: string | undefined, schemaName: string, startTimestamp: string, endTimestamp: string) {
        let query = format(PostDeploymentValidationQueries.EventVolumeCheck, {
            prodEnvironment: dbName,
            prodSchema: schemaName,
            startTimestamp: startTimestamp,
            endTimestamp: endTimestamp
        })
        return await this.runPreparedStatement(query);
    }

    async getEventsDataVolumePerType(dbName: string | undefined, schemaName: string, startTimestamp: string, endTimestamp: string) {
        let query = format(PostDeploymentValidationQueries.EventsPerTypeVolumeCheck, {
            prodEnvironment: dbName,
            prodSchema: schemaName,
            startTimestamp: startTimestamp,
            endTimestamp: endTimestamp
        })
        return await this.runPreparedStatement(query);
    }

    async getEventsDataVolumePerTypePerVID(dbName: string | undefined, schemaName: string, startTimestamp: string, endTimestamp: string) {
        let query = format(PostDeploymentValidationQueries.EventsPerVIDVolumeCheck, {
            prodEnvironment: dbName,
            prodSchema: schemaName,
            startTimestamp: startTimestamp,
            endTimestamp: endTimestamp
        })
        return await this.runPreparedStatement(query);
    }

    async getTripsDataVolume(dbName: string | undefined, schemaName: string, startTimestamp: string, endTimestamp: string) {
        let query = format(PostDeploymentValidationQueries.TripsVolumeCheck, {
            prodEnvironment: dbName,
            prodSchema: schemaName,
            startTimestamp: startTimestamp,
            endTimestamp: endTimestamp
        })
        return await this.runPreparedStatement(query);
    }

    async getTripsDataVolumePerType(dbName: string | undefined, schemaName: string, startTimestamp: string, endTimestamp: string) {
        let query = format(PostDeploymentValidationQueries.TripsPerVIDVolumeCheck, {
            prodEnvironment: dbName,
            prodSchema: schemaName,
            startTimestamp: startTimestamp,
            endTimestamp: endTimestamp
        })
        return await this.runPreparedStatement(query);
    }
}

export default PostDeploymentValidationDataAccessor;