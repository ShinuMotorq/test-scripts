import SnowflakeSyncDataAccessor from '../data-access/snowflakes-sync-data-accessor';
import { SnowflakeSyncServiceConfig } from '../objects/config-schemas/snowflakes-sync-service-config';
import Manager from './base-manager';
import logger from "../common/logger";

class SnowflakeSyncManager extends Manager {
    private snowflakesSyncDataAccess: SnowflakeSyncDataAccessor | null = null
    private serviceConfig: SnowflakeSyncServiceConfig
    private scope = 'SnowflakeSetupManager'

    constructor(serviceConfig: SnowflakeSyncServiceConfig) {
        super();
        this.serviceConfig = serviceConfig;
        this.snowflakesSyncDataAccess = new SnowflakeSyncDataAccessor();
    }

    async loadAndDedupFeedData() {
        let rows: any = await this.snowflakesSyncDataAccess!.useSchema(this.serviceConfig.schemaName);
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
        rows = await this.snowflakesSyncDataAccess!.loadFeedDataFromStage(
            this.serviceConfig.dbName,
            this.serviceConfig.schemaName,
            this.serviceConfig.stageName)
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
        rows = await this.snowflakesSyncDataAccess!.createDedupTempFromFeed();
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
        rows = await this.snowflakesSyncDataAccess!.removeRowNum();
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
        rows = await this.snowflakesSyncDataAccess!.createInitLoadDedupedTable();
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
        rows = await this.snowflakesSyncDataAccess!.loadInitLoadDedupedTable();
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
        rows = await this.snowflakesSyncDataAccess!.dropDedupTempTable();
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
    }

    async loadAndDedupTelemetryData() {
        let rows: any = await this.snowflakesSyncDataAccess!.createDedupTempTableTelemetry();
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
        rows = await this.snowflakesSyncDataAccess!.insertTelemetryToDedupTemp();
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
        rows = await this.snowflakesSyncDataAccess!.mergeTelemetryInternal();
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
    }

    async loadAndDedupEventsData() {
        let rows: any = await this.snowflakesSyncDataAccess!.createDedupTempForEvents();
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
        rows = await this.snowflakesSyncDataAccess!.insertEventToDedupTemp();
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
        rows = await this.snowflakesSyncDataAccess!.mergeEvents();
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
    }

    async loadAndDedupTripsData() {
        let rows: any = await this.snowflakesSyncDataAccess!.createDedupTempForTrips();
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
        rows = await this.snowflakesSyncDataAccess!.insertTripToDedupTemp();
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
        rows = await this.snowflakesSyncDataAccess!.mergeTrips();
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
    }

    async deleteInitLoadV3() {
        let rows: any = await this.snowflakesSyncDataAccess!.deleteInitLoadV3();
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
    }
}

export default SnowflakeSyncManager;