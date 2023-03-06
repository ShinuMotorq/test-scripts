import SnowflakeSyncManager from "../managers/snowflakes-sync-manager";
import { SnowflakeSyncServiceConfig } from "../objects/snowflakes-sync-service-config";
import Service from "./base-service";
import logger from "../common/logger"
import SnowflakeClient from "../clients/snowflakes-client";


class SnowflakeSyncService extends Service {

    private snowflakesSyncManager: SnowflakeSyncManager
    private serviceConfig: SnowflakeSyncServiceConfig
    private scope = "SnowflakeSyncService"

    constructor() {
        super();
        this.serviceConfig = this.fetchServiceConfig();
        this.snowflakesSyncManager = new SnowflakeSyncManager(this.serviceConfig)
    }

    /**
 * @returns service specific config
 */
    fetchServiceConfig(): SnowflakeSyncServiceConfig {
        return require('../../configs/snowflake-sync-config.json')
    }

    /**
     * @override
     * Service level override for init()
     */
    async init() {
        logger.info(this.scope, "~~~~~~ Snowflake sync started ~~~~~~")
        try {
           await this.snowflakesSyncManager.loadAndDedupFeedData()
           await this.snowflakesSyncManager.loadAndDedupTelemetryData()
           await this.snowflakesSyncManager.loadAndDedupEventsData()
           await this.snowflakesSyncManager.loadAndDedupTripsData()
           await this.snowflakesSyncManager.deleteInitLoadV3()
        } catch (err: any) {
            logger.error(this.scope, err.message);
            logger.error(this.scope, "xxxxxxxx Snowflake sync failed xxxxxxxx")
        } finally {
            // -- TearDown scripts here --            
            await SnowflakeClient.terminateOpenConnection()
        }
        logger.info(this.scope, "~~~~~~ Snowflake sync ends here ~~~~~~")
    }
}

export default SnowflakeSyncService;