import { RegressionPrepServiceConfig } from "../objects/config-schemas/regression-prep-service-config";
import Service from "./base-service";
import RegressionPrepManager from "../managers/regression-prep-manager";
import logger from "../common/logger";
import SnowflakeClient from "../clients/snowflakes-client";

class RegressionPrepService extends Service {

    private serviceConfig: RegressionPrepServiceConfig;
    private regressionPrepManager: RegressionPrepManager;
    private scope = "RegressionPrepService"

    constructor() {
        super();
        this.serviceConfig = this.fetchServiceConfig();
        this.regressionPrepManager = new RegressionPrepManager(this.serviceConfig)
    }

    /**
     * @returns service specific config
     */
    fetchServiceConfig(): RegressionPrepServiceConfig {
        return require('../../configs/regression-prep-config.json')
    }

    /**
     * @override
     * Service level override for init()
     */
    async init() {
        logger.info(this.scope, "~~~~~~ Regression prep started ~~~~~~")
        try {
            await this.regressionPrepManager.selectSchema()
            await this.regressionPrepManager.setupVehiclesTable();
            await this.regressionPrepManager.setupProdTelemetryTable();
            await this.regressionPrepManager.setupProdEventsTable();
            await this.regressionPrepManager.setupProdTripsTable();
        } catch (err: any) {
            logger.error(this.scope, err.message);
            logger.error(this.scope, "xxxxxxxx Regression prep failed xxxxxxxx")
        } finally {
            // -- TearDown scripts here --
            await SnowflakeClient.terminateOpenConnection()
        }
        logger.info(this.scope, "~~~~~~ Regression prep ends here ~~~~~~")
    }
}

export default RegressionPrepService;