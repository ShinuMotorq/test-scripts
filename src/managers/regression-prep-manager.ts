import RegressionPrepDataAccessor from "../data-access/regression-prep-data-accessor";
import { RegressionPrepServiceConfig } from "../objects/config-schemas/regression-prep-service-config";
import Manager from "./base-manager";
import logger from "../common/logger";

class RegressionPrepManager extends Manager {

    private serviceConfig: RegressionPrepServiceConfig;
    private regressionPrepDataAccess: RegressionPrepDataAccessor;
    private scope = 'RegressionPrepManager';

    constructor(serviceConfig: RegressionPrepServiceConfig) {
        super()
        this.serviceConfig = serviceConfig;
        this.regressionPrepDataAccess = new RegressionPrepDataAccessor();
    }

    async selectSchema() {
        let rows: any = await this.regressionPrepDataAccess.showSchemas();
        logger.info(this.scope, `Snowflakes response : Found ${rows.length} schemas`)
        rows = await this.regressionPrepDataAccess.useSchema(this.serviceConfig.replayContext.schemaName);
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
    }

    async setupVehiclesTable() {
        let rows: any = await this.regressionPrepDataAccess.createVehiclesTable(
            this.serviceConfig.vehicles.entities[0].prodEnvironment,
            this.serviceConfig.vehicles.entities[0].prodSchema
        );
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
        for (let env of this.serviceConfig.vehicles.entities) {
            rows = await this.regressionPrepDataAccess.loadVehiclesTable(
                env.prodEnvironment,
                env.prodSchema,
                this.serviceConfig.vehicles.datasource,
            );
            logger.info(this.scope, `Snowflakes response : ${rows[0].status}`);
        }
    }

    async setupProdTelemetryTable() {
        let rows: any = await this.regressionPrepDataAccess.createProdTelemetryTable(
            this.serviceConfig.prodTables.prodTelemetryTableName,
            this.serviceConfig.prodTables.prodEnvironment,
            this.serviceConfig.prodTables.prodSchema
        );
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`);
        rows = await this.regressionPrepDataAccess.loadProdTelemetryTable(
            this.serviceConfig.prodTables.prodTelemetryTableName,
            this.serviceConfig.prodTables.prodEnvironment,
            this.serviceConfig.prodTables.prodSchema,
            this.serviceConfig.vehicles.datasource,
            this.serviceConfig.prodTables.minTimestamp,
            this.serviceConfig.prodTables.maxTimestamp
        );
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`);
    }

    async setupProdEventsTable() {
        let rows: any = await this.regressionPrepDataAccess.createProdEventsTable(
            this.serviceConfig.prodTables.prodEventsTableName,
            this.serviceConfig.prodTables.prodEnvironment,
            this.serviceConfig.prodTables.prodSchema
        )
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
        rows = await this.regressionPrepDataAccess.loadProdEventsTable(
            this.serviceConfig.prodTables.prodEventsTableName,
            this.serviceConfig.prodTables.prodEnvironment,
            this.serviceConfig.prodTables.prodSchema,
            this.serviceConfig.vehicles.datasource,
            this.serviceConfig.prodTables.minTimestamp,
            this.serviceConfig.prodTables.maxTimestamp
        )
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
    }

    async setupProdTripsTable() {
        let rows: any = await this.regressionPrepDataAccess.createProdTripsTable(
            this.serviceConfig.prodTables.prodTripsTableName,
            this.serviceConfig.prodTables.prodEnvironment,
            this.serviceConfig.prodTables.prodSchema
        )
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
        rows = await this.regressionPrepDataAccess.loadProdTripsTable(
            this.serviceConfig.prodTables.prodTripsTableName,
            this.serviceConfig.prodTables.prodEnvironment,
            this.serviceConfig.prodTables.prodSchema,
            this.serviceConfig.vehicles.datasource,
            this.serviceConfig.prodTables.minTimestamp,
            this.serviceConfig.prodTables.maxTimestamp
        )
        logger.info(this.scope, `Snowflakes response : ${rows[0].status}`)
    }
}

export default RegressionPrepManager;