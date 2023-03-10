import PostDeploymentValidationManager from '../managers/post-deployment-validation-manager';
import { PostDeploymentValidationsServiceConfig as PostDeploymentValidationsServiceConfig } from '../objects/config-schemas/post-deployment-validations-service-config';
import Service from './base-service';
import logger from "../common/logger";
import SnowflakeClient from '../clients/snowflakes-client';
import BaseReportManager from '../managers/reports-manager';


class PostDeploymentValidationsService extends Service {

    private serviceConfig: PostDeploymentValidationsServiceConfig;
    private postDeploymentValidationManager: PostDeploymentValidationManager;
    private baseReportManager: BaseReportManager
    private scope = 'PostDeploymentValidationsService';

    constructor() {
        super();
        this.serviceConfig = this.fetchServiceConfig();
        this.postDeploymentValidationManager = new PostDeploymentValidationManager(this.serviceConfig);
        this.baseReportManager = new BaseReportManager();
    }

    /**
     * @returns service specific config
     */
    fetchServiceConfig(): PostDeploymentValidationsServiceConfig {
        return require('../../configs/post-deployment-validations-config.json');
    }

    /**
     * @override
     * Service level override for init()
     */
    async init() {
        logger.info(this.scope, "~~~~~~ Post deployment validation started ~~~~~~")
        try {
            let telemetryVolume = await this.postDeploymentValidationManager.compareTelemetryVolume();
            let telemetryVolumePerVID = await this.postDeploymentValidationManager.compareTelemetryVolumePerVID();
            let eventsVolume = await this.postDeploymentValidationManager.compareEventsVolume();
            let eventsVolumePerType = await this.postDeploymentValidationManager.compareEventsVolumePerType();
            let eventsVolumePerTypePerVID = await this.postDeploymentValidationManager.compareEventsVolumePerTypePerVID();
            let tripsVolume = await this.postDeploymentValidationManager.compareTripsVolume();
            let tripsVolumePerType = await this.postDeploymentValidationManager.compareTripsVolumePerType();
            await this.baseReportManager.generatePostDeploymentValidationReport(this.serviceConfig.environment, {
                telemetryVolume,
                telemetryVolumePerVID,
                eventsVolume,
                eventsVolumePerType,
                eventsVolumePerTypePerVID,
                tripsVolume,
                tripsVolumePerType
            })
        } catch (err: any) {
            logger.error(this.scope, err.message);
            logger.error(this.scope, "xxxxxxxx Post deployment validation failed xxxxxxxx");
        } finally {
            // -- TearDown scripts here --
            await SnowflakeClient.terminateOpenConnection();
        }
        logger.info(this.scope, "~~~~~~ Post deployment validation ends here ~~~~~~");
    }
}

export default PostDeploymentValidationsService;