import PostDeploymentValidationDataAccessor from "../data-access/post-deployment-validation-data-accessor";
import { PostDeploymentValidationsServiceConfig } from "../objects/config-schemas/post-deployment-validations-service-config";
import Manager from "./base-manager";
import logger from "../common/logger";
import _ from "lodash";

class PostDeploymentValidationManager extends Manager {

    private serviceConfig: PostDeploymentValidationsServiceConfig;
    private postDeploymentValidationDataAccessor: PostDeploymentValidationDataAccessor;
    private scope = 'PostDeploymentValidationManager'

    constructor(serviceConfig: PostDeploymentValidationsServiceConfig) {
        super();
        this.serviceConfig = serviceConfig;
        this.postDeploymentValidationDataAccessor = new PostDeploymentValidationDataAccessor();
    }

    async compareTelemetryVolume() {
        let previousTelemetryRows: any = await this.postDeploymentValidationDataAccessor.getTelemetryDataVolume(
            this.serviceConfig.environment,
            this.serviceConfig.schema,
            this.serviceConfig.prevStartTimestamp,
            this.serviceConfig.prevEndTimestamp);
        logger.info(this.scope, `Snowflakes response : ${previousTelemetryRows['TELEMETRYVOLUME']}`)
        let currentTelemetryRows: any = await this.postDeploymentValidationDataAccessor.getTelemetryDataVolume(
            this.serviceConfig.environment,
            this.serviceConfig.schema,
            this.serviceConfig.currentStartTimestamp,
            this.serviceConfig.currentEndTimestamp);
        logger.info(this.scope, `Snowflakes response : ${currentTelemetryRows['TELEMETRYVOLUME']}`)

        // Compare telemetry values
        let previousTelemetryCount = (<Object>_.first(previousTelemetryRows))['TELEMETRYVOLUME']
        let currentTelemetryCount = (<Object>_.first(currentTelemetryRows))['TELEMETRYVOLUME']

        let telemetryVolumeDifference = Math.abs(currentTelemetryCount - previousTelemetryCount)
        let telemetryVolumeDifferencePercentage = Math.abs(currentTelemetryCount - previousTelemetryCount) / Math.max(currentTelemetryCount, previousTelemetryCount) * 100
        return {
            previousTelemetryCount,
            currentTelemetryCount,
            telemetryVolumeDifference,
            // telemetryVolumeDifferencePercentage
        }
    }

    async compareTelemetryVolumePerVID() {
        let previousTelemetryRows: any = await this.postDeploymentValidationDataAccessor.getTelemetryDataVolumePerVID(
            this.serviceConfig.environment,
            this.serviceConfig.schema,
            this.serviceConfig.prevStartTimestamp,
            this.serviceConfig.prevEndTimestamp);
        logger.info(this.scope, `Snowflakes response : Found ${previousTelemetryRows.length} rows`)
        let currentTelemetryRows: any = await this.postDeploymentValidationDataAccessor.getTelemetryDataVolumePerVID(
            this.serviceConfig.environment,
            this.serviceConfig.schema,
            this.serviceConfig.currentStartTimestamp,
            this.serviceConfig.currentEndTimestamp);
        logger.info(this.scope, `Snowflakes response : Found ${currentTelemetryRows.length} rows`)
        // Compare telemetry values

        let previousTelemetryVolumePerVID = previousTelemetryRows.slice(0, 10)
        let currentTelemetryVolumePerVID = currentTelemetryRows.slice(0, 10)
        return { previousTelemetryVolumePerVID, currentTelemetryVolumePerVID }
    }

    async compareEventsVolume() {
        let previousEventsRows: any = await this.postDeploymentValidationDataAccessor.getEventsDataVolume(
            this.serviceConfig.environment,
            this.serviceConfig.schema,
            this.serviceConfig.prevStartTimestamp,
            this.serviceConfig.prevEndTimestamp);
        logger.info(this.scope, `Snowflakes response : ${previousEventsRows['EVENTSVOLUME']}`)
        let currentEventsRows: any = await this.postDeploymentValidationDataAccessor.getEventsDataVolume(
            this.serviceConfig.environment,
            this.serviceConfig.schema,
            this.serviceConfig.currentStartTimestamp,
            this.serviceConfig.currentEndTimestamp);
        logger.info(this.scope, `Snowflakes response : ${currentEventsRows['EVENTSVOLUME']}`)

        // Compare events values
        let previousEventsCount = (<Object>_.first(previousEventsRows))['EVENTSVOLUME']
        let currentEventsCount = (<Object>_.first(currentEventsRows))['EVENTSVOLUME']

        let eventsVolumeDifference = Math.abs(currentEventsCount - previousEventsCount)
        let eventsVolumeDifferencePercentage = eventsVolumeDifference * Math.max(previousEventsRows, currentEventsRows)
        return {
            previousEventsCount,
            currentEventsCount,
            eventsVolumeDifference,
            // eventsVolumeDifferencePercentage
        }
    }

    async compareEventsVolumePerType() {
        let previousEventsRows: any = await this.postDeploymentValidationDataAccessor.getEventsDataVolumePerType(
            this.serviceConfig.environment,
            this.serviceConfig.schema,
            this.serviceConfig.prevStartTimestamp,
            this.serviceConfig.prevEndTimestamp);
        logger.info(this.scope, `Snowflakes response : Found ${previousEventsRows.length} rows`)
        let currentEventsRows: any = await this.postDeploymentValidationDataAccessor.getEventsDataVolumePerType(
            this.serviceConfig.environment,
            this.serviceConfig.schema,
            this.serviceConfig.currentStartTimestamp,
            this.serviceConfig.currentEndTimestamp);
        logger.info(this.scope, `Snowflakes response : Found ${currentEventsRows.length} rows`)
        // Compare events values

        let previousEventsVolumePerType = previousEventsRows.slice(0, 25)
        let currentEventsVolumePerType = currentEventsRows.slice(0, 25)
        return { previousEventsVolumePerType, currentEventsVolumePerType }
    }

    async compareEventsVolumePerTypePerVID() {
        let previousEventsRows: any = await this.postDeploymentValidationDataAccessor.getEventsDataVolumePerTypePerVID(
            this.serviceConfig.environment,
            this.serviceConfig.schema,
            this.serviceConfig.prevStartTimestamp,
            this.serviceConfig.prevEndTimestamp);
        logger.info(this.scope, `Snowflakes response : Found ${previousEventsRows.length} rows`)
        let currentEventsRows: any = await this.postDeploymentValidationDataAccessor.getEventsDataVolumePerTypePerVID(
            this.serviceConfig.environment,
            this.serviceConfig.schema,
            this.serviceConfig.currentStartTimestamp,
            this.serviceConfig.currentEndTimestamp);
        logger.info(this.scope, `Snowflakes response : Found ${currentEventsRows.length} rows`)
        // Compare events values

        let previousEventsVolumePerTypePerVID = previousEventsRows.slice(0, 100)
        let currentEventsVolumePerTypePerVID = currentEventsRows.slice(0, 100)
        return { previousEventsVolumePerTypePerVID, currentEventsVolumePerTypePerVID }
    }

    async compareTripsVolume() {
        let previousTripsRows: any = await this.postDeploymentValidationDataAccessor.getTripsDataVolume(
            this.serviceConfig.environment,
            this.serviceConfig.schema,
            this.serviceConfig.prevStartTimestamp,
            this.serviceConfig.prevEndTimestamp);
        logger.info(this.scope, `Snowflakes response : ${previousTripsRows['TRIPSVOLUME']}`)
        let currentTripsRows: any = await this.postDeploymentValidationDataAccessor.getTripsDataVolume(
            this.serviceConfig.environment,
            this.serviceConfig.schema,
            this.serviceConfig.currentStartTimestamp,
            this.serviceConfig.currentEndTimestamp);
        logger.info(this.scope, `Snowflakes response : ${currentTripsRows['TRIPSVOLUME']}`)

        // Compare trips values
        let previousTripsCount = (<Object>_.first(previousTripsRows))['TRIPSVOLUME']
        let currentTripsCount = (<Object>_.first(currentTripsRows))['TRIPSVOLUME']

        let tripsVolumeDifference = Math.abs(currentTripsCount - previousTripsCount)
        let tripsVolumeDifferencePercentage = tripsVolumeDifference * Math.max(previousTripsRows, currentTripsRows)
        return {
            previousTripsCount,
            currentTripsCount,
            tripsVolumeDifference,
            // tripsVolumeDifferencePercentage
        }
    }

    async compareTripsVolumePerType() {
        let previousTripsRows: any = await this.postDeploymentValidationDataAccessor.getTripsDataVolumePerType(
            this.serviceConfig.environment,
            this.serviceConfig.schema,
            this.serviceConfig.prevStartTimestamp,
            this.serviceConfig.prevEndTimestamp);
        logger.info(this.scope, `Snowflakes response : Found ${previousTripsRows.length} rows`)
        let currentTripsRows: any = await this.postDeploymentValidationDataAccessor.getTripsDataVolumePerType(
            this.serviceConfig.environment,
            this.serviceConfig.schema,
            this.serviceConfig.currentStartTimestamp,
            this.serviceConfig.currentEndTimestamp);
        logger.info(this.scope, `Snowflakes response : Found ${currentTripsRows.length} rows`)
        // Compare trips values

        let previousEventsVolumePerType = previousTripsRows.slice(0, 10)
        let currentEventsVolumePerType = currentTripsRows.slice(0, 10)
        return { previousEventsVolumePerType, currentEventsVolumePerType }
    }
}

export default PostDeploymentValidationManager;