import { RequestType } from "../common/enums";
import Service from "./base-service";
import PostDeploymentValidationsService from "./post-deployment-validations-service";
import RegressionPrepService from "./regression-prep-service";
import SnowflakeSetupService from "./snowflakes-setup-service";
import SnowflakeSyncService from "./snowflakes-sync-service";

class ServiceProvider {

    constructor() { }

    getService(requestType: RequestType) {
        switch (requestType) {
            case RequestType.SNOWFLAKE_SETUP: return new SnowflakeSetupService();
            case RequestType.SNOWFLAKE_SYNC: return new SnowflakeSyncService();
            case RequestType.POST_DEPLOYMENT_VALIDATION: return new PostDeploymentValidationsService();
            case RequestType.REGRESSION_PREP: return new RegressionPrepService();
            default: return new Service();
        }
    }
}

export default ServiceProvider;