import { RequestType } from "../common/enums";
import Service from "./base-service";
import PrimaryValidationService from "./primary-validations-service";
import SnowflakeSetupService from "./snowflakes-setup-service";
import SnowflakeSyncService from "./snowflakes-sync-service";

class ServiceProvider {

    constructor() { }

    getService(requestType : RequestType) {
        switch(requestType) {
            case RequestType.SNOWFLAKE_SETUP : return new SnowflakeSetupService();
            case RequestType.SNOWFLAKE_SYNC : return new SnowflakeSyncService();
            case RequestType.PRIMARY_VALIDATIONS : return new PrimaryValidationService();
            default : return new Service();
        }
    }
}

export default ServiceProvider;