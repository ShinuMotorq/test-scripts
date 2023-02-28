import { RequestType } from "../common/enums";
import PrimaryValidationService from "./primary-validations-service";
import SnowflakeSyncSetupService from "./sfsync-setup-service";

class ServiceProvider {

    constructor() {

    }

    getService(requestType : RequestType) {
        switch(requestType) {
            case RequestType.SNOWFLAKE_SYNC : return new SnowflakeSyncSetupService();
            case RequestType.PRIMARY_VALIDATIONS : return new PrimaryValidationService();
        }
    }
}

export default ServiceProvider;