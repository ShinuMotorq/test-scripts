import { PrimaryValidationServiceConfig } from '../objects/primary-validation-service-config';


class PrimaryValidationService extends Service {

    private serviceConfig: PrimaryValidationServiceConfig;

    constructor() {
        super();
        this.serviceConfig = this.fetchServiceConfig();
    }

    fetchServiceConfig(): PrimaryValidationServiceConfig {
        return require('../../configs/snowflake-sync-config.json')
    }

    async init() {

    }
}

export default PrimaryValidationService;