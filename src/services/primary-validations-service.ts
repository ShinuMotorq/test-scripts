import { PrimaryValidationServiceConfig } from '../objects/primary-validation-service-config';
import Service from './base-service';


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