import logger from "../common/logger"

class Service {    

    constructor() { }

    fetchServiceConfig(): any {        
        throw Error("No implementation found for 'fetchServiceConfig' for the requested service")
    }

    async init() {
        throw Error("No implementation found for the requested service")
    }
    
}

export default Service