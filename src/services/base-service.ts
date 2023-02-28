class Service {

    constructor() { }

    async init() {
        throw Error("No implementation found for the requested service")
    }
    
}