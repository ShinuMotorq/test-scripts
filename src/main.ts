import CommonUtils from "./utils/common-utils";
import ServiceProvider from "./services/service-provider";
import { RequestType } from "./common/enums";
import Service from "./services/base-service";
import logger from "./common/logger";

/**
 * Entry point. Gets request type from args to fetch the required services
 */
async function main() {
    let args = await (new CommonUtils()).getParams();
    logger.info("MAIN", `Found request for ${args.type.map(arg => arg.toUpperCase()).join(", ")}`);
    for (let arg of args.type) {
        let service: Service = (new ServiceProvider()).getService(RequestType[arg.toUpperCase()])
        await service.init();
    }
}

main();