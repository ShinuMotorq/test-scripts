import CommonUtils from "./utils/common-utils";
import ServiceProvider from "./services/service-provider";
import { RequestType } from "./common/enums";
import Service from "./services/base-service";

/**
 * Entry point. Gets request type from args to fetch the required services
 */
async function main() {    
    let args = await (new CommonUtils()).getParams();
    let service : Service = (new ServiceProvider()).getService(RequestType[args.type.toUpperCase()])
    await service.init();
}

main();