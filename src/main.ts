import CommonUtils from "./utils/common-utils";
import ServiceProvider from "./services/service-provider";
import { RequestType } from "./common/enums";

/**
 * Entry point. Gets request type from args to fetch required service
 */
async function main() {    
    let args = await (new CommonUtils()).getParams();
    let service : Service = (new ServiceProvider()).getService(RequestType[args.type])
    await service.init();
}

main();