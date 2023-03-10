import BaseErrorHandler from "../clients/error-handler";
import { POST_DEPLOYMENT_VALIDATION_REPORT, REPORTS_FILEPATH } from "../common/constants";
import Errors from '../common/errors';
import * as fs from "fs";
import logger from "../common/logger";

class BaseReportManager {

    private scope = 'BaseReportManager'

    constructor() {

    }

    async generatePostDeploymentValidationReport(environment: string, volumeMetrics) {
        let reportDir = `${REPORTS_FILEPATH}/${POST_DEPLOYMENT_VALIDATION_REPORT}`
        try {
            if (!fs.existsSync(`${reportDir}`))
                fs.mkdirSync(`${reportDir}`, { recursive: true })
            fs.writeFileSync(`${reportDir}/${environment}_report.json`, JSON.stringify(volumeMetrics, null, 2))
            logger.info(this.scope, `Report "${reportDir}/${environment}_report.json" generated succesfully!!`)
        } catch (err: any) {
            throw new BaseErrorHandler(Errors.ReportGenerationFailed, { errorMessage: err.message });
        }
    }

}

export default BaseReportManager;