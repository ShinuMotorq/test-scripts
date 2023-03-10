import * as SnowflakeSetupQueries from "../queries/snowflakes-setup-queries"
import format from 'string-template'
import BaseSnowflakesDataAccessor from "./base-snowflakes-data-accessor";


class SnowflakeSetupDataAccessor extends BaseSnowflakesDataAccessor {    

    constructor() {
        super()
    }

    async createStage(stageName, containerUrl, containerToken) {
        let query = format(SnowflakeSetupQueries.CreateStage, {
            stageName,
            containerUrl,
            containerToken,
        })
        return await this.runPreparedStatement(query);
    }

    async verifyStage(stageName) {
        let query = format(SnowflakeSetupQueries.VerifyStage, { stageName })
        return await this.runPreparedStatement(query);
    }

    async createFileFormat(avroFileFormatName) {
        let query = format(SnowflakeSetupQueries.CreateFileFormat, { avroFileFormatName })
        return await this.runPreparedStatement(query)
    }

    async alterStageFileFormat(avroFileFormatName, stageName) {
        let query = format(SnowflakeSetupQueries.AlterStageFileFormat, {
            avroFileFormatName,
            stageName
        })
        return await this.runPreparedStatement(query)
    }

}

export default SnowflakeSetupDataAccessor;