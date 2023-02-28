import SnowflakeClient from "../clients/snowflakes-client";
import * as SnowflakeSyncSetupQueries from "../queries/sync-setup-queries"
import * as format from 'string-template';


class SnowflakeSyncSetupDataAccess {

    private sfClient: SnowflakeClient | null = null

    constructor() { }

    async showSchemas(): Promise<any> {
        this.sfClient = await SnowflakeClient.getInstance();
        return await this.sfClient.runStatement(SnowflakeSyncSetupQueries.ShowSchemas)
    }

    async createSchema(schemaName: string | undefined): Promise<any> {
        // let connection = await SnowflakeDBClient.getConnection()
        // let rows: any = await this.snowflakesClient.runStatement(connection, format(SnowflakeSyncSetupQueries.CreateSchema, { schemaName }))
        // console.log(rows)

        this.sfClient = await SnowflakeClient.getInstance();
        return await this.sfClient!.runStatement(format(SnowflakeSyncSetupQueries.CreateSchema, { schemaName }))        
    }

    async useSchema(schemaName: string | undefined) {
        this.sfClient = await SnowflakeClient.getInstance();
        return await this.sfClient!.runStatement(format(SnowflakeSyncSetupQueries.UseSchema, { schemaName }))        
    }

    async showStages() {
        this.sfClient = await SnowflakeClient.getInstance();
        return await this.sfClient.runStatement(SnowflakeSyncSetupQueries.ShowStages)
    }

    async createStage(stageName, containerUrl, containerToken) {
        this.sfClient = await SnowflakeClient.getInstance()
        return await this.sfClient!.runStatement(format(SnowflakeSyncSetupQueries.CreateStage, {
            stageName,
            containerUrl,
            containerToken,
        }))        
    }

    async createFileFormat(avroFileFormatName) {
        this.sfClient = await SnowflakeClient.getInstance()
        return await this.sfClient.runStatement(format(SnowflakeSyncSetupQueries.CreateFileFormat, {
            avroFileFormatName
        }));
    }

    async alterStageFileFormat(avroFileFormatName, stageName) {
        this.sfClient = await SnowflakeClient.getInstance()
        return await this.sfClient.runStatement(format(SnowflakeSyncSetupQueries.AlterStageFileFormat, {
            avroFileFormatName,
            stageName
        }));
    }
}

export default SnowflakeSyncSetupDataAccess;