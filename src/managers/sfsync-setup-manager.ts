import * as SnowflakeSyncSetupQueries from "../queries/sync-setup-queries"
import * as format from 'string-template';
import SnowflakeClient from "../clients/snowflakes-client";
import SnowflakeSyncSetupDataAccess from "../data-access/sfsync-setup-data-access";

class SnowflakeSyncSetupManager {
    /**
     * @todo : decouple DB calls to DAO class with abstraction over different DB
     */
    private sfClient: SnowflakeClient | null = null
    private sfSyncSetupDataAccess: SnowflakeSyncSetupDataAccess | null = null

    constructor() {
    }

    async getAllSchemas(): Promise<string[]> {
        let rows: any = await this.sfSyncSetupDataAccess?.showSchemas();
        return rows.map(row => row.name);
    }

    async createSchema(schemaName: string | undefined) {
        let rows: any = await this.sfSyncSetupDataAccess?.createSchema(schemaName);
        console.log(rows)
    }

    async useSchema(schemaName: string | undefined) {
        let rows: any = await this.sfSyncSetupDataAccess?.useSchema(schemaName);
        console.log(rows)
    }

    async getAllStages(): Promise<string[]> {
        let rows: any = await this.sfSyncSetupDataAccess?.showStages();
        return rows.map(row => row.name);
    }

    async createStage(stageName, containerUrl, containerToken) {
        let rows: any = await this.sfSyncSetupDataAccess?.createStage(stageName, containerUrl, containerToken);
        console.log(rows)
    }

    async createAndAlterFileformat(avroFileFormatName, stageName) {
        let rows: any = await this.sfSyncSetupDataAccess?.createFileFormat(avroFileFormatName)
        console.log(rows);
        rows = await this.sfSyncSetupDataAccess?.alterStageFileFormat(
            avroFileFormatName,
            stageName
        )
        console.log(rows);
    }


}

export { SnowflakeSyncSetupManager };