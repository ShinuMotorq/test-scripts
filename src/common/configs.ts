import { config } from 'dotenv';
import { AppConfig } from '../schemas/config-schemas';

/**
 * All configs init here
 */

config()

const appConfig: AppConfig = {
    snowflakeConnectionConfig: {
        account: process.env.SNOWFLAKE_ACCOUNT || "",
        username: process.env.SNOWFLAKE_USERNAME || "",
        password: process.env.SNOWFLAKE_PASSWORD || "",
        database: process.env.SNOWFLAKE_DATABASE,
        warehouse: process.env.SNOWFLAKE_WAREHOUSE,
        role: process.env.SNOWFLAKE_ROLE
    },
    snowflakeSchema: process.env.SNOWFLAKE_SCHEMA,
    snowflakeStage: process.env.SNOWFLAKE_STAGE,
    blobStorage: {
        blobStorageConnectionString: process.env.BLOB_STORAGE_CONNECTION_STRING || "",
        storageAccountAccessKey: process.env.AZURE_STORAGE_ACCESS_KEY || "",
        storageAccountName: process.env.AZURE_STORAGE_ACCOUNT || "",
        containerConfig: {
            containerUrl: process.env.BLOB_CONTAINER_URL || "",
            containerToken: process.env.BLOB_CONTAINER_TOKEN || "",
        }
    }
}

/**
 * Add schema validation to the configs
 */
export default appConfig;