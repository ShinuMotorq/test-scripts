import { BlobStorageConfig } from "../objects/blob-storage-config"
import { SnowflakeConnectionConfig } from "../objects/snowflakes-config"

export type AppConfig = {
    snowflakeConnectionConfig: SnowflakeConnectionConfig,
    blobStorage?: BlobStorageConfig
    // snowflakeSchema? : string,
    // snowflakeStage? : string,
}