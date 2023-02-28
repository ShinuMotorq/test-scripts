import { BlobStorageConfig } from "../objects/blob-storage-config"
import { SnowflakeConnectionConfig } from "../objects/snowflakes-config"

export type AppConfig = {
    snowflakeConnectionConfig : SnowflakeConnectionConfig,
    snowflakeSchema? : string,
    snowflakeStage? : string,
    blobStorage? : BlobStorageConfig
}