export default {
    ConfigNotFound : {
        message : "ConfigNotFound",
        description : "Missing config : {config}"
    },
    SnowflakeConnectionFailed : {
        message : "SnowflakeConnectionFailed",
        description : "Unable to connect to the snowflakes instance due to the below error : \n{err}"
    },
    SnowflakeQueryFailed : {
        message : "SnowflakeQueryFailed",
        description : "Failed to execute statement : || {query} ||' due to the following error: \n{err}"
    },
    SchemaAlreadyExists : {
        message : "SchemaAlreadyExists",
        description : "Schema {schemaName} already exists!"
    },
    StageAlreadyExists : {
        message : "StageAlreadyExists",
        description : "Stage {stageName} already exists!"
    }
}