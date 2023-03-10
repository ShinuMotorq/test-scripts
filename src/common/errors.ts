export default {
    ArgNotFound: {
        message: "ArgNotFound",
        description: "Missing arg : {arg}"
    },
    ConfigNotFound: {
        message: "ConfigNotFound",
        description: "Missing config : {config}"
    },
    SnowflakeConnectionFailed: {
        message: "SnowflakeConnectionFailed",
        description: "Unable to connect to the snowflakes instance due to the below error : \n{errorMessage}"
    },
    SnowflakeQueryFailed: {
        message: "SnowflakeQueryFailed",
        description: "Failed to execute statement : || {query} ||' due to the following error: \n{errorMessage}"
    },
    SchemaAlreadyExists: {
        message: "SchemaAlreadyExists",
        description: "Schema {schemaName} already exists!"
    },
    StageAlreadyExists: {
        message: "StageAlreadyExists",
        description: "Stage {stageName} already exists!"
    },
    ReportGenerationFailed: {
        message: "ReportGenerationFailed",
        description: "Report generation failed due to below error:\n{errorMessage}"
    }
}