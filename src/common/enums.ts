export enum SqlKeywords {
    SELECT = 'SELECT',
    FROM = 'FROM',
    WHERE = 'WHERE',
    ORDER = 'ORDER',
    BY = 'BY',
    GROUP = 'GROUP',
    DESC = 'DESC',
    HAVING = 'HAVING',
    JOIN = 'JOIN',
    ON = 'ON',
    IN = 'IN',
    CREATE = 'CREATE'
}

export enum SqlSelectOptions {
    ALL = '*',
    COLUMNS = 'columns'
}

export enum RequestType {
    RESOURCE_CREATION = "resource_creation",
    SNOWFLAKE_SETUP = "snowflake_setup",
    SNOWFLAKE_SYNC = "snowflake_sync",
    POST_DEPLOYMENT_VALIDATION = "post_deployment_validation",
    REGRESSION_PREP = "regression_prep"
}

export enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}