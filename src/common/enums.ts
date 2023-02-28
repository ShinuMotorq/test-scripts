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
    SNOWFLAKE_SYNC = "snowflake_sync",
    PRIMARY_VALIDATIONS = "primary_validations"
}