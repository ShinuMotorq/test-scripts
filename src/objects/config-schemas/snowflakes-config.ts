export type SnowflakeConnectionConfig = {
    account: string;
    username: string;
    password: string;
    region?: string;
    database?: string | undefined;
    schema?: string | undefined;
    warehouse?: string | undefined;
    role?: string | undefined;
    timeout?: number | undefined;
    application?: string;
    /**
     * Refer @ConnectionOptions
     */
}