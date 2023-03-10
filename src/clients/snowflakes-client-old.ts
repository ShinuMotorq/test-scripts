import * as snowflake from "snowflake-sdk";
import { config } from 'dotenv';
import { SnowflakeConnectionConfig } from '../objects/config-schemas/snowflakes-config'
import appConfig from "../common/configs";

/**
 * @deprecated
 * Singletonized Snowflake connection
 */
class SnowflakeDBClient {
    constructor() { }

    private static connection: snowflake.Connection | null = null;

    public static async getConnection(): Promise<snowflake.Connection> {
        if (SnowflakeDBClient.connection == null) {
            let sfClient = new SnowflakeDBClient()
            SnowflakeDBClient.connection = await sfClient.createNewConnection();
        }
        return SnowflakeDBClient.connection;
    }

    private async createNewConnection(): Promise<snowflake.Connection> {
        let connection = await snowflake.createConnection(appConfig.snowflakeConnectionConfig)
        return new Promise((resolve, reject) => {
            connection.connect((err, con) => {
                if (err) {
                    console.log(`Unable to connect to the snowflakes instance due to the below error : \n${err}`)
                    reject(err)
                } else {
                    console.log(`Connected to snowflakes successfull !! ID : ${con.getId()}`)
                    resolve(con)
                }
            })
        })
    }

    public async runStatement(connection: snowflake.Connection, sqlStatement: string, binds?: any[]) {
        return new Promise((resolve, reject) => {
            connection.execute({
                sqlText: sqlStatement,
                binds: binds,
                complete: function (err, stmt, rows) {
                    if (err) {
                        console.error(`Failed to execute statement : "${stmt.getSqlText()}" due to the following error: \n${err.message}`);
                        reject(err)
                    } else {
                        console.log(`Statment : "${stmt.getSqlText()}" executed successfully!`)
                        resolve(rows)
                    }
                }
            })
        })
    }
}

export default SnowflakeDBClient;