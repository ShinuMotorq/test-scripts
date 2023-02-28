import * as sf from 'snowflake-sdk';
import appConfig from '../common/configs';
import { connect } from 'http2';

/**
 * Singletonized Snowflake client
 */
class SnowflakeClient {

    private static instance: SnowflakeClient | null = null;
    private connection: sf.Connection | null = null;

    private constructor() { 
        this.connection = sf.createConnection(appConfig.snowflakeConnectionConfig)        
    }

    public static async getInstance(): Promise<SnowflakeClient> {
        if (!SnowflakeClient.instance) {
            SnowflakeClient.instance = new SnowflakeClient();
        }
        await SnowflakeClient.instance.connect();
        return SnowflakeClient.instance;
    }

    public async connect(): Promise<sf.Connection> {        
        return new Promise((resolve, reject) => {            
            this.connection!.connect((err, con) => {
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

    public async runStatement(sqlStatement: string, binds?: any[]) {
        return new Promise((resolve, reject) => {            
            this.connection!.execute({
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

    public static async terminateOpenConnection() {        
        return new Promise((resolve, reject) => {
            SnowflakeClient.instance!.connection?.destroy((err, con) => {
                if (err) {
                    console.log(`Unable to destroy to the snowflakes instance due to the below error : \n${err}`)
                    reject(err)
                } else {
                    console.log(`Successfully terminated snowflake connection ID : ${con.getId()}`)
                    resolve(con)
                }
                SnowflakeClient.instance = null;
            })
        })
    }
}

export default SnowflakeClient;