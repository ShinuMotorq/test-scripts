import * as sf from 'snowflake-sdk';
import appConfig from '../common/configs';
import logger from "../common/logger";
import Errors from '../common/errors';
import BaseErrorHandler from './error-handler';

/**
 * Singletonized Snowflake client
 */
class SnowflakeClient {

    private static instance: SnowflakeClient | null = null;
    private connection: sf.Connection | null = null;
    private scope: string = 'SnowflakeClient'

    private constructor() {
        this.connection = sf.createConnection(appConfig.snowflakeConnectionConfig)
    }

    public static async getInstance(): Promise<SnowflakeClient> {
        if (!SnowflakeClient.instance) {
            try {
                SnowflakeClient.instance = new SnowflakeClient();
                await SnowflakeClient.instance.connect();
            } catch (err: any) {
                throw new BaseErrorHandler(Errors.SnowflakeConnectionFailed, { error: err.message })
            }
        }
        return SnowflakeClient.instance;
    }

    public async connect(): Promise<sf.Connection> {
        const me = this;
        return new Promise((resolve, reject) => {
            this.connection!.connect((err, con) => {
                if (err) {
                    logger.error(me.scope, `Unable to connect to the snowflakes instance due to the below error : \n${err}`)
                    reject(err)
                } else {
                    logger.info(me.scope, `Connected to snowflakes successfull !! ID : ${con.getId()}`)
                    resolve(con)
                }
            })
        })
    }

    public async runStatement(sqlStatement: string, binds?: any[]) {
        const me = this;
        logger.info(me.scope, `Executing query : "${sqlStatement}"`)        
        return new Promise((resolve, reject) => {
            this.connection!.execute({
                sqlText: sqlStatement,
                binds: binds,
                complete: function (err, stmt, rows) {
                    if (err) {
                        logger.error(me.scope, `Failed to execute statement : "${stmt.getSqlText()}" due to the following error: \n${err.message}`);
                        reject(err)
                    } else {
                        logger.info(me.scope, `Query executed successfully!`)
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
                    logger.error('SnowflakeClient', `Unable to destroy to the snowflakes instance due to the below error : \n${err}`)
                    reject(err)
                } else {
                    logger.info('SnowflakeClient', `Successfully terminated snowflake connection ID : ${con.getId()}`)
                    resolve(con)
                }
                SnowflakeClient.instance = null;
            })
        })
    }
}

export default SnowflakeClient;