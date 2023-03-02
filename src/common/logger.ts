import { LogLevel } from "./enums";

/**
 * Console level logger. Class should be able to get the scope and params too
 */
class Logger {
    private static loggerInstance: Logger;

    private constructor() { }

    public static getInstance() {
        if (!Logger.loggerInstance) {
            Logger.loggerInstance = new Logger()
        }
        return Logger.loggerInstance;
    }

    private log(logLevel: LogLevel, message: string) {
        switch (logLevel) {
            case LogLevel.Error: console.log("\x1b[31m", `[${LogLevel[logLevel]}] ${message}`);
                break;
            case LogLevel.Debug: console.log("\x1b[33m", `[${LogLevel[logLevel]}] ${message}`);
                break;
            case LogLevel.Info: console.log("\x1b[34m", `[${LogLevel[logLevel]}] ${message}`);
                break;
            default: console.log("\x1b[31m", `[${LogLevel[logLevel]}] ${message}`);
        }
    }

    public debug(scope: string, message: string) {
        this.log(LogLevel.Debug, `${scope} - ${message}`);
    }

    public info(scope: string, message: string) {
        this.log(LogLevel.Info, `${scope} - ${message}`);
    }

    public warn(scope: string, message: string) {
        this.log(LogLevel.Warn, `${scope} - ${message}`);
    }

    public error(scope: string, message: string) {
        this.log(LogLevel.Error, `${scope} - ${message}`);
    }
}

export default Logger.getInstance()