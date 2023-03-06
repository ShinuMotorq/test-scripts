import { CustomError } from '../objects/custom-error';
import format from 'string-template'

/**
 * Base error handler for all Test script errors
 */
class BaseErrorHandler extends Error {

    private error: CustomError;
    private params: {}

    constructor(error: CustomError, params: {}) {
        super()
        this.error = error;
        this.params = params;
        this.throwError()
    }

    private throwError() {
        let errorMessage = this.error.description;
        let templateParam = {}
        for (let param in this.params) {
            templateParam[param] = this.params[param]
        }
        errorMessage = format(errorMessage, templateParam)
        throw Error(errorMessage);
    }


}

export default BaseErrorHandler;
