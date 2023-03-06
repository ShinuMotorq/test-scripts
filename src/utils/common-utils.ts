import yargs from 'yargs'
import { Args } from '../objects/args';
import Errors from '../common/errors';
import BaseErrorHandler from '../clients/error-handler';

class CommonUtils {

    constructor() { }

    async getParams() {
        let cmdArgs: any = await yargs.parse()
        let args: Args = <Args>{}
        if (!cmdArgs['type']) throw new BaseErrorHandler(Errors.ArgNotFound, { arg: "type" })
        args.type = cmdArgs['type'].split(',')
        return args;
    }
}

export default CommonUtils;