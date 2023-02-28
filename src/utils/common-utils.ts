import yargs from 'yargs'
import { Args } from '../objects/args';

class CommonUtils {

    constructor() { }

    async getParams() {
        let args: Args = <Args><unknown>await yargs.parse()
        return args;
    }
}

export default CommonUtils;