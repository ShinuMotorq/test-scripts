import _ from 'lodash';
import { SqlKeywords, SqlSelectOptions } from '../common/enums';

/**
 * @deprecated - Not required for current scope
 * Builds simple queries
 */
class QueryBuilder {

    private query: string = '';
    private table: Array<string> = []

    constructor() {

    }

    select(selectOpts: SqlSelectOptions, ...columnName: Array<string>) {
        this.query += `${SqlKeywords.SELECT} `
        if (selectOpts == SqlSelectOptions.ALL) {
            this.query += ``
        } else {
            this.query += columnName.join(', ')
        }
        return this;
    }

    from(tableName: string) {
        this.query = _.padEnd(this.query, 1, SqlKeywords.FROM)
        this.query = _.pad(this.query, 1, tableName)        
        return this;
    }

    build() {
        return this.query;
    }
}

export default QueryBuilder;