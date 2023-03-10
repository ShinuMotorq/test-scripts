export const CreateStage: string =
    `CREATE OR REPLACE STAGE {stageName}
    url = '{containerUrl}'
    credentials = (azure_sas_token = '{containerToken}');`
export const VerifyStage: string = 'list @{stageName};'

export const CreateFileFormat: string = `CREATE OR REPLACE FILE FORMAT {avroFileFormatName} TYPE = avro;`
export const AlterStageFileFormat: string = `ALTER STAGE {stageName} SET file_format = {avroFileFormatName}`
export const CreateReplayTables: Array<string> = [
    'CREATE OR REPLACE TABLE INIT_LOAD_V3 LIKE {prodDB}.{prodSchema}.INIT_LOAD_V3',
    'CREATE OR REPLACE TABLE TELEMETRY_INTERNAL LIKE {prodDB}.{prodSchema}.TELEMETRY_INTERNAL',
    'CREATE OR REPLACE TABLE EVENTS_INTERNAL LIKE {prodDB}.{prodSchema}.EVENTS_INTERNAL',
    'CREATE OR REPLACE TABLE TRIPS_INTERNAL LIKE {prodDB}.{prodSchema}.TRIPS_INTERNAL'
]
export const CreateFunctions: Array<string> = [
    `CREATE OR REPLACE FUNCTION getPositionV3(position string)
        RETURNS string
        LANGUAGE JAVASCRIPT
    AS
    $$
        if(POSITION == 'lf' || POSITION == 'fl')	return "frontLeft";
        if(POSITION == 'rf' || POSITION == 'fr')	return "frontRight";
        if(POSITION == 'lr' || POSITION == 'rl')	return "rearLeft";
        if(POSITION == 'rr')	return "rearRight";
    $$;`,
    `CREATE OR REPLACE FUNCTION getEventNameV3(EVENTSUBTYPE string)
        RETURNS string
        LANGUAGE JAVASCRIPT
    AS
    $$
        switch(EVENTSUBTYPE) {
            case 'oilLifeLow':
                return 'engineOilLifeLow';
            case 'oilLifeCritical':
                return 'engineOilLifeCritical';
            case 'oilLifeOk':
                return 'engineOilLifeOk';
            case 'speedingAbsolute':
                return 'absoluteSpeeding';
            case 'dtcTriggered':
                return 'dtcSet';
            default:
                return EVENTSUBTYPE;
        }
    $$;`,
    `CREATE OR REPLACE FUNCTION getEventTypeV3(EVENTTYPE string)
        RETURNS string
        LANGUAGE JAVASCRIPT
    AS
    $$
        var eventTypeV3 = EVENTTYPE.replace(/[Ee]vent$/, '');
        if(EVENTTYPE == 'dtcEvent') {
            eventTypeV3 = 'health';
        }
        else if(EVENTTYPE == 'anomalyEvent') {
            eventTypeV3 = 'anomalyEvent';
        }
        return eventTypeV3;
    $$;`,
    `CREATE OR REPLACE FUNCTION getEventUnitsV3(UNITS string)
        RETURNS string
        LANGUAGE JAVASCRIPT
    AS
    $$
        if(UNITS == '%') {
            return 'pct';
        }
        if(UNITS == 'volts') {
            return 'V';
        }
        return UNITS;
    $$;`
];