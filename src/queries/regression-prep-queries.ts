export const CreateVehiclesTable = 'CREATE OR REPLACE TABLE VEHICLES LIKE {prodEnvironment}.{prodSchema}.VEHICLES;'
export const LoadVehiclesData = `INSERT INTO VEHICLES (
    SELECT * FROM {prodEnvironment}.{prodSchema}.VEHICLES where DATASOURCE='{datasource}' and ENROLLMENTSTATUS='ENROLLED' and ID in (
        SELECT DISTINCT VEHICLE_ID from TELEMETRY_INTERNAL
    )
);`

export const CreateProdTelemetryTable = `CREATE OR REPLACE TABLE {prodTelemetryTable} LIKE {prodEnvironment}.{prodSchema}.TELEMETRY_INTERNAL;`
export const LoadProdTelemetryData = `INSERT INTO {prodTelemetryTable} (
    SELECT * FROM {prodEnvironment}.{prodSchema}.TELEMETRY_INTERNAL WHERE VEHICLE_ID IN (
        SELECT ID FROM VEHICLES WHERE DATASOURCE='{datasource}'
    ) AND TIMESTAMP >= '{startTimestamp}' AND TIMESTAMP <= '{endTimestamp}'
);`

export const CreateProdEventTable = `CREATE OR REPLACE TABLE {prodEventTable} LIKE {prodEnvironment}.{prodSchema}.EVENTS_INTERNAL;`
export const LoadProdEventTable = `INSERT INTO {prodEventTable} (
    SELECT * FROM {prodEnvironment}.{prodSchema}.EVENTS_INTERNAL WHERE VEHICLE_ID IN (
        SELECT ID FROM VEHICLES WHERE DATASOURCE='{datasource}'
    ) AND TIMESTAMP >= '{startTimestamp}' AND TIMESTAMP <= '{endTimestamp}'
);`

export const CreateProdTripTable = `CREATE OR REPLACE TABLE {prodTripsTable} LIKE {prodEnvironment}.{prodSchema}.TRIPS_INTERNAL;`
export const LoadProdTripTable = `INSERT INTO {prodTripsTable} (
    SELECT * FROM {prodEnvironment}.{prodSchema}.TRIPS_INTERNAL WHERE VEHICLE_ID IN (
        SELECT ID FROM VEHICLES WHERE DATASOURCE='{datasource}'
    ) AND START_TIMESTAMP >= '{startTimestamp}' AND END_TIMESTAMP <= '{endTimestamp}'
)`