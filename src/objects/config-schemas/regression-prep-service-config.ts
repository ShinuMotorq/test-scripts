export type RegressionPrepServiceConfig = {
    vehicles: Vehicles;
    prodTables: ProdTable;
    replayContext: ReplayContext;
}

type Vehicles = {
    datasource: string,
    entities: Array<Entity>
}

type Entity = {
    prodEnvironment: string,
    prodSchema: string
}

type ProdTable = {
    minTimestamp: string,
    maxTimestamp: string,
    prodEnvironment: string,
    prodSchema: string,
    prodTelemetryTableName: string,
    prodEventsTableName: string,
    prodTripsTableName: string,
}

type ReplayContext = {
    dbBame: string,
    schemaName: string,
    stageName: string
}