export const TelemetryVolumeCheck = `SELECT count(*) AS TelemetryVolume FROM {prodEnvironment}.{prodSchema}.TELEMETRY_INTERNAL 
    WHERE timestamp > '{startTimestamp}' and timestamp < '{endTimestamp}'`
export const TelemetryPerVinVolumeCheck = `SELECT VEHICLE_ID, count(*) AS TelementryPerVID FROM {prodEnvironment}.{prodSchema}.TELEMETRY_INTERNAL 
    WHERE timestamp > '{startTimestamp}' and timestamp < '{endTimestamp}' 
    GROUP BY VEHICLE_ID
    ORDER BY TelementryPerVID DESC;`

export const EventVolumeCheck = `SELECT count(*) AS EventsVolume FROM {prodEnvironment}.{prodSchema}.EVENTS_INTERNAL 
    WHERE timestamp > '{startTimestamp}' and timestamp < '{endTimestamp}';`
export const EventsPerTypeVolumeCheck = `SELECT NAME, count(*) as EventsPerType FROM {prodEnvironment}.{prodSchema}.EVENTS_INTERNAL 
    WHERE timestamp > '{startTimestamp}' and timestamp < '{endTimestamp}'
    GROUP BY NAME
    ORDER BY EventsPerType DESC;`
export const EventsPerVIDVolumeCheck = `SELECT VEHICLE_ID, NAME, count(*) as EventsPerVID FROM {prodEnvironment}.{prodSchema}.EVENTS_INTERNAL 
    WHERE timestamp > '{startTimestamp}' and timestamp < '{endTimestamp}'
    GROUP BY VEHICLE_ID, NAME
    ORDER BY EventsPerVID DESC;`

export const TripsVolumeCheck = `SELECT count(*) AS TripsVolume FROM {prodEnvironment}.{prodSchema}.TRIPS_INTERNAL 
    WHERE START_TIMESTAMP > '{startTimestamp}' and END_TIMESTAMP < '{endTimestamp}'`
export const TripsPerVIDVolumeCheck = `SELECT VEHICLE_ID, count(*) as TripsPerVID FROM {prodEnvironment}.{prodSchema}.TRIPS_INTERNAL 
    WHERE START_TIMESTAMP > '{startTimestamp}' and END_TIMESTAMP < '{endTimestamp}'
    GROUP BY VEHICLE_ID
    ORDER BY TripsPerVID DESC;`