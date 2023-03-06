export const UseSchema: string = 'USE SCHEMA {schemaName};'
export const LoadFeedData = `copy into INIT_LOAD_V3 from (select parse_json(HEX_DECODE_STRING($1:Body)) from @{dbName}.{schemaName}.{stageName})`
export const CreateDedupTempFromFeed =
    `CREATE OR REPLACE TABLE dedup_temp as
        SELECT * FROM 
            (SELECT *, ROW_NUMBER() OVER (PARTITION BY T.body:dId, T.body:id order by T.body:id) rowNum 
            FROM INIT_LOAD_V3 T) 
        X WHERE rowNum=1;`
export const RemoveRowNum = `ALTER TABLE dedup_temp DROP rowNum;`
export const CreateInitLoadDedupedTable = `CREATE OR REPLACE TABLE INIT_LOAD_DEDUPED LIKE INIT_LOAD_V3`
export const LoadInitLoadDedupedTable = `INSERT INTO INIT_LOAD_DEDUPED (SELECT * FROM dedup_temp);`
export const DropDedupTempTable = `DROP TABLE dedup_temp;`
export const CreateDedupTempTableTelemetry = `CREATE OR REPLACE TABLE DEDUP_TEMP LIKE TELEMETRY_INTERNAL;`
export const InsertTelemetryToDedupTemp =
    `INSERT INTO DEDUP_TEMP (ID,DEVICE_ID,VEHICLE_ID,TIMESTAMP,LOCATION_LAT,LOCATION_LON,SPEED,SPEED_SIGNAL_TYPE,SPEED_UNITS,IGNITION_STATUS,ODOMETER,ODOMETER_SIGNAL_TYPE,ODOMETER_UNITS,ENGINE_RUN_TIME,ENGINE_RUN_TIME_UNITS,FUEL_LEVEL,FUEL_LEVEL_UNITS,BATTERY_VOLTAGE,BATTERY_VOLTAGE_UNITS,ENGINE_COOLANT_TEMP,ENGINE_COOLANT_TEMP_UNITS,CHECK_ENGINE_LIGHT,ENGINE_OIL_LIFE,ENGINE_OIL_LIFE_UNITS,DEVICE_CONNECTIVITY_STATUS,TIRE_PRESSURE_FRONT_LEFT,TIRE_PRESSURE_FRONT_RIGHT,TIRE_PRESSURE_REAR_LEFT,TIRE_PRESSURE_REAR_RIGHT,TIRE_PRESSURE_UNITS,ENGINE_IDLE_TIME,ENGINE_IDLE_TIME_UNITS,PROCESSED_TIMESTAMP,RAW,HEADING,SUNROOF_STATUS,WINDOW_STATUS_FRONT_LEFT,WINDOW_STATUS_FRONT_RIGHT,WINDOW_STATUS_REAR_LEFT,WINDOW_STATUS_REAR_RIGHT,EV_BATTERY_LEVEL,EV_BATTERY_RANGE,EV_CHARGING_STATE,EV_PLUG_STATUS,EV_TIME_TO_FULL_CHARGE,EV_CHARGING_VOLTAGE,EV_CHARGING_CURRENT,EV_CHARGE_TYPE,SNOWFLAKE_WRITE_TIMESTAMP,LOCAL_TIMESTAMP,TIME_ZONE_ID,EV_CHARGING_ENERGY_ADDED,ENGINE_RPM)
            (
                SELECT ID,DEVICE_ID,VEHICLE_ID,TIMESTAMP,LOCATION_LAT,LOCATION_LON,SPEED,SPEED_SIGNAL_TYPE,SPEED_UNITS,IGNITION_STATUS,ODOMETER,ODOMETER_SIGNAL_TYPE,ODOMETER_UNITS,ENGINE_RUN_TIME,ENGINE_RUN_TIME_UNITS,FUEL_LEVEL,FUEL_LEVEL_UNITS,BATTERY_VOLTAGE,BATTERY_VOLTAGE_UNITS,ENGINE_COOLANT_TEMP,ENGINE_COOLANT_TEMP_UNITS,CHECK_ENGINE_LIGHT,ENGINE_OIL_LIFE,ENGINE_OIL_LIFE_UNITS,DEVICE_CONNECTIVITY_STATUS,TIRE_PRESSURE_FRONT_LEFT,TIRE_PRESSURE_FRONT_RIGHT,TIRE_PRESSURE_REAR_LEFT,TIRE_PRESSURE_REAR_RIGHT,TIRE_PRESSURE_UNITS,ENGINE_IDLE_TIME,ENGINE_IDLE_TIME_UNITS,PROCESSED_TIMESTAMP,RAW,HEADING,SUNROOF_STATUS,WINDOW_STATUS_FRONT_LEFT,WINDOW_STATUS_FRONT_RIGHT,WINDOW_STATUS_REAR_LEFT,WINDOW_STATUS_REAR_RIGHT,EV_BATTERY_LEVEL,EV_BATTERY_RANGE,EV_CHARGING_STATE,EV_PLUG_STATUS,EV_TIME_TO_FULL_CHARGE,EV_CHARGING_VOLTAGE,EV_CHARGING_CURRENT,EV_CHARGE_TYPE,SNOWFLAKE_WRITE_TIMESTAMP,LOCAL_TIMESTAMP,TIME_ZONE_ID,EV_CHARGING_ENERGY_ADDED,ENGINE_RPM FROM (
    select 
            T.body:id::STRING as id,
            T.body:dId::STRING as device_id,
            T.body:vId::STRING as vehicle_id,
            T.body:ts_src::DATETIME as timestamp,
            T.body:ts_local::DATETIME as local_timestamp,
            T.body:tzId::STRING as time_zone_id,
            T.body:location:coordinates[1]::FLOAT as location_lat,
            T.body:location:coordinates[0]::FLOAT as location_lon,
         

            ifnull( iff(T.body:signalType::STRING = 'canBus', T.body:speed_can_mph::FLOAT, null ), 
                ifnull(iff(T.body:signalType::STRING = 'GPS', T.body:speed_gps_mph::FLOAT, null ) ,
                iff( T.body:speed_can_mph::FLOAT is not null,  T.body:speed_can_mph::FLOAT,  T.body:speed_gps_mph::FLOAT) )
            )as SPEED,
            iff(speed::float is null, 
                null, 
                IFF(T.body:signalType::STRING is not null, 
                T.body:signalType::STRING, iff(T.body:speed_can_mph::FLOAT is not null, 'canBus', 'GPS'))
            ) as SPEED_SIGNAL_TYPE,
            iff( SPEED::FLOAT is not null, 'mph'::STRING , null) as SPEED_UNITS, 

            T.body:is::STRING as ignition_status,

            ifnull( iff(T.body:signalType::STRING = 'canBus', T.body:odm_can_mi::FLOAT, null ), 
                ifnull(iff(T.body:signalType::STRING = 'GPS',T.body:odomtr_gps_mi::FLOAT, null ) ,
                iff( T.body:odm_can_mi::FLOAT is not null,  T.body:odm_can_mi::FLOAT,  T.body:odomtr_gps_mi::FLOAT) )
            ) as ODOMETER,
            iff(ODOMETER::float is null, 
                null, 
                iff(T.body:signalType::STRING is not null, 
                T.body:signalType::STRING, iff( T.body:odm_can_mi::FLOAT is not null, 'canBus', 'GPS'))
            ) as ODOMETER_SIGNAL_TYPE,
            iff( ODOMETER::FLOAT is not null, 'mi'::STRING , null) as ODOMETER_UNITS, 

            T.body:engineRunTimeCanHrs::FLOAT as ENGINE_RUN_TIME,
            iff( ENGINE_RUN_TIME::FLOAT is not null, 'hrs'::STRING, null) as ENGINE_RUN_TIME_UNITS, 
            T.body:fuel_pct::FLOAT as fuel_level,
            iff( fuel_level::FLOAT is not null, 'pct'::STRING , null) as fuel_level_units, 
            T.body:vehicle_b_volt::FLOAT as battery_voltage,
            iff( battery_voltage::FLOAT is not null, 'V'::STRING, null) as BATTERY_VOLTAGE_UNITS, 
            T.body:ectemp_c::FLOAT as ENGINE_COOLANT_TEMP,
            iff( ENGINE_COOLANT_TEMP::float is not null, 'F'::STRING, null) as ENGINE_COOLANT_TEMP_UNITS, 
            iff(T.body:mil_s::STRING = 'off', 'off', iff(T.body:mil_s::STRING is not null, 'on', null)) as CHECK_ENGINE_LIGHT,
            T.body:oil_life_pct::FLOAT as ENGINE_OIL_LIFE,
            iff( ENGINE_OIL_LIFE::FLOAT is not null, 'pct'::STRING, null) as ENGINE_OIL_LIFE_UNITS, 

            T.body:ps::STRING as device_connectivity_status, 

            IFNULL(T.body:tire_pressure:flPsi::FLOAT, T.body:tire_pressure:lfPsi::FLOAT) as TIRE_PRESSURE_FRONT_LEFT,
            IFNULL(T.body:tire_pressure:frPsi::FLOAT, T.body:tire_pressure:rfPsi::FLOAT) as TIRE_PRESSURE_FRONT_RIGHT,
            IFNULL(T.body:tire_pressure:rlPsi::FLOAT, T.body:tire_pressure:lrPsi::FLOAT) as TIRE_PRESSURE_REAR_LEFT,
            T.body:tire_pressure:rrPsi::FLOAT as TIRE_PRESSURE_REAR_RIGHT,
            iff(TIRE_PRESSURE_FRONT_LEFT::FLOAT is not null or TIRE_PRESSURE_FRONT_RIGHT::FLOAT is not null or
                TIRE_PRESSURE_REAR_LEFT::FLOAT is not null or TIRE_PRESSURE_REAR_RIGHT::FLOAT is not null,  'psi'::STRING, null) as TIRE_PRESSURE_UNITS,
            T.body::VARIANT as raw,
            T.body:heading::NUMBER as HEADING,
            T.body:sunRoofStatus::STRING as SUNROOF_STATUS,
            T.body:windowStatus:frontRight as WINDOW_STATUS_FRONT_RIGHT,
            T.body:acceleration::FLOAT as ACCELERATION,
            T.body:accelerationLat::FLOAT as ACCELERATION_LAT,
            T.body:ambientTemp::FLOAT as AMBIENT_TEMP,
            T.body:gearPosition::STRING as GEAR_POSITION,
            T.body:seatbeltStatus::STRING as SEATBELT_STATUS,
            T.body:engineSpeed::NUMBER as ENGINE_SPEED,
            T.body:windowStatus:frontLeft as WINDOW_STATUS_FRONT_LEFT,
            T.body:windowStatus:rearLeft as WINDOW_STATUS_REAR_LEFT,
            T.body:windowStatus:rearRight as WINDOW_STATUS_REAR_RIGHT,
            T.body:evBatteryLevel::STRING as EV_BATTERY_LEVEL,
            T.body:evBatteryRange as  EV_BATTERY_RANGE,
            T.body:evChargingState as EV_CHARGING_STATE,
            T.body:evChargingEnergyAdded as  EV_CHARGING_ENERGY_ADDED,
            T.body:engineIdleTimeCanHrs as ENGINE_IDLE_TIME,
            iff( ENGINE_IDLE_TIME::FLOAT is not null, 'hrs'::STRING, null) as ENGINE_IDLE_TIME_UNITS, 
            T.body:evPlugStatus::STRING as  EV_PLUG_STATUS,
            T.body:evTimeToFullCharge::INTEGER as EV_TIME_TO_FULL_CHARGE,
            T.body:evChargingVoltage::FLOAT as  EV_CHARGING_VOLTAGE,
            T.body:evChargingCurrent::FLOAT as  EV_CHARGING_CURRENT,
            T.body:evChargeType::STRING as  EV_CHARGE_TYPE,
            T.body:engineRpm::INTEGER as  ENGINE_RPM,
            TO_TIMESTAMP(T.body:_ts::INTEGER)::DATETIME as PROCESSED_TIMESTAMP,
            SYSDATE() as SNOWFLAKE_WRITE_TIMESTAMP
        from INIT_LOAD_DEDUPED T where T.body:type='COMBINEDFEED' and  not (ARRAY_SIZE(T.body:types) = 1 AND ARRAY_CONTAINS('HEALTHEXTENDED'::VARIANT, T.body:types) 
        and T.body:crankingVoltage is not null )
    )
            )`
export const MergeTelemetryInternal = 
    `MERGE INTO TELEMETRY_INTERNAL A USING DEDUP_TEMP B ON A.ID = B.ID AND A.DEVICE_ID = B.DEVICE_ID
    WHEN NOT MATCHED THEN INSERT
        (ID,DEVICE_ID,VEHICLE_ID,TIMESTAMP,LOCATION_LAT,LOCATION_LON,SPEED,SPEED_SIGNAL_TYPE,SPEED_UNITS,IGNITION_STATUS,ODOMETER,ODOMETER_SIGNAL_TYPE,ODOMETER_UNITS,ENGINE_RUN_TIME,ENGINE_RUN_TIME_UNITS,FUEL_LEVEL,FUEL_LEVEL_UNITS,BATTERY_VOLTAGE,BATTERY_VOLTAGE_UNITS,ENGINE_COOLANT_TEMP,ENGINE_COOLANT_TEMP_UNITS,CHECK_ENGINE_LIGHT,ENGINE_OIL_LIFE,ENGINE_OIL_LIFE_UNITS,DEVICE_CONNECTIVITY_STATUS,TIRE_PRESSURE_FRONT_LEFT,TIRE_PRESSURE_FRONT_RIGHT,TIRE_PRESSURE_REAR_LEFT,TIRE_PRESSURE_REAR_RIGHT,TIRE_PRESSURE_UNITS,ENGINE_IDLE_TIME,ENGINE_IDLE_TIME_UNITS,PROCESSED_TIMESTAMP,RAW,HEADING,SUNROOF_STATUS,WINDOW_STATUS_FRONT_LEFT,WINDOW_STATUS_FRONT_RIGHT,WINDOW_STATUS_REAR_LEFT,WINDOW_STATUS_REAR_RIGHT,EV_BATTERY_LEVEL,EV_BATTERY_RANGE,EV_CHARGING_STATE,EV_PLUG_STATUS,EV_TIME_TO_FULL_CHARGE,EV_CHARGING_VOLTAGE,EV_CHARGING_CURRENT,EV_CHARGE_TYPE,SNOWFLAKE_WRITE_TIMESTAMP,LOCAL_TIMESTAMP,TIME_ZONE_ID,EV_CHARGING_ENERGY_ADDED,ENGINE_RPM)
        VALUES
        (ID,DEVICE_ID,VEHICLE_ID,TIMESTAMP,LOCATION_LAT,LOCATION_LON,SPEED,SPEED_SIGNAL_TYPE,SPEED_UNITS,IGNITION_STATUS,ODOMETER,ODOMETER_SIGNAL_TYPE,ODOMETER_UNITS,ENGINE_RUN_TIME,ENGINE_RUN_TIME_UNITS,FUEL_LEVEL,FUEL_LEVEL_UNITS,BATTERY_VOLTAGE,BATTERY_VOLTAGE_UNITS,ENGINE_COOLANT_TEMP,ENGINE_COOLANT_TEMP_UNITS,CHECK_ENGINE_LIGHT,ENGINE_OIL_LIFE,ENGINE_OIL_LIFE_UNITS,DEVICE_CONNECTIVITY_STATUS,TIRE_PRESSURE_FRONT_LEFT,TIRE_PRESSURE_FRONT_RIGHT,TIRE_PRESSURE_REAR_LEFT,TIRE_PRESSURE_REAR_RIGHT,TIRE_PRESSURE_UNITS,ENGINE_IDLE_TIME,ENGINE_IDLE_TIME_UNITS,PROCESSED_TIMESTAMP,RAW,HEADING,SUNROOF_STATUS,WINDOW_STATUS_FRONT_LEFT,WINDOW_STATUS_FRONT_RIGHT,WINDOW_STATUS_REAR_LEFT,WINDOW_STATUS_REAR_RIGHT,EV_BATTERY_LEVEL,EV_BATTERY_RANGE,EV_CHARGING_STATE,EV_PLUG_STATUS,EV_TIME_TO_FULL_CHARGE,EV_CHARGING_VOLTAGE,EV_CHARGING_CURRENT,EV_CHARGE_TYPE,SNOWFLAKE_WRITE_TIMESTAMP,LOCAL_TIMESTAMP,TIME_ZONE_ID,EV_CHARGING_ENERGY_ADDED,ENGINE_RPM)`
export const CreateDedupTempForEvents = `CREATE OR REPLACE TABLE DEDUP_TEMP LIKE EVENTS_INTERNAL;`
export const InsertEventToDedupTemp =
    `INSERT INTO DEDUP_TEMP (ID,VEHICLE_ID,DEVICE_ID,TIMESTAMP,LOCAL_TIMESTAMP,TIME_ZONE_ID,TYPE,NAME,LOCATION_LAT,LOCATION_LON,VALUE,THRESHOLD,UNITS,DURATION,DURATION_UNITS,POSITION,CODE,DESCRIPTION,ROAD_SPEED_LIMIT,GEOFENCE_ID,GEOFENCE_NAME,PROCESSED_TIMESTAMP,RAW,VALUE_STRING,THRESHOLD_STRING,DISTANCE,DISTANCE_UNITS,STARTING_VALUE,MAX_VOLTAGE,SPEED,AIRBAG_STATUS,GEOFENCE_VERSION,GEOFENCES,SNOWFLAKE_WRITE_TIMESTAMP,SW_CODE,SW_NAME,SW_SYSTEM,SW_SOURCE_DESC,CHARGING_SESSION_ENERGY_ADDED,RPM_THRESHOLD,PTO_DURATION_THRESHOLD,PTO_DURATION,RPM_AVG,PTO_UNITS)
    (
        SELECT ID,VEHICLE_ID,DEVICE_ID,TIMESTAMP,LOCAL_TIMESTAMP,TIME_ZONE_ID,TYPE,NAME,LOCATION_LAT,LOCATION_LON,VALUE,THRESHOLD,UNITS,DURATION,DURATION_UNITS,POSITION,CODE,DESCRIPTION,ROAD_SPEED_LIMIT,GEOFENCE_ID,GEOFENCE_NAME,PROCESSED_TIMESTAMP,RAW,VALUE_STRING,THRESHOLD_STRING,DISTANCE,DISTANCE_UNITS,STARTING_VALUE,MAX_VOLTAGE,SPEED,AIRBAG_STATUS,GEOFENCE_VERSION,GEOFENCES,SNOWFLAKE_WRITE_TIMESTAMP,SW_CODE,SW_NAME,SW_SYSTEM,SW_SOURCE_DESC,CHARGING_SESSION_ENERGY_ADDED,RPM_THRESHOLD,PTO_DURATION_THRESHOLD,PTO_DURATION,RPM_AVG,PTO_UNITS FROM ( 
            SELECT
            T.body:id::STRING as id,
            T.body:vId::STRING as vehicle_id,
            T.body:dId::STRING as device_id,
            T.body:ts_src::DATETIME as timestamp,
            T.body:ts_local_src::DATETIME as local_timestamp,
            T.body:tzId::STRING as time_zone_id,
            getEventTypeV3(T.body:e_type::STRING) as type,
            getEventNameV3(T.body:e_stype::STRING) as name,
            T.body:location:coordinates[1]::FLOAT as location_lat,
            T.body:location:coordinates[0]::FLOAT as location_lon,
            TRY_TO_DOUBLE(T.body:value::STRING)::FLOAT as value,
            iff(T.body:e_stype::STRING != 'powerTakeOff', TRY_TO_DOUBLE(T.body:threshold::STRING)::FLOAT, null) as threshold,
            getEventUnitsV3(T.body:units::STRING) as units,
            iff(T.body:e_stype::STRING = 'evCharging', T.body:duration:value::FLOAT, T.body:durationSec::FLOAT) as duration,
            iff( duration::STRING is not null, 'sec'::STRING, null) as duration_units, 
            getPositionV3(T.body:position::STRING) as position,      
            T.body:code::STRING as code,
            T.body:desc::STRING as description,
            T.body:roadSpeedLimit::FLOAT as road_speed_limit,
            T.body:geofenceId::STRING as geofence_id,
            T.body:geofenceName::STRING as geofence_name,
            T.body:geofenceVersion::NUMBER as geofence_version,
            T.body:geofences::VARIANT as geofences,
            T.body::VARIANT as raw,
            iff(T.body:e_stype::STRING = 'exceptionFuelType', T.body:value::STRING, null) as VALUE_STRING,
            iff(T.body:e_stype::STRING = 'exceptionFuelType', T.body:threshold::STRING, null) as THRESHOLD_STRING,
            iff(T.body:e_stype::STRING = 'serviceWarningSet' OR T.body:e_stype::STRING = 'serviceWarningCleared', T.body:code::STRING, null) as SW_CODE,
            iff(T.body:e_stype::STRING = 'serviceWarningSet' OR T.body:e_stype::STRING = 'serviceWarningCleared', T.body:name::STRING, null) as SW_NAME,
            iff(T.body:e_stype::STRING = 'serviceWarningSet' OR T.body:e_stype::STRING = 'serviceWarningCleared', T.body:sourceDescription::STRING, null) as SW_SOURCE_DESC,
            iff(T.body:e_stype::STRING = 'serviceWarningSet' OR T.body:e_stype::STRING = 'serviceWarningCleared', T.body:system::STRING, null) as SW_SYSTEM,
            T.body:distance:value::FLOAT as distance,
            T.body:distance:units::STRING as distance_units,
            T.body:startingValue::FLOAT as starting_value,
            T.body:evChargingSessionEnergyAdded::FLOAT as charging_session_energy_added,
            T.body:maxVoltage::FLOAT as max_voltage,
            T.body:speed:value::FLOAT as speed,
            T.body:airbagStatus::STRING as airbag_status,
            iff(T.body:e_stype::STRING = 'powerTakeOff' or T.body:e_stype::STRING = 'longIdling', T.body:rpmThreshold::FLOAT, null) as RPM_THRESHOLD,
            iff(T.body:e_stype::STRING = 'powerTakeOff' or T.body:e_stype::STRING = 'longIdling', T.body:ptoDurationThreshold::FLOAT, null) as PTO_DURATION_THRESHOLD,
            iff(T.body:e_stype::STRING = 'powerTakeOff' or T.body:e_stype::STRING = 'longIdling', T.body:ptoDuration::FLOAT, null) as PTO_DURATION,
            iff(T.body:e_stype::STRING = 'powerTakeOff' or T.body:e_stype::STRING = 'longIdling', T.body:rpmAvg::FLOAT, null) as RPM_AVG,
            iff(T.body:e_stype::STRING = 'powerTakeOff' or T.body:e_stype::STRING = 'longIdling', T.body:ptoUnits::STRING, null) as PTO_UNITS,
            TO_TIMESTAMP(T.body:_ts::INTEGER)::DATETIME as PROCESSED_TIMESTAMP,
            SYSDATE() as SNOWFLAKE_WRITE_TIMESTAMP
            from INIT_LOAD_DEDUPED T where T.body:type='EVENT' and T.body:e_type::STRING!='anomalyEvent'
        )
    )`
export const MergeEvents = 
    `MERGE INTO EVENTS_INTERNAL A USING DEDUP_TEMP B ON A.ID = B.ID AND A.DEVICE_ID = B.DEVICE_ID
    WHEN NOT MATCHED THEN INSERT
        (ID,VEHICLE_ID,DEVICE_ID,TIMESTAMP,LOCAL_TIMESTAMP,TIME_ZONE_ID,TYPE,NAME,LOCATION_LAT,LOCATION_LON,VALUE,THRESHOLD,UNITS,DURATION,DURATION_UNITS,POSITION,CODE,DESCRIPTION,ROAD_SPEED_LIMIT,GEOFENCE_ID,GEOFENCE_NAME,PROCESSED_TIMESTAMP,RAW,VALUE_STRING,THRESHOLD_STRING,DISTANCE,DISTANCE_UNITS,STARTING_VALUE,MAX_VOLTAGE,SPEED,AIRBAG_STATUS,GEOFENCE_VERSION,GEOFENCES,SNOWFLAKE_WRITE_TIMESTAMP,SW_CODE,SW_NAME,SW_SYSTEM,SW_SOURCE_DESC,CHARGING_SESSION_ENERGY_ADDED,RPM_THRESHOLD,PTO_DURATION_THRESHOLD,PTO_DURATION,RPM_AVG,PTO_UNITS)
        VALUES
        (ID,VEHICLE_ID,DEVICE_ID,TIMESTAMP,LOCAL_TIMESTAMP,TIME_ZONE_ID,TYPE,NAME,LOCATION_LAT,LOCATION_LON,VALUE,THRESHOLD,UNITS,DURATION,DURATION_UNITS,POSITION,CODE,DESCRIPTION,ROAD_SPEED_LIMIT,GEOFENCE_ID,GEOFENCE_NAME,PROCESSED_TIMESTAMP,RAW,VALUE_STRING,THRESHOLD_STRING,DISTANCE,DISTANCE_UNITS,STARTING_VALUE,MAX_VOLTAGE,SPEED,AIRBAG_STATUS,GEOFENCE_VERSION,GEOFENCES,SNOWFLAKE_WRITE_TIMESTAMP,SW_CODE,SW_NAME,SW_SYSTEM,SW_SOURCE_DESC,CHARGING_SESSION_ENERGY_ADDED,RPM_THRESHOLD,PTO_DURATION_THRESHOLD,PTO_DURATION,RPM_AVG,PTO_UNITS)`
export const CreateDedupTempForTrips = `CREATE OR REPLACE TABLE DEDUP_TEMP LIKE TRIPS_INTERNAL;`
export const InsertTripToDedupTemp =
    `INSERT INTO DEDUP_TEMP (ID,VEHICLE_ID,DEVICE_ID,START_TIMESTAMP,END_TIMESTAMP,LOCAL_START_TIMESTAMP,LOCAL_END_TIMESTAMP,TIME_ZONE_ID,START_LOCATION_LAT,START_LOCATION_LON,END_LOCATION_LAT,END_LOCATION_LON,DISTANCE,DISTANCE_UNITS,DURATION,DURATION_UNITS,PATH,PATH_ENCODING,PROCESSED_TIMESTAMP,RAW,DATATYPE,START_ODOMETER,START_ODOMETER_SIGNAL_TYPE,START_ODOMETER_UNITS,END_ODOMETER,END_ODOMETER_SIGNAL_TYPE,END_ODOMETER_UNITS,SNOWFLAKE_WRITE_TIMESTAMP)
    (
        SELECT ID,VEHICLE_ID,DEVICE_ID,START_TIMESTAMP,END_TIMESTAMP,LOCAL_START_TIMESTAMP,LOCAL_END_TIMESTAMP,TIME_ZONE_ID,START_LOCATION_LAT,START_LOCATION_LON,END_LOCATION_LAT,END_LOCATION_LON,DISTANCE,DISTANCE_UNITS,DURATION,DURATION_UNITS,PATH,PATH_ENCODING,PROCESSED_TIMESTAMP,RAW,DATATYPE,START_ODOMETER,START_ODOMETER_SIGNAL_TYPE,START_ODOMETER_UNITS,END_ODOMETER,END_ODOMETER_SIGNAL_TYPE,END_ODOMETER_UNITS,SNOWFLAKE_WRITE_TIMESTAMP FROM (
            SELECT 
            T.body:id::STRING as id,
            T.body:vId::STRING as vehicle_id,
            T.body:dId::STRING as device_id,
            T.body:ts_start::DATETIME as start_timestamp,
            T.body:ts_end::DATETIME as end_timestamp,
            T.body:ts_local_start::DATETIME as local_start_timestamp,
            T.body:ts_local_end::DATETIME as local_end_timestamp,  
            TO_TIMESTAMP(T.body:_ts::INTEGER)::DATETIME as PROCESSED_TIMESTAMP,
            T.body:tzId::STRING as time_zone_id,      
            T.body:location_start:coordinates[1]::FLOAT as start_location_lat,
            T.body:location_start:coordinates[0]::FLOAT as start_location_lon,
            T.body:location_end:coordinates[1]::FLOAT as end_location_lat,
            T.body:location_end:coordinates[0]::FLOAT as end_location_lon,
            T.body:dis_mi::FLOAT as distance,
            iff( distance::STRING is not null,'mi'::STRING, null) as distance_units, 
            T.body:dur_s::FLOAT as duration,
            iff( duration::STRING is not null,'sec'::STRING , null) as duration_units,
            ROUND(T.body:trip_start_odo:value::FLOAT,1) as START_ODOMETER,
            T.body:trip_start_odo:signalType::STRING as START_ODOMETER_SIGNAL_TYPE,
            iff( START_ODOMETER::STRING is not null,'mi'::STRING, null) as START_ODOMETER_UNITS, 
            ROUND(T.body:trip_end_odo:value::FLOAT, 1) as END_ODOMETER,
            T.body:trip_end_odo:signalType::STRING as END_ODOMETER_SIGNAL_TYPE,
            iff( END_ODOMETER::STRING is not null,'mi'::STRING, null) as END_ODOMETER_UNITS, 
            T.body:path::STRING as path,
            iff( path::STRING is not null, T.body:pathEncoding::STRING , null) as path_encoding,
            IFNULL(T.body:dataType::STRING , 'trip') as dataType,
            SYSDATE() as SNOWFLAKE_WRITE_TIMESTAMP,
            T.body::VARIANT as raw
            from INIT_LOAD_DEDUPED T where T.body:type='TRIP' and start_location_lat is not NULL and start_location_lon is not NULL 
        )
    )`
export const MergeTrips = 
    `MERGE INTO TRIPS_INTERNAL A USING DEDUP_TEMP B ON A.ID = B.ID AND A.DEVICE_ID = B.DEVICE_ID
    WHEN NOT MATCHED THEN INSERT
        (ID,VEHICLE_ID,DEVICE_ID,START_TIMESTAMP,END_TIMESTAMP,LOCAL_START_TIMESTAMP,LOCAL_END_TIMESTAMP,TIME_ZONE_ID,START_LOCATION_LAT,START_LOCATION_LON,END_LOCATION_LAT,END_LOCATION_LON,DISTANCE,DISTANCE_UNITS,DURATION,DURATION_UNITS,PATH,PATH_ENCODING,PROCESSED_TIMESTAMP,RAW,DATATYPE,START_ODOMETER,START_ODOMETER_SIGNAL_TYPE,START_ODOMETER_UNITS,END_ODOMETER,END_ODOMETER_SIGNAL_TYPE,END_ODOMETER_UNITS,SNOWFLAKE_WRITE_TIMESTAMP)
        VALUES
        (ID,VEHICLE_ID,DEVICE_ID,START_TIMESTAMP,END_TIMESTAMP,LOCAL_START_TIMESTAMP,LOCAL_END_TIMESTAMP,TIME_ZONE_ID,START_LOCATION_LAT,START_LOCATION_LON,END_LOCATION_LAT,END_LOCATION_LON,DISTANCE,DISTANCE_UNITS,DURATION,DURATION_UNITS,PATH,PATH_ENCODING,PROCESSED_TIMESTAMP,RAW,DATATYPE,START_ODOMETER,START_ODOMETER_SIGNAL_TYPE,START_ODOMETER_UNITS,END_ODOMETER,END_ODOMETER_SIGNAL_TYPE,END_ODOMETER_UNITS,SNOWFLAKE_WRITE_TIMESTAMP)`
export const DeleteInitLoadV3 = `DELETE FROM INIT_LOAD_V3`