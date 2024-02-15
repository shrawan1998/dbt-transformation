{{
    config(
        materialized='ephemeral',
    )
}}

SELECT
    JSON_EXTRACT_SCALAR(object, '$.building_id') AS building_id,
    JSON_EXTRACT_SCALAR(object, '$.field') AS field,
    JSON_EXTRACT_SCALAR(object, '$.time') AS timestamp,
    JSON_EXTRACT_SCALAR(object, '$.start') AS start,
    JSON_EXTRACT_SCALAR(object, '$.stop') AS stop,
    JSON_EXTRACT_SCALAR(object, '$.value') AS value,
    JSON_EXTRACT_SCALAR(object, '$.timezone_offset') AS timezone_offset,
    JSON_EXTRACT_SCALAR(object, '$.building_name') AS building_name,
    JSON_EXTRACT_SCALAR(object, '$.hive_id') AS hive_id,
    JSON_EXTRACT_SCALAR(object, '$.hive_serial') AS hive_serial,
    JSON_EXTRACT_SCALAR(object, '$.sensor_id') AS sensor_id,
    JSON_EXTRACT_SCALAR(object, '$.space_id') AS space_id,
    JSON_EXTRACT_SCALAR(object, '$.space_name') AS space_name,
    JSON_EXTRACT_SCALAR(object, '$.room_id') AS room_id,
    JSON_EXTRACT_SCALAR(object, '$.room_name') AS room_name,
    JSON_EXTRACT_SCALAR(object, '$.zone_id') AS zone_id,
    JSON_EXTRACT_SCALAR(object, '$.zone_name') AS zone_name,
    JSON_EXTRACT_SCALAR(object, '$.mac_address') AS mac_address,
    _airbyte_extracted_at AS created_date,
    "Butlr" as data_source
FROM `podium-datalake-dev-38a8.transformed_events.equipment_events`,
    UNNEST(JSON_EXTRACT_ARRAY(data, '$')) AS object

