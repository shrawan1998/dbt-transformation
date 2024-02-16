{{
    config(
        materialized='incremental',
        partition_by={
            "field": "created_date",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by = ['created_date']
    )
}}

WITH source_data AS (
    SELECT
        JSON_EXTRACT_SCALAR(object, '$.time') AS datetime,
        JSON_EXTRACT_SCALAR(object, '$.sensor_id') AS sensor_id,
        JSON_EXTRACT_SCALAR(object, '$.space_id') AS space_id,
        JSON_EXTRACT_SCALAR(object, '$.space_name') AS space_name,
        JSON_EXTRACT_SCALAR(object, '$.mac_address') AS mac_address,
        JSON_EXTRACT_SCALAR(object, '$.measurement') AS measurement,
        object AS src_event,
        _airbyte_extracted_at AS created_date,
        "Butlr" AS data_source
    FROM `podium-datalake-dev-38a8.transformed_events.equipment_events`,
        UNNEST(JSON_EXTRACT_ARRAY(data, '$')) AS object

    {% if is_incremental() %}
        WHERE _airbyte_extracted_at > (SELECT max(created_date) FROM {{ this }})
    {% endif %}
)

SELECT 
    sd.datetime,
    sd.mac_address AS src_id,
    sd.measurement AS src_event_type,
    "Equipment does not exist" AS error,
    "equipment" AS type,
    "invalid" AS status,
    sd.src_event,
    sd.created_date
FROM source_data sd
LEFT JOIN {{ ref('reference_data') }} rd
    ON sd.mac_address = rd.src_equipment_id
WHERE rd.src_equipment_id is null
