{{ config(materialized='table') }}

with source_data as (

   SELECT
        CAST(JSON_EXTRACT_SCALAR(_airbyte_data, '$.metric') AS string) AS metric_name,
        CAST(JSON_EXTRACT_SCALAR(_airbyte_data, '$.building') AS string) AS building_code,
        CAST(JSON_EXTRACT_SCALAR(_airbyte_data, '$.level') AS string) AS floor_code,
        CAST(JSON_EXTRACT_SCALAR(_airbyte_data, '$.area') AS string) AS space_code,
        CAST(REGEXP_REPLACE(JSON_EXTRACT_SCALAR(_airbyte_data, '$.datetime'), r'(\d+)-(\d+)-(\d+) (\d+):(\d+)', r'\3-\2-\1 \4:\5:00') AS timestamp) AS timestamp,
        _airbyte_loaded_at AS created_date,
        CAST(JSON_EXTRACT_SCALAR(_airbyte_data, '$.in_count') AS int64) AS in_count,
        CAST(JSON_EXTRACT_SCALAR(_airbyte_data, '$.out_count') AS int64) AS out_count,
        'nexpa' AS data_source
    FROM
        `airbyte_internal.transformed_events_raw__stream_carpark_occupancy`
)

select * from source_data
