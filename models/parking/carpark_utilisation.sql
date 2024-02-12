{{
    config(
        materialized='incremental'
    )
}}

WITH source_data AS (

    SELECT
        CAST(JSON_EXTRACT_SCALAR(_airbyte_data, '$.metric') AS string) AS metric_name,
        CAST(JSON_EXTRACT_SCALAR(_airbyte_data, '$.building') AS string) AS building_code,
        CAST(JSON_EXTRACT_SCALAR(_airbyte_data, '$.level') AS string) AS floor_code,
        CAST(JSON_EXTRACT_SCALAR(_airbyte_data, '$.area') AS string) AS space_code,
        CAST(REGEXP_REPLACE(JSON_EXTRACT_SCALAR(_airbyte_data, '$.datetime'), r'(\d+)-(\d+)-(\d+) (\d+):(\d+)', r'\3-\2-\1 \4:\5:00') AS timestamp) AS timestamp,
        CAST(JSON_EXTRACT_SCALAR(_airbyte_data, '$.average_occupancy_percentage') AS int64) AS average_occupancy_percentage,
        _airbyte_loaded_at AS created_date,
        'nexpa' AS data_source
    FROM `airbyte_internal.transformed_events_raw__stream_carpark_utilisation`

    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    -- (uses > to include records whose timestamp occurred since the last run of this model)
    WHERE _airbyte_loaded_at > (SELECT max(created_date) FROM {{ this }})

    {% endif %}
    
)

-- Write a merge statement to append new records to the target table
-- https://docs.getdbt.com/docs/build/incremental-models#supported-incremental-strategies-by-adapter

SELECT * FROM source_data;