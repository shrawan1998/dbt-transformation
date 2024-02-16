{{
    config(
        materialized='incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by={
            "field": "timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by = ["timestamp","data_source"]
    )
}}
   with source_data as (
    SELECT
        CAST(metric AS STRING) AS metric_name,
        CAST(building AS STRING) AS building_code,
        CAST(level AS STRING) AS floor_code,
        CAST(area AS STRING) AS space_code,
        CASE
            -- Check for format 'dd-mm-yyyy hh:mm'
            WHEN REGEXP_CONTAINS(datetime, r'^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$') THEN PARSE_TIMESTAMP('%d-%m-%Y %H:%M', datetime)
            -- Check for format 'dd/mm/yyyy hh:mm:ss'
            WHEN REGEXP_CONTAINS(datetime, r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$') THEN PARSE_TIMESTAMP('%d/%m/%Y %H:%M:%S', datetime)
            -- Add more formats as needed
            ELSE NULL
        END AS timestamp,
        CAST(average_occupancy_percentage AS INT64) AS average_occupancy_percentage,
        _airbyte_extracted_at AS created_date,
        'nexpa' AS data_source
        FROM `transformed_events.carpark_utilisation`

    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    -- (uses > to include records whose timestamp occurred since the last run of this model)
        WHERE _airbyte_extracted_at > (SELECT max(created_date) FROM {{ this }})
        -- WHERE _airbyte_extracted_at > _dbt_max_partition
        
    {% endif %}

 )
    SELECT * FROM source_data