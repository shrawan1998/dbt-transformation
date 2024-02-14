{{
    config(
        materialized='incremental',
        partition_by={
            "field": "created_date",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by = ["created_date"],
        unique_key = ['metric_name', 'building_code', 'floor_code', 'space_code', 'timestamp', 'data_source'],
        merge_update_columns = ['average_occupancy_percentage', 'created_date']
    )
}}
   with source_data as (
    SELECT
        CAST(metric AS string) AS metric_name,
        CAST(building AS string) AS building_code,
        CAST(level AS string) AS floor_code,
        CAST(area AS string) AS space_code,
        -- CAST(REGEXP_REPLACE(datetime, r'(\d+)-(\d+)-(\d+) (\d+):(\d+)', r'\3-\2-\1 \4:\5:00') AS timestamp) AS timestamp,
        -- CAST(date(datetime) AS timestamp) AS timestamp,
        -- datetime AS timestamp,
        PARSE_TIMESTAMP('%d/%m/%Y %H:%M:%S', datetime) AS timestamp,
        CAST(average_occupancy_percentage AS int64) AS average_occupancy_percentage,
        _airbyte_extracted_at AS created_date,
        'nexpa' AS data_source
    FROM `transformed_events.carpark_utilisation`

    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    -- (uses > to include records whose timestamp occurred since the last run of this model)
        WHERE _airbyte_extracted_at > (SELECT max(created_date) FROM {{ this }})
    )
    SELECT * FROM source_data

    {% endif %}

    {% if not is_incremental() %}

    -- this filter will only be applied on a first run
    )
    SELECT * FROM source_data

    {% endif %}