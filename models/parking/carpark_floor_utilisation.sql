{{
    config(
        materialized='incremental',
        incremental_strategy = 'merge',
        partition_by={
            "field": "created_date",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by = ["created_date"],
        unique_key = ['metric_name', 'building_code', 'floor_code', 'space_code', 'timestamp', 'data_source'],
        merge_update_columns = ['average_occupancy_percentage', 'created_date'],
        incremental_predicates = [
            "DATE(DBT_INTERNAL_DEST.created_date) > DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)"
        ]
    )
}}
    with source_data as (
    SELECT
        CAST(metric AS string) AS metric_name,
        CAST(building AS string) AS building_code,
        CAST(level AS string) AS floor_code,
        CAST(area AS string) AS space_code,
        CASE
            -- Check for format 'dd-mm-yyyy hh:mm'
            WHEN REGEXP_CONTAINS(datetime, r'^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$') THEN PARSE_TIMESTAMP('%d-%m-%Y %H:%M', datetime)
            -- Check for format 'dd/mm/yyyy hh:mm:ss'
            WHEN REGEXP_CONTAINS(datetime, r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$') THEN PARSE_TIMESTAMP('%d/%m/%Y %H:%M:%S', datetime)
            -- Add more formats as needed
            ELSE NULL
        END AS timestamp,
        CAST(average_occupancy_percentage AS int64) AS average_occupancy_percentage,
        _airbyte_extracted_at AS created_date,
        'nexpa' AS data_source
    FROM `transformed_events.carpark_floor_utilisation`

    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    -- (uses > to include records whose timestamp occurred since the last run of this model)
        WHERE _airbyte_extracted_at > (SELECT max(created_date) FROM {{ this }})
        
    {% endif %}

 )
    SELECT * FROM source_data
    
