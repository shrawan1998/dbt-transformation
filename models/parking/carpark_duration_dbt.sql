{{
    config(
        materialized='incremental',
        unique_key = ['metric_name', 'building_code', 'floor_code', 'space_code', 'timestamp', 'data_source'],
        merge_update_columns = ['t1', 't2', 't3', 't4', 't5', 't6', 't7', 't8', 't9', 't10', 't11', 't12', 't13', 't14', 't15', 'created_date']
    )
}}

-- with source_data as (

    SELECT 
        CAST(metric as string) as metric_name,
        CAST(building as string) as building_code,
        CAST(level as string) as floor_code,
        CAST(area as string) as space_code,
        -- CAST(REGEXP_REPLACE(datetime, r'(\d+)-(\d+)-(\d+) (\d+):(\d+)', r'\3-\2-\1 \4:\5:00') AS timestamp) AS timestamp,
        CAST(date(datetime) as timestamp) as timestamp,
        CAST(t1H as int64) as t1,
        CAST(t2H as int64) as t2,
        CAST(t3H as int64) as t3,
        CAST(t4H as int64) as t4,
        CAST(t5H as int64) as t5,
        CAST(t6H as int64) as t6,
        CAST(t7H as int64) as t7,
        CAST(t8H as int64) as t8,
        CAST(t9H as int64) as t9,
        CAST(t10H as int64) as t10,
        CAST(t11H as int64) as t11,
        CAST(t12H as int64) as t12,
        CAST(t13H as int64) as t13,
        CAST(t14H as int64) as t14,
        CAST(t15H as int64) as t15,
        _airbyte_extracted_at as created_date,
        'nexpa' AS data_source
    FROM `transformed_events.carpark_duration` 

    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    -- (uses > to include records whose timestamp occurred since the last run of this model)
    WHERE _airbyte_extracted_at > (SELECT max(created_date) FROM {{ this }})

    {% endif %}
    
-- )

-- SELECT * FROM source_data;
