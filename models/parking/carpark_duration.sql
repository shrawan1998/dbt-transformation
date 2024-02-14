{{
    config(
        materialized='incremental',
        unique_key = ['metric_name', 'building_code', 'floor_code', 'space_code', 'timestamp', 'data_source'],
        merge_update_columns = ['t1', 't2', 't3', 't4', 't5', 't6', 't7', 't8', 't9', 't10', 't11', 't12', 't13', 't14', 't15', 'created_date']
    )
}}

-- with source_data as (

    SELECT 
        CAST(json_extract_scalar(_airbyte_data, '$.metric') as string) as metric_name,
        CAST(json_extract_scalar(_airbyte_data, '$.building') as string) as building_code,
        CAST(json_extract_scalar(_airbyte_data, '$.level') as string) as floor_code,
        CAST(json_extract_scalar(_airbyte_data, '$.area') as string) as space_code,
        CAST(REGEXP_REPLACE(JSON_EXTRACT_SCALAR(_airbyte_data, '$.datetime'), r'(\d+)-(\d+)-(\d+) (\d+):(\d+)', r'\3-\2-\1 \4:\5:00') AS timestamp) AS timestamp,
        CAST(json_extract_scalar(_airbyte_data, '$.t1H') as int64) as t1,
        CAST(json_extract_scalar(_airbyte_data, '$.t2H') as int64) as t2,
        CAST(json_extract_scalar(_airbyte_data, '$.t3H') as int64) as t3,
        CAST(json_extract_scalar(_airbyte_data, '$.t4H') as int64) as t4,
        CAST(json_extract_scalar(_airbyte_data, '$.t5H') as int64) as t5,
        CAST(json_extract_scalar(_airbyte_data, '$.t6H') as int64) as t6,
        CAST(json_extract_scalar(_airbyte_data, '$.t7H') as int64) as t7,
        CAST(json_extract_scalar(_airbyte_data, '$.t8H') as int64) as t8,
        CAST(json_extract_scalar(_airbyte_data, '$.t9H') as int64) as t9,
        CAST(json_extract_scalar(_airbyte_data, '$.t10H') as int64) as t10,
        CAST(json_extract_scalar(_airbyte_data, '$.t11H') as int64) as t11,
        CAST(json_extract_scalar(_airbyte_data, '$.t12H') as int64) as t12,
        CAST(json_extract_scalar(_airbyte_data, '$.t13H') as int64) as t13,
        CAST(json_extract_scalar(_airbyte_data, '$.t14H') as int64) as t14,
        CAST(json_extract_scalar(_airbyte_data, '$.t15H') as int64) as t15,
        _airbyte_loaded_at as created_date,
        'nexpa' AS data_source
    FROM `transformed_events.carpark_duration` 

    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    -- (uses > to include records whose timestamp occurred since the last run of this model)
    WHERE _airbyte_loaded_at > (SELECT max(created_date) FROM {{ this }})

    {% endif %}
    
-- )

-- SELECT * FROM source_data;
