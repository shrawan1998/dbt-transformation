{{
    config(
        materialized='incremental',
        incremental_strategy = 'merge',
        -- partition columns to be used in the destination table 
        partition_by={
            "field": "created_date",
            "data_type": "timestamp",
            "granularity": "day"
        },
        -- clustering columns to be used in the destination table
        cluster_by = ["created_date"],
        -- specify the columns that will be used to determine if a record is new or needs to be updated
        unique_key = ['metric_name', 'building_code', 'floor_code', 'space_code', 'timestamp', 'data_source'],
        -- specify the columns that will be updated when a record with the same unique key is found
        -- if you want to update all columns, don't specify this merge_update_columns parameter at all
        merge_update_columns = ['t1', 't2', 't3', 't4', 't5', 't6', 't7', 't8', 't9', 't10', 't11', 't12', 't13', 't14', 't15','t16','t17','t18',
        't19','t20','t21','t22','t23','tlt1D','tgt1D', 'created_date'],
        -- specify incremental_predicates to filter out records that are not needed for the incremental run from the destination table
        incremental_predicates = [
            "DATE(DBT_INTERNAL_DEST.created_date) > DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)"
        ]
    )
}}

with source_data as (

    SELECT 
        CAST(metric as string) as metric_name,
        CAST(building as string) as building_code,
        CAST(level as string) as floor_code,
        CAST(area as string) as space_code,
        PARSE_TIMESTAMP('%d/%m/%Y %H:%M:%S', datetime) as timestamp,
        CAST(t_1 as int64) as t1,
        CAST(t_2 as int64) as t2,
        CAST(t_3 as int64) as t3,
        CAST(t_4 as int64) as t4,
        CAST(t_5 as int64) as t5,
        CAST(t_6 as int64) as t6,
        CAST(t_7 as int64) as t7,
        CAST(t_8 as int64) as t8,
        CAST(t_9 as int64) as t9,
        CAST(t_10 as int64) as t10,
        CAST(t_11 as int64) as t11,
        CAST(t_12 as int64) as t12,
        CAST(t_13 as int64) as t13,
        CAST(t_14 as int64) as t14,
        CAST(t_15 as int64) as t15,
        CAST(t_16 as int64) as t16,
        CAST(t_17 as int64) as t17,
        CAST(t_18 as int64) as t18,
        CAST(t_19 as int64) as t19,
        CAST(t_20 as int64) as t20,
        CAST(t_21 as int64) as t21,
        CAST(t_22 as int64) as t22,
        CAST(t_23 as int64) as t23,
        CAST(t_1D as int64) as tlt1D,
        CAST(t_1D_1 as int64) as tgt1D,
        _airbyte_extracted_at as created_date,
        'nexpa' AS data_source
    FROM `transformed_events.carpark_duration` 

    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    -- (uses > to include records whose timestamp occurred since the last run of this model)
        WHERE _airbyte_extracted_at > (SELECT max(created_date) FROM {{ this }})
        
    {% endif %}

 )
    SELECT * FROM source_data
