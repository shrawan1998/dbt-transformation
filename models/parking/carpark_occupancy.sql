{{ config(materialized='incremental'
    unique_key = ['metric_name', 'building_code', 'floor_code', 'space_code', 'timestamp', 'data_source'],
        merge_update_columns = ['in_count', 'out_count', 'created_date'],
        schema='processed_data_SO'
        alias='carpack-occupancy-so'
        ) }}

with airbyte_final_data as (

   SELECT
      
        CAST(metric AS string) AS metric_name,
        CAST(building AS string) AS building_code,
        CAST(level AS string) AS floor_code,
        CAST(area AS string) AS space_code,
        CAST(date(datetime) AS timestamp) AS timestamp,
        CAST(in_count AS int64) AS in_count,
        CAST(out_count AS int64) AS out_count,
         _airbyte_extracted_at AS created_date,
    FROM
        `transformed_events.carpark_occupancy`
)

select * from airbyte_final_data
{% if is_incremental() %}
where _airbyte_extracted_at > (select MAX(created_date)) FROM {{this}}
{% endif %}`

