{{ config(materialized='table') }}

with source_data as (

    select 
        _airbyte_extracted_at as created_at,
        metric as metric_name,
        building as building_code,
        level as floor_code,
        area as space_code,
        TIMESTAMP(datetime) as timestamp,
        average_occupancy_percentage,
        'nexpa' as data_source
    from `airbyte_internal.transformed_events_raw__stream_carpark_utilisation`
    
)

select * from source_data
