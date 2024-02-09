{{ config(materialized='table') }}

with source_data as (

    select 
        _airbyte_extracted_at as created_at,
        metric as metric_name,
        building as building_code,
        level as floor_code,
        area as space_code,
        TIMESTAMP(datetime) as timestamp,
        'nexpa' as data_source,
        t_1 as t<1,
        t_2 as t<2,
        t_3 as t<3,
        t_4 as t<4,
        t_5 as t<5,
        t_6 as t<6,
        t_7 as t<7,
        t_8 as t<8,
        t_9 as t<9,
        t_10 as t<10,
        t_11 as t<11,
        t_12 as t<12,
        t_13 as t<13,
        t_14 as t<14,
        t_15 as t<15,
        t_16 as t<16,
        t_17 as t<17,
        t_18 as t<18,
        t_19 as t<19,
        t_20 as t<20,
        t_21 as t<21,
        t_22 as t<22,
        t_23 as t<23,
        t_1D as t<1D,
        t_1D_1 as t>1D
    from `airbyte_internal.transformed_events_raw__stream_carpark_occupancy`
    
)

select * from source_data
