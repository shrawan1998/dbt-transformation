{{
    config(
        materialized='incremental',
        partition_by={
            "field": "created_date",
            "data_type": "timestamp",
            "granularity": "hour"
        },
        cluster_by = ['created_date']
    )
}}

SELECT 
    sd.field,
    sd.hive_id,
    sd.hive_serial,
    sd.mac_address,
    sd.measurement,
    sd.sensor_id,
    sd.space_id,
    sd.space_name,
    sd.start,
    sd.stop,
    sd.timestamp,
    sd.timezone_offset,
    sd.value,
    rd.building_code,
    rd.building_tenant_code,
    rd.building_primary_function,
    rd.floor_code,
    rd.floor_type,
    rd.space_code,
    rd.space_type,
    rd.zone_code,
    rd.zone_type,
    rd.site_code,
    rd.site_type,
    rd.equipment_code,
    rd.equipment_type,
    rd.tenancy_space_code,
    rd.src_equipment_id
FROM {{ ref('source_data') }} sd
INNER JOIN {{ ref('reference_data') }} rd
    ON sd.mac_address = rd.src_equipment_id
