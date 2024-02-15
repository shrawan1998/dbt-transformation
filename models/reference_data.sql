{{
    config(
        materialized='ephemeral',
    )
}}

SELECT 
    building_code,
    building_tenant_code,
    building_primary_function,
    floor_code,
    floor_type,
    space_code,
    space_type,
    zone_code,
    zone_type,
    site_code,
    site_type,
    equipment_code,
    equipment_type,
    tenancy_space_code,
    src_equipment_id
FROM `podium_asset_management.vw_equipment`
    WHERE src_equipment_id is not null and is_active = true
