{{ config(
    materialized='incremental',
    unique_key=['building_id', 'building_code', 'mac_address'],
    merge_update_columns=['started_from', 'end_date', 'created_date'],
    schema='processed_events',
    alias='butlr_transformation'
) }}

WITH source_data AS (
    SELECT
        CAST(building_id AS int64) AS building_id,
        CAST(building_code AS string) AS building_code,
        CAST(mac_address AS string) AS mac_id,
        CAST(start AS timestamp) AS started_from,
        CAST(stop AS timestamp) AS end_date,
        CAST(time AS timestamp) AS created_date,
        'butlr' AS data_source
    FROM `transformed_events.BUTLR_Connector`

    {% if is_incremental() %}
    WHERE time > (SELECT MAX(created_date) FROM {{ this }})
    {% endif %}
)

SELECT * FROM source_data;
