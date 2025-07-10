
{{ config(materialized='table') }}

SELECT device_id
FROM {{ ref("events_raw") }}