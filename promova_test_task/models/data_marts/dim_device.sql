
{{ config(materialized='table') }}

SELECT GENERATE_UUID() as device_sk, device_id, user_id
FROM {{ ref("events_raw") }}