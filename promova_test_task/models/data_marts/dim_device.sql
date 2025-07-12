
{{ config(materialized='table') }}

SELECT GENERATE_UUID() as device_sk, device_id, count(user_id) as
FROM {{ ref("events_raw") }}