{{ config(materialized='table') }}

SELECT GENERATE_UUID() as user_sk, user_id
FROM {{ref("events_raw")}}