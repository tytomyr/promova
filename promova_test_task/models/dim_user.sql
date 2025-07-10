{{ config(materialized='table') }}

SELECT user_id
FROM {{ref("events_raw")}}