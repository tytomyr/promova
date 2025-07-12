{{ config(materialized='table') }}

SELECT GENERATE_UUID() as user_sk, user_id, count(user_id) as transactions_made
FROM {{ref("transactions_raw")}}
group by user_id