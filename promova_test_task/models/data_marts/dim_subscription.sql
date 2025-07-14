{{ config(materialized='table') }}


select GENERATE_UUID() as subscription_sk, tr.subscription_id
from {{ref("transactions_raw")}} tr
