{{ config(materialized='table') }}


select
       GENERATE_UUID() as transaction_sk,
       tr.transaction_id,
       tr.subscription_id,
       tr.transaction_time,
       date(transaction_time) as calendar_sk
from {{ref("transactions_raw")}} tr