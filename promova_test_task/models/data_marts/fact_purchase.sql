{{ config(materialized='table') }}
-- transaction_id, subscription_id, amount


SELECT GENERATE_UUID() as transaction_sk, tr.transaction_id, tr.subscription_id, tr.transaction_time
FROM {{ref("transactions_raw")}} tr