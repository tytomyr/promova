{{ config(materialized='table') }}
-- transaction_id, subscription_id, amount


SELECT GENERATE_UUID() as subscription_sk, tr.subscription_id, tr.user_id
FROM {{ref("transactions_raw")}} tr
