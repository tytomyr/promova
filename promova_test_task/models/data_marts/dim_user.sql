{{ config(materialized='table') }}

WITH subscriptions_per_user AS (
  SELECT
    user_id,
    COUNT(*) AS subscriptions
  FROM {{ ref("dim_subscription") }}
  GROUP BY user_id
),

devices_per_user AS (
  SELECT
    user_id,
    COUNT(*) AS devices
  FROM {{ ref("dim_device") }}
  GROUP BY user_id
)

SELECT
  GENERATE_UUID() AS user_sk,
  tr.user_id,
  COALESCE(s.subscriptions, 0) AS subscriptions,
  COALESCE(d.devices, 0) AS devices
FROM {{ ref("transactions_raw") }} tr
LEFT JOIN subscriptions_per_user s ON tr.user_id = s.user_id
LEFT JOIN devices_per_user d ON tr.user_id = d.user_id
GROUP BY tr.user_id, s.subscriptions, d.devices
