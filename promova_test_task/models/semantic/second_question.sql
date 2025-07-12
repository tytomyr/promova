-- Який відсоток користувачів має кілька девайсів з одним акаунтом?

{{ config(materialized='table') }}

with selected_users as (
  select user_id
  from {{ ref("dim_user") }}
  where subscriptions = 1 and devices > 1
)

select
  count(*) * 100.0 / (select count(*) from {{ ref("dim_user") }})
    as users_with_multiple_device_and_one_subscription_pct
from selected_users
