{{ config(materialized='table') }}


with unique_event_users as (
    select user_id from {{ ref("events_raw") }}
    qualify row_number() over (partition by user_id order by event_time desc) = 1
),
unique_transasction_users as (
    select user_id from {{ ref("transactions_raw") }}
    qualify row_number() over (partition by user_id order by transaction_time desc) = 1
),
all_users as (
    select user_id from unique_event_users
    union distinct
    select user_id from unique_transasction_users
)
select GENERATE_UUID() AS user_sk, user_id,
from all_users
