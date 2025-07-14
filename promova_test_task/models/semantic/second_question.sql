{{ config(materialized='view') }}


--Який відсоток користувачів має кілька девайсів з одним акаунтом? (same user_id and different device_id?)


with calculated_users as (select user_id, count(distinct device_id)
                          from {{ ref("fact_user_activity") }}
                          where event_type = 'login'
                          and user_id is not null
                          group by user_id
                          having count(distinct device_id) > 1)
select(
    (select count(*) from calculated_users)
    /
    (select count(distinct user_id) from {{ ref("dim_user") }} where user_id is not null) * 100
    ) as user_with_multiple_device_percentage
