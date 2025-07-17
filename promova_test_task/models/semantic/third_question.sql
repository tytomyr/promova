{{ config(materialized='view') }}


-- Який відсоток унікальних користувачів (uniq_user_id) відносно всіх дублів device_id-user_id
-- які генеруються в системі через перелогіни юзерів або повторне встановлення додатку на нових девайсах?


-- Question: чим відрізняється наявність кількох аккаунтів від дублючих акаунтів?
-- Assumption: дублючі аккаунти - user_id is null


with users_counts as (
            select fu.device_id, count(distinct fu.user_id) as users
            from {{ ref("fact_user_activity") }} fu
            where user_id is not null
            group by fu.device_id
),

device_count as (
            select device_id
            from users_counts
            where users > 1
),

unique_users as (
  select distinct user_id
  from {{ ref('fact_user_activity') }}
  where device_id in (select device_id from device_count)
),

total_users as (
            select count(distinct user_id) as all_users
            from {{ ref("fact_user_activity") }}
            where user_id is not null
)

select(
            (select count(*) from unique_users)
            /
            (select count( distinct user_id) from {{ ref("fact_user_activity") }} where user_id is not null) * 100
) as duplicates_percentage

