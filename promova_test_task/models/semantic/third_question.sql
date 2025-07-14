{{ config(materialized='view') }}


-- Який відсоток унікальних користувачів (uniq_user_id) відносно всіх дублів device_id-user_id
-- які генеруються в системі через перелогіни юзерів або повторне встановлення додатку на нових девайсах?


-- Question: чим відрізняється наявність кількох аккаунтів від дублючих акаунтів?
-- Assumption: дублючі аккаунти - user_id is null


with counted_duplicates_ver1 as (
            select fu.device_id, count(distinct fu.user_id) as duplicates
            from {{ ref("fact_user_activity") }} fu
            group by fu.device_id
            having duplicates > 1
),

counted_duplicates_ver2 as (
            select count(distinct device_id) as duplicates
            from {{ ref("fact_user_activity") }} fu
            where user_id is null
            group by user_id
),

distinct_users as (
            select count(distinct user_id) as all_users
            from {{ ref("fact_user_activity") }}
            where user_id is not null
)

select(
            (select * from counted_duplicates_ver2)
            /
            (select * from distinct_users) * 100
) as duplicates_percentage

