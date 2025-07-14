{{ config(materialized='view') }}


-- Визначити користувачів які одночасно користувались кількома девайсами
-- (таких які одночасно були залогіненими в один акаунт user_id на кількох device_id).

-- Question: Що означає користуватись одночасно?
-- Assumption: login та app_open events протягом одного тижня означає що людина користувалась одночасно


with users_with_multiple_device as (
        select c.week_of_year , u.user_id, count(distinct u.device_id) as device
        from {{ref("fact_user_activity")}} u
        left join {{ ref("dim_calendar") }} c on u.calendar_sk = c.date_day
        where event_type in ('login', 'app_open') and user_id is not null
        group by u.user_id, c.week_of_year
)

select user_id, device
from users_with_multiple_device f
where device > 1
