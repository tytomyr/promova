{{ config(materialized='view') }}


--Який відсоток користувачів мають кілька акаунтів (різні user_id)?


-- question: як ми можемо визначити що різні аккаунти належать одній людині? (payment details could help),
-- та як ми рахуємо користувачів? (тільки зареєстрованих чи без реєстрації також?)
-- assumption: визначимо користувача з однією підпискою та декількома акаунтами, враховуємо тільки зареєстрованих


with users_with_subscriptions as (
    select user_id, count(distinct subscription_id) as sub_count
    from {{ ref("transactions_raw") }}
    group by user_id
    having count(distinct subscription_id) = 1
),

calculated_users as (
    select user_id, count(distinct device_id) as device
    from {{ ref("fact_user_activity") }}
    where event_type = 'login'
    and user_id in (select user_id from users_with_subscriptions)
    group by user_id
    having count(distinct device_id) > 1
)

select(
    (select count(*) from calculated_users) * 100
    /
    (select count(distinct user_id) from {{ ref("fact_user_activity")}} where user_id is not null)
) as multiple_account_percentage

