{{ config(materialized='view') }}

-- Розрахувати середню кількість куплених продуктів на унікального платного користувача
-- (має бути пораховано як загальна кількість куплених підписок subscription_id
-- поділених на кількість uniq_user_id які купили хоч один продукт).

-- Question: what is unique user id?
-- Assumption: unique users are non-duplicate


with users_with_subscriptions as (
    select user_id, count(distinct subscription_id) as sub_count
    from {{ ref("transactions_raw") }}
    group by user_id
),

users_with_purchase as (
    select count(distinct user_id)
    from {{ ref("transactions_raw") }}
)


select(
    (select sum(sub_count) from users_with_subscriptions)
    /
    (select * from users_with_purchase)
) as sub_per_user