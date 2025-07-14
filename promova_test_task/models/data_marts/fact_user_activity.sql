{{ config(materialized='table') }}


select
    er.event_hash,
    er.event_type,
    er.event_time,
    dd.device_id,
    du.user_id,
    date(er.event_time) as calendar_sk
from {{ref("events_raw")}} er
left join {{ref("dim_user")}} du ON du.user_id = er.user_id
left join {{ref("dim_device")}} dd ON dd.device_id = er.device_id