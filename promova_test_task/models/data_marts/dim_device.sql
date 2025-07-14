{{ config(materialized='table') }}


with unique_devices as (
    select device_id from {{ ref("events_raw") }}
    qualify row_number() over (partition by device_id order by event_time desc) = 1
)
select GENERATE_UUID() as device_sk, device_id
from unique_devices