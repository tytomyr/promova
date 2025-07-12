{{ config(materialized='table') }}

select GENERATE_UUID() as product_sk, product_name
from {{ref("transactions_raw")}}