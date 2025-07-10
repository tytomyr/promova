{{ config(materialized='table') }}

select product_name
from {{ref("transactions_raw")}}