{{
    config(
        materialized='incremental',
        alias='fct_order_customer_analysis',
        unique_key='c_custkey'
    )
}}


with customers as (
select
  c_custkey,
  c_name,
  c_mktsegment
  from SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.CUSTOMER
  order by 1
  limit 2
),

orders as (
select
  o_orderkey,
  o_custkey,
  o_orderstatus,
  o_totalprice
  from SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS
),

final as (
select
  c.c_custkey,
  sum(o.o_totalprice),
  current_timestamp as data_load_ts
from customers c
inner join orders o
  on c.c_custkey=o.o_custkey
group by
  c.c_custkey
)

select * from final
{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where data_load_ts > (select max(data_load_ts) from {{ this }})
{% endif %}
