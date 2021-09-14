{{
    config(
        materialized='incremental',
        alias='fct_order_customer_date_analysis',
        unique_key='sk_key'
    )
}}


with customers as (
select
  c_custkey,
  c_name,
  c_mktsegment
  from SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.CUSTOMER
),

orders as (
select
  o_orderkey,
  o_orderdate,
  o_custkey,
  o_orderstatus,
  o_totalprice
  from SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS
  where o_orderdate='1993-12-22'
),

final as (
select
  {{ dbt_utils.surrogate_key(['c.c_custkey', 'o.o_orderdate']) }} as sk_key,
  c.c_custkey,
  o.o_orderdate,
  sum(o.o_totalprice),
  current_timestamp as data_load_ts
from customers c
inner join orders o
  on c.c_custkey=o.o_custkey
group by
  sk_key,
  c.c_custkey,
  o.o_orderdate
)

select * from final
{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where data_load_ts > (select max(data_load_ts) from {{ this }})
{% endif %}

