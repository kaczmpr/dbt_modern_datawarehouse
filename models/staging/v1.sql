{{ config(materialized='table') }}


with source_data as (

select *
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.CUSTOMER

),

final as (
select *
from source_data
)

select * from final

