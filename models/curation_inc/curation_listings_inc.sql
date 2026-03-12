{{
    config(
        database=var('inc_database'),
        schema=var('inc_schema'),
        materialized='incremental',
        unique_key='id'
    )
}}
with raw_listings as (
    SELECT * from {{ source("raw_airbnb_data", "listings") }}
)
select t1.*
from raw_listings t1

{% if is_incremental() %}
where load_timestamp > (select max(load_timestamp) from {{this}} )

{% else %}
join (select id, max(load_timestamp) as load_timestamp from {{ source("raw_airbnb_data", "listings") }} group by 1) t2
on t1.id = t2.id and t1.load_timestamp = t2.load_timestamp

{% endif %}