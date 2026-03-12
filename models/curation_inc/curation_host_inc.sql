{{
    config(
        database=var('inc_database'),
        schema=var('inc_schema'),
        materialized='incremental',
        unique_key='host_id'
    )
}}
with raw_hosts as (
    SELECT * from {{ source("raw_airbnb_data", "hosts") }}
)
select t1.*
from raw_hosts t1

{% if is_incremental() %}
where load_timestamp > (select max(load_timestamp) from {{this}} )

{% else %}
join (select host_id, max(load_timestamp) as load_timestamp from {{ source("raw_airbnb_data", "hosts") }} group by 1) t2
on t1.host_id = t2.host_id and t1.load_timestamp = t2.load_timestamp

{% endif %}