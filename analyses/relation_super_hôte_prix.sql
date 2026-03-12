-- Relation entre la qualification super-hôte et le prix

with listings as (
    select * from {{ ref('curation_listings') }}
),

hosts as (
    select * from {{ ref('curation_hosts') }}
),

listings_hosts as (
    select
        h.is_superhost,
        h.host_neighbourhood  as quartier,
        l.price,
        l.room_type
    from listings l
    join hosts h on l.host_id = h.host_id
    where l.price is not null
      and h.host_neighbourhood is not null
)

select
    is_superhost,
    quartier,
    room_type,
    count(*)  as nb_listings,
    round(avg(price), 2) as prix_moyen,
    percentile_cont(0.5) within group (order by price) as prix_median,
    min(price)  as prix_min,
    max(price)  as prix_max
from listings_hosts
group by 1, 2, 3
order by is_superhost desc, prix_moyen desc
