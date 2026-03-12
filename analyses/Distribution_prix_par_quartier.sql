with listings as (
    select * from {{ ref('curation_listings') }}
),

hosts as (
    select * from {{ ref('curation_hosts') }}
)

select
    h.host_neighbourhood as quartier,
    count(*) as nb_listings,
    round(avg(l.price), 2) as prix_moyen,
    percentile_cont(0.5) within group (order by l.price)  as prix_median,
    min(l.price) as prix_min,
    max(l.price) as prix_max
from listings l
join hosts h on l.host_id = h.host_id
where h.host_neighbourhood is not null          
  and l.price is not null
group by 1
order by prix_moyen desc