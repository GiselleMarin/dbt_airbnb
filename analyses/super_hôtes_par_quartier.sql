with hosts as (
    select * from {{ ref('curation_hosts') }}
)

select
    host_neighbourhood as quartier,
    count(*) as nb_total_hosts,
    sum(case when is_superhost then 1 else 0 end) as nb_superhosts,
    sum(case when not is_superhost then 1 else 0 end) as nb_non_superhosts,
    round( sum(case when is_superhost then 1 else 0 end) * 100.0 / nullif(count(*), 0) , 1) as pct_superhosts
from hosts
where host_neighbourhood is not null
group by 1
order by pct_superhosts desc