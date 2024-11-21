/*
4 - Using the modeled DataMart, generate a SQL query to identify the top
industries by number of organizations.
*/
with ranked_industry as (
    select
        des_organization_industry,
        count(cod_id_organization) as qty_organization,
        rank() over (order by count(cod_id_organization) desc) as rank_position
    from {{ ref('dim_organization') }}
    group by des_organization_industry
)
select 
    rank_position as num_rank_position,
    des_organization_industry,
    qty_organization
from ranked_industry
order by rank_position