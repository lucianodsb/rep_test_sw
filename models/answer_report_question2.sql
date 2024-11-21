/*
2 - Using the modeled DataMart, generate a SQL query that identifies the top 3
corporate customers by Revenue per month in the last 3 months.
*/

with ranked_customers_by_revenue as (
    select 
        agg.num_reference_month, 
        agg.cod_id_organization,
        agg.nam_organization,
        agg.des_organization_industry,
        sum(agg.val_revenue) as val_revenue,
        rank() over (
            partition by agg.num_reference_month
            order by sum(agg.val_revenue) desc
        ) as rank_position
    from {{ref('agg_monthly_indicators')}} agg
     where agg.num_reference_month > to_char(dateadd(month, -3, current_date), 'yyyymm')  
    group by 
        1,2,3,4
)
select 
    num_reference_month,
    rank_position as num_tca_rank_position,
    cod_id_organization,
    nam_organization,
    des_organization_industry,
    val_revenue
from ranked_customers_by_revenue
where rank_position <= 3
order by 1 desc, 2



