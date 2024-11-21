/*
5 - Using the modeled DataMart, generate a SQL query that calculates the month-
over-month revenue growth rate for Swile..
*/


with revenue_per_month as (
    select
        num_reference_month,
        sum(val_revenue) as total_revenue
    from {{ref('agg_monthly_indicators')}}
    group by num_reference_month
),

previous_month_revenue as (
    select
        num_reference_month,
        total_revenue,
        lag(total_revenue) over (order by num_reference_month) as previous_month_revenue
    from revenue_per_month
),

growth_rate_calculation as (
    select
        num_reference_month,
        total_revenue,
        previous_month_revenue,
        case
            when previous_month_revenue is not null then
                round(
                    (total_revenue - previous_month_revenue) 
                    / previous_month_revenue * 100,
                    2
                )
            else null
        end as mum_growth_rate
    from previous_month_revenue
)

select
    num_reference_month,
    round(total_revenue,2) as total_revenue,
    round(previous_month_revenue,2) as previous_month_revenue,
    round(mum_growth_rate,2) as growth_rate_percentage_mom
from growth_rate_calculation
order by num_reference_month desc


