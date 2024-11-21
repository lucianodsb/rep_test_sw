/*
3 -  Using the modeled DataMart, define the most used wallets in number of
approved orders and calculate their TCA per month
*/

with ranked_customers_by_qty_order_credit_item_approved as (
    select 
        agg.num_reference_month, 
        agg.cod_id_wallet,
        agg.nam_wallet,
        agg.des_wallet,
        sum(qty_order_credit_item_approved) qty_order_credit_item_approved,
        sum(agg.val_total_credited_amount) as val_total_credited_amount,
        rank() over (
            partition by agg.num_reference_month
            order by sum(agg.qty_order_credit_item_approved) desc
        ) as rank_position
    from {{ref('agg_monthly_indicators')}} agg
    group by 
       1,2,3,4
    order by 1 desc, rank_position   
)
select 
    num_reference_month,
    rank_position as num_rank_position_by_qty_order_credit_item,
    qty_order_credit_item_approved,
    cod_id_wallet,
    nam_wallet,
    des_wallet,
    val_total_credited_amount
from ranked_customers_by_qty_order_credit_item_approved
order by 1 desc, 2