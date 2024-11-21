with monthly_indicators as (
    select  TO_CHAR(fat.dat_schedule_order_credit, 'YYYYMM')  num_reference_month,
        fat.cod_id_organization,
        fat.cod_id_wallet,
        fee.val_related_fee,
        sum(val_total_credited) val_total_credited_amount,
        count(cod_id_order_credit_item) as qty_order_credit_item_approved
    from {{ref('fact_order_credit_item')}} fat
    left join {{ref('fact_aux_organization_montly_fee')}} fee 
    on TO_CHAR(fat.dat_schedule_order_credit, 'YYYYMM') = fee.num_reference_month
    and fat.cod_id_organization = fee.cod_id_organization
    where des_status_order_credit_item = 'Approved'
    group by 1,2,3,4
),

monthly_indicators_complement as (
    select 
      ind.num_reference_month,
      ind.cod_id_organization,
      dmorg.nam_organization,
      dmorg.des_organization_industry,
      ind.cod_id_wallet,
      dmwallet.nam_wallet,
      dmwallet.des_wallet,
      ind.val_related_fee,
      ind.val_total_credited_amount,
      ind.qty_order_credit_item_approved,
      round(ind.val_total_credited_amount*ind.val_related_fee,2) as val_revenue 
    from monthly_indicators ind
    left join {{ref('dim_organization')}} dmorg on
      ind.cod_id_organization = dmorg.cod_id_organization
    left join {{ref('dim_wallet')}} dmwallet on
      ind.cod_id_wallet = dmwallet.cod_id_wallet  
)
select * from monthly_indicators_complement