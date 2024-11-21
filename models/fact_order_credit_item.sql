with fact_order_credit_item as (
    select 
        oci.dt_schedule as dat_schedule_order_credit,
        oci.order_credit_id as cod_id_order_credit,
        oci.order_credit_item_id as cod_id_order_credit_item,
        oci.id_employee as cod_id_employee,
        emp.cod_id_organization as cod_id_organization,
        oci.status as des_status_order_credit_item,
        oci.total_credited_value as val_total_credited,
        oci.wallet_id as cod_id_wallet       
    from {{source('sources','order_credit_item')}}  oci
    left join {{ref('dim_employee')}} emp ON
    oci.id_employee = emp.cod_id_employee
    
)

select * from fact_order_credit_item

