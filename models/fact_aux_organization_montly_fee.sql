with organization_monthly_employee_count as (
    select 
      to_char(dat_schedule_order_credit, 'YYYYMM')  num_reference_month,
      fat.cod_id_organization,
      count(distinct fat.cod_id_employee) qty_employee_credited
    from {{ref('fact_order_credit_item')}} fat
    where fat.des_status_order_credit_item = 'Approved'
    group by 1,2
),

organization_monthly_employee_fee as (
    select 
      num_reference_month,
      cod_id_organization,
      qty_employee_credited,
      rules.fee as val_related_fee
    from  organization_monthly_employee_count a
    left join {{ref('revenue_rules')}} rules on 
      a.qty_employee_credited >= rules.min_employees and 
      (rules.max_employees is null or a.qty_employee_credited <= rules.max_employees)
)

select * from organization_monthly_employee_fee