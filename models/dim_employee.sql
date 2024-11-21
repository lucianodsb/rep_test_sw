with dim_employee as (
    select
     id_employee as cod_id_employee,
     organization_id as cod_id_organization,
     employee_gender as des_employee_gender,
     employee_mobile_app as des_employee_mobile_app
    from {{source('sources','employee')}}
)

select * from dim_employee