with dim_organization as (
    select
     organization_id as cod_id_organization,
     organization_name as nam_organization,
     organization_industry as des_organization_industry
    from {{source('sources','organization')}}
)

select * from dim_organization