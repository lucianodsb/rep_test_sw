with dim_wallet as (
    select
     wallet_id as cod_id_wallet,
     name as nam_wallet,
     description as des_wallet
    from {{source('sources','wallet')}}
)

select * from dim_wallet