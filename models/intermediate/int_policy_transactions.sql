with valid_policies as (

    select *
    from {{ ref('stg_policies') }}

),

mtas as (

    select
        p.policy_id,
        p.policy_number,
        'MTA' as transaction_event_type,
        m.adjustment_effective_date as transaction_date,
        m.created_timestamp as transaction_timestamp,
        cast(null as varchar) as product_code,
        cast(null as varchar) as cancellation_reason,
        m.currency_code,
        m.adjustment_net_premium as net_amount,
        m.adjustment_gross_premium as gross_amount,
        m.adjustment_commission_amount as commission_amount,
        m.adjustment_tax_amount as tax_amount
    from {{ ref('stg_mtas') }} m
    inner join valid_policies p
        on m.policy_id = p.policy_id
    where m.is_excluded = false
      and m.is_rated = true

),

bolt_ons as (

    select
        p.policy_id,
        p.policy_number,
        'BOLT_ON' as transaction_event_type,
        b.bolt_on_start_date as transaction_date,
        b.created_timestamp as transaction_timestamp,
        b.product_code,
        cast(null as varchar) as cancellation_reason,
        b.currency_code,
        b.net_premium as net_amount,
        b.gross_premium as gross_amount,
        b.commission_amount,
        b.tax_amount
    from {{ ref('stg_bolt_ons') }} b
    inner join valid_policies p
        on b.policy_number = p.policy_number
    where b.is_excluded = false

),

cancellations as (

    select
        p.policy_id,
        p.policy_number,
        'CANCELLATION' as transaction_event_type,
        c.cancellation_effective_date as transaction_date,
        c.created_timestamp as transaction_timestamp,
        c.product_code,
        c.cancellation_reason,
        c.currency_code,
        -c.net_premium as net_amount,
        -c.gross_premium as gross_amount,
        -c.commission_amount as commission_amount,
        -c.tax_amount as tax_amount
    from {{ ref('stg_cancellations') }} c
    inner join valid_policies p
        on c.policy_id = p.policy_id
    where c.is_excluded = false

)

select * from mtas
union all
select * from bolt_ons
union all
select * from cancellations