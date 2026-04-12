with mtas as (

    select
        policy_id,
        enquiry_version_key,
        enquiry_key,
        quote_key,
        order_version_key as transaction_reference,
        'MTA' as transaction_event_type,
        adjustment_effective_date as transaction_date,
        created_timestamp as transaction_timestamp,
        cast(null as varchar) as product_code,
        cast(null as varchar) as cancellation_reason,
        currency_code,
        adjustment_net_premium as net_amount,
        adjustment_gross_premium as gross_amount,
        adjustment_commission_amount as commission_amount,
        adjustment_tax_amount as tax_amount,
        is_rated,
        is_discount_applied
    from {{ ref('stg_mtas') }}
    where is_excluded = false
      and is_rated = true

),

bolt_ons as (

    select
        p.policy_id,
        b.enquiry_version_key,
        b.enquiry_key,
        cast(null as number) as quote_key,
        b.order_key as transaction_reference,
        'BOLT_ON' as transaction_event_type,
        b.bolt_on_start_date as transaction_date,
        b.created_timestamp as transaction_timestamp,
        b.product_code,
        cast(null as varchar) as cancellation_reason,
        b.currency_code,
        b.net_premium as net_amount,
        b.gross_premium as gross_amount,
        b.commission_amount,
        b.tax_amount,
        cast(null as boolean) as is_rated,
        cast(null as boolean) as is_discount_applied
    from {{ ref('stg_bolt_ons') }} b
    left join {{ ref('fct_policies') }} p
      on b.policy_number = p.policy_number
    where b.is_excluded = false

),

cancellations as (

    select
        policy_id,
        cast(null as number) as enquiry_version_key,
        cast(null as number) as enquiry_key,
        cast(null as number) as quote_key,
        order_key as transaction_reference,
        'CANCELLATION' as transaction_event_type,
        cancellation_effective_date as transaction_date,
        created_timestamp as transaction_timestamp,
        product_code,
        cancellation_reason,
        currency_code,
        -net_premium as net_amount,
        -gross_premium as gross_amount,
        -commission_amount as commission_amount,
        -tax_amount as tax_amount,
        cast(null as boolean) as is_rated,
        cast(null as boolean) as is_discount_applied
    from {{ ref('stg_cancellations') }}
    where is_excluded = false

)

select * from mtas
union all
select * from bolt_ons
union all
select * from cancellations