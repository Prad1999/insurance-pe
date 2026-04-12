with policies as (

    select *
    from {{ ref('fct_policies') }}
    where is_excluded = false

),

transaction_summary as (

    select
        policy_id,

        sum(case when transaction_event_type = 'MTA' then net_amount else 0 end) as total_mta_net_amount,
        sum(case when transaction_event_type = 'MTA' then gross_amount else 0 end) as total_mta_gross_amount,

        sum(case when transaction_event_type = 'BOLT_ON' then net_amount else 0 end) as total_bolt_on_net_amount,
        sum(case when transaction_event_type = 'BOLT_ON' then gross_amount else 0 end) as total_bolt_on_gross_amount,

        sum(case when transaction_event_type = 'CANCELLATION' then net_amount else 0 end) as total_cancellation_net_amount,
        sum(case when transaction_event_type = 'CANCELLATION' then gross_amount else 0 end) as total_cancellation_gross_amount

    from {{ ref('fct_policy_transactions') }}
    group by policy_id

)

select
    p.policy_id,
    p.policy_number,
    p.created_date as policy_created_date,
    p.policy_start_date,
    p.policy_end_date,

    p.business_type,
    p.transaction_type,
    p.transaction_source,
    p.insurance_type,
    p.origin,
    p.product_code,
    p.payment_frequency,
    p.currency_code,

    p.net_premium as written_net_premium,
    p.gross_premium as written_gross_premium,
    p.commission_amount as written_commission_amount,
    p.tax_amount as written_tax_amount,

    coalesce(t.total_bolt_on_net_amount, 0) as bolt_on_net_revenue,
    coalesce(t.total_bolt_on_gross_amount, 0) as bolt_on_gross_revenue,

    coalesce(t.total_mta_net_amount, 0) as mta_net_adjustment,
    coalesce(t.total_mta_gross_amount, 0) as mta_gross_adjustment,

    coalesce(t.total_cancellation_net_amount, 0) as cancellation_net_amount,
    coalesce(t.total_cancellation_gross_amount, 0) as cancellation_gross_amount,

    p.net_premium + coalesce(t.total_mta_net_amount, 0) as net_premium_after_mta,
    p.gross_premium + coalesce(t.total_mta_gross_amount, 0) as gross_premium_after_mta

from policies p
left join transaction_summary t
    on p.policy_id = t.policy_id