with policies as (

    select *
    from {{ ref('fct_policies') }}

),

bolt_on_summary as (

    select
        policy_id,
        count(*) as bolt_on_count,
        count(distinct product_code) as distinct_bolt_on_product_count
    from {{ ref('fct_policy_transactions') }}
    where transaction_event_type = 'BOLT_ON'
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

    case
        when b.policy_id is not null then true
        else false
    end as has_bolt_on,

    coalesce(b.bolt_on_count, 0) as bolt_on_count,
    coalesce(b.distinct_bolt_on_product_count, 0) as distinct_bolt_on_product_count

from policies p
left join bolt_on_summary b
    on p.policy_id = b.policy_id