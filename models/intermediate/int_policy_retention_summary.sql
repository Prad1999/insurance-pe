with policies as (

    select *
    from {{ ref('stg_policies') }}

),

cancellations as (

    select
        policy_id,
        min(transaction_date) as cancellation_date
    from {{ ref('int_policy_transactions') }}
    where transaction_event_type = 'CANCELLATION'
    group by policy_id

)

select
    p.policy_id,
    p.policy_number,
    p.created_date,
    p.policy_start_date,
    p.policy_end_date,
    p.business_type,
    p.transaction_type,
    p.transaction_source,
    p.insurance_type,
    p.origin,
    p.product_code,
    p.payment_frequency,
    p.is_excluded,
    case
        when c.policy_id is not null then true
        else false
    end as is_cancelled,
    c.cancellation_date,
    datediff(day, p.policy_start_date, c.cancellation_date) as days_to_cancellation
from policies p
left join cancellations c
    on p.policy_id = c.policy_id