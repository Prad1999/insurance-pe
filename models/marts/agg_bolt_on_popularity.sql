select
    product_code,
    count(*) as bolt_on_event_count,
    count(distinct policy_id) as policy_attach_count,
    sum(net_amount) as total_bolt_on_net_revenue,
    sum(gross_amount) as total_bolt_on_gross_revenue
from {{ ref('int_policy_transactions') }}
where transaction_event_type = 'BOLT_ON'
group by product_code
order by policy_attach_count desc, bolt_on_event_count desc