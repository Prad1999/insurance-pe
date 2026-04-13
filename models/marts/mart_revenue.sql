select *
from {{ ref('int_policy_revenue_summary') }}
where is_excluded = false