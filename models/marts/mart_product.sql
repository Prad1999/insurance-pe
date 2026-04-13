select *
from {{ ref('int_policy_bolt_on_summary') }}
where is_excluded = false