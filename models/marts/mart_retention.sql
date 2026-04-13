select *
from {{ ref('int_policy_retention_summary') }}
where is_excluded = false