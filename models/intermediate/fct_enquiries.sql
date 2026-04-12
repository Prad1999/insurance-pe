with base as (

    select *
    from {{ ref('int_enquiry_versions') }}
    where is_excluded = false

)

select *
from base