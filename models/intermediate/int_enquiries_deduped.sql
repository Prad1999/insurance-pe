with deduped as (

    select distinct *
    from {{ ref('stg_enquiries') }}

)

select
    enquiry_version_key,
    enquiry_key,
    utc_created,
    created_date,
    created_timestamp,
    business_type,
    transaction_type,
    transaction_source,
    insurance_type,
    previous_policy_id,
    origin,
    is_quoted,
    cheapest_quote_key,
    cheapest_product_code,
    cheapest_net_premium,
    cheapest_gross_premium,
    is_excluded,
    exclusion_category,
    enquiry_type,
    risk_key,
    risk_postcode

from deduped