select 
    enquiry_version_key,
    enquiry_key,

    utc_created,
    date as created_date,
    created as created_timestamp,

    business_type,
    transaction_type,
    transaction_source,
    replace(insurance_type, ' ', '_') as insurance_type,

    previous_policy_id,
    origin,

    case
        when quoted = 1 then true
        else false 
    end as is_quoted,

    cheapest_quote_key,
    cheapest_product_code,
    cheapest_net as cheapest_net_premium,
    cheapest_gross as cheapest_gross_premium,

    case 
        when excluded = 1 then true 
        else false 
    end as is_excluded,

    exclusion_category,
    enquiry_type,

    risk_key,
    upper(trim(risk_postcode)) as risk_postcode 

from {{ source('raw', 'enquiries') }}