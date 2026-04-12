select 
    policy_id,
    enquiry_version_key,
    enquiry_key,
    quote_key,

    utc_created,
    time_zone,
    date as created_date,
    created as created_timestamp,

    start_date as policy_start_date,
    end_date as policy_end_date,
    adjustment_effective_date,

    transaction_source,
    replace(insurance_type, ' ', '_') as insurance_type,
    endorsements,
    currency_code,

    adjustment_gross as adjustment_gross_premium,
    adjustment_net as adjustment_net_premium,
    adjustment_commission_amount,
    adjustment_commission_rate,
    adjustment_tax_amount,
    adjustment_tax_rate,

    annualised_gross as annualised_gross_premium,
    annualised_net as annualised_net_premium,
    annualised_commission_amount,
    annualised_commission_load,
    annualised_tax_amount,

    quoted_adjustment_gross,
    quoted_adjustment_net,
    quoted_adjustment_commission_amount,
    quoted_adjustment_tax_amount,

    case 
        when excluded = 1 then true 
        else false
    end as is_excluded,

    case 
        when rated = 1 then true 
        else false
    end as is_rated,

    order_version_key,

    discount_gross,
    discount_commission_amount,
    discount_tax_amount,
    max_discount_percentage,
    max_discount_amount,
    discounts_already_applied,
    max_discount_available,

    case 
        when discount_applied = 1 then true 
        else false 
    end as is_discount_applied 

from {{ source('raw', 'mtas') }}