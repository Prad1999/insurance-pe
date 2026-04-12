select 
    policy_id,
    policy_number,

    enquiry_version_key,
    enquiry_key,
    quote_key,
    order_key,
    payment_key,

    utc_created,
    time_zone,
    date as created_date,
    created as created_timestamp,

    business_type,
    transaction_type,
    transaction_source,
    replace(insurance_type, ' ', '_') as insurance_type,
    origin,
    product_code,
    product_data_key,
    endorsements,

    start_date as policy_start_date,
    end_date as policy_end_date,

    currency_code,

    gross as gross_premium,
    net as net_premium,
    commission_amount,
    commission_rate,
    tax_amount,
    tax_rate,

    quoted_gross as quoted_gross_premium,
    quoted_net as quoted_net_premium,
    quoted_commission_amount,
    quoted_tax_amount,

    commission_discount,
    max_commission_discount,
    max_commission_discount_rate,
    commission_load,

    payment_frequency,

    case 
        when excluded = 1 then true 
        else false 
    end as is_excluded,

    risk_key,
    upper(trim(risk_postcode)) as risk_postcode 

from {{ source('raw', 'policies') }}