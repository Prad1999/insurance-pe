select 
    policy_id,
    policy_number,
    order_key,
    payment_key,

    utc_created,
    time_zone,
    date as created_date,
    created as created_timestamp,

    business_line,
    business_type,
    transaction_type,
    transaction_source,

    product_code,

    start_date as bolt_on_start_date,
    end_date as bolt_on_end_date,

    currency_code,

    gross as gross_premium,
    net as net_premium,
    commission_amount,
    commission_rate,
    tax_amount,
    tax_rate,

    case 
        when excluded = 1 then true
        else false
    end as is_excluded,

    enquiry_key,
    enquiry_version_key,

    case
        when adjustment = 1 then true 
        else false
    end as is_adjustment

from {{ source('raw', 'bolt_ons') }}