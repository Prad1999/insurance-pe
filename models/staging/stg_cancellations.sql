select 
    policy_id,
    policy_number,
    order_key,
    payment_key,

    utc_created,
    date as created_date,
    created as created_timestamp,

    business_type,
    transaction_type,
    origin,
    product_code,

    effective_date_time_zone,
    effective_date as cancellation_effective_date,

    time_on_risk,
    to_inception,
    reason as cancellation_reason,

    case
        when claimed = 1 then true 
        else false 
    end as has_claimed,

    case 
        when in_cooling_off_period = 1 then true 
        else false 
    end as is_in_cooling_off_period,

    cooling_off_period,

    currency_code,

    gross as gross_premium,
    net as net_premium,
    commission_amount,
    tax_amount,
    commission_load,
    cancellation_fee,

    case 
        when excluded = 1 then true 
        else false 
    end as is_excluded,

    case 
        when is_fee_excluded = 1 then true 
        else false 
    end as is_fee_excluded, 

    maximum_cancellation_fee,
    waive_fee_reason

from {{ source('raw', 'cancellations') }}

