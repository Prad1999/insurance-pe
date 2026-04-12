with base as (

    select *
    from {{ ref('stg_policies') }}

)

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
    created_date,
    created_timestamp,
    business_type,
    transaction_type,
    transaction_source,
    insurance_type,
    origin,
    product_code,
    product_data_key,
    endorsements,
    policy_start_date,
    policy_end_date,
    currency_code,
    gross_premium,
    net_premium,
    commission_amount,
    commission_rate,
    tax_amount,
    tax_rate,
    quoted_gross_premium,
    quoted_net_premium,
    quoted_commission_amount,
    quoted_tax_amount,
    commission_discount,
    max_commission_discount,
    max_commission_discount_rate,
    commission_load,
    payment_frequency,
    is_excluded,
    risk_key,
    risk_postcode

from base