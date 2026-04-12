select
    policy_id,
    policy_number,
    business_type,
    transaction_type,
    transaction_source,
    insurance_type,
    origin,
    product_code,
    product_data_key,
    endorsements,
    payment_frequency,
    currency_code,
    risk_key,
    risk_postcode

from {{ ref('fct_policies') }}