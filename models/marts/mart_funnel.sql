with enquiries as (

    select *
    from {{ ref('fct_enquiries') }}
    where is_quoted = true

),

policies as (

    select *
    from {{ ref('fct_policies') }}

)

select
    e.enquiry_version_key,
    e.enquiry_key,
    e.cheapest_quote_key as quote_key,

    e.created_date as enquiry_date,
    e.business_type,
    e.transaction_type,
    e.transaction_source,
    e.insurance_type,
    e.origin,
    e.enquiry_type,

    e.cheapest_product_code as quoted_product_code,
    e.cheapest_net_premium as quoted_net_premium,
    e.cheapest_gross_premium as quoted_gross_premium,

    p.policy_id,
    p.policy_number,

    case
        when p.policy_id is not null then true
        else false
    end as is_converted,

    p.created_date as policy_created_date,
    p.policy_start_date,
    p.policy_end_date,

    p.product_code as sold_product_code,
    p.net_premium as sold_net_premium,
    p.gross_premium as sold_gross_premium

from enquiries e
left join policies p
    on e.enquiry_version_key = p.enquiry_version_key