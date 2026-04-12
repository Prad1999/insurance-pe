with products as (

    select distinct product_code
    from {{ ref('stg_policies') }}

    union

    select distinct product_code
    from {{ ref('stg_bolt_ons') }}

    union

    select distinct cheapest_product_code as product_code
    from {{ ref('stg_enquiries') }}
    where cheapest_product_code is not null

)

select
    product_code,
    case
        when product_code in ('key_cover', 'legal', 'home_emergency') then 'bolt_on'
        when product_code like 'qgl%' then 'core_policy'
        else 'other'
    end as product_category

from products