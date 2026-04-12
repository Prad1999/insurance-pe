with all_dates as (

    select created_date as calendar_date
    from {{ ref('fct_enquiries') }}

    union

    select created_date as calendar_date
    from {{ ref('fct_policies') }}

    union

    select policy_start_date as calendar_date
    from {{ ref('fct_policies') }}

    union

    select policy_end_date as calendar_date
    from {{ ref('fct_policies') }}

    union

    select transaction_date as calendar_date
    from {{ ref('fct_policy_transactions') }}

)

select distinct
    calendar_date,
    year(calendar_date) as calendar_year,
    month(calendar_date) as calendar_month,
    day(calendar_date) as calendar_day,
    quarter(calendar_date) as calendar_quarter,
    monthname(calendar_date) as month_name,
    dayname(calendar_date) as day_name

from all_dates
where calendar_date is not null