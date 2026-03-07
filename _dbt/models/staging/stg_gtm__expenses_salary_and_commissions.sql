-- grain: one row per calendar month per sales channel
-- natural key: month_date + sales_channel
-- source: gtm_case.expenses_salary_and_commissions
-- note: source is wide (one row per month, two channel columns)
--       unpivoted here to long format (one row per month per channel)

{{
    config(
        contract={
            "enforced": true
        }
    )
}}

with source as (
    select * from {{ source('gtm_case', 'expenses_salary_and_commissions') }}
),

cleaned as (
    select
        to_date(
            '01-' || month,
            'DD-Mon-YY'
        )                                                               as month_date,
        replace(
            regexp_replace(outbound_sales_team, '[^0-9,]', '', 'g'),
            ',', '.'
        )::numeric(12, 2)                                              as outbound_cost_usd,

        replace(
            regexp_replace(inbound_sales_team, '[^0-9,]', '', 'g'),
            ',', '.'
        )::numeric(12, 2)                                              as inbound_cost_usd

    from source
),

unpivoted as (
    select month_date, 'outbound' as sales_channel, outbound_cost_usd as salary_and_commissions_usd
    from cleaned
    union all
    select month_date, 'inbound' as sales_channel, inbound_cost_usd as salary_and_commissions_usd
    from cleaned
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['month_date', 'sales_channel']) }}  as expense_salary_sk,
        month_date,
        sales_channel,
        salary_and_commissions_usd
    from unpivoted
)

select * from final