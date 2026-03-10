-- grain: one row per calendar month
-- natural key: month_date
-- source: gtm_case.expenses_advertising

{{
    config(
        contract={
            "enforced": true
        }
    )
}}

with source as (
    select * from {{ source('gtm_case', 'expenses_advertising') }}
),

cleaned as (
    select
        -- month: parse 'Jan-24' → '2024-01-01'
        to_date(
            '01-' || month,
            'DD-Mon-YY'
        )                                                           as month_date,

        -- advertising spend: strip 'US$', spaces, replace comma decimal → numeric
        replace(
            regexp_replace(advertising, '[^0-9,]', '', 'g'),
            ',', '.'
        )::numeric(12, 2)                       as advertising_spend_usd

            from source
        ),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['month_date']) }}      as expense_advertising_sk,
        month_date,
        advertising_spend_usd
    from cleaned
)

select * from final