-- grain: one row per sales channel per calendar month
-- natural key: month_date + channel
-- upstream: stg_gtm__expenses_advertising, stg_gtm__expenses_salary_and_commissions
--
-- modeling decisions:
--   - advertising spend attributed exclusively to inbound. all paid advertising
--     (Facebook, Google) drives inbound form submissions — outbound BDR activity
--     generates no ad spend, only headcount cost. follows directly from the business model.
--   - salary costs already split by channel in source, require no allocation.
--   - total_cost_usd is the authoritative CAC cost denominator per channel per month.
--
-- trade-offs:
--   - advertising cannot be split within inbound (e.g. Facebook vs Google) —
--     source provides monthly aggregate only. noted as a data gap.
--   - 6 months of data (Jan–Jun 2024) limits trend analysis.
--
-- type notes:
--   - numeric(12,2) + numeric(12,2) produces numeric(13,2) in postgres.
--     total_cost_usd cast to numeric(12,2) at point of introduction.
--     advertising_spend_usd coalesced to 0 for outbound rows before summing.

{{
    config(
        materialized='view',
        contract={"enforced": true}
    )
}}

with salary as (
    select * from {{ ref('stg_gtm__expenses_salary_and_commissions') }}
),

advertising as (
    select * from {{ ref('stg_gtm__expenses_advertising') }}
),

joined as (
    select
        s.month_date,
        s.sales_channel                                                 as channel,
        s.salary_and_commissions_usd,

        -- advertising attributed to inbound only; outbound receives 0
        case
            when s.sales_channel = 'inbound'
            then coalesce(a.advertising_spend_usd, 0)
            else 0::numeric(12,2)
        end                                                             as advertising_spend_usd,

        -- numeric(12,2) + numeric(12,2) = numeric(13,2) in postgres
        -- cast to numeric(12,2) here — point of introduction for this field
        (
            s.salary_and_commissions_usd
            + case
                when s.sales_channel = 'inbound'
                then coalesce(a.advertising_spend_usd, 0)
                else 0::numeric(12,2)
              end
        )::numeric(12,2)                                               as total_cost_usd

    from salary s
    left join advertising a
        on s.month_date = a.month_date
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['month_date', 'channel']) }}   as expense_channel_month_sk,
        month_date,
        channel,
        salary_and_commissions_usd,
        advertising_spend_usd,
        total_cost_usd
    from joined
)

select * from final