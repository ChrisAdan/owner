-- grain: one row per channel per calendar month
-- natural key: channel + month_date
-- upstream: stg_gtm__expenses_advertising, stg_gtm__expenses_salary_and_commissions
--
-- modeling decisions:
--   - advertising spend is attributed exclusively to inbound channel.
--     rationale: all paid advertising (Facebook, Google) drives inbound form submissions.
--     outbound (BDR cold outreach) generates no ad spend — its only cost is headcount.
--     attributing advertising to inbound is not an assumption; it follows directly from
--     the business model described in the brief.
--   - salary and commissions are already split by channel in the source (inbound/outbound).
--     no allocation required — the source table is the authority.
--   - total_cost_usd = advertising_spend_usd (inbound only) + salary_and_commissions_usd
--     this is the correct CAC denominator per channel per month.
--
-- trade-offs:
--   - advertising spend cannot be further split within inbound (e.g. Facebook vs Google)
--     as the source only provides a monthly aggregate. noted as a data gap.
--   - 6 months of data (Jan–Jun 2024) limits trend analysis. cohort conclusions
--     should be treated as directional, not statistically definitive.

{{
    config(
        materialized='ephemeral'
    )
}}

with salary as (
    select
        month_date,
        sales_channel                           as channel,
        salary_and_commissions_usd
    from {{ ref('stg_gtm__expenses_salary_and_commissions') }}
),

advertising as (
    select
        month_date,
        'inbound'                               as channel,
        advertising_spend_usd
    from {{ ref('stg_gtm__expenses_advertising') }}
),

-- left join: outbound rows will have null advertising spend (outbound has no ad cost)
joined as (
    select
        s.month_date,
        s.channel,
        s.salary_and_commissions_usd,
        coalesce(a.advertising_spend_usd, 0)    as advertising_spend_usd
    from salary s
    left join advertising a
        on  s.month_date = a.month_date
        and s.channel    = a.channel
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['month_date', 'channel']) }}   as expense_channel_month_sk,
        month_date,
        channel,
        salary_and_commissions_usd,
        advertising_spend_usd,
        salary_and_commissions_usd
            + advertising_spend_usd                                         as total_cost_usd
    from joined
)

select * from final