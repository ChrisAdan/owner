-- grain: one row per channel per calendar month
-- natural key: channel + month_date
-- upstream: int_funnel__conversion_rates, int_expenses__by_channel_month,
--           int_restaurant__profile, int_leads__enriched
--
-- modeling decisions:
--   - scoped to jan–jun 2024 via inner join to expenses.
--     expense data only covers this window — joining on month_date naturally
--     limits cac to months where cost data exists. full funnel history
--     is available in mart_gtm_funnel without this constraint.
--   - cac = total_cost_usd / new_customers_won. null-safe via safe_divide.
--   - avg ltv metrics are scoped to won leads only, attributed to the
--     cohort month using the same channel-aware derivation as the funnel model
--     (inbound: form_submission_date, outbound: first_sales_call_at).
--   - cac_ltv_ratio = avg_estimated_ltv_usd / cac_usd. values > 3 indicate
--     healthy saas/subscription unit economics.
--
-- trade-offs:
--   - ltv is estimated from predicted_monthly_gmv_usd, not realized revenue.
--     treat as a directional prioritization signal, not financial actuals.
--   - cohort attribution means recent months undercount closed_won — leads
--     created in may/jun 2024 may not have had time to close within the
--     expense window. interpret late-window cac with caution.
--
-- type notes:
--   - closed_won cast to ::numeric before division — postgres count() returns bigint,
--     and bigint/bigint = bigint integer division, which overflows in downstream expressions.
--   - cac_ltv_ratio computed in a separate cte (with_ratio) to avoid nesting safe_divide,
--     which produces unpredictable type inference in postgres.

{{
    config(
        materialized='table',
        contract={"enforced": true}
    )
}}

with funnel as (
    select * from {{ ref('int_funnel__conversion_rates') }}
),

expenses as (
    select * from {{ ref('int_expenses__by_channel_month') }}
),

leads_enriched as (
    select * from {{ ref('int_leads__enriched') }}
),

restaurant as (
    select * from {{ ref('int_restaurant__profile') }}
),

won_leads_monthly as (
    select
        r.predicted_monthly_gmv_usd,
        r.estimated_annual_ltv_usd,
        l.channel,
        case
            when l.channel = 'inbound'
            then date_trunc('month', l.form_submission_date::timestamp)::date
            else date_trunc('month', l.first_sales_call_at)::date
        end                                                             as lead_month
    from restaurant r
    inner join leads_enriched l
        on r.lead_sk = l.lead_sk
    where r.is_won = true
),

won_ltv_by_month as (
    select
        lead_month                                                      as month_date,
        channel,
        avg(predicted_monthly_gmv_usd)                                  as avg_predicted_monthly_gmv_usd,
        avg(estimated_annual_ltv_usd)                                   as avg_estimated_ltv_usd
    from won_leads_monthly
    group by 1, 2
),

joined as (
    select
        f.month_date,
        f.channel,
        e.total_cost_usd,
        f.closed_won                                                    as new_customers_won,
        {{ dbt_utils.safe_divide('e.total_cost_usd', 'f.closed_won::numeric') }}
                                                                        as cac_usd,
        w.avg_predicted_monthly_gmv_usd,
        w.avg_estimated_ltv_usd
    from funnel f
    inner join expenses e
        on  f.month_date = e.month_date
        and f.channel    = e.channel
    left join won_ltv_by_month w
        on  f.month_date = w.month_date
        and f.channel    = w.channel
),

-- separate cte avoids nesting safe_divide; referencing cac_usd directly
with_ratio as (
    select
        *,
        {{ dbt_utils.safe_divide('avg_estimated_ltv_usd', 'cac_usd') }} as cac_ltv_ratio
    from joined
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['month_date', 'channel']) }}   as cac_ltv_sk,
        month_date,
        channel,
        total_cost_usd,
        new_customers_won,
        cac_usd,
        avg_predicted_monthly_gmv_usd,
        avg_estimated_ltv_usd,
        cac_ltv_ratio
    from with_ratio
)

select * from final