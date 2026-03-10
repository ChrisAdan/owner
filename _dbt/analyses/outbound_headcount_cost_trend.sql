-- analysis: outbound_headcount_cost_trend
--
-- purpose: verify the README claim that outbound headcount cost grew 31%
-- from January to June 2024 ($38,076 → $50,000) while CAC increased 4x.
--
-- sources salary_and_commissions_usd directly from int_expenses__by_channel_month,
-- which is the authoritative cost source upstream of mart_cac_ltv.
-- joins to mart_cac_ltv for won count and CAC to show the cost/yield divergence
-- side by side.
--
-- run with: dbt compile --select outbound_headcount_cost_trend

with expenses as (
    select
        month_date,
        salary_and_commissions_usd                          as headcount_cost_usd,
        total_cost_usd
    from {{ ref('int_expenses__by_channel_month') }}
    where channel = 'outbound'
),

cac as (
    select
        month_date,
        new_customers_won,
        cac_usd,
        cac_ltv_ratio
    from {{ ref('mart_cac_ltv') }}
    where channel = 'outbound'
),

first_month as (
    select min(month_date) as first_month from expenses
)

select
    e.month_date,
    e.headcount_cost_usd,
    e.total_cost_usd,
    c.new_customers_won,
    c.cac_usd,
    c.cac_ltv_ratio,
    round(
        (e.headcount_cost_usd
            / first_value(e.headcount_cost_usd)
                over (order by e.month_date)
            - 1) * 100,
        1
    )                                                       as headcount_cost_growth_pct
from expenses e
join cac c
    on e.month_date = c.month_date
order by e.month_date