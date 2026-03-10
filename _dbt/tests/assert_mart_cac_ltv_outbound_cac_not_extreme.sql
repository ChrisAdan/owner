-- test: assert_mart_cac_ltv_outbound_cac_not_extreme
--
-- flags months where outbound CAC exceeds inbound CAC by more than 10x.
-- a 10x gap is not a hard business rule but indicates either a data
-- anomaly (e.g. near-zero outbound wins inflating the ratio) or a
-- channel efficiency collapse worth surfacing explicitly.
--
-- june 2024 outbound CAC ($6,250) vs inbound ($1,500) = ~4x — within bounds.
-- if the ratio ever exceeds 10x, this test will surface it for review.
--
-- returns violating months — test passes when zero rows returned.

with pivoted as (
    select
        month_date,
        max(case when channel = 'inbound'  then cac_usd end)   as inbound_cac,
        max(case when channel = 'outbound' then cac_usd end)   as outbound_cac
    from {{ ref('mart_cac_ltv') }}
    where cac_usd is not null
    group by month_date
)

select
    month_date,
    inbound_cac,
    outbound_cac,
    round(outbound_cac / nullif(inbound_cac, 0), 2)    as cac_ratio
from pivoted
where outbound_cac / nullif(inbound_cac, 0) > 10