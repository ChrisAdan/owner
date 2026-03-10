-- test: assert_mart_cac_ltv_two_channels_per_month
--
-- every month in mart_cac_ltv must have exactly one inbound row and one
-- outbound row. a missing channel means an upstream join dropped data
-- (e.g. expenses missing a channel, or funnel producing no leads for a month).
--
-- returns rows that violate the assertion — test passes when zero rows returned.

select
    month_date,
    count(*)                as channel_count,
    array_agg(channel)      as channels_present
from {{ ref('mart_cac_ltv') }}
group by month_date
having count(*) != 2