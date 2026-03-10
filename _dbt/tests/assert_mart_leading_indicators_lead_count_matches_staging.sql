-- test: assert_mart_leading_indicators_lead_count_matches_staging
--
-- the mart is lead-grain and should contain one row per lead in staging.
-- a count mismatch means either the incremental watermark missed leads,
-- a join filtered rows unexpectedly, or duplicates were introduced.
--
-- returns one row with the delta when counts differ — test passes when zero rows returned.

with staging_count as (
    select count(*) as n from {{ ref('stg_gtm__leads') }}
),

mart_count as (
    select count(*) as n from {{ ref('mart_leading_indicators') }}
)

select
    staging_count.n     as staging_leads,
    mart_count.n        as mart_leads,
    mart_count.n - staging_count.n as delta
from staging_count
cross join mart_count
where staging_count.n != mart_count.n