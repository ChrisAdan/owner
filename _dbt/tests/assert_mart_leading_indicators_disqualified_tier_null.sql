-- test: assert_mart_leading_indicators_disqualified_tier_null
--
-- disqualified leads are excluded from scoring by design — their
-- conversion_probability_tier must always be null. a non-null tier on a
-- disqualified lead means the is_disqualified flag and the tier CASE
-- expression have drifted out of sync.
--
-- returns violating rows — test passes when zero rows returned.

select
    lead_id,
    lead_sk,
    status,
    is_disqualified,
    conversion_probability_tier
from {{ ref('mart_leading_indicators') }}
where
    is_disqualified = true
    and conversion_probability_tier is not null