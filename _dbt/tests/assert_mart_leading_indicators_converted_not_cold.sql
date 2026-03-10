-- test: assert_mart_leading_indicators_converted_not_cold
--
-- a lead that actually converted should not be scored cold. cold tier
-- represents ~1.6% conversion rate — if a converted lead lands there,
-- the tier logic is not capturing the signal that caused the conversion.
-- this is a data quality signal, not a hard correctness failure, but
-- systematic violations indicate the tier thresholds need revisiting.
--
-- note: converted leads that are also disqualified have null tier and are
-- excluded — disqualification takes precedence.
--
-- returns violating rows — test passes when zero rows returned.

select
    lead_id,
    lead_sk,
    channel,
    is_converted,
    is_disqualified,
    connected_with_decision_maker,
    speed_to_first_contact_hours,
    total_activity_count,
    predicted_monthly_gmv_usd,
    conversion_probability_tier
from {{ ref('mart_leading_indicators') }}
where
    is_converted = true
    and is_disqualified is not true
    and conversion_probability_tier != 'converted'