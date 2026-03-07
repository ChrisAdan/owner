-- grain: one row per lead (restaurant prospect)
-- natural key: lead_sk
-- upstream: int_leads__enriched
--
-- modeling decisions:
--   - grain is lead not account. owner's data has no deduplicated account dimension —
--     account_id only appears on converted leads via opportunities. using lead as grain
--     preserves the full prospect universe including unconverted leads.
--   - marketplace_count: derived by counting comma-separated values in
--     marketplaces_used_cleaned. proxy for 3p platform dependency and motivation
--     to consolidate onto a 1p solution.
--   - has_olo_tool: boolean derived from olo_tools_cleaned being non-null.
--     restaurants with existing olo tools are already aware of 1p ordering —
--     different sales motion vs pure 3p-dependent restaurants.
--   - estimated_annual_ltv_usd: owner revenue model has two components:
--       1. $500/month subscription = $6,000/year
--       2. 5% take rate on online gmv = predicted_monthly_gmv_usd * 0.05 * 12
--     combined: predicted_monthly_gmv_usd * 0.6 + 6000
--     null where predicted_monthly_gmv_usd is null (8% of leads).
--     this is a proxy — actual ltv depends on realized gmv, not predicted.
--
-- trade-offs:
--   - predicted_monthly_gmv_usd is an estimate from the lead record, not observed revenue.
--     ltv figures should be interpreted as expected value for prioritization,
--     not as financial forecasts.
--   - cuisine_types and marketplace lists are not normalized/deduplicated here —
--     bridge table explosion deferred as out of scope for this case study.

{{
    config(
        materialized='ephemeral'
    )
}}

with enriched as (
    select * from {{ ref('int_leads__enriched') }}
),

final as (
    select
        lead_sk,
        lead_id,
        channel,
        is_converted,
        is_won,

        -- firmographics
        location_count,
        cuisine_types_cleaned,

        -- marketplace presence
        case
            when marketplaces_used_cleaned is null then 0
            else array_length(
                string_to_array(marketplaces_used_cleaned, ','),
                1
            )
        end                                                             as marketplace_count,

        -- olo tool flag: competitive displacement signal
        case
            when olo_tools_cleaned is not null then true
            else false
        end                                                             as has_olo_tool,

        -- gmv and ltv
        predicted_monthly_gmv_usd,

        -- ltv: $500/mo subscription + 5% take rate on predicted gmv, annualized
        case
            when predicted_monthly_gmv_usd is not null
            then round(
                (predicted_monthly_gmv_usd * 0.05 * 12) + (500 * 12),
                2
            )
        end                                                             as estimated_annual_ltv_usd

    from enriched
)

select * from final