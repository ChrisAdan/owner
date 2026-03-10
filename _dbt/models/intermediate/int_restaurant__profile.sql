-- grain: one row per lead (restaurant prospect)
-- natural key: lead_sk
-- upstream: int_leads__enriched
--
-- modeling decisions:
--   - grain is lead not account. account_id only appears on converted leads via
--     opportunities. using lead grain preserves the full prospect universe.
--   - marketplace_count: count of comma-separated values in marketplaces_used_cleaned.
--     proxy for 3p platform dependency and motivation to consolidate to 1p.
--   - has_olo_tool: boolean from olo_tools_cleaned being non-null.
--     different sales motion vs pure 3p-dependent restaurants.
--   - estimated_annual_ltv_usd: owner revenue model:
--       $500/month subscription = $6,000/year
--       5% take rate on online gmv = predicted_monthly_gmv_usd * 0.05 * 12
--     combined: predicted_monthly_gmv_usd * 0.6 + 6000
--     null where predicted_monthly_gmv_usd is null (8% of leads).
--
-- trade-offs:
--   - predicted_monthly_gmv_usd is an estimate, not observed revenue.
--     ltv figures are expected value for prioritization, not financial forecasts.
--   - cuisine_types and marketplace lists not normalized here —
--     bridge table explosion deferred as out of scope for this case study.
--
-- type notes:
--   - round(numeric, int) returns numeric with unspecified precision in postgres.
--     estimated_annual_ltv_usd cast to numeric(12,2) at point of introduction.
--   - array_length() returns integer — marketplace_count correctly typed as integer.
--   - snowflake note: array_length(string_to_array(col, ','), 1) is postgres syntax.
--     switch to array_size(split(col, ',')) when promoting to snowflake adapter.

{{
    config(
        materialized='view',
        contract={"enforced": true}
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

        -- firmographics passed through
        location_count,
        cuisine_types_cleaned,

        -- marketplace count: number of 3p delivery platforms in use
        -- array_length returns integer — correctly typed
        case
            when marketplaces_used_cleaned is null then 0
            else array_length(
                string_to_array(marketplaces_used_cleaned, ','),
                1
            )
        end                                                             as marketplace_count,

        -- olo tool flag
        case
            when olo_tools_cleaned is not null then true
            else false
        end                                                             as has_olo_tool,

        -- gmv passed through (numeric(12,2) from staging)
        predicted_monthly_gmv_usd,

        -- ltv: round() returns unspecified numeric — cast to final type here
        case
            when predicted_monthly_gmv_usd is not null
            then (
                round(
                    (predicted_monthly_gmv_usd * 0.05 * 12) + (500 * 12),
                    2
                )
            )::numeric(12,2)
        end                                                             as estimated_annual_ltv_usd

    from enriched
)

select * from final