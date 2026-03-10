-- grain: one row per lead
-- natural key: lead_sk
-- upstream: int_leads__enriched, int_restaurant__profile
--
-- modeling decisions:
--   - conversion_probability_tier is a rule-based classifier derived from two
--     empirically-supported signals in this dataset:
--
--       connected_with_decision_maker:
--         true + activity > 0:  24.5% conversion rate (n=9,772)
--         true + no activity:   41.4% conversion rate (n=29)   <- fresh/high-intent
--         false + activity > 0:  3.0% conversion rate (n=13,218)
--         false + no activity:   1.0% conversion rate (n=4,037)
--
--       speed_to_first_contact_hours (inbound only):
--         converted leads p50 = 39h, p75 = 69h vs all-inbound p50 = 88h
--         sub-72h response associated with ~2x conversion vs slower response
--
--     tier definitions:
--       hot:  connected_with_decision_maker = true
--             rationale: 10x conversion lift vs not connected.
--       warm: not hot, has engagement (activity > 0) AND
--             (inbound with speed <= 72h OR predicted_monthly_gmv > p75 [$6,279])
--       cold: all remaining leads
--
--   - activity_density_score: total_activity_count / greatest(days_in_funnel, 1)
--     normalizes engagement intensity by time in funnel.
--     null where days_in_funnel is null (no anchor timestamps available).
--
-- trade-offs:
--   - tier logic is rule-based not model-based. transparent, auditable, and
--     directly tied to observed conversion rates in this dataset.
--   - outbound leads cannot be scored on speed — warm tier for outbound relies
--     on activity + gmv signals only.
--   - activity_density_score is a relative signal — use for ranking within
--     cohorts, not as a standalone threshold.
--
-- incremental notes:
--   - strategy: delete+insert on lead_sk (postgres does not support MERGE pre-v15)
--   - watermark: last_sales_activity_at — any lead touched since last run reprocesses.
--     catches status changes (e.g. working → disqualified) as well as new activity,
--     since both update last_sales_activity_at in the CRM.
--   - postgres restriction: aggregate functions are not allowed in WHERE clauses,
--     even inside a subquery. watermark is resolved in a CTE before the leads CTE
--     and joined via cross join to produce a scalar comparison value.
--   - initial load and schema changes: dbt build --full-refresh
--
-- type notes:
--   - final cte casts all columns to their contract-declared types explicitly.
--     postgres infers text for string expressions and unparameterized numeric for
--     decimal expressions; on_schema_change='fail' treats these as type mismatches
--     on incremental runs. explicit casts at this layer ensure table physical types
--     match the contract on full-refresh, so subsequent incremental runs pass cleanly.

{{
    config(
        materialized='incremental',
        unique_key='lead_sk',
        incremental_strategy='delete+insert',
        on_schema_change='fail',
        contract={"enforced": true}
    )
}}

{% if is_incremental() %}

with watermark as (
    -- resolve max watermark before leads CTE to satisfy postgres restriction:
    -- aggregate functions are not allowed in WHERE, even in subqueries.
    -- cross join below produces a single scalar value for the filter.
    select max(last_sales_activity_at) as max_activity_at
    from {{ this }}
    where last_sales_activity_at is not null
),

leads as (
    select l.*
    from {{ ref('int_leads__enriched') }} l
    cross join watermark w
    where l.last_sales_activity_at >= w.max_activity_at
),

{% else %}

with leads as (
    select * from {{ ref('int_leads__enriched') }}
),

{% endif %}

restaurant as (
    select * from {{ ref('int_restaurant__profile') }}
),

joined as (
    select
        l.lead_sk,
        l.lead_id,
        l.channel,
        l.status,
        l.is_converted,
        l.is_disqualified,
        l.connected_with_decision_maker,
        l.speed_to_first_contact_hours,
        l.days_in_funnel,
        l.total_activity_count,
        l.last_sales_activity_at,
        r.predicted_monthly_gmv_usd,
        r.estimated_annual_ltv_usd,
        r.marketplace_count,
        r.has_olo_tool,

        -- activity density: touches per day in funnel
        -- cast to numeric explicitly; greatest() on numeric preserves type
        case
            when l.days_in_funnel is not null and l.total_activity_count is not null
            then round(
                l.total_activity_count::numeric / greatest(l.days_in_funnel, 1::numeric),
                4
            )
        end                                                             as activity_density_score,

        -- conversion probability tier
        -- thresholds from observed conversion rates (see header comments)
        case
            when l.is_converted = true
                then 'converted'
            when is_disqualified then null
            when l.connected_with_decision_maker = true
                then 'hot'
            when l.total_activity_count > 0
                and (
                    l.speed_to_first_contact_hours <= 72
                    or r.predicted_monthly_gmv_usd > 6279
                )
                then 'warm'
            else 'cold'
        end                                                             as conversion_probability_tier

    from leads l
    left join restaurant r
        on l.lead_sk = r.lead_sk
),

final as (
    select
        lead_sk::varchar,
        lead_id::varchar,
        channel::varchar,
        status::varchar,
        is_converted,
        is_disqualified,
        connected_with_decision_maker,
        speed_to_first_contact_hours::numeric(10,2),
        days_in_funnel::numeric(8,2),
        total_activity_count,
        last_sales_activity_at,
        activity_density_score::numeric(8,4),
        predicted_monthly_gmv_usd::numeric(12,2),
        estimated_annual_ltv_usd::numeric(12,2),
        marketplace_count,
        has_olo_tool,
        conversion_probability_tier::varchar
    from joined
)

select * from final