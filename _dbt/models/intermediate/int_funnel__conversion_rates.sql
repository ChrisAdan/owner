-- grain: one row per channel per calendar month
-- natural key: channel + month_date
-- upstream: int_leads__enriched
--
-- modeling decisions:
--   - lead month is channel-aware:
--       inbound  → date_trunc('month', form_submission_date)
--       outbound → date_trunc('month', first_sales_call_at)
--     rationale: these are the earliest reliable timestamps per channel that mark
--     when a lead entered the funnel. using a consistent start event per channel
--     avoids mixing inbound form dates with outbound call dates in the same field.
--   - demo_set counted from opportunity.demo_set_date being non-null
--   - demo_held counted from opportunity.demo_held = true
--   - closed_won and closed_lost counted from opportunity.stage_name
--   - all rates computed via dbt_utils.safe_divide to handle zero-denominator months
--     (early months and outbound pre-2024 have very low volumes)
--
-- trade-offs:
--   - lead month attribution uses lead entry date, not close date. a lead created in
--     jan that closes in mar is counted in jan. this is a cohort view, not a
--     period-activity view. appropriate for pipeline efficiency analysis.
--   - expense data only covers jan–jun 2024. cac calculation joining this model
--     to int_expenses__by_channel_month will naturally limit to that window.
--   - jul 2024 data is partial — downstream consumers should filter or flag accordingly.

{{
    config(
        materialized='ephemeral'
    )
}}

with enriched as (
    select * from {{ ref('int_leads__enriched') }}
),

with_month as (
    select
        *,
        -- channel-aware lead month derivation
        case
            when channel = 'inbound'
            then date_trunc('month', form_submission_date::timestamp)::date
            else date_trunc('month', first_sales_call_at)::date
        end                                                             as lead_month
    from enriched
    where
        -- exclude leads with no anchor timestamp (ungroupable)
        (channel = 'inbound' and form_submission_date is not null)
        or (channel = 'outbound' and first_sales_call_at is not null)
),

aggregated as (
    select
        lead_month                                                      as month_date,
        channel,
        count(*)                                                        as leads_created,
        count(demo_set_date)                                            as demos_set,
        count(case when demo_held = true then 1 end)                    as demos_held,
        count(case when is_won = true then 1 end)                       as closed_won,
        count(case when is_lost = true then 1 end)                      as closed_lost
    from with_month
    group by 1, 2
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['month_date', 'channel']) }}   as funnel_channel_month_sk,
        month_date,
        channel,
        leads_created,
        demos_set,
        demos_held,
        closed_won,
        closed_lost,

        -- conversion rates — safe_divide guards against zero denominators
        -- in low-volume months (early history, sparse outbound pre-2024)
        {{ dbt_utils.safe_divide('demos_set', 'leads_created') }}           as lead_to_demo_set_rate,
        {{ dbt_utils.safe_divide('demos_held', 'demos_set') }}              as demo_set_to_held_rate,
        {{ dbt_utils.safe_divide('closed_won', 'demos_held') }}             as demo_to_close_rate,
        {{ dbt_utils.safe_divide('closed_won', 'leads_created') }}          as overall_conversion_rate

    from aggregated
)

select * from final