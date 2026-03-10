-- grain: one row per lead
-- natural key: lead_id
-- upstream: stg_gtm__leads, stg_gtm__opportunities
--
-- modeling decisions:
--   - channel derived from form_submission_date: inbound leads have a form submission,
--     outbound leads are BDR-sourced cold contacts with no form. this is the authoritative
--     channel signal available in the data.
--   - speed_to_first_contact_hours: computed for inbound only (form_submission_date →
--     first_sales_call_at). outbound has no reference start point — first_sales_call_at
--     is the beginning of the engagement, not a response to an event. nulled for outbound
--     with documentation. this is a key leading indicator for inbound conversion.
--   - days_in_funnel: channel-aware start point.
--       inbound  → form_submission_date to last_sales_activity_at
--       outbound → first_sales_call_at to last_sales_activity_at (best available proxy)
--     null when required timestamps are missing.
--   - opportunity fields joined via converted_opportunity_id → opportunity_id.
--     left join preserves all leads including unconverted (90% of leads have no opportunity).
--
-- trade-offs:
--   - outbound days_in_funnel uses first_sales_call_at as a proxy start.
--     this understates true funnel duration if BDR outreach preceded the first logged call.
--   - stage_name and is_won/is_lost pulled from opportunities at current snapshot —
--     no historical stage progression available in source data.
--
-- type notes:
--   - extract(epoch from interval) returns float8 in postgres.
--     speed_to_first_contact_hours and days_in_funnel are cast to their final
--     numeric types here

{{
    config(
        materialized='view',
        contract={"enforced": true}
    )
}}

with leads as (
    select * from {{ ref('stg_gtm__leads') }}
),

opportunities as (
    select * from {{ ref('stg_gtm__opportunities') }}
),

joined as (
    select
        -- keys
        l.lead_sk,
        l.lead_id,
        l.converted_opportunity_id,
        o.opportunity_sk,
        o.account_id,

        -- channel derivation
        {{ derive_channel('l.form_submission_date') }}                          as channel,

        -- funnel state from opportunity (null for unconverted leads)
        o.stage_name,
        o.is_won,
        o.is_lost,
        o.is_open,
        o.attribution_source,
        o.lost_reason,
        o.demo_held,
        o.demo_set_date,
        o.close_date,

        -- lead lifecycle
        l.status,
        l.is_converted,
        l.is_disqualified,

        -- engagement activity (integer types from staging, passed through unchanged)
        l.sales_call_count,
        l.sales_text_count,
        l.sales_email_count,
        l.total_activity_count,
        l.connected_with_decision_maker,

        -- restaurant firmographics
        l.location_count,
        l.predicted_monthly_gmv_usd,
        l.marketplaces_used_cleaned,
        l.olo_tools_cleaned,
        l.cuisine_types_cleaned,

        -- timing fields passed through from staging (already typed correctly)
        l.form_submission_date,
        l.first_sales_call_at,
        l.first_text_sent_at,
        l.first_meeting_booked_at,
        l.last_sales_call_at,
        l.last_sales_activity_at,
        l.last_sales_email_at,
        o.created_at                                                            as opportunity_created_at,
        o.demo_scheduled_at,
        o.last_sales_call_at                                                    as opp_last_sales_call_at,

        -- speed to first contact: inbound only
        -- hours_between returns float8 via extract(epoch); cast to final type here
        case
            when l.form_submission_date is not null
            then (
                {{ hours_between('l.form_submission_date::timestamp', 'l.first_sales_call_at') }}
            )::numeric(10,2)
        end                                                                     as speed_to_first_contact_hours,

        -- days in funnel: channel-aware, float8 cast to final type here
        case
            when l.form_submission_date is not null
            then (
                {{ hours_between('l.form_submission_date::timestamp', 'l.last_sales_activity_at') }}
                / 24.0
            )::numeric(8,2)
            else (
                {{ hours_between('l.first_sales_call_at', 'l.last_sales_activity_at') }}
                / 24.0
            )::numeric(8,2)
        end                                                                     as days_in_funnel

    from leads l
    left join opportunities o
        on l.converted_opportunity_id = o.opportunity_id
)

select * from joined