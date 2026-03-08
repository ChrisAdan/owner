-- grain: one row per channel per calendar month
-- natural key: channel + month_date
-- upstream: int_funnel__conversion_rates, int_leads__enriched
--
-- modeling decisions:
--   - rate columns passed through from int_funnel__conversion_rates, which already
--     casts them to numeric(6,4) at the intermediate layer. no re-casting needed here.
--   - avg engagement metrics scoped to converted leads for speed/days_in_funnel —
--     these measure the successful path. avg_activity_count includes all leads.
--   - speed_to_first_contact is inbound only by construction; outbound rows
--     will have null avg_speed_to_first_contact_hours — expected and documented.
--
-- trade-offs:
--   - full history from 2020 included. outbound only appears from dec 2023.
--     filter to 2024 for channel comparison analysis.
--   - avg_days_in_funnel for outbound uses first_sales_call_at as proxy start —
--     may understate true duration.
--
-- type notes:
--   - rate columns are already numeric(6,4) from the intermediate layer.
--     no re-casting required except demo_set_to_held_rate (see below).
--   - avg() of numeric returns unspecified numeric precision in postgres.
--     engagement avg columns cast to their declared precision here — point of
--     introduction for these fields.
--   - demo_set_to_held_rate is declared numeric(8,4) not numeric(6,4).
--     rationale: demos_held can exceed demos_set in a given cohort month due to
--     timing mismatch — a demo set in month N may be held in month N+1, causing
--     the held count to exceed the set count for that month. ratio > 1.0 overflows
--     numeric(6,4) (max value 99.9999). numeric(8,4) allows up to 9999.9999.
--     this is a data characteristic, not a data quality issue — documented here
--     so downstream consumers understand the metric correctly.

{{
    config(
        materialized='table',
        contract={"enforced": true}
    )
}}

with funnel as (
    select * from {{ ref('int_funnel__conversion_rates') }}
),

leads as (
    select * from {{ ref('int_leads__enriched') }}
),

leads_with_month as (
    select
        *,
        case
            when channel = 'inbound'
            then date_trunc('month', form_submission_date::timestamp)::date
            else date_trunc('month', first_sales_call_at)::date
        end                                                             as lead_month
    from leads
    where
        (channel = 'inbound'  and form_submission_date is not null)
        or (channel = 'outbound' and first_sales_call_at is not null)
),

engagement_by_month as (
    select
        lead_month                                                      as month_date,
        channel,

        -- avg() of numeric returns unspecified precision in postgres
        -- cast to declared type here — point of introduction for these fields
        avg(
            case when is_converted = true then speed_to_first_contact_hours end
        )::numeric(10,2)                                                as avg_speed_to_first_contact_hours,

        avg(
            case when is_converted = true then days_in_funnel end
        )::numeric(8,2)                                                 as avg_days_in_funnel,

        avg(total_activity_count::numeric)::numeric(8,2)                as avg_activity_count

    from leads_with_month
    group by 1, 2
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['f.month_date', 'f.channel']) }}   as gtm_funnel_sk,
        f.month_date,
        f.channel,
        f.leads_created,
        f.demos_set,
        f.demos_held,
        f.closed_won,
        f.closed_lost,

        -- rate columns already numeric(6,4) from int layer — passed through unchanged
        f.lead_to_demo_set_rate,
        -- exception: widened to numeric(8,4) — see type notes above
        f.demo_set_to_held_rate::numeric(8,4)                           as demo_set_to_held_rate,
        f.demo_to_close_rate,
        f.overall_conversion_rate,

        e.avg_speed_to_first_contact_hours,
        e.avg_days_in_funnel,
        e.avg_activity_count

    from funnel f
    left join engagement_by_month e
        on  f.month_date = e.month_date
        and f.channel    = e.channel
)

select * from final