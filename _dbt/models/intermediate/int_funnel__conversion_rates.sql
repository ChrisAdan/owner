-- grain: one row per channel per calendar month
-- natural key: channel + month_date
-- upstream: int_leads__enriched
--
-- modeling decisions:
--   - lead month is channel-aware:
--       inbound  → date_trunc('month', form_submission_date)
--       outbound → date_trunc('month', first_sales_call_at)
--     rationale: earliest reliable timestamp per channel marking funnel entry.
--   - demo_set counted from opportunity.demo_set_date being non-null
--   - demo_held counted from opportunity.demo_held = true
--   - all rates use safe_divide to handle zero denominators in low-volume months
--
-- trade-offs:
--   - cohort view: leads attributed to entry month, not close month.
--   - expense data only covers jan–jun 2024 — cac calculations joining this model
--     to int_expenses__by_channel_month are naturally scoped to that window.
--   - jul 2024 data is partial.
--
-- type notes:
--   - count() returns bigint in postgres — leads_created et al. declared bigint.
--   - safe_divide(bigint::numeric, bigint::numeric) returns numeric (unspecified).
--     rate columns cast to numeric(6,4) here — point of introduction — so
--     downstream marts inherit the correct precision without recasting.

{{
    config(
        materialized='view',
        contract={"enforced": true}
    )
}}

with enriched as (
    select * from {{ ref('int_leads__enriched') }}
),

with_month as (
    select
        *,
        case
            when channel = 'inbound'
            then date_trunc('month', form_submission_date::timestamp)::date
            else date_trunc('month', first_sales_call_at)::date
        end                                                             as lead_month
    from enriched
    where
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

        -- type notes:
        --   - count() returns bigint in postgres — leads_created et al. declared bigint.
        --   - safe_divide expands to: (numerator) / nullif((denominator), 0)
        --     without outer parens, ::numeric(N,M) binds to the denominator nullif(),
        --     not the division result — casting a large denominator (e.g. leads_created=1388)
        --     into numeric(6,4) causes overflow. outer parens required:
        --     ( safe_divide(...) )::numeric(6,4)
        ({{ dbt_utils.safe_divide('demos_set::numeric', 'leads_created::numeric') }})::numeric(6,4)
                                                                        as lead_to_demo_set_rate,
                                                                        -- add precision to allow ratios > 1
                                                                        -- possible for demos booked and held between months
        ({{ dbt_utils.safe_divide('demos_held::numeric', 'demos_set::numeric') }})::numeric(8,4)
                                                                        as demo_set_to_held_rate,
        ({{ dbt_utils.safe_divide('closed_won::numeric', 'demos_held::numeric') }})::numeric(6,4)
                                                                        as demo_to_close_rate,
        ({{ dbt_utils.safe_divide('closed_won::numeric', 'leads_created::numeric') }})::numeric(6,4)
                                                                        as overall_conversion_rate

    from aggregated
)

select * from final