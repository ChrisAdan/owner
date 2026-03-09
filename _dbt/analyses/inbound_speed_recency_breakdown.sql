-- analysis: inbound_speed_recency_breakdown
--
-- purpose: decompose the >168h speed bucket to understand how much of the
-- low conversion rate is driven by genuinely stale leads (2020 form submissions
-- contacted in 2024) vs. leads with a week-to-month response delay.
--
-- context: 1,319 leads have speed > 10,000h (form in 2020, called in 2024).
-- if these dominate the >168h bucket they compress the apparent conversion rate
-- for leads with a 1–4 week response delay, which may be a meaningfully
-- different and more actionable signal.
--
-- run with: dbt compile --select inbound_speed_recency_breakdown

select
    case
        when speed_to_first_contact_hours <= 168   then '1_contacted_le_1_week'
        when speed_to_first_contact_hours <= 720   then '2_contacted_1w_to_1mo'
        when speed_to_first_contact_hours <= 8760  then '3_contacted_1mo_to_1yr'
        when speed_to_first_contact_hours >  8760  then '4_stale_gt_1yr'
    end                                                     as recency_bucket,
    count(*)                                                as leads,
    count(case when is_converted = true then 1 end)         as converted,
    round(
        count(case when is_converted = true then 1 end)::numeric
        / nullif(count(*), 0),
        3
    )                                                       as conversion_rate,
    round(min(speed_to_first_contact_hours)::numeric, 0)    as min_hours,
    round(max(speed_to_first_contact_hours)::numeric, 0)    as max_hours
from {{ ref('int_leads__enriched') }}
where
    channel = 'inbound'
    and speed_to_first_contact_hours > 0
group by 1
order by min_hours