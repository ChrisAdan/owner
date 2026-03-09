-- analysis: july_outbound_completeness
--
-- purpose: verify whether July 2024 outbound data represents a genuine
-- conversion collapse or a data cutoff artifact before citing in documentation.
--
-- key question: is the dataset simply incomplete for July, or did outbound
-- conversion genuinely drop to near-zero?
--
-- run with: dbt compile --select july_outbound_completeness
-- then execute compiled SQL from target/compiled/

-- 1. volume and conversion by channel for July
select
    channel,
    count(*)                                                as leads_created,
    count(case when is_won = true then 1 end)               as closed_won,
    round(
        count(case when is_won = true then 1 end)::numeric
        / nullif(count(*), 0),
        4
    )                                                       as conversion_rate,
    min(
        case when channel = 'inbound'
            then form_submission_date
            else first_sales_call_at::date
        end
    )                                                       as earliest_entry,
    max(
        case when channel = 'inbound'
            then form_submission_date
            else first_sales_call_at::date
        end
    )                                                       as latest_entry,
    max(last_sales_activity_at::date)                       as latest_activity
from {{ ref('int_leads__enriched') }}
where
    (channel = 'inbound'
        and date_trunc('month', form_submission_date::timestamp) = '2024-07-01')
    or
    (channel = 'outbound'
        and date_trunc('month', first_sales_call_at) = '2024-07-01')
group by 1