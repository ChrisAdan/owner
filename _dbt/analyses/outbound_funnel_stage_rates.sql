-- analysis: outbound_funnel_stage_rates
--
-- purpose: verify the claim in the README that outbound lead → demo set rate
-- fell from 5.3% in January 2024 to 1.8% in June 2024, and that the drop is
-- concentrated at the top of the funnel rather than at the AE close stage.
--
-- run with: dbt compile --select outbound_funnel_stage_rates

select
    month_date,
    channel,
    leads_created,
    demos_set,
    demos_held,
    closed_won,
    lead_to_demo_set_rate,
    demo_set_to_held_rate,
    demo_to_close_rate,
    overall_conversion_rate
from {{ ref('mart_gtm_funnel') }}
where
    channel = 'outbound'
    and month_date between '2024-01-01' and '2024-06-01'
order by month_date