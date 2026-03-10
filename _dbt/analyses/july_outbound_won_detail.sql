-- analysis: july_outbound_won_detail
--
-- purpose: inspect the single outbound won lead in July 2024 to determine
-- whether it represents a genuine close or an edge case
-- (e.g. a lead that closed much later than it entered the funnel).
--
-- run with: dbt compile --select july_outbound_won_detail

select
    l.lead_id,
    l.first_sales_call_at::date                             as entry_date,
    o.close_date,
    o.stage_name,
    o.demo_set_date,
    o.demo_held,
    l.total_activity_count,
    l.days_in_funnel
from {{ ref('int_leads__enriched') }} l
join {{ ref('stg_gtm__opportunities') }} o
    on l.converted_opportunity_id = o.opportunity_id
where
    date_trunc('month', l.first_sales_call_at) = '2024-07-01'
    and l.channel = 'outbound'
    and o.is_won = true