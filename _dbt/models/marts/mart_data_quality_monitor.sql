-- grain: one row per calendar month
-- natural key: month_date
-- upstream: int_audit__by_month, mart_cac_ltv
--
-- modeling decisions:
--   - revenue proxy: new_customers_won * avg_estimated_ltv_usd from mart_cac_ltv.
--     this is estimated annual ltv attributed to won leads in the entry cohort month,
--     not realized revenue — directional signal only, consistent with mart_cac_ltv.
--   - scoped to jan–jun 2024 via inner join to mart_cac_ltv (expense window).
--     audit history outside this window exists but has no revenue counterpart.
--   - audit metrics summed across both source tables and all categories per month,
--     then broken out by source_table for leads vs opportunities comparison.
--   - mom_revenue_change and mom_audit_change: month-over-month deltas using lag().
--     null for the first month in the window — expected.
--
-- intended use:
--   - monthly health check: did bad data spike in a month revenue dropped?
--   - leading indicator: audit density rising before revenue impact is visible
--     suggests upstream CRM data quality issues worth investigating.

{{
    config(
        materialized='table',
        contract={"enforced": true}
    )
}}

with audit as (
    select * from {{ ref('int_audit__by_month') }}
),

revenue as (
    -- sum across channels to get monthly totals
    select
        month_date,
        sum(new_customers_won)                                          as new_customers_won,
        sum(new_customers_won * avg_estimated_ltv_usd)                  as estimated_monthly_ltv_usd,
        sum(total_cost_usd)                                             as total_cost_usd,
        sum(cac_usd * new_customers_won) / nullif(sum(new_customers_won), 0)
                                                                        as blended_cac_usd
    from {{ ref('mart_cac_ltv') }}
    group by 1
),

-- total audit findings per month across all tables/categories
audit_totals as (
    select
        month_date,
        sum(finding_count)                                              as total_findings,
        sum(affected_records)                                           as total_affected_records,
        sum(monthly_record_count)                                       as total_records_in_scope
    from audit
    group by 1
),

-- per-source breakdown for leads vs opportunities
audit_by_source as (
    select
        month_date,
        sum(finding_count) filter (where source_table = 'leads')        as leads_findings,
        sum(finding_count) filter (where source_table = 'opportunities') as opps_findings,
        sum(affected_records) filter (where source_table = 'leads')     as leads_affected,
        sum(affected_records) filter (where source_table = 'opportunities') as opps_affected
    from audit
    group by 1
),

joined as (
    select
        r.month_date,
        r.new_customers_won,
        r.estimated_monthly_ltv_usd,
        r.total_cost_usd,
        r.blended_cac_usd,
        coalesce(t.total_findings, 0)                                   as total_audit_findings,
        coalesce(t.total_affected_records, 0)                           as total_audit_affected_records,
        coalesce(t.total_records_in_scope, 0)                           as total_records_in_scope,
        round(
            coalesce(t.total_affected_records, 0)::numeric
            / nullif(t.total_records_in_scope, 0),
            4
        )                                                               as overall_audit_density,
        coalesce(s.leads_findings, 0)                                   as leads_audit_findings,
        coalesce(s.opps_findings, 0)                                    as opps_audit_findings,
        coalesce(s.leads_affected, 0)                                   as leads_audit_affected,
        coalesce(s.opps_affected, 0)                                    as opps_audit_affected
    from revenue r
    left join audit_totals t   on r.month_date = t.month_date
    left join audit_by_source s on r.month_date = s.month_date
),

with_mom as (
    select
        *,
        estimated_monthly_ltv_usd
            - lag(estimated_monthly_ltv_usd) over (order by month_date)
                                                                        as mom_revenue_change_usd,
        total_audit_findings
            - lag(total_audit_findings) over (order by month_date)
                                                                        as mom_audit_findings_change
    from joined
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['month_date']) }}::varchar as dq_monitor_sk,
        month_date,
        new_customers_won::bigint,
        estimated_monthly_ltv_usd::numeric(12,2),
        total_cost_usd::numeric(12,2),
        blended_cac_usd::numeric(12,2),
        mom_revenue_change_usd::numeric(12,2),
        total_audit_findings::integer,
        total_audit_affected_records::integer,
        total_records_in_scope::integer,
        overall_audit_density::numeric(6,4),
        leads_audit_findings::integer,
        opps_audit_findings::integer,
        leads_audit_affected::integer,
        opps_audit_affected::integer,
        mom_audit_findings_change::integer
    from with_mom
)

select * from final