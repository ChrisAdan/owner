-- grain: one row per source_table per audit_category per calendar month
-- natural key: month_date + source_table + audit_category
-- upstream: stg_gtm__leads__audit, stg_gtm__opportunities__audit,
--           stg_gtm__leads, stg_gtm__opportunities
--
-- modeling decisions:
--   - audit findings have no native timestamp — month is attributed by joining
--     back to the source record's calendar anchor:
--       leads:         form_submission_date (channel-aware entry point)
--       opportunities: created_at (opportunity creation date)
--   - finding_count: raw audit row count for the month/category.
--   - affected_records: distinct record_keys flagged, which may be less than
--     finding_count if one record hits multiple categories (impossible here since
--     grain is already record + category, but explicit for clarity).
--   - pct_of_monthly_total: affected_records / total records created that month
--     for the source table. provides a density signal independent of volume.

{{
    config(
        materialized='view',
        contract={"enforced": true}
    )
}}

with leads_audit as (
    select * from {{ ref('stg_gtm__leads__audit') }}
),

opps_audit as (
    select * from {{ ref('stg_gtm__opportunities__audit') }}
),

leads as (
    select
        lead_sk,
        lead_id,
        form_submission_date,
        date_trunc('month', form_submission_date::timestamp)::date      as month_date
    from {{ ref('stg_gtm__leads') }}
    where form_submission_date is not null
),

opps as (
    select
        opportunity_sk,
        opportunity_id,
        created_at,
        date_trunc('month', created_at)::date                           as month_date
    from {{ ref('stg_gtm__opportunities') }}
    where created_at is not null
),

-- monthly volume denominators for pct calculation
leads_monthly_volume as (
    select
        month_date,
        count(*)                                                        as monthly_record_count
    from leads
    group by 1
),

opps_monthly_volume as (
    select
        month_date,
        count(*)                                                        as monthly_record_count
    from opps
    group by 1
),

-- join audit findings to their source record's month anchor
leads_audit_dated as (
    select
        a.audit_category,
        a.record_key,
        l.month_date
    from leads_audit a
    inner join leads l
        on a.record_key = l.lead_id
),

opps_audit_dated as (
    select
        a.audit_category,
        a.record_key,
        o.month_date
    from opps_audit a
    inner join opps o
        on a.record_key = o.opportunity_id
),

-- aggregate per month + category
leads_agg as (
    select
        d.month_date,
        'leads'::varchar                                                as source_table,
        d.audit_category,
        count(*)                                                        as finding_count,
        count(distinct d.record_key)                                    as affected_records,
        v.monthly_record_count
    from leads_audit_dated d
    inner join leads_monthly_volume v
        on d.month_date = v.month_date
    group by 1, 2, 3, v.monthly_record_count
),

opps_agg as (
    select
        d.month_date,
        'opportunities'::varchar                                        as source_table,
        d.audit_category,
        count(*)                                                        as finding_count,
        count(distinct d.record_key)                                    as affected_records,
        v.monthly_record_count
    from opps_audit_dated d
    inner join opps_monthly_volume v
        on d.month_date = v.month_date
    group by 1, 2, 3, v.monthly_record_count
),

unioned as (
    select * from leads_agg
    union all
    select * from opps_agg
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['month_date', 'source_table', 'audit_category']) }}
                                                                        as audit_month_sk,
        month_date,
        source_table::varchar,
        audit_category::varchar,
        finding_count::integer,
        affected_records::integer,
        monthly_record_count::integer,
        round(
            affected_records::numeric / nullif(monthly_record_count, 0),
            4
        )::numeric(6,4)                                                 as pct_of_monthly_total
    from unioned
)

select * from final