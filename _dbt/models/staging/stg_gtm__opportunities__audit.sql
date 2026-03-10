-- grain: one row per audit finding
-- natural key: opportunity_id + audit_category
-- source: gtm_case.opportunities (raw, pre-deduplication)
--
-- purpose: dead letter queue for source data anomalies in opportunities.
--   runs alongside stg_gtm__opportunities as a companion view.
--   does not filter or block — surfaces findings for monitoring and downstream use.
--
-- categories:
--   duplicate_row          exact duplicate opportunity_ids in source export (salesforce artifact)
--   millennium_date_defect close_date or demo_set_date stored as 0024-xx-xx
--   missing_lost_reason    closed lost stage with null lost_reason_c
--   missing_attribution    how_did_you_hear_about_us_c null (76% population — directional signal only)

{{
    config(
        materialized='view',
        contract={"enforced": true}
    )
}}

with source as (
    select * from {{ source('gtm_case', 'opportunities') }}
),

deduplicated as (
    -- mirror stg_gtm__opportunities deduplication so downstream audit CTEs
    -- operate on the same grain. duplicate_rows CTE intentionally reads source.
    select distinct * from source
),

duplicate_rows as (
    select
        opportunity_id                                                  as record_key,
        'duplicate_row'                                                 as audit_category,
        'opportunity_id appears more than once in source export'        as audit_detail,
        count(*) || ' total rows for this opportunity_id'              as diagnostic_info
    from source
    group by opportunity_id
    having count(*) > 1
),

millennium_defect as (
    select
        opportunity_id                                                  as record_key,
        'millennium_date_defect'                                        as audit_category,
        'close_date or demo_set_date contains 0024-xx-xx prefix'        as audit_detail,
        'close_date: ' || coalesce(close_date::varchar, 'null')
            || ' | demo_set_date: ' || coalesce(demo_set_date::varchar, 'null')
                                                                        as diagnostic_info
    from deduplicated
    where
        (close_date    is not null and close_date::varchar    like '0024-%')
        or (demo_set_date is not null and demo_set_date::varchar like '0024-%')
),

missing_lost_reason as (
    select
        opportunity_id                                                  as record_key,
        'missing_lost_reason'                                           as audit_category,
        'closed lost opportunity with null lost_reason_c'               as audit_detail,
        'stage_name: ' || coalesce(stage_name, 'null')
            || ' | business_issue: ' || coalesce(business_issue_c, 'null')
                                                                        as diagnostic_info
    from deduplicated
    where stage_name = 'Closed Lost'
      and (lost_reason_c is null or trim(lost_reason_c) = '')
),

missing_attribution as (
    select
        opportunity_id                                                  as record_key,
        'missing_attribution'                                           as audit_category,
        'how_did_you_hear_about_us_c is null'                           as audit_detail,
        'stage_name: ' || coalesce(stage_name, 'null')                 as diagnostic_info
    from deduplicated
    where how_did_you_hear_about_us_c is null
       or trim(how_did_you_hear_about_us_c) = ''
),

unioned as (
    select * from duplicate_rows
    union all
    select * from millennium_defect
    union all
    select * from missing_lost_reason
    union all
    select * from missing_attribution
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['record_key', 'audit_category']) }}
                                                                        as audit_sk,
        'opportunities'::varchar                                        as source_table,
        record_key::varchar,
        audit_category::varchar,
        audit_detail::varchar,
        diagnostic_info::varchar
    from unioned
)

select * from final