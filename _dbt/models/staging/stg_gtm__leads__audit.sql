-- grain: one row per audit finding
-- natural key: lead_id + audit_category
-- source: gtm_case.leads (raw, pre-cleaning)
--
-- purpose: dead letter queue for source data anomalies in leads.
--   runs alongside stg_gtm__leads as a companion view.
--   does not filter or block — surfaces findings for monitoring and downstream use.
--
-- categories:
--   millennium_date_defect    form_submission_date stored as 0024-xx-xx
--   dirty_status_value        status = 'Incorrect_Contact_Data' (underscore variant)
--   negative_speed_to_contact first_sales_call_date precedes form_submission_date
--     — likely outbound-first leads later reclassified as inbound; not removed
--   stale_lead                form_submission_date in 2020, first contact in 2024
--     — real values, not defects; flagged for recency filtering in speed analysis
--   converted_missing_opp     is_converted = true but converted_opportunity_id is null
--     — indicates a crm linkage gap; these leads show as converted with no traceable opp

{{
    config(
        materialized='view',
        contract={"enforced": true}
    )
}}

with source as (
    select * from {{ source('gtm_case', 'leads') }}
),

millennium_defect as (
    select
        lead_id                                                         as record_key,
        'millennium_date_defect'                                        as audit_category,
        'form_submission_date contains 0024-xx-xx prefix'               as audit_detail,
        'raw value: ' || coalesce(form_submission_date::varchar, 'null')
                                                                        as diagnostic_info
    from source
    where form_submission_date is not null
      and form_submission_date::varchar like '0024-%'
),

dirty_status as (
    select
        lead_id                                                         as record_key,
        'dirty_status_value'                                            as audit_category,
        'status contains underscore variant: Incorrect_Contact_Data'    as audit_detail,
        'raw value: ' || status                                         as diagnostic_info
    from source
    where status = 'Incorrect_Contact_Data'
),

negative_speed as (
    select
        lead_id                                                         as record_key,
        'negative_speed_to_contact'                                     as audit_category,
        'first_sales_call_date precedes form_submission_date'           as audit_detail,
        'speed_hours: ' || round(
            extract(epoch from (
                {{ clean_timestamp('first_sales_call_date') }}
                - {{ fix_millennium_date('form_submission_date') }}::timestamp
            )) / 3600.0,
            1
        )::varchar
                                                                        as diagnostic_info
    from source
    where {{ clean_timestamp('first_sales_call_date') }} is not null
      and {{ fix_millennium_date('form_submission_date') }} is not null
      and {{ clean_timestamp('first_sales_call_date') }}
            < {{ fix_millennium_date('form_submission_date') }}::timestamp
),

stale_leads as (
    select
        lead_id                                                         as record_key,
        'stale_lead'                                                    as audit_category,
        'form_submission_date in 2020, first contact not until 2024'    as audit_detail,
        'submission: ' || {{ fix_millennium_date('form_submission_date') }}::varchar
            || ' | first_call: ' || coalesce(
                {{ clean_timestamp('first_sales_call_date') }}::varchar,
                'null'
            )                                                           as diagnostic_info
    from source
    where {{ fix_millennium_date('form_submission_date') }} < '2021-01-01'::date
      and {{ clean_timestamp('first_sales_call_date') }} >= '2024-01-01'::timestamp
),

converted_missing_opp as (
    select
        lead_id                                                         as record_key,
        'converted_missing_opp'                                         as audit_category,
        'lead status is Converted but converted_opportunity_id is null' as audit_detail,
        'status: ' || coalesce(status, 'null')                         as diagnostic_info
    from source
    where status = 'Converted'
      and (converted_opportunity_id is null or trim(converted_opportunity_id) = '')
),

unioned as (
    select * from millennium_defect
    union all
    select * from dirty_status
    union all
    select * from negative_speed
    union all
    select * from stale_leads
    union all
    select * from converted_missing_opp
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['record_key', 'audit_category']) }}
                                                                        as audit_sk,
        'leads'::varchar                                                as source_table,
        record_key::varchar,
        audit_category::varchar,
        audit_detail::varchar,
        diagnostic_info::varchar
    from unioned
)

select * from final