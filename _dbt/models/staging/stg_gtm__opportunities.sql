-- grain: one row per sales opportunity
-- natural key: opportunity_id
-- source: gtm_case.opportunities
--
-- data quality notes:
--   - 4 opportunity_ids are exact duplicates in source (salesforce export artifact)
--     deduplicated via distinct * before cleaning — no data loss, rows are byte-for-byte identical
--   - close_date and demo_set_date have a millennium prefix defect ('0024-...' not '2024-...')
--     corrected via fix_millennium_date macro (same defect as leads.form_submission_date)
--   - created_date, demo_time, last_sales_call_date_time are clean — no defect
--   - lost_reason_c is null on 4 closed lost rows — passed through as null, flagged in audit
--   - how_did_you_hear_about_us_c is 76% null — sparse but directionally useful where populated

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
    -- 4 opportunity_ids are perfect duplicates in the source export
    -- distinct * is safe here — rows are identical on every field
    select distinct * from source
),

cleaned as (
    select
        -- keys
        opportunity_id,
        account_id,

        -- funnel stage
        stage_name,

        -- derived stage flags for convenience downstream
        case when stage_name = 'Closed Won'  then true else false end   as is_won,
        case when stage_name = 'Closed Lost' then true else false end   as is_lost,
        case when stage_name not in (
            'Closed Won', 'Closed Lost'
        ) then true else false end                                       as is_open,

        -- attribution
        nullif(trim(how_did_you_hear_about_us_c), '')                   as attribution_source,

        -- loss signals
        nullif(trim(lost_reason_c), '')                                 as lost_reason,
        nullif(trim(closed_lost_notes_c), '')                           as closed_lost_notes,
        nullif(trim(business_issue_c), '')                              as business_issue,

        -- demo
        demo_held::boolean                                              as demo_held,

        -- clean timestamps: no defect on these fields
        created_date::timestamp                                         as created_at,
        demo_time::timestamp                                            as demo_scheduled_at,
        last_sales_call_date_time::timestamp                            as last_sales_call_at,

        -- defective dates: millennium prefix corrected via shared macro
        {{ fix_millennium_date('demo_set_date') }}                      as demo_set_date,
        {{ fix_millennium_date('close_date') }}                         as close_date

    from deduplicated
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['opportunity_id']) }}      as opportunity_sk,
        opportunity_id,
        account_id,
        stage_name,
        is_won,
        is_lost,
        is_open,
        attribution_source,
        lost_reason,
        closed_lost_notes,
        business_issue,
        demo_held,
        created_at,
        demo_scheduled_at,
        last_sales_call_at,
        demo_set_date,
        close_date
    from cleaned
)

select * from final