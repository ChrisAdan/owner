-- grain: one row per sales opportunity
-- natural key: opportunity_id
-- source: gtm_case.opportunities
--
-- data quality notes:
--   - close_date and demo_set_date have a millennium prefix defect ('0024-...' not '2024-...')
--     corrected below via string replacement before casting
--   - created_date, demo_time, last_sales_call_date_time are clean (no defect)
--   - lost_reason_c is null on 4 closed lost rows — passed through as null, flagged in audit
--   - how_did_you_hear_about_us_c is 76% null — sparse but directionally useful

{{
    config(
        contract={"enforced": true}
    )
}}

with source as (
    select * from {{ source('gtm_case', 'opportunities') }}
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

        -- dates: created_date and demo_time are clean
    created_date::timestamp                                             as created_at,
    demo_time::timestamp                                                as demo_scheduled_at,
        last_sales_call_date_time::timestamp                            as last_sales_call_at,

        -- dates: close_date and demo_set_date have millennium prefix defect
        -- '0024-07-19' → '2024-07-19' via replacing leading '00' with '20'
        case
            when demo_set_date::varchar not in ('', '001-01-01')
            then overlay(demo_set_date::varchar placing '20' from 1 for 2)::date
        end                                                             as demo_set_date,

        case
            when close_date::varchar not in ('', '0001-01-01')
            then overlay(close_date::varchar placing '20' from 1 for 2)::date
        end                                                             as close_date

    from source
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['opportunity_id']) }}  as opportunity_sk,
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