-- grain: one row per lead
-- natural key: lead_id
-- source: gtm_case.leads
--
-- data quality notes:
--   - form_submission_date only: has millennium prefix defect ('0024-...' not '2024-...')
--     corrected via fix_millennium_date macro
--   - all other date/timestamp fields are clean
--   - predicted_sales_with_owner: european decimal format — parsed via parse_european_numeric macro
--   - status: one dirty value 'Incorrect_Contact_Data' normalized to 'Incorrect Contact Data'
--   - array fields: stringified python lists cleaned via clean_category_list macro
--     full explode into bridge tables happens in intermediate layer
--   - converted_opportunity_id: populated on exactly 2,794 rows — matches opportunities grain

{{
    config(
        materialized='view',
        contract={"enforced": true}
    )
}}

with source as (
    select * from {{ source('gtm_case', 'leads') }}
),

cleaned as (
    select
        -- keys
        lead_id,
        nullif(trim(converted_opportunity_id), '')                          as converted_opportunity_id,

        -- lead status — normalize dirty duplicate value
        case
            when status = 'Incorrect_Contact_Data' then 'Incorrect Contact Data'
            else status
        end                                                                 as status,

        -- derived status flags
        case when status = 'Converted' then true else false end             as is_converted,
        case when status in (
            'Disqualified', 'Not Interested', 'Incorrect Contact Data',
            'Incorrect_Contact_Data', 'No Longer With Company'
        ) then true else false end                                          as is_disqualified,

        -- engagement activity counts
        sales_call_count::integer                                           as sales_call_count,
        sales_text_count::integer                                           as sales_text_count,
        sales_email_count::integer                                          as sales_email_count,

        -- total activity count derived for downstream scoring
        coalesce(sales_call_count::integer, 0)
            + coalesce(sales_text_count::integer, 0)
            + coalesce(sales_email_count::integer, 0)                      as total_activity_count,

        -- decision maker flag
        connected_with_decision_maker::boolean                             as connected_with_decision_maker,

        -- restaurant firmographics
        location_count::integer                                             as location_count,

        -- predicted gmv: european decimal → numeric
        {{ parse_european_numeric('predicted_sales_with_owner') }}          as predicted_monthly_gmv_usd,

        -- array fields: clean python list syntax, null empty lists
        {{ clean_category_list('marketplaces_used') }}                      as marketplaces_used_cleaned,
        {{ clean_category_list('online_ordering_used') }}                   as olo_tools_cleaned,
        {{ clean_category_list('cuisine_types') }}                          as cuisine_types_cleaned,

        -- dates: form_submission_date only has millennium prefix defect
        {{ fix_millennium_date('form_submission_date') }}                   as form_submission_date,

        -- timestamps: all clean, no defect
        {{ clean_timestamp('first_sales_call_date') }}                      as first_sales_call_at,
        {{ clean_timestamp('first_text_sent_date') }}                       as first_text_sent_at,
        {{ clean_timestamp('first_meeting_booked_date') }}                  as first_meeting_booked_at,
        {{ clean_timestamp('last_sales_call_date') }}                       as last_sales_call_at,
        {{ clean_timestamp('last_sales_activity_date') }}                   as last_sales_activity_at,
        {{ clean_timestamp('last_sales_email_date') }}                      as last_sales_email_at

    from source
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['lead_id']) }}                 as lead_sk,
        lead_id,
        converted_opportunity_id,
        status,
        is_converted,
        is_disqualified,
        sales_call_count,
        sales_text_count,
        sales_email_count,
        total_activity_count,
        connected_with_decision_maker,
        location_count,
        predicted_monthly_gmv_usd,
        marketplaces_used_cleaned,
        olo_tools_cleaned,
        cuisine_types_cleaned,
        form_submission_date,
        first_sales_call_at,
        first_text_sent_at,
        first_meeting_booked_at,
        last_sales_call_at,
        last_sales_activity_at,
        last_sales_email_at
    from cleaned
)

select * from final