-- analysis: inbound_speed_conversion_buckets
--
-- purpose: validate the speed-to-first-contact conversion cliff cited
-- in the GTM opportunities section of the README.
--
-- claim: inbound leads contacted within 72 hours convert at ~4x the rate
-- of leads contacted after 7 days.
--
-- negative speed values (26 leads where call precedes form submission)
-- are excluded — these are a data quality artifact, not meaningful signal.
-- see source data quality notes in README for detail.
--
-- run with: dbt compile --select inbound_speed_conversion_buckets

select
    case
        when speed_to_first_contact_hours <= 24  then '1_le_24h'
        when speed_to_first_contact_hours <= 72  then '2_24_to_72h'
        when speed_to_first_contact_hours <= 168 then '3_72_to_168h'
        when speed_to_first_contact_hours >  168 then '4_gt_168h'
    end                                                     as speed_bucket,
    count(*)                                                as leads,
    count(case when is_converted = true then 1 end)         as converted,
    count(case when is_won = true then 1 end)               as won,
    round(
        count(case when is_converted = true then 1 end)::numeric
        / nullif(count(*), 0),
        3
    )                                                       as conversion_rate,
    round(
        count(case when is_won = true then 1 end)::numeric
        / nullif(count(*), 0),
        3
    )                                                       as won_rate,
    round(avg(speed_to_first_contact_hours)::numeric, 1)    as avg_speed_hours
from {{ ref('int_leads__enriched') }}
where
    channel = 'inbound'
    and speed_to_first_contact_hours is not null
    and speed_to_first_contact_hours > 0
group by 1
order by 1