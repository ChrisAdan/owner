{% docs speed_to_first_contact_hours %}
Hours elapsed between a lead's inbound form submission and the first logged sales call.
Computed for inbound leads only — outbound leads have no reference start event so this
field is null for outbound. Computed via the `hours_between` macro.

A key leading indicator: research and GTM best practice consistently show that
faster response to inbound interest correlates with higher conversion rates.
Use to identify SDR response time patterns and set contact SLA targets.
{% enddocs %}

{% docs days_in_funnel %}
Number of days a lead has been active in the sales funnel. Channel-aware start point:

- Inbound: `form_submission_date` → `last_sales_activity_at`
- Outbound: `first_sales_call_at` → `last_sales_activity_at` (best available proxy —
  may understate true duration if BDR outreach preceded the first logged call)

Null when required timestamps are missing. Use to identify stalled leads and
benchmark expected time-to-close by channel.
{% enddocs %}

{% docs total_activity_count %}
Sum of all logged sales touchpoints for a lead: calls + texts + emails.
Null counts coalesced to 0 before summing. Measures engagement intensity —
higher activity counts on unconverted leads may signal friction or poor fit,
while lower counts on converted leads may signal high-quality prospect targeting.
{% enddocs %}
