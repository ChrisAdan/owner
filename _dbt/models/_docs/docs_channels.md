{% docs sales_channel %}
Sales team channel. One of `inbound` (SDR-driven, paid ad traffic) or
`outbound` (BDR-driven, cold outreach). Derived from source column name during unpivot
in `stg_gtm__expenses_salary_and_commissions`.
{% enddocs %}

{% docs channel_derived %}
Sales channel derived from lead data. Inbound leads have a non-null `form_submission_date` —
the prospect submitted an interest form via paid advertising (Facebook, Google).
Outbound leads have no form submission — they were identified and cold-contacted by
the BDR team. This is the authoritative channel signal available in the leads data.
Derived via the `derive_channel` macro in `int_leads__enriched`.
{% enddocs %}
