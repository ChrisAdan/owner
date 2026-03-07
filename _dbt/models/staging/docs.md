{% docs european_currency_format %}
Raw value exported from internal finance system using European number formatting:
`US$` prefix, space as thousands separator, comma as decimal separator (e.g. `US$ 55 779,40`).
Parsed to numeric in staging by stripping non-numeric characters and replacing comma with period.
{% enddocs %}

{% docs month_label_format %}
Month label in `Mon-YY` format (e.g. `Jan-24`). Parsed to the first day of that month
as a `date` type in staging (e.g. `2024-01-01`).
{% enddocs %}

{% docs surrogate_key %}
Surrogate primary key generated via `dbt_utils.generate_surrogate_key()`. Deterministic
md5 hash of the natural key column(s). Used as the stable join key for downstream models.
{% enddocs %}

{% docs sales_channel %}
Sales team channel. One of `inbound` (SDR-driven, paid ad traffic) or
`outbound` (BDR-driven, cold outreach). Derived from source column name during unpivot.
{% enddocs %}
