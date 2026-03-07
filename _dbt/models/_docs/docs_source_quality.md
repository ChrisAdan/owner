{% docs european_currency_format %}
Raw value exported from internal finance system using European number formatting:
`US$` prefix, space as thousands separator, comma as decimal separator (e.g. `US$ 55 779,40`).
Parsed to numeric in staging by stripping non-numeric characters and replacing comma with period.
{% enddocs %}

{% docs month_label_format %}
Month label in `Mon-YY` format (e.g. `Jan-24`). Parsed to the first day of that month
as a `date` type in staging (e.g. `2024-01-01`).
{% enddocs %}

{% docs millennium_date_defect %}
Known Salesforce export defect affecting date fields in this dataset. The millennium
prefix is dropped during export, producing `0024-xx-xx` instead of `2024-xx-xx`.
Corrected in staging via the `fix_millennium_date` macro, which overlays `'20'` onto
characters 1–2 of the raw string before casting to date.

Affected fields:

- `leads.form_submission_date`
- `opportunities.close_date`
- `opportunities.demo_set_date`

All timestamp fields (`created_date`, `demo_time`, `last_sales_call_date_time`,
`first_sales_call_date`, etc.) are unaffected.
{% enddocs %}
