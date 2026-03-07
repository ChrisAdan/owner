{% docs predicted_monthly_gmv_usd %}
Estimated monthly Gross Merchandise Value (GMV) if the restaurant adopts Owner.
Sourced from `leads.predicted_sales_with_owner`. Parsed from European decimal format
(comma separator) in staging.

Used as an LTV proxy throughout this project — actual transaction-level revenue data
is not available in the source dataset. Owner's pricing model generates revenue via
a 5% take rate on online orders, so `predicted_monthly_gmv_usd * 0.05 * 12` yields
an estimated annual revenue contribution per customer.
{% enddocs %}

{% docs estimated_annual_ltv_usd %}
Estimated annual revenue Owner would generate from this restaurant if converted.
Calculated as: `predicted_monthly_gmv_usd * 0.05 * 12` (5% take rate, annualized)
plus `$500/month * 12` subscription revenue = `predicted_monthly_gmv_usd * 0.6 + 6000`.

This is a proxy LTV based on predicted GMV — not actual realized revenue. Should be
interpreted as an expected value signal for prospect prioritization, not a financial forecast.
Null where `predicted_monthly_gmv_usd` is null (8% of leads).
{% enddocs %}

{% docs marketplace_count %}
Number of distinct third-party delivery platforms the restaurant is active on
(e.g. Grubhub, DoorDash, Uber Eats). Derived by counting comma-separated values
in `marketplaces_used_cleaned`. A proxy for the restaurant's current digital
ordering maturity and dependency on third-party platforms — higher counts suggest
stronger motivation to consolidate onto a first-party solution like Owner.
{% enddocs %}

{% docs has_olo_tool %}
Boolean flag indicating whether the restaurant currently uses any first-party
online ordering tool (e.g. Toast, Square, Olo, Chownow). Derived from
`olo_tools_cleaned` being non-null. Restaurants with existing OLO tools are
already aware of first-party ordering — a different sales motion than restaurants
relying entirely on third-party marketplaces.
{% enddocs %}
