# AGENTS.md

Operational context for AI coding agents (Claude Code, Cursor, Copilot) working in this repository. Read this before making changes to any dbt model, test, schema, or semantic layer file.

---

## What this repo is

A dbt analytics engineering project on Owner's GTM dataset. Four source tables: `leads` (27,056 rows), `opportunities` (2,794 rows), `expenses_advertising` (6 rows), `expenses_salary_and_commissions` (6 rows). Six months of data: Jan–Jun 2024.

The project produces four mart tables consumed by a Snowflake Cortex Analyst / Claude query agent via `semantic/semantic_model.yaml` and `_dbt/models/metrics.yml`.

---

## Build commands

Always run from `_dbt/`. Seeds must be loaded separately — `dbt build` interleaves seed tests with model tests and will fail if run cold.

```bash
dbt seed                                                   # load source data (once)
dbt build --full-refresh --exclude resource_type:seed      # initial build or schema change
dbt build --exclude resource_type:seed                     # subsequent incremental runs
dbt compile --select <analysis_name>                       # compile an analysis to target/compiled/
dbt docs generate && dbt docs serve                        # local docs
```

The CI workflow (`.github/workflows/ci.yml`) runs `dbt build` on push. It does not run `--full-refresh` — if you change a mart contract, you need to trigger a full-refresh manually or the incremental run will fail with an `on_schema_change='fail'` error.

---

## Adapter: Postgres (dev) / Snowflake (prod)

Local dev target is **Postgres 15** (`gtm_case_dev` schema). Production target is **Snowflake** (`DEMO_DB.GTM_CASE_PROD`).

One known Postgres-specific function in the codebase:

```sql
-- _dbt/models/intermediate/int_restaurant__profile.sql
-- Postgres:
array_length(string_to_array(col, ','), 1)
-- Snowflake equivalent:
array_size(split(col, ','))
```

All other SQL is cross-adapter. The explicit `::varchar` and `::numeric(p,s)` casts in mart `final` CTEs are Postgres workarounds — harmless on Snowflake, do not remove them without testing.

---

## Contract enforcement

All models use `contract: enforced: true`. Every column has a declared `data_type`. If you add a column, you must declare it in `schema.yml` with the correct type before running. If you change a column type, you must run `--full-refresh`.

**The most common failure mode**: adding a new column to a mart and running incrementally without declaring it. The build will fail at the contract check with a type mismatch, not at SQL execution. Fix: add the column to `schema.yml`, then run `--full-refresh`.

Explicit casts in `final` CTEs are required on Postgres because `on_schema_change='fail'` treats `text` vs `varchar` and unparameterized `numeric` vs `numeric(p,s)` as mismatches. Always cast string expressions to `::varchar` and decimal expressions to their declared `::numeric(p,s)` in the `final` CTE of any mart.

---

## Incremental strategies

All three incremental marts use `delete+insert`. Postgres pre-v15 does not support `MERGE` — do not switch strategies without verifying the Snowflake target version.

| Mart                      | Unique key              | Watermark / lookback                                 |
| ------------------------- | ----------------------- | ---------------------------------------------------- |
| `mart_cac_ltv`            | `[month_date, channel]` | `var('incremental_lookback_months')` trailing months |
| `mart_gtm_funnel`         | `[month_date, channel]` | Same lookback on `leads_with_month` and final join   |
| `mart_leading_indicators` | `lead_sk`               | `last_sales_activity_at` watermark via cross join    |

The lookback window exists because leads can close months after they enter the funnel, retroactively changing cohort CAC and funnel rates. Do not remove it.

The watermark CTE pattern in `mart_leading_indicators` is deliberate — Postgres does not allow aggregate functions in WHERE clauses, even in subqueries. The watermark is resolved in a separate CTE and joined via `cross join`. Do not inline it.

---

## Source data quality

Several source issues are handled in staging. Do not "fix" these by modifying seeds or adding upstream transformations:

- **Millennium date defect** — `form_submission_date`, `close_date`, `demo_set_date` stored as `0024-xx-xx`. Corrected by `fix_millennium_date()` macro in staging.
- **European numeric format** — GMV and expense fields use comma decimal, space thousands separator. Corrected by `parse_european_numeric()` macro in staging.
- **Stringified Python lists** — `marketplaces_used`, `online_ordering_used`, `cuisine_types` are raw Salesforce export artifacts. Cleaned by `clean_category_list()` macro in staging.
- **Opportunity duplicates** — 4 exact duplicate rows. Handled by `distinct *` in `stg_gtm__opportunities`. The audit model reads from `source` intentionally to surface them.
- **26 leads with negative speed-to-contact** — call precedes form submission. Surfaced in audit, not filtered. Filter `speed_to_first_contact_hours > 0` in any analysis using this field.
- **1,319 stale 2020 leads** — contacted in 2024. Surfaced in audit. Exclude or decompose by recency for speed-based analysis.

---

## Audit models (DLQ)

Each source table has a companion audit view:

- `stg_gtm__leads__audit` — categories: `millennium_date_defect`, `dirty_status_value`, `negative_speed_to_contact`, `stale_lead`, `converted_missing_opp`
- `stg_gtm__opportunities__audit` — categories: `duplicate_row`, `millennium_date_defect`, `missing_lost_reason`, `missing_attribution`

The opportunities audit reads from the `deduplicated` CTE for all categories except `duplicate_rows`, which reads from `source`. This is intentional — without it, the 4 duplicate source rows generate duplicate audit SKs and the unique test fails.

Do not add new audit categories without updating the `accepted_values` test in `schema.yml`.

---

## Semantic layer

Two files define the LLM query interface on top of the marts:

- `_dbt/models/metrics.yml` — dbt Semantic Layer / MetricFlow spec. 3 semantic models, 15 metrics.
- `semantic/semantic_model.yaml` — Snowflake Cortex Analyst spec. Same 3 tables, 6 verified queries.

If you add or rename a mart column that is referenced in either file, update both. Column descriptions in these files are not decorative — they are the semantic grounding that prevents an LLM query agent from generating plausible-but-wrong SQL. Keep them accurate and specific.

Key business context encoded in these descriptions (do not remove):

- CAC is cohort-attributed to entry month, not close month — late-window months undercount
- `demo_set_to_held_rate` can exceed 1.0 — this is a data characteristic, not an error
- `speed_to_first_contact_hours` is null for outbound leads by design
- Advertising spend is attributed to inbound only — outbound has no ad spend
- Tier thresholds are data-derived: 72h = converted-lead p75, $6,279 GMV = dataset p75

---

## Key metrics (quick reference)

| Metric               | Definition                           | Jun 2024 outbound signal |
| -------------------- | ------------------------------------ | ------------------------ |
| CAC                  | `total_cost_usd / new_customers_won` | $6,250 (4x Jan)          |
| CAC:LTV ratio        | `avg_estimated_ltv_usd / cac_usd`    | 1.6x (floor ~1x)         |
| Lead→demo set rate   | `demos_set / leads_created`          | 1.8% (was 5.3% in Jan)   |
| Overall conversion   | `closed_won / leads_created`         | 0.37% (was 0.90%)        |
| Hot tier conversion  | `connected_with_dm = true`           | 24.5% observed           |
| Warm tier conversion | activity + speed/GMV signal          | 5.2% observed            |

LTV proxy formula: `(predicted_monthly_gmv_usd × 0.05 × 12) + (500 × 12)` — Owner's 5% GMV take rate plus $500/mo subscription, annualized. Not realized revenue.

---

## What to extend vs what to leave alone

**Safe to extend:**

- Add new analyses in `_dbt/analyses/` — they don't affect the build
- Add new metrics to `metrics.yml` and `semantic_model.yaml`
- Add new columns to intermediate or mart models (with contract declaration and full-refresh)
- Add new singular tests in `_dbt/tests/`

**Requires care:**

- Changing mart grain or unique keys — requires full-refresh and downstream impact check
- Adding audit categories — update `accepted_values` in schema.yml
- Changing the `derive_channel` macro — it is the single source of truth for channel logic; changing it affects all downstream models
- Modifying `fix_millennium_date` or `parse_european_numeric` — used in both staging and audit models; test both paths

**Do not:**

- Modify seeds to "fix" source data issues — the macros handle this intentionally
- Remove explicit casts from mart `final` CTEs without testing on Postgres
- Filter out the negative-speed or stale-lead rows in staging — they are surfaced in audit for a reason
- Use positional references in `GROUP BY` clauses — Postgres rejects aggregate functions referenced positionally
