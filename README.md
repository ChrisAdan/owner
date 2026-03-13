# Owner GTM Analytics — Case Study

A dbt analytics engineering project built on Owner's GTM dataset: four source tables covering 27,056 leads, 2,794 opportunities, and six months of channel cost data (Jan–Jun 2024).

---

## Project Structure

```
_dbt/
├── analyses/             # Validation queries for data-backed claims in this README
├── macros/               # Reusable cleaning and derivation logic
├── models/
│   ├── _docs/            # Reusable doc blocks by domain
│   ├── staging/          # One model per source table; cleaning, typing, deduplication
│   ├── intermediate/     # Joins, enrichment, and derived metrics
│   ├── marts/            # Business-grain incremental tables for consumption
│   └── semantic/         # MetricFlow time spine + metrics.yml semantic layer spec
├── seeds/                # Source data proxies (replaced by {{ source() }} on Snowflake)
└── tests/                # Singular data tests
semantic/
└── semantic_model.yaml   # Snowflake Cortex Analyst spec (LLM query grounding)
```

Analyses use `{{ ref() }}` and compile via `dbt compile --select <analysis_name>`. Compiled SQL lands in `target/compiled/` and can be run directly against any target.

### Layer responsibilities

**Staging** — raw → typed. Every field cast to its final type here. Source issues resolved: millennium date defect, European numeric format, stringified Python lists, 4 duplicate opportunities. Each source table has a companion audit model (`stg_gtm__*__audit`) that surfaces data quality findings as a dead letter queue without blocking the main model. All staging models are views with contract enforcement.

**Intermediate** — typed → enriched. Joins, channel derivation, funnel aggregation, LTV estimation, and audit rollup to calendar month. All intermediate models are views with contract enforcement. Types are cast on initialization for downstream propagation. See type notes in each model for specifics.

**Marts** — enriched → business grain. Four models: three incremental (`delete+insert`) and one table-materialized. Each is contract-enforced with explicit numeric precision on every column. Lookback window for incrementals controlled by `var('incremental_lookback_months')` (default: 2). All columns cast explicitly in the `final` CTE to match contract-declared types — required for Postgres, where `on_schema_change='fail'` treats `text` vs `varchar` and `numeric` vs `numeric(p,s)` as mismatches.

**Semantic layer** — governed metric definitions on top of the marts. `_dbt/models/metrics.yml` defines the dbt Semantic Layer / MetricFlow spec. `semantic/semantic_model.yaml` defines the parallel Snowflake Cortex Analyst spec. Both ground an LLM query agent in canonical metric semantics so plain-English GTM questions resolve to correct, governed SQL without an analyst in the loop.

---

## Macros

| Macro                                      | Purpose                                                           |
| ------------------------------------------ | ----------------------------------------------------------------- |
| `fix_millennium_date(col)`                 | Corrects `0024-xx-xx` → `2024-xx-xx` date defect in source        |
| `parse_european_numeric(col)`              | Strips `US$`, tab, non-breaking space; converts comma decimal     |
| `clean_timestamp(col)`                     | Nullif/trim/cast to timestamp                                     |
| `clean_category_list(col)`                 | Strips Python list syntax (`['a', 'b']` → `a, b`)                 |
| `derive_channel(form_submission_date_col)` | Returns `inbound` or `outbound` based on form submission presence |
| `hours_between(start_col, end_col)`        | `extract(epoch from (end - start)) / 3600.0`                      |

---

## Data Models

### `mart_cac_ltv`

One row per channel per month, scoped to Jan–Jun 2024. Combines funnel conversion counts with cost data to produce CAC and LTV estimates.

**Key modeling decisions:**

- Advertising spend attributed exclusively to inbound — paid ads drive inbound form submissions by construction. Outbound BDR activity generates headcount cost only.
- Salary costs arrive pre-split by channel in source — no allocation required.
- CAC computed as `total_cost_usd / closed_won` within entry-month cohort. Cohort attribution understates true CAC for long sales cycles but is appropriate for pipeline efficiency analysis.
- Inner join to expenses naturally limits rows to months where cost data exists. Full funnel history lives in `mart_gtm_funnel`.
- Incremental: lookback filter applied to both `funnel` and `won_leads_monthly` CTEs to keep LTV averages consistent with reprocessed cohort months. Expenses not filtered — six rows, fully loaded on initial run.

### `mart_gtm_funnel`

One row per channel per month, full history from 2020. Funnel conversion counts and rates plus average engagement timing metrics per cohort.

**Key modeling decisions:**

- Lead month is channel-aware: inbound uses `form_submission_date`, outbound uses `first_sales_call_at`.
- `demo_set_to_held_rate` declared `numeric(8,4)` not `numeric(6,4)`. Cohort timing means `demos_held` can exceed `demos_set` in a given entry month (demo set in month N, held in month N+1), producing ratios > 1.0. This is a data characteristic, not a quality issue.
- Engagement averages scoped to converted leads — measuring the path that worked. `avg_activity_count` includes all leads as a baseline signal.
- Incremental: lookback filter on `leads_with_month` (row-level) and on the final join to `funnel`. Filter not applied inside the `funnel` CTE itself — it is already aggregated, and filtering it would silently drop cohort rows rather than reprocess them.

### `mart_leading_indicators`

One row per lead. Scores the full prospect universe with a rule-based conversion probability tier derived from observed conversion rates.

| Tier      | Rule                                                        | Observed conversion rate |
| --------- | ----------------------------------------------------------- | ------------------------ |
| hot       | `connected_with_decision_maker = true`                      | 24.5%                    |
| warm      | activity > 0 AND (speed ≤ 72h OR predicted GMV > $6,279/mo) | 5.2%                     |
| converted | `is_converted = true` (not otherwise hot/warm)              | — active customer        |
| cold      | all remaining                                               | 1.6%                     |
| null      | `is_disqualified = true`                                    | excluded from scoring    |

Tier thresholds are data-derived: speed 72h = converted-lead p75; GMV $6,279 = dataset p75. Hot and warm signals take precedence over `converted` — a converted lead who also reached a DM stays `hot` as the stronger upsell signal. `activity_density_score` (total touches / days in funnel) is designed for ranking within tiers, not as a standalone cutoff.

Incremental: watermark on `last_sales_activity_at`. Any lead touched since the last run reprocesses — catches status changes (e.g. `working` → `disqualified`) as well as new activity. Watermark resolved in a separate `watermark` CTE joined via `cross join` to satisfy Postgres's restriction on aggregates in WHERE clauses.

### `mart_data_quality_monitor`

One row per calendar month, scoped to Jan–Jun 2024. Joins estimated revenue (`new_customers_won × avg_estimated_ltv_usd` from `mart_cac_ltv`) with monthly audit finding counts and density metrics rolled up from both source tables via `int_audit__by_month`.

**Key modeling decisions:**

- Revenue proxy is estimated annual LTV attributed to cohort-month won leads — directional signal only, consistent with `mart_cac_ltv`. Not realized revenue.
- Audit findings attributed to their source record's calendar anchor: `form_submission_date` for leads, `created_at` for opportunities.
- `overall_audit_density` = affected records / total records in scope. Provides a normalized bad-data rate independent of volume changes.
- MoM delta columns (`mom_revenue_change_usd`, `mom_audit_findings_change`) computed via `lag()`. Null for the first month in the window.
- Depends on `mart_cac_ltv` — mart-to-mart reference, intentional. The expense window scoping already lives in `mart_cac_ltv`; duplicating it here would create drift risk.

---

<<<<<<< HEAD
<<<<<<< Updated upstream
=======
=======
>>>>>>> 58338d78a675b9ca5f3f581518e4316ec8b87430
## Semantic Layer

The mart layer is designed to serve both human analysts and LLM query agents. Two semantic specs sit on top of the marts — one for each integration path.

### dbt Semantic Layer / MetricFlow (`_dbt/models/metrics.yml`)

Defines three semantic models (`gtm_channel_month`, `gtm_funnel`, `lead_universe`) and 15 named metrics covering CAC, LTV, funnel rates, and lead tier counts. Requires dbt Core 1.6+ and the dbt Semantic Layer API.

```bash
# Example queries via dbt-sl CLI or a connected BI tool
<<<<<<< HEAD
mf query --metrics cac_usd,cac_ltv_ratio --group-by channel,month_date__month
mf query --metrics hot_lead_count,warm_lead_count --group-by channel
mf query --metrics lead_to_demo_set_rate --group-by channel,month_date__month
=======
mf query --metrics cac_usd,cac_ltv_ratio --group-by channel,metric_time__month
mf query --metrics hot_lead_count,warm_lead_count --group-by channel
mf query --metrics lead_to_demo_set_rate --group-by channel,metric_time__month
>>>>>>> 58338d78a675b9ca5f3f581518e4316ec8b87430
```

### Snowflake Cortex Analyst (`semantic/semantic_model.yaml`)

Parallel spec in Snowflake's format. Describes the same three tables with `base_table` references pointing at `DEMO_DB.GTM_CASE_PROD.*`. Includes six `verified_queries` — pre-validated question/SQL pairs used as few-shot grounding examples for the query agent.

Upload to a Snowflake stage and reference in your Cortex Analyst configuration. Once mounted, the layer supports plain-English GTM queries without an analyst in the loop:

> "What was outbound CAC each month in 2024?"  
> "Is the CAC:LTV ratio improving or deteriorating?"  
> "How many hot leads are there right now?"  
> "Which channel has better unit economics?"

Both specs include rich column descriptions with business context — tier thresholds, cohort attribution caveats, channel definitions — so query agents produce accurate, grounded answers rather than syntactically correct but semantically wrong SQL.

---

<<<<<<< HEAD
>>>>>>> Stashed changes
=======
>>>>>>> 58338d78a675b9ca5f3f581518e4316ec8b87430
## Source Data Quality Notes

| Issue                    | Detail                                                                                    | Resolution                                                                                                                        |
| ------------------------ | ----------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| Millennium date defect   | `FORM_SUBMISSION_DATE`, `CLOSE_DATE`, `DEMO_SET_DATE` stored as `0024-xx-xx`              | `fix_millennium_date` macro corrects at staging                                                                                   |
| European numeric format  | Expense and GMV fields use `US$\t`, non-breaking space thousands separator, comma decimal | `parse_european_numeric` macro handles at staging                                                                                 |
| Stringified Python lists | `MARKETPLACES_USED`, `OLO_TOOLS`, `CUISINE_TYPES` stored as `"['doordash', 'grubhub']"`   | `clean_category_list` macro strips brackets/quotes                                                                                |
| Opportunity duplicates   | 4 exact duplicate rows in opportunities source                                            | `distinct *` in `stg_gtm__opportunities`                                                                                          |
| Negative speed values    | 26 leads where `first_sales_call_at` precedes `form_submission_date` (up to -968h)        | Surfaced in audit, not filtered. Likely outbound-first leads later reclassified as inbound. Filter `> 0` for conversion analysis. |
| Stale inbound leads      | 1,319 leads with 2020 form submissions not contacted until 2024 (up to 34,457h)           | Surfaced in audit, flagged for recency filtering in speed-based analysis.                                                         |
| Null lost reason         | 4 closed lost opportunities with null `lost_reason_c`                                     | Surfaced in audit, passed through as null in staging.                                                                             |

---

## Running Locally (Postgres)

Seeds must be loaded before build. `dbt build` interleaves seeds with model tests in a way that causes tests to run before seed data exists — separate the steps:

```bash
cd _dbt
pip install dbt-postgres
dbt deps
dbt seed                                                  # load source data once
dbt build --full-refresh --exclude resource_type:seed     # initial build
dbt build --exclude resource_type:seed                    # subsequent incremental runs
```

Set Postgres credentials in `~/.dbt/profiles.yml` under the `dev` target (`gtm_case_dev` schema).

---

## Switching to Snowflake

Two changes required when targeting Snowflake (`demo_db.gtm_case`):

1. **`profiles.yml`** — point the `prod` target at Snowflake. Credentials go in `~/.dbt/profiles.yml`; do not commit them.

2. **`int_restaurant__profile`** — one Postgres-specific function:

   ```sql
   -- Postgres
   array_length(string_to_array(col, ','), 1)
   -- Snowflake
   array_size(split(col, ','))
   ```

3. **Final CTE type casts** — the explicit `::varchar` and `::numeric(p,s)` casts in mart `final` CTEs are Postgres workarounds for `on_schema_change='fail'` type matching. Snowflake handles these equivalences natively; the casts are harmless to leave in place but can be removed for cleaner SQL.

4. **Semantic layer** — update `base_table` references in `semantic/semantic_model.yaml` if your Snowflake database or schema names differ from `DEMO_DB.GTM_CASE_PROD`. No changes to `_dbt/models/metrics.yml` required.

Staging models already use `{{ source('gtm_case', 'table_name') }}` — no changes needed there. Seeds remain as the local dev proxy; on Snowflake they are bypassed entirely by the source definitions.

---

## dbt Docs

Docs are published to GitHub Pages on manual trigger via the GitHub Actions UI: **Actions → Publish dbt Docs → Run workflow**.

To generate and browse docs locally:

```bash
cd _dbt
dbt docs generate
dbt docs serve
```

> **Note:** GitHub Pages deployment requires the repository to be public, or a GitHub Pro/Team/Enterprise account. On a free plan with a private repo the deploy step will succeed but the page will not be accessible. Docs can always be served locally with `dbt docs serve`.

---

## GTM Opportunities

### Opportunity 1: The Outbound Motion Is Deteriorating — Diagnose Before Scaling

Outbound CAC increased 4x over six months while headcount cost grew only 31%. The problem is yield, not cost.

> Verify: [`analyses/july_outbound_completeness`](_dbt/analyses/july_outbound_completeness.sql) · [`analyses/outbound_funnel_stage_rates`](_dbt/analyses/outbound_funnel_stage_rates.sql) · [`analyses/outbound_headcount_cost_trend`](_dbt/analyses/outbound_headcount_cost_trend.sql)

| Month    | Outbound CAC | Won | CAC:LTV |
| -------- | ------------ | --- | ------- |
| Jan 2024 | $1,586       | 24  | 5.9x    |
| Feb 2024 | $1,872       | 23  | 4.2x    |
| Mar 2024 | $2,605       | 17  | 3.2x    |
| Apr 2024 | $2,766       | 17  | 3.8x    |
| May 2024 | $2,092       | 23  | 4.7x    |
| Jun 2024 | $6,250       | 8   | 1.6x    |

Won customers per month dropped from 24 to 8 on roughly constant lead volume (~2,000–2,700/month). Overall outbound conversion rate fell from 0.90% in January to 0.37% in June. A 1.6x CAC:LTV ratio in June is approaching the floor of viability for a subscription business. July data covers only the first 10 days of the month and is excluded from trend analysis.

**What the data cannot tell us — and what to instrument:**

The dataset does not capture list quality, BDR tenure, or the BizOps enrichment process. `outbound_funnel_stage_rates` shows the drop is concentrated at the top of the funnel — lead → demo set rate fell from 5.3% in January to 1.8% in June — which points to list quality or contact rate rather than AE close performance.

**Recommendation:** Before scaling outbound headcount, instrument list source and enrichment quality as CRM dimensions. Segment conversion rates by list source, prospect tier, and BDR tenure. The current data model can absorb these dimensions in `int_leads__enriched` and surface them in `mart_gtm_funnel` without structural changes.

---

### Opportunity 2: Lead Scoring as Queue Prioritization Infrastructure

The behavioral signals already in the CRM are strongly predictive of conversion. The opportunity is operationalizing them as a real-time SDR/BDR prioritization queue rather than working leads in arrival order.

> Verify: [`analyses/inbound_speed_conversion_buckets`](_dbt/analyses/inbound_speed_conversion_buckets.sql) · [`analyses/inbound_speed_recency_breakdown`](_dbt/analyses/inbound_speed_recency_breakdown.sql) · [`analyses/july_outbound_won_detail`](_dbt/analyses/july_outbound_won_detail.sql)

**Decision maker contact** is the strongest binary signal: 24.5% conversion when reached vs 2.5% otherwise — a 10x lift. This drives the `hot` tier (9,801 leads, 36% of the prospect universe).

**Response speed for inbound** shows a sharp and consistent cliff. The `> 168h` bucket is dominated by 1,418 stale leads with 2020 form submissions contacted in 2024; decomposing by recency makes the pattern clearer:

| Speed to first contact | Conversion rate |
| ---------------------- | --------------- |
| ≤ 24 hours             | 48.6%           |
| 24–72 hours            | 46.4%           |
| 72–168 hours           | 38.1%           |
| 1 week – 1 month       | 19.9%           |
| 1 month – 1 year       | 15.1%           |
| > 1 year (stale)       | 8.9%            |

The actionable boundary is 72 hours: leads contacted within that window convert at ~47%; after a week conversion drops to ~20% and keeps falling. Suppressing the 1,418 stale leads from the active queue frees SDR capacity for the high-conversion fresh inbound pool.

**Activity engagement** confirms untouched leads do not self-convert: zero-activity leads convert at 1.3% vs 12.1% for any-activity leads, making `total_activity_count = 0` a reliable cold-tier suppression signal.

**GMV as a quality signal:** The LTV spread across won customers is meaningful — p90 customers generate $10,515/yr vs $7,216 for p25, a $3,299 annual difference. At 600 won customers/year, shifting the mix toward higher-GMV conversions is worth ~$2M in recurring revenue without acquiring additional customers. The warm tier threshold (GMV > $6,279 = dataset p75) captures this segment.

**What `mart_leading_indicators` enables:**

A scored, tiered view of every lead: daily hot-tier queue for DM-reachable contacts; warm-tier flagging of high-GMV or fast-response-window inbound leads; cold-tier triage to suppress stale inventory; `converted` tier separating active customers for upsell/expansion targeting. `activity_density_score` ranks within tiers by engagement intensity independent of funnel duration.

**The longer-term extension:** The rule-based tier model is a foundation. With sufficient history, thresholds become trained probability estimates — the mart grain and feature set (speed, activity, GMV, DM contact, marketplace signals) map directly to a binary classification training dataset. The infrastructure supports that upgrade without structural changes.
