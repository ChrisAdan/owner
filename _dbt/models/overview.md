{% docs __overview__ %}

# Owner GTM Analytics — Case Study

Built by [Chris Adan](https://github.com/ChrisAdan) · [GitHub Repo](https://github.com/ChrisAdan/owner)

---

A dbt analytics engineering project built on Owner's GTM dataset: four source tables covering **27,056 leads**, **2,794 opportunities**, and **six months of channel cost data** (Jan–Jun 2024).

## Layers

| Layer            | Grain                     | Purpose                                                                        |
| ---------------- | ------------------------- | ------------------------------------------------------------------------------ |
| **Staging**      | One row per source record | Cleaning, typing, deduplication. Contract-enforced views.                      |
| **Intermediate** | Joins and aggregations    | Enrichment, funnel aggregation, LTV estimation, audit rollups.                 |
| **Marts**        | Business grain            | Incremental tables for consumption. CAC/LTV, funnel, lead scoring, DQ monitor. |
| **Audit**        | One row per finding       | Dead letter queue for source data anomalies in leads and opportunities.        |

## Key Marts

**`mart_cac_ltv`** — CAC and LTV by channel and month, Jan–Jun 2024. Outbound CAC deteriorated 4x over six months on flat lead volume — the drop is concentrated at lead → demo set rate, pointing to list quality not AE performance.

**`mart_gtm_funnel`** — Full funnel history from 2020. Conversion rates, demo rates, and engagement timing per channel per month.

**`mart_leading_indicators`** — One row per lead. Rule-based conversion tier (hot / warm / converted / cold) derived from observed conversion rates. Decision maker contact = 10x lift. Sub-72h inbound response = ~2x lift.

**`mart_data_quality_monitor`** — Monthly audit finding counts joined to estimated revenue. Designed to surface data quality spikes coinciding with revenue drops.

## Source Data Notes

| Issue                                    | Resolution                                       |
| ---------------------------------------- | ------------------------------------------------ |
| Millennium date defect (`0024-xx-xx`)    | `fix_millennium_date` macro at staging           |
| European numeric format                  | `parse_european_numeric` macro at staging        |
| Stringified Python lists                 | `clean_category_list` macro at staging           |
| 4 duplicate opportunities                | `distinct *` in `stg_gtm__opportunities`         |
| 26 leads with negative speed-to-contact  | Surfaced in audit, not filtered                  |
| 1,319 stale 2020 leads contacted in 2024 | Surfaced in audit, flagged for recency filtering |

## Navigation Tips

Use the **Project** tab to explore models by layer. Use the **Database** tab to browse by schema. Click the lineage icon (bottom-right) to open the full DAG — sources are lime green, seeds brown, staging blue, intermediate plum, marts amber.

{% enddocs %}
