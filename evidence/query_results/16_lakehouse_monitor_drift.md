# Lakehouse Monitoring drift metrics on PD inference log (sample of latest windows)

Executed 2026-11-26 08:41 UTC on serverless compute against Unity Catalog.

```sql
select window.start window_start, window.end window_end, column_name, drift_type, slice_key, slice_value, round(js_distance,4) js_distance, round(population_stability_index,4) psi from demo_unified_model_monitoring.credit_monitoring.pd_inference_log_drift_metrics where column_name<>':table' and slice_key is null order by window.start desc, column_name limit 40
```

Result (40 rows):

| window_start | window_end | column_name | drift_type | slice_key | slice_value | js_distance | psi |
|---|---|---|---|---|---|---|---|
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | annual_revenue_usd_m | CONSECUTIVE | None | None | nan | 0.0467 |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | annual_revenue_usd_m | BASELINE | None | None | nan | 0.0131 |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | annual_revenue_usd_m | CONSECUTIVE | None | None | nan | 0.0467 |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | collateral_coverage | CONSECUTIVE | None | None | nan | 0.0582 |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | collateral_coverage | CONSECUTIVE | None | None | nan | 0.0582 |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | collateral_coverage | BASELINE | None | None | nan | 0.0264 |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | counterparty_id | CONSECUTIVE | None | None | nan | nan |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | counterparty_id | BASELINE | None | None | nan | nan |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | counterparty_id | CONSECUTIVE | None | None | nan | nan |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | country | BASELINE | None | None | 0.0466 | nan |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | country | CONSECUTIVE | None | None | 0.0772 | nan |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | country | CONSECUTIVE | None | None | 0.0772 | nan |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | days_past_due_max_12m | CONSECUTIVE | None | None | nan | 0.0042 |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | days_past_due_max_12m | CONSECUTIVE | None | None | nan | 0.0042 |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | days_past_due_max_12m | BASELINE | None | None | nan | 0.0072 |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | debt_to_ebitda | CONSECUTIVE | None | None | nan | 0.0483 |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | debt_to_ebitda | CONSECUTIVE | None | None | nan | 0.0483 |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | debt_to_ebitda | BASELINE | None | None | nan | 0.2938 |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | interest_coverage | CONSECUTIVE | None | None | nan | 0.078 |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | interest_coverage | CONSECUTIVE | None | None | nan | 0.078 |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | interest_coverage | BASELINE | None | None | nan | 0.0204 |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | internal_rating | BASELINE | None | None | nan | 0.0216 |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | internal_rating | CONSECUTIVE | None | None | nan | 0.0411 |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | internal_rating | CONSECUTIVE | None | None | nan | 0.0411 |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | label | CONSECUTIVE | None | None | nan | nan |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | label | BASELINE | None | None | nan | nan |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | label | CONSECUTIVE | None | None | nan | nan |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | label_time | BASELINE | None | None | nan | nan |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | label_time | CONSECUTIVE | None | None | nan | nan |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | label_time | CONSECUTIVE | None | None | nan | nan |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | pd_probability | CONSECUTIVE | None | None | nan | 0.0768 |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | pd_probability | BASELINE | None | None | nan | 0.2566 |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | pd_probability | CONSECUTIVE | None | None | nan | 0.0768 |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | prediction | CONSECUTIVE | None | None | nan | 0.0134 |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | prediction | CONSECUTIVE | None | None | nan | 0.0134 |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | prediction | BASELINE | None | None | nan | 0.1156 |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | product | CONSECUTIVE | None | None | 0.0202 | nan |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | product | CONSECUTIVE | None | None | 0.0202 | nan |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | product | BASELINE | None | None | 0.0535 | nan |
| 2026-11-25 00:00:00 | 2026-11-26 00:00:00 | record_source | CONSECUTIVE | None | None | 0.8326 | nan |
