# Latest daily health status per model (gold)

Executed 2026-11-26 08:40 UTC on serverless compute against Unity Catalog.

```sql
select model_name, metric_date, n_metrics, n_green, n_amber, n_red, round(health_score,3) health_score, status, red_metrics, amber_metrics from demo_unified_model_monitoring.hub_gold.fact_model_health_daily qualify row_number() over (partition by model_id order by metric_date desc)=1 order by model_name
```

Result (5 rows):

| model_name | metric_date | n_metrics | n_green | n_amber | n_red | health_score | status | red_metrics | amber_metrics |
|---|---|---|---|---|---|---|---|---|---|
| Counterparty Default PD | 2026-11-25 | 4 | 2 | 0 | 2 | 50.0 | red | Threshold calibration, Data drift (PSI) |  |
| Credit Policy Assistant (RAG) | 2026-11-25 | 10 | 9 | 1 | 0 | 95.0 | amber |  | Precision@K |
| RM Client Brief Agent | 2026-11-26 | 7 | 4 | 0 | 3 | 57.1 | red | Memory conflict rate, Memory-grounded answers, PII held in memory |  |
| Shared LLM Endpoint | 2026-11-25 | 3 | 2 | 1 | 0 | 83.3 | amber |  | p95 latency |
| Trade Volume Forecast | 2026-11-25 | 8 | 7 | 1 | 0 | 93.8 | amber |  | Data drift (PSI) |
