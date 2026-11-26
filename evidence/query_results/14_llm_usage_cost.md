# LLM token usage and cost by day (last 14 days)

Executed 2026-11-26 08:41 UTC on serverless compute against Unity Catalog.

```sql
select date(event_time) d, endpoint_name, count(*) calls, sum(input_tokens) in_tok, sum(output_tokens) out_tok, round(sum(input_tokens)*1.0/1e6 + sum(output_tokens)*3.0/1e6, 4) est_cost_usd, round(percentile(latency_ms,0.95)) p95_latency_ms, sum(case when status_code<>200 then 1 else 0 end) errors from demo_unified_model_monitoring.ai_factory_monitoring.llm_usage_log where event_time >= (select max(event_time) - interval 14 days from demo_unified_model_monitoring.ai_factory_monitoring.llm_usage_log) group by 1,2 order by 1,2
```

Result (15 rows):

| d | endpoint_name | calls | in_tok | out_tok | est_cost_usd | p95_latency_ms | errors |
|---|---|---|---|---|---|---|---|
| 2026-11-11 | databricks-claude-sonnet-4-5 | 221 | 338818 | 51573 | 0.4935 | 2060.0 | 1 |
| 2026-11-12 | databricks-claude-sonnet-4-5 | 1251 | 1961521 | 309135 | 2.8889 | 2130.0 | 8 |
| 2026-11-13 | databricks-claude-sonnet-4-5 | 1345 | 2092666 | 326108 | 3.071 | 2343.0 | 6 |
| 2026-11-14 | databricks-claude-sonnet-4-5 | 477 | 745686 | 116619 | 1.0955 | 2424.0 | 3 |
| 2026-11-15 | databricks-claude-sonnet-4-5 | 458 | 733780 | 117434 | 1.0861 | 2201.0 | 0 |
| 2026-11-16 | databricks-claude-sonnet-4-5 | 1123 | 1768751 | 270151 | 2.5792 | 2446.0 | 10 |
| 2026-11-17 | databricks-claude-sonnet-4-5 | 1239 | 1991663 | 310560 | 2.9233 | 2249.0 | 10 |
| 2026-11-18 | databricks-claude-sonnet-4-5 | 1276 | 2048590 | 319041 | 3.0057 | 2476.0 | 8 |
| 2026-11-19 | databricks-claude-sonnet-4-5 | 1225 | 1976462 | 307493 | 2.8989 | 2598.0 | 8 |
| 2026-11-20 | databricks-claude-sonnet-4-5 | 1237 | 2009525 | 315520 | 2.9561 | 2285.0 | 7 |
| 2026-11-21 | databricks-claude-sonnet-4-5 | 473 | 788365 | 123510 | 1.1589 | 2330.0 | 1 |
| 2026-11-22 | databricks-claude-sonnet-4-5 | 445 | 736809 | 113803 | 1.0782 | 2199.0 | 3 |
| 2026-11-23 | databricks-claude-sonnet-4-5 | 1375 | 2233623 | 345550 | 3.2703 | 2432.0 | 6 |
| 2026-11-24 | databricks-claude-sonnet-4-5 | 1215 | 1991122 | 312567 | 2.9288 | 2384.0 | 4 |
| 2026-11-25 | databricks-claude-sonnet-4-5 | 321 | 406894 | 39832 | 0.5264 | 4922.0 | 0 |
