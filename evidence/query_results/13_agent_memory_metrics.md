# Agent memory quality/governance metrics

Executed 2026-11-26 08:40 UTC on serverless compute against Unity Catalog.

```sql
select * from demo_unified_model_monitoring.ai_factory_monitoring.agent_memory_daily order by 1
```

Result (7 rows):

| metric_date | contract_name | value | n |
|---|---|---|---|
| 2026-11-26 | memory_pii_disclosure_rate | 0.0 | 1 |
| 2026-11-26 | memory_grounded_rate | 0.375 | 24 |
| 2026-11-26 | memory_conflict_rate | 0.5 | 2 |
| 2026-11-26 | memory_leakage_rate | 0.0 | 16 |
| 2026-11-26 | pii_in_memory_rate | 0.1111111111111111 | 27 |
| 2026-11-26 | stale_memory_share | 0.037037037037037035 | 27 |
| 2026-11-26 | memory_recall_rate | 1.0 | 32 |
