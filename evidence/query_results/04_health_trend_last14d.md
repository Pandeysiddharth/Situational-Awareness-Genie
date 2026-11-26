# Daily health status counts, last 14 days

Executed 2026-11-26 08:40 UTC on serverless compute against Unity Catalog.

```sql
select metric_date, count(*) models, sum(case when status='RED' then 1 else 0 end) red, sum(case when status='AMBER' then 1 else 0 end) amber, sum(case when status='GREEN' then 1 else 0 end) green, round(avg(health_score),3) avg_health from demo_unified_model_monitoring.hub_gold.fact_model_health_daily where metric_date >= (select max(metric_date) - interval 13 days from demo_unified_model_monitoring.hub_gold.fact_model_health_daily) group by 1 order by 1
```

Result (14 rows):

| metric_date | models | red | amber | green | avg_health |
|---|---|---|---|---|---|
| 2026-11-13 | 5 | 0 | 0 | 0 | 79.76 |
| 2026-11-14 | 5 | 0 | 0 | 0 | 79.76 |
| 2026-11-15 | 5 | 0 | 0 | 0 | 78.38 |
| 2026-11-16 | 5 | 0 | 0 | 0 | 81.22 |
| 2026-11-17 | 5 | 0 | 0 | 0 | 79.96 |
| 2026-11-18 | 5 | 0 | 0 | 0 | 82.28 |
| 2026-11-19 | 5 | 0 | 0 | 0 | 79.96 |
| 2026-11-20 | 5 | 0 | 0 | 0 | 86.2 |
| 2026-11-21 | 5 | 0 | 0 | 0 | 86.2 |
| 2026-11-22 | 5 | 0 | 0 | 0 | 87.44 |
| 2026-11-23 | 5 | 0 | 0 | 0 | 87.44 |
| 2026-11-24 | 5 | 0 | 0 | 0 | 87.22 |
| 2026-11-25 | 5 | 0 | 0 | 0 | 81.34 |
| 2026-11-26 | 1 | 0 | 0 | 0 | 57.1 |
