# PD inference volume and mean prediction by day (last 14 days)

Executed 2026-11-26 08:40 UTC on serverless compute against Unity Catalog.

```sql
select date(request_time) d, record_source, count(*) n, round(avg(pd_probability),4) avg_pd, round(avg(prediction),4) flag_rate, round(avg(label),4) observed_default_rate from demo_unified_model_monitoring.credit_monitoring.pd_inference_log where request_time >= (select max(request_time) - interval 14 days from demo_unified_model_monitoring.credit_monitoring.pd_inference_log) group by 1,2 order by 1,2
```

Result (15 rows):

| d | record_source | n | avg_pd | flag_rate | observed_default_rate |
|---|---|---|---|---|---|
| 2026-11-11 | synthetic_history | 87 | 0.1898 | 0.2529 | 0.2069 |
| 2026-11-12 | synthetic_history | 418 | 0.1936 | 0.2895 | 0.2321 |
| 2026-11-13 | synthetic_history | 456 | 0.1935 | 0.2873 | 0.239 |
| 2026-11-14 | synthetic_history | 75 | 0.1445 | 0.16 | 0.2 |
| 2026-11-15 | synthetic_history | 87 | 0.1867 | 0.3218 | 0.2299 |
| 2026-11-16 | synthetic_history | 509 | 0.1802 | 0.2574 | 0.2024 |
| 2026-11-17 | synthetic_history | 456 | 0.184 | 0.2741 | 0.2121 |
| 2026-11-18 | synthetic_history | 384 | 0.1719 | 0.2474 | nan |
| 2026-11-19 | synthetic_history | 420 | 0.2002 | 0.2833 | nan |
| 2026-11-20 | synthetic_history | 456 | 0.1867 | 0.2763 | nan |
| 2026-11-21 | synthetic_history | 79 | 0.1969 | 0.3165 | nan |
| 2026-11-22 | synthetic_history | 66 | 0.1658 | 0.2273 | nan |
| 2026-11-23 | synthetic_history | 456 | 0.1806 | 0.2456 | nan |
| 2026-11-24 | synthetic_history | 388 | 0.1734 | 0.2397 | nan |
| 2026-11-25 | live | 550 | 0.1985 | 0.2909 | nan |
