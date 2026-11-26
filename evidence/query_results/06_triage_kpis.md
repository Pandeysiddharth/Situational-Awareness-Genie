# Alert triage KPIs per owner team (Lakebase-synced triage inbox)

Executed 2026-11-26 08:40 UTC on serverless compute against Unity Catalog.

```sql
select * from demo_unified_model_monitoring.hub_gold.fact_triage_kpis order by owner_team
```

Result (3 rows):

| as_of | owner_team | open_alerts | open_p1 | unacknowledged | mtta_hours | mttr_hours | false_positive_rate |
|---|---|---|---|---|---|---|---|
| 2026-11-26 | AI Factory | 5 | 1 | 5 | 3.5 | 36.1 | 0.083 |
| 2026-11-26 | CIB Credit Risk Analytics | 7 | 4 | 0 | 3.0 | 28.7 | 0.25 |
| 2026-11-26 | CIB Markets Analytics | 1 | 0 | 0 | 2.9 | 38.1 | 0.0 |
