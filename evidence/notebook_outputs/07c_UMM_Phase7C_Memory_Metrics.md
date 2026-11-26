# Captured output: `07c_UMM_Phase7C_Memory_Metrics.ipynb`

This is the plain-text output stored in the notebook the last time it ran interactively on Databricks. HTML tables are converted to markdown.


## Cell 1

```text
Installing mlflow[databricks]>=3.1 and databricks-agents …
mlflow 3.18.0 · databricks-agents 1.11.0
57 memory turns to score
✓ scored 57 turns → demo_unified_model_monitoring.ai_factory_monitoring.agent_memory_metrics
✓ store scan: 27 entries · 3 with contact details · 1 stale or volatile
```
.table-result-container { max-height: 300px; overflow: auto; } table, th, td { border: 1px solid black; border-collapse: collapse; } th, td { padding: 5px; } th { text-align: left; } metric_date contract_name value n 2026-11-26 memory_conflict_rate 0.5 2 2026-11-26 memory_grounded_rate 0.375 24 2026-11-26 memory_leakage_rate 0.0 16 2026-11-26 memory_pii_disclosure_rate 0.0 1 2026-11-26 memory_recall_rate 1.0 32 2026-11-26 pii_in_memory_rate 0.1111111111111111 27 2026-11-26 stale_memory_share 0.037037037037037035 27
```text
✓ registered 7 memory metrics in dim_metric, map_model_metric and metric_source_map
```
.table-result-container { max-height: 300px; overflow: auto; } table, th, td { border: 1px solid black; border-collapse: collapse; } th, td { padding: 5px; } th { text-align: left; } check result value Memory turns scored PASS 57 No cross-RM leakage PASS 0.0 Store scan finds the seeded phone number PASS 0.1111111111111111 Store scan finds stale or volatile entries PASS 0.037037037037037035 Recall works PASS 1.0 All 7 memory metrics have values PASS 7
```text
Expected story: leakage stays at 0 (store and actor scoping work); the store scan flags the seeded phone number (amber), so the governance team sees memory holding PII before any disclosure. Re-run the Phase 5 hub in refresh mode to see these on the dashboard.
```
