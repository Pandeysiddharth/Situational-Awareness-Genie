# Captured output: `07d_UMM_Phase7D_Triage_Sync.ipynb`

This is the plain-text output stored in the notebook the last time it ran interactively on Databricks. HTML tables are converted to markdown.


## Cell 1

```text
✓ 0 new alerts added to the triage inbox
✓ 0 open alerts auto-resolved · 0 in-progress alerts flagged as recovered
```
.table-result-container { max-height: 300px; overflow: auto; } table, th, td { border: 1px solid black; border-collapse: collapse; } th, td { padding: 5px; } th { text-align: left; } as_of owner_team open_alerts open_p1 unacknowledged mtta_hours mttr_hours false_positive_rate 2026-11-26 AI Factory 5 1 5 3.5 36.1 0.083 2026-11-26 CIB Markets Analytics 1 0 0 2.9 38.1 0.0 2026-11-26 CIB Credit Risk Analytics 7 4 0 3.0 28.7 0.25
```text
✓ umm-demo-daily: added ['memory_traffic', 'memory_metrics']
✓ umm-demo-hub-refresh: added ['triage_sync']
One-time step: in UMM_Phase7C_Memory_Metrics → Environment, add mlflow[databricks]>=3.1 and databricks-agents (same as 3C), so scheduled runs have the judge packages.
```
.table-result-container { max-height: 300px; overflow: auto; } table, th, td { border: 1px solid black; border-collapse: collapse; } th, td { padding: 5px; } th { text-align: left; } check result value Triage inbox rows PASS 48 Triage events rows PASS 84 KPI rows (one per owner team) PASS 3
