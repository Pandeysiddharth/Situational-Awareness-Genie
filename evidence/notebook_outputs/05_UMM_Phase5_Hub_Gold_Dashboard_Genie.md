# Captured output: `05_UMM_Phase5_Hub_Gold_Dashboard_Genie.ipynb`

This is the plain-text output stored in the notebook the last time it ran interactively on Databricks. HTML tables are converted to markdown.


## Cell 1

```text
Mode full · history from 2026-08-27 · PD threshold 0.225
  ⚠ rm_client_brief_agent.memory_recall_rate: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `drift_type` cannot be resolved. Did you mean one of the following? [`me
  ⚠ rm_client_brief_agent.memory_grounded_rate: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `drift_type` cannot be resolved. Did you mean one of the following? [`me
  ⚠ rm_client_brief_agent.memory_conflict_rate: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `drift_type` cannot be resolved. Did you mean one of the following? [`me
  ⚠ rm_client_brief_agent.memory_leakage_rate: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `drift_type` cannot be resolved. Did you mean one of the following? [`me
  ⚠ rm_client_brief_agent.memory_pii_disclosure_rate: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `drift_type` cannot be resolved. Did you mean one of the following? [`me
  ⚠ rm_client_brief_agent.pii_in_memory_rate: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `drift_type` cannot be resolved. Did you mean one of the following? [`me
  ⚠ rm_client_brief_agent.stale_memory_share: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `drift_type` cannot be resolved. Did you mean one of the following? [`me
✓ monitor values: 1,351 metric-days
```
```text
/home/spark-a1976972-4301-493f-9269-1a/.ipykernel/75/command-8099243853666333-3545793860:158: FutureWarning: DataFrameGroupBy.apply operated on the grouping columns. This behavior is deprecated, and in a future version of pandas the grouping columns will be excluded from the operation. Either pass `include_groups=False` to exclude the groupings or explicitly select the grouping columns after groupby to silence this warning.
  out = d.groupby(keys).apply(lambda g: pd.Series({k: f(g) for k, f in funcs.items()}))
/home/spark-a1976972-4301-493f-9269-1a/.ipykernel/75/command-8099243853666333-3545793860:158: FutureWarning: DataFrameGroupBy.apply operated on the grouping columns. This behavior is deprecated, and in a future version of pandas the grouping columns will be excluded from the operation. Either pass `include_groups=False` to exclude the groupings or explicitly select the grouping columns after groupby to silence this warning.
  out = d.groupby(keys).apply(lambda g: pd.Series({k: f(g) for k, f in funcs.items()}))
/home/spark-a1976972-4301-493f-9269-1a/.ipykernel/75/command-8099243853666333-3545793860:158: FutureWarning: DataFrameGroupBy.apply operated on the grouping columns. This behavior is deprecated, and in a future version of pandas the grouping columns will be excluded from the operation. Either pass `include_groups=False` to exclude the groupings or explicitly select the grouping columns after groupby to silence this warning.
  out = d.groupby(keys).apply(lambda g: pd.Series({k: f(g) for k, f in funcs.items()}))
/home/spark-a1976972-4301-493f-9269-1a/.ipykernel/75/command-8099243853666333-3545793860:158: FutureWarning: DataFrameGroupBy.apply operated on the grouping columns. This behavior is deprecated, and in a future version of pandas the grouping columns will be excluded from the operation. Either pass `include_groups=False` to exclude the groupings or explicitly select the grouping columns after groupby to silence this warning.
  out = d.groupby(keys).apply(lambda g: pd.Series({k: f(g) for k, f in funcs.items()}))
/home/spark-a1976972-4301-493f-9269-1a/.ipykernel/75/command-8099243853666333-3545793860:158: FutureWarning: DataFrameGroupBy.apply operated on the grouping columns. This behavior is deprecated, and in a future version of pandas the grouping columns will be excluded from the operation. Either pass `include_groups=False` to exclude the groupings or explicitly select the grouping columns after groupby to silence this warning.
  out = d.groupby(keys).apply(lambda g: pd.Series({k: f(g) for k, f in funcs.items()}))
/home/spark-a1976972-4301-493f-9269-1a/.ipykernel/75/command-8099243853666333-3545793860:158: FutureWarning: DataFrameGroupBy.apply operated on the grouping columns. This behavior is deprecated, and in a future version of pandas the grouping columns will be excluded from the operation. Either pass `include_groups=False` to exclude the groupings or explicitly select the grouping columns after groupby to silence this warning.
  out = d.groupby(keys).apply(lambda g: pd.Series({k: f(g) for k, f in funcs.items()}))
/home/spark-a1976972-4301-493f-9269-1a/.ipykernel/75/command-8099243853666333-3545793860:158: FutureWarning: DataFrameGroupBy.apply operated on the grouping columns. This behavior is deprecated, and in a future version of pandas the grouping columns will be excluded from the operation. Either pass `include_groups=False` to exclude the groupings or explicitly select the grouping columns after groupby to silence this warning.
  out = d.groupby(keys).apply(lambda g: pd.Series({k: f(g) for k, f in funcs.items()}))
/home/spark-a1976972-4301-493f-9269-1a/.ipykernel/75/command-8099243853666333-3545793860:158: FutureWarning: DataFrameGroupBy.apply operated on the grouping columns. This behavior is deprecated, and in a future version of pandas the grouping columns will be excluded from the operation. Either pass `include_groups=False` to exclude the groupings or explicitly select the grouping columns after groupby to silence this warning.
  out = d.groupby(keys).apply(lambda g: pd.Series({k: f(g) for k, f in funcs.items()}))
/home/spark-a1976972-4301-493f-9269-1a/.ipykernel/75/command-8099243853666333-3545793860:158: FutureWarning: DataFrameGroupBy.apply operated on the grouping columns. This behavior is deprecated, and in a future version of pandas the grouping columns will be excluded from the operation. Either pass `include_groups=False` to exclude the groupings or explicitly select the grouping columns after groupby to silence this warning.
  out = d.groupby(keys).apply(lambda g: pd.Series({k: f(g) for k, f in funcs.items()}))
/home/spark-a1976972-4301-493f-9269-1a/.ipykernel/75/command-8099243853666333-3545793860:158: FutureWarning: DataFrameGroupBy.apply operated on the grouping columns. This behavior is deprecated, and in a future version of pandas the grouping columns will be excluded from the operation. Either pass `include_groups=False` to exclude the groupings or explicitly select the grouping columns after groupby to silence this warning.
  out = d.groupby(keys).apply(lambda g: pd.Series({k: f(g) for k, f in funcs.items()}))
```
```text
✓ memory metrics: 7 metric-days from demo_unified_model_monitoring.ai_factory_monitoring.agent_memory_daily
✓ backfill values: 4,048 metric-days (incl. 807 pooled-window drift values)
✓ monitor vs backfill on 1,072 overlapping metric-days: 98.6% within 2%
✓ merged facts: 4,048 metric-days · {'backfill': 2162, 'hub_direct': 7, 'hub_pooled': 807, 'monitor': 1072}
✓ calibrated 13 thresholds:
               model_id              metric  amber    red  baseline_mean     sd
pd_counterparty_default            accuracy 0.8302 0.8007         0.8607 0.0102
pd_counterparty_default           precision 0.3876 0.3595         0.4438 0.0187
pd_counterparty_default              recall 0.4514 0.4042         0.5458 0.0315
pd_counterparty_default            f1_macro 0.6655 0.6425         0.7025 0.0123
pd_counterparty_default   approval_rate_gap 0.3193 0.3726         0.2128 0.0355
pd_counterparty_default         quality_gap 0.4591 0.5038         0.3697 0.0298
pd_counterparty_default concept_drift_score 0.0322 0.0622         0.0022 0.0042
pd_counterparty_default       data_drift_js 0.0530 0.0830         0.0230 0.0039
pd_counterparty_default model_quality_drift 0.0720 0.1075         0.0009 0.0237
  trade_volume_forecast         quality_gap 0.1052 0.1352         0.0752 0.0089
  trade_volume_forecast       data_drift_js 0.0300 0.0600         0.0000 0.0000
  trade_volume_forecast model_quality_drift 0.4324 0.6763        -0.0553 0.1626
  trade_volume_forecast concept_drift_score 0.2504 0.3265         0.0980 0.0508
✓ scored 4,048 metric-days · {'green': 3830, 'red': 113, 'amber': 105}
✓ 456 model-days · 78 alert events · latest day 2026-11-26:
           model_name status  health_score                                                       red_metrics amber_metrics
RM Client Brief Agent    red          57.1 Memory conflict rate, Memory-grounded answers, PII held in memory
```
```text
/home/spark-a1976972-4301-493f-9269-1a/.ipykernel/75/command-8099243853666333-3545793860:507: FutureWarning: DataFrameGroupBy.apply operated on the grouping columns. This behavior is deprecated, and in a future version of pandas the grouping columns will be excluded from the operation. Either pass `include_groups=False` to exclude the groupings or explicitly select the grouping columns after groupby to silence this warning.
  health = mh[mh["scope"] != "descoped"].groupby(["model_id", "metric_date"]).apply(model_day).reset_index()
```
```text
✓ demo_unified_model_monitoring.hub_gold.fact_metric_health_daily: 4,048 rows
✓ demo_unified_model_monitoring.hub_gold.fact_model_health_daily: 456 rows
✓ demo_unified_model_monitoring.hub_gold.fact_alerts: 78 rows
✓ demo_unified_model_monitoring.hub_gold.fact_metric_slice_daily: 3,942 rows
✓ demo_unified_model_monitoring.hub_gold.mv_model_health · metric view
✓ demo_unified_model_monitoring.hub_gold.mv_metric_health · metric view
Warehouse d1fcbbdbe403bdbc
✓ dashboard published: https://adb-7405615658009638.18.azuredatabricks.net/dashboardsv3/01f1e8bd396e10bc90a5bd4fc30bb37a/published
  definition also saved to /Shared/model_monitoring/UMM_dashboard_definition.lvdash.json
✓ alert: UMM · models in red today
✓ alert: UMM · new breaches today
✓ Genie space updated: https://adb-7405615658009638.18.azuredatabricks.net/genie/rooms/01f1e8bd3a2c16ceab9561ac5638e175

✓ hub job umm-demo-hub-refresh (156494604409159) · 3 tasks
```
.table-result-container { max-height: 300px; overflow: auto; } table, th, td { border: 1px solid black; border-collapse: collapse; } th, td { padding: 5px; } th { text-align: left; } check result S1 PD data drift (PSI) breaches PASS S1/S2 PD accuracy breaches PASS S1 PD positive rate breaches PASS S3 forecast MAE breaches PASS S4 RAG safety breaches PASS S5 RAG groundedness breaches PASS S7 agent recovery breaches PASS S8 agent permission compliance breaches PASS S9 agent approval adherence breaches PASS S6 agent token creep breaches PASS False-alarm rate in quiet days (7, 44) = 0.1% PASS Every in-scope metric scored (50) PASS Backfill matches monitor on overlap (99% within 2%) PASS Dashboard published PASS Genie space created PASS Alerts created PASS Hub job created PASS
```text
Phase 5 complete.
```
