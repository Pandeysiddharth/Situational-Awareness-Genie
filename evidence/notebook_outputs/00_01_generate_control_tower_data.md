# Captured output: `00_01_generate_control_tower_data.ipynb`

This is the plain-text output stored in the notebook the last time it ran interactively on Databricks. HTML tables are converted to markdown.


## Cell 2

```text
Target: cib_models.model_monitoring | window dates 2026-07-16 to 2026-10-13
```

## Cell 10

```text
unified_metrics: 14,126 rows | boundary_ledger: 357 | rejected_fields_log: 33
```

## Cell 12

```text
alerts: 14 | serving_cost_daily: 1,080
```

## Cell 14

```text
Boundary check passed: 0 raw rows, 0 log rows, no non-allowlisted metrics, no small slices.
Avg bytes per workspace-day: 9747
status
BREACH      242
OK        13764
WARN        120
```

## Cell 16

```text
wrote cib_models.model_monitoring.unified_metrics: 14,126 rows
wrote cib_models.model_monitoring.model_registry: 12 rows
wrote cib_models.model_monitoring.metric_allowlist: 12 rows
wrote cib_models.model_monitoring.alerts: 14 rows
wrote cib_models.model_monitoring.boundary_ledger: 357 rows
wrote cib_models.model_monitoring.rejected_fields_log: 33 rows
wrote cib_models.model_monitoring.serving_cost_daily: 1,080 rows
```

## Cell 18

```text
DataFrame[]
```

## Cell 19

.table-result-container { max-height: 300px; overflow: auto; } table, th, td { border: 1px solid black; border-collapse: collapse; } th, td { padding: 5px; } th { text-align: left; } current_status models OK 4 BREACH 4 STALE 3 WARN 1
.table-result-container { max-height: 300px; overflow: auto; } table, th, td { border: 1px solid black; border-collapse: collapse; } th, td { padding: 5px; } th { text-align: left; } model_name workspace region current_status breached_metrics warned_metrics aml_alert_scorer cib-fcc-prod-uks UK South BREACH feature_psi_max, positive_rate, prediction_drift_js corporate_pd cib-credit-prod-uks UK South BREACH auc, recall f1 sanctions_name_match cib-fcc-prod-uks UK South BREACH freshness_hours, row_count feature_psi_max txn_fraud_classifier cib-fcc-prod-uks UK South BREACH freshness_hours, row_count feature_psi_max early_warning_watchlist cib-credit-prod-uks UK South OK kyc_risk_rating cib-fcc-prod-uks UK South OK lgd_model cib-credit-prod-uks UK South OK trade_finance_risk cib-txnbank-prod-cin Central India OK client_propensity cib-markets-prod-sea Southeast Asia STALE fx_flow_forecast cib-markets-prod-sea Southeast Asia STALE trade_surveillance_anomaly cib-markets-prod-sea Southeast Asia STALE cashflow_forecast cib-txnbank-prod-cin Central India WARN freshness_hours, row_count
