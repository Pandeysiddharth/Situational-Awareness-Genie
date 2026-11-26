# Model portfolio and registered versions (hub reference)

Executed 2026-11-26 08:40 UTC on serverless compute against Unity Catalog.

```sql
select model_id, uc_model_name, model_version, alias, live_from, live_to, decision_threshold, holdout_auc, holdout_f1_macro, holdout_mae, holdout_rmse from demo_unified_model_monitoring.hub_gold.model_version_registry order by model_id, model_version
```

Result (5 rows):

| model_id | uc_model_name | model_version | alias | live_from | live_to | decision_threshold | holdout_auc | holdout_f1_macro | holdout_mae | holdout_rmse |
|---|---|---|---|---|---|---|---|---|---|---|
| credit_policy_rag | demo_unified_model_monitoring.ai_factory_ml.credit_policy_rag | 1 | champion | 2026-11-25 |  | nan | nan | nan | nan | nan |
| pd_counterparty_default | demo_unified_model_monitoring.credit_ml.pd_counterparty_default | 1 | previous | 2026-08-27 | 2026-10-10 | 0.225 | 0.8196 | 0.6874 | nan | nan |
| pd_counterparty_default | demo_unified_model_monitoring.credit_ml.pd_counterparty_default | 2 | champion | 2026-10-11 |  | 0.225 | 0.8272 | 0.6964 | nan | nan |
| rm_client_brief_agent | demo_unified_model_monitoring.ai_factory_ml.rm_client_brief_agent | 1 | champion | 2026-11-25 |  | nan | nan | nan | nan | nan |
| trade_volume_forecast | demo_unified_model_monitoring.markets_ml.trade_volume_forecast | 1 | champion | 2026-08-27 |  | nan | nan | nan | 17.165 | 30.648 |
