# Platform state captured 2026-11-26 08:42 UTC

Sources: Serving Endpoints API, Unity Catalog Models API, Lakehouse Monitoring API.

## Model Serving endpoints

| endpoint | ready | config_update | served | task |
|---|---|---|---|---|
| umm-demo-pd-endpoint | READY | NOT_UPDATING | demo_unified_model_monitoring.credit_ml.pd_counterparty_default v2 |  |
| umm-demo-rag-endpoint | READY | NOT_UPDATING | demo_unified_model_monitoring.ai_factory_ml.credit_policy_rag v1, demo_unified_model_monitoring.ai_factory_ml.credit_policy_rag v2 | agent/v1/responses |
| umm-demo-agent-endpoint | READY | NOT_UPDATING | demo_unified_model_monitoring.ai_factory_ml.rm_client_brief_agent v1, demo_unified_model_monitoring.ai_factory_ml.rm_client_brief_agent v2 | agent/v1/responses |
| umm-demo-llm | NOT FOUND | Endpoint with name 'umm-demo-llm' does not exist. |  |  |
| cib-credit-pd | READY | NOT_UPDATING | demo_unified_model_monitoring.spoke_corp_banking.credit_pd_model v1 |  |
| cib-fx-anomaly | READY | NOT_UPDATING | demo_unified_model_monitoring.spoke_markets.fx_anomaly_model v1 |  |
| cib-payments-forecast | READY | NOT_UPDATING | demo_unified_model_monitoring.spoke_txn_banking.payments_volume_forecast_model v1 |  |

## Unity Catalog registered models

| model | version | status | created | run_id | aliases |
|---|---|---|---|---|---|
| demo_unified_model_monitoring.credit_ml.pd_counterparty_default | 2 | READY | 2026-11-25 08:17:58 UTC | 8127a54e783647b5b058bdcc2d7a6c68 | champion->v2, previous->v1 |
| demo_unified_model_monitoring.credit_ml.pd_counterparty_default | 1 | READY | 2026-11-25 08:17:53 UTC | cc65f15754d54a5aa3ab31e08ebe91d5 | champion->v2, previous->v1 |
| demo_unified_model_monitoring.markets_ml.trade_volume_forecast | 1 | READY | 2026-11-25 08:18:06 UTC | 563541e45fb3438396f6c9f83079fe39 | champion->v1 |
| demo_unified_model_monitoring.ai_factory_ml.credit_policy_rag | 2 | READY | 2026-11-26 04:33:21 UTC | ae67c72dbb8e4150974314e948973cee | champion->v2 |
| demo_unified_model_monitoring.ai_factory_ml.credit_policy_rag | 1 | READY | 2026-11-25 09:07:01 UTC | fc79441076be432cbf2cb00d480efc0b | champion->v2 |
| demo_unified_model_monitoring.ai_factory_ml.rm_client_brief_agent | 2 | READY | 2026-11-26 04:33:37 UTC | e04e855e85b448f487a6d916d344224e | champion->v2 |
| demo_unified_model_monitoring.ai_factory_ml.rm_client_brief_agent | 1 | READY | 2026-11-25 09:07:24 UTC | bdffb253ef5a427087b2f9ce632f7867 | champion->v2 |
| demo_unified_model_monitoring.spoke_corp_banking.credit_pd_model | 1 | READY | 2026-11-25 07:21:24 UTC | 7f9d42eaaae34bff9937f5e60d3bb3e4 | champion->v1 |
| demo_unified_model_monitoring.spoke_markets.fx_anomaly_model | 1 | READY | 2026-11-25 07:21:37 UTC | 9b967c1d1f1b4ac9983a86aab22a3772 | champion->v1 |
| demo_unified_model_monitoring.spoke_txn_banking.payments_volume_forecast_model | 1 | READY | 2026-11-25 07:21:47 UTC | ed2270d6abf34ff38eeca57a8614ec93 | champion->v1 |

## Lakehouse Monitoring (data profiling) monitors

| table | status | profile | custom_metrics | refreshes | last_refresh | last_refresh_end |
|---|---|---|---|---|---|---|
| demo_unified_model_monitoring.credit_monitoring.pd_inference_log | MONITOR_STATUS_ACTIVE | inference_log | 10 | 2 | SUCCESS | 2026-11-26 02:17:23 UTC |
| demo_unified_model_monitoring.markets_monitoring.trade_volume_scoring_log | MONITOR_STATUS_ACTIVE | inference_log | 4 | 2 | SUCCESS | 2026-11-26 02:19:38 UTC |
| demo_unified_model_monitoring.ai_factory_monitoring.rag_request_metrics | MONITOR_STATUS_ACTIVE | time_series | 11 | 2 | SUCCESS | 2026-11-26 02:11:04 UTC |
| demo_unified_model_monitoring.ai_factory_monitoring.agent_request_metrics | MONITOR_STATUS_ACTIVE | time_series | 13 | 2 | SUCCESS | 2026-11-26 02:21:17 UTC |
| demo_unified_model_monitoring.ai_factory_monitoring.llm_usage_log | MONITOR_STATUS_ACTIVE | time_series | 4 | 2 | SUCCESS | 2026-11-26 02:10:47 UTC |
