# Captured output: `04a_UMM_Phase4_Monitors_And_Job.ipynb`

This is the plain-text output stored in the notebook the last time it ran interactively on Databricks. HTML tables are converted to markdown.


## Cell 1

```text
Notify: ['pandey.siddharth@databricks.com'] · PD threshold 0.225 · prices in/out 1.0/3.0 per 1M tokens
✓ residual on demo_unified_model_monitoring.markets_monitoring.trade_volume_scoring_log
✓ residual on demo_unified_model_monitoring.markets_ml.trade_volume_baseline
✓ label/prediction types match on demo_unified_model_monitoring.credit_monitoring.pd_inference_log (bigint)
✓ label/prediction types match on demo_unified_model_monitoring.credit_ml.pd_baseline (bigint)
• keeping existing monitor on demo_unified_model_monitoring.credit_monitoring.pd_inference_log
• keeping existing monitor on demo_unified_model_monitoring.markets_monitoring.trade_volume_scoring_log
• keeping existing monitor on demo_unified_model_monitoring.ai_factory_monitoring.rag_request_metrics
• keeping existing monitor on demo_unified_model_monitoring.ai_factory_monitoring.agent_request_metrics
• keeping existing monitor on demo_unified_model_monitoring.ai_factory_monitoring.llm_usage_log
✓ agent_request_metrics: SUCCESS 
✓ llm_usage_log: SUCCESS 
✓ rag_request_metrics: SUCCESS 
✓ pd_inference_log: SUCCESS 
✓ trade_volume_scoring_log: SUCCESS 
✓ demo_unified_model_monitoring.hub_ref.metric_source_map: 46 metrics, 46 with values
  pd_counterparty_default.label_drift_js: PSI on 'label', thresholds 0.10 / 0.25
  trade_volume_forecast.label_drift_js: PSI on 'label', thresholds 0.10 / 0.25
  trade_volume_forecast.concept_drift_score: PSI on 'residual', thresholds 0.10 / 0.25
```
.table-result-container { max-height: 300px; overflow: auto; } table, th, td { border: 1px solid black; border-collapse: collapse; } th, td { padding: 5px; } th { text-align: left; } model_id contract_name source_kind source_table source_column drift_type column_filter aggregation windows_with_values found scope credit_policy_rag avg_tokens_per_request profile demo_unified_model_monitoring.ai_factory_monitoring.rag_request_metrics_profile_metrics avg_tokens_per_request :table ratio_to_baseline 31 true in_scope credit_policy_rag context_relevance profile demo_unified_model_monitoring.ai_factory_monitoring.rag_request_metrics_profile_metrics context_relevance :table value 31 true in_scope credit_policy_rag cost_per_successful_query profile demo_unified_model_monitoring.ai_factory_monitoring.rag_request_metrics_profile_metrics cost_per_successful_query :table ratio_to_baseline 31 true in_scope credit_policy_rag groundedness profile demo_unified_model_monitoring.ai_factory_monitoring.rag_request_metrics_profile_metrics groundedness :table value 31 true in_scope credit_policy_rag precision_at_k profile demo_unified_model_monitoring.ai_factory_monitoring.rag_request_metrics_profile_metrics precision_at_k :table value 31 true in_scope credit_policy_rag query_intent_alignment profile demo_unified_model_monitoring.ai_factory_monitoring.rag_request_metrics_profile_metrics query_intent_alignment :table value 31 true in_scope credit_policy_rag recall_at_k profile demo_unified_model_monitoring.ai_factory_monitoring.rag_request_metrics_profile_metrics recall_at_k :table value 31 true in_scope credit_policy_rag response_relevance profile demo_unified_model_monitoring.ai_factory_monitoring.rag_request_metrics_profile_metrics response_relevance :table value 31 true in_scope credit_policy_rag safety_violation_rate profile demo_unified_model_monitoring.ai_factory_monitoring.rag_request_metrics_profile_metrics safety_violation_rate :table value 31 true in_scope credit_policy_rag user_satisfaction_score profile demo_unified_model_monitoring.ai_factory_monitoring.rag_request_metrics_profile_metrics user_satisfaction_score :table value 31 true in_scope llm_endpoint_shared error_rate profile demo_unified_model_monitoring.ai_factory_monitoring.llm_usage_log_profile_metrics error_rate :table value 31 true supporting llm_endpoint_shared p95_latency_ms profile demo_unified_model_monitoring.ai_factory_monitoring.llm_usage_log_profile_metrics p95_latency_ms :table ratio_to_baseline 31 true supporting llm_endpoint_shared tokens_per_request profile demo_unified_model_monitoring.ai_factory_monitoring.llm_usage_log_profile_metrics tokens_per_request :table ratio_to_baseline 31 true supporting pd_counterparty_default accuracy profile demo_unified_model_monitoring.credit_monitoring.pd_inference_log_profile_metrics accuracy_score :table value 23 true in_scope pd_counterparty_default approval_rate_gap profile demo_unified_model_monitoring.credit_monitoring.pd_inference_log_profile_metrics approval_rate :table max_minus_min_over_slices:country 30 true in_scope pd_counterparty_default concept_drift_score drift demo_unified_model_monitoring.credit_monitoring.pd_inference_log_drift_metrics concept_drift_score BASELINE :table value 23 true in_scope pd_counterparty_default data_drift_js drift demo_unified_model_monitoring.credit_monitoring.pd_inference_log_drift_metrics js_distance BASELINE features max_over_features 30 true in_scope pd_counterparty_default data_drift_psi drift demo_unified_model_monitoring.credit_monitoring.pd_inference_log_drift_metrics population_stability_index BASELINE features max_over_features 30 true in_scope pd_counterparty_default f1_macro profile demo_unified_model_monitoring.credit_monitoring.pd_inference_log_profile_metrics f1_macro_c :table value 23 true in_scope pd_counterparty_default label_drift_js drift demo_unified_model_monitoring.credit_monitoring.pd_inference_log_drift_metrics population_stability_index BASELINE label value 23 true
```text
found pd_live_processor: /Users/pandey.siddharth@databricks.com/Unified_Model_Monitoring/FEBar_Demo/UMM_Phase2_PD_Live_Processor
  found keepalive: /Users/pandey.siddharth@databricks.com/Unified_Model_Monitoring/FEBar_Demo/UMM_Phase4_KeepAlive
  found genai_traffic: /Users/pandey.siddharth@databricks.com/Unified_Model_Monitoring/FEBar_Demo/UMM_Phase3B_Send_Traffic
  found genai_process: /Users/pandey.siddharth@databricks.com/Unified_Model_Monitoring/FEBar_Demo/UMM_Phase3C_Process_And_Validate
✓ created job umm-demo-daily (646829627611828)
```
.table-result-container { max-height: 300px; overflow: auto; } table, th, td { border: 1px solid black; border-collapse: collapse; } th, td { padding: 5px; } th { text-align: left; } check result value pd_counterparty_default: monitor active PASS MONITOR_STATUS_ACTIVE pd_counterparty_default: first refresh PASS SUCCESS trade_volume_forecast: monitor active PASS MONITOR_STATUS_ACTIVE trade_volume_forecast: first refresh PASS SUCCESS credit_policy_rag: monitor active PASS MONITOR_STATUS_ACTIVE credit_policy_rag: first refresh PASS SUCCESS rm_client_brief_agent: monitor active PASS MONITOR_STATUS_ACTIVE rm_client_brief_agent: first refresh PASS SUCCESS llm_endpoint_shared: monitor active PASS MONITOR_STATUS_ACTIVE llm_endpoint_shared: first refresh PASS SUCCESS In-scope contract metrics with values PASS 43/43 Supporting Lane D metrics with values PASS 3/3 Median daily windows per metric (monitors compute the last 30 days) PASS 31 Daily job PASS 646829627611828
```text
Phase 4 complete. Monitor dashboards are in /Shared/model_monitoring
```
