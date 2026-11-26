# Monitored models / agents in the hub

Executed 2026-11-26 08:40 UTC on serverless compute against Unity Catalog.

```sql
select model_id, model_name, asset_type, lane_name, problem_type, risk_tier, serving_mode, endpoint_name, monitored_table, owner_team, current_version, status from demo_unified_model_monitoring.hub_gold.dim_model order by model_id
```

Result (5 rows):

| model_id | model_name | asset_type | lane_name | problem_type | risk_tier | serving_mode | endpoint_name | monitored_table | owner_team | current_version | status |
|---|---|---|---|---|---|---|---|---|---|---|---|
| credit_policy_rag | Credit Policy Assistant (RAG) | model | RAG / agent traces | rag | Tier 2 | real_time | umm-demo-rag-endpoint | demo_unified_model_monitoring.ai_factory_monitoring.rag_request_metrics | AI Factory | 1.6.0 | active |
| llm_endpoint_shared | Shared LLM Endpoint | llm_endpoint | LLM service | llm_service | n/a | real_time | databricks-claude-sonnet-4-5 | demo_unified_model_monitoring.ai_factory_monitoring.llm_usage_log | AI Factory |  | active |
| pd_counterparty_default | Counterparty Default PD | model | Real-time classical | classification | Tier 1 | real_time | umm-demo-pd-endpoint | demo_unified_model_monitoring.credit_monitoring.pd_inference_log | CIB Credit Risk Analytics | 2 | active |
| rm_client_brief_agent | RM Client Brief Agent | model | RAG / agent traces | agent | Tier 1 | real_time | umm-demo-agent-endpoint | demo_unified_model_monitoring.ai_factory_monitoring.agent_request_metrics | AI Factory | 2.3.0 | active |
| trade_volume_forecast | Trade Volume Forecast | model | Batch classical | regression | Tier 2 | batch_nightly |  | demo_unified_model_monitoring.markets_monitoring.trade_volume_scoring_log | CIB Markets Analytics | 1 | active |
