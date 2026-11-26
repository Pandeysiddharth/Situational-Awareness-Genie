# Most recent 40 status-transition alerts

Executed 2026-11-26 08:40 UTC on serverless compute against Unity Catalog.

```sql
select alert_id, model_name, display_name, category, metric_date, alert_type, from_status, to_status, round(eval_value,4) eval_value, threshold, message from demo_unified_model_monitoring.hub_gold.fact_alerts order by metric_date desc, alert_id limit 40
```

Result (40 rows):

| alert_id | model_name | display_name | category | metric_date | alert_type | from_status | to_status | eval_value | threshold | message |
|---|---|---|---|---|---|---|---|---|---|---|
| rm_client_brief_agent\|memory_conflict_rate\|2026-11-26 | RM Client Brief Agent | Memory conflict rate | Risk and Safety | 2026-11-26 | breach | green | red | 0.5 | 0.15 | RM Client Brief Agent: Memory conflict rate went red (0.5 vs 0.15) |
| rm_client_brief_agent\|memory_grounded_rate\|2026-11-26 | RM Client Brief Agent | Memory-grounded answers | Performance | 2026-11-26 | breach | green | red | 0.375 | 0.75 | RM Client Brief Agent: Memory-grounded answers went red (0.375 vs 0.75) |
| rm_client_brief_agent\|pii_in_memory_rate\|2026-11-26 | RM Client Brief Agent | PII held in memory | Risk and Safety | 2026-11-26 | breach | green | red | 0.1111 | 0.02 | RM Client Brief Agent: PII held in memory went red (0.1111 vs 0.02) |
| credit_policy_rag\|precision_at_k\|2026-11-25 | Credit Policy Assistant (RAG) | Precision@K | Performance | 2026-11-25 | breach | green | amber | 0.2684 | 0.28 | Credit Policy Assistant (RAG): Precision@K went amber (0.2684 vs 0.28) |
| llm_endpoint_shared\|p95_latency_ms\|2026-11-25 | Shared LLM Endpoint | p95 latency | Operations | 2026-11-25 | breach | green | amber | 1.3823 | 1.3 | Shared LLM Endpoint: p95 latency went amber (1.382 vs 1.3) |
| rm_client_brief_agent\|avg_tokens_per_request\|2026-11-25 | RM Client Brief Agent | Avg tokens per request | Optimisation | 2026-11-25 | resolved | amber | green | 1.1545 | 1.2 | RM Client Brief Agent: Avg tokens per request recovered to green (1.154 vs 1.2) |
| rm_client_brief_agent\|cost_per_request\|2026-11-25 | RM Client Brief Agent | Cost | Optimisation | 2026-11-25 | resolved | amber | green | 1.1285 | 1.2 | RM Client Brief Agent: Cost recovered to green (1.129 vs 1.2) |
| rm_client_brief_agent\|decision_path_validity\|2026-11-25 | RM Client Brief Agent | Decision-path validity | Performance | 2026-11-25 | breach | green | red | 0.7411 | 0.75 | RM Client Brief Agent: Decision-path validity went red (0.7411 vs 0.75) |
| rm_client_brief_agent\|plan_quality\|2026-11-25 | RM Client Brief Agent | Plan quality | Performance | 2026-11-25 | breach | green | amber | 0.8046 | 0.85 | RM Client Brief Agent: Plan quality went amber (0.8046 vs 0.85) |
| rm_client_brief_agent\|trace_completeness\|2026-11-25 | RM Client Brief Agent | Trace completeness | Risk and Safety | 2026-11-25 | breach | green | amber | 0.9436 | 0.95 | RM Client Brief Agent: Trace completeness went amber (0.9436 vs 0.95) |
| rm_client_brief_agent\|approval_adherence_rate\|2026-11-24 | RM Client Brief Agent | Approval adherence | Risk and Safety | 2026-11-24 | resolved | red | green | 1.0 | 0.98 | RM Client Brief Agent: Approval adherence recovered to green (1 vs 0.98) |
| trade_volume_forecast\|model_quality_drift\|2026-11-24 | Trade Volume Forecast | Model drift | Performance | 2026-11-24 | resolved | red | green | 0.2439 | 0.4324 | Trade Volume Forecast: Model drift recovered to green (0.2439 vs 0.4324) |
| trade_volume_forecast\|quality_gap\|2026-11-22 | Trade Volume Forecast | Fairness: quality gap | Risk and Safety | 2026-11-22 | resolved | amber | green | 0.101 | 0.1052 | Trade Volume Forecast: Fairness: quality gap recovered to green (0.101 vs 0.1052) |
| trade_volume_forecast\|mae\|2026-11-20 | Trade Volume Forecast | Mean Absolute Error | Performance | 2026-11-20 | resolved | red | green | 1.0347 | 1.25 | Trade Volume Forecast: Mean Absolute Error recovered to green (1.035 vs 1.25) |
| trade_volume_forecast\|rmse\|2026-11-20 | Trade Volume Forecast | Root Mean Squared Error | Performance | 2026-11-20 | resolved | red | green | 1.0759 | 1.25 | Trade Volume Forecast: Root Mean Squared Error recovered to green (1.076 vs 1.25) |
| rm_client_brief_agent\|approval_adherence_rate\|2026-11-19 | RM Client Brief Agent | Approval adherence | Risk and Safety | 2026-11-19 | breach | green | red | 0.9444 | 0.95 | RM Client Brief Agent: Approval adherence went red (0.9444 vs 0.95) |
| rm_client_brief_agent\|cost_per_request\|2026-11-19 | RM Client Brief Agent | Cost | Optimisation | 2026-11-19 | breach | green | amber | 1.215 | 1.2 | RM Client Brief Agent: Cost went amber (1.215 vs 1.2) |
| credit_policy_rag\|precision_at_k\|2026-11-18 | Credit Policy Assistant (RAG) | Precision@K | Performance | 2026-11-18 | resolved | amber | green | 0.3031 | 0.28 | Credit Policy Assistant (RAG): Precision@K recovered to green (0.3031 vs 0.28) |
| rm_client_brief_agent\|cost_per_request\|2026-11-18 | RM Client Brief Agent | Cost | Optimisation | 2026-11-18 | resolved | amber | green | 1.1998 | 1.2 | RM Client Brief Agent: Cost recovered to green (1.2 vs 1.2) |
| trade_volume_forecast\|concept_drift_score\|2026-11-18 | Trade Volume Forecast | Concept drift | Performance | 2026-11-18 | resolved | amber | green | 0.2107 | 0.2504 | Trade Volume Forecast: Concept drift recovered to green (0.2107 vs 0.2504) |
| trade_volume_forecast\|data_drift_psi\|2026-11-17 | Trade Volume Forecast | Data drift (PSI) | Performance | 2026-11-17 | breach | green | amber | 0.1078 | 0.1 | Trade Volume Forecast: Data drift (PSI) went amber (0.1078 vs 0.1) |
| credit_policy_rag\|groundedness\|2026-11-16 | Credit Policy Assistant (RAG) | Groundedness | Performance | 2026-11-16 | resolved | amber | green | 0.8675 | 0.85 | Credit Policy Assistant (RAG): Groundedness recovered to green (0.8675 vs 0.85) |
| credit_policy_rag\|recall_at_k\|2026-11-16 | Credit Policy Assistant (RAG) | Recall@K | Performance | 2026-11-16 | resolved | amber | green | 0.7555 | 0.75 | Credit Policy Assistant (RAG): Recall@K recovered to green (0.7555 vs 0.75) |
| pd_counterparty_default\|concept_drift_score\|2026-11-16 | Counterparty Default PD | Concept drift | Performance | 2026-11-16 | resolved | amber | green | 0.0295 | 0.0322 | Counterparty Default PD: Concept drift recovered to green (0.02948 vs 0.0322) |
| credit_policy_rag\|context_relevance\|2026-11-15 | Credit Policy Assistant (RAG) | Context relevance | Performance | 2026-11-15 | resolved | amber | green | 0.8011 | 0.8 | Credit Policy Assistant (RAG): Context relevance recovered to green (0.8011 vs 0.8) |
| pd_counterparty_default\|positive_rate_at_threshold\|2026-11-15 | Counterparty Default PD | Threshold calibration | Risk and Safety | 2026-11-15 | escalation | amber | red | 0.6854 | 0.6 | Counterparty Default PD: Threshold calibration went red (0.6854 vs 0.6) |
| rm_client_brief_agent\|avg_tokens_per_request\|2026-11-15 | RM Client Brief Agent | Avg tokens per request | Optimisation | 2026-11-15 | breach | green | amber | 1.2254 | 1.2 | RM Client Brief Agent: Avg tokens per request went amber (1.225 vs 1.2) |
| rm_client_brief_agent\|cost_per_request\|2026-11-15 | RM Client Brief Agent | Cost | Optimisation | 2026-11-15 | breach | green | amber | 1.2263 | 1.2 | RM Client Brief Agent: Cost went amber (1.226 vs 1.2) |
| pd_counterparty_default\|concept_drift_score\|2026-11-14 | Counterparty Default PD | Concept drift | Performance | 2026-11-14 | breach | green | amber | 0.0328 | 0.0322 | Counterparty Default PD: Concept drift went amber (0.03281 vs 0.0322) |
| trade_volume_forecast\|concept_drift_score\|2026-11-13 | Trade Volume Forecast | Concept drift | Performance | 2026-11-13 | breach | green | amber | 0.2813 | 0.2504 | Trade Volume Forecast: Concept drift went amber (0.2813 vs 0.2504) |
| pd_counterparty_default\|accuracy\|2026-11-12 | Counterparty Default PD | Accuracy | Performance | 2026-11-12 | escalation | amber | red | 0.7981 | 0.8007 | Counterparty Default PD: Accuracy went red (0.7981 vs 0.8007) |
| credit_policy_rag\|context_relevance\|2026-11-11 | Credit Policy Assistant (RAG) | Context relevance | Performance | 2026-11-11 | breach | green | amber | 0.7895 | 0.8 | Credit Policy Assistant (RAG): Context relevance went amber (0.7895 vs 0.8) |
| credit_policy_rag\|groundedness\|2026-11-11 | Credit Policy Assistant (RAG) | Groundedness | Performance | 2026-11-11 | breach | green | amber | 0.8269 | 0.85 | Credit Policy Assistant (RAG): Groundedness went amber (0.8269 vs 0.85) |
| credit_policy_rag\|precision_at_k\|2026-11-11 | Credit Policy Assistant (RAG) | Precision@K | Performance | 2026-11-11 | breach | green | amber | 0.249 | 0.28 | Credit Policy Assistant (RAG): Precision@K went amber (0.249 vs 0.28) |
| credit_policy_rag\|recall_at_k\|2026-11-11 | Credit Policy Assistant (RAG) | Recall@K | Performance | 2026-11-11 | breach | green | amber | 0.6988 | 0.75 | Credit Policy Assistant (RAG): Recall@K went amber (0.6988 vs 0.75) |
| pd_counterparty_default\|positive_rate_at_threshold\|2026-11-11 | Counterparty Default PD | Threshold calibration | Risk and Safety | 2026-11-11 | escalation | amber | red | 0.605 | 0.6 | Counterparty Default PD: Threshold calibration went red (0.605 vs 0.6) |
| trade_volume_forecast\|quality_gap\|2026-11-11 | Trade Volume Forecast | Fairness: quality gap | Risk and Safety | 2026-11-11 | escalation | amber | red | 0.1566 | 0.1352 | Trade Volume Forecast: Fairness: quality gap went red (0.1566 vs 0.1352) |
| rm_client_brief_agent\|escalation_appropriateness\|2026-11-10 | RM Client Brief Agent | Escalation appropriateness | Risk and Safety | 2026-11-10 | resolved | amber | green | 0.9594 | 0.85 | RM Client Brief Agent: Escalation appropriateness recovered to green (0.9594 vs 0.85) |
| rm_client_brief_agent\|permission_compliance_rate\|2026-11-10 | RM Client Brief Agent | Permission compliance | Risk and Safety | 2026-11-10 | resolved | amber | green | 1.0 | 0.99 | RM Client Brief Agent: Permission compliance recovered to green (1 vs 0.99) |
| trade_volume_forecast\|model_quality_drift\|2026-11-10 | Trade Volume Forecast | Model drift | Performance | 2026-11-10 | escalation | amber | red | 1.2584 | 0.6763 | Trade Volume Forecast: Model drift went red (1.258 vs 0.6763) |
