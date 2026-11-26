# Captured output: `01_umm_all_in_one.ipynb`

This is the plain-text output stored in the notebook the last time it ran interactively on Databricks. HTML tables are converted to markdown.


## Cell 1

```text
History window: 2026-08-27 → 2026-11-24 (90 days)
Catalogs: {'credit': 'demo_unified_model_monitoring', 'markets': 'demo_unified_model_monitoring', 'ai_factory': 'demo_unified_model_monitoring', 'hub': 'demo_unified_model_monitoring'}
Single-catalog mode: using demo_unified_model_monitoring
✓ schema demo_unified_model_monitoring.credit_raw
✓ schema demo_unified_model_monitoring.credit_ml
✓ schema demo_unified_model_monitoring.credit_monitoring
✓ schema demo_unified_model_monitoring.markets_raw
✓ schema demo_unified_model_monitoring.markets_ml
✓ schema demo_unified_model_monitoring.markets_monitoring
✓ schema demo_unified_model_monitoring.ai_factory_raw
✓ schema demo_unified_model_monitoring.ai_factory_ml
✓ schema demo_unified_model_monitoring.ai_factory_monitoring
✓ schema demo_unified_model_monitoring.hub_ref
✓ schema demo_unified_model_monitoring.hub_gold
✓ /Shared/model_monitoring
✓ demo_unified_model_monitoring.hub_ref.dim_model: 5 rows
✓ demo_unified_model_monitoring.hub_ref.dim_metric: 43 rows
✓ demo_unified_model_monitoring.hub_ref.map_model_metric: 49 rows
model_id                 scope     
credit_policy_rag        descoped       3
                         in_scope      10
llm_endpoint_shared      supporting     3
pd_counterparty_default  in_scope      12
rm_client_brief_agent    in_scope      13
trade_volume_forecast    in_scope       8
dtype: int64
✓ demo_unified_model_monitoring.hub_ref.scenario_calendar: 10 rows
✓ demo_unified_model_monitoring.hub_ref.ref_llm_pricing: 1 rows
Training default rate: 0.115
Scoring requests: 29,261 · outcomes available: 27,004
✓ demo_unified_model_monitoring.credit_raw.pd_training: 24,000 rows
✓ demo_unified_model_monitoring.credit_raw.pd_scoring_requests: 29,261 rows
✓ demo_unified_model_monitoring.credit_raw.pd_outcomes: 27,004 rows
dataset_split
scoring_history     2700
train              21900
dtype: int64
✓ demo_unified_model_monitoring.markets_raw.trade_volume_daily: 24,600 rows
RAG requests: 18,331
Agent requests: 16,337
LLM calls: 92,174
✓ demo_unified_model_monitoring.ai_factory_raw.policy_doc_registry: 37 rows
✓ demo_unified_model_monitoring.ai_factory_monitoring.rag_request_metrics: 18,331 rows
✓ demo_unified_model_monitoring.ai_factory_monitoring.agent_request_metrics: 16,337 rows
✓ demo_unified_model_monitoring.ai_factory_monitoring.llm_usage_log: 92,174 rows
```
.table-result-container { max-height: 300px; overflow: auto; } table, th, td { border: 1px solid black; border-collapse: collapse; } th, td { padding: 5px; } th { text-align: left; } check result value expected pd_training: has rows PASS 24000 > 0 pd_req: has rows PASS 29261 > 0 pd_out: has rows PASS 27004 > 0 tv: has rows PASS 24600 > 0 docs: has rows PASS 37 > 0 rag: has rows PASS 18331 > 0 agent: has rows PASS 16337 > 0 llm: has rows PASS 92174 > 0 dim_model: has rows PASS 5 > 0 dim_metric: has rows PASS 43 > 0 map: has rows PASS 49 > 0 cal: has rows PASS 10 > 0 price: has rows PASS 1 > 0 pd_req: covers 90 days PASS 90 = 90 rag: covers 90 days PASS 90 = 90 agent: covers 90 days PASS 90 = 90 llm: covers 90 days PASS 90 = 90 trade volume: scoring days PASS 90 = 90 trade volume: 30 series PASS 30 = 30 dim_model: 5 assets PASS 5 = 5 map: in-scope contract metrics PASS 43 = 43 map: descoped metrics PASS 3 = 3 map: supporting Lane D metrics PASS 3 = 3 scenario calendar: 10 scenarios PASS 10 = 10 PD: training default rate 8-16% PASS 0.1154 0.08–0.16 S1: debt_to_ebitda late / early PASS 1.3758 > 1.25 S2: IN default rate after / before PASS 2.6269 > 1.6 PD: no outcomes for last label-delay window PASS 0 = 0 S3: FX volume during shock / pre-shock PASS 1.5542 > 1.4 RAG: baseline groundedness PASS 0.9238 > 0.88 S5: groundedness during stale index PASS 0.8156 S5: precision@5 during stale index PASS 0.2337 RAG: baseline safety violation rate PASS 0.0009 S4: safety violation rate during probe PASS 0.0513 > 0.03 (red) S7: recovery success during timeouts PASS 0.5439 Agent: baseline recovery success PASS 0.9574 > 0.80 S8: permission compliance during bad release PASS 0.9576 Agent: baseline permission compliance PASS 0.9995 > 0.99 S9: approval adherence during bypass PASS 0.7619 S6: tokens per request, last 7 days / baseline PASS 1.2368 > 1.2 (amber) Lane D: both apps tagged PASS 2 = 2 Lane D: tokens reconcile with request tables PASS 0 = 0
```text
All 42 checks passed. Phase 1 data is ready.
```
