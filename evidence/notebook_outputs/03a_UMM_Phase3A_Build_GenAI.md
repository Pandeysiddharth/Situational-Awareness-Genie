# Captured output: `03a_UMM_Phase3A_Build_GenAI.ipynb`

This is the plain-text output stored in the notebook the last time it ran interactively on Databricks. HTML tables are converted to markdown.


## Cell 1

```text
mlflow 3.18.0 · databricks-agents 1.11.0
```
```text
If you are using MLflow Tracing, you can migrate your traces to Unity Catalog for unlimited storage, fine-grained access controls, and queryability from notebooks, SQL, and dashboards. Learn more: https://docs.databricks.com/aws/en/mlflow3/genai/tracing/migrate-traces-to-uc
If you are using MLflow Tracing, you can migrate your traces to Unity Catalog for unlimited storage, fine-grained access controls, and queryability from notebooks, SQL, and dashboards. Learn more: https://docs.databricks.com/aws/en/mlflow3/genai/tracing/migrate-traces-to-uc
```
```text
mlflow 3.18.0
LLM: databricks-claude-sonnet-4-5 · embeddings: databricks-gte-large-en
✓ demo_unified_model_monitoring.ai_factory_raw.policy_doc_chunks: 148 rows
✓ demo_unified_model_monitoring.ai_factory_ml.policy_chunks_index_source: 144 rows
✓ demo_unified_model_monitoring.ai_factory_raw.rag_question_bank: 116 rows
✓ demo_unified_model_monitoring.ai_factory_raw.agent_clients: 50 rows
✓ demo_unified_model_monitoring.ai_factory_raw.agent_exposures: 150 rows
✓ demo_unified_model_monitoring.ai_factory_raw.agent_limits_restricted: 50 rows
✓ demo_unified_model_monitoring.ai_factory_raw.agent_news: 100 rows
Reusing Vector Search endpoint meridian-ai-search
Creating index demo_unified_model_monitoring.ai_factory_ml.policy_chunks_index
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_ENDPOINT · 0 rows
  index PROVISIONING_INDEX · 0 rows
  index PROVISIONING_PIPELINE_RESOURCES · 0 rows
✓ index ready · 144 rows
Smoke query: ['LIM-003@v2', 'LIM-003@v2', 'LIM-003@v2']
✓ AI Gateway logging on databricks-claude-sonnet-4-5 → demo_unified_model_monitoring.ai_factory_monitoring.fm_shared_payload (+ system.serving.endpoint_usage)
```
```text
If you are using MLflow Tracing, you can migrate your traces to Unity Catalog for unlimited storage, fine-grained access controls, and queryability from notebooks, SQL, and dashboards. Learn more: https://docs.databricks.com/aws/en/mlflow3/genai/tracing/migrate-traces-to-uc
2026/11/25 09:06:47 WARNING mlflow.models.model: `artifact_path` is deprecated. Please use `name` instead.
🔗 View Logged Model at: https://adb-7405615658009638.18.azuredatabricks.net/ml/experiments/1123681276224058/models/m-129a21851d6b463bb5d21a3fb833b7db?o=7405615658009638
2026/11/25 09:06:48 WARNING mlflow.pyfunc: You have manually traced predict with @mlflow.trace, but this is unnecessary with ResponsesAgent subclasses. You can remove the @mlflow.trace decorator and it will be automatically traced.
2026/11/25 09:06:48 INFO mlflow.pyfunc: Predicting on input example to validate output
2026/11/25 09:06:55 WARNING mlflow.pyfunc: You have manually traced predict with @mlflow.trace, but this is unnecessary with ResponsesAgent subclasses. You can remove the @mlflow.trace decorator and it will be automatically traced.
Successfully registered model 'demo_unified_model_monitoring.ai_factory_ml.credit_policy_rag'.
```
```text
Uploading artifacts:   0%|          | 0/12 [00:00<?, ?it/s]
```
```text
🔗 Created version '1' of model 'demo_unified_model_monitoring.ai_factory_ml.credit_policy_rag': https://adb-7405615658009638.18.azuredatabricks.net/explore/data/models/demo_unified_model_monitoring/ai_factory_ml/credit_policy_rag/version/1?o=7405615658009638
```
```text
✓ registered demo_unified_model_monitoring.ai_factory_ml.credit_policy_rag v1
```
```text
Downloading artifacts:   0%|          | 0/1 [00:00<?, ?it/s]
```
```text
/databricks/python/lib/python3.12/site-packages/databricks/agents/deployments.py:641: UserWarning: This endpoint is being deployed without a feedback model, which has been deprecated.
For more information, see: https://docs.databricks.com/aws/en/generative-ai/agent-framework/feedback-model
  warnings.warn(
```
```text
Deployment of demo_unified_model_monitoring.ai_factory_ml.credit_policy_rag version 1 initiated.  This can take up to 15 minutes and the Review App & Query Endpoint will not work until this deployment finishes.

    View status: https://adb-7405615658009638.18.azuredatabricks.net/ml/endpoints/umm-demo-rag-endpoint/?o=7405615658009638
    Review App: https://adb-7405615658009638.18.azuredatabricks.net/ml/review-v2/5caedf535abd4f1e8be89c180f64535a/chat?o=7405615658009638

You can refer back to the links above from the endpoint detail page at https://adb-7405615658009638.18.azuredatabricks.net/ml/endpoints/umm-demo-rag-endpoint/?o=7405615658009638.

To set up monitoring for your deployed agent, see:
https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/production-monitoring
Deploying demo_unified_model_monitoring.ai_factory_ml.credit_policy_rag v1 → umm-demo-rag-endpoint
```
```text
If you are using MLflow Tracing, you can migrate your traces to Unity Catalog for unlimited storage, fine-grained access controls, and queryability from notebooks, SQL, and dashboards. Learn more: https://docs.databricks.com/aws/en/mlflow3/genai/tracing/migrate-traces-to-uc
2026/11/25 09:07:09 WARNING mlflow.models.model: `artifact_path` is deprecated. Please use `name` instead.
🔗 View Logged Model at: https://adb-7405615658009638.18.azuredatabricks.net/ml/experiments/1123681276224059/models/m-bba4fc1fc07145bd9b44313aaf1fd5fc?o=7405615658009638
2026/11/25 09:07:10 WARNING mlflow.pyfunc: You have manually traced predict with @mlflow.trace, but this is unnecessary with ResponsesAgent subclasses. You can remove the @mlflow.trace decorator and it will be automatically traced.
2026/11/25 09:07:10 INFO mlflow.pyfunc: Predicting on input example to validate output
2026/11/25 09:07:16 WARNING mlflow.pyfunc: You have manually traced predict with @mlflow.trace, but this is unnecessary with ResponsesAgent subclasses. You can remove the @mlflow.trace decorator and it will be automatically traced.
Successfully registered model 'demo_unified_model_monitoring.ai_factory_ml.rm_client_brief_agent'.
```
```text
Uploading artifacts:   0%|          | 0/12 [00:00<?, ?it/s]
```
```text
🔗 Created version '1' of model 'demo_unified_model_monitoring.ai_factory_ml.rm_client_brief_agent': https://adb-7405615658009638.18.azuredatabricks.net/explore/data/models/demo_unified_model_monitoring/ai_factory_ml/rm_client_brief_agent/version/1?o=7405615658009638
```
```text
✓ registered demo_unified_model_monitoring.ai_factory_ml.rm_client_brief_agent v1
```
```text
Downloading artifacts:   0%|          | 0/1 [00:00<?, ?it/s]
```
```text
/databricks/python/lib/python3.12/site-packages/databricks/agents/deployments.py:641: UserWarning: This endpoint is being deployed without a feedback model, which has been deprecated.
For more information, see: https://docs.databricks.com/aws/en/generative-ai/agent-framework/feedback-model
  warnings.warn(
```
```text
Deployment of demo_unified_model_monitoring.ai_factory_ml.rm_client_brief_agent version 1 initiated.  This can take up to 15 minutes and the Review App & Query Endpoint will not work until this deployment finishes.

    View status: https://adb-7405615658009638.18.azuredatabricks.net/ml/endpoints/umm-demo-agent-endpoint/?o=7405615658009638
    Review App: https://adb-7405615658009638.18.azuredatabricks.net/ml/review-v2/7e18c4e19a2d47f0bbb784b9a4966cfa/chat?o=7405615658009638

You can refer back to the links above from the endpoint detail page at https://adb-7405615658009638.18.azuredatabricks.net/ml/endpoints/umm-demo-agent-endpoint/?o=7405615658009638.

To set up monitoring for your deployed agent, see:
https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/production-monitoring
Deploying demo_unified_model_monitoring.ai_factory_ml.rm_client_brief_agent v1 → umm-demo-agent-endpoint
  09:07:32 umm-demo-rag-endpoint: {'ready': 'NOT_READY', 'config_update': 'IN_PROGRESS', 'suspend': 'NOT_SUSPENDED', 'system_update_failure': False}
  09:08:32 umm-demo-rag-endpoint: {'ready': 'NOT_READY', 'config_update': 'IN_PROGRESS', 'suspend': 'NOT_SUSPENDED', 'system_update_failure': False}
  09:09:32 umm-demo-rag-endpoint: {'ready': 'NOT_READY', 'config_update': 'IN_PROGRESS', 'suspend': 'NOT_SUSPENDED', 'system_update_failure': False}
  09:10:32 umm-demo-rag-endpoint: {'ready': 'NOT_READY', 'config_update': 'IN_PROGRESS', 'suspend': 'NOT_SUSPENDED', 'system_update_failure': False}
✓ umm-demo-rag-endpoint READY
✓ umm-demo-agent-endpoint READY
RAG: Under the Counterparty Limit Setting and Utilisation Standard, the single counterparty limit for unrated corporates is **USD 15 million**. Any proposal above this must carry a documented rationale and be recorded in the credit system before approval. [LIM-003@v2] 
   docs: ['LIM-003@v2', 'LIM-001@v1']
Agent: ## Exposure Summary: Orion Semiconductors Pte Ltd

**Client ID:** CL0001  
**Sector:** Technology | **Country:** Singapore  
**Internal Rating:** 5  
**Relationship Manager:** Omar Haddad

### Current Exposures by Product:

| Product | Exposure (USD M) | Limit (USD M) | Utilization |
|---------|---- 
   tools: ['get_client_profile', 'get_exposure_summary']
✓ demo_unified_model_monitoring.hub_ref.genai_runtime_config: 1 rows
```
```text
If you are using MLflow Tracing, you can migrate your traces to Unity Catalog for unlimited storage, fine-grained access controls, and queryability from notebooks, SQL, and dashboards. Learn more: https://docs.databricks.com/aws/en/mlflow3/genai/tracing/migrate-traces-to-uc
```
```text
✓ umm_demo_rag: safety scorer running (50% sample)
✓ umm_demo_rag: groundedness scorer running (50% sample)
✓ umm_demo_rag: relevance_to_query scorer running (50% sample)
```
```text
If you are using MLflow Tracing, you can migrate your traces to Unity Catalog for unlimited storage, fine-grained access controls, and queryability from notebooks, SQL, and dashboards. Learn more: https://docs.databricks.com/aws/en/mlflow3/genai/tracing/migrate-traces-to-uc
```
```text
✓ umm_demo_agent: safety scorer running (50% sample)
```
.table-result-container { max-height: 300px; overflow: auto; } table, th, td { border: 1px solid black; border-collapse: collapse; } th, td { padding: 5px; } th { text-align: left; } check result value LLM endpoint picked PASS databricks-claude-sonnet-4-5 Index rows = published chunks PASS 144 Question bank size PASS 116 RAG endpoint READY PASS umm-demo-rag-endpoint Agent endpoint READY PASS umm-demo-agent-endpoint RAG smoke: retrieved LIM-003@v2 PASS ['LIM-003@v2', 'LIM-001@v1'] Agent smoke: profile tool called PASS ['get_client_profile', 'get_exposure_summary'] Lane D AI Gateway table configured PASS demo_unified_model_monitoring.ai_factory_monitoring.fm_shared_payload
```text
Phase 3A complete. Run UMM_Phase3B_Send_Traffic next.
```
