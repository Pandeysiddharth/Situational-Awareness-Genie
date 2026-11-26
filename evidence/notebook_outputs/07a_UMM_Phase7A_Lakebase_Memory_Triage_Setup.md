# Captured output: `07a_UMM_Phase7A_Lakebase_Memory_Triage_Setup.ipynb`

This is the plain-text output stored in the notebook the last time it ran interactively on Databricks. HTML tables are converted to markdown.


## Cell 1

```text
mlflow 3.18.0 · databricks-agents 1.13.0
✓ Agent memory API available
• session store umm-rm-agent-sessions-asia exists
• session store umm-rm-agent-sessions-west exists
• session store umm-rag-sessions exists
• memory store umm-rm-agent-memory-asia exists
• memory store umm-rm-agent-memory-west exists
• memory pipeline for asia exists (205a115e-edb2-44e7-bb3b-dd402335b0b7)
• memory pipeline for west exists (235e2f9a-6945-40e6-9575-0fafd17730b3)
✓ seeded 27 memory entries for 8 RMs · roster in demo_unified_model_monitoring.ai_factory_raw.rm_roster
• Lakebase project umm-ops exists
✓ Lakebase endpoint projects/umm-ops/branches/production/endpoints/primary · host ep-delicate-frost-e8rbamtn.database.centralus.azuredatabricks.net
✓ umm.alert_inbox and umm.alert_triage_events ready
✓ triage inbox seeded: {'resolved': 32, 'investigating': 6, 'open': 5, 'false_positive': 3, 'acknowledged': 2} · 84 events
```
```text
If you are using MLflow Tracing, you can migrate your traces to Unity Catalog for unlimited storage, fine-grained access controls, and queryability from notebooks, SQL, and dashboards. Learn more: https://docs.databricks.com/aws/en/mlflow3/genai/tracing/migrate-traces-to-uc
2026/11/26 04:33:08 WARNING mlflow.models.model: `artifact_path` is deprecated. Please use `name` instead.
🔗 View Logged Model at: https://adb-7405615658009638.18.azuredatabricks.net/ml/experiments/1123681276224058/models/m-e284b9b65dd242edaf888575afb350c9?o=7405615658009638
2026/11/26 04:33:08 WARNING mlflow.pyfunc: You have manually traced predict with @mlflow.trace, but this is unnecessary with ResponsesAgent subclasses. You can remove the @mlflow.trace decorator and it will be automatically traced.
2026/11/26 04:33:08 INFO mlflow.pyfunc: Predicting on input example to validate output
2026/11/26 04:33:15 WARNING mlflow.pyfunc: You have manually traced predict with @mlflow.trace, but this is unnecessary with ResponsesAgent subclasses. You can remove the @mlflow.trace decorator and it will be automatically traced.
Registered model 'demo_unified_model_monitoring.ai_factory_ml.credit_policy_rag' already exists. Creating a new version of this model...
```
```text
Uploading artifacts:   0%|          | 0/12 [00:00<?, ?it/s]
```
```text
🔗 Created version '2' of model 'demo_unified_model_monitoring.ai_factory_ml.credit_policy_rag': https://adb-7405615658009638.18.azuredatabricks.net/explore/data/models/demo_unified_model_monitoring/ai_factory_ml/credit_policy_rag/version/2?o=7405615658009638
```
```text
✓ registered demo_unified_model_monitoring.ai_factory_ml.credit_policy_rag v2
```
```text
If you are using MLflow Tracing, you can migrate your traces to Unity Catalog for unlimited storage, fine-grained access controls, and queryability from notebooks, SQL, and dashboards. Learn more: https://docs.databricks.com/aws/en/mlflow3/genai/tracing/migrate-traces-to-uc
2026/11/26 04:33:24 WARNING mlflow.models.model: `artifact_path` is deprecated. Please use `name` instead.
🔗 View Logged Model at: https://adb-7405615658009638.18.azuredatabricks.net/ml/experiments/1123681276224059/models/m-69c5ab17d6f44dafb094c6b3806b0a28?o=7405615658009638
2026/11/26 04:33:24 WARNING mlflow.pyfunc: You have manually traced predict with @mlflow.trace, but this is unnecessary with ResponsesAgent subclasses. You can remove the @mlflow.trace decorator and it will be automatically traced.
2026/11/26 04:33:24 INFO mlflow.pyfunc: Predicting on input example to validate output
2026/11/26 04:33:30 WARNING mlflow.pyfunc: You have manually traced predict with @mlflow.trace, but this is unnecessary with ResponsesAgent subclasses. You can remove the @mlflow.trace decorator and it will be automatically traced.
Registered model 'demo_unified_model_monitoring.ai_factory_ml.rm_client_brief_agent' already exists. Creating a new version of this model...
```
```text
Uploading artifacts:   0%|          | 0/12 [00:00<?, ?it/s]
```
```text
🔗 Created version '2' of model 'demo_unified_model_monitoring.ai_factory_ml.rm_client_brief_agent': https://adb-7405615658009638.18.azuredatabricks.net/explore/data/models/demo_unified_model_monitoring/ai_factory_ml/rm_client_brief_agent/version/2?o=7405615658009638
```
```text
✓ registered demo_unified_model_monitoring.ai_factory_ml.rm_client_brief_agent v2
```
```text
Downloading artifacts:   0%|          | 0/1 [00:00<?, ?it/s]
```
```text
Note: Databricks recommends Databricks Apps for new agent deployments.
For guidance on how to migrate, see https://docs.databricks.com/aws/en/agents/custom-agents/migrate-agent-to-apps

For this deployment:
View status: https://adb-7405615658009638.18.azuredatabricks.net/ml/endpoints/umm-demo-rag-endpoint/?o=7405615658009638
Review App: https://adb-7405615658009638.18.azuredatabricks.net/ml/review-v2/5caedf535abd4f1e8be89c180f64535a/chat?o=7405615658009638

Production monitoring: https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/production-monitoring
Deploying demo_unified_model_monitoring.ai_factory_ml.credit_policy_rag v2 → umm-demo-rag-endpoint
```
```text
Downloading artifacts:   0%|          | 0/1 [00:00<?, ?it/s]
```
```text
Note: Databricks recommends Databricks Apps for new agent deployments.
For guidance on how to migrate, see https://docs.databricks.com/aws/en/agents/custom-agents/migrate-agent-to-apps

For this deployment:
View status: https://adb-7405615658009638.18.azuredatabricks.net/ml/endpoints/umm-demo-agent-endpoint/?o=7405615658009638
Review App: https://adb-7405615658009638.18.azuredatabricks.net/ml/review-v2/7e18c4e19a2d47f0bbb784b9a4966cfa/chat?o=7405615658009638

Production monitoring: https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/production-monitoring
Deploying demo_unified_model_monitoring.ai_factory_ml.rm_client_brief_agent v2 → umm-demo-agent-endpoint
✓ umm-demo-rag-endpoint READY
✓ umm-demo-agent-endpoint READY
```
.table-result-container { max-height: 300px; overflow: auto; } table, th, td { border: 1px solid black; border-collapse: collapse; } th, td { padding: 5px; } th { text-align: left; } check result value Memory search works · asia desk PASS 4 Other desk's RM has nothing in asia store PASS 0 Memory search works · west desk PASS 4 Other desk's RM has nothing in west store PASS 0 Triage inbox rows PASS 48 Memory pipelines PASS 2 Agent v2.3.0 returns memory proposals PASS [{'path': '/preferences/brief-format.md', 'content': 'RM prefers client briefs formatted with five bullets maximum.'}]
```text
Phase 7A complete. Next: UMM_Phase7B_Memory_Traffic.
```
