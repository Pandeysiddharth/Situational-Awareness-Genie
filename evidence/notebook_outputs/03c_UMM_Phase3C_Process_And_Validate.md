# Captured output: `03c_UMM_Phase3C_Process_And_Validate.ipynb`

This is the plain-text output stored in the notebook the last time it ran interactively on Databricks. HTML tables are converted to markdown.


## Cell 1

```text
mlflow 3.18.0 · databricks-agents 1.11.0
160 live responses to process (100 RAG, 60 agent)
✓ judged 100 RAG and 60 agent responses · 0 judge errors
✓ appended 100 live rows to demo_unified_model_monitoring.ai_factory_monitoring.rag_request_metrics
✓ appended 60 live rows to demo_unified_model_monitoring.ai_factory_monitoring.agent_request_metrics
✓ 740 judge results logged to traces
✓ Lane D: appended 321 calls from demo_unified_model_monitoring.ai_factory_monitoring.fm_shared_payload
```
.table-result-container { max-height: 300px; overflow: auto; } table, th, td { border: 1px solid black; border-collapse: collapse; } th, td { padding: 5px; } th { text-align: left; } check result value expected RAG live rows PASS 100 > 0 Agent live rows PASS 60 > 0 RAG judge coverage PASS 1.00000 >= 0.9 RAG live groundedness PASS 1.0 >= 0.6 RAG live precision@5 PASS 0.2 > 0.1 Agent judge coverage PASS 1.00000 >= 0.9 Agent resolves client first PASS 0.92308 >= 0.8 Lane D live rows PASS 321 > 0 (re-run later if AI Gateway rows are pending) Lane D attributed to both apps WARN 0 = 2 (needs usage_context support) Judge results on traces PASS 740 > 0
.table-result-container { max-height: 300px; overflow: auto; } table, th, td { border: 1px solid black; border-collapse: collapse; } th, td { padding: 5px; } th { text-align: left; } record_source n groundedness context_relevance response_relevance safety_violation avg_tokens live 100 0.92 0.89 0.98 0.0 586.0 synthetic_history 1415 0.918 0.875 0.924 0.001 2500.0
.table-result-container { max-height: 300px; overflow: auto; } table, th, td { border: 1px solid black; border-collapse: collapse; } th, td { padding: 5px; } th { text-align: left; } record_source n task_success tool_accuracy permission_compliance approval_adherence recovery avg_tokens live 60 0.917 0.8 1.0 1.0 1.0 6060.0 synthetic_history 1309 0.941 0.962 1.0 0.896 0.973 7704.0
```text
Phase 3 complete.
```
