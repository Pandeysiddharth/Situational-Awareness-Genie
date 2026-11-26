# umm-demo-daily / run 290566913448832 / task `genai_traffic`

* task_run_id: 884606621725544
* notebook: /Users/pandey.siddharth@databricks.com/Unified_Model_Monitoring/FEBar_Demo/UMM_Phase3B_Send_Traffic
* start: 2026-11-26 00:32:32 UTC  end: 2026-11-26 00:38:26 UTC
* result_state: SUCCESS  message: 

## Captured cell output
```text
Batch 202611260032 · umm-demo-rag-endpoint · umm-demo-agent-endpoint
100 RAG requests · 60 agent requests
warm-up umm-demo-rag-endpoint: OK in 76158 ms 
warm-up umm-demo-agent-endpoint: OK in 85909 ms 
✓ 160/160 requests succeeded

app | status | n | avg_latency_ms | with_trace
credit_policy_rag | OK | 100 | 1855.0 | 100
rm_client_brief_agent | OK | 60 | 8312.0 | 60
Next: run UMM_Phase3C_Process_And_Validate (AI Gateway rows can take up to an hour to land).
```
