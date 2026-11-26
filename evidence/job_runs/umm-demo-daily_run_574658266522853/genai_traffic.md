# umm-demo-daily / run 574658266522853 / task `genai_traffic`

* task_run_id: 810200526172842
* notebook: /Users/pandey.siddharth@databricks.com/Unified_Model_Monitoring/FEBar_Demo/UMM_Phase3B_Send_Traffic
* start: 2026-11-25 10:16:56 UTC  end: 2026-11-25 10:21:47 UTC
* result_state: SUCCESS  message: 

## Captured cell output
```text
Batch 202611251017 · umm-demo-rag-endpoint · umm-demo-agent-endpoint
100 RAG requests · 60 agent requests
warm-up umm-demo-rag-endpoint: OK in 48382 ms 
warm-up umm-demo-agent-endpoint: OK in 51365 ms 
✓ 160/160 requests succeeded

app | status | n | avg_latency_ms | with_trace
credit_policy_rag | OK | 100 | 1837.0 | 100
rm_client_brief_agent | OK | 60 | 8335.0 | 60
Next: run UMM_Phase3C_Process_And_Validate (AI Gateway rows can take up to an hour to land).
```
