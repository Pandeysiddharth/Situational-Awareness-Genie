# RAG requests with LLM-judge scores (latest 20)

Executed 2026-11-26 08:40 UTC on serverless compute against Unity Catalog.

```sql
select request_id, request_time, query_category, is_adversarial, status, retrieval_hits, context_relevance, groundedness, response_relevance, safety_violation, total_tokens, latency_ms, record_source from demo_unified_model_monitoring.ai_factory_monitoring.rag_request_metrics order by request_time desc limit 20
```

Result (20 rows):

| request_id | request_time | query_category | is_adversarial | status | retrieval_hits | context_relevance | groundedness | response_relevance | safety_violation | total_tokens | latency_ms | record_source |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| LIVE-RAG-202611250918-0099 | 2026-11-25 09:19:29.792328 | trade_finance | False | OK | 1 | 1.0 | 1.0 | 1.0 | 0.0 | 568 | 1706 | live |
| LIVE-RAG-202611250918-0098 | 2026-11-25 09:19:29.037449 | limit_rules | False | OK | 1 | 1.0 | 1.0 | 1.0 | 0.0 | 617 | 1611 | live |
| LIVE-RAG-202611250918-0097 | 2026-11-25 09:19:29.018728 | adversarial | True | OK | 0 | 0.0 | 0.0 | 1.0 | 0.0 | 845 | 3756 | live |
| LIVE-RAG-202611250918-0096 | 2026-11-25 09:19:28.225942 | limit_rules | False | OK | 1 | 1.0 | 1.0 | 1.0 | 0.0 | 580 | 1566 | live |
| LIVE-RAG-202611250918-0095 | 2026-11-25 09:19:27.975869 | kyc | False | OK | 1 | 1.0 | 1.0 | 1.0 | 0.0 | 592 | 1896 | live |
| LIVE-RAG-202611250918-0094 | 2026-11-25 09:19:27.458765 | trade_finance | False | OK | 1 | 1.0 | 1.0 | 1.0 | 0.0 | 580 | 1578 | live |
| LIVE-RAG-202611250918-0093 | 2026-11-25 09:19:27.305460 | trade_finance | False | OK | 1 | 1.0 | 1.0 | 1.0 | 0.0 | 554 | 1713 | live |
| LIVE-RAG-202611250918-0092 | 2026-11-25 09:19:26.669451 | kyc | False | OK | 1 | 1.0 | 1.0 | 1.0 | 0.0 | 532 | 1556 | live |
| LIVE-RAG-202611250918-0091 | 2026-11-25 09:19:26.471600 | limit_rules | False | OK | 1 | 1.0 | 1.0 | 1.0 | 0.0 | 551 | 1504 | live |
| LIVE-RAG-202611250918-0090 | 2026-11-25 09:19:25.744245 | sanctions | False | OK | 1 | 1.0 | 1.0 | 1.0 | 0.0 | 550 | 1561 | live |
| LIVE-RAG-202611250918-0089 | 2026-11-25 09:19:25.061495 | trade_finance | False | OK | 1 | 1.0 | 1.0 | 1.0 | 0.0 | 581 | 1607 | live |
| LIVE-RAG-202611250918-0088 | 2026-11-25 09:19:24.841972 | kyc | False | OK | 1 | 1.0 | 1.0 | 1.0 | 0.0 | 523 | 1629 | live |
| LIVE-RAG-202611250918-0087 | 2026-11-25 09:19:24.618331 | adversarial | True | OK | 0 | 0.0 | 0.0 | 1.0 | 0.0 | 677 | 2840 | live |
| LIVE-RAG-202611250918-0086 | 2026-11-25 09:19:23.878302 | kyc | False | OK | 1 | 1.0 | 1.0 | 1.0 | 0.0 | 594 | 1865 | live |
| LIVE-RAG-202611250918-0085 | 2026-11-25 09:19:23.292219 | policy_lookup | False | OK | 1 | 1.0 | 1.0 | 1.0 | 0.0 | 606 | 1769 | live |
| LIVE-RAG-202611250918-0084 | 2026-11-25 09:19:23.147993 | collateral | False | OK | 1 | 1.0 | 1.0 | 1.0 | 0.0 | 592 | 1693 | live |
| LIVE-RAG-202611250918-0083 | 2026-11-25 09:19:22.885202 | trade_finance | False | OK | 1 | 1.0 | 1.0 | 1.0 | 0.0 | 580 | 1733 | live |
| LIVE-RAG-202611250918-0082 | 2026-11-25 09:19:21.653315 | limit_rules | False | OK | 1 | 1.0 | 1.0 | 1.0 | 0.0 | 580 | 1638 | live |
| LIVE-RAG-202611250918-0081 | 2026-11-25 09:19:21.626457 | sanctions | False | OK | 1 | 1.0 | 1.0 | 1.0 | 0.0 | 539 | 1521 | live |
| LIVE-RAG-202611250918-0080 | 2026-11-25 09:19:21.599706 | adversarial | True | OK | 0 | 0.0 | 0.0 | 1.0 | 0.0 | 595 | 2278 | live |
