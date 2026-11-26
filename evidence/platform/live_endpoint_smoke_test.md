# Live endpoint smoke test (2026-11-26 08:47 UTC)

## 1. Credit PD model: `umm-demo-pd-endpoint` (UC model `credit_ml.pd_counterparty_default` v2)
I took the 5 most recent requests from `credit_monitoring.pd_inference_log`, sent their features back to the endpoint, and compared the result with the logged probability.

| request_id | pd_probability | rescored_pd | abs_diff |
|---|---|---|---|
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-3 | 0.06425368034115699 | 0.06425368034115699 | 0.0 |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-1 | 0.04262056617060066 | 0.04262056617060066 | 0.0 |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-2 | 0.01666922908129344 | 0.01666922908129344 | 0.0 |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-0 | 0.18781627306848345 | 0.18781627306848345 | 0.0 |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-4 | 0.02087042747007001 | 0.02087042747007001 | 0.0 |

Result: all 5 re-scored probabilities match the logged values (abs_diff = 0.0), so the served model and the inference log agree.

Raw response:
```json
{
 "predictions": [
  {
   "prediction": 0,
   "pd_probability": 0.06425368034115699
  },
  {
   "prediction": 0,
   "pd_probability": 0.04262056617060066
  },
  {
   "prediction": 0,
   "pd_probability": 0.01666922908129344
  },
  {
   "prediction": 0,
   "pd_probability": 0.18781627306848345
  },
  {
   "prediction": 0,
   "pd_probability": 0.02087042747007001
  }
 ]
}
```

## 2. Credit Policy RAG agent: `umm-demo-rag-endpoint`
Question: "What is the maximum single-name exposure limit for a counterparty rated below investment grade?"

Answer from the agent:
```text
Based on the policy context provided, I cannot answer your specific question about the maximum single-name exposure limit for a counterparty rated **below investment grade**.

The context contains:
- A limit of **USD 15 million** for **unrated corporates** [LIM-003@v2]
- A limit of **USD 100 million** as the maximum without Group Credit Committee approval [LIM-001@v1]

However, neither document specifically addresses limits for counterparties that are **rated below investment grade** (i.e., sub-investment grade or speculative grade ratings). The policy excerpt for unrated corporates is distinct from below-investment-grade rated entities.

To answer your question accurately, I would need policy context that specifically covers sub-investment grade or below-investment-grade rated counterparties.
```
The answer is grounded: it cites the retrieved documents (LIM-003@v2, LIM-001@v1) and declines to state a limit that the context doesn't cover.

Full response (truncated to 6000 chars):
```json
{
 "custom_outputs": {
  "app_version": "1.6.0",
  "usage": {
   "cache_read_input_tokens": 0,
   "completion_tokens": 181,
   "prompt_tokens": 580,
   "total_tokens": 761,
   "cache_creation_input_tokens": 0
  },
  "contexts": [
   "Counterparty Limit Setting and Utilisation Standard (document LIM-003, version v2, effective 2026-11-10). Under this standard, the single counterparty limit for unrated corporates is USD 15 million. Any proposal above this must carry a documented rationale and be recorded in the credit system before approval.",
   "Counterparty Limit Setting and Utilisation Standard (document LIM-003, version v2, effective 2026-11-10). For monitoring purposes, the utilisation level that triggers an early-warning alert is 75%. Breaches are reported in the monthly risk pack and tracked until closed.",
   "Counterparty Limit Setting and Utilisation Standard (document LIM-003, version v2, effective 2026-11-10). The limit review frequency is every 12 months. Exceptions must be time-bound, approved in writing and reviewed at least annually.",
   "Counterparty Limit Setting and Utilisation Standard (document LIM-003, version v2, effective 2026-11-10). This standard sets the CIB requirements for limit setting, utilisation and breach handling. It applies to relationship managers, credit officers and limit controllers in every country where Meridian Global Bank books Corporate and Investment Banking business. Local rules apply where stricter.",
   "Single Obligor Limit Policy (document LIM-001, version v1, effective 2025-07-23). Under this standard, the maximum limit without Group Credit Committee approval is USD 100 million. Any proposal above this must carry a documented rationale and be recorded in the credit system before approval."
  ],
  "n_llm_calls": 1,
  "retrieved_doc_ids": [
   "LIM-003@v2",
   "LIM-001@v1"
  ],
  "app": "credit_policy_rag",
  "k": 5
 },
 "object": "response",
 "databricks_output": {
  "databricks_request_id": "337dc2ef-592f-4f17-b033-430da8fb0670"
 },
 "id": "337dc2ef-592f-4f17-b033-430da8fb0670",
 "output": [
  {
   "type": "message",
   "id": "c41dcb75-6287-4abf-b5ff-fc4a0610c405",
   "role": "assistant",
   "content": [
    {
     "type": "output_text",
     "text": "Based on the policy context provided, I cannot answer your specific question about the maximum single-name exposure limit for a counterparty rated **below investment grade**.\n\nThe context contains:\n- A limit of **USD 15 million** for **unrated corporates** [LIM-003@v2]\n- A limit of **USD 100 million** as the maximum without Group Credit Committee approval [LIM-001@v1]\n\nHowever, neither document specifically addresses limits for counterparties that are **rated below investment grade** (i.e., sub-investment grade or speculative grade ratings). The policy excerpt for unrated corporates is distinct from below-investment-grade rated entities.\n\nTo answer your question accurately, I would need policy context that specifically covers sub-investment grade or below-investment-grade rated counterparties."
    }
   ]
  }
 ]
}
```
