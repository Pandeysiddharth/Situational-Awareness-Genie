# Triage workflow events (latest 30)

Executed 2026-11-26 08:40 UTC on serverless compute against Unity Catalog.

```sql
select * from demo_unified_model_monitoring.hub_gold.fact_alert_triage_events order by at desc limit 30
```

Result (30 rows):

| event_id | alert_id | action | from_status | to_status | actor | note | at |
|---|---|---|---|---|---|---|---|
| 42 | rm_client_brief_agent\|cost_per_request\|2026-11-19 | resolve | acknowledged | resolved | arjun.rao@demo-cib.example | Root cause identified; monitoring recovery | 2026-11-21 04:33:14.052188 |
| 36 | rm_client_brief_agent\|approval_adherence_rate\|2026-11-19 | resolve | acknowledged | resolved | nina.park@demo-cib.example | Root cause identified; monitoring recovery | 2026-11-19 19:02:08.449786 |
| 35 | rm_client_brief_agent\|approval_adherence_rate\|2026-11-19 | acknowledge | open | acknowledged | nina.park@demo-cib.example | None | 2026-11-19 07:09:10.213668 |
| 41 | rm_client_brief_agent\|cost_per_request\|2026-11-19 | acknowledge | open | acknowledged | arjun.rao@demo-cib.example | None | 2026-11-19 05:27:18.189369 |
| 38 | rm_client_brief_agent\|avg_tokens_per_request\|2026-11-15 | resolve | acknowledged | resolved | nina.park@demo-cib.example | Root cause identified; monitoring recovery | 2026-11-17 10:16:40.380427 |
| 40 | rm_client_brief_agent\|cost_per_request\|2026-11-15 | resolve | acknowledged | false_positive | nina.park@demo-cib.example | Within expected variance after review | 2026-11-17 08:24:56.256300 |
| 72 | trade_volume_forecast\|data_drift_psi\|2026-11-17 | investigate | acknowledged | investigating | james.ong@demo-cib.example | Investigating with model owner | 2026-11-17 05:53:12.248408 |
| 71 | trade_volume_forecast\|data_drift_psi\|2026-11-17 | acknowledge | open | acknowledged | james.ong@demo-cib.example | None | 2026-11-17 03:53:12.248408 |
| 20 | pd_counterparty_default\|concept_drift_score\|2026-11-14 | resolve | acknowledged | resolved | mei.lin@demo-cib.example | Root cause identified; monitoring recovery | 2026-11-15 21:34:23.799634 |
| 37 | rm_client_brief_agent\|avg_tokens_per_request\|2026-11-15 | acknowledge | open | acknowledged | nina.park@demo-cib.example | None | 2026-11-15 06:40:43.335368 |
| 28 | pd_counterparty_default\|positive_rate_at_threshold\|2026-11-15 | investigate | acknowledged | investigating | mei.lin@demo-cib.example | Investigating with model owner | 2026-11-15 06:24:26.249652 |
| 39 | rm_client_brief_agent\|cost_per_request\|2026-11-15 | acknowledge | open | acknowledged | nina.park@demo-cib.example | None | 2026-11-15 05:37:18.670560 |
| 27 | pd_counterparty_default\|positive_rate_at_threshold\|2026-11-15 | acknowledge | open | acknowledged | mei.lin@demo-cib.example | None | 2026-11-15 04:24:26.249652 |
| 70 | trade_volume_forecast\|concept_drift_score\|2026-11-13 | resolve | acknowledged | resolved | james.ong@demo-cib.example | Root cause identified; monitoring recovery | 2026-11-15 00:57:39.261149 |
| 19 | pd_counterparty_default\|concept_drift_score\|2026-11-14 | acknowledge | open | acknowledged | mei.lin@demo-cib.example | None | 2026-11-14 07:46:29.129745 |
| 82 | trade_volume_forecast\|quality_gap\|2026-11-11 | resolve | acknowledged | resolved | james.ong@demo-cib.example | Root cause identified; monitoring recovery | 2026-11-14 01:33:28.883345 |
| 6 | credit_policy_rag\|precision_at_k\|2026-11-11 | resolve | acknowledged | resolved | claire.ma@demo-cib.example | Root cause identified; monitoring recovery | 2026-11-13 09:04:15.715059 |
| 69 | trade_volume_forecast\|concept_drift_score\|2026-11-13 | acknowledge | open | acknowledged | james.ong@demo-cib.example | None | 2026-11-13 06:37:02.315441 |
| 4 | credit_policy_rag\|groundedness\|2026-11-11 | resolve | acknowledged | resolved | claire.ma@demo-cib.example | Root cause identified; monitoring recovery | 2026-11-12 15:10:05.338842 |
| 18 | pd_counterparty_default\|accuracy\|2026-11-12 | investigate | acknowledged | investigating | ravi.iyer@demo-cib.example | Investigating with model owner | 2026-11-12 08:54:16.389945 |
| 17 | pd_counterparty_default\|accuracy\|2026-11-12 | acknowledge | open | acknowledged | ravi.iyer@demo-cib.example | None | 2026-11-12 06:54:16.389945 |
| 10 | credit_policy_rag\|recall_at_k\|2026-11-11 | resolve | acknowledged | resolved | claire.ma@demo-cib.example | Root cause identified; monitoring recovery | 2026-11-11 23:05:05.442687 |
| 2 | credit_policy_rag\|context_relevance\|2026-11-11 | resolve | acknowledged | resolved | nina.park@demo-cib.example | Root cause identified; monitoring recovery | 2026-11-11 18:10:53.266623 |
| 78 | trade_volume_forecast\|model_quality_drift\|2026-11-10 | resolve | acknowledged | resolved | james.ong@demo-cib.example | Root cause identified; monitoring recovery | 2026-11-11 10:06:02.596267 |
| 26 | pd_counterparty_default\|positive_rate_at_threshold\|2026-11-11 | investigate | acknowledged | investigating | mei.lin@demo-cib.example | Investigating with model owner | 2026-11-11 08:50:57.388577 |
| 5 | credit_policy_rag\|precision_at_k\|2026-11-11 | acknowledge | open | acknowledged | claire.ma@demo-cib.example | None | 2026-11-11 08:42:39.504074 |
| 25 | pd_counterparty_default\|positive_rate_at_threshold\|2026-11-11 | acknowledge | open | acknowledged | mei.lin@demo-cib.example | None | 2026-11-11 06:50:57.388577 |
| 9 | credit_policy_rag\|recall_at_k\|2026-11-11 | acknowledge | open | acknowledged | claire.ma@demo-cib.example | None | 2026-11-11 06:33:41.964979 |
| 1 | credit_policy_rag\|context_relevance\|2026-11-11 | acknowledge | open | acknowledged | nina.park@demo-cib.example | None | 2026-11-11 04:40:51.023981 |
| 81 | trade_volume_forecast\|quality_gap\|2026-11-11 | acknowledge | open | acknowledged | james.ong@demo-cib.example | None | 2026-11-11 04:37:43.257133 |
