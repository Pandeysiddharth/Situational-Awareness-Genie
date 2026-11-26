# Unity Catalog table inventory: catalog `demo_unified_model_monitoring`

Row counts from live `SELECT count(*)` on 2026-11-26 08:39 UTC. ERR rows are vector-search index internals that cannot be read with Spark.

| table_schema | table_name | table_type | row_count | comment |
|---|---|---|---|---|
| ai_factory_ml | __dacb8223_161e_4ef7_8871_8886f3060018___online_index_view | MANAGED | ERR [DELTA_READ_TABLE_WITHOUT_COLUMNS] You are trying to read a  | None |
| ai_factory_ml | credit_policy_rag_payload | MANAGED | 317 | None |
| ai_factory_ml | event_log_dacb8223_161e_4ef7_8871_8886f3060018 | MANAGED | 21 | None |
| ai_factory_ml | policy_chunks_index | FOREIGN | ERR (org.apache.spark.SparkClassNotFoundException) [DATA_SOURCE_ | Managed Vector Index with Delta Sync |
| ai_factory_ml | policy_chunks_index_source | MANAGED | 144 | Vector Search source: currently published policy chunks (triggered sync). |
| ai_factory_ml | rm_client_brief_agent_payload | MANAGED | 259 | None |
| ai_factory_monitoring | agent_memory_daily | MANAGED | 7 | Daily memory metrics for the RM client brief agent (Phase 7C). Read by the hub gold refresh. |
| ai_factory_monitoring | agent_memory_metrics | MANAGED | 57 | None |
| ai_factory_monitoring | agent_memory_responses | MANAGED | 69 | None |
| ai_factory_monitoring | agent_memory_snapshot | MANAGED | 8 | None |
| ai_factory_monitoring | agent_request_metrics | MANAGED | 16397 | Lane C. One row per agent request with tool, permission, approval and judge outcomes. record_source = synthetic_history \| live. |
| ai_factory_monitoring | agent_request_metrics_drift_metrics | MANAGED | 7560 | None |
| ai_factory_monitoring | agent_request_metrics_profile_metrics | MANAGED | 7812 | None |
| ai_factory_monitoring | fm_shared_payload | MANAGED | 1139 | None |
| ai_factory_monitoring | genai_live_responses | MANAGED | 480 | None |
| ai_factory_monitoring | llm_usage_log | MANAGED | 92495 | Lane D. One row per call to the shared LLM endpoint, tagged with request_tags['app']. record_source = synthetic_history \| live. |
| ai_factory_monitoring | llm_usage_log_drift_metrics | MANAGED | 1144 | None |
| ai_factory_monitoring | llm_usage_log_profile_metrics | MANAGED | 1196 | None |
| ai_factory_monitoring | rag_request_metrics | MANAGED | 18431 | Lane C. One row per RAG request with judge results (0/1), retrieval IDs, feedback and tokens. record_source = synthetic_history \| live. |
| ai_factory_monitoring | rag_request_metrics_drift_metrics | MANAGED | 6480 | None |
| ai_factory_monitoring | rag_request_metrics_profile_metrics | MANAGED | 6723 | None |
| ai_factory_raw | agent_clients | MANAGED | 50 | Synthetic client data behind the RM client brief agent's tools. |
| ai_factory_raw | agent_exposures | MANAGED | 150 | Synthetic client data behind the RM client brief agent's tools. |
| ai_factory_raw | agent_limits_restricted | MANAGED | 50 | Synthetic client data behind the RM client brief agent's tools. |
| ai_factory_raw | agent_news | MANAGED | 100 | Synthetic client data behind the RM client brief agent's tools. |
| ai_factory_raw | policy_doc_chunks | MANAGED | 148 | All policy document chunks, every version. The index source holds the published version only. |
| ai_factory_raw | policy_doc_registry | MANAGED | 37 | CIB policy document registry for the RAG assistant. LIM-003 has a v2 effective on scenario S5 day. |
| ai_factory_raw | rag_question_bank | MANAGED | 116 | Labelled RAG questions with expected document versions, plus adversarial prompts. |
| ai_factory_raw | rm_roster | MANAGED | 8 | None |
| credit_ml | pd_baseline | MANAGED | 12514 | PD baseline: time-based holdout for each model version, scored by a model trained without it. |
| credit_monitoring | pd_endpoint_payload | MANAGED | 31 | None |
| credit_monitoring | pd_inference_log | MANAGED | 29811 | Lane A monitored table for the PD model. Backfilled history plus live endpoint traffic (record_source). Labels arrive 7 days after scoring. |
| credit_monitoring | pd_inference_log_drift_metrics | MANAGED | 26026 | None |
| credit_monitoring | pd_inference_log_profile_metrics | MANAGED | 18304 | None |
| credit_raw | pd_outcomes | MANAGED | 27004 | Default outcomes for PD scoring requests, arriving 7 days after scoring (demo proxy for the 12-month default horizon). |
| credit_raw | pd_scoring_requests | MANAGED | 29261 | Synthetic PD scoring requests for the 90-day demo window (features only). Contains scripted drift scenarios S1, S2. |
| credit_raw | pd_training | MANAGED | 24000 | Synthetic CIB counterparty applications with default outcome. 12 months before the demo window. PD v1 training data. |
| hub_gold | dim_metric_thresholds | VIEW | 56 | None |
| hub_gold | dim_model | VIEW | 5 | None |
| hub_gold | fact_alert_triage | MANAGED | 48 | Alert triage state from Lakebase (umm.alert_inbox): status, priority, assignee, acknowledged/resolved times. Synced nightly. |
| hub_gold | fact_alert_triage_events | MANAGED | 84 | Every triage action from Lakebase (umm.alert_triage_events): who did what, when. |
| hub_gold | fact_alerts | MANAGED | 78 | Status changes per model metric: breach (green to amber/red), escalation (amber to red) and resolved (back to green). |
| hub_gold | fact_metric_health_daily | MANAGED | 4048 | One row per model, contract metric and day: raw daily value, evaluated value (rolling mean, or ratio to baseline), thresholds and status (green/amber/red). source = monitor (data profiling outputs), backfill (same definition, computed from the monitored table) or hub_pooled (drift on a trailing 7/14-day pooled window). |
| hub_gold | fact_metric_slice_daily | MANAGED | 3942 | Slice-level detail: per-country values behind the fairness gaps, groundedness by RAG query category, and task success by agent intent. |
| hub_gold | fact_model_health_daily | MANAGED | 456 | One row per model per day: health score (100 = all metrics green; amber counts 1, red counts 2), worst status, and the metrics in red or amber. |
| hub_gold | fact_triage_kpis | MANAGED | 3 | Triage KPIs by owner team: open alerts by priority, mean time to acknowledge and resolve (hours, last 30 days), false-positive rate. |
| hub_gold | model_version_registry | VIEW | 5 | None |
| hub_gold | mv_metric_health | METRIC_VIEW | 4048 | None |
| hub_gold | mv_model_health | METRIC_VIEW | 456 | None |
| hub_ref | dim_metric | MANAGED | 50 | UMM metric dictionary. One row per bucket + contract name, from the CIB metric catalogue. |
| hub_ref | dim_model | MANAGED | 5 | UMM model inventory. One row per monitored model or LLM endpoint. |
| hub_ref | genai_runtime_config | MANAGED | 1 | Runtime settings chosen by Phase 3A. Read by Phase 3B/3C. |
| hub_ref | hub_runtime_config | MANAGED | 1 | None |
| hub_ref | map_model_metric | MANAGED | 56 | Metric applicability, implementation and thresholds per model. scope = in_scope \| descoped \| supporting. |
| hub_ref | memory_runtime_config | MANAGED | 1 | None |
| hub_ref | metric_source_map | MANAGED | 53 | None |
| hub_ref | model_version_registry | MANAGED | 5 | Registered model versions, live periods, policy thresholds and holdout metrics. |
| hub_ref | ref_llm_pricing | MANAGED | 1 | Token pricing used by hub cost metrics. Illustrative values for the demo. |
| hub_ref | scenario_calendar | MANAGED | 10 | PRESENTER REFERENCE ONLY. Scripted demo scenarios. Do not add to the Genie space. |
| markets_ml | trade_volume_baseline | MANAGED | 2700 | Forecast baseline: last 90 days of training period, scored by a model trained without them. |
| markets_monitoring | trade_volume_scoring_log | MANAGED | 2730 | Lane B monitored table for the trade volume forecast. One row per trade_date x country x product with actual (label). |
| markets_monitoring | trade_volume_scoring_log_drift_metrics | MANAGED | 21120 | None |
| markets_monitoring | trade_volume_scoring_log_profile_metrics | MANAGED | 14640 | None |
| markets_raw | trade_volume_daily | MANAGED | 24630 | Synthetic daily trade volume by country and product. 2 years train + 90-day demo window. Contains scenario S3 (FX volatility shock). |
| spoke_corp_banking | cib_credit_pd_payload | MANAGED | 10 | None |
| spoke_corp_banking | credit_applications | MANAGED | 20000 | Synthetic corporate credit applications with 12m default label |
| spoke_markets | cib_fx_anomaly_payload | MANAGED | 10 | None |
| spoke_markets | fx_trades | MANAGED | 50000 | Synthetic FX trades with anomaly label |
| spoke_txn_banking | cib_payments_forecast_payload | MANAGED | 10 | None |
| spoke_txn_banking | payment_volumes | MANAGED | 5616 | Synthetic daily cross-border payment counts per corridor with lag features |
