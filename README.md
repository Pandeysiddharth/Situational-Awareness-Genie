# Unified Model Monitoring (UMM): CIB hub-and-spoke model and GenAI monitoring on Databricks

A working demo of one monitoring "control tower" for every model a Corporate & Investment Bank runs: classical ML (credit PD, trade-volume forecast), a RAG assistant, a tool-calling agent with long-term memory, and a shared LLM endpoint. Spoke data lands in Unity Catalog; the hub standardises the metrics into gold tables and metric views; the results are served through an AI/BI dashboard, a Genie space, Lakehouse Monitoring and an alert-triage workflow backed by Lakebase.

Workspace: Azure Databricks. Catalog: `demo_unified_model_monitoring` (70 tables/views across 14 schemas). Exported 2026-11-26 08:47 UTC.

## Execution evidence (read this first)
Everything below is committed as text (markdown, JSON or ipynb with outputs). There are no screenshots.

| What | Where | What it shows |
|---|---|---|
| Notebooks with stored outputs | `notebooks/*.ipynb` | Every code cell's captured stdout and display tables from the last interactive run |
| The same outputs as plain markdown | `evidence/notebook_outputs/*.md` | Readable without a Jupyter renderer |
| Scheduled job runs | `evidence/job_runs/README.md` and one folder per run | Run IDs, start/end times, per-task status, captured cell output and error text from the Jobs API |
| Live query results | `evidence/query_results/*.md` | 16 SQL queries (SQL plus result table) plus a full table inventory with row counts, run against the gold, monitoring and inference tables at export time |
| Platform state | `evidence/platform/platform_state.md` | Serving endpoints READY, UC model versions and aliases, 5 Lakehouse monitors ACTIVE with successful refreshes |
| Live model output | `evidence/platform/live_endpoint_smoke_test.md` | PD endpoint re-scores 5 logged requests (identical probabilities); the RAG agent answers a policy question with citations |

### Headline results (from the files above)
* Latest hub health (`03_latest_model_health.md`): Counterparty Default PD **red** (threshold calibration, PSI drift); RM Client Brief Agent **red** (memory conflict, memory-grounded answers, PII held in memory); RAG, Shared LLM and Trade Volume Forecast **amber**.
* Triage KPIs (`06_triage_kpis.md`): Credit Risk Analytics has 7 open alerts (4 P1), MTTA 3.0h, MTTR 28.7h.
* Model versions (`01_model_registry.md`): PD v1 to v2 champion switch on 2026-10-11 (holdout AUC 0.8196 to 0.8272); forecast MAE 17.2.
* All 5 Lakehouse Monitoring monitors are ACTIVE and their last refresh was SUCCESS (`platform_state.md`).

### Known issues (reported as found, not fixed)
* `umm-demo-hub-refresh` job: its only run (2026-11-26 03:00 UTC) **FAILED** because the `gold_refresh` task points at a notebook that no longer exists ("New Notebook 2026-11-25 18:35:02"). The gold layer was built by running `05_UMM_Phase5_Hub_Gold_Dashboard_Genie` interactively; its output is in the notebook.
* `umm-demo-daily` job: 2 of 3 runs succeeded. The first run failed because of a notebook access/path error. In the scheduled run, the `genai_process` task exited right after installing packages ("Packages installed and Python restarted. Run the notebook again"), so the job reports SUCCESS even though that step did no processing. The memory tasks were added to the job after that run.
* Phase 5 notebook output shows `UNRESOLVED_COLUMN drift_type` warnings for the 7 agent-memory metrics on the monitor-values path. Those metrics are still produced through `07c_UMM_Phase7C_Memory_Metrics` (see `13_agent_memory_metrics.md`).
* `app/` (`cib-model-monitor`) is the **unmodified Databricks AppKit scaffold**. The app was created but never deployed (no deployments, compute stopped). It is included for completeness and is not part of the evidenced build.
* Data is synthetic. Seeded scenarios (drift, threshold miscalibration, PII in memory) are deliberate, so the monitors have something to catch.

## Repository layout
```
notebooks/            14 Databricks notebooks (.ipynb with outputs), run order = filename prefix
notebooks/source/     the same notebooks as Databricks .py source
dashboards/           AI/BI dashboard definitions (.lvdash.json): hub dashboard (6 pages, 25 widgets) + 5 auto-generated monitor dashboards
genie/                Genie space "UMM · Model Monitoring Genie" serialized config
jobs/                 Lakeflow Jobs settings: umm-demo-daily (00:30 UTC), umm-demo-hub-refresh (03:00 UTC)
app/                  Databricks App scaffold (AppKit, TypeScript); not deployed
evidence/             text execution evidence (see table above)
```

## Build phases (notebooks)
| # | Notebook | Purpose |
|---|---|---|
| 00 | 01_generate_control_tower_data | Early control-tower seed data |
| 01 | umm_all_in_one | Catalog/schemas, 90-day synthetic history for credit, markets, AI factory |
| 02a/b | UMM_Phase2_* | Train and register PD v1/v2 + trade-volume forecast (MLflow, UC), serve PD, live PD processor |
| 03a-c | UMM_Phase3* | Build the RAG agent (vector search) and RM client-brief agent, send traffic, judge responses with MLflow GenAI judges, land AI Gateway usage |
| 04a/b | UMM_Phase4_* | Lakehouse Monitoring monitors (inference_log / time_series + custom metrics), jobs, keep-alive traffic |
| 05 | UMM_Phase5_Hub_Gold_Dashboard_Genie | Hub reference + gold facts, metric views, AI/BI dashboard, Genie space |
| 07a-d | UMM_Phase7* | Agent memory stores, Lakebase triage inbox, memory traffic and metrics, triage sync to gold |

## Reproducing
Import `notebooks/` into a Databricks workspace (serverless) and run them in prefix order, setting the catalog widget/variable at the top of each notebook. Recreate the dashboards from `dashboards/*.lvdash.json` and the jobs from `jobs/*.job.json` (update notebook paths and the warehouse ID). Workspace-specific IDs are kept for traceability only.
