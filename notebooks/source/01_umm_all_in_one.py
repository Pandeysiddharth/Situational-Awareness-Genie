# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # UMM Demo · Phase 1 · All-in-one
# MAGIC
# MAGIC Self-contained version of notebooks 00–06 (no `%run` needed). Attach to serverless compute and click **Run all**.
# MAGIC
# MAGIC Configured for single-catalog mode in `demo_unified_model_monitoring`. Each simulated workspace becomes a set of schemas: `credit_*`, `markets_*`, `ai_factory_*`, `hub_*`.

# COMMAND ----------

USE_SINGLE_CATALOG = True
SINGLE_CATALOG = "demo_unified_model_monitoring"

ANCHOR_DATE = None          # e.g. "2026-12-01" to pin the last day of history; None = yesterday
HISTORY_DAYS = 90           # days of backfilled history shown in the demo
LABEL_DELAY_DAYS = 7        # PD outcomes arrive this many days after scoring (demo proxy, see README)
SEED = 20261125

# COMMAND ----------

import datetime as dt
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

try:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
except Exception as e:  # some serverless configs disallow this; timestamps are generated as UTC either way
    print(f"Note: could not set session timezone ({e})")

# --- Simulated workspaces ----------------------------------------------------
# In production each "area" is a separate workspace. In the demo each one is a catalog.
AREAS = {
    "credit": {
        "catalog": "umm_demo_cib_credit",
        "schemas": ["raw", "ml", "monitoring"],
        "simulated_workspace": "cib-credit-prod-sg",
        "owner_team": "CIB Credit Risk Analytics",
        "owner_mailbox": "cib-credit-analytics@example.com",
    },
    "markets": {
        "catalog": "umm_demo_cib_markets",
        "schemas": ["raw", "ml", "monitoring"],
        "simulated_workspace": "cib-markets-prod-sg",
        "owner_team": "CIB Markets Analytics",
        "owner_mailbox": "cib-markets-analytics@example.com",
    },
    "ai_factory": {
        "catalog": "umm_demo_ai_factory",
        "schemas": ["raw", "ml", "monitoring"],
        "simulated_workspace": "ai-factory-prod-sg",
        "owner_team": "AI Factory",
        "owner_mailbox": "ai-factory-platform@example.com",
    },
    "hub": {
        "catalog": "umm_demo_hub",
        "schemas": ["ref", "gold"],
        "simulated_workspace": "umm-hub-prod-sg",
        "owner_team": "CIB Model Risk & Monitoring",
        "owner_mailbox": "cib-model-monitoring@example.com",
    },
}


def catalog_for(area: str) -> str:
    return SINGLE_CATALOG if USE_SINGLE_CATALOG else AREAS[area]["catalog"]


def schema_for(area: str, schema: str) -> str:
    return f"{area}_{schema}" if USE_SINGLE_CATALOG else schema


def fq(area: str, schema: str, table: str) -> str:
    """Fully qualified table name, respecting the single-catalog fallback."""
    return f"{catalog_for(area)}.{schema_for(area, schema)}.{table}"


# --- Model identifiers -------------------------------------------------------
PD_MODEL = "pd_counterparty_default"
FCST_MODEL = "trade_volume_forecast"
RAG_APP = "credit_policy_rag"
AGENT_APP = "rm_client_brief_agent"
LLM_ENDPOINT = "llm_endpoint_shared"
LLM_ENDPOINT_NAME = "umm-demo-llm"

# --- Time window -------------------------------------------------------------
HISTORY_END = (dt.date.fromisoformat(ANCHOR_DATE) if ANCHOR_DATE
               else dt.date.today() - dt.timedelta(days=1))
HISTORY_START = HISTORY_END - dt.timedelta(days=HISTORY_DAYS - 1)
DAYS = pd.date_range(HISTORY_START, HISTORY_END, freq="D")


def day_date(d: int) -> dt.date:
    """Calendar date for history day index d (0 = first day of history)."""
    return HISTORY_START + dt.timedelta(days=int(d))


# --- Scenario calendar (day indexes are 0-based within the history window) ---
# Each scenario is a scripted "something went amber" moment. Generators read these
# windows so the calendar and the data can never disagree.
SCENARIOS = [
    dict(scenario_id="S0_PD_V2_DEPLOY", model_id=PD_MODEL, start_day=45, end_day=HISTORY_DAYS - 1,
         kind="deployment", title="PD model v2 goes live",
         narrative="Retrained PD model (v2) replaces v1. Metrics split by model_version show a step change.",
         expected_signals="Step change in PD metrics when sliced by model_version"),
    dict(scenario_id="S1_PD_LEVERAGE_DRIFT", model_id=PD_MODEL, start_day=60, end_day=HISTORY_DAYS - 1,
         kind="data_drift", title="Leverage drift in PD inputs",
         narrative="debt_to_ebitda ramps up ~35% and utilisation_pct by ~15 points over days 60-69, then stays elevated.",
         expected_signals="data_drift_psi, data_drift_js amber then red; positive_rate_at_threshold rises"),
    dict(scenario_id="S2_PD_IN_DEFAULTS", model_id=PD_MODEL, start_day=66, end_day=HISTORY_DAYS - 1,
         kind="concept_drift", title="Defaults rise in India beyond what features explain",
         narrative="Observed default rate for IN counterparties roughly doubles without a matching move in inputs.",
         expected_signals="label_drift_js, concept_drift_score, recall drop, quality_gap and approval_rate_gap by country"),
    dict(scenario_id="S3_FCST_FX_VOL_SHOCK", model_id=FCST_MODEL, start_day=72, end_day=80,
         kind="market_event", title="FX volatility shock",
         narrative="fx_vol_index spikes from ~8 to ~19 and FX product volumes jump and become erratic for 9 days.",
         expected_signals="data drift on fx_vol_index, MAE/RMSE spike, concept_drift_score (residual drift), model_quality_drift"),
    dict(scenario_id="S4_RAG_SAFETY_PROBE", model_id=RAG_APP, start_day=55, end_day=57,
         kind="safety", title="Adversarial prompt burst",
         narrative="About 20% of RAG traffic is prompt-injection or PII-probing for 3 days.",
         expected_signals="safety_violation_rate red; query_intent_alignment dips"),
    dict(scenario_id="S5_RAG_STALE_INDEX", model_id=RAG_APP, start_day=75, end_day=80,
         kind="retrieval", title="Limits policy updated, vector index stale for 6 days",
         narrative="LIM-003 v2 takes effect on day 75 but the index still serves v1 until a re-index on day 81. Limit questions spike.",
         expected_signals="groundedness, context_relevance, precision_at_k, recall_at_k amber; user_satisfaction_score dips"),
    dict(scenario_id="S6_AGENT_TOKEN_CREEP", model_id=AGENT_APP, start_day=50, end_day=HISTORY_DAYS - 1,
         kind="cost", title="Prompt bloat drives token creep",
         narrative="Agent tokens per call grow ~0.7% per day from day 50.",
         expected_signals="avg_tokens_per_request and cost_per_request cross amber (1.2x baseline) around day 80"),
    dict(scenario_id="S7_AGENT_TOOL_TIMEOUTS", model_id=AGENT_APP, start_day=62, end_day=65,
         kind="reliability", title="Exposure API timeouts",
         narrative="get_exposure_summary times out on ~45% of calls and the agent often fails to recover.",
         expected_signals="recovery_success_rate red; tool_accuracy, trace_completeness, task_success_rate amber"),
    dict(scenario_id="S8_AGENT_BAD_RELEASE", model_id=AGENT_APP, start_day=70, end_day=72,
         kind="permissions", title="Agent v2.1.0 calls a restricted tool",
         narrative="A new prompt template makes the agent call get_limit_details for users who aren't entitled, instead of escalating. Hotfix v2.1.1 on day 73.",
         expected_signals="permission_compliance_rate red; escalation_appropriateness red; decision_path_validity dips"),
    dict(scenario_id="S9_AGENT_APPROVAL_BYPASS", model_id=AGENT_APP, start_day=84, end_day=86,
         kind="governance", title="Client briefs sent without approval",
         narrative="send_brief executes without the required approval flag on ~25% of calls.",
         expected_signals="approval_adherence_rate red"),
]


def scenario(sid: str) -> dict:
    return next(s for s in SCENARIOS if s["scenario_id"] == sid)


def in_scenario(sid: str, d: int) -> bool:
    s = scenario(sid)
    return s["start_day"] <= d <= s["end_day"]


# --- Helpers -----------------------------------------------------------------
def sigmoid(x):
    return 1.0 / (1.0 + np.exp(-x))


def request_seconds(rng, n, asia_weight=0.7):
    """Seconds-after-midnight (UTC), weighted to Asia then London business hours."""
    asia = rng.normal(4.0, 2.5, n)
    london = rng.normal(10.5, 2.0, n)
    hours = np.where(rng.random(n) < asia_weight, asia, london)
    return (np.clip(hours, 0, 23.99) * 3600).astype(int)


def write_table(pdf, area, schema, table, array_cols=(), nullable_cols=(), date_cols=(),
                map_cols=None, cdf=False, comment=None):
    """Write a pandas DataFrame to a managed Delta table.

    array_cols   : pipe-delimited strings -> array<string> ('' -> empty array)
    nullable_cols: float columns where NaN means "not applicable" -> SQL NULL
    date_cols    : convert to DATE
    map_cols     : {target_col: (key_literal, source_col)} -> map<string,string>
    """
    sdf = spark.createDataFrame(pdf)
    for c in array_cols:
        sdf = sdf.withColumn(
            c, F.when(F.col(c) == "", F.array().cast("array<string>")).otherwise(F.split(F.col(c), r"\|")))
    for c in nullable_cols:
        sdf = sdf.withColumn(c, F.when(F.isnan(F.col(c)), F.lit(None).cast("double")).otherwise(F.col(c)))
    for c in date_cols:
        sdf = sdf.withColumn(c, F.to_date(F.col(c)))
    for target, (key, src) in (map_cols or {}).items():
        sdf = sdf.withColumn(target, F.create_map(F.lit(key), F.col(src))).drop(src)
    name = fq(area, schema, table)
    sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)
    if cdf:
        spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    if comment:
        safe = comment.replace("'", "\\'")
        spark.sql(f"COMMENT ON TABLE {name} IS '{safe}'")
    print(f"✓ {name}: {spark.table(name).count():,} rows")
    return name


print(f"History window: {HISTORY_START} → {HISTORY_END} ({HISTORY_DAYS} days)")
print("Catalogs:", {a: catalog_for(a) for a in AREAS})

# COMMAND ----------

# MAGIC %md
# MAGIC # UMM Demo · 01 · Catalogs, schemas and shared folder
# MAGIC
# MAGIC Creates the four simulated workspaces as catalogs (or as schemas, in single-catalog mode) and the `/Shared/model_monitoring` folder that the lane sheet specifies for monitoring assets.
# MAGIC
# MAGIC If `CREATE CATALOG` fails with a permissions or storage-location error, set `USE_SINGLE_CATALOG = True` in `00_config` and re-run from here.

# COMMAND ----------

if not USE_SINGLE_CATALOG:
    for area, cfg in AREAS.items():
        cat = cfg["catalog"]
        try:
            spark.sql(f"CREATE CATALOG IF NOT EXISTS {cat} "
                      f"COMMENT 'UMM demo · simulates workspace {cfg['simulated_workspace']} ({cfg['owner_team']})'")
            print(f"✓ catalog {cat}")
        except Exception as e:
            raise RuntimeError(
                f"Could not create catalog {cat}. Set USE_SINGLE_CATALOG = True in 00_config "
                f"and point SINGLE_CATALOG at a catalog you own.\n\n{e}")
else:
    # Use the existing catalog; no CREATE CATALOG privilege needed
    try:
        spark.sql(f"DESCRIBE CATALOG {SINGLE_CATALOG}").collect()
    except Exception as e:
        raise RuntimeError(f"Catalog {SINGLE_CATALOG} not found or not accessible. Check the name and your USE CATALOG grant.\n\n{e}")
    print(f"Single-catalog mode: using {SINGLE_CATALOG}")

for area, cfg in AREAS.items():
    for schema in cfg["schemas"]:
        name = f"{catalog_for(area)}.{schema_for(area, schema)}"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {name} "
                  f"COMMENT 'UMM demo · {area} · {schema}'")
        print(f"✓ schema {name}")

# COMMAND ----------

# Shared folder for monitoring dashboards and assets (lane sheet: "Assets directory")
try:
    from databricks.sdk import WorkspaceClient
    WorkspaceClient().workspace.mkdirs("/Shared/model_monitoring")
    print("✓ /Shared/model_monitoring")
except Exception as e:
    print(f"Create /Shared/model_monitoring manually in the Workspace browser ({e})")

# COMMAND ----------

# MAGIC %md
# MAGIC # UMM Demo · 02 · Reference tables (hub)
# MAGIC
# MAGIC Writes the hub's reference layer to `umm_demo_hub.ref`:
# MAGIC
# MAGIC | Table | Purpose |
# MAGIC |---|---|
# MAGIC | `dim_model` | Model inventory: owner, bucket, lane, risk tier, monitored table, profile settings |
# MAGIC | `dim_metric` | Metric dictionary from the UMM catalogue, one row per bucket + contract name |
# MAGIC | `map_model_metric` | Which metric applies to which model, how it's produced, and its amber/red thresholds |
# MAGIC | `scenario_calendar` | Presenter reference for the scripted demo moments. **Exclude from the Genie space.** |
# MAGIC | `ref_llm_pricing` | Illustrative token prices for cost metrics. Replace with contracted rates. |
# MAGIC
# MAGIC Thresholds are starting values. We'll tune the PD and forecast ones in Phase 2 once the trained models' baselines are known.

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_model

# COMMAND ----------

def area_of(model_id):
    return {PD_MODEL: "credit", FCST_MODEL: "markets", RAG_APP: "ai_factory",
            AGENT_APP: "ai_factory", LLM_ENDPOINT: "ai_factory"}[model_id]

models = [
    dict(model_id=PD_MODEL, model_name="Counterparty Default PD", asset_type="model",
         bucket="supervised_ml", problem_type="classification", lane="A", lane_name="Real-time classical",
         profile_type="InferenceLog", risk_tier="Tier 1",
         use_case="Probability of default for CIB counterparties at credit application and limit review",
         serving_mode="real_time", endpoint_name="umm-demo-pd-endpoint",
         monitored_table=fq("credit", "monitoring", "pd_inference_log"),
         baseline_table=fq("credit", "ml", "pd_baseline"),
         timestamp_col="request_time", model_id_col="model_version",
         prediction_col="prediction", label_col="label", slicing_exprs="country|product",
         granularity="1 day", current_version="2", go_live_date="2025-03-17"),
    dict(model_id=FCST_MODEL, model_name="Trade Volume Forecast", asset_type="model",
         bucket="supervised_ml", problem_type="regression", lane="B", lane_name="Batch classical",
         profile_type="InferenceLog", risk_tier="Tier 2",
         use_case="Next-day trade volume by country and product for liquidity and capacity planning",
         serving_mode="batch_nightly", endpoint_name="",
         monitored_table=fq("markets", "monitoring", "trade_volume_scoring_log"),
         baseline_table=fq("markets", "ml", "trade_volume_baseline"),
         timestamp_col="scoring_time", model_id_col="model_version",
         prediction_col="prediction", label_col="label", slicing_exprs="country|product",
         granularity="1 day", current_version="3", go_live_date="2024-11-04"),
    dict(model_id=RAG_APP, model_name="Credit Policy Assistant (RAG)", asset_type="model",
         bucket="generative_ai", problem_type="rag", lane="C", lane_name="RAG / agent traces",
         profile_type="TimeSeries", risk_tier="Tier 2",
         use_case="Answers RM and credit officer questions from CIB credit, limits, KYC, sanctions and collateral policy",
         serving_mode="real_time", endpoint_name="umm-demo-rag-endpoint",
         monitored_table=fq("ai_factory", "monitoring", "rag_request_metrics"),
         baseline_table="", timestamp_col="request_time", model_id_col="",
         prediction_col="", label_col="", slicing_exprs="app_name|query_category",
         granularity="1 day", current_version="1.4.2", go_live_date="2025-08-11"),
    dict(model_id=AGENT_APP, model_name="RM Client Brief Agent", asset_type="model",
         bucket="ai_agents", problem_type="agent", lane="C", lane_name="RAG / agent traces",
         profile_type="TimeSeries", risk_tier="Tier 1",
         use_case="Builds client briefs for RMs from profile, exposure and news tools; can send briefs with approval",
         serving_mode="real_time", endpoint_name="umm-demo-agent-endpoint",
         monitored_table=fq("ai_factory", "monitoring", "agent_request_metrics"),
         baseline_table="", timestamp_col="request_time", model_id_col="",
         prediction_col="", label_col="", slicing_exprs="app_name|intent",
         granularity="1 day", current_version="2.1.1", go_live_date="2025-10-06"),
    dict(model_id=LLM_ENDPOINT, model_name="Shared LLM Endpoint", asset_type="llm_endpoint",
         bucket="llm_service", problem_type="llm_service", lane="D", lane_name="LLM service",
         profile_type="TimeSeries", risk_tier="n/a",
         use_case="Foundation model endpoint behind the RAG assistant and the client brief agent (AI Gateway)",
         serving_mode="real_time", endpoint_name=LLM_ENDPOINT_NAME,
         monitored_table=fq("ai_factory", "monitoring", "llm_usage_log"),
         baseline_table="", timestamp_col="event_time", model_id_col="",
         prediction_col="", label_col="", slicing_exprs="request_tags['app']",
         granularity="1 day", current_version="", go_live_date="2025-08-11"),
]
for m in models:
    a = area_of(m["model_id"])
    m.update(source_area=a, source_catalog=catalog_for(a),
             source_workspace=AREAS[a]["simulated_workspace"],
             owner_team=AREAS[a]["owner_team"], owner_mailbox=AREAS[a]["owner_mailbox"],
             refresh_schedule="Daily 02:00 UTC", notify_on="failure",
             assets_directory="/Shared/model_monitoring", status="active")

dim_model = pd.DataFrame(models)
write_table(dim_model, "hub", "ref", "dim_model", array_cols=["slicing_exprs"], date_cols=["go_live_date"],
            comment="UMM model inventory. One row per monitored model or LLM endpoint.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric specs → dim_metric and map_model_metric
# MAGIC
# MAGIC `threshold_type`
# MAGIC * `absolute`: compare the metric value with `amber_threshold` / `red_threshold`
# MAGIC * `relative_to_baseline`: compare value ÷ baseline value (baseline = first 30 days of history, or the training baseline for drift)
# MAGIC
# MAGIC `direction`: `higher_better`, `lower_better`, or `deviation` (absolute relative change from baseline in either direction).

# COMMAND ----------

BUCKET = {PD_MODEL: "supervised_ml", FCST_MODEL: "supervised_ml", RAG_APP: "generative_ai",
          AGENT_APP: "ai_agents", LLM_ENDPOINT: "llm_service"}

# (model_id, contract_name, display_name, category, how_we_get_it, implementation,
#  direction, threshold_type, amber, red, unit, scope)
# scope: "in_scope" | "descoped" | "supporting" (ops metric outside the contract catalogue)
S = []
def m(*row):
    S.append(row)

LBL = "Built in (needs labels)"
# --- 1. PD · supervised_ml · classification · Lane A ---
m(PD_MODEL, "accuracy", "Accuracy", "Performance", LBL, "InferenceLog built-in classification metric", "higher_better", "absolute", 0.88, 0.85, "ratio", "in_scope")
m(PD_MODEL, "precision", "Precision", "Performance", LBL, "InferenceLog built-in, positive class = 1", "higher_better", "absolute", 0.60, 0.50, "ratio", "in_scope")
m(PD_MODEL, "recall", "Recall", "Performance", LBL, "InferenceLog built-in, positive class = 1", "higher_better", "absolute", 0.55, 0.45, "ratio", "in_scope")
m(PD_MODEL, "f1_macro", "F1", "Performance", LBL, "InferenceLog built-in F1, macro average", "higher_better", "absolute", 0.70, 0.65, "ratio", "in_scope")
m(PD_MODEL, "data_drift_js", "Data drift (JS)", "Performance", "Built in (drift metrics table)", "Built-in drift vs training baseline; max Jensen-Shannon distance across features", "lower_better", "absolute", 0.10, 0.20, "distance", "in_scope")
m(PD_MODEL, "data_drift_psi", "Data drift (PSI)", "Performance", "Built in (drift metrics table)", "Built-in drift vs training baseline; max PSI across numeric features", "lower_better", "absolute", 0.10, 0.25, "index", "in_scope")
m(PD_MODEL, "concept_drift_score", "Concept drift", "Performance", "Custom SQL metric (custom drift metric, needs labels)", "Custom drift metric: change in |observed default rate - mean predicted PD| vs baseline", "lower_better", "absolute", 0.03, 0.06, "ratio", "in_scope")
m(PD_MODEL, "label_drift_js", "Label drift", "Performance", "Built in (drift on the label column)", "Built-in JS distance on label column vs baseline", "lower_better", "absolute", 0.05, 0.10, "distance", "in_scope")
m(PD_MODEL, "model_quality_drift", "Model drift", "Performance", "Custom SQL metric (custom drift on accuracy, precision, F1)", "Custom drift metric: F1 drop vs previous window", "lower_better", "absolute", 0.05, 0.10, "ratio", "in_scope")
m(PD_MODEL, "approval_rate_gap", "Fairness: approval rate gap", "Risk and Safety", "Custom SQL metric (compares slices)", "Approval rate (prediction = 0) per country slice; hub computes max - min", "lower_better", "absolute", 0.10, 0.20, "ratio", "in_scope")
m(PD_MODEL, "quality_gap", "Fairness: quality gap", "Risk and Safety", "Custom SQL metric (compares slices)", "F1 per country slice; hub computes max - min", "lower_better", "absolute", 0.10, 0.20, "ratio", "in_scope")
m(PD_MODEL, "positive_rate_at_threshold", "Threshold calibration", "Risk and Safety", "Custom SQL metric", "Custom aggregate: share of requests with pd_probability >= 0.5, vs baseline", "deviation", "relative_to_baseline", 0.30, 0.60, "ratio", "in_scope")

# --- 2. Trade volume forecast · supervised_ml · regression · Lane B ---
m(FCST_MODEL, "mae", "Mean Absolute Error", "Performance", "Built in (regression, needs labels)", "InferenceLog built-in regression metric", "lower_better", "relative_to_baseline", 1.25, 1.50, "usd_m", "in_scope")
m(FCST_MODEL, "rmse", "Root Mean Squared Error", "Performance", "Built in (regression, needs labels)", "InferenceLog built-in regression metric", "lower_better", "relative_to_baseline", 1.25, 1.50, "usd_m", "in_scope")
m(FCST_MODEL, "data_drift_js", "Data drift (JS)", "Performance", "Built in (drift metrics table)", "Built-in drift vs training baseline; max JS across features", "lower_better", "absolute", 0.10, 0.20, "distance", "in_scope")
m(FCST_MODEL, "data_drift_psi", "Data drift (PSI)", "Performance", "Built in (drift metrics table)", "Built-in drift vs training baseline; max PSI across numeric features", "lower_better", "absolute", 0.10, 0.25, "index", "in_scope")
m(FCST_MODEL, "label_drift_js", "Label drift", "Performance", "Built in (drift on the label column)", "Built-in JS distance on actual volume vs baseline", "lower_better", "absolute", 0.10, 0.20, "distance", "in_scope")
m(FCST_MODEL, "concept_drift_score", "Concept drift", "Performance", "Custom SQL metric (custom drift metric, needs labels)", "Custom drift metric: JS distance of residual distribution vs baseline", "lower_better", "absolute", 0.10, 0.20, "distance", "in_scope")
m(FCST_MODEL, "model_quality_drift", "Model drift", "Performance", "Custom SQL metric (custom drift on accuracy, precision, F1)", "Custom drift metric: relative MAE increase vs previous window", "lower_better", "absolute", 0.20, 0.40, "ratio", "in_scope")
m(FCST_MODEL, "quality_gap", "Fairness: quality gap", "Risk and Safety", "Custom SQL metric (compares slices)", "Normalised MAE (MAE / mean actual) per country slice; hub computes max - min", "lower_better", "absolute", 0.10, 0.20, "ratio", "in_scope")

# --- 4. Credit policy RAG · generative_ai · Lane C ---
J = "LLM judge or scorer on traces"
m(RAG_APP, "query_intent_alignment", "Query intent alignment", "Performance", J, "Custom guidelines judge -> per-request 0/1 -> TimeSeries custom aggregate avg", "higher_better", "absolute", 0.85, 0.75, "ratio", "in_scope")
m(RAG_APP, "context_relevance", "Context relevance", "Performance", J, "Retrieval relevance scorer -> per-request 0/1 -> avg", "higher_better", "absolute", 0.80, 0.70, "ratio", "in_scope")
m(RAG_APP, "groundedness", "Groundedness", "Performance", J, "Retrieval groundedness scorer -> per-request 0/1 -> avg", "higher_better", "absolute", 0.85, 0.75, "ratio", "in_scope")
m(RAG_APP, "response_relevance", "Response relevance", "Performance", J, "Relevance-to-query scorer -> per-request 0/1 -> avg", "higher_better", "absolute", 0.85, 0.75, "ratio", "in_scope")
m(RAG_APP, "precision_at_k", "Precision@K", "Performance", "Custom SQL metric (needs relevance labels)", "avg(size(array_intersect(retrieved_doc_ids, expected_doc_ids)) / k), k = 5", "higher_better", "absolute", 0.28, 0.22, "ratio", "in_scope")
m(RAG_APP, "recall_at_k", "Recall@K", "Performance", "Custom SQL metric (needs relevance labels)", "avg(size(array_intersect(retrieved_doc_ids, expected_doc_ids)) / size(expected_doc_ids))", "higher_better", "absolute", 0.75, 0.60, "ratio", "in_scope")
m(RAG_APP, "user_satisfaction_score", "End user satisfaction", "Performance", "Human input or user feedback (ratings logged with traces)", "Share of thumbs-up among rated requests", "higher_better", "absolute", 0.75, 0.65, "ratio", "in_scope")
m(RAG_APP, "safety_violation_rate", "Safety violation rate", "Risk and Safety", "LLM judge or scorer on traces (or guardrail flags)", "Safety scorer -> per-request 0/1 -> avg", "lower_better", "absolute", 0.01, 0.03, "ratio", "in_scope")
m(RAG_APP, "avg_tokens_per_request", "Average tokens per request", "Optimisation", "System tables or traces", "avg(total_tokens) per request", "lower_better", "relative_to_baseline", 1.20, 1.50, "tokens", "in_scope")
m(RAG_APP, "cost_per_successful_query", "Cost per successful query", "Optimisation", "System tables or traces (billing usage plus success criteria)", "Hub: Lane D token cost / requests passing groundedness and response_relevance", "lower_better", "relative_to_baseline", 1.20, 1.50, "usd", "in_scope")
DESC = "Descoped for demo: requires a semantic cache in front of the RAG chain"
m(RAG_APP, "cached_answer_quality_pass_rate", "Query caching", "Optimisation", "Custom SQL metric", DESC, "higher_better", "absolute", None, None, "ratio", "descoped")
m(RAG_APP, "cache_hit_rate", "Cache hit rate", "Optimisation", "Custom SQL metric", DESC, "higher_better", "absolute", None, None, "ratio", "descoped")
m(RAG_APP, "token_cost_savings", "Token and cost savings", "Optimisation", "System tables or traces", DESC, "higher_better", "absolute", None, None, "usd", "descoped")

# --- 5. RM client brief agent · ai_agents · Lane C ---
SP = "Custom SQL metric (from trace steps)"
m(AGENT_APP, "task_success_rate", "Task success / instruction adherence", "Performance", J, "Task success judge -> per-request 0/1 -> avg", "higher_better", "absolute", 0.85, 0.75, "ratio", "in_scope")
m(AGENT_APP, "tool_accuracy", "Tool accuracy", "Performance", SP, "Called tools = expected tools and no unrecovered tool error -> avg", "higher_better", "absolute", 0.90, 0.80, "ratio", "in_scope")
m(AGENT_APP, "plan_quality", "Plan quality", "Performance", J, "Plan quality judge -> per-request 0/1 -> avg", "higher_better", "absolute", 0.85, 0.75, "ratio", "in_scope")
m(AGENT_APP, "decision_path_validity", "Decision-path validity", "Performance", J, "Decision path judge over the full trace -> avg", "higher_better", "absolute", 0.85, 0.75, "ratio", "in_scope")
m(AGENT_APP, "user_feedback_score", "User feedback", "Performance", "Human input or user feedback", "Share of thumbs-up among rated requests", "higher_better", "absolute", 0.75, 0.65, "ratio", "in_scope")
m(AGENT_APP, "permission_compliance_rate", "Permission compliance", "Risk and Safety", SP, "1 - share of requests calling a tool the user's role isn't entitled to", "higher_better", "absolute", 0.99, 0.97, "ratio", "in_scope")
m(AGENT_APP, "safety_violation_rate", "Safety violation rate", "Risk and Safety", J, "Safety scorer -> per-request 0/1 -> avg", "lower_better", "absolute", 0.01, 0.03, "ratio", "in_scope")
m(AGENT_APP, "trace_completeness", "Trace completeness", "Risk and Safety", "Custom SQL metric (against required steps)", "Required step sequence present for the intent -> avg", "higher_better", "absolute", 0.95, 0.90, "ratio", "in_scope")
m(AGENT_APP, "approval_adherence_rate", "Approval adherence", "Risk and Safety", "Custom SQL metric", "Approved send_brief calls / all send_brief calls", "higher_better", "absolute", 0.98, 0.95, "ratio", "in_scope")
m(AGENT_APP, "escalation_appropriateness", "Escalation appropriateness", "Risk and Safety", J, "Escalation judge, scored where escalation was expected or happened -> avg", "higher_better", "absolute", 0.85, 0.75, "ratio", "in_scope")
m(AGENT_APP, "recovery_success_rate", "Recovery success", "Risk and Safety", "Custom SQL metric", "Recovered requests / requests with a tool error", "higher_better", "absolute", 0.80, 0.65, "ratio", "in_scope")
m(AGENT_APP, "cost_per_request", "Cost", "Optimisation", "System tables or traces", "Hub: Lane D token cost / agent requests", "lower_better", "relative_to_baseline", 1.20, 1.50, "usd", "in_scope")
m(AGENT_APP, "avg_tokens_per_request", "Avg tokens per request", "Optimisation", "System tables or traces", "avg(total_tokens) per request", "lower_better", "relative_to_baseline", 1.20, 1.50, "tokens", "in_scope")

# --- Lane D · shared LLM endpoint (supporting ops metrics, outside the contract catalogue) ---
m(LLM_ENDPOINT, "error_rate", "Error rate", "Operations", "AI Gateway usage / inference table", "Share of calls with status_code >= 400, sliced by request_tags['app']", "lower_better", "absolute", 0.01, 0.03, "ratio", "supporting")
m(LLM_ENDPOINT, "p95_latency_ms", "p95 latency", "Operations", "AI Gateway usage / inference table", "percentile(latency_ms, 0.95)", "lower_better", "relative_to_baseline", 1.30, 1.60, "ms", "supporting")
m(LLM_ENDPOINT, "tokens_per_request", "Tokens per call", "Operations", "AI Gateway usage / inference table", "avg(total_tokens)", "lower_better", "relative_to_baseline", 1.20, 1.50, "tokens", "supporting")

cols = ["model_id", "contract_name", "display_name", "category", "how_we_get_it", "implementation",
        "direction", "threshold_type", "amber_threshold", "red_threshold", "unit", "scope"]
spec = pd.DataFrame(S, columns=cols)
spec["bucket"] = spec["model_id"].map(BUCKET)
spec["metric_key"] = spec["bucket"] + "." + spec["contract_name"]
spec["amber_threshold"] = spec["amber_threshold"].astype(float)
spec["red_threshold"] = spec["red_threshold"].astype(float)

dim_metric = (spec.drop_duplicates("metric_key")
              [["metric_key", "bucket", "contract_name", "display_name", "category", "how_we_get_it", "unit"]]
              .assign(in_contract_catalogue=lambda x: x["bucket"] != "llm_service"))
map_model_metric = spec[["model_id", "metric_key", "contract_name", "bucket", "scope", "implementation",
                         "direction", "threshold_type", "amber_threshold", "red_threshold", "unit"]]

write_table(dim_metric, "hub", "ref", "dim_metric",
            comment="UMM metric dictionary. One row per bucket + contract name, from the CIB metric catalogue.")
write_table(map_model_metric, "hub", "ref", "map_model_metric", nullable_cols=["amber_threshold", "red_threshold"],
            comment="Metric applicability, implementation and thresholds per model. scope = in_scope | descoped | supporting.")

print(spec.groupby(["model_id", "scope"]).size())

# COMMAND ----------

# MAGIC %md
# MAGIC ## scenario_calendar and ref_llm_pricing

# COMMAND ----------

cal = pd.DataFrame(SCENARIOS)
cal["start_date"] = cal["start_day"].map(lambda d: day_date(d).isoformat())
cal["end_date"] = cal["end_day"].map(lambda d: day_date(d).isoformat())
write_table(cal, "hub", "ref", "scenario_calendar", date_cols=["start_date", "end_date"],
            comment="PRESENTER REFERENCE ONLY. Scripted demo scenarios. Do not add to the Genie space.")

pricing = pd.DataFrame([dict(endpoint_name=LLM_ENDPOINT_NAME, input_usd_per_1m_tokens=1.00,
                             output_usd_per_1m_tokens=3.00, effective_from=HISTORY_START.isoformat(),
                             note="Illustrative demo pricing. Replace with contracted rates.")])
write_table(pricing, "hub", "ref", "ref_llm_pricing", date_cols=["effective_from"],
            comment="Token pricing used by hub cost metrics. Illustrative values for the demo.")

# COMMAND ----------

# MAGIC %md
# MAGIC # UMM Demo · 03 · PD model data (credit workspace)
# MAGIC
# MAGIC Generates synthetic CIB counterparty data for the PD model:
# MAGIC
# MAGIC | Table | Contents |
# MAGIC |---|---|
# MAGIC | `raw.pd_training` | ~24k labelled applications from the 12 months before the history window (v1 training data and drift baseline) |
# MAGIC | `raw.pd_scoring_requests` | ~90 days of scoring requests (features only, as the serving endpoint would see them) |
# MAGIC | `raw.pd_outcomes` | Default outcomes, arriving `LABEL_DELAY_DAYS` after scoring, so the latest week has no labels yet |
# MAGIC
# MAGIC Phase 2 trains v1 and v2, scores this history, and builds `monitoring.pd_inference_log`.
# MAGIC
# MAGIC **Scenarios:** S1 (leverage drift from day 60) and S2 (India default rise from day 66).

# COMMAND ----------

rng = np.random.default_rng(SEED + 1)

COUNTRIES = ["SG", "HK", "IN", "AE", "GB", "US", "CN", "KR"]
COUNTRY_P = [0.18, 0.17, 0.15, 0.10, 0.12, 0.12, 0.09, 0.07]
COUNTRY_EFFECT = {"SG": -0.30, "HK": -0.10, "IN": 0.25, "AE": 0.05, "GB": -0.20, "US": -0.15, "CN": 0.20, "KR": 0.0}
PRODUCTS = ["Term Loan", "Revolving Credit", "Trade Finance", "Project Finance"]
PRODUCT_P = [0.35, 0.30, 0.25, 0.10]
PRODUCT_EFFECT = {"Term Loan": 0.0, "Revolving Credit": 0.15, "Trade Finance": -0.25, "Project Finance": 0.35}
SECTORS = ["Manufacturing", "Commodities", "Technology", "Real Estate", "Financial Institutions",
           "Infrastructure", "Consumer"]
SECTOR_P = [0.20, 0.16, 0.14, 0.12, 0.14, 0.12, 0.12]
SECTOR_EFFECT = {"Manufacturing": 0.0, "Commodities": 0.30, "Technology": -0.10, "Real Estate": 0.35,
                 "Financial Institutions": -0.25, "Infrastructure": 0.05, "Consumer": 0.0}

IN_DEFAULT_SHIFT = 1.1   # extra log-odds for IN during S2: not visible in any feature


def make_features(n, rng, leverage_drift=0.0):
    """Counterparty features. A hidden latent risk factor drives all of them so they're correlated.
    leverage_drift (0..1) raises debt_to_ebitda ~35% and utilisation_pct ~15 points (scenario S1)."""
    latent = rng.normal(0, 1, n)
    df = pd.DataFrame({
        "country": rng.choice(COUNTRIES, n, p=COUNTRY_P),
        "product": rng.choice(PRODUCTS, n, p=PRODUCT_P),
        "sector": rng.choice(SECTORS, n, p=SECTOR_P),
    })
    df["annual_revenue_usd_m"] = np.round(rng.lognormal(5.5, 1.0, n), 2)
    df["debt_to_ebitda"] = np.round(np.clip(rng.gamma(4.0, 0.75, n) * np.exp(0.25 * latent), 0.2, 15)
                                    * (1 + 0.35 * leverage_drift), 3)
    df["interest_coverage"] = np.round(np.clip(rng.lognormal(1.4 - 0.25 * latent, 0.45, n), 0.3, 30), 3)
    df["utilisation_pct"] = np.round(np.clip(rng.beta(2.5, 3.0, n) * 100 + 8 * latent + 15 * leverage_drift, 0, 100), 2)
    dpd_flag = rng.random(n) < sigmoid(-2.5 + latent)
    df["days_past_due_max_12m"] = np.where(dpd_flag, rng.choice([15, 30, 60, 90], n, p=[0.45, 0.30, 0.15, 0.10]), 0)
    df["internal_rating"] = np.clip(np.round(5 + 1.5 * latent + rng.normal(0, 0.8, n)), 1, 10).astype(int)
    df["years_relationship"] = rng.integers(1, 30, n)
    df["collateral_coverage"] = np.round(np.clip(rng.normal(1.1, 0.35, n), 0, 3), 3)
    df["requested_limit_usd_m"] = np.round(df["annual_revenue_usd_m"] * rng.uniform(0.05, 0.30, n), 2)
    df["counterparty_id"] = ["CP" + str(x).zfill(6) for x in rng.integers(1, 8000, n)]
    df["_latent"] = latent
    return df


def default_probability(df, country_shift=None):
    z = (-1.95
         + 0.30 * (df["debt_to_ebitda"] - 3.0)
         - 0.55 * np.log(df["interest_coverage"])
         + 0.020 * (df["utilisation_pct"] - 45)
         + 0.012 * df["days_past_due_max_12m"]
         + 0.22 * (df["internal_rating"] - 5)
         - 0.45 * (df["collateral_coverage"] - 1.0)
         + df["country"].map(COUNTRY_EFFECT)
         + df["product"].map(PRODUCT_EFFECT)
         + df["sector"].map(SECTOR_EFFECT)
         + 0.30 * df["_latent"])                     # unobserved risk the model can't fully see
    if country_shift is not None:
        z = z + country_shift
    return sigmoid(z)


FEATURE_COLS = ["counterparty_id", "country", "product", "sector", "annual_revenue_usd_m", "debt_to_ebitda",
                "interest_coverage", "utilisation_pct", "days_past_due_max_12m", "internal_rating",
                "years_relationship", "collateral_coverage", "requested_limit_usd_m"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Training set (12 months before the history window)

# COMMAND ----------

N_TRAIN = 24000
train = make_features(N_TRAIN, rng)
train_start = pd.Timestamp(HISTORY_START) - pd.Timedelta(days=365)
train["request_time"] = (train_start + pd.to_timedelta(rng.integers(0, 365, N_TRAIN), unit="D")
                         + pd.to_timedelta(request_seconds(rng, N_TRAIN), unit="s"))
train["default_flag"] = (rng.random(N_TRAIN) < default_probability(train)).astype(int)
train["request_id"] = [f"PDT-{i:06d}" for i in range(N_TRAIN)]
pd_training = train[["request_id", "request_time"] + FEATURE_COLS + ["default_flag"]].sort_values("request_time")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 90 days of scoring requests and delayed outcomes

# COMMAND ----------

s1, s2 = scenario("S1_PD_LEVERAGE_DRIFT"), scenario("S2_PD_IN_DEFAULTS")
req_parts, out_parts = [], []
for d, day in enumerate(DAYS):
    n = max(20, int(rng.normal(420, 35))) if day.weekday() < 5 else max(10, int(rng.normal(70, 12)))
    drift = 0.0 if d < s1["start_day"] else min(1.0, (d - s1["start_day"] + 1) / 10)   # 10-day ramp
    f = make_features(n, rng, leverage_drift=drift)
    f["request_time"] = day + pd.to_timedelta(request_seconds(rng, n), unit="s")
    f["request_id"] = [f"PDR-{d:03d}-{i:05d}" for i in range(n)]
    shift = np.where((f["country"] == "IN") & (d >= s2["start_day"]), IN_DEFAULT_SHIFT, 0.0)
    f["default_flag"] = (rng.random(n) < default_probability(f, shift)).astype(int)
    f["outcome_time"] = (f["request_time"] + pd.Timedelta(days=LABEL_DELAY_DAYS)
                         + pd.to_timedelta(rng.integers(0, 12 * 3600, n), unit="s"))
    req_parts.append(f[["request_id", "request_time"] + FEATURE_COLS])
    out_parts.append(f[["request_id", "outcome_time", "default_flag"]])

pd_scoring_requests = pd.concat(req_parts, ignore_index=True)
outcomes_all = pd.concat(out_parts, ignore_index=True)
cutoff = pd.Timestamp(HISTORY_END) + pd.Timedelta(days=1)          # end of the last history day
pd_outcomes = outcomes_all[outcomes_all["outcome_time"] < cutoff].reset_index(drop=True)

print(f"Training default rate: {pd_training['default_flag'].mean():.3f}")
print(f"Scoring requests: {len(pd_scoring_requests):,} · outcomes available: {len(pd_outcomes):,}")

# COMMAND ----------

write_table(pd_training, "credit", "raw", "pd_training",
            comment="Synthetic CIB counterparty applications with default outcome. 12 months before the demo window. PD v1 training data.")
write_table(pd_scoring_requests, "credit", "raw", "pd_scoring_requests",
            comment="Synthetic PD scoring requests for the 90-day demo window (features only). Contains scripted drift scenarios S1, S2.")
write_table(pd_outcomes, "credit", "raw", "pd_outcomes",
            comment=f"Default outcomes for PD scoring requests, arriving {LABEL_DELAY_DAYS} days after scoring (demo proxy for the 12-month default horizon).")

# COMMAND ----------

# MAGIC %md
# MAGIC # UMM Demo · 04 · Trade volume data (markets workspace)
# MAGIC
# MAGIC Generates daily trade volumes for 6 countries × 5 products, covering 2 years before the history window (training) plus the 90-day history window (scored nightly in Phase 2).
# MAGIC
# MAGIC | Table | Contents |
# MAGIC |---|---|
# MAGIC | `raw.trade_volume_daily` | One row per trade_date × country × product, with actual volume and drivers. `dataset_split` = `train` or `scoring_history` |
# MAGIC
# MAGIC **Scenario:** S3, an FX volatility shock on days 72–80. Volumes jump beyond what the volatility driver explains, so the model's residuals drift.

# COMMAND ----------

rng = np.random.default_rng(SEED + 2)

COUNTRY_BASE = {"SG": 420, "HK": 510, "IN": 260, "AE": 180, "GB": 640, "US": 580}   # USD m per weekday
COUNTRY_CLIENTS = {"SG": 900, "HK": 1100, "IN": 650, "AE": 400, "GB": 1300, "US": 1200}
PRODUCT_SCALE = {"FX Spot": 1.00, "FX Forward": 0.55, "FX Swap": 0.80, "Rates Swap": 0.35, "Trade Finance": 0.12}
FX_PRODUCTS = {"FX Spot", "FX Forward", "FX Swap"}
WEEKDAY_FACTOR = np.array([1.00, 1.02, 1.03, 1.02, 1.05, 0.08, 0.05])   # Mon..Sun

train_start = pd.Timestamp(HISTORY_START) - pd.Timedelta(days=730)
dates = pd.date_range(train_start, pd.Timestamp(HISTORY_END), freq="D")
T = len(dates)
hist_offset = (pd.Timestamp(HISTORY_START) - train_start).days   # index of history day 0

# --- Shared market drivers ---
s3 = scenario("S3_FCST_FX_VOL_SHOCK")
fx_vol = np.empty(T)
fx_vol[0] = 8.0
for t in range(1, T):
    fx_vol[t] = 8.0 + 0.92 * (fx_vol[t - 1] - 8.0) + rng.normal(0, 0.6)
shock = np.zeros(T)
for k in range(s3["end_day"] - s3["start_day"] + 1):
    shock[hist_offset + s3["start_day"] + k] = 11.0 * np.exp(-k / 4.0)
fx_vol = np.clip(fx_vol + shock, 3.0, None)
in_event = shock > 0

rates = 3.8 + np.cumsum(rng.normal(0, 0.02, T))
is_weekend = dates.weekday >= 5
bdays = pd.Series(~is_weekend, index=dates)
month_end = np.zeros(T, dtype=bool)
for _, grp in bdays[bdays].groupby([bdays[bdays].index.year, bdays[bdays].index.month]):
    month_end[dates.get_indexer(grp.index[-2:])] = True        # last 2 business days of each month
is_holiday = np.zeros(T, dtype=bool)
is_holiday[rng.choice(np.where(~is_weekend)[0], size=int(T / 365 * 10), replace=False)] = True

trend = 1 + 0.00035 * np.arange(T)
wd = WEEKDAY_FACTOR[dates.weekday]

rows = []
for c, base in COUNTRY_BASE.items():
    for p, scale in PRODUCT_SCALE.items():
        vol = base * scale * trend * wd * np.where(month_end, 1.25, 1.0) * np.where(is_holiday, 0.30, 1.0)
        if p in FX_PRODUCTS:
            vol = vol * (1 + 0.035 * (fx_vol - 8.0))                       # explainable volatility effect
            vol = vol * np.where(in_event, 1.30 * rng.lognormal(0, 0.20, T), 1.0)   # unexplained S3 surge
        vol = vol * rng.lognormal(0, 0.07, T)
        clients = np.round(COUNTRY_CLIENTS[c] * scale * wd * rng.lognormal(0, 0.05, T)).astype(int)
        rows.append(pd.DataFrame({
            "trade_date": dates, "country": c, "product": p,
            "volume_usd_m": np.round(vol, 3),
            "fx_vol_index": np.round(fx_vol, 3), "rates_level_pct": np.round(rates, 4),
            "active_clients": clients, "is_month_end": month_end, "is_weekend": is_weekend,
            "is_holiday": is_holiday,
        }))

trade_volume_daily = pd.concat(rows, ignore_index=True)
trade_volume_daily["dataset_split"] = np.where(trade_volume_daily["trade_date"] < pd.Timestamp(HISTORY_START),
                                               "train", "scoring_history")
print(trade_volume_daily.groupby("dataset_split").size())

# COMMAND ----------

write_table(trade_volume_daily, "markets", "raw", "trade_volume_daily", date_cols=["trade_date"],
            comment="Synthetic daily trade volume by country and product. 2 years train + 90-day demo window. Contains scenario S3 (FX volatility shock).")

# COMMAND ----------

# MAGIC %md
# MAGIC # UMM Demo · 05 · GenAI history (AI Factory workspace)
# MAGIC
# MAGIC The RAG assistant and the agent are deployed in Phase 3, but the demo needs 90 days of trend from day one. This notebook backfills the **flattened, monitoring-ready** tables in the shape that Phase 3's live trace processing will append to. Every backfilled row carries `record_source = 'synthetic_history'`, and live rows will be `'live'`.
# MAGIC
# MAGIC | Table | Lane | Grain |
# MAGIC |---|---|---|
# MAGIC | `raw.policy_doc_registry` | n/a | One row per policy document version (Phase 3 generates the document text from this) |
# MAGIC | `monitoring.rag_request_metrics` | C | One row per RAG request: judge results, retrieval IDs, feedback, tokens |
# MAGIC | `monitoring.agent_request_metrics` | C | One row per agent request: tools, permissions, approvals, judges, tokens |
# MAGIC | `monitoring.llm_usage_log` | D | One row per LLM call, tagged with `request_tags['app']` |
# MAGIC
# MAGIC Judge columns are per-request 0/1 values, so a TimeSeries profile's custom metric is a plain `avg()`. NULL means "not applicable" (for example, no feedback given, or no approval required).
# MAGIC
# MAGIC **Scenarios:** S4 (safety probe), S5 (stale index), S6 (token creep), S7 (tool timeouts), S8 (bad release), S9 (approval bypass).

# COMMAND ----------

rng = np.random.default_rng(SEED + 3)
NAN = np.nan
K = 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Policy document registry

# COMMAND ----------

DOC_TITLES = {
    "CP": ("policy_lookup", ["Corporate Credit Approval Authority Matrix", "CIB Credit Risk Appetite Statement",
                             "Obligor Risk Rating Methodology", "Annual Credit Review Standard",
                             "Watchlist and Early Alert Procedure", "Credit Exception Handling Standard"]),
    "LIM": ("limit_rules", ["Single Obligor Limit Policy", "Country Risk Limit Framework",
                            "Counterparty Limit Setting and Utilisation Standard", "Sector Concentration Limits",
                            "Intraday Limit Excess Procedure", "Limit Breach Escalation Standard"]),
    "KYC": ("kyc", ["CIB Client Due Diligence Standard", "Enhanced Due Diligence Triggers",
                    "Beneficial Ownership Verification", "Periodic KYC Refresh Procedure",
                    "PEP Screening Procedure", "Client Exit Procedure"]),
    "SAN": ("sanctions", ["Sanctions Screening Standard", "Dual-Use Goods Guidance",
                          "Sanctions Alert Disposition Procedure", "Restricted Jurisdictions Governance",
                          "Vessel and Shipping Screening", "Sanctions Escalation Matrix"]),
    "COL": ("collateral", ["Collateral Eligibility Policy", "Collateral Valuation Standard", "Haircut Methodology",
                           "Guarantee Acceptance Standard", "Collateral Monitoring Procedure",
                           "Netting and Set-off Guidance"]),
    "TF": ("trade_finance", ["Documentary Credit Processing Standard", "Trade-Based Money Laundering Red Flags",
                             "Bank Guarantee Issuance Procedure", "Supply Chain Finance Standard",
                             "Trade Finance Pricing Guidance", "Discrepant Documents Handling"]),
}
s5 = scenario("S5_RAG_STALE_INDEX")
UPDATED_DOC = "LIM-003"
V2_DAY = s5["start_day"]                 # LIM-003 v2 takes effect
REINDEX_DAY = s5["end_day"] + 1          # index catches up

docs = []
for prefix, (cat, titles) in DOC_TITLES.items():
    for i, title in enumerate(titles, start=1):
        doc_id = f"{prefix}-{i:03d}"
        if doc_id == UPDATED_DOC:
            docs.append(dict(doc_key=f"{doc_id}@v1", doc_id=doc_id, version="v1", title=title, query_category=cat,
                             effective_from=(HISTORY_START - dt.timedelta(days=400)).isoformat(),
                             effective_to=day_date(V2_DAY - 1).isoformat(), status="superseded"))
            docs.append(dict(doc_key=f"{doc_id}@v2", doc_id=doc_id, version="v2", title=title, query_category=cat,
                             effective_from=day_date(V2_DAY).isoformat(), effective_to="", status="current"))
        else:
            docs.append(dict(doc_key=f"{doc_id}@v1", doc_id=doc_id, version="v1", title=title, query_category=cat,
                             effective_from=(HISTORY_START - dt.timedelta(days=400)).isoformat(),
                             effective_to="", status="current"))
doc_registry = pd.DataFrame(docs)
DOCS_BY_CAT = {cat: [f"{p}-{i:03d}" for i in range(1, 7)] for p, (cat, _) in DOC_TITLES.items()}
ALL_DOC_IDS = [d for ids in DOCS_BY_CAT.values() for d in ids]


def doc_key_at(doc_id, d):
    """Version of a document in force on day d."""
    if doc_id == UPDATED_DOC:
        return f"{doc_id}@v2" if d >= V2_DAY else f"{doc_id}@v1"
    return f"{doc_id}@v1"


def indexed_key_at(doc_id, d):
    """Version the vector index serves on day d (lags during S5)."""
    if doc_id == UPDATED_DOC:
        return f"{doc_id}@v2" if d >= REINDEX_DAY else f"{doc_id}@v1"
    return f"{doc_id}@v1"

# COMMAND ----------

# MAGIC %md
# MAGIC ## RAG request metrics (Lane C)

# COMMAND ----------

CATS = ["policy_lookup", "limit_rules", "kyc", "sanctions", "collateral", "trade_finance"]
CAT_P = np.array([0.22, 0.18, 0.18, 0.14, 0.14, 0.14])
CAT_P_S5 = np.array([0.17, 0.35, 0.14, 0.11, 0.12, 0.11])
SEGMENTS, SEG_P = ["Relationship Manager", "Credit Officer", "Risk Manager"], [0.5, 0.3, 0.2]

rag_rows = []
for d, day in enumerate(DAYS):
    n = max(20, int(rng.normal(250, 20))) if day.weekday() < 5 else max(30, int(rng.normal(90, 12)))
    s4_on, s5_on = in_scenario("S4_RAG_SAFETY_PROBE", d), in_scenario("S5_RAG_STALE_INDEX", d)
    secs = request_seconds(rng, n)
    cats = rng.choice(CATS, n, p=CAT_P_S5 if s5_on else CAT_P)
    for i in range(n):
        cat = cats[i]
        adversarial = rng.random() < (0.20 if s4_on else 0.008)
        n_exp = rng.choice([1, 2, 3], p=[0.45, 0.40, 0.15])
        exp_ids = list(rng.choice(DOCS_BY_CAT[cat], n_exp, replace=False))
        if s5_on and cat == "limit_rules" and UPDATED_DOC not in exp_ids:
            exp_ids[0] = UPDATED_DOC                     # everyone is asking about the new limits standard
        expected = [doc_key_at(x, d) for x in exp_ids]

        retrieved = []
        for x in exp_ids:
            if rng.random() < 0.90:
                retrieved.append(indexed_key_at(x, d))   # during S5 this returns the superseded v1
        pool = [x for x in DOCS_BY_CAT[cat] if x not in exp_ids] + \
               list(rng.choice([x for x in ALL_DOC_IDS if x not in DOCS_BY_CAT[cat]], 4, replace=False))
        for x in rng.permutation(pool):
            if len(retrieved) >= K:
                break
            key = indexed_key_at(x, d)
            if key not in retrieved:
                retrieved.append(key)
        hits = len(set(retrieved) & set(expected))
        recall = hits / len(expected)

        error = rng.random() < 0.005
        if error:
            grounded = ctx = resp = intent = safety_v = feedback = NAN
        else:
            grounded = float(rng.random() < 0.50 + 0.47 * recall)
            ctx = float(rng.random() < 0.50 + 0.42 * recall)
            resp = float(rng.random() < (0.93 if grounded else 0.80))
            intent = float(rng.random() < (0.50 if adversarial else 0.92))
            safety_v = float(adversarial and rng.random() < (0.30 if s4_on else 0.15))
            if rng.random() < 0.40:
                p_up = 0.20 if safety_v else (0.92 if grounded else 0.50)
                feedback = float(rng.random() < p_up)
            else:
                feedback = NAN
        n_calls = 2 if rng.random() < 0.5 else 1            # optional query rewrite + generation
        inp = int(rng.normal(900 + 260 * K, 180))
        out = 0 if error else int(max(60, rng.normal(300, 80)))
        rag_rows.append(dict(
            request_id=f"RAG-{d:03d}-{i:05d}", trace_id=f"tr-rag-{d:03d}{i:05d}",
            request_time=day + pd.Timedelta(seconds=int(secs[i])), app_name=RAG_APP, model_id=RAG_APP,
            app_version="1.4.2", user_segment=rng.choice(SEGMENTS, p=SEG_P), query_category=cat,
            is_adversarial=bool(adversarial), status="ERROR" if error else "OK", k=K,
            expected_doc_ids="|".join(expected), retrieved_doc_ids="|".join(retrieved), retrieval_hits=hits,
            query_intent_alignment=intent, context_relevance=ctx, groundedness=grounded,
            response_relevance=resp, safety_violation=safety_v, user_feedback_positive=feedback,
            input_tokens=inp, output_tokens=out, total_tokens=inp + out, n_llm_calls=n_calls,
            latency_ms=int(rng.lognormal(np.log(2100), 0.35)), record_source="synthetic_history"))

rag = pd.DataFrame(rag_rows)
print(f"RAG requests: {len(rag):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent request metrics (Lane C)

# COMMAND ----------

INTENTS = ["client_brief", "exposure_summary", "limit_check", "out_of_scope", "send_brief_to_client"]
INTENT_P = [0.50, 0.22, 0.10, 0.10, 0.08]
ROLES, ROLE_P = ["RM", "Senior RM", "Analyst"], [0.60, 0.25, 0.15]
EXPECTED = {
    "client_brief": ["get_client_profile", "get_exposure_summary", "search_news", "draft_brief"],
    "exposure_summary": ["get_client_profile", "get_exposure_summary"],
    "limit_check": ["get_client_profile", "get_limit_details"],
    "out_of_scope": [],
    "send_brief_to_client": ["get_client_profile", "get_exposure_summary", "draft_brief", "send_brief"],
}
REQUIRED = {
    "client_brief": {"get_client_profile", "get_exposure_summary", "draft_brief"},
    "exposure_summary": {"get_client_profile", "get_exposure_summary"},
    "limit_check": {"get_client_profile"},
    "out_of_scope": set(),
    "send_brief_to_client": {"get_client_profile", "get_exposure_summary", "draft_brief", "send_brief"},
}
s6 = scenario("S6_AGENT_TOKEN_CREEP")

agent_rows = []
for d, day in enumerate(DAYS):
    n = max(40, int(rng.normal(220, 18))) if day.weekday() < 5 else max(40, int(rng.normal(90, 10)))
    s7_on, s8_on, s9_on = (in_scenario("S7_AGENT_TOOL_TIMEOUTS", d), in_scenario("S8_AGENT_BAD_RELEASE", d),
                           in_scenario("S9_AGENT_APPROVAL_BYPASS", d))
    version = "2.0.3" if d < scenario("S8_AGENT_BAD_RELEASE")["start_day"] else ("2.1.0" if s8_on else "2.1.1")
    creep = 1 + 0.007 * max(0, d - s6["start_day"])
    secs = request_seconds(rng, n, asia_weight=0.75)
    for i in range(n):
        intent = rng.choice(INTENTS, p=INTENT_P)
        role = rng.choice(ROLES, p=ROLE_P)
        entitled = role == "Senior RM"
        expected = list(EXPECTED[intent])
        required = set(REQUIRED[intent])
        if intent == "limit_check" and entitled:
            required.add("get_limit_details")
        if intent == "limit_check" and not entitled:
            expected = ["get_client_profile"]                       # correct path: don't call the limit tool
        escalation_expected = intent == "out_of_scope" or (intent == "limit_check" and not entitled)

        called = list(expected)
        if rng.random() > 0.95:                                     # tool selection slip
            if called and rng.random() < 0.5:
                called.pop(int(rng.integers(len(called))))
            else:
                called.append("search_news")
        violated = False
        if intent == "limit_check" and not entitled and rng.random() < (0.60 if s8_on else 0.005):
            called.append("get_limit_details")
            violated = True

        if escalation_expected and not violated:
            escalated = rng.random() < (0.55 if s8_on else 0.98)
        else:
            escalated = (not violated) and rng.random() < 0.005

        errors, exposure_failed = 0, False
        for tool in called:
            p_fail = 0.45 if (s7_on and tool == "get_exposure_summary") else 0.01
            if rng.random() < p_fail:
                errors += 1
                exposure_failed |= tool == "get_exposure_summary"
        if errors:
            recovered = float(rng.random() < (0.50 if (s7_on and exposure_failed) else 0.97))
        else:
            recovered = NAN
        ok_exec = errors == 0 or recovered == 1.0

        approval_required = "send_brief" in called
        approval_obtained = float(rng.random() < (0.75 if s9_on else 0.999)) if approval_required else NAN

        tool_correct = set(called) == set(expected) and ok_exec
        trace_complete = required.issubset(set(called)) and ok_exec
        task_ok = tool_correct and trace_complete and not violated and (escalated == escalation_expected)
        task_success = float(rng.random() < (0.97 if task_ok else 0.35))
        plan_quality = float(rng.random() < (0.95 if tool_correct else 0.55))
        decision_valid = float(rng.random() < (0.96 if (tool_correct and not violated and escalated == escalation_expected) else 0.45))
        esc_appropriate = (float(escalated == escalation_expected) if (escalation_expected or escalated) else NAN)
        adversarial = rng.random() < 0.003
        safety_v = float(adversarial and rng.random() < 0.15)
        feedback = float(rng.random() < (0.92 if task_success else 0.35)) if rng.random() < 0.40 else NAN

        n_calls = 1 + len(called) + errors
        inp = int(sum(rng.normal(1350, 150, n_calls)) * creep)
        out = int(sum(np.maximum(40, rng.normal(220, 50, n_calls))) * creep)
        latency = int(900 * n_calls + 400 * len(called) + 5000 * errors + rng.normal(0, 300))
        agent_rows.append(dict(
            request_id=f"AGT-{d:03d}-{i:05d}", trace_id=f"tr-agt-{d:03d}{i:05d}",
            request_time=day + pd.Timedelta(seconds=int(secs[i])), app_name=AGENT_APP, model_id=AGENT_APP,
            app_version=version, user_role=role, intent=intent, status="OK" if ok_exec else "ERROR",
            expected_tools="|".join(expected), called_tools="|".join(called), tool_calls_total=len(called),
            tool_errors=errors, recovered=recovered, restricted_tool_called=bool(violated),
            permission_compliant=float(not violated), escalation_expected=bool(escalation_expected),
            escalated=bool(escalated), escalation_appropriate=esc_appropriate,
            approval_required=bool(approval_required), approval_obtained=approval_obtained,
            tool_correct=float(tool_correct), trace_complete=float(trace_complete), task_success=task_success,
            plan_quality=plan_quality, decision_path_validity=decision_valid, is_adversarial=bool(adversarial),
            safety_violation=safety_v, user_feedback_positive=feedback, n_llm_calls=n_calls,
            input_tokens=inp, output_tokens=out, total_tokens=inp + out, latency_ms=max(500, latency),
            record_source="synthetic_history"))

agent = pd.DataFrame(agent_rows)
print(f"Agent requests: {len(agent):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## LLM usage log (Lane D), derived from the same requests so tokens reconcile

# COMMAND ----------

def explode_calls(df, app):
    n = df["n_llm_calls"].to_numpy()
    idx = np.repeat(np.arange(len(df)), n)
    call_no = np.concatenate([np.arange(k) for k in n])
    rep = df.iloc[idx].reset_index(drop=True)
    nn = n[idx]
    # Split request tokens evenly across calls; the last call takes the remainder so totals match exactly
    in_share = rep["input_tokens"].to_numpy() // nn
    out_share = rep["output_tokens"].to_numpy() // nn
    last = call_no == nn - 1
    in_tok = np.where(last, rep["input_tokens"].to_numpy() - in_share * (nn - 1), in_share)
    out_tok = np.where(last, rep["output_tokens"].to_numpy() - out_share * (nn - 1), out_share)
    per_call_latency = rep["latency_ms"].to_numpy() / nn
    status = np.full(len(rep), 200)
    status[rng.random(len(rep)) < 0.003] = 429
    status[rng.random(len(rep)) < 0.002] = 500
    if "status" in rep:
        status = np.where(last & (rep["status"].to_numpy() == "ERROR") & (app == RAG_APP), 500, status)
    return pd.DataFrame({
        "call_id": [f"{r}-c{c}" for r, c in zip(rep["request_id"], call_no)],
        "event_time": rep["request_time"] + pd.to_timedelta(call_no * per_call_latency, unit="ms"),
        "endpoint_name": LLM_ENDPOINT_NAME, "served_entity": "fm-chat-default",
        "_app": app, "client_request_id": rep["request_id"],
        "input_tokens": in_tok.astype(int), "output_tokens": out_tok.astype(int),
        "total_tokens": (in_tok + out_tok).astype(int),
        "latency_ms": np.maximum(80, per_call_latency * rng.lognormal(0, 0.2, len(rep))).astype(int),
        "status_code": status.astype(int), "record_source": "synthetic_history",
    })

llm_usage = pd.concat([explode_calls(rag, RAG_APP), explode_calls(agent, AGENT_APP)], ignore_index=True)
llm_usage = llm_usage.sort_values("event_time").reset_index(drop=True)
print(f"LLM calls: {len(llm_usage):,}")

# COMMAND ----------

write_table(doc_registry, "ai_factory", "raw", "policy_doc_registry", date_cols=["effective_from"],
            comment="CIB policy document registry for the RAG assistant. LIM-003 has a v2 effective on scenario S5 day.")

RAG_NULLABLE = ["query_intent_alignment", "context_relevance", "groundedness", "response_relevance",
                "safety_violation", "user_feedback_positive"]
write_table(rag, "ai_factory", "monitoring", "rag_request_metrics",
            array_cols=["expected_doc_ids", "retrieved_doc_ids"], nullable_cols=RAG_NULLABLE, cdf=True,
            comment="Lane C. One row per RAG request with judge results (0/1), retrieval IDs, feedback and tokens. record_source = synthetic_history | live.")

AGENT_NULLABLE = ["recovered", "escalation_appropriate", "approval_obtained", "user_feedback_positive"]
write_table(agent, "ai_factory", "monitoring", "agent_request_metrics",
            array_cols=["expected_tools", "called_tools"], nullable_cols=AGENT_NULLABLE, cdf=True,
            comment="Lane C. One row per agent request with tool, permission, approval and judge outcomes. record_source = synthetic_history | live.")

write_table(llm_usage, "ai_factory", "monitoring", "llm_usage_log", map_cols={"request_tags": ("app", "_app")},
            cdf=True,
            comment="Lane D. One row per call to the shared LLM endpoint, tagged with request_tags['app']. record_source = synthetic_history | live.")

# COMMAND ----------

# MAGIC %md
# MAGIC # UMM Demo · 06 · Validate Phase 1
# MAGIC
# MAGIC Checks that every table exists, covers the full history window, and that each scripted scenario is visible in the raw data. The notebook fails if any check fails, so it can sit at the end of a job.

# COMMAND ----------

results = []


def check(name, sql, test, expect):
    """Run a scalar SQL query and record pass/fail."""
    value = spark.sql(sql).collect()[0][0]
    ok = value is not None and bool(test(value))
    results.append((name, "PASS" if ok else "FAIL", str(round(value, 4) if isinstance(value, float) else value), expect))


def day_idx(col):
    return f"datediff(to_date({col}), DATE'{HISTORY_START}')"


T = {
    "pd_training": fq("credit", "raw", "pd_training"),
    "pd_req": fq("credit", "raw", "pd_scoring_requests"),
    "pd_out": fq("credit", "raw", "pd_outcomes"),
    "tv": fq("markets", "raw", "trade_volume_daily"),
    "docs": fq("ai_factory", "raw", "policy_doc_registry"),
    "rag": fq("ai_factory", "monitoring", "rag_request_metrics"),
    "agent": fq("ai_factory", "monitoring", "agent_request_metrics"),
    "llm": fq("ai_factory", "monitoring", "llm_usage_log"),
    "dim_model": fq("hub", "ref", "dim_model"),
    "dim_metric": fq("hub", "ref", "dim_metric"),
    "map": fq("hub", "ref", "map_model_metric"),
    "cal": fq("hub", "ref", "scenario_calendar"),
    "price": fq("hub", "ref", "ref_llm_pricing"),
}

# COMMAND ----------

# --- Existence and coverage ---
for k, t in T.items():
    check(f"{k}: has rows", f"SELECT count(*) FROM {t}", lambda v: v > 0, "> 0")

for k, col in [("pd_req", "request_time"), ("rag", "request_time"), ("agent", "request_time"), ("llm", "event_time")]:
    check(f"{k}: covers {HISTORY_DAYS} days", f"SELECT count(DISTINCT to_date({col})) FROM {T[k]}",
          lambda v: v == HISTORY_DAYS, f"= {HISTORY_DAYS}")
check("trade volume: scoring days", f"SELECT count(DISTINCT trade_date) FROM {T['tv']} WHERE dataset_split = 'scoring_history'",
      lambda v: v == HISTORY_DAYS, f"= {HISTORY_DAYS}")
check("trade volume: 30 series", f"SELECT count(DISTINCT country, product) FROM {T['tv']}", lambda v: v == 30, "= 30")

# --- Reference layer ---
check("dim_model: 5 assets", f"SELECT count(*) FROM {T['dim_model']}", lambda v: v == 5, "= 5")
check("map: in-scope contract metrics", f"SELECT count(*) FROM {T['map']} WHERE scope = 'in_scope'", lambda v: v == 43, "= 43")
check("map: descoped metrics", f"SELECT count(*) FROM {T['map']} WHERE scope = 'descoped'", lambda v: v == 3, "= 3")
check("map: supporting Lane D metrics", f"SELECT count(*) FROM {T['map']} WHERE scope = 'supporting'", lambda v: v == 3, "= 3")
check("scenario calendar: 10 scenarios", f"SELECT count(*) FROM {T['cal']}", lambda v: v == 10, "= 10")

# COMMAND ----------

# --- PD scenarios ---
check("PD: training default rate 8-16%", f"SELECT avg(default_flag) FROM {T['pd_training']}",
      lambda v: 0.08 <= v <= 0.16, "0.08–0.16")
check("S1: debt_to_ebitda late / early",
      f"""SELECT avg(CASE WHEN {day_idx('request_time')} >= 75 THEN debt_to_ebitda END)
               / avg(CASE WHEN {day_idx('request_time')} < 45 THEN debt_to_ebitda END) FROM {T['pd_req']}""",
      lambda v: v > 1.25, "> 1.25")
check("S2: IN default rate after / before",
      f"""SELECT avg(CASE WHEN {day_idx('r.request_time')} >= 66 THEN o.default_flag END)
               / avg(CASE WHEN {day_idx('r.request_time')} < 60 THEN o.default_flag END)
          FROM {T['pd_req']} r JOIN {T['pd_out']} o USING (request_id) WHERE r.country = 'IN'""",
      lambda v: v > 1.6, "> 1.6")
check("PD: no outcomes for last label-delay window",
      f"""SELECT count(*) FROM {T['pd_req']} r JOIN {T['pd_out']} o USING (request_id)
          WHERE to_date(r.request_time) > DATE'{HISTORY_END}' - INTERVAL {LABEL_DELAY_DAYS} DAYS""",
      lambda v: v == 0, "= 0")

# --- Forecast scenario ---
check("S3: FX volume during shock / pre-shock",
      f"""SELECT avg(CASE WHEN {day_idx('trade_date')} BETWEEN 72 AND 80 THEN volume_usd_m END)
               / avg(CASE WHEN {day_idx('trade_date')} BETWEEN 50 AND 71 THEN volume_usd_m END)
          FROM {T['tv']} WHERE product LIKE 'FX%' AND NOT is_weekend""",
      lambda v: v > 1.4, "> 1.4")

# COMMAND ----------

# --- RAG scenarios ---
check("RAG: baseline groundedness", f"SELECT avg(groundedness) FROM {T['rag']} WHERE {day_idx('request_time')} < 45",
      lambda v: v > 0.88, "> 0.88")
check("S5: groundedness during stale index",
      f"SELECT avg(groundedness) FROM {T['rag']} WHERE {day_idx('request_time')} BETWEEN 75 AND 80",
      lambda v: v < 0.85, "< 0.85 (amber)")
check("S5: precision@5 during stale index",
      f"""SELECT avg(size(array_intersect(retrieved_doc_ids, expected_doc_ids)) / k) FROM {T['rag']}
          WHERE {day_idx('request_time')} BETWEEN 75 AND 80""",
      lambda v: v < 0.28, "< 0.28 (amber)")
check("RAG: baseline safety violation rate",
      f"SELECT avg(safety_violation) FROM {T['rag']} WHERE {day_idx('request_time')} < 50", lambda v: v < 0.01, "< 0.01")
check("S4: safety violation rate during probe",
      f"SELECT avg(safety_violation) FROM {T['rag']} WHERE {day_idx('request_time')} BETWEEN 55 AND 57",
      lambda v: v > 0.03, "> 0.03 (red)")

# --- Agent scenarios ---
A = T["agent"]
check("S7: recovery success during timeouts",
      f"SELECT avg(recovered) FROM {A} WHERE {day_idx('request_time')} BETWEEN 62 AND 65", lambda v: v < 0.65, "< 0.65 (red)")
check("Agent: baseline recovery success",
      f"SELECT avg(recovered) FROM {A} WHERE {day_idx('request_time')} < 60", lambda v: v > 0.80, "> 0.80")
check("S8: permission compliance during bad release",
      f"SELECT avg(permission_compliant) FROM {A} WHERE {day_idx('request_time')} BETWEEN 70 AND 72",
      lambda v: v < 0.97, "< 0.97 (red)")
check("Agent: baseline permission compliance",
      f"SELECT avg(permission_compliant) FROM {A} WHERE {day_idx('request_time')} < 70", lambda v: v > 0.99, "> 0.99")
check("S9: approval adherence during bypass",
      f"SELECT avg(approval_obtained) FROM {A} WHERE {day_idx('request_time')} BETWEEN 84 AND 86",
      lambda v: v < 0.95, "< 0.95 (red)")
check("S6: tokens per request, last 7 days / baseline",
      f"""SELECT avg(CASE WHEN {day_idx('request_time')} >= {HISTORY_DAYS - 7} THEN total_tokens END)
               / avg(CASE WHEN {day_idx('request_time')} < 30 THEN total_tokens END) FROM {A}""",
      lambda v: v > 1.2, "> 1.2 (amber)")

# --- Lane D reconciliation ---
check("Lane D: both apps tagged", f"SELECT count(DISTINCT request_tags['app']) FROM {T['llm']}", lambda v: v == 2, "= 2")
check("Lane D: tokens reconcile with request tables",
      f"""SELECT abs((SELECT sum(total_tokens) FROM {T['llm']})
                   - (SELECT sum(total_tokens) FROM {T['rag']}) - (SELECT sum(total_tokens) FROM {A}))""",
      lambda v: v == 0, "= 0")

# COMMAND ----------

report = spark.createDataFrame(results, ["check", "result", "value", "expected"])
display(report)
failed = [r for r in results if r[1] == "FAIL"]
if failed:
    raise AssertionError(f"{len(failed)} check(s) failed: " + ", ".join(r[0] for r in failed))
print(f"All {len(results)} checks passed. Phase 1 data is ready.")