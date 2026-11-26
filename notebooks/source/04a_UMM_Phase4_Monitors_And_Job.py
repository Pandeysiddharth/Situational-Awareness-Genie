# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # UMM Demo · Phase 4 · Data profiling monitors, custom metrics and the daily job
# MAGIC
# MAGIC Self-contained and safe to run as one cell. Runtime is about 20–40 minutes, mostly the first monitor refreshes over 90 days of history.
# MAGIC
# MAGIC | Step | What happens |
# MAGIC |---|---|
# MAGIC | 1 | Prepare tables: add a `residual` column to the forecast log (its drift is the forecast's concept drift) |
# MAGIC | 2 | Create 5 monitors with the lane-sheet settings: profile type, timestamp, model ID, slices, baseline, 1-day granularity, 02:00 UTC schedule, failure email, `<catalog>.<area>_monitoring` output, `/Shared/model_monitoring` assets |
# MAGIC | 3 | Add custom metrics for every contract metric that isn't built in |
# MAGIC | 4 | Wait for the first refreshes |
# MAGIC | 5 | Build `hub_ref.metric_source_map`, which records where each of the 46 metrics lives in the monitor outputs, and check that each has values |
# MAGIC | 6 | Create the daily job `umm-demo-daily` (00:30 UTC) that keeps the demo data fresh |
# MAGIC
# MAGIC Re-running deletes and recreates the monitors (the monitored tables are untouched).

# COMMAND ----------

USE_SINGLE_CATALOG = True
SINGLE_CATALOG = "demo_unified_model_monitoring"
MULTI_CATALOGS = {"credit": "umm_demo_cib_credit", "markets": "umm_demo_cib_markets",
                  "ai_factory": "umm_demo_ai_factory", "hub": "umm_demo_hub"}

NOTIFY_EMAILS = None              # None = the current user; or a list of addresses
ASSETS_ROOT = "/Shared/model_monitoring"
MONITOR_CRON = "0 0 2 * * ?"      # 02:00 UTC daily (lane sheet)
JOB_NAME = "umm-demo-daily"
JOB_CRON = "0 30 0 * * ?"         # 00:30 UTC: data is fresh before the 02:00 monitor refresh
JOB_NOTEBOOKS = {                 # None = same folder as this notebook, with these names
    "pd_live_processor": "UMM_Phase2_PD_Live_Processor",
    "keepalive": "UMM_Phase4_KeepAlive",
    "genai_traffic": "UMM_Phase3B_Send_Traffic",
    "genai_process": "UMM_Phase3C_Process_And_Validate",
}
JOB_NOTEBOOK_DIR = None           # e.g. "/Workspace/Users/you@x.com/umm_demo"; None = this notebook's folder
DAILY_RAG, DAILY_AGENT = 40, 25
CREATE_JOB = True
RECREATE_MONITORS = False      # False = keep existing monitors and only create missing ones
REFRESH_TIMEOUT_MIN = 60


def catalog_for(area):
    return SINGLE_CATALOG if USE_SINGLE_CATALOG else MULTI_CATALOGS[area]


def schema_for(area, schema):
    return f"{area}_{schema}" if USE_SINGLE_CATALOG else schema


def fq(area, schema, table):
    return f"{catalog_for(area)}.{schema_for(area, schema)}.{table}"

# COMMAND ----------

import json, time
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
ME = spark.sql("SELECT current_user()").collect()[0][0]
EMAILS = NOTIFY_EMAILS or [ME]
dim_model = spark.table(fq("hub", "ref", "dim_model")).toPandas().set_index("model_id")
registry = spark.table(fq("hub", "ref", "model_version_registry")).toPandas()
PD_THRESHOLD = float(registry.loc[registry["model_id"] == "pd_counterparty_default", "decision_threshold"].dropna().iloc[0])
price = spark.table(fq("hub", "ref", "ref_llm_pricing")).collect()[0]
P_IN, P_OUT = float(price.input_usd_per_1m_tokens), float(price.output_usd_per_1m_tokens)
COST_SQL = f"(input_tokens * {P_IN} + output_tokens * {P_OUT}) / 1000000.0"


def mon(method, table, suffix="", body=None):
    return w.api_client.do(method, f"/api/2.1/unity-catalog/tables/{table}/monitor{suffix}", body=body)


from databricks.sdk.errors import NotFound


def mon_get(table):
    """Monitor info, or None if the table has no monitor yet."""
    try:
        return mon("GET", table)
    except NotFound:                     # includes ResourceDoesNotExist
        return None
    except Exception as e:
        msg = str(e).lower()
        if any(s in msg for s in ("cannot find monitor", "does not exist", "not found", "404")):
            return None
        raise


print(f"Notify: {EMAILS} · PD threshold {PD_THRESHOLD} · prices in/out {P_IN}/{P_OUT} per 1M tokens")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Prepare tables

# COMMAND ----------

for t in [fq("markets", "monitoring", "trade_volume_scoring_log"), fq("markets", "ml", "trade_volume_baseline")]:
    if "residual" not in spark.table(t).columns:
        spark.sql(f"ALTER TABLE {t} ADD COLUMNS (residual DOUBLE COMMENT 'label - prediction')")
    spark.sql(f"UPDATE {t} SET residual = label - prediction WHERE residual IS NULL")
    print(f"✓ residual on {t}")

# Monitors require label and prediction to have the same type (PD: prediction is BIGINT)
for t in [fq("credit", "monitoring", "pd_inference_log"), fq("credit", "ml", "pd_baseline")]:
    types = dict(spark.table(t).dtypes)
    if types["label"] != types["prediction"]:
        (spark.table(t).withColumn("label", F.col("label").cast(types["prediction"]))
         .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t))
        spark.sql(f"ALTER TABLE {t} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        print(f"✓ label cast {types['label']} → {types['prediction']} on {t}")
    else:
        print(f"✓ label/prediction types match on {t} ({types['label']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2–3 · Monitor specs and custom metrics
# MAGIC
# MAGIC Custom metric names match the contract names wherever they don't clash with a built-in. PD performance metrics also get `_c` custom versions, so the hub always has a stable column even if built-in names differ between releases.

# COMMAND ----------

DOUBLE = T.StructField("output", T.DoubleType()).json()


def agg(name, sql):
    return {"type": "CUSTOM_METRIC_TYPE_AGGREGATE", "name": name, "input_columns": [":table"],
            "definition": sql, "output_data_type": DOUBLE}


def drift(name, sql):
    return {"type": "CUSTOM_METRIC_TYPE_DRIFT", "name": name, "input_columns": [":table"],
            "definition": sql, "output_data_type": DOUBLE}


# PD confusion-matrix pieces over labelled rows only
TP = "sum(CASE WHEN label = 1 AND prediction = 1 THEN 1.0 ELSE 0.0 END)"
FP = "sum(CASE WHEN label = 0 AND prediction = 1 THEN 1.0 ELSE 0.0 END)"
FN = "sum(CASE WHEN label = 1 AND prediction = 0 THEN 1.0 ELSE 0.0 END)"
TN = "sum(CASE WHEN label = 0 AND prediction = 0 THEN 1.0 ELSE 0.0 END)"
PD_METRICS = [
    agg("accuracy_c", f"({TP} + {TN}) / nullif({TP} + {TN} + {FP} + {FN}, 0)"),
    agg("precision_c", f"{TP} / nullif({TP} + {FP}, 0)"),
    agg("recall_c", f"{TP} / nullif({TP} + {FN}, 0)"),
    agg("f1_macro_c", f"(2 * {TP} / nullif(2 * {TP} + {FP} + {FN}, 0) + 2 * {TN} / nullif(2 * {TN} + {FN} + {FP}, 0)) / 2"),
    agg("labelled_share", "avg(CASE WHEN label IS NOT NULL THEN 1.0 ELSE 0.0 END)"),
    agg("approval_rate", "avg(CASE WHEN prediction = 0 THEN 1.0 ELSE 0.0 END)"),
    agg("positive_rate_at_threshold", f"avg(CASE WHEN pd_probability >= {PD_THRESHOLD} THEN 1.0 ELSE 0.0 END)"),
    agg("calibration_gap", "abs(avg(CASE WHEN label IS NOT NULL THEN label END) - avg(CASE WHEN label IS NOT NULL THEN pd_probability END))"),
    drift("concept_drift_score", "{{current_df}}.calibration_gap - {{base_df}}.calibration_gap"),
    drift("model_quality_drift", "{{base_df}}.f1_macro_c - {{current_df}}.f1_macro_c"),
]
FCST_METRICS = [
    agg("mae_c", "avg(abs(prediction - label))"),
    agg("rmse_c", "sqrt(avg(pow(prediction - label, 2)))"),
    agg("nmae", "avg(abs(prediction - label)) / nullif(avg(label), 0)"),
    drift("model_quality_drift", "({{current_df}}.mae_c - {{base_df}}.mae_c) / nullif({{base_df}}.mae_c, 0)"),
]
HAS_EXP = "size(expected_doc_ids) > 0"
HITS = "size(array_intersect(retrieved_doc_ids, expected_doc_ids))"
RAG_METRICS = [
    agg("query_intent_alignment", "avg(query_intent_alignment)"),
    agg("context_relevance", "avg(context_relevance)"),
    agg("groundedness", "avg(groundedness)"),
    agg("response_relevance", "avg(response_relevance)"),
    agg("precision_at_k", f"avg(CASE WHEN {HAS_EXP} THEN {HITS} / k END)"),
    agg("recall_at_k", f"avg(CASE WHEN {HAS_EXP} THEN {HITS} / size(expected_doc_ids) END)"),
    agg("user_satisfaction_score", "avg(user_feedback_positive)"),
    agg("safety_violation_rate", "avg(safety_violation)"),
    agg("avg_tokens_per_request", "avg(total_tokens)"),
    agg("cost_per_successful_query",
        f"sum({COST_SQL}) / nullif(sum(CASE WHEN groundedness = 1 AND response_relevance = 1 THEN 1 ELSE 0 END), 0)"),
    agg("error_rate_app", "avg(CASE WHEN status = 'ERROR' THEN 1.0 ELSE 0.0 END)"),
]
AGENT_METRICS = [
    agg("task_success_rate", "avg(task_success)"),
    agg("tool_accuracy", "avg(tool_correct)"),
    agg("plan_quality", "avg(plan_quality)"),
    agg("decision_path_validity", "avg(decision_path_validity)"),
    agg("user_feedback_score", "avg(user_feedback_positive)"),
    agg("permission_compliance_rate", "avg(permission_compliant)"),
    agg("safety_violation_rate", "avg(safety_violation)"),
    agg("trace_completeness", "avg(trace_complete)"),
    agg("approval_adherence_rate", "avg(approval_obtained)"),
    agg("escalation_appropriateness", "avg(escalation_appropriate)"),
    agg("recovery_success_rate", "avg(recovered)"),
    agg("cost_per_request", f"avg({COST_SQL})"),
    agg("avg_tokens_per_request", "avg(total_tokens)"),
]
LLM_METRICS = [
    agg("error_rate", "avg(CASE WHEN status_code >= 400 THEN 1.0 ELSE 0.0 END)"),
    agg("p95_latency_ms", "percentile_approx(latency_ms, 0.95)"),
    agg("tokens_per_request", "avg(total_tokens)"),
    agg("cost_usd", f"sum({COST_SQL})"),
]


def slices(model_id):
    return [s for s in list(dim_model.loc[model_id, "slicing_exprs"]) if s]


def schema_of(table):
    return ".".join(table.split(".")[:2])


SPECS = {
    "pd_counterparty_default": dict(
        table=fq("credit", "monitoring", "pd_inference_log"), baseline=fq("credit", "ml", "pd_baseline"),
        inference_log={"problem_type": "PROBLEM_TYPE_CLASSIFICATION", "timestamp_col": "request_time",
                       "model_id_col": "model_version", "prediction_col": "prediction", "label_col": "label",
                       "granularities": ["1 day"]},
        custom=PD_METRICS),
    "trade_volume_forecast": dict(
        table=fq("markets", "monitoring", "trade_volume_scoring_log"), baseline=fq("markets", "ml", "trade_volume_baseline"),
        inference_log={"problem_type": "PROBLEM_TYPE_REGRESSION", "timestamp_col": "scoring_time",
                       "model_id_col": "model_version", "prediction_col": "prediction", "label_col": "label",
                       "granularities": ["1 day"]},
        custom=FCST_METRICS),
    "credit_policy_rag": dict(
        table=fq("ai_factory", "monitoring", "rag_request_metrics"), baseline=None,
        time_series={"timestamp_col": "request_time", "granularities": ["1 day"]}, custom=RAG_METRICS),
    "rm_client_brief_agent": dict(
        table=fq("ai_factory", "monitoring", "agent_request_metrics"), baseline=None,
        time_series={"timestamp_col": "request_time", "granularities": ["1 day"]}, custom=AGENT_METRICS),
    "llm_endpoint_shared": dict(
        table=fq("ai_factory", "monitoring", "llm_usage_log"), baseline=None,
        time_series={"timestamp_col": "event_time", "granularities": ["1 day"]}, custom=LLM_METRICS),
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create monitors

# COMMAND ----------

try:
    w.workspace.mkdirs(ASSETS_ROOT)
except Exception:
    pass

for model_id, s in SPECS.items():
    t = s["table"]
    if mon_get(t) and not RECREATE_MONITORS:
        print(f"• keeping existing monitor on {t}")
        continue
    if mon_get(t):
        mon("DELETE", t)
        short = t.split(".")[-1]
        for suffix in ("_profile_metrics", "_drift_metrics"):
            spark.sql(f"DROP TABLE IF EXISTS {schema_of(t)}.{short}{suffix}")
        print(f"  removed existing monitor on {t}")
        time.sleep(5)
    body = {"assets_dir": f"{ASSETS_ROOT}/{t.split('.')[-1]}", "output_schema_name": schema_of(t),
            "slicing_exprs": slices(model_id), "custom_metrics": s["custom"],
            "schedule": {"quartz_cron_expression": MONITOR_CRON, "timezone_id": "UTC"},
            "notifications": {"on_failure": {"email_addresses": EMAILS}}}
    if s.get("inference_log"):
        body["inference_log"] = s["inference_log"]
    else:
        body["time_series"] = s["time_series"]
    if s.get("baseline"):
        body["baseline_table_name"] = s["baseline"]
    res = mon("POST", t, body=body)
    print(f"✓ monitor on {t} · {len(s['custom'])} custom metrics · slices {slices(model_id)} · {res.get('status')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Wait for the first refreshes

# COMMAND ----------

def latest_refresh(t):
    refs = mon("GET", t, "/refreshes").get("refreshes", [])
    return max(refs, key=lambda r: int(r.get("refresh_id", 0))) if refs else None


deadline = time.time() + REFRESH_TIMEOUT_MIN * 60
pending = {s["table"] for s in SPECS.values()}
started = set()
while pending and time.time() < deadline:
    progress = []
    for t in sorted(pending):
        m = mon_get(t) or {}
        status = m.get("status", "missing").replace("MONITOR_STATUS_", "")
        if status == "ERROR" or status == "FAILED":
            print(f"✗ {t.split('.')[-1]}: monitor {status}. Check the table's Quality tab for details.")
            pending.discard(t)
            continue
        if m.get("status") != "MONITOR_STATUS_ACTIVE":
            progress.append(f"{t.split('.')[-1]}={status}")
            continue
        r0 = latest_refresh(t)
        progress.append(f"{t.split('.')[-1]}={r0.get('state') if r0 else 'no refresh yet'}")
        r = latest_refresh(t)
        if r is None and t not in started:
            mon("POST", t, "/refreshes")
            started.add(t)
            continue
        if r and r.get("state") in ("SUCCESS", "FAILED", "CANCELED"):
            print(f"{'✓' if r['state'] == 'SUCCESS' else '✗'} {t.split('.')[-1]}: {r['state']} {r.get('message', '')[:300]}")
            pending.discard(t)
    if pending:
        print(f"  {time.strftime('%H:%M:%S')} " + " · ".join(progress))
        time.sleep(60)
if pending:
    print(f"Still refreshing after {REFRESH_TIMEOUT_MIN} min: {pending}. Re-run from this cell to keep waiting.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Metric source map and coverage
# MAGIC
# MAGIC `hub_ref.metric_source_map` tells Phase 5 exactly where to read each contract metric:
# MAGIC * `profile` + `:table`: one value per window (and per slice / model version)
# MAGIC * `drift` + `BASELINE` or `CONSECUTIVE`: drift vs the baseline table or the previous window
# MAGIC * `max_over_features`: max drift across input feature columns
# MAGIC * `max_minus_min_over_slices`: fairness gap across the `country` slice
# MAGIC
# MAGIC Built-in column names are discovered from the actual output tables rather than assumed.

# COMMAND ----------

def outputs(model_id):
    m = mon_get(SPECS[model_id]["table"]) or {}
    return m.get("profile_metrics_table_name"), m.get("drift_metrics_table_name")


def first_col(table, candidates):
    cols = set(spark.table(table).columns) if table and spark.catalog.tableExists(table) else set()
    return next((c for c in candidates if c in cols), None)


PD, FC, RAG, AGT, LLM = ("pd_counterparty_default", "trade_volume_forecast", "credit_policy_rag",
                         "rm_client_brief_agent", "llm_endpoint_shared")
NON_FEATURES = {PD: ["label", "prediction", "model_version", "request_id", "counterparty_id", "label_time",
                     "served_entity", "record_source", "pd_probability", "request_time"],
                FC: ["label", "prediction", "model_version", "residual", "record_source", "scoring_time", "trade_date"]}
# (model, contract, kind, column candidates, drift_type, column_filter, aggregation)
MAP = [
    (PD, "accuracy", "profile", ["accuracy_score", "accuracy_c"], None, ":table", "value"),
    (PD, "precision", "profile", ["precision_c"], None, ":table", "value"),
    (PD, "recall", "profile", ["recall_c"], None, ":table", "value"),
    (PD, "f1_macro", "profile", ["f1_macro_c"], None, ":table", "value"),
    (PD, "data_drift_js", "drift", ["js_distance"], "BASELINE", "features", "max_over_features"),
    (PD, "data_drift_psi", "drift", ["population_stability_index", "psi"], "BASELINE", "features", "max_over_features"),
    (PD, "concept_drift_score", "drift", ["concept_drift_score"], "BASELINE", ":table", "value"),
    (PD, "label_drift_js", "drift", ["js_distance", "population_stability_index"], "BASELINE", "label", "value"),
    (PD, "model_quality_drift", "drift", ["model_quality_drift"], "CONSECUTIVE", ":table", "value"),
    (PD, "approval_rate_gap", "profile", ["approval_rate"], None, ":table", "max_minus_min_over_slices:country"),
    (PD, "quality_gap", "profile", ["f1_macro_c"], None, ":table", "max_minus_min_over_slices:country"),
    (PD, "positive_rate_at_threshold", "profile", ["positive_rate_at_threshold"], None, ":table", "ratio_to_baseline"),
    (FC, "mae", "profile", ["mean_absolute_error", "mean_average_error", "mae_c"], None, ":table", "ratio_to_baseline"),
    (FC, "rmse", "profile", ["root_mean_squared_error", "rmse_c"], None, ":table", "ratio_to_baseline"),
    (FC, "data_drift_js", "drift", ["js_distance"], "BASELINE", "features", "max_over_features"),
    (FC, "data_drift_psi", "drift", ["population_stability_index", "psi"], "BASELINE", "features", "max_over_features"),
    (FC, "label_drift_js", "drift", ["js_distance", "population_stability_index"], "BASELINE", "label", "value"),
    (FC, "concept_drift_score", "drift", ["js_distance", "population_stability_index"], "BASELINE", "residual", "value"),
    (FC, "model_quality_drift", "drift", ["model_quality_drift"], "CONSECUTIVE", ":table", "value"),
    (FC, "quality_gap", "profile", ["nmae"], None, ":table", "max_minus_min_over_slices:country"),
]
for c in ["query_intent_alignment", "context_relevance", "groundedness", "response_relevance", "precision_at_k",
          "recall_at_k", "user_satisfaction_score", "safety_violation_rate", "avg_tokens_per_request", "cost_per_successful_query"]:
    MAP.append((RAG, c, "profile", [c], None, ":table", "ratio_to_baseline" if c in ("avg_tokens_per_request", "cost_per_successful_query") else "value"))
for c in ["task_success_rate", "tool_accuracy", "plan_quality", "decision_path_validity", "user_feedback_score",
          "permission_compliance_rate", "safety_violation_rate", "trace_completeness", "approval_adherence_rate",
          "escalation_appropriateness", "recovery_success_rate", "cost_per_request", "avg_tokens_per_request"]:
    MAP.append((AGT, c, "profile", [c], None, ":table", "ratio_to_baseline" if c in ("cost_per_request", "avg_tokens_per_request") else "value"))
for c in ["error_rate", "p95_latency_ms", "tokens_per_request"]:
    MAP.append((LLM, c, "profile", [c], None, ":table", "ratio_to_baseline" if c != "error_rate" else "value"))

def windows_with_values(table, col, kind, dtype, colf, aggr, model_id):
    where = ["log_type = 'INPUT'"] if kind == "profile" else [f"drift_type = '{dtype}'"]
    if colf == "features":
        excl = ", ".join(f"'{x}'" for x in NON_FEATURES[model_id] + [":table"])
        where.append(f"column_name NOT IN ({excl})")
    else:
        where.append(f"column_name = '{colf}'")
    where.append(f"slice_key = '{aggr.split(':')[1]}'" if aggr.startswith("max_minus_min_over_slices") else "slice_key IS NULL")
    where.append(f"`{col}` IS NOT NULL")
    return spark.sql(f"SELECT count(DISTINCT window.start) FROM {table} WHERE {' AND '.join(where)}").collect()[0][0]


rows = []
for model_id, contract, kind, cands, dtype, colf, aggr in MAP:
    prof, drf = outputs(model_id)
    table = prof if kind == "profile" else drf
    cols = set(spark.table(table).columns) if table and spark.catalog.tableExists(table) else set()
    col, n = None, 0
    for c in cands:                      # first candidate that exists AND has values
        if c in cols:
            n = windows_with_values(table, c, kind, dtype, colf, aggr, model_id)
            if n > 0:
                col = c
                break
    rows.append(dict(model_id=model_id, contract_name=contract, source_kind=kind, source_table=table or "",
                     source_column=col or "", drift_type=dtype or "", column_filter=colf, aggregation=aggr,
                     windows_with_values=int(n), found=bool(col) and n > 0))

smap = pd.DataFrame(rows)
mm = spark.table(fq("hub", "ref", "map_model_metric")).toPandas()[["model_id", "contract_name", "scope"]]
smap = smap.merge(mm, on=["model_id", "contract_name"], how="left")
spark.createDataFrame(smap).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("hub", "ref", "metric_source_map"))
print(f"✓ {fq('hub', 'ref', 'metric_source_map')}: {len(smap)} metrics, {smap['found'].sum()} with values")

# Monitors compute JS only for categorical columns. Where PSI stands in (numeric label / residual),
# switch that metric to standard PSI bands so thresholds match the measure.
mmt = fq("hub", "ref", "map_model_metric")
for r in smap[(smap["source_column"] == "population_stability_index") & smap["contract_name"].isin(["label_drift_js", "concept_drift_score"])].itertuples():
    spark.sql(f"""UPDATE {mmt} SET amber_threshold = 0.10, red_threshold = 0.25, unit = 'index',
                  implementation = 'Built-in PSI on numeric column {r.column_filter} vs baseline (monitors compute JS for categorical columns only)'
                  WHERE model_id = '{r.model_id}' AND contract_name = '{r.contract_name}'""")
    print(f"  {r.model_id}.{r.contract_name}: PSI on '{r.column_filter}', thresholds 0.10 / 0.25")
display(spark.table(fq("hub", "ref", "metric_source_map")).orderBy("found", "model_id", "contract_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Daily job `umm-demo-daily`
# MAGIC
# MAGIC Order: process yesterday's PD endpoint traffic → keep-alive (new forecast day, matured PD outcomes, new PD traffic) → GenAI traffic → judge and append. The monitors then refresh on their own 02:00 UTC schedule.
# MAGIC
# MAGIC The job uses serverless compute. One-time console step: open `UMM_Phase3C_Process_And_Validate`, then **Environment** (right panel) → add the dependencies `mlflow[databricks]>=3.1` and `databricks-agents` → Apply. Job runs then start with the right packages.

# COMMAND ----------

job_id = None
if CREATE_JOB:
    here = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    folder = JOB_NOTEBOOK_DIR or here.rsplit("/", 1)[0]
    paths, missing = {}, []
    for key, name in JOB_NOTEBOOKS.items():
        p = name if name.startswith("/") else f"{folder}/{name}"
        try:
            st = w.workspace.get_status(p)
            if str(getattr(st, "object_type", "")).endswith("NOTEBOOK"):
                paths[key] = p
            else:
                missing.append(f"{p} (exists but is a {st.object_type}, import it as a notebook)")
        except Exception:
            missing.append(p)
    for k, p in paths.items():
        print(f"  found {k}: {p}")
    if missing:
        print("⚠ Job not created. These notebooks weren't found as notebooks:\n  " + "\n  ".join(missing) +
              "\nImport them (Workspace → ⋮ → Import → File) into one folder, set JOB_NOTEBOOK_DIR if it differs, and re-run this cell.")
    else:
        def task(key, after=None, params=None):
            t = {"task_key": key, "notebook_task": {"notebook_path": paths[key], "source": "WORKSPACE",
                                                    "base_parameters": params or {}}, "timeout_seconds": 3600}
            if after:
                t["depends_on"] = [{"task_key": after}]
            return t
        settings = {
            "name": JOB_NAME, "max_concurrent_runs": 1, "tags": {"project": "umm_demo"},
            "schedule": {"quartz_cron_expression": JOB_CRON, "timezone_id": "UTC", "pause_status": "UNPAUSED"},
            "email_notifications": {"on_failure": EMAILS},
            "tasks": [task("pd_live_processor"), task("keepalive", "pd_live_processor"),
                      task("genai_traffic", "keepalive", {"n_rag": str(DAILY_RAG), "n_agent": str(DAILY_AGENT)}),
                      task("genai_process", "genai_traffic")],
        }
        existing = [j for j in w.api_client.do("GET", "/api/2.1/jobs/list", query={"name": JOB_NAME}).get("jobs", [])]
        if existing:
            job_id = existing[0]["job_id"]
            w.api_client.do("POST", "/api/2.1/jobs/reset", body={"job_id": job_id, "new_settings": settings})
            print(f"✓ updated job {JOB_NAME} ({job_id})")
        else:
            job_id = w.api_client.do("POST", "/api/2.1/jobs/create", body=settings)["job_id"]
            print(f"✓ created job {JOB_NAME} ({job_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate

# COMMAND ----------

results = []
for model_id, s in SPECS.items():
    m = mon_get(s["table"]) or {}
    r = latest_refresh(s["table"]) or {}
    results.append((f"{model_id}: monitor active", m.get("status", "missing"), m.get("status") == "MONITOR_STATUS_ACTIVE"))
    results.append((f"{model_id}: first refresh", r.get("state", "none"), r.get("state") == "SUCCESS"))
inscope = smap[smap["scope"] == "in_scope"]
support = smap[smap["scope"] == "supporting"]
results.append(("In-scope contract metrics with values", f"{int(inscope['found'].sum())}/{len(inscope)}",
                bool(inscope["found"].all()) and len(inscope) == 43))
results.append(("Supporting Lane D metrics with values", f"{int(support['found'].sum())}/{len(support)}",
                bool(support["found"].all()) and len(support) == 3))
win = int(smap.loc[smap["source_kind"] == "profile", "windows_with_values"].median())
results.append(("Median daily windows per metric (monitors compute the last 30 days)", win, win >= 28))
results.append(("Daily job", job_id or "not created", job_id is not None or not CREATE_JOB))

report = pd.DataFrame([(n, "PASS" if ok else "FAIL", str(v)) for n, v, ok in results], columns=["check", "result", "value"])
display(report)
missing = smap[~smap["found"] & smap["scope"].isin(["in_scope", "supporting"])]
if len(missing):
    print("Metrics without values:\n", missing[["model_id", "contract_name", "source_table", "source_column"]].to_string())
if (report["result"] == "FAIL").any():
    raise AssertionError("Failed: " + ", ".join(report.loc[report["result"] == "FAIL", "check"]))
print("Phase 4 complete. Monitor dashboards are in", ASSETS_ROOT)
