# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # UMM Demo · Phase 5 · Hub: gold layer, health scoring, dashboard, alerts and Genie
# MAGIC
# MAGIC Self-contained and safe to run as one cell. In `full` mode (default) it builds everything, in about 10 minutes. The daily hub job runs it in `refresh` mode, which rebuilds only the gold tables.
# MAGIC
# MAGIC | Step | What happens |
# MAGIC |---|---|
# MAGIC | 1 | **Collect**: read every contract metric from the monitor outputs, using `metric_source_map` |
# MAGIC | 2 | **Backfill**: compute the same metrics from the monitored tables for days the monitors don't cover (they compute the last 30 days) |
# MAGIC | 3 | **Calibrate** (full mode): set amber/red for metrics whose thresholds were placeholders, from each model's real baseline behaviour |
# MAGIC | 4 | **Score**: daily status per metric (green/amber/red), daily health per model, and an alert log of status changes |
# MAGIC | 5 | **Serve**: UC metric views, the AI/BI dashboard (3 altitudes), SQL alerts, the Genie space, and the 03:00 UTC hub refresh job |
# MAGIC
# MAGIC Gold tables land in `hub_gold`. Every metric row carries `source = monitor | backfill | hub_pooled`, so lineage is explicit. Drift metrics for the PD model and the forecast use trailing pooled windows (7 and 14 days): daily windows are too small for stable distribution drift, especially the forecast's 30 rows a day.

# COMMAND ----------

USE_SINGLE_CATALOG = True
SINGLE_CATALOG = "demo_unified_model_monitoring"
MULTI_CATALOGS = {"credit": "umm_demo_cib_credit", "markets": "umm_demo_cib_markets",
                  "ai_factory": "umm_demo_ai_factory", "hub": "umm_demo_hub"}

MODE = "full"                      # "full" = everything · "refresh" = gold tables only (used by the hub job)
BASELINE_DAYS = 30                 # baseline for ratio metrics = median of the first 30 days of history
CALIBRATION_DAYS = (7, 44)         # window used to calibrate placeholder thresholds (before any scenario)
EVAL_WINDOW = {"supervised_ml": 7, "generative_ai": 3, "ai_agents": 3, "llm_service": 3}   # rolling days
WAREHOUSE_ID = None                # None = reuse a serverless warehouse, else create "umm-demo-wh"
DASHBOARD_NAME = "UMM · Unified Model Monitoring"
GENIE_TITLE = "UMM · Model Monitoring Genie"
HUB_JOB_NAME = "umm-demo-hub-refresh"
HUB_JOB_CRON = "0 0 3 * * ?"       # 03:00 UTC, after the 02:00 monitor refresh
ASSETS_ROOT = "/Shared/model_monitoring"

try:                               # the hub job passes mode=refresh
    dbutils.widgets.text("mode", MODE)
    MODE = dbutils.widgets.get("mode") or MODE
except Exception:
    pass


def catalog_for(area):
    return SINGLE_CATALOG if USE_SINGLE_CATALOG else MULTI_CATALOGS[area]


def schema_for(area, schema):
    return f"{area}_{schema}" if USE_SINGLE_CATALOG else schema


def fq(area, schema, table):
    return f"{catalog_for(area)}.{schema_for(area, schema)}.{table}"

# COMMAND ----------

import json, math, time, uuid
import numpy as np
import pandas as pd

GOLD = lambda t: fq("hub", "gold", t)
REF = lambda t: fq("hub", "ref", t)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_for('hub')}.{schema_for('hub', 'gold')}")

dim_model = spark.table(REF("dim_model")).toPandas()
dim_metric = spark.table(REF("dim_metric")).toPandas()
mmm = spark.table(REF("map_model_metric")).toPandas()
smap = spark.table(REF("metric_source_map")).toPandas()
registry = spark.table(REF("model_version_registry")).toPandas()
cal = spark.table(REF("scenario_calendar")).toPandas()
price = spark.table(REF("ref_llm_pricing")).toPandas().iloc[0]
P_IN, P_OUT = float(price["input_usd_per_1m_tokens"]), float(price["output_usd_per_1m_tokens"])
PD_THRESHOLD = float(registry.loc[registry["model_id"] == "pd_counterparty_default", "decision_threshold"].dropna().iloc[0])
HISTORY_START = pd.Timestamp(cal["start_date"].min()) - pd.Timedelta(days=int(cal["start_day"].min()))
TODAY = pd.Timestamp.utcnow().tz_localize(None).normalize()
print(f"Mode {MODE} · history from {HISTORY_START.date()} · PD threshold {PD_THRESHOLD}")

PD, FC, RAG, AGT, LLM = ("pd_counterparty_default", "trade_volume_forecast", "credit_policy_rag",
                         "rm_client_brief_agent", "llm_endpoint_shared")
BUCKET = dict(zip(dim_model["model_id"], dim_model["bucket"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Collect from the monitor outputs

# COMMAND ----------

NON_FEATURES = {PD: ["label", "prediction", "model_version", "request_id", "counterparty_id", "label_time",
                     "served_entity", "record_source", "pd_probability", "request_time"],
                FC: ["label", "prediction", "model_version", "residual", "record_source", "scoring_time", "trade_date"]}


def monitor_rows(r):
    """Daily values of one contract metric from the monitor output tables."""
    if not r.found:
        return pd.DataFrame()
    cols = spark.table(r.source_table).columns
    ver = "model_version" if "model_version" in cols else "CAST(NULL AS STRING)"
    where = ["log_type = 'INPUT'"] if r.source_kind == "profile" else [f"drift_type = '{r.drift_type}'"]
    if r.column_filter == "features":
        excl = ", ".join(f"'{x}'" for x in NON_FEATURES[r.model_id] + [":table"])
        where.append(f"column_name NOT IN ({excl})")
    else:
        where.append(f"column_name = '{r.column_filter}'")
    gap = r.aggregation.startswith("max_minus_min_over_slices")
    where.append(f"slice_key = '{r.aggregation.split(':')[1]}'" if gap else "slice_key IS NULL")
    where.append(f"`{r.source_column}` IS NOT NULL")
    if r.column_filter == "features":
        sql = (f"SELECT to_date(window.start) AS metric_date, {ver} AS model_version, max(`{r.source_column}`) AS value "
               f"FROM {r.source_table} WHERE {' AND '.join(where)} GROUP BY ALL")
    elif gap:
        sql = (f"SELECT to_date(window.start) AS metric_date, {ver} AS model_version, "
               f"max(`{r.source_column}`) - min(`{r.source_column}`) AS value "
               f"FROM {r.source_table} WHERE {' AND '.join(where)} GROUP BY ALL")
    else:
        sql = (f"SELECT to_date(window.start) AS metric_date, {ver} AS model_version, `{r.source_column}` AS value "
               f"FROM {r.source_table} WHERE {' AND '.join(where)}")
    df = spark.sql(sql).toPandas()
    df["model_id"], df["contract_name"] = r.model_id, r.contract_name
    return df


mon_parts = []
for r in smap.itertuples():
    try:
        mon_parts.append(monitor_rows(r))
    except Exception as e:
        print(f"  ⚠ {r.model_id}.{r.contract_name}: {str(e)[:160]}")
mon_parts = [p for p in mon_parts if len(p)]
mon = pd.concat(mon_parts, ignore_index=True) if mon_parts else pd.DataFrame()
if len(mon):
    mon["metric_date"] = pd.to_datetime(mon["metric_date"])
    mon["value"] = pd.to_numeric(mon["value"], errors="coerce")
    # One value per model, metric and day: prefer the all-versions row, else the newest version that day
    mon["_v"] = pd.to_numeric(mon["model_version"], errors="coerce").fillna(1e9)
    mon = (mon.dropna(subset=["value"]).sort_values("_v", ascending=False)
           .drop_duplicates(["model_id", "contract_name", "metric_date"]).drop(columns="_v"))
    mon["source"] = "monitor"
print(f"✓ monitor values: {len(mon):,} metric-days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Backfill from the monitored tables (same definitions)

# COMMAND ----------

def load(area, table):
    return spark.table(fq(area, "monitoring", table)).toPandas()


def load_base(area, table):
    return spark.table(fq(area, "ml", table)).toPandas()


def daily(df, ts_col, funcs, by=None):
    d = df.assign(metric_date=pd.to_datetime(df[ts_col]).dt.normalize())
    keys = ["metric_date"] + ([by] if by else [])
    out = d.groupby(keys).apply(lambda g: pd.Series({k: f(g) for k, f in funcs.items()}))
    return out.reset_index()


def safe_div(a, b):
    return a / b if b else np.nan


def conf(g):
    lab = g[g["label"].notna()]
    y, p = lab["label"].astype(int), lab["prediction"].astype(int)
    return ((y == 1) & (p == 1)).sum(), ((y == 0) & (p == 1)).sum(), ((y == 1) & (p == 0)).sum(), ((y == 0) & (p == 0)).sum()


def f1_macro(g):
    tp, fp, fn, tn = conf(g)
    a, b = safe_div(2 * tp, 2 * tp + fp + fn), safe_div(2 * tn, 2 * tn + fn + fp)
    return np.nanmean([a, b]) if not (np.isnan(a) and np.isnan(b)) else np.nan


PD_FUNCS = {
    "accuracy": lambda g: (lambda c: safe_div(c[0] + c[3], sum(c)))(conf(g)),
    "precision": lambda g: (lambda c: safe_div(c[0], c[0] + c[1]))(conf(g)),
    "recall": lambda g: (lambda c: safe_div(c[0], c[0] + c[2]))(conf(g)),
    "f1_macro": f1_macro,
    "positive_rate_at_threshold": lambda g: (g["pd_probability"] >= PD_THRESHOLD).mean(),
    "_calibration_gap": lambda g: (lambda l: abs(l["label"].mean() - l["pd_probability"].mean()) if len(l) else np.nan)(g[g["label"].notna()]),
    "_approval_rate": lambda g: (g["prediction"] == 0).mean(),
}
FC_FUNCS = {
    "mae": lambda g: (g["prediction"] - g["label"]).abs().mean(),
    "rmse": lambda g: math.sqrt(((g["prediction"] - g["label"]) ** 2).mean()),
    "_nmae": lambda g: safe_div((g["prediction"] - g["label"]).abs().mean(), g["label"].mean()),
}


def cost(g):
    return (g["input_tokens"] * P_IN + g["output_tokens"] * P_OUT).sum() / 1e6


def hits(g):
    return g.apply(lambda r: len(set(list(r["retrieved_doc_ids"])) & set(list(r["expected_doc_ids"]))), axis=1)


RAG_FUNCS = {
    **{c: (lambda c: lambda g: g[c].mean())(c) for c in ["query_intent_alignment", "context_relevance", "groundedness", "response_relevance"]},
    "precision_at_k": lambda g: (lambda m: (hits(g[m]) / g.loc[m, "k"]).mean() if m.any() else np.nan)(g["expected_doc_ids"].apply(len) > 0),
    "recall_at_k": lambda g: (lambda m: (hits(g[m]) / g.loc[m, "expected_doc_ids"].apply(len)).mean() if m.any() else np.nan)(g["expected_doc_ids"].apply(len) > 0),
    "user_satisfaction_score": lambda g: g["user_feedback_positive"].mean(),
    "safety_violation_rate": lambda g: g["safety_violation"].mean(),
    "avg_tokens_per_request": lambda g: g["total_tokens"].mean(),
    "cost_per_successful_query": lambda g: safe_div(cost(g), ((g["groundedness"] == 1) & (g["response_relevance"] == 1)).sum()),
}
AGT_COLS = {"task_success_rate": "task_success", "tool_accuracy": "tool_correct", "plan_quality": "plan_quality",
            "decision_path_validity": "decision_path_validity", "user_feedback_score": "user_feedback_positive",
            "permission_compliance_rate": "permission_compliant", "safety_violation_rate": "safety_violation",
            "trace_completeness": "trace_complete", "approval_adherence_rate": "approval_obtained",
            "escalation_appropriateness": "escalation_appropriate", "recovery_success_rate": "recovered"}
AGT_FUNCS = {**{k: (lambda c: lambda g: g[c].mean())(c) for k, c in AGT_COLS.items()},
             "cost_per_request": lambda g: safe_div(cost(g), len(g)), "avg_tokens_per_request": lambda g: g["total_tokens"].mean()}
LLM_FUNCS = {"error_rate": lambda g: (g["status_code"] >= 400).mean(),
             "p95_latency_ms": lambda g: g["latency_ms"].quantile(0.95), "tokens_per_request": lambda g: g["total_tokens"].mean()}


def psi(base, cur, bins=10):
    base, cur = pd.Series(base).dropna().astype(float), pd.Series(cur).dropna().astype(float)
    if len(base) < 20 or len(cur) < 5:
        return np.nan
    edges = np.unique(np.quantile(base, np.linspace(0, 1, bins + 1)))
    if len(edges) < 3:            # (near) binary column: use its values as categories
        cats = sorted(set(base) | set(cur))
        e = np.array([(base == c).mean() for c in cats]) + 1e-4
        a = np.array([(cur == c).mean() for c in cats]) + 1e-4
    else:
        edges[0], edges[-1] = -np.inf, np.inf
        e = np.histogram(base, edges)[0] / len(base) + 1e-4
        a = np.histogram(cur, edges)[0] / len(cur) + 1e-4
    return float(np.sum((a - e) * np.log(a / e)))


def js(base, cur):
    cats = sorted(set(base.dropna()) | set(cur.dropna()))
    p = np.array([(base == c).mean() for c in cats]) + 1e-12
    q = np.array([(cur == c).mean() for c in cats]) + 1e-12
    m = (p + q) / 2
    kl = lambda x, y: np.sum(x * np.log2(x / y))
    return float(math.sqrt(max(0.0, 0.5 * kl(p, m) + 0.5 * kl(q, m))))


POOL_DAYS = {PD: 7, FC: 14}   # drift uses trailing pooled windows: daily windows are too small (and the forecast has 30 rows/day)
DRIFT_CONTRACTS = ["data_drift_psi", "data_drift_js", "label_drift_js", "concept_drift_score", "model_quality_drift"]


def pooled_drift(log, base, ts_col, W, numeric, categorical, kind, base_gap=None):
    d = log.assign(_day=pd.to_datetime(log[ts_col]).dt.normalize())
    rows = []
    first = d["_day"].min()
    for day in sorted(d["_day"].unique()):
        if (day - first).days < W - 1:        # only score once a full window is available
            continue
        g = d[(d["_day"] > day - pd.Timedelta(days=W)) & (d["_day"] <= day)]
        prev = d[(d["_day"] > day - pd.Timedelta(days=2 * W)) & (d["_day"] <= day - pd.Timedelta(days=W))]
        r = {"metric_date": day,
             "data_drift_psi": np.nanmax([psi(base[c], g[c]) for c in numeric]),
             "data_drift_js": np.nanmax([js(base[c].astype(str), g[c].astype(str)) for c in categorical]),
             "label_drift_js": psi(base["label"], g["label"])}
        if kind == "pd":
            lab = g[g["label"].notna()]
            r["concept_drift_score"] = abs(lab["label"].mean() - lab["pd_probability"].mean()) - base_gap if len(lab) > 50 else np.nan
            r["model_quality_drift"] = f1_macro(prev) - f1_macro(g) if len(prev) and g["label"].notna().sum() > 50 else np.nan
        else:
            r["concept_drift_score"] = psi(base["residual"], g["residual"])
            mae_c, mae_p = (g["prediction"] - g["label"]).abs().mean(), (prev["prediction"] - prev["label"]).abs().mean()
            r["model_quality_drift"] = (mae_c - mae_p) / mae_p if len(prev) and mae_p else np.nan
        rows.append(r)
    out = pd.DataFrame(rows).melt(id_vars="metric_date", var_name="contract_name", value_name="value")
    out["source"] = "hub_pooled"
    return out


def melt(df, model_id):
    keep = [c for c in df.columns if c != "metric_date" and not c.startswith("_")]
    out = df.melt(id_vars="metric_date", value_vars=keep, var_name="contract_name", value_name="value")
    out["model_id"] = model_id
    return out


def gap(df, ts_col, func, model_id, contract):
    s = daily(df, ts_col, {"v": func}, by="country")
    g = s.groupby("metric_date")["v"].agg(lambda x: x.max() - x.min()).reset_index(name="value")
    g["model_id"], g["contract_name"] = model_id, contract
    return g, s.assign(model_id=model_id, contract_name=contract)


bf, slices = [], []
pd_log, pd_base = load("credit", "pd_inference_log"), load_base("credit", "pd_baseline")
pdd = daily(pd_log, "request_time", PD_FUNCS)
base_cal = abs(pd_base["label"].mean() - pd_base["pd_probability"].mean())
bf.append(melt(pdd, PD))
pooled = []
pd_num = ["annual_revenue_usd_m", "debt_to_ebitda", "interest_coverage", "utilisation_pct", "days_past_due_max_12m",
          "internal_rating", "years_relationship", "collateral_coverage", "requested_limit_usd_m"]
pooled.append(pooled_drift(pd_log, pd_base, "request_time", POOL_DAYS[PD], pd_num, ["country", "product", "sector"], "pd", base_cal).assign(model_id=PD))
for contract, fn in [("approval_rate_gap", PD_FUNCS["_approval_rate"]), ("quality_gap", f1_macro)]:
    g, s = gap(pd_log, "request_time", fn, PD, contract)
    bf.append(g)
    slices.append(s)

fc_log, fc_base = load("markets", "trade_volume_scoring_log"), load_base("markets", "trade_volume_baseline")
fcd = daily(fc_log, "scoring_time", FC_FUNCS)
bf.append(melt(fcd, FC))
# Series-level inputs only: fx_vol_lag1 and rates_lag1 are market-wide (one value per day), so distribution drift isn't meaningful for them
fc_num = ["vol_lag1", "vol_lag7", "vol_roll7", "vol_roll28", "clients_lag7"]
pooled.append(pooled_drift(fc_log, fc_base, "scoring_time", POOL_DAYS[FC], fc_num, ["country", "product"], "fc").assign(model_id=FC))
g, s = gap(fc_log, "scoring_time", FC_FUNCS["_nmae"], FC, "quality_gap")
bf.append(g)
slices.append(s)

rag = load("ai_factory", "rag_request_metrics")
bf.append(melt(daily(rag, "request_time", RAG_FUNCS), RAG))
agt = load("ai_factory", "agent_request_metrics")
bf.append(melt(daily(agt, "request_time", AGT_FUNCS), AGT))
llm = load("ai_factory", "llm_usage_log")
bf.append(melt(daily(llm, "event_time", LLM_FUNCS), LLM))

# Slice detail for Genie ("which country / category / intent?")
for df, ts, by, funcs, mid in [(rag, "request_time", "query_category", {"groundedness": RAG_FUNCS["groundedness"], "context_relevance": RAG_FUNCS["context_relevance"]}, RAG),
                               (agt, "request_time", "intent", {"task_success_rate": AGT_FUNCS["task_success_rate"], "tool_accuracy": AGT_FUNCS["tool_accuracy"]}, AGT)]:
    s = daily(df, ts, funcs, by=by).melt(id_vars=["metric_date", by], var_name="contract_name", value_name="v")
    slices.append(s.rename(columns={by: "slice_value"}).assign(model_id=mid, slice_key=by))

backfill = pd.concat(bf, ignore_index=True)
backfill["metric_date"] = pd.to_datetime(backfill["metric_date"])
backfill = backfill.dropna(subset=["value"])
backfill = backfill[np.isfinite(backfill["value"].astype(float))]
backfill["source"], backfill["model_version"] = "backfill", None
pooled = pd.concat(pooled, ignore_index=True)
pooled["metric_date"] = pd.to_datetime(pooled["metric_date"])
pooled = pooled.dropna(subset=["value"])
pooled = pooled[np.isfinite(pooled["value"].astype(float))]
pooled["model_version"] = None
backfill = pd.concat([backfill, pooled], ignore_index=True)
POOLED_KEYS = set(zip(pooled["model_id"], pooled["contract_name"]))
MEM_DAILY = fq("ai_factory", "monitoring", "agent_memory_daily")      # Phase 7C (agent memory on Lakebase)
if spark.catalog.tableExists(MEM_DAILY):
    md = spark.table(MEM_DAILY).toPandas()
    md = md.assign(model_id=AGT, source="hub_direct", model_version=None)[["model_id", "contract_name", "metric_date", "value", "source", "model_version"]]
    md["metric_date"] = pd.to_datetime(md["metric_date"])
    backfill = pd.concat([backfill, md.dropna(subset=["value"])], ignore_index=True)
    print(f"✓ memory metrics: {len(md):,} metric-days from {MEM_DAILY}")
print(f"✓ backfill values: {len(backfill):,} metric-days (incl. {len(pooled):,} pooled-window drift values)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge: monitor values win; backfill fills the days before monitor coverage

# COMMAND ----------

wanted = set(zip(smap["model_id"], smap["contract_name"]))
backfill = backfill[[k in wanted for k in zip(backfill["model_id"], backfill["contract_name"])]]
cols = ["model_id", "contract_name", "metric_date", "model_version", "value", "source"]
facts = pd.concat([mon[cols] if len(mon) else pd.DataFrame(columns=cols), backfill[cols]], ignore_index=True)
facts = facts.sort_values("source", key=lambda s: s.map({"hub_pooled": 0, "hub_direct": 0, "monitor": 1, "backfill": 2})) \
             .drop_duplicates(["model_id", "contract_name", "metric_date"]).sort_values(["model_id", "contract_name", "metric_date"])
facts["value"] = facts["value"].astype(float)
facts["metric_date"] = pd.to_datetime(facts["metric_date"])
facts = facts.reset_index(drop=True)

# Consistency check on the overlap (non-drift metrics): backfill should match the monitor closely
consistency = None
if len(mon):
    ov = mon.merge(backfill, on=["model_id", "contract_name", "metric_date"], suffixes=("_m", "_b"))
    ov = ov[~ov["contract_name"].str.contains("drift") & (ov["source_b"] == "backfill")]
    if len(ov):
        ov["rel_diff"] = (ov["value_m"] - ov["value_b"]).abs() / ov["value_m"].abs().clip(lower=1e-6)
        consistency = float((ov["rel_diff"] < 0.02).mean())
        print(f"✓ monitor vs backfill on {len(ov):,} overlapping metric-days: {consistency:.1%} within 2%")
print(f"✓ merged facts: {len(facts):,} metric-days · {facts.groupby('source').size().to_dict()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3–4 · Calibrate placeholder thresholds and score health
# MAGIC
# MAGIC * **Evaluated value:** a rolling mean of daily values. That's 7 days for classical models (labels and slices are noisy) and 3 days for GenAI, agents and the LLM endpoint (high volume, short incidents).
# MAGIC * **Ratio metrics:** compared with their baseline (median of the first 30 days).
# MAGIC * **Calibration** (full mode only): metrics whose starting thresholds were placeholders get control limits from days 7–44, before any scenario. Amber is mean ± max(3σ, 0.03) and red is mean ± max(4.5σ, 0.06). The result is written back to `map_model_metric` with its basis.

# COMMAND ----------

for col, typ in [("threshold_basis", "STRING"), ("baseline_value", "DOUBLE"), ("eval_window_days", "INT")]:
    if col not in spark.table(REF("map_model_metric")).columns:
        spark.sql(f"ALTER TABLE {REF('map_model_metric')} ADD COLUMNS ({col} {typ})")
mmm = spark.table(REF("map_model_metric")).toPandas()
thr = mmm[mmm["scope"].isin(["in_scope", "supporting"])].set_index(["model_id", "contract_name"])

day_idx = lambda s: (s - HISTORY_START).dt.days
facts["day"] = day_idx(facts["metric_date"])
facts = facts.sort_values(["model_id", "contract_name", "metric_date"])


def eval_series(g, model_id, contract):
    w = EVAL_WINDOW[BUCKET[model_id]]
    s = g.set_index("metric_date")["value"]
    if (model_id, contract) not in POOLED_KEYS:          # pooled drift is already a trailing window
        s = s.rolling(f"{w}D", min_periods=1).mean()
    t = thr.loc[(model_id, contract)]
    base = g.loc[g["day"].between(0, BASELINE_DAYS - 1), "value"].median()
    if t["threshold_type"] == "relative_to_baseline":
        s = s / base if base else s * np.nan
        if t["direction"] == "deviation":
            s = (s - 1).abs()
    return s.to_numpy(), base


CALIBRATE = {PD: ["accuracy", "precision", "recall", "f1_macro", "approval_rate_gap", "quality_gap",
                  "concept_drift_score", "data_drift_js", "model_quality_drift"],
             FC: ["quality_gap", "data_drift_js", "model_quality_drift", "concept_drift_score"]}
facts["eval_value"], facts["baseline_value"] = np.nan, np.nan
for (mid, c), g in facts.groupby(["model_id", "contract_name"]):
    if (mid, c) not in thr.index:
        continue
    ev, base = eval_series(g, mid, c)
    facts.loc[g.index, "eval_value"] = ev
    facts.loc[g.index, "baseline_value"] = base

if MODE == "full":
    updates = []
    for mid, contracts in CALIBRATE.items():
        for c in contracts:
            g = facts[(facts["model_id"] == mid) & (facts["contract_name"] == c) & facts["day"].between(*CALIBRATION_DAYS)]
            if len(g) < 10 or (mid, c) not in thr.index:
                continue
            mu, sd = g["eval_value"].mean(), g["eval_value"].std()
            hb = thr.loc[(mid, c), "direction"] == "higher_better"
            amber = mu - max(3 * sd, 0.03) if hb else mu + max(3 * sd, 0.03)
            red = mu - max(4.5 * sd, 0.06) if hb else mu + max(4.5 * sd, 0.06)
            updates.append((mid, c, round(amber, 4), round(red, 4), round(mu, 4), round(sd, 4)))
            thr.loc[(mid, c), ["amber_threshold", "red_threshold"]] = [round(amber, 4), round(red, 4)]
    for mid, c, a, r, mu, sd in updates:
        spark.sql(f"""UPDATE {REF('map_model_metric')} SET amber_threshold = {a}, red_threshold = {r},
                      threshold_basis = 'calibrated: {EVAL_WINDOW[BUCKET[mid]]}-day mean {mu} ± max(3σ,0.03)/max(4.5σ,0.06), σ={sd}, days {CALIBRATION_DAYS[0]}-{CALIBRATION_DAYS[1]}'
                      WHERE model_id = '{mid}' AND contract_name = '{c}'""")
    spark.sql(f"""UPDATE {REF('map_model_metric')} SET threshold_basis = coalesce(threshold_basis, 'catalogue default')
                  WHERE scope IN ('in_scope', 'supporting')""")
    print(f"✓ calibrated {len(updates)} thresholds:")
    print(pd.DataFrame(updates, columns=["model_id", "metric", "amber", "red", "baseline_mean", "sd"]).to_string(index=False))

if MODE == "full":
    for mid, c in sorted(POOLED_KEYS):
        spark.sql(f"""UPDATE {REF('metric_source_map')} SET source_kind = 'hub_pooled',
                      aggregation = 'trailing {POOL_DAYS[mid]}-day pooled window vs baseline, computed in the hub (monitor daily drift kept for drill-down)'
                      WHERE model_id = '{mid}' AND contract_name = '{c}'""")
        spark.sql(f"""UPDATE {REF('map_model_metric')} SET eval_window_days = {POOL_DAYS[mid]}
                      WHERE model_id = '{mid}' AND contract_name = '{c}'""")

# Baseline value and evaluation window, stored for transparency
bases = facts.groupby(["model_id", "contract_name"])["baseline_value"].first()
for (mid, c), b in bases.items():
    if pd.notna(b):
        win = POOL_DAYS[mid] if (mid, c) in POOLED_KEYS else EVAL_WINDOW[BUCKET[mid]]
        spark.sql(f"""UPDATE {REF('map_model_metric')} SET baseline_value = {float(b)}, eval_window_days = {win}
                      WHERE model_id = '{mid}' AND contract_name = '{c}'""")


def status(row):
    if pd.isna(row["eval_value"]):
        return "no_data"
    t = thr.loc[(row["model_id"], row["contract_name"])]
    a, r, v = t["amber_threshold"], t["red_threshold"], row["eval_value"]
    if pd.isna(a):
        return "green"
    if t["direction"] == "higher_better":
        return "red" if v < r else "amber" if v < a else "green"
    return "red" if v > r else "amber" if v > a else "green"


facts = facts[[k in thr.index for k in zip(facts["model_id"], facts["contract_name"])]].copy()
facts["status"] = facts.apply(status, axis=1)
meta = (thr.reset_index()[["model_id", "contract_name", "amber_threshold", "red_threshold", "direction", "threshold_type", "unit", "scope", "bucket"]]
        .merge(dim_metric[["metric_key", "bucket", "contract_name", "display_name", "category"]], on=["bucket", "contract_name"], how="left")
        .merge(dim_model[["model_id", "model_name", "owner_team", "risk_tier", "lane", "source_workspace"]], on="model_id"))
mh = facts.merge(meta, on=["model_id", "contract_name"], how="left")
mh["display_name"] = mh["display_name"].fillna(mh["contract_name"])
mh["category"] = mh["category"].fillna("Operations")
print(f"✓ scored {len(mh):,} metric-days · {mh['status'].value_counts().to_dict()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model health and alert log

# COMMAND ----------

def model_day(g):
    s = g[g["status"] != "no_data"]
    n, na, nr = len(s), int((s["status"] == "amber").sum()), int((s["status"] == "red").sum())
    worst = s.sort_values("status", key=lambda x: x.map({"red": 0, "amber": 1, "green": 2}))
    worst = worst[worst["status"].isin(["red", "amber"])]
    return pd.Series({
        "n_metrics": n, "n_green": n - na - nr, "n_amber": na, "n_red": nr,
        "health_score": round(100 * (1 - (na + 2 * nr) / (2 * n)), 1) if n else np.nan,
        "status": "red" if nr else "amber" if na else "green",
        "red_metrics": ", ".join(worst.loc[worst["status"] == "red", "display_name"]),
        "amber_metrics": ", ".join(worst.loc[worst["status"] == "amber", "display_name"]),
    })


health = mh[mh["scope"] != "descoped"].groupby(["model_id", "metric_date"]).apply(model_day).reset_index()
health = health.merge(dim_model[["model_id", "model_name", "bucket", "owner_team", "risk_tier", "lane", "source_workspace", "current_version"]], on="model_id")

alerts = []
for (mid, c), g in mh.sort_values("metric_date").groupby(["model_id", "contract_name"]):
    prev = "green"
    for r in g.itertuples():
        if r.status == "no_data":
            continue
        rank = {"green": 0, "amber": 1, "red": 2}
        if r.status != prev and (rank[r.status] > rank[prev] or r.status == "green"):
            kind = "resolved" if r.status == "green" else "breach" if prev == "green" else "escalation"
            thr_v = r.red_threshold if r.status == "red" else r.amber_threshold
            alerts.append(dict(alert_id=f"{mid}|{c}|{r.metric_date.date()}", model_id=mid, model_name=r.model_name,
                               owner_team=r.owner_team, contract_name=c, display_name=r.display_name, category=r.category,
                               metric_date=r.metric_date, alert_type=kind, from_status=prev, to_status=r.status,
                               eval_value=float(r.eval_value), threshold=float(thr_v) if pd.notna(thr_v) else None,
                               message=f"{r.model_name}: {r.display_name} {('recovered to green' if kind == 'resolved' else f'went {r.status}')} "
                                       f"({r.eval_value:.4g}{'' if pd.isna(thr_v) else f' vs {thr_v:.4g}'})"))
        prev = r.status
alerts = pd.DataFrame(alerts)
today = health["metric_date"].max()
print(f"✓ {len(health):,} model-days · {len(alerts)} alert events · latest day {today.date()}:")
print(health[health["metric_date"] == today][["model_name", "status", "health_score", "red_metrics", "amber_metrics"]].to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write gold tables

# COMMAND ----------

def write_gold(pdf, table, comment, col_comments=None):
    name = GOLD(table)
    sdf = spark.createDataFrame(pdf)
    for c, t in sdf.dtypes:
        if c == "metric_date":
            sdf = sdf.withColumn(c, sdf[c].cast("date"))
    sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)
    spark.sql(f"COMMENT ON TABLE {name} IS '{comment.replace(chr(39), chr(92) + chr(39))}'")
    for c, txt in (col_comments or {}).items():
        try:
            spark.sql(f"ALTER TABLE {name} ALTER COLUMN {c} COMMENT '{txt.replace(chr(39), chr(92) + chr(39))}'")
        except Exception:
            pass
    print(f"✓ {name}: {spark.table(name).count():,} rows")


mh_out = mh[["metric_date", "model_id", "model_name", "owner_team", "risk_tier", "bucket", "lane", "source_workspace",
             "contract_name", "metric_key", "display_name", "category", "unit", "value", "eval_value", "baseline_value",
             "amber_threshold", "red_threshold", "direction", "threshold_type", "status", "scope", "source", "model_version"]].copy()
mh_out["model_version"] = mh_out["model_version"].astype(str).replace({"None": None, "nan": None})
write_gold(mh_out, "fact_metric_health_daily",
           "One row per model, contract metric and day: raw daily value, evaluated value (rolling mean, or ratio to baseline), thresholds and status (green/amber/red). source = monitor (data profiling outputs), backfill (same definition, computed from the monitored table) or hub_pooled (drift on a trailing 7/14-day pooled window).",
           {"value": "Daily metric value for that day", "eval_value": "Value compared with thresholds: rolling mean (7 days classical, 3 days GenAI/agents/LLM); ratio to baseline for relative metrics",
            "status": "green, amber, red or no_data", "baseline_value": "Median of the first 30 days of history"})
write_gold(health, "fact_model_health_daily",
           "One row per model per day: health score (100 = all metrics green; amber counts 1, red counts 2), worst status, and the metrics in red or amber.",
           {"health_score": "100 x (1 - (amber + 2 x red) / (2 x metrics with data))", "status": "Worst metric status that day"})
if len(alerts):
    write_gold(alerts, "fact_alerts", "Status changes per model metric: breach (green to amber/red), escalation (amber to red) and resolved (back to green).")
sl = pd.concat([s.rename(columns={"country": "slice_value"}).assign(slice_key=s.get("slice_key", "country")) if "country" in s.columns else s
                for s in slices], ignore_index=True)
sl["slice_key"] = sl["slice_key"].fillna("country")
sl = sl.rename(columns={"v": "value"})[["metric_date", "model_id", "contract_name", "slice_key", "slice_value", "value"]].dropna(subset=["value"])
sl = sl.merge(dim_model[["model_id", "model_name"]], on="model_id")
write_gold(sl, "fact_metric_slice_daily",
           "Slice-level detail: per-country values behind the fairness gaps, groundedness by RAG query category, and task success by agent intent.")
spark.sql(f"CREATE OR REPLACE VIEW {GOLD('dim_model')} AS SELECT * FROM {REF('dim_model')}")
spark.sql(f"CREATE OR REPLACE VIEW {GOLD('dim_metric_thresholds')} AS SELECT m.*, d.display_name, d.category FROM {REF('map_model_metric')} m LEFT JOIN {REF('dim_metric')} d ON m.metric_key = d.metric_key")
spark.sql(f"CREATE OR REPLACE VIEW {GOLD('model_version_registry')} AS SELECT * FROM {REF('model_version_registry')}")

if MODE != "full":
    dbutils.notebook.exit(f"Gold refresh complete · latest day {today.date()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Metric views (one semantic layer for the dashboard and Genie)

# COMMAND ----------

MV = {
    "mv_model_health": (GOLD("fact_model_health_daily"), """
dimensions:
  - name: Date
    expr: metric_date
  - name: Model
    expr: model_name
  - name: Bucket
    expr: bucket
  - name: Owner Team
    expr: owner_team
  - name: Risk Tier
    expr: risk_tier
  - name: Status
    expr: status
measures:
  - name: Health Score
    expr: AVG(health_score)
  - name: Red Models
    expr: COUNT(DISTINCT CASE WHEN status = 'red' THEN model_id END)
  - name: Amber Models
    expr: COUNT(DISTINCT CASE WHEN status = 'amber' THEN model_id END)
  - name: Metrics In Breach
    expr: SUM(n_amber + n_red)
"""),
    "mv_metric_health": (GOLD("fact_metric_health_daily"), """
dimensions:
  - name: Date
    expr: metric_date
  - name: Model
    expr: model_name
  - name: Owner Team
    expr: owner_team
  - name: Metric
    expr: display_name
  - name: Category
    expr: category
  - name: Status
    expr: status
  - name: Source
    expr: source
measures:
  - name: Value
    expr: AVG(value)
  - name: Evaluated Value
    expr: AVG(eval_value)
  - name: Breaches
    expr: COUNT_IF(status IN ('amber', 'red'))
  - name: Red Breaches
    expr: COUNT_IF(status = 'red')
"""),
}
mv_kind = {}
for name, (src, body) in MV.items():
    yaml = f"version: 0.1\nsource: {src}\n{body}"
    try:
        spark.sql(f"CREATE OR REPLACE VIEW {GOLD(name)} WITH METRICS LANGUAGE YAML AS $${yaml}$$")
        mv_kind[name] = "metric view"
    except Exception as e:
        spark.sql(f"CREATE OR REPLACE VIEW {GOLD(name)} AS SELECT * FROM {src}")
        mv_kind[name] = "plain view (metric views unavailable)"
        print(f"  {name}: metric view not created ({str(e)[:150]})")
    print(f"✓ {GOLD(name)} · {mv_kind[name]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL warehouse

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
ME = spark.sql("SELECT current_user()").collect()[0][0]
if WAREHOUSE_ID is None:
    whs = w.api_client.do("GET", "/api/2.0/sql/warehouses").get("warehouses", [])
    serverless = [x for x in whs if x.get("enable_serverless_compute")]
    if serverless:
        WAREHOUSE_ID = sorted(serverless, key=lambda x: x.get("state") != "RUNNING")[0]["id"]
    else:
        WAREHOUSE_ID = w.api_client.do("POST", "/api/2.0/sql/warehouses", body={
            "name": "umm-demo-wh", "cluster_size": "2X-Small", "enable_serverless_compute": True,
            "warehouse_type": "PRO", "auto_stop_mins": 10, "max_num_clusters": 1})["id"]
print(f"Warehouse {WAREHOUSE_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## AI/BI dashboard: portfolio → governance → classical ML → GenAI & agents → metric explorer

# COMMAND ----------

H, M, A, S = GOLD("fact_model_health_daily"), GOLD("fact_metric_health_daily"), GOLD("fact_alerts"), GOLD("fact_metric_slice_daily")
DATASETS = {
    "today": f"SELECT model_name, owner_team, bucket, risk_tier, status, health_score, red_metrics, amber_metrics FROM {H} WHERE metric_date = (SELECT max(metric_date) FROM {H}) ORDER BY health_score",
    "kpis": f"SELECT count(*) AS models, sum(CASE WHEN status='red' THEN 1 ELSE 0 END) AS red, sum(CASE WHEN status='amber' THEN 1 ELSE 0 END) AS amber, sum(CASE WHEN status='green' THEN 1 ELSE 0 END) AS green, round(avg(health_score),1) AS avg_health FROM {H} WHERE metric_date = (SELECT max(metric_date) FROM {H})",
    "health_trend": f"SELECT metric_date, model_name, health_score FROM {H}",
    "inventory": f"""SELECT d.model_name, d.owner_team, d.risk_tier, d.bucket, d.lane, d.current_version, d.source_workspace,
                    max(h.metric_date) AS last_monitored, sum(h.n_red) AS red_metric_days_30d, sum(h.n_amber) AS amber_metric_days_30d
                    FROM {GOLD('dim_model')} d LEFT JOIN {H} h ON d.model_id = h.model_id AND h.metric_date >= date_sub(current_date(), 30)
                    GROUP BY ALL ORDER BY red_metric_days_30d DESC""",
    "alerts": f"SELECT metric_date, model_name, owner_team, display_name AS metric, alert_type, to_status, round(eval_value,4) AS value, round(threshold,4) AS threshold, message FROM {A} ORDER BY metric_date DESC LIMIT 200",
    "breach_by_category": f"SELECT category, model_name, count(*) AS breach_days FROM {M} WHERE status IN ('amber','red') AND metric_date >= date_sub(current_date(), 30) GROUP BY ALL",
    "pd_perf": f"SELECT metric_date, display_name AS metric, eval_value FROM {M} WHERE model_id='pd_counterparty_default' AND contract_name IN ('accuracy','precision','recall','f1_macro')",
    "pd_drift": f"SELECT metric_date, display_name AS metric, value FROM {M} WHERE model_id='pd_counterparty_default' AND contract_name IN ('data_drift_psi','data_drift_js','label_drift_js','concept_drift_score')",
    "pd_fair": f"SELECT metric_date, slice_value AS country, value AS approval_rate FROM {S} WHERE model_id='pd_counterparty_default' AND contract_name='approval_rate_gap'",
    "fc_err": f"SELECT metric_date, display_name AS metric, value FROM {M} WHERE model_id='trade_volume_forecast' AND contract_name IN ('mae','rmse')",
    "rag_quality": f"SELECT metric_date, display_name AS metric, eval_value FROM {M} WHERE model_id='credit_policy_rag' AND contract_name IN ('groundedness','context_relevance','response_relevance','precision_at_k','recall_at_k')",
    "safety": f"SELECT metric_date, model_name, eval_value AS safety_violation_rate FROM {M} WHERE contract_name='safety_violation_rate'",
    "agent_gov": f"SELECT metric_date, display_name AS metric, eval_value FROM {M} WHERE model_id='rm_client_brief_agent' AND contract_name IN ('permission_compliance_rate','approval_adherence_rate','recovery_success_rate','escalation_appropriateness','tool_accuracy')",
    "cost": f"SELECT metric_date, model_name, value AS usd FROM {M} WHERE contract_name IN ('cost_per_request','cost_per_successful_query')",
    "explorer": f"SELECT model_name, display_name AS metric, category, round(value,4) AS value, round(eval_value,4) AS evaluated, round(amber_threshold,4) AS amber, round(red_threshold,4) AS red, status, source FROM {M} WHERE metric_date = (SELECT max(metric_date) FROM {M}) ORDER BY CASE status WHEN 'red' THEN 0 WHEN 'amber' THEN 1 ELSE 2 END, model_name",
}


def fields(cols):
    return [{"name": c, "expression": f"`{c}`"} for c in cols]


def widget(name, ds, cols, spec, x, y, wdt, hgt):
    return {"widget": {"name": name, "queries": [{"name": "main_query", "query": {"datasetName": ds, "fields": fields(cols), "disaggregated": True}}],
                       "spec": spec}, "position": {"x": x, "y": y, "width": wdt, "height": hgt}}


def frame(title):
    return {"showTitle": True, "title": title}


def line(name, ds, x, yv, color, title, pos):
    return widget(name, ds, [x, yv, color], {"version": 3, "widgetType": "line", "frame": frame(title), "encodings": {
        "x": {"fieldName": x, "scale": {"type": "temporal"}, "displayName": "Date"},
        "y": {"fieldName": yv, "scale": {"type": "quantitative"}, "displayName": yv.replace("_", " ")},
        "color": {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color.replace("_", " ")}}}, *pos)


def bar(name, ds, x, yv, color, title, pos):
    return widget(name, ds, [x, yv, color], {"version": 3, "widgetType": "bar", "frame": frame(title), "encodings": {
        "x": {"fieldName": x, "scale": {"type": "categorical"}, "displayName": x.replace("_", " ")},
        "y": {"fieldName": yv, "scale": {"type": "quantitative"}, "displayName": yv.replace("_", " ")},
        "color": {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color.replace("_", " ")}}}, *pos)


def counter(name, ds, col, title, pos):
    return widget(name, ds, [col], {"version": 2, "widgetType": "counter", "frame": frame(title),
                                     "encodings": {"value": {"fieldName": col, "displayName": title}}}, *pos)


def table(name, ds, cols, title, pos):
    return widget(name, ds, cols, {"version": 1, "widgetType": "table", "frame": frame(title), "encodings": {
        "columns": [{"fieldName": c, "displayName": c.replace("_", " "), "order": i} for i, c in enumerate(cols)]}}, *pos)


def text(name, md, pos):
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": [md]}}, "position": dict(zip(["x", "y", "width", "height"], pos))}


TODAY_COLS = ["model_name", "owner_team", "bucket", "risk_tier", "status", "health_score", "red_metrics", "amber_metrics"]
pages = [
    ("portfolio", "Portfolio overview", [
        text("t1", "## One place for every CIB model\nHealth across classical ML, RAG, agents and the shared LLM endpoint. Green = all metrics within thresholds; amber/red = at least one breach.", (0, 0, 6, 2)),
        counter("k1", "kpis", "models", "Models monitored", (0, 2, 1, 3)), counter("k2", "kpis", "red", "Red", (1, 2, 1, 3)),
        counter("k3", "kpis", "amber", "Amber", (2, 2, 1, 3)), counter("k4", "kpis", "green", "Green", (3, 2, 1, 3)),
        counter("k5", "kpis", "avg_health", "Average health score", (4, 2, 2, 3)),
        table("tt", "today", TODAY_COLS, "Model status today", (0, 5, 6, 6)),
        line("ht", "health_trend", "metric_date", "health_score", "model_name", "Health score, last 90 days", (0, 11, 6, 7))]),
    ("governance", "Governance & model risk", [
        text("t2", "## Ongoing monitoring evidence\nInventory, ownership and breach history per model, with every status change logged.", (0, 0, 6, 2)),
        table("inv", "inventory", ["model_name", "owner_team", "risk_tier", "bucket", "lane", "current_version", "source_workspace", "last_monitored", "red_metric_days_30d", "amber_metric_days_30d"], "Model inventory (last 30 days)", (0, 2, 6, 6)),
        bar("bc", "breach_by_category", "category", "breach_days", "model_name", "Breach days by category (30 days)", (0, 8, 3, 6)),
        table("al", "alerts", ["metric_date", "model_name", "owner_team", "metric", "alert_type", "to_status", "value", "threshold", "message"], "Alert log", (3, 8, 3, 6))]),
    ("classical", "Classical ML", [
        line("p1", "pd_perf", "metric_date", "eval_value", "metric", "PD model · performance (7-day mean)", (0, 0, 3, 6)),
        line("p2", "pd_drift", "metric_date", "value", "metric", "PD model · drift vs baseline", (3, 0, 3, 6)),
        line("p3", "pd_fair", "metric_date", "approval_rate", "country", "PD model · approval rate by country", (0, 6, 3, 6)),
        line("f1", "fc_err", "metric_date", "value", "metric", "Trade volume forecast · error (USD m)", (3, 6, 3, 6))]),
    ("genai", "GenAI & agents", [
        line("g1", "rag_quality", "metric_date", "eval_value", "metric", "Credit policy RAG · retrieval and answer quality", (0, 0, 3, 6)),
        line("g2", "safety", "metric_date", "safety_violation_rate", "model_name", "Safety violation rate", (3, 0, 3, 6)),
        line("g3", "agent_gov", "metric_date", "eval_value", "metric", "Client brief agent · controls", (0, 6, 3, 6)),
        line("g4", "cost", "metric_date", "usd", "model_name", "Cost per request / successful query (USD)", (3, 6, 3, 6))]),
    ("explorer", "Metric explorer", [
        table("ex", "explorer", ["model_name", "metric", "category", "value", "evaluated", "amber", "red", "status", "source"], "Every contract metric, latest day", (0, 0, 6, 12))]),
]
TRIAGE_T, TRIAGE_KPI = GOLD("fact_alert_triage"), GOLD("fact_triage_kpis")
if spark.catalog.tableExists(TRIAGE_T) and spark.catalog.tableExists(TRIAGE_KPI):
    DATASETS["triage_kpi"] = f"SELECT owner_team, open_alerts, open_p1, unacknowledged, mtta_hours, mttr_hours, false_positive_rate FROM {TRIAGE_KPI}"
    DATASETS["triage_open"] = f"SELECT priority, model_name, display_name AS metric, triage_status, assignee, metric_date, message FROM {TRIAGE_T} WHERE triage_status IN ('open','acknowledged','investigating') ORDER BY priority, metric_date DESC"
    DATASETS["memory"] = f"SELECT metric_date, display_name AS metric, eval_value FROM {M} WHERE model_id='rm_client_brief_agent' AND contract_name LIKE '%memory%'"
    pages.insert(2, ("ops", "Triage & agent memory", [
        text("t3", "## Alert triage (Lakebase) and agent memory\nTriage state is written by the app to Lakebase and synced here nightly. Memory metrics cover recall, grounding, conflicts, leakage and PII held in memory.", (0, 0, 6, 2)),
        table("tk", "triage_kpi", ["owner_team", "open_alerts", "open_p1", "unacknowledged", "mtta_hours", "mttr_hours", "false_positive_rate"], "Triage KPIs by owner team", (0, 2, 6, 4)),
        table("to", "triage_open", ["priority", "model_name", "metric", "triage_status", "assignee", "metric_date", "message"], "Open alerts", (0, 6, 6, 6)),
        line("mm", "memory", "metric_date", "eval_value", "metric", "Client brief agent · memory metrics", (0, 12, 6, 6))]))
dashboard = {
    "datasets": [{"name": k, "displayName": k.replace("_", " "), "queryLines": [v]} for k, v in DATASETS.items()],
    "pages": [{"name": n, "displayName": d, "layout": l, "pageType": "PAGE_TYPE_CANVAS"} for n, d, l in pages],
}

dash_id = None
existing = [d for d in w.api_client.do("GET", "/api/2.0/lakeview/dashboards", query={"page_size": 200}).get("dashboards", [])
            if d.get("display_name") == DASHBOARD_NAME]
body = {"display_name": DASHBOARD_NAME, "warehouse_id": WAREHOUSE_ID, "serialized_dashboard": json.dumps(dashboard),
        "parent_path": ASSETS_ROOT}
try:
    if existing:
        dash_id = existing[0]["dashboard_id"]
        w.api_client.do("PATCH", f"/api/2.0/lakeview/dashboards/{dash_id}",
                        body={k: v for k, v in body.items() if k != "parent_path"})
    else:
        dash_id = w.api_client.do("POST", "/api/2.0/lakeview/dashboards", body=body)["dashboard_id"]
    w.api_client.do("POST", f"/api/2.0/lakeview/dashboards/{dash_id}/published",
                    body={"warehouse_id": WAREHOUSE_ID, "embed_credentials": True})
    host = w.config.host.rstrip("/")
    print(f"✓ dashboard published: {host}/dashboardsv3/{dash_id}/published")
except Exception as e:
    print(f"⚠ dashboard API failed ({str(e)[:300]})")
with open("/tmp/umm_dashboard.lvdash.json", "w") as f:
    json.dump(dashboard, f)
try:
    import base64
    w.workspace.upload(f"{ASSETS_ROOT}/UMM_dashboard_definition.lvdash.json", open("/tmp/umm_dashboard.lvdash.json", "rb"),
                       format=__import__("databricks.sdk.service.workspace", fromlist=["ImportFormat"]).ImportFormat.AUTO, overwrite=True)
    print(f"  definition also saved to {ASSETS_ROOT}/UMM_dashboard_definition.lvdash.json")
except Exception:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL alerts (evaluated by the hub job after each gold refresh)

# COMMAND ----------

ALERTS = {
    "UMM · models in red today": (f"SELECT count(*) AS red_models, concat_ws('; ', collect_list(concat(model_name, ': ', red_metrics))) AS detail FROM {H} WHERE metric_date = (SELECT max(metric_date) FROM {H}) AND status = 'red'", "red_models"),
    "UMM · new breaches today": (f"SELECT count(*) AS new_breaches, concat_ws('; ', collect_list(message)) AS detail FROM {A} WHERE metric_date = (SELECT max(metric_date) FROM {H}) AND alert_type IN ('breach', 'escalation')", "new_breaches"),
}
alert_ids = {}
try:
    _existing_alerts = {a.get("display_name"): a.get("id") for a in w.api_client.do("GET", "/api/2.0/sql/alerts", query={"page_size": 200}).get("results", [])}
except Exception:
    _existing_alerts = {}
for name, (sql, col) in ALERTS.items():
    if name in _existing_alerts:
        alert_ids[name] = _existing_alerts[name]
        print(f"• alert exists: {name}")
        continue
    try:
        q = w.api_client.do("POST", "/api/2.0/sql/queries", body={"query": {
            "display_name": name, "warehouse_id": WAREHOUSE_ID, "query_text": sql, "parent_path": ASSETS_ROOT}})
        a = w.api_client.do("POST", "/api/2.0/sql/alerts", body={"alert": {
            "display_name": name, "query_id": q["id"], "parent_path": ASSETS_ROOT, "seconds_to_retrigger": 0,
            "condition": {"op": "GREATER_THAN", "operand": {"column": {"name": col}}, "threshold": {"value": {"double_value": 0}}}}})
        alert_ids[name] = a["id"]
        print(f"✓ alert: {name}")
    except Exception as e:
        print(f"⚠ alert '{name}' not created ({str(e)[:200]}). Create it in SQL → Alerts with this query:\n{sql}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Genie space
# MAGIC
# MAGIC Built on the gold tables and metric views, with instructions and sample questions. The presenter's `scenario_calendar` is deliberately left out.

# COMMAND ----------

GENIE_EXTRA = [t for t in (GOLD("fact_alert_triage"), GOLD("fact_triage_kpis")) if spark.catalog.tableExists(t)]
GENIE_TABLES = GENIE_EXTRA + [GOLD("fact_model_health_daily"), GOLD("fact_metric_health_daily"), GOLD("fact_alerts"),
                GOLD("fact_metric_slice_daily"), GOLD("dim_model"), GOLD("dim_metric_thresholds"), GOLD("model_version_registry"),
                GOLD("mv_model_health"), GOLD("mv_metric_health")]
INSTRUCTIONS = f"""You answer questions about the health of Standard Chartered CIB's machine learning and AI models, monitored in one hub.
Models: Counterparty Default PD (classification, Tier 1), Trade Volume Forecast (regression), Credit Policy Assistant (RAG), RM Client Brief Agent (tool-calling agent), and the Shared LLM Endpoint behind the last two.
- "Status" is green, amber or red per metric per day. A model's status is its worst metric. Health score 100 means all metrics are green; amber counts 1 and red counts 2.
- Use fact_model_health_daily for model-level questions, fact_metric_health_daily for metric-level questions, fact_alerts for "when did X start/stop", and fact_metric_slice_daily for country, query category or agent intent breakdowns.
- "Today" or "now" means the latest metric_date in the table, not current_date().
- eval_value is what's compared with thresholds (rolling mean, or ratio to baseline for relative metrics); value is the raw daily figure. Quote both when explaining a breach.
- Owner teams: CIB Credit Risk Analytics, CIB Markets Analytics, AI Factory. "AI Factory models" means owner_team = 'AI Factory'.
- Fairness questions about the PD model use approval_rate_gap and quality_gap, with country detail in fact_metric_slice_daily.
- When asked "why", list the metrics in red_metrics / amber_metrics and give each metric's value against its threshold.
- Triage questions ("who is working on", "what's unacknowledged", "time to resolve") use fact_alert_triage and fact_triage_kpis. Priorities: P1 = red on a Tier 1 model, P2 = other red, P3 = amber.
- Agent memory metrics (memory_recall_rate, memory_grounded_rate, memory_conflict_rate, memory_leakage_rate, memory_pii_disclosure_rate, pii_in_memory_rate, stale_memory_share) belong to the RM Client Brief Agent."""
SAMPLES = ["Which models are red or amber today, and why?", "When did the PD model's accuracy first go red?",
           "Show groundedness for the credit policy assistant over the last 30 days",
           "Which metrics breached for AI Factory models this week?",
           "Which country has the lowest approval rate for the PD model this month?",
           "How has the agent's cost per request changed versus its baseline?",
           "List every alert for the client brief agent in the last 30 days",
           "Which P1 alerts are still unacknowledged, and who owns them?",
           "Is the client brief agent's memory holding any personal data?"]

space_id = None
try:
    _prev = spark.table(REF("hub_runtime_config")).collect()[0].asDict().get("genie_space_id") or None
except Exception:
    _prev = None
serialized = {"version": 1,
              "config": {"sample_questions": [{"id": uuid.uuid4().hex, "question": [q]} for q in SAMPLES]},
              "data_sources": {"tables": [{"identifier": t} for t in sorted(GENIE_TABLES)]},
              "instructions": {"text_instructions": [{"id": uuid.uuid4().hex, "content": [INSTRUCTIONS]}]}}
if _prev:
    try:
        w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{_prev}", body={"serialized_space": json.dumps(serialized), "title": GENIE_TITLE})
        space_id = _prev
        print(f"✓ Genie space updated: {w.config.host.rstrip('/')}/genie/rooms/{space_id}")
    except Exception as e:
        space_id = _prev
        print(f"⚠ Couldn't update the existing Genie space ({str(e)[:150]}). Add these tables to it manually: {GENIE_EXTRA}")
for attempt, payload in enumerate([] if space_id else [
        {"title": GENIE_TITLE, "description": "Ask about model health across the CIB portfolio.", "warehouse_id": WAREHOUSE_ID,
         "parent_path": ASSETS_ROOT, "serialized_space": json.dumps(serialized)},
        {"title": GENIE_TITLE, "description": "Ask about model health across the CIB portfolio.", "warehouse_id": WAREHOUSE_ID,
         "table_identifiers": GENIE_TABLES}]):
    try:
        res = w.api_client.do("POST", "/api/2.0/genie/spaces", body=payload)
        space_id = res.get("space_id") or res.get("id")
        print(f"✓ Genie space: {w.config.host.rstrip('/')}/genie/rooms/{space_id}")
        break
    except Exception as e:
        last_err = str(e)[:250]
if space_id is None:
    print(f"⚠ Genie API not available here ({last_err}). Create it in 2 minutes: Genie → New → add these tables:\n  " +
          "\n  ".join(GENIE_TABLES) + "\nthen paste the instructions and sample questions printed below.")
with open("/tmp/genie_instructions.md", "w") as f:
    f.write("# Instructions\n\n" + INSTRUCTIONS + "\n\n# Sample questions\n\n" + "\n".join(f"- {q}" for q in SAMPLES))
print(open("/tmp/genie_instructions.md").read() if space_id is None else "")
spark.createDataFrame(pd.DataFrame([dict(warehouse_id=WAREHOUSE_ID, dashboard_id=dash_id or "", genie_space_id=space_id or "",
                                         alert_ids=json.dumps(alert_ids))])).write.mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(REF("hub_runtime_config"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hub refresh job (03:00 UTC): gold refresh → alerts

# COMMAND ----------

hub_job = None
try:
    here = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    tasks = [{"task_key": "gold_refresh", "notebook_task": {"notebook_path": here, "source": "WORKSPACE",
                                                            "base_parameters": {"mode": "refresh"}}, "timeout_seconds": 3600}]
    for i, (name, aid) in enumerate(alert_ids.items()):
        tasks.append({"task_key": f"alert_{i + 1}", "depends_on": [{"task_key": "gold_refresh"}],
                      "sql_task": {"warehouse_id": WAREHOUSE_ID, "alert": {"alert_id": aid, "subscriptions": [{"user_name": ME}]}}})
    settings = {"name": HUB_JOB_NAME, "max_concurrent_runs": 1, "tags": {"project": "umm_demo"},
                "schedule": {"quartz_cron_expression": HUB_JOB_CRON, "timezone_id": "UTC", "pause_status": "UNPAUSED"},
                "email_notifications": {"on_failure": [ME]}, "tasks": tasks}
    ex = w.api_client.do("GET", "/api/2.1/jobs/list", query={"name": HUB_JOB_NAME}).get("jobs", [])
    if ex:
        hub_job = ex[0]["job_id"]
        w.api_client.do("POST", "/api/2.1/jobs/reset", body={"job_id": hub_job, "new_settings": settings})
    else:
        hub_job = w.api_client.do("POST", "/api/2.1/jobs/create", body=settings)["job_id"]
    print(f"✓ hub job {HUB_JOB_NAME} ({hub_job}) · {len(tasks)} tasks")
except Exception as e:
    print(f"⚠ hub job not created ({str(e)[:250]})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate: scenarios light up when they should, and stay quiet otherwise

# COMMAND ----------

def status_on(mid, c, day):
    r = mh[(mh["model_id"] == mid) & (mh["contract_name"] == c) & (mh["day"] == day)]
    return r["status"].iloc[0] if len(r) else "no_data"


def any_breach(mid, c, lo, hi):
    r = mh[(mh["model_id"] == mid) & (mh["contract_name"] == c) & mh["day"].between(lo, hi)]
    return r["status"].isin(["amber", "red"]).any()


checks = [
    ("S1 PD data drift (PSI) breaches", any_breach(PD, "data_drift_psi", 66, 89)),
    ("S1/S2 PD accuracy breaches", any_breach(PD, "accuracy", 70, 89)),
    ("S1 PD positive rate breaches", any_breach(PD, "positive_rate_at_threshold", 70, 89)),
    ("S3 forecast MAE breaches", any_breach(FC, "mae", 74, 82)),
    ("S4 RAG safety breaches", any_breach(RAG, "safety_violation_rate", 55, 58)),
    ("S5 RAG groundedness breaches", any_breach(RAG, "groundedness", 75, 81)),
    ("S7 agent recovery breaches", any_breach(AGT, "recovery_success_rate", 62, 66)),
    ("S8 agent permission compliance breaches", any_breach(AGT, "permission_compliance_rate", 70, 73)),
    ("S9 agent approval adherence breaches", any_breach(AGT, "approval_adherence_rate", 84, 87)),
    ("S6 agent token creep breaches", any_breach(AGT, "avg_tokens_per_request", 82, 89)),
]
quiet = mh[mh["day"].between(*CALIBRATION_DAYS) & (mh["scope"] == "in_scope")]
fa = float(quiet["status"].isin(["amber", "red"]).mean()) if len(quiet) else 0.0
checks.append((f"False-alarm rate in quiet days {CALIBRATION_DAYS} = {fa:.1%}", fa < 0.05))
n_scored = mh[mh["scope"] == "in_scope"][["model_id", "contract_name"]].drop_duplicates().shape[0]
checks.append((f"Every in-scope metric scored ({n_scored})", n_scored >= 43))
if consistency is not None:
    checks.append((f"Backfill matches monitor on overlap ({consistency:.0%} within 2%)", consistency >= 0.8))
checks += [("Dashboard published", dash_id is not None), ("Genie space created", space_id is not None),
           ("Alerts created", len(alert_ids) == len(ALERTS)), ("Hub job created", hub_job is not None)]
report = pd.DataFrame([(n, "PASS" if ok else "FAIL") for n, ok in checks], columns=["check", "result"])
display(report)
hard = report[(report["result"] == "FAIL") & ~report["check"].str.match("Dashboard|Genie|Alerts|Hub job")]
if len(hard):
    raise AssertionError("Failed: " + ", ".join(hard["check"]))
print("Phase 5 complete." + ("" if report["result"].eq("PASS").all() else " Some serving steps need a manual touch; see the ⚠ messages above."))
