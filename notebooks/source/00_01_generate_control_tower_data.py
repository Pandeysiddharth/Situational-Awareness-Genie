# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Unified Model Monitoring Control Tower: synthetic hub data generator
# MAGIC
# MAGIC Builds the **hub** tables for the demo. Everything here is synthetic, there is no customer data.
# MAGIC
# MAGIC The notebook simulates what each spoke workspace would send. Candidate rows pass through an **export gate**
# MAGIC (the same logic the real exporter job would run): non-allowlisted metrics and non-contract columns are rejected,
# MAGIC and small slices are suppressed. The Data Boundary Ledger tables are built from what the gate actually did.
# MAGIC
# MAGIC **Planted stories (so the demo has something to find)**
# MAGIC 1. `aml_alert_scorer`: gradual drift over about 25 days, driven by the `country = SG` slice. Accuracy labels have not arrived yet.
# MAGIC 2. `payments_gateway_feed` outage: volume collapses in the last 3 days for `txn_fraud_classifier` and `sanctions_name_match` (UK) and partly for `cashflow_forecast` (India).
# MAGIC 3. `corporate_pd`: silent accuracy decay with no input drift, only visible once labels arrive (30 day lag).
# MAGIC 4. Silent spoke: workspace `cib-markets-prod-sea` stopped sending metrics 3 days ago (serving cost still flows from system tables).
# MAGIC 5. Healthy majority plus a few resolved historical breaches.
# MAGIC
# MAGIC Re-run before each demo so the dates roll forward.

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta, time

try:
    dbutils.widgets.text("catalog", "main", "Catalog")
    dbutils.widgets.text("schema", "model_monitoring", "Schema")
    dbutils.widgets.text("days", "90", "Days of history")
    dbutils.widgets.text("seed", "42", "Random seed")
    CATALOG = dbutils.widgets.get("catalog")
    SCHEMA = dbutils.widgets.get("schema")
    DAYS = int(dbutils.widgets.get("days"))
    SEED = int(dbutils.widgets.get("seed"))
except NameError:  # running outside Databricks (local test)
    CATALOG, SCHEMA, DAYS, SEED = "main", "model_monitoring", 90, 42

rng = np.random.default_rng(SEED)
END_DATE = date.today() - timedelta(days=1)          # latest metric window = yesterday
DAYS_LIST = [END_DATE - timedelta(days=k) for k in range(DAYS - 1, -1, -1)]
MIN_GROUP_SIZE = 100                                  # slices smaller than this are never exported
STALE_AFTER_HOURS = 30
print(f"Target: {CATALOG}.{SCHEMA} | window dates {DAYS_LIST[0]} to {DAYS_LIST[-1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference data: workspaces, models, metric allowlist

# COMMAND ----------

WS = {
    "cib-fcc-prod-uks":      {"region": "UK South",       "transport": "Option 1 - Shared catalog"},
    "cib-credit-prod-uks":   {"region": "UK South",       "transport": "Option 1 - Shared catalog"},
    "cib-markets-prod-sea":  {"region": "Southeast Asia", "transport": "Option 2a - API push"},
    "cib-txnbank-prod-cin":  {"region": "Central India",  "transport": "Option 2a - API push"},
}
SILENT_WS = {"cib-markets-prod-sea"}      # story 4: no metrics for days_ago 0, 1, 2
OUTAGE = {                                  # story 2: name -> (volume factor, freshness hours)
    "txn_fraud_classifier": (0.05, 28.0),
    "sanctions_name_match": (0.05, 29.0),
    "cashflow_forecast":    (0.60, 20.5),
}

def model(name, ver, ws, stream, serving, owner, upstream, tier, rows, lag=None,
          auc=None, auc_thr=None, f1=None, f1_thr=0.5, pos=None, lat=None, slices=False, null=1.0, onboard=300):
    return dict(name=name, ver=ver, ws=ws, stream=stream, serving=serving, owner=owner, upstream=upstream,
                tier=tier, rows=rows, lag=lag, auc=auc, auc_thr=auc_thr, f1=f1, f1_thr=f1_thr, pos=pos,
                lat=lat, slices=slices, null=null, onboard=onboard)

MODELS = [
    model("aml_alert_scorer", "v7", "cib-fcc-prod-uks", "Financial Crime Compliance", "realtime", "Priya Nair",
          "transaction_monitoring_feed", "High", 180_000, lag=14, auc=0.88, auc_thr=0.80, f1=0.62, pos=0.04, lat=120, slices=True, onboard=520),
    model("txn_fraud_classifier", "v4", "cib-fcc-prod-uks", "Financial Crime Compliance", "realtime", "Marcus Webb",
          "payments_gateway_feed", "High", 2_400_000, lag=7, auc=0.96, auc_thr=0.90, f1=0.78, pos=0.006, lat=45, slices=True, onboard=410),
    model("sanctions_name_match", "v3", "cib-fcc-prod-uks", "Financial Crime Compliance", "realtime", "Elena Petrova",
          "payments_gateway_feed", "High", 650_000, lag=21, auc=0.94, auc_thr=0.88, f1=0.71, pos=0.012, lat=70, onboard=365),
    model("kyc_risk_rating", "v5", "cib-fcc-prod-uks", "Financial Crime Compliance", "batch", "Priya Nair",
          "kyc_customer_master_feed", "Medium", 90_000, lag=30, auc=0.85, auc_thr=0.78, f1=0.66, pos=0.15, slices=True, onboard=600),
    model("corporate_pd", "v12", "cib-credit-prod-uks", "Credit Risk", "batch", "Hannah Clarke",
          "obligor_financials_feed", "High", 42_000, lag=30, auc=0.82, auc_thr=0.75, f1=0.60, pos=0.035, onboard=700),
    model("lgd_model", "v6", "cib-credit-prod-uks", "Credit Risk", "batch", "Hannah Clarke",
          "collateral_valuation_feed", "Medium", 15_000, onboard=480),
    model("early_warning_watchlist", "v3", "cib-credit-prod-uks", "Credit Risk", "batch", "Tomas Ruiz",
          "obligor_financials_feed", "Medium", 38_000, lag=60, auc=0.79, auc_thr=0.70, f1=0.60, f1_thr=0.45, pos=0.08, onboard=250),
    model("trade_surveillance_anomaly", "v2", "cib-markets-prod-sea", "Markets", "batch", "Wei Lim",
          "trade_blotter_feed", "High", 520_000, pos=0.01, onboard=200),
    model("client_propensity", "v4", "cib-markets-prod-sea", "Markets", "batch", "Aisha Khan",
          "client_interaction_feed", "Low", 120_000, lag=7, auc=0.74, auc_thr=0.65, f1=0.42, f1_thr=0.35, pos=0.09, slices=True, onboard=330),
    model("fx_flow_forecast", "v3", "cib-markets-prod-sea", "Markets", "batch", "Wei Lim",
          "market_data_feed", "Medium", 8_000, onboard=290),
    model("cashflow_forecast", "v2", "cib-txnbank-prod-cin", "Transaction Banking", "batch", "Rohan Mehta",
          "payments_gateway_feed", "Medium", 300_000, onboard=180),
    model("trade_finance_risk", "v3", "cib-txnbank-prod-cin", "Transaction Banking", "batch", "Rohan Mehta",
          "trade_docs_feed", "High", 25_000, lag=30, auc=0.83, auc_thr=0.76, f1=0.60, pos=0.05, onboard=260),
]

# metric_name: (description, category, unit, default_threshold, breach_direction)
ALLOW = {
    "row_count":           ("Predictions scored in the window", "Volume", "rows", None, "BELOW"),
    "null_pct":            ("Share of null values in model inputs", "Data quality", "percent", 5.0, "ABOVE"),
    "feature_psi_max":     ("Largest population stability index across input features", "Drift", "psi", 0.20, "ABOVE"),
    "prediction_drift_js": ("Jensen-Shannon divergence of predictions vs baseline", "Drift", "js_divergence", 0.15, "ABOVE"),
    "positive_rate":       ("Share of predictions flagged positive", "Drift", "ratio", None, "ABOVE"),
    "freshness_hours":     ("Hours since the last prediction was written", "Freshness", "hours", 24.0, "ABOVE"),
    "latency_p95_ms":      ("95th percentile serving latency (real-time models)", "Performance", "ms", 500.0, "ABOVE"),
    "error_rate_pct":      ("Percent of serving requests that failed (real-time models)", "Performance", "percent", 1.0, "ABOVE"),
    "auc":                 ("Area under the ROC curve, available once labels arrive", "Accuracy", "score", None, "BELOW"),
    "f1":                  ("F1 score, available once labels arrive", "Accuracy", "score", 0.5, "BELOW"),
    "precision":           ("Precision, available once labels arrive", "Accuracy", "score", 0.5, "BELOW"),
    "recall":              ("Recall, available once labels arrive", "Accuracy", "score", 0.5, "BELOW"),
}
DIRECTION = {k: v[4] for k, v in ALLOW.items()}
ACCURACY_METRICS = ("auc", "f1", "precision", "recall")
CONTRACT_COLS = ["model_name", "model_version", "workspace", "region", "workstream", "window_start", "window_end",
                 "slice_name", "slice_value", "metric_name", "metric_value", "threshold", "status"]
SLICE_SHARES = {"SG": 0.22, "HK": 0.20, "GB": 0.30, "IN": 0.18, "AE": 0.10}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers: thresholds, status, row emission, and the export gate

# COMMAND ----------

def clip01(x):
    return max(0.0, min(1.0, float(x)))

def thr_for(m, metric, share=1.0):
    if metric == "row_count":
        return 0.5 * m["rows"] * share
    if metric == "positive_rate":
        return 1.6 * m["pos"]
    if metric == "auc":
        return m["auc_thr"]
    if metric in ("f1", "precision", "recall"):
        return m["f1_thr"]
    return ALLOW[metric][3]

def status_of(metric, value, thr):
    if thr is None:
        return "OK"
    if DIRECTION[metric] == "ABOVE":
        if value > thr: return "BREACH"
        if value > 0.8 * thr: return "WARN"
    else:
        if value < thr: return "BREACH"
        if value < thr * (1.4 if metric == "row_count" else 1.05): return "WARN"
    return "OK"

def emit(rows, m, d, metric, value, thr, slice_name="overall", slice_value="all", slice_n=None, share=1.0):
    value = float(value)
    rows.append(dict(
        model_name=m["name"], model_version=m["ver"], workspace=m["ws"], region=WS[m["ws"]]["region"],
        workstream=m["stream"], window_start=datetime.combine(d, time.min),
        window_end=datetime.combine(d + timedelta(days=1), time.min),
        slice_name=slice_name, slice_value=slice_value, metric_name=metric,
        metric_value=round(value, 5), threshold=None if thr is None else round(float(thr), 5),
        status=status_of(metric, value, thr),
        slice_n=10**9 if slice_n is None else int(slice_n)))   # slice_n is internal, never exported

EXTRA_COLUMN_REASONS = {
    "customer_id":    "Potential identifier, not in the metrics contract",
    "account_number": "Potential identifier, not in the metrics contract",
    "client_name":    "Potential identifier, not in the metrics contract",
    "free_text_note": "Free text is not allowed in the metrics contract",
    "top_values":     "Frequent-value lists can expose real values",
}
EXTRA_METRIC_REASONS = {
    "column_max_value": "Raw value statistic, not on the metric allowlist",
    "column_min_value": "Raw value statistic, not on the metric allowlist",
    "frequent_items":   "Frequent-value lists can expose real values",
}

def export_gate(cands, ws, run_date):
    """Same logic the real spoke exporter would run. Returns (accepted, rejected_log_rows, suppressed_slices)."""
    rejected = []
    bad_metric = ~cands["metric_name"].isin(ALLOW.keys())
    for mname in cands.loc[bad_metric, "metric_name"].unique():
        rejected.append((ws, run_date, f"metric:{mname}", EXTRA_METRIC_REASONS.get(mname, "Metric not on the allowlist")))
    c = cands.loc[~bad_metric].copy()

    small = (c["slice_name"] != "overall") & (c["slice_n"] < MIN_GROUP_SIZE)
    suppressed = int(c.loc[small, ["model_name", "slice_name", "slice_value"]].drop_duplicates().shape[0])
    c = c.loc[~small]

    for col in [x for x in c.columns if x not in CONTRACT_COLS and x != "slice_n"]:
        rejected.append((ws, run_date, col, EXTRA_COLUMN_REASONS.get(col, "Column is not in the metrics contract")))
    return c[CONTRACT_COLS].copy(), rejected, suppressed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-model, per-day metric generation (with the planted stories)

# COMMAND ----------

def live_rows(m, d, k):
    """Metrics that are available the day after the window. k = days ago (0 = latest window)."""
    rows, name = [], m["name"]
    weekend = d.weekday() >= 5
    seas = (0.8 if m["serving"] == "realtime" else 0.95) if weekend else 1.0
    out = OUTAGE.get(name) if k <= 2 else None
    r_aml = clip01((25 - k) / 25) if name == "aml_alert_scorer" else 0.0

    vol_factor = seas * rng.normal(1, 0.03)
    row_count = m["rows"] * vol_factor * ((out[0] * rng.normal(1, 0.05)) if out else 1.0)
    emit(rows, m, d, "row_count", row_count, thr_for(m, "row_count"))

    null_pct = rng.normal(4.4, 0.1) if (name == "kyc_risk_rating" and k in (9, 10)) else max(0.05, rng.normal(m["null"], 0.15))
    emit(rows, m, d, "null_pct", null_pct, thr_for(m, "null_pct"))

    psi = max(0.005, rng.normal(0.05, 0.008) + 0.30 * r_aml + (0.12 if out and out[0] < 0.1 else 0.0))
    emit(rows, m, d, "feature_psi_max", psi, thr_for(m, "feature_psi_max"))

    js = max(0.005, rng.normal(0.05, 0.008) + 0.23 * r_aml ** 1.3)
    emit(rows, m, d, "prediction_drift_js", js, thr_for(m, "prediction_drift_js"))

    pos = None
    if m["pos"]:
        pos = rng.normal(m["pos"], m["pos"] * 0.03) * (1 + 1.2 * r_aml)
        emit(rows, m, d, "positive_rate", pos, thr_for(m, "positive_rate"))

    if m["serving"] == "realtime":
        lat = rng.normal(m["lat"], m["lat"] * 0.06) + (520 if (name == "txn_fraud_classifier" and k in (34, 35)) else 0)
        emit(rows, m, d, "latency_p95_ms", lat, thr_for(m, "latency_p95_ms"))
        err = abs(rng.normal(0.12, 0.03)) + (0.6 if (name == "txn_fraud_classifier" and k in (34, 35)) else 0)
        emit(rows, m, d, "error_rate_pct", err, thr_for(m, "error_rate_pct"))
        fresh = abs(rng.normal(0.3, 0.08))
    else:
        fresh = abs(rng.normal(5, 1.2))
    if out:
        fresh = out[1] + rng.normal(0, 0.5)
    if name == "lgd_model" and k in (39, 40):
        fresh = 30 + rng.normal(0, 0.5)
    if name == "fx_flow_forecast" and k in (4, 5):
        fresh = 21 + rng.normal(0, 0.4)
    emit(rows, m, d, "freshness_hours", fresh, thr_for(m, "freshness_hours"))

    if m["slices"]:
        for country, share in SLICE_SHARES.items():
            n = row_count * share * rng.normal(1, 0.05)
            if name == "aml_alert_scorer" and country == "SG":
                s_js = 0.06 + 0.40 * r_aml ** 1.2 + rng.normal(0, 0.008)
                s_pos = m["pos"] * (1 + 1.8 * r_aml) * rng.normal(1, 0.04)
            else:
                s_js = max(0.005, js * rng.normal(1, 0.12))
                s_pos = (pos if pos else m["pos"]) * rng.normal(1, 0.08)
            emit(rows, m, d, "row_count", n, thr_for(m, "row_count", share), "country", country, n, share)
            emit(rows, m, d, "prediction_drift_js", s_js, thr_for(m, "prediction_drift_js"), "country", country, n)
            emit(rows, m, d, "positive_rate", s_pos, thr_for(m, "positive_rate"), "country", country, n)
        tiny = int(rng.integers(5, 40))          # tiny slice: will be suppressed by the gate
        emit(rows, m, d, "row_count", tiny, thr_for(m, "row_count", 0.0005), "country", "MU", tiny)
        emit(rows, m, d, "prediction_drift_js", js, thr_for(m, "prediction_drift_js"), "country", "MU", tiny)
        emit(rows, m, d, "positive_rate", m["pos"], thr_for(m, "positive_rate"), "country", "MU", tiny)
    return rows, vol_factor * (out[0] if out else 1.0)

def label_rows(m, d, k):
    """Accuracy metrics for window d (k days ago). Exported only after the label lag has passed."""
    rows, name = [], m["name"]
    decay_pd = clip01((75 - k) / 45) if name == "corporate_pd" else 0.0   # story 3
    r_aml = clip01((25 - k) / 25) if name == "aml_alert_scorer" else 0.0
    nz = 0.0 if name == "corporate_pd" else 1.0      # smooth decay so alerts do not flap around the threshold
    auc = m["auc"] - 0.12 * decay_pd - 0.05 * r_aml + nz * rng.normal(0, 0.002)
    f1 = m["f1"] - 0.10 * decay_pd - 0.06 * r_aml + nz * rng.normal(0, 0.003)
    emit(rows, m, d, "auc", auc, thr_for(m, "auc"))
    emit(rows, m, d, "f1", f1, thr_for(m, "f1"))
    emit(rows, m, d, "precision", f1 + 0.04 + nz * rng.normal(0, 0.002), thr_for(m, "precision"))
    emit(rows, m, d, "recall", f1 - 0.04 + nz * rng.normal(0, 0.002), thr_for(m, "recall"))
    return rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simulate every spoke, every day, through the export gate

# COMMAND ----------

accepted_frames, ledger_rows, rejected_rows = [], [], []
cost_factor = {}                                   # (model, date) -> relative volume, drives serving cost
FORCED_EXTRAS = {                                  # make sure the Boundary Ledger always has recent examples
    ("cib-fcc-prod-uks", 1): ["customer_id"],
    ("cib-fcc-prod-uks", 6): ["top_values", "metric:frequent_items"],
    ("cib-credit-prod-uks", 3): ["account_number"],
    ("cib-txnbank-prod-cin", 4): ["metric:column_max_value"],
    ("cib-txnbank-prod-cin", 10): ["free_text_note"],
}
by_ws = {w: [m for m in MODELS if m["ws"] == w] for w in WS}

for d in DAYS_LIST:
    k = (END_DATE - d).days
    run_date = d + timedelta(days=1)
    sync_at = datetime.combine(run_date, time(2, 30)) + timedelta(minutes=int(rng.integers(0, 25)))
    for ws, info in WS.items():
        rows = []
        for m in by_ws[ws]:
            live, vf = live_rows(m, d, k)
            cost_factor[(m["name"], d)] = vf
            if ws in SILENT_WS and k <= 2:
                continue
            rows += live
            if m["lag"] is not None and (d - timedelta(days=m["lag"])) >= DAYS_LIST[0]:
                d_lag = d - timedelta(days=m["lag"])
                rows += label_rows(m, d_lag, (END_DATE - d_lag).days)
        if ws in SILENT_WS and k <= 2:
            continue                                 # spoke is silent: nothing exported, no ledger row
        cands = pd.DataFrame(rows)

        extras = list(FORCED_EXTRAS.get((ws, k), []))
        if rng.random() < 0.08:
            extras.append(str(rng.choice(list(EXTRA_COLUMN_REASONS))))
        for e in extras:
            if e.startswith("metric:"):
                src = cands[cands["slice_name"] == "overall"].iloc[[0]].copy()
                src["metric_name"] = e.split(":", 1)[1]
                cands = pd.concat([cands, src], ignore_index=True)
            else:
                cands[e] = None

        accepted, rej, suppressed = export_gate(cands, ws, run_date)
        accepted["received_at"] = sync_at
        accepted_frames.append(accepted)
        rejected_rows += rej
        n = len(accepted)
        ledger_rows.append(dict(
            workspace=ws, region=info["region"], run_date=run_date,
            raw_rows_exported=0, log_rows_exported=0, metric_rows_exported=n,
            bytes_transferred=int(n * rng.integers(190, 230) + rng.integers(1200, 1800)),
            rejected_fields_count=len(rej), suppressed_slices_count=suppressed,
            last_sync_at=sync_at, transport_option=info["transport"]))

unified_metrics = pd.concat(accepted_frames, ignore_index=True)
boundary_ledger = pd.DataFrame(ledger_rows)
rejected_fields_log = pd.DataFrame(rejected_rows, columns=["workspace", "run_date", "field_name", "reason"])
print(f"unified_metrics: {len(unified_metrics):,} rows | boundary_ledger: {len(boundary_ledger)} | rejected_fields_log: {len(rejected_fields_log)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registry, allowlist, alerts and serving cost

# COMMAND ----------

model_registry = pd.DataFrame([dict(
    model_name=m["name"], model_version=m["ver"], workstream=m["stream"], workspace=m["ws"],
    region=WS[m["ws"]]["region"], serving_type=m["serving"], owner=m["owner"], upstream_feed=m["upstream"],
    risk_tier=m["tier"], onboarded_at=datetime.combine(END_DATE - timedelta(days=m["onboard"]), time(9, 0)),
    active=True) for m in MODELS])

metric_allowlist = pd.DataFrame([dict(
    metric_name=k, description=v[0], category=v[1], unit=v[2], default_threshold=v[3], breach_direction=v[4])
    for k, v in ALLOW.items()])

# ---- alerts: one alert per consecutive run of BREACH on the overall slice -----------------------------
tier_of = {m["name"]: m["tier"] for m in MODELS}
CRITICAL_METRICS = {"prediction_drift_js", "feature_psi_max", "auc", "row_count", "freshness_hours"}
ACKED = {"txn_fraud_classifier": ("Marcus Webb", "Investigating payments_gateway_feed outage with the payments platform team")}
alerts, n_alert = [], 0
overall = unified_metrics[unified_metrics["slice_name"] == "overall"].sort_values("window_start")
for (mname, ws, metric), g in overall.groupby(["model_name", "workspace", "metric_name"]):
    g = g.reset_index(drop=True)
    is_b = g["status"] == "BREACH"
    if not is_b.any():
        continue
    episode = (is_b != is_b.shift()).cumsum()
    for _, e in g[is_b].groupby(episode[is_b]):
        first, last_idx = e.iloc[0], e.index[-1]
        ongoing = last_idx == len(g) - 1
        tier = tier_of[mname]
        sev = ("Critical" if (tier == "High" and metric in CRITICAL_METRICS) else
               "High" if tier in ("High", "Medium") else "Medium")
        status, assignee, note, resolved_at = "OPEN", None, "Auto-generated from threshold breach", pd.NaT
        if ongoing and mname in ACKED:
            status, (assignee, note) = "ACK", ACKED[mname]
        if not ongoing:
            status, resolved_at = "RESOLVED", g.loc[last_idx + 1, "received_at"]
            assignee, note = "Platform on-call", "Recovered, no action needed"
        n_alert += 1
        alerts.append(dict(alert_id=f"ALT-{n_alert:04d}", model_name=mname, workspace=ws,
                           region=first["region"], metric_name=metric, severity=sev,
                           opened_at=first["received_at"], status=status, assignee=assignee,
                           note=note, resolved_at=resolved_at))

# ---- stale spoke alerts (story 4) --------------------------------------------------------------------
as_of = boundary_ledger["last_sync_at"].max()
for ws in SILENT_WS:
    last = boundary_ledger.loc[boundary_ledger["workspace"] == ws, "last_sync_at"].max()
    for m in by_ws[ws]:
        n_alert += 1
        alerts.append(dict(alert_id=f"ALT-{n_alert:04d}", model_name=m["name"], workspace=ws,
                           region=WS[ws]["region"], metric_name="sync_stale",
                           severity="Critical" if m["tier"] == "High" else "High",
                           opened_at=last + timedelta(hours=STALE_AFTER_HOURS), status="OPEN", assignee=None,
                           note=f"No metrics received from {ws} for {int((as_of - last).total_seconds() // 3600)} hours",
                           resolved_at=pd.NaT))
alerts = pd.DataFrame(alerts)

# ---- serving cost (stands in for daily billing totals from system tables; keeps flowing for silent spokes) ----
cost_rows = []
for m in MODELS:
    ep = m["name"].replace("_", "-") + ("-endpoint" if m["serving"] == "realtime" else "-batch-job")
    base_dbu = (20 + m["rows"] / 20_000) if m["serving"] == "realtime" else (8 + m["rows"] / 60_000)
    for d in DAYS_LIST:
        vf = cost_factor[(m["name"], d)]
        dbus = base_dbu * (0.7 + 0.3 * min(vf, 1.2)) * rng.normal(1, 0.04)
        cost_rows.append(dict(date=d, workspace=m["ws"], region=WS[m["ws"]]["region"], endpoint_name=ep,
                              model_name=m["name"], dbus=round(dbus, 2), cost_usd=round(dbus * 0.07, 2)))
serving_cost_daily = pd.DataFrame(cost_rows)
print(f"alerts: {len(alerts)} | serving_cost_daily: {len(serving_cost_daily):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity checks (these should all pass before writing)

# COMMAND ----------

assert list(unified_metrics.columns) == CONTRACT_COLS + ["received_at"], "hub table must follow the metrics contract"
assert (boundary_ledger["raw_rows_exported"] == 0).all() and (boundary_ledger["log_rows_exported"] == 0).all()
assert unified_metrics["metric_name"].isin(ALLOW.keys()).all(), "non-allowlisted metric reached the hub"
assert not ((unified_metrics["slice_name"] == "country") & (unified_metrics["slice_value"] == "MU")).any(), "small slice leaked"
silent_last = boundary_ledger[boundary_ledger["workspace"].isin(SILENT_WS)]["last_sync_at"].max()
assert (as_of - silent_last) > timedelta(hours=STALE_AFTER_HOURS), "silent spoke should look stale"
print("Boundary check passed: 0 raw rows, 0 log rows, no non-allowlisted metrics, no small slices.")
print("Avg bytes per workspace-day:", int(boundary_ledger["bytes_transferred"].mean()))
print(unified_metrics.groupby("status").size().to_string())

# ==== WRITE TO UNITY CATALOG ====

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write tables (with table and column comments so Genie understands them)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} COMMENT 'Hub schema for the Unified Model Monitoring Control Tower. Aggregated monitoring metrics only, no customer data.'")

def write_table(pdf, name, ddl_schema, table_comment, col_comments=None):
    fq = f"{CATALOG}.{SCHEMA}.{name}"
    clean = pdf.copy()
    for col in clean.select_dtypes(include=["datetime64"]).columns:
        if clean[col].dt.tz is None:
            clean[col] = clean[col].dt.tz_localize("UTC")
    clean = clean.astype(object)
    rows = [tuple(None if (v is pd.NaT or (isinstance(v, float) and np.isnan(v))) else v for v in r)
            for r in clean.itertuples(index=False, name=None)]
    sdf = spark.createDataFrame(rows, schema=ddl_schema)
    sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq)
    spark.sql(f"COMMENT ON TABLE {fq} IS '{table_comment}'")
    for col, text in (col_comments or {}).items():
        spark.sql(f"ALTER TABLE {fq} ALTER COLUMN {col} COMMENT '{text}'")
    print(f"wrote {fq}: {len(rows):,} rows")

write_table(unified_metrics, "unified_metrics",
    "model_name string, model_version string, workspace string, region string, workstream string, "
    "window_start timestamp, window_end timestamp, slice_name string, slice_value string, metric_name string, "
    "metric_value double, threshold double, status string, received_at timestamp",
    "One row per model, metric, time window and slice. Aggregated monitoring metrics received from spoke workspaces. Contains no customer data.",
    {"slice_name": "overall for the whole model, or the segment dimension such as country. Use overall unless a slice is asked for",
     "slice_value": "all for overall, otherwise the segment value such as SG",
     "metric_name": "Allowlisted metric. See metric_allowlist for meaning, unit and breach direction",
     "metric_value": "Aggregated numeric value of the metric for the window",
     "threshold": "Alert threshold for this row. NULL means informational only",
     "status": "OK, WARN or BREACH. Derived by the spoke exporter from metric_value and threshold",
     "window_start": "Start of the monitored time window (daily). Use CAST(window_start AS DATE) for the date",
     "received_at": "When the hub received the row. Accuracy metrics arrive weeks after their window because labels are delayed"})

write_table(model_registry, "model_registry",
    "model_name string, model_version string, workstream string, workspace string, region string, serving_type string, "
    "owner string, upstream_feed string, risk_tier string, onboarded_at timestamp, active boolean",
    "Config table with one row per monitored model. Adding a model is one row here.",
    {"serving_type": "realtime or batch", "risk_tier": "High, Medium or Low model risk tier",
     "upstream_feed": "Name of the upstream data feed. Models sharing a feed are affected by the same upstream incident"})

write_table(metric_allowlist, "metric_allowlist",
    "metric_name string, description string, category string, unit string, default_threshold double, breach_direction string",
    "The only metrics a spoke is allowed to export. breach_direction ABOVE means a value above threshold breaches, BELOW means a value below threshold breaches.")

write_table(alerts, "alerts",
    "alert_id string, model_name string, workspace string, region string, metric_name string, severity string, "
    "opened_at timestamp, status string, assignee string, note string, resolved_at timestamp",
    "One row per alert. Created when a metric breaches or a workspace stops reporting (metric_name sync_stale).",
    {"severity": "Critical, High or Medium", "status": "OPEN, ACK or RESOLVED"})

write_table(boundary_ledger, "boundary_ledger",
    "workspace string, region string, run_date date, raw_rows_exported long, log_rows_exported long, "
    "metric_rows_exported long, bytes_transferred long, rejected_fields_count int, suppressed_slices_count int, "
    "last_sync_at timestamp, transport_option string",
    "One row per workspace per export run. Proves what left each spoke: raw and log rows are always 0, only metric rows move.",
    {"raw_rows_exported": "Raw inference rows exported. Always 0 by design",
     "log_rows_exported": "Serving or request log rows exported. Always 0 by design",
     "rejected_fields_count": "Columns or metrics the export gate refused to send",
     "suppressed_slices_count": "Slices dropped because they were below the minimum group size"})

write_table(rejected_fields_log, "rejected_fields_log",
    "workspace string, run_date date, field_name string, reason string",
    "Fields or metrics the export gate blocked at the source, with the reason. Values are never stored, only the field name.")

write_table(serving_cost_daily, "serving_cost_daily",
    "date date, workspace string, region string, endpoint_name string, model_name string, dbus double, cost_usd double",
    "Daily serving cost per endpoint (aggregated billing totals). Keeps arriving even when a spoke stops sending metrics.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Views that make Genie and the app simpler and more accurate

# COMMAND ----------

T = f"{CATALOG}.{SCHEMA}"

spark.sql(f"""
CREATE OR REPLACE VIEW {T}.v_model_daily_status
COMMENT 'One row per model per day on the overall slice: number of metrics in BREACH or WARN and the worst status. Use for heatmaps and history.'
AS
SELECT model_name, model_version, workspace, region, workstream,
       CAST(window_start AS DATE) AS window_date,
       SUM(CASE WHEN status = 'BREACH' THEN 1 ELSE 0 END) AS breach_metrics,
       SUM(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END) AS warn_metrics,
       CASE WHEN SUM(CASE WHEN status = 'BREACH' THEN 1 ELSE 0 END) > 0 THEN 'BREACH'
            WHEN SUM(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END) > 0 THEN 'WARN'
            ELSE 'OK' END AS worst_status
FROM {T}.unified_metrics
WHERE slice_name = 'overall'
GROUP BY model_name, model_version, workspace, region, workstream, CAST(window_start AS DATE)
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {T}.v_current_status
COMMENT 'One row per model with its current status: the worst status across each metric latest value. STALE means the workspace stopped sending metrics. Use this for questions about right now.'
AS
WITH latest AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY model_name, model_version, workspace, metric_name
                               ORDER BY window_start DESC) AS rn
  FROM {T}.unified_metrics WHERE slice_name = 'overall'
),
cur AS (
  SELECT model_name, model_version, workspace, region, workstream,
         MAX(window_start) AS latest_window,
         SUM(CASE WHEN status = 'BREACH' THEN 1 ELSE 0 END) AS breach_metrics,
         SUM(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END) AS warn_metrics,
         concat_ws(', ', collect_list(CASE WHEN status = 'BREACH' THEN metric_name END)) AS breached_metrics,
         concat_ws(', ', collect_list(CASE WHEN status = 'WARN' THEN metric_name END)) AS warned_metrics
  FROM latest WHERE rn = 1
  GROUP BY model_name, model_version, workspace, region, workstream
),
ws_sync AS (SELECT workspace, MAX(last_sync_at) AS last_sync_at FROM {T}.boundary_ledger GROUP BY workspace),
ref AS (SELECT MAX(last_sync_at) AS as_of FROM {T}.boundary_ledger)
SELECT c.model_name, c.model_version, c.workspace, c.region, c.workstream,
       r.risk_tier, r.owner, r.upstream_feed, r.serving_type,
       c.latest_window, w.last_sync_at, c.breach_metrics, c.warn_metrics, c.breached_metrics, c.warned_metrics,
       CASE WHEN w.last_sync_at < ref.as_of - INTERVAL {STALE_AFTER_HOURS} HOURS THEN 'STALE'
            WHEN c.breach_metrics > 0 THEN 'BREACH'
            WHEN c.warn_metrics > 0 THEN 'WARN'
            ELSE 'OK' END AS current_status
FROM cur c
JOIN {T}.model_registry r ON r.model_name = c.model_name AND r.model_version = c.model_version AND r.workspace = c.workspace
JOIN ws_sync w ON w.workspace = c.workspace
CROSS JOIN ref
""")

# COMMAND ----------

display(spark.sql(f"SELECT current_status, COUNT(*) AS models FROM {T}.v_current_status GROUP BY current_status ORDER BY models DESC"))
display(spark.sql(f"SELECT model_name, workspace, region, current_status, breached_metrics, warned_metrics FROM {T}.v_current_status ORDER BY current_status, model_name"))