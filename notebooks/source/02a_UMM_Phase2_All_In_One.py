# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # UMM Demo · Phase 2 · Classical ML models
# MAGIC
# MAGIC Self-contained (no `%run`). Attach serverless compute and **Run all**.
# MAGIC
# MAGIC | Step | What happens |
# MAGIC |---|---|
# MAGIC | 1 | Train PD **v1** (pre-history data) and **v2** (adds history days 0–44), register both in UC |
# MAGIC | 2 | Train the trade volume forecast and register it in UC |
# MAGIC | 3 | Backfill 90 days of PD predictions into `credit_monitoring.pd_inference_log` (v1 before day 45, v2 after), with delayed labels |
# MAGIC | 4 | Backfill nightly forecasts into `markets_monitoring.trade_volume_scoring_log` |
# MAGIC | 5 | Write holdout-scored baselines for drift and performance comparison |
# MAGIC | 6 | Record model versions in the hub and refresh the scenario notes |
# MAGIC | 7 | Deploy PD v2 to a serving endpoint with an AI Gateway inference table, and send live traffic (optional, `DEPLOY_ENDPOINT`) |
# MAGIC | 8 | Validate |
# MAGIC
# MAGIC Live endpoint traffic lands in the inference table within about an hour. `UMM_Phase2_PD_Live_Processor` then appends it to `pd_inference_log` with `record_source = 'live'`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

USE_SINGLE_CATALOG = True
SINGLE_CATALOG = "demo_unified_model_monitoring"
MULTI_CATALOGS = {"credit": "umm_demo_cib_credit", "markets": "umm_demo_cib_markets",
                  "ai_factory": "umm_demo_ai_factory", "hub": "umm_demo_hub"}

DEPLOY_ENDPOINT = True            # set False to skip serving (backfill and monitoring still work)
PD_ENDPOINT = "umm-demo-pd-endpoint"
LIVE_REQUESTS = 200               # requests sent to the endpoint after deployment
EXPERIMENT_PATH = "/Shared/model_monitoring/umm_demo_training"
SEED = 20261125

# COMMAND ----------

import json, time
import numpy as np
import pandas as pd
import sklearn, cloudpickle, mlflow
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from pyspark.sql import functions as F
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.metrics import (accuracy_score, f1_score, mean_absolute_error, mean_squared_error,
                             precision_score, recall_score, roc_auc_score)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder


def catalog_for(area):
    return SINGLE_CATALOG if USE_SINGLE_CATALOG else MULTI_CATALOGS[area]


def schema_for(area, schema):
    return f"{area}_{schema}" if USE_SINGLE_CATALOG else schema


def fq(area, schema, table):
    return f"{catalog_for(area)}.{schema_for(area, schema)}.{table}"


def write_table(pdf, area, schema, table, nullable_cols=(), nullable_int_cols=(), date_cols=(),
                cdf=False, comment=None):
    sdf = spark.createDataFrame(pdf)
    for c in nullable_cols:
        sdf = sdf.withColumn(c, F.when(F.isnan(F.col(c)), F.lit(None).cast("double")).otherwise(F.col(c)))
    for c in nullable_int_cols:
        sdf = sdf.withColumn(c, F.when(F.isnan(F.col(c)), F.lit(None)).otherwise(F.col(c)).cast("int"))
    for c in date_cols:
        sdf = sdf.withColumn(c, F.to_date(F.col(c)))
    name = fq(area, schema, table)
    sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)
    if cdf:
        spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    if comment:
        spark.sql(f"COMMENT ON TABLE {name} IS '{comment.replace(chr(39), chr(92) + chr(39))}'")
    print(f"✓ {name}: {spark.table(name).count():,} rows")
    return name


try:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
except Exception:
    pass

PD_UC_MODEL = fq("credit", "ml", "pd_counterparty_default")
FCST_UC_MODEL = fq("markets", "ml", "trade_volume_forecast")

# The history window comes from the Phase 1 data, so Phase 2 can run on any later day
_r = spark.sql(f"SELECT min(to_date(request_time)) AS s, max(to_date(request_time)) AS e "
               f"FROM {fq('credit', 'raw', 'pd_scoring_requests')}").collect()[0]
HISTORY_START, HISTORY_END = pd.Timestamp(_r.s), pd.Timestamp(_r.e)
cal = spark.table(fq("hub", "ref", "scenario_calendar")).toPandas().set_index("scenario_id")
V2_DAY = int(cal.loc["S0_PD_V2_DEPLOY", "start_day"])

mlflow.set_registry_uri("databricks-uc")
try:
    mlflow.set_experiment(EXPERIMENT_PATH)
except Exception as e:
    user = spark.sql("SELECT current_user()").collect()[0][0]
    EXPERIMENT_PATH = f"/Users/{user}/umm_demo_training"
    mlflow.set_experiment(EXPERIMENT_PATH)
    print(f"Using {EXPERIMENT_PATH} ({e})")
client = MlflowClient()
PIP = [f"scikit-learn=={sklearn.__version__}", f"pandas=={pd.__version__}",
       f"numpy=={np.__version__}", f"cloudpickle=={cloudpickle.__version__}"]

print(f"History {HISTORY_START.date()} → {HISTORY_END.date()} · PD v2 deploys on day {V2_DAY}")
print(f"Models: {PD_UC_MODEL}, {FCST_UC_MODEL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model wrappers and registration helper
# MAGIC
# MAGIC Both models are logged as pyfunc wrappers so the serving endpoint returns named outputs (`prediction`, `pd_probability`), which keeps inference-table parsing simple.

# COMMAND ----------

class PDModel(mlflow.pyfunc.PythonModel):
    """Counterparty PD: returns class at the policy threshold plus the probability."""

    def __init__(self, pipeline, threshold, features):
        self.pipeline, self.threshold, self.features = pipeline, float(threshold), list(features)

    def predict(self, context, model_input, params=None):
        p = self.pipeline.predict_proba(model_input[self.features])[:, 1]
        return pd.DataFrame({"prediction": (p >= self.threshold).astype(int), "pd_probability": p})


class ForecastModel(mlflow.pyfunc.PythonModel):
    """Next-day trade volume (USD m) for one country × product."""

    def __init__(self, pipeline, features):
        self.pipeline, self.features = pipeline, list(features)

    def predict(self, context, model_input, params=None):
        return pd.DataFrame({"prediction": self.pipeline.predict(model_input[self.features])})


def log_and_register(model_obj, uc_name, x_example, run_name, params, metrics, tags):
    with mlflow.start_run(run_name=run_name) as run:
        mlflow.log_params(params)
        mlflow.log_metrics(metrics)
        mlflow.set_tags(tags)
        sig = infer_signature(x_example, model_obj.predict(None, x_example))
        info = mlflow.pyfunc.log_model(artifact_path="model", python_model=model_obj, signature=sig,
                                       input_example=x_example.head(3), registered_model_name=uc_name,
                                       pip_requirements=PIP)
    version = getattr(info, "registered_model_version", None)
    if version is None:
        version = max(int(mv.version) for mv in client.search_model_versions(f"name='{uc_name}'")
                      if mv.run_id == run.info.run_id)
    version = str(version)
    for k, v in tags.items():
        client.set_model_version_tag(uc_name, version, k, str(v))
    print(f"✓ registered {uc_name} v{version}")
    return version

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · PD model v1 and v2

# COMMAND ----------

PD_CAT = ["country", "product", "sector"]
PD_NUM = ["annual_revenue_usd_m", "debt_to_ebitda", "interest_coverage", "utilisation_pct",
          "days_past_due_max_12m", "internal_rating", "years_relationship", "collateral_coverage",
          "requested_limit_usd_m"]
PD_FEATURES = PD_CAT + PD_NUM


def pd_pipeline():
    pre = ColumnTransformer(
        [("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=np.nan), PD_CAT)],
        remainder="passthrough", verbose_feature_names_out=False)
    clf = HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05, max_leaf_nodes=31,
                                         categorical_features=[True] * len(PD_CAT) + [False] * len(PD_NUM),
                                         random_state=SEED)
    return Pipeline([("pre", pre), ("clf", clf)])


def pd_holdout(df, threshold=None):
    """Time-ordered 80/20 split. Returns holdout frame with probabilities, chosen threshold, metrics."""
    df = df.sort_values("request_time").reset_index(drop=True)
    cut = int(len(df) * 0.8)
    fit, hold = df.iloc[:cut], df.iloc[cut:].copy()
    tmp = pd_pipeline().fit(fit[PD_FEATURES], fit["default_flag"])
    hold["pd_probability"] = tmp.predict_proba(hold[PD_FEATURES])[:, 1]
    if threshold is None:   # policy threshold: maximise F1 on the holdout
        grid = np.round(np.arange(0.10, 0.51, 0.025), 3)
        threshold = float(grid[np.argmax([f1_score(hold["default_flag"], hold["pd_probability"] >= t)
                                          for t in grid])])
    hold["prediction"] = (hold["pd_probability"] >= threshold).astype(int)
    y, yhat = hold["default_flag"], hold["prediction"]
    metrics = dict(holdout_auc=roc_auc_score(y, hold["pd_probability"]), holdout_accuracy=accuracy_score(y, yhat),
                   holdout_precision=precision_score(y, yhat), holdout_recall=recall_score(y, yhat),
                   holdout_f1_macro=f1_score(y, yhat, average="macro"), holdout_default_rate=y.mean())
    return hold, threshold, {k: round(float(v), 4) for k, v in metrics.items()}


pd_train = spark.table(fq("credit", "raw", "pd_training")).toPandas()
pd_req = spark.table(fq("credit", "raw", "pd_scoring_requests")).toPandas()
pd_out = spark.table(fq("credit", "raw", "pd_outcomes")).toPandas()
pd_req["day"] = (pd_req["request_time"].dt.normalize() - HISTORY_START).dt.days
pd_req = pd_req.merge(pd_out, on="request_id", how="left")

# v1: trained on the 12 months before the window
hold_v1, PD_THRESHOLD, m_v1 = pd_holdout(pd_train)
pd_v1_model = PDModel(pd_pipeline().fit(pd_train[PD_FEATURES], pd_train["default_flag"]), PD_THRESHOLD, PD_FEATURES)
print(f"Policy threshold: {PD_THRESHOLD} · v1 holdout {m_v1}")

# v2: retrained with labelled history before the deployment day (same policy threshold)
v2_data = pd.concat([pd_train, pd_req[(pd_req["day"] < V2_DAY) & pd_req["default_flag"].notna()]
                     .assign(default_flag=lambda x: x["default_flag"].astype(int))[pd_train.columns]],
                    ignore_index=True)
hold_v2, _, m_v2 = pd_holdout(v2_data, threshold=PD_THRESHOLD)
pd_v2_model = PDModel(pd_pipeline().fit(v2_data[PD_FEATURES], v2_data["default_flag"]), PD_THRESHOLD, PD_FEATURES)
print(f"v2 holdout {m_v2}")

x_ex = pd_train[PD_FEATURES].head(20)
common = dict(model_id="pd_counterparty_default", decision_threshold=PD_THRESHOLD, algorithm="HistGradientBoostingClassifier")
PD_V1 = log_and_register(pd_v1_model, PD_UC_MODEL, x_ex, "pd_v1", {"threshold": PD_THRESHOLD, "train_rows": len(pd_train)},
                         m_v1, {**common, "training_data": "pd_training (12 months pre-window)"})
PD_V2 = log_and_register(pd_v2_model, PD_UC_MODEL, x_ex, "pd_v2", {"threshold": PD_THRESHOLD, "train_rows": len(v2_data)},
                         m_v2, {**common, "training_data": f"pd_training + history days 0-{V2_DAY - 1}"})
client.set_registered_model_alias(PD_UC_MODEL, "champion", PD_V2)
client.set_registered_model_alias(PD_UC_MODEL, "previous", PD_V1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Trade volume forecast
# MAGIC
# MAGIC Nightly batch: the forecast for day D uses actuals up to D-1, yesterday's market drivers, and calendar features known in advance. When the FX shock hits, the model is a day behind, so errors spike, which is the realistic failure mode.

# COMMAND ----------

FC_CAT = ["country", "product"]
FC_NUM = ["dow", "dom", "is_month_end", "is_holiday", "fx_vol_lag1", "rates_lag1", "vol_lag1", "vol_lag7",
          "vol_roll7", "vol_roll28", "clients_lag7"]
FC_FEATURES = FC_CAT + FC_NUM


def fc_features(tv):
    tv = tv.sort_values(["country", "product", "trade_date"]).copy()
    g = tv.groupby(["country", "product"])
    tv["vol_lag1"] = g["volume_usd_m"].shift(1)
    tv["vol_lag7"] = g["volume_usd_m"].shift(7)
    tv["vol_roll7"] = g["volume_usd_m"].transform(lambda s: s.shift(1).rolling(7).mean())
    tv["vol_roll28"] = g["volume_usd_m"].transform(lambda s: s.shift(1).rolling(28).mean())
    tv["fx_vol_lag1"] = g["fx_vol_index"].shift(1)
    tv["rates_lag1"] = g["rates_level_pct"].shift(1)
    tv["clients_lag7"] = g["active_clients"].shift(7)
    tv["dow"] = tv["trade_date"].dt.weekday
    tv["dom"] = tv["trade_date"].dt.day
    tv["is_month_end"] = tv["is_month_end"].astype(int)
    tv["is_holiday"] = tv["is_holiday"].astype(int)
    return tv.dropna(subset=["vol_roll28", "clients_lag7"]).reset_index(drop=True)


def fc_pipeline():
    pre = ColumnTransformer(
        [("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=np.nan), FC_CAT)],
        remainder="passthrough", verbose_feature_names_out=False)
    reg = HistGradientBoostingRegressor(max_iter=400, learning_rate=0.05,
                                        categorical_features=[True] * len(FC_CAT) + [False] * len(FC_NUM),
                                        random_state=SEED)
    return Pipeline([("pre", pre), ("reg", reg)])


tv = spark.table(fq("markets", "raw", "trade_volume_daily")).toPandas()
tv["trade_date"] = pd.to_datetime(tv["trade_date"])
tvf = fc_features(tv)
fc_train = tvf[tvf["dataset_split"] == "train"]
fc_score = tvf[tvf["dataset_split"] == "scoring_history"].copy()

hold_cut = fc_train["trade_date"].max() - pd.Timedelta(days=90)
fc_tmp = fc_pipeline().fit(fc_train.loc[fc_train["trade_date"] <= hold_cut, FC_FEATURES],
                           fc_train.loc[fc_train["trade_date"] <= hold_cut, "volume_usd_m"])
fc_hold = fc_train[fc_train["trade_date"] > hold_cut].copy()
fc_hold["prediction"] = fc_tmp.predict(fc_hold[FC_FEATURES])
m_fc = dict(holdout_mae=mean_absolute_error(fc_hold["volume_usd_m"], fc_hold["prediction"]),
            holdout_rmse=float(np.sqrt(mean_squared_error(fc_hold["volume_usd_m"], fc_hold["prediction"]))))
m_fc = {k: round(float(v), 3) for k, v in m_fc.items()}

fc_model = ForecastModel(fc_pipeline().fit(fc_train[FC_FEATURES], fc_train["volume_usd_m"]), FC_FEATURES)
FC_V = log_and_register(fc_model, FCST_UC_MODEL, fc_train[FC_FEATURES].head(20), "trade_volume_forecast",
                        {"train_rows": len(fc_train)}, m_fc,
                        dict(model_id="trade_volume_forecast", algorithm="HistGradientBoostingRegressor",
                             training_data="trade_volume_daily (2 years pre-window)"))
client.set_registered_model_alias(FCST_UC_MODEL, "champion", FC_V)
print(f"Forecast holdout {m_fc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Backfill PD inference log and baseline
# MAGIC
# MAGIC Lane A settings: timestamp `request_time`, model ID `model_version`, `prediction` and `label`. Labels arrive 7 days after scoring, so the latest week is NULL ("labels pending").

# COMMAND ----------

is_v2 = pd_req["day"] >= V2_DAY
scored = []
for mask, model, version in [(~is_v2, pd_v1_model, PD_V1), (is_v2, pd_v2_model, PD_V2)]:
    part = pd_req[mask].copy()
    out = model.predict(None, part)
    part["pd_probability"] = out["pd_probability"].to_numpy()
    part["prediction"] = out["prediction"].to_numpy()
    part["model_version"] = version
    scored.append(part)
pd_log = pd.concat(scored, ignore_index=True).sort_values("request_time")
pd_log["label"] = pd_log["default_flag"].astype(float)
pd_log["label_time"] = pd_log["outcome_time"]
pd_log["served_entity"] = "synthetic_backfill"
pd_log["record_source"] = "synthetic_history"
PD_LOG_COLS = (["request_id", "request_time", "model_version"] + PD_FEATURES + ["counterparty_id",
               "pd_probability", "prediction", "label", "label_time", "served_entity", "record_source"])
pd_log = pd_log[PD_LOG_COLS]

write_table(pd_log, "credit", "monitoring", "pd_inference_log", nullable_int_cols=["label"], cdf=True,
            comment="Lane A monitored table for the PD model. Backfilled history plus live endpoint traffic (record_source). Labels arrive 7 days after scoring.")

pd_base = pd.concat([hold_v1.assign(model_version=PD_V1), hold_v2.assign(model_version=PD_V2)], ignore_index=True)
pd_base["label"] = pd_base["default_flag"].astype(float)
pd_base["label_time"] = pd_base["request_time"] + pd.Timedelta(days=7)
pd_base["served_entity"] = "holdout"
pd_base["record_source"] = "baseline"
write_table(pd_base[PD_LOG_COLS], "credit", "ml", "pd_baseline", nullable_int_cols=["label"],
            comment="PD baseline: time-based holdout for each model version, scored by a model trained without it.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Backfill forecast scoring log and baseline (Lane B)

# COMMAND ----------

fc_score["prediction"] = fc_model.predict(None, fc_score)["prediction"].to_numpy()
FC_LOG_COLS = (["trade_date", "scoring_time", "country", "product", "model_version"] + FC_NUM +
               ["prediction", "label", "record_source"])


def fc_log_frame(df, version, source):
    out = df.copy()
    out["scoring_time"] = out["trade_date"] + pd.Timedelta(hours=2)    # nightly batch at 02:00 UTC
    out["label"] = out["volume_usd_m"]
    out["model_version"] = version
    out["record_source"] = source
    return out[FC_LOG_COLS]


write_table(fc_log_frame(fc_score, FC_V, "synthetic_history"), "markets", "monitoring", "trade_volume_scoring_log",
            date_cols=["trade_date"], cdf=True,
            comment="Lane B monitored table for the trade volume forecast. One row per trade_date x country x product with actual (label).")
write_table(fc_log_frame(fc_hold, FC_V, "baseline"), "markets", "ml", "trade_volume_baseline", date_cols=["trade_date"],
            comment="Forecast baseline: last 90 days of training period, scored by a model trained without them.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Hub updates: model versions, inventory, scenario notes

# COMMAND ----------

registry = pd.DataFrame([
    dict(model_id="pd_counterparty_default", uc_model_name=PD_UC_MODEL, model_version=PD_V1, alias="previous",
         live_from=str(HISTORY_START.date()), live_to=str((HISTORY_START + pd.Timedelta(days=V2_DAY - 1)).date()),
         decision_threshold=PD_THRESHOLD, training_data="12 months pre-window", **m_v1),
    dict(model_id="pd_counterparty_default", uc_model_name=PD_UC_MODEL, model_version=PD_V2, alias="champion",
         live_from=str((HISTORY_START + pd.Timedelta(days=V2_DAY)).date()), live_to="",
         decision_threshold=PD_THRESHOLD, training_data=f"pre-window + history days 0-{V2_DAY - 1}", **m_v2),
    dict(model_id="trade_volume_forecast", uc_model_name=FCST_UC_MODEL, model_version=FC_V, alias="champion",
         live_from=str(HISTORY_START.date()), live_to="", decision_threshold=np.nan,
         training_data="2 years pre-window", **m_fc),
])
write_table(registry, "hub", "ref", "model_version_registry", nullable_cols=["decision_threshold"] +
            [c for c in registry.columns if c.startswith("holdout_")],
            comment="Registered model versions, live periods, policy thresholds and holdout metrics.")

dm = fq("hub", "ref", "dim_model")
spark.sql(f"UPDATE {dm} SET current_version = '{PD_V2}' WHERE model_id = 'pd_counterparty_default'")
spark.sql(f"UPDATE {dm} SET current_version = '{FC_V}' WHERE model_id = 'trade_volume_forecast'")

mm = fq("hub", "ref", "map_model_metric")
spark.sql(f"""UPDATE {mm} SET implementation = 'Custom aggregate: share of requests with pd_probability >= {PD_THRESHOLD} (policy threshold), vs baseline'
              WHERE model_id = 'pd_counterparty_default' AND contract_name = 'positive_rate_at_threshold'""")

sc = fq("hub", "ref", "scenario_calendar")
spark.sql(f"""UPDATE {sc} SET expected_signals = 'data_drift_psi and data_drift_js red; positive_rate_at_threshold roughly doubles (red); accuracy falls from ~0.88 to ~0.81 (red) as the default mix shifts'
              WHERE scenario_id = 'S1_PD_LEVERAGE_DRIFT'""")
spark.sql(f"""UPDATE {sc} SET expected_signals = 'label_drift_js and concept_drift_score (calibration gap) rise, worst in the IN slice; approval_rate_gap widens. Precision and recall hold up because the model still ranks risk well: a talking point that one green metric does not mean a healthy model'
              WHERE scenario_id = 'S2_PD_IN_DEFAULTS'""")
print("✓ hub reference tables updated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Serve PD v2 with an AI Gateway inference table (optional)
# MAGIC
# MAGIC Creates (or updates) `umm-demo-pd-endpoint` with scale-to-zero and logs every request to `credit_monitoring.pd_endpoint_payload`. Endpoint creation usually takes 10–20 minutes.

# COMMAND ----------

if DEPLOY_ENDPOINT:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()

    def endpoint_get(name):
        try:
            return w.api_client.do("GET", f"/api/2.0/serving-endpoints/{name}")
        except Exception as e:
            if "does not exist" in str(e).lower() or "RESOURCE_DOES_NOT_EXIST" in str(e) or "404" in str(e):
                return None
            raise

    served = [{"name": f"pd-v{PD_V2}", "entity_name": PD_UC_MODEL, "entity_version": PD_V2,
               "workload_size": "Small", "scale_to_zero_enabled": True}]
    ai_gateway = {"inference_table_config": {"catalog_name": catalog_for("credit"),
                                             "schema_name": schema_for("credit", "monitoring"),
                                             "table_name_prefix": "pd_endpoint", "enabled": True}}
    if endpoint_get(PD_ENDPOINT) is None:
        w.api_client.do("POST", "/api/2.0/serving-endpoints",
                        body={"name": PD_ENDPOINT, "config": {"served_entities": served}, "ai_gateway": ai_gateway,
                              "tags": [{"key": "project", "value": "umm_demo"}]})
        print(f"Creating {PD_ENDPOINT} …")
    else:
        w.api_client.do("PUT", f"/api/2.0/serving-endpoints/{PD_ENDPOINT}/config", body={"served_entities": served})
        w.api_client.do("PUT", f"/api/2.0/serving-endpoints/{PD_ENDPOINT}/ai-gateway", body=ai_gateway)
        print(f"Updating {PD_ENDPOINT} …")

    deadline = time.time() + 45 * 60
    while True:
        state = endpoint_get(PD_ENDPOINT)["state"]
        if state.get("ready") == "READY" and state.get("config_update") in (None, "NOT_UPDATING"):
            print("✓ endpoint READY")
            break
        if state.get("config_update") == "UPDATE_FAILED":
            raise RuntimeError(f"Endpoint update failed: {state}. Check the endpoint's Events/Logs tab.")
        if time.time() > deadline:
            raise TimeoutError(f"Endpoint not ready after 45 minutes: {state}. Re-run this cell to keep waiting.")
        print(f"  {time.strftime('%H:%M:%S')} {state}")
        time.sleep(60)
else:
    print("DEPLOY_ENDPOINT = False · skipping serving")

# COMMAND ----------

if DEPLOY_ENDPOINT:
    live = pd_req[pd_req["day"] >= pd_req["day"].max() - 2].sample(n=LIVE_REQUESTS, random_state=SEED)
    records = json.loads(live[PD_FEATURES].to_json(orient="records"))
    ok = 0
    for i in range(0, len(records), 20):
        resp = w.api_client.do("POST", f"/serving-endpoints/{PD_ENDPOINT}/invocations",
                               body={"dataframe_records": records[i:i + 20]})
        ok += len(resp.get("predictions", []))
    print(f"✓ {ok} live predictions served. Example: {resp['predictions'][0]}")
    print("Inference table rows appear in credit_monitoring.pd_endpoint_payload within ~1 hour. "
          "Then run UMM_Phase2_PD_Live_Processor.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Validate

# COMMAND ----------

results = []


def check(name, value, ok, expect):
    results.append((name, "PASS" if ok else "FAIL", str(round(value, 4) if isinstance(value, float) else value), expect))


def sql1(q):
    return spark.sql(q).collect()[0][0]


L = fq("credit", "monitoring", "pd_inference_log")
day = f"datediff(to_date(request_time), DATE'{HISTORY_START.date()}')"
n_req = sql1(f"SELECT count(*) FROM {fq('credit', 'raw', 'pd_scoring_requests')}")
check("PD log: one row per request", sql1(f"SELECT count(*) FROM {L} WHERE record_source = 'synthetic_history'"),
      sql1(f"SELECT count(*) FROM {L} WHERE record_source = 'synthetic_history'") == n_req, f"= {n_req}")
v1_wrong = sql1(f"SELECT count(*) FROM {L} WHERE {day} < {V2_DAY} AND model_version <> '{PD_V1}'")
check("PD log: v1 before day 45", v1_wrong, v1_wrong == 0, "= 0")
v2_wrong = sql1(f"SELECT count(*) FROM {L} WHERE {day} >= {V2_DAY} AND model_version <> '{PD_V2}' AND record_source = 'synthetic_history'")
check("PD log: v2 from day 45", v2_wrong, v2_wrong == 0, "= 0")
pending = sql1(f"SELECT count(*) FROM {L} WHERE label IS NOT NULL AND to_date(request_time) > DATE'{HISTORY_END.date()}' - INTERVAL 7 DAYS")
check("PD log: last 7 days unlabelled", pending, pending == 0, "= 0")

acc = lambda lo, hi: sql1(f"SELECT avg(CASE WHEN prediction = label THEN 1.0 ELSE 0.0 END) FROM {L} "
                          f"WHERE label IS NOT NULL AND {day} BETWEEN {lo} AND {hi}")
a_base, a_late = acc(0, V2_DAY - 1), acc(70, 82)
check("PD: baseline accuracy", a_base, 0.85 <= a_base <= 0.92, "0.85–0.92")
check("S1/S2: accuracy drop (base − late)", a_base - a_late, a_base - a_late > 0.03, "> 0.03")
pos = lambda lo, hi: sql1(f"SELECT avg(prediction) FROM {L} WHERE {day} BETWEEN {lo} AND {hi}")
check("S1: positive rate late / base", pos(75, 89) / pos(0, V2_DAY - 1), pos(75, 89) / pos(0, V2_DAY - 1) > 1.6, "> 1.6")
cal_in = sql1(f"SELECT abs(avg(label) - avg(pd_probability)) FROM {L} WHERE label IS NOT NULL AND country = 'IN' AND {day} BETWEEN 66 AND 82")
check("S2: IN calibration gap", cal_in, cal_in > 0.10, "> 0.10")

FL = fq("markets", "monitoring", "trade_volume_scoring_log")
fday = f"datediff(trade_date, DATE'{HISTORY_START.date()}')"
n_fc = sql1(f"SELECT count(*) FROM {FL}")
check("Forecast log: 90 days × 30 series", n_fc, n_fc == 2700, "= 2700")
mae = lambda lo, hi: sql1(f"SELECT avg(abs(prediction - label)) FROM {FL} WHERE {fday} BETWEEN {lo} AND {hi}")
check("S3: MAE shock / pre-shock", mae(74, 82) / mae(30, 60), mae(74, 82) / mae(30, 60) > 2.0, "> 2.0")

for t in [fq("credit", "ml", "pd_baseline"), fq("markets", "ml", "trade_volume_baseline"), fq("hub", "ref", "model_version_registry")]:
    n = sql1(f"SELECT count(*) FROM {t}")
    check(f"{t.split('.')[-1]}: has rows", n, n > 0, "> 0")
nv = sql1(f"SELECT count(DISTINCT model_version) FROM {fq('credit', 'ml', 'pd_baseline')}")
check("PD baseline: both versions", nv, nv == 2, "= 2")

champ = client.get_model_version_by_alias(PD_UC_MODEL, "champion").version
check("PD champion alias = v2", champ, str(champ) == PD_V2, f"= {PD_V2}")
smoke = mlflow.pyfunc.load_model(f"models:/{PD_UC_MODEL}@champion").predict(pd_train[PD_FEATURES].head(5))
check("PD champion loads and predicts", len(smoke), list(smoke.columns) == ["prediction", "pd_probability"], "5 rows, 2 cols")

if DEPLOY_ENDPOINT:
    st = endpoint_get(PD_ENDPOINT)
    check("Endpoint READY", st["state"].get("ready"), st["state"].get("ready") == "READY", "READY")
    itc = (st.get("ai_gateway") or {}).get("inference_table_config", {})
    check("Inference table enabled", str(itc.get("enabled")), bool(itc.get("enabled")), "True")

report = spark.createDataFrame(results, ["check", "result", "value", "expected"])
display(report)
failed = [r for r in results if r[1] == "FAIL"]
if failed:
    raise AssertionError(f"{len(failed)} check(s) failed: " + ", ".join(r[0] for r in failed))
print(f"All {len(results)} checks passed. Phase 2 is ready.")