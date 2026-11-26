# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # UMM Demo · Phase 4 · Daily keep-alive
# MAGIC
# MAGIC Runs in the `umm-demo-daily` job, so the classical models keep producing fresh data and the demo never goes stale:
# MAGIC 1. **Forecast:** generates each missing day of trade volumes up to yesterday, scores it with the `champion` model (nightly batch), and appends to `trade_volume_scoring_log` as `record_source = 'live'`.
# MAGIC 2. **PD outcomes:** live PD requests older than 7 days get a simulated default outcome (labels mature).
# MAGIC 3. **PD traffic:** sends a day's worth of requests to the PD endpoint. They land in its inference table and are processed by the next run.
# MAGIC
# MAGIC Safe to run manually too. Each step skips what's already done.

# COMMAND ----------

USE_SINGLE_CATALOG = True
SINGLE_CATALOG = "demo_unified_model_monitoring"
MULTI_CATALOGS = {"credit": "umm_demo_cib_credit", "markets": "umm_demo_cib_markets"}
PD_ENDPOINT = "umm-demo-pd-endpoint"
PD_DAILY_REQUESTS = 150
PD_OUTCOME_UPLIFT = 1.15     # matured defaults run a little above predicted: mild ongoing drift


def fq(area, schema, table):
    if USE_SINGLE_CATALOG:
        return f"{SINGLE_CATALOG}.{area}_{schema}.{table}"
    return f"{MULTI_CATALOGS[area]}.{schema}.{table}"

# COMMAND ----------

import json
import numpy as np
import pandas as pd
import mlflow
from pyspark.sql import functions as F
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
mlflow.set_registry_uri("databricks-uc")
rng = np.random.default_rng()
YESTERDAY = (pd.Timestamp.utcnow().tz_localize(None).normalize() - pd.Timedelta(days=1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Forecast: new days of actuals, scored by the champion model

# COMMAND ----------

RAW_TV = fq("markets", "raw", "trade_volume_daily")
LOG_TV = fq("markets", "monitoring", "trade_volume_scoring_log")
FC_UC = fq("markets", "ml", "trade_volume_forecast")
FC_CAT = ["country", "product"]
FC_NUM = ["dow", "dom", "is_month_end", "is_holiday", "fx_vol_lag1", "rates_lag1", "vol_lag1", "vol_lag7",
          "vol_roll7", "vol_roll28", "clients_lag7"]

tv = spark.table(RAW_TV).toPandas()
tv["trade_date"] = pd.to_datetime(tv["trade_date"])
last = tv["trade_date"].max()
new_days = pd.date_range(last + pd.Timedelta(days=1), YESTERDAY, freq="D")
print(f"Last trade date {last.date()} · {len(new_days)} new day(s) to generate")

if len(new_days):
    hist = tv[tv["trade_date"] > last - pd.Timedelta(days=60)].copy()
    drivers = tv.groupby("trade_date")[["fx_vol_index", "rates_level_pct"]].first().sort_index()
    fx, rates = float(drivers["fx_vol_index"].iloc[-1]), float(drivers["rates_level_pct"].iloc[-1])
    new_rows = []
    for day in new_days:
        fx = 8.0 + 0.92 * (fx - 8.0) + rng.normal(0, 0.6)
        rates = rates + rng.normal(0, 0.02)
        is_weekend = day.weekday() >= 5
        bdays_left = len(pd.bdate_range(day, day + pd.offsets.MonthEnd(0)))
        is_month_end = (not is_weekend) and bdays_left <= 2
        for (c, p), g in hist.groupby(["country", "product"]):
            same_dow = g[g["trade_date"].dt.weekday == day.weekday()].tail(4)
            base = same_dow["volume_usd_m"].median() if len(same_dow) else g["volume_usd_m"].median()
            base = base / (1.25 if same_dow["is_month_end"].any() else 1.0)
            vol = base * (1.25 if is_month_end else 1.0) * rng.lognormal(0, 0.07)
            if p.startswith("FX"):
                vol *= 1 + 0.035 * (fx - 8.0) / 3
            clients = int(g[g["trade_date"].dt.weekday == day.weekday()]["active_clients"].tail(4).median()
                          if len(same_dow) else g["active_clients"].median())
            new_rows.append(dict(trade_date=day, country=c, product=p, volume_usd_m=round(float(vol), 3),
                                 fx_vol_index=round(fx, 3), rates_level_pct=round(rates, 4), active_clients=clients,
                                 is_month_end=bool(is_month_end), is_weekend=bool(is_weekend), is_holiday=False,
                                 dataset_split="scoring_live"))
        hist = pd.concat([hist, pd.DataFrame(new_rows[-30:])], ignore_index=True)
    new_raw = pd.DataFrame(new_rows)
    spark.createDataFrame(new_raw).withColumn("trade_date", F.to_date("trade_date")) \
        .select(*spark.table(RAW_TV).columns).write.mode("append").saveAsTable(RAW_TV)

    # Features for the new days use the full recent history (lags and rolling windows)
    full = pd.concat([tv[tv["trade_date"] > last - pd.Timedelta(days=60)], new_raw], ignore_index=True)
    full = full.sort_values(["country", "product", "trade_date"])
    g = full.groupby(["country", "product"])
    full["vol_lag1"] = g["volume_usd_m"].shift(1)
    full["vol_lag7"] = g["volume_usd_m"].shift(7)
    full["vol_roll7"] = g["volume_usd_m"].transform(lambda s: s.shift(1).rolling(7).mean())
    full["vol_roll28"] = g["volume_usd_m"].transform(lambda s: s.shift(1).rolling(28).mean())
    full["fx_vol_lag1"] = g["fx_vol_index"].shift(1)
    full["rates_lag1"] = g["rates_level_pct"].shift(1)
    full["clients_lag7"] = g["active_clients"].shift(7)
    full["dow"] = full["trade_date"].dt.weekday
    full["dom"] = full["trade_date"].dt.day
    full["is_month_end"] = full["is_month_end"].astype(int)
    full["is_holiday"] = full["is_holiday"].astype(int)
    score = full[full["trade_date"].isin(new_days)].copy()

    champ = w.api_client.do("GET", f"/api/2.1/unity-catalog/models/{FC_UC}/aliases/champion")
    version = str(champ.get("version"))
    model = mlflow.pyfunc.load_model(f"models:/{FC_UC}@champion")
    score["prediction"] = model.predict(score[FC_CAT + FC_NUM])["prediction"].to_numpy()
    score["label"] = score["volume_usd_m"]
    score["residual"] = score["label"] - score["prediction"]
    score["scoring_time"] = score["trade_date"] + pd.Timedelta(hours=2)
    score["model_version"] = version
    score["record_source"] = "live"
    log_cols = spark.table(LOG_TV).columns
    sdf = spark.createDataFrame(score[[c for c in log_cols if c in score.columns]]).withColumn("trade_date", F.to_date("trade_date"))
    sdf.select(*log_cols).write.mode("append").saveAsTable(LOG_TV)
    print(f"✓ forecast: {len(score)} rows scored by v{version} for {len(new_days)} day(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · PD outcomes mature after 7 days

# COMMAND ----------

LOG_PD = fq("credit", "monitoring", "pd_inference_log")
due = spark.sql(f"""SELECT request_id, pd_probability, request_time FROM {LOG_PD}
                    WHERE record_source = 'live' AND label IS NULL
                      AND request_time < current_timestamp() - INTERVAL 7 DAYS""").toPandas()
if len(due):
    p = np.clip(due["pd_probability"].to_numpy() * PD_OUTCOME_UPLIFT, 0, 0.99)
    due["label"] = (rng.random(len(due)) < p).astype(int)
    due["label_time"] = due["request_time"] + pd.Timedelta(days=7)
    spark.createDataFrame(due[["request_id", "label", "label_time"]]).createOrReplaceTempView("matured")
    spark.sql(f"""MERGE INTO {LOG_PD} t USING matured s ON t.request_id = s.request_id
                  WHEN MATCHED THEN UPDATE SET t.label = CAST(s.label AS BIGINT), t.label_time = s.label_time""")
print(f"✓ PD outcomes matured: {len(due)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · PD endpoint traffic for today

# COMMAND ----------

FEATURES = ["country", "product", "sector", "annual_revenue_usd_m", "debt_to_ebitda", "interest_coverage",
            "utilisation_pct", "days_past_due_max_12m", "internal_rating", "years_relationship",
            "collateral_coverage", "requested_limit_usd_m"]
try:
    w.api_client.do("GET", f"/api/2.0/serving-endpoints/{PD_ENDPOINT}")
    sample = spark.sql(f"""SELECT {', '.join(FEATURES)} FROM {fq('credit', 'raw', 'pd_scoring_requests')}
                           WHERE request_time >= (SELECT max(request_time) FROM {fq('credit', 'raw', 'pd_scoring_requests')}) - INTERVAL 14 DAYS
                           ORDER BY rand() LIMIT {PD_DAILY_REQUESTS}""").toPandas()
    for c in ["annual_revenue_usd_m", "debt_to_ebitda", "interest_coverage", "collateral_coverage", "requested_limit_usd_m"]:
        sample[c] = (sample[c] * rng.lognormal(0, 0.05, len(sample))).round(3)
    records = json.loads(sample.to_json(orient="records"))
    served = 0
    for i in range(0, len(records), 25):
        for attempt in range(6):
            try:
                served += len(w.api_client.do("POST", f"/serving-endpoints/{PD_ENDPOINT}/invocations",
                                              body={"dataframe_records": records[i:i + 25]}).get("predictions", []))
                break
            except Exception:
                import time
                time.sleep(20 * (attempt + 1))     # scale-to-zero wake-up
    print(f"✓ PD traffic: {served} predictions served")
except Exception as e:
    print(f"PD endpoint not available, skipping traffic ({str(e)[:150]})")
