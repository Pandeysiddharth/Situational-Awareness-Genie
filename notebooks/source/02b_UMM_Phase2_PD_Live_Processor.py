# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # UMM Demo · PD live inference processor
# MAGIC
# MAGIC Parses the AI Gateway inference table (`credit_monitoring.pd_endpoint_payload`) and appends new successful requests to `credit_monitoring.pd_inference_log` with `record_source = 'live'`. It's idempotent: requests already in the log are skipped.
# MAGIC
# MAGIC Run it about an hour after sending endpoint traffic. Phase 4 will schedule it daily, before the 02:00 UTC monitor refresh.
# MAGIC
# MAGIC In production, `model_version` would come from joining `served_entity_id` to `system.serving.served_entities`. For the demo, it's read from the endpoint's current config, since only one version is served.

# COMMAND ----------

USE_SINGLE_CATALOG = True
SINGLE_CATALOG = "demo_unified_model_monitoring"
MULTI_CATALOGS = {"credit": "umm_demo_cib_credit"}
PD_ENDPOINT = "umm-demo-pd-endpoint"


def fq(area, schema, table):
    if USE_SINGLE_CATALOG:
        return f"{SINGLE_CATALOG}.{area}_{schema}.{table}"
    return f"{MULTI_CATALOGS[area]}.{schema}.{table}"


PAYLOAD = fq("credit", "monitoring", "pd_endpoint_payload")
LOG = fq("credit", "monitoring", "pd_inference_log")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from databricks.sdk import WorkspaceClient

if not spark.catalog.tableExists(PAYLOAD):
    dbutils.notebook.exit(f"{PAYLOAD} doesn't exist yet. Inference tables can take up to an hour to appear after the first request.")

w = WorkspaceClient()
ep = w.api_client.do("GET", f"/api/2.0/serving-endpoints/{PD_ENDPOINT}")
served_version = str(ep["config"]["served_entities"][0]["entity_version"])
served_name = ep["config"]["served_entities"][0]["name"]

FEATURES = T.StructType([
    T.StructField("country", T.StringType()), T.StructField("product", T.StringType()),
    T.StructField("sector", T.StringType()), T.StructField("annual_revenue_usd_m", T.DoubleType()),
    T.StructField("debt_to_ebitda", T.DoubleType()), T.StructField("interest_coverage", T.DoubleType()),
    T.StructField("utilisation_pct", T.DoubleType()), T.StructField("days_past_due_max_12m", T.LongType()),
    T.StructField("internal_rating", T.LongType()), T.StructField("years_relationship", T.LongType()),
    T.StructField("collateral_coverage", T.DoubleType()), T.StructField("requested_limit_usd_m", T.DoubleType()),
])
REQ = T.StructType([T.StructField("dataframe_records", T.ArrayType(FEATURES))])
RESP = T.StructType([T.StructField("predictions", T.ArrayType(T.StructType([
    T.StructField("prediction", T.IntegerType()), T.StructField("pd_probability", T.DoubleType())])))])

raw = spark.table(PAYLOAD)
time_col = (F.col("request_time") if "request_time" in raw.columns
            else (F.col("timestamp_ms") / 1000).cast("timestamp"))
rid_col = "databricks_request_id" if "databricks_request_id" in raw.columns else "request_id"

parsed = (raw.where(F.col("status_code") == 200)
          .select(F.col(rid_col).alias("rid"), time_col.alias("request_time"),
                  F.from_json("request", REQ).alias("req"), F.from_json("response", RESP).alias("resp"))
          .select("rid", "request_time", F.col("req.dataframe_records").alias("recs"),
                  F.col("resp.predictions").alias("preds"))
          .select("rid", "request_time", F.arrays_zip("recs", "preds").alias("z"))
          .select("rid", "request_time", F.posexplode("z").alias("pos", "r"))
          .select(F.concat_ws("-", "rid", "pos").alias("request_id"), "request_time",
                  F.lit(served_version).alias("model_version"),
                  *[F.col(f"r.recs.{f.name}").alias(f.name) for f in FEATURES.fields],
                  F.lit(None).cast("string").alias("counterparty_id"),
                  F.col("r.preds.pd_probability").alias("pd_probability"),
                  F.col("r.preds.prediction").alias("prediction"),
                  F.lit(None).cast("long").alias("label"), F.lit(None).cast("timestamp").alias("label_time"),
                  F.lit(served_name).alias("served_entity"), F.lit("live").alias("record_source")))

# Align to the log's column order and types, then append only new requests
log_schema = spark.table(LOG).schema
aligned = parsed.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in log_schema.fields])
existing = spark.table(LOG).where("record_source = 'live'").select("request_id")
new_rows = aligned.join(existing, "request_id", "left_anti")
n = new_rows.count()
if n:
    new_rows.write.mode("append").saveAsTable(LOG)
print(f"✓ appended {n} live requests to {LOG} (model_version {served_version})")
display(spark.sql(f"SELECT record_source, model_version, count(*) AS n, max(request_time) AS latest FROM {LOG} GROUP BY ALL ORDER BY 1, 2"))
