# Databricks notebook source
# MAGIC %md
# MAGIC # Fusion ROC – Daily Threat Landscape
# MAGIC ## Bronze → Silver for `azure_o365` (UKS region) + Delta Share to EA
# MAGIC
# MAGIC **Runs in:** `scdbw-splunk-az-uks-prod` (UKS) – must finish before the EA job (`01_bronze_to_silver_ea`) starts.
# MAGIC **Reads:** `data_cyber_prod_uks.azure.azure_o365` (Kafka/Event Hub JSON payload in `message`)
# MAGIC **Writes:** `{target_catalog}.{target_schema}.slv_email_policy_event` – same schema as the Proofpoint feed in EA
# MAGIC
# MAGIC ### Why a separate notebook
# MAGIC `azure_o365` holds subject, sender and recipient data for every SCB mailbox and sits in UKS by design. Rather than
# MAGIC federating or sharing the raw Bronze table, this notebook **parses and minimises in region** and only the conformed
# MAGIC Silver table is shared (Databricks-to-Databricks Delta Sharing). Nothing leaves UKS except:
# MAGIC * event timestamp, direction, policy / rule names, action taken
# MAGIC * sender **domain** and SHA-256 **hash** of the sender address (no clear-text address)
# MAGIC * recipient **domains** and counts (no addresses)
# MAGIC * SHA-256 **hash** of the subject (lets EA count repeats / correlate with Proofpoint without seeing the text)
# MAGIC * attachment count, country
# MAGIC
# MAGIC Section 4 contains the one-time share setup SQL (run by a metastore admin).

# COMMAND ----------

# MAGIC %md ## 0. Parameters

# COMMAND ----------

dbutils.widgets.text("target_catalog", "data_cyber_prod_uks", "Target catalog (UKS)")
dbutils.widgets.text("target_schema", "fusion_silver", "Target schema")
dbutils.widgets.text("snapshot_date", "", "Snapshot date (YYYY-MM-DD, blank = today UTC)")
dbutils.widgets.text("source_table", "data_cyber_prod_uks.azure.azure_o365", "Bronze source")
dbutils.widgets.text("internal_domains", "sc.com,standardchartered.com", "Internal email domains (comma-separated)")
dbutils.widgets.dropdown("full_refresh", "false", ["true", "false"], "Full refresh")

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F

CATALOG = dbutils.widgets.get("target_catalog")
SCHEMA = dbutils.widgets.get("target_schema")
SILVER = f"{CATALOG}.{SCHEMA}"
SOURCE = dbutils.widgets.get("source_table")
FULL_REFRESH = dbutils.widgets.get("full_refresh") == "true"
INTERNAL_RE = "@(" + "|".join(d.strip().replace(".", "\\\\.") for d in dbutils.widgets.get("internal_domains").split(",")) + ")$"

_sd = dbutils.widgets.get("snapshot_date").strip()
SNAPSHOT_DATE = _sd if _sd else datetime.now(timezone.utc).date().isoformat()
RUN_TS = datetime.now(timezone.utc)
RUN_ID = f"uks_o365_silver_{SNAPSHOT_DATE}_{RUN_TS.strftime('%H%M%S')}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")
print(f"Run {RUN_ID}\nSnapshot date : {SNAPSHOT_DATE}\nSource        : {SOURCE}\nTarget        : {SILVER}.slv_email_policy_event")

# COMMAND ----------

# MAGIC %md ## 1. Payload field map
# MAGIC `azure_o365.message` is a JSON string. The paths below assume the Office 365 Management Activity / Exchange message-trace shape.
# MAGIC **Validate against a real record** (`SELECT message FROM ... LIMIT 5`) and adjust – nothing else in the notebook needs to change.

# COMMAND ----------

O365_FIELDS = {
    "event_id":     "$.Id",
    "event_ts":     "$.CreationTime",
    "operation":    "$.Operation",
    "sender":       "$.Sender",
    "recipients":   "$.Recipients",              # array or ';'-joined
    "subject":      "$.Subject",
    "policy_name":  "$.PolicyDetails[0].PolicyName",
    "rule_name":    "$.PolicyDetails[0].Rules[0].RuleName",
    "action_taken": "$.PolicyDetails[0].Rules[0].Actions[0]",
    "severity":     "$.PolicyDetails[0].Rules[0].Severity",
    "direction":    "$.Directionality",
    "attachments":  "$.ExchangeMetaData.Attachments",   # array
    "user_country": "$.UserCountry",
}

def j(field):
    return f"get_json_object(message, '{O365_FIELDS[field]}')"

# Peek at a sample payload to confirm the map (safe – stays in UKS)
display(spark.sql(f"SELECT ingestion_timestamp, left(message, 400) AS message_head FROM {SOURCE} WHERE ingestion_date = DATE'{SNAPSHOT_DATE}' LIMIT 3"))

# COMMAND ----------

# MAGIC %md ## 2. Parse, minimise and write `slv_email_policy_event`

# COMMAND ----------

o365_sql = f"""
WITH raw AS (
    SELECT message, ingestion_date, ingestion_timestamp
    FROM {SOURCE}
    WHERE ingestion_date BETWEEN date_sub(DATE'{SNAPSHOT_DATE}', 2) AND DATE'{SNAPSHOT_DATE}'
      AND message IS NOT NULL
),
parsed AS (
    SELECT
        coalesce({j("event_id")}, sha2(message, 256))                               AS event_id,
        'O365'                                                                     AS source_system,
        coalesce(try_cast({j("event_ts")} AS TIMESTAMP), ingestion_timestamp)       AS event_ts,
        lower(coalesce({j("direction")}, {j("operation")}, 'unknown'))              AS direction,
        {j("sender")}                                                              AS sender_raw,
        coalesce(from_json({j("recipients")}, 'array<string>'),
                 split({j("recipients")}, '[;,]'))                                  AS recipients_arr,
        {j("subject")}                                                             AS subject_raw,
        {j("policy_name")}                                                         AS policy_name,
        {j("rule_name")}                                                           AS rule_name,
        {j("action_taken")}                                                        AS action_taken,
        CASE WHEN lower({j("severity")}) IN ('critical')          THEN 'Critical'
             WHEN lower({j("severity")}) IN ('high')              THEN 'High'
             WHEN lower({j("severity")}) IN ('medium','moderate') THEN 'Medium'
             WHEN lower({j("severity")}) IN ('low','informational','info') THEN 'Low'
             ELSE 'Unknown' END                                                    AS severity_norm,
        coalesce(from_json({j("attachments")}, 'array<string>'), array())           AS attachments_arr,
        {j("user_country")}                                                        AS country,
        ingestion_date
    FROM raw
)
SELECT
    event_id,
    source_system,
    event_ts,
    direction,
    lower(regexp_extract(sender_raw, '@(.+)$', 1))                                                   AS sender_domain,
    sha2(lower(trim(sender_raw)), 256)                                                               AS sender_hash,
    array_distinct(transform(recipients_arr, r -> lower(regexp_extract(trim(r), '@(.+)$', 1))))      AS recipient_domains,
    size(recipients_arr)                                                                              AS recipient_count,
    size(filter(recipients_arr, r -> lower(trim(r)) NOT RLIKE '{INTERNAL_RE}'))                      AS external_recipient_count,
    sha2(lower(trim(subject_raw)), 256)                                                              AS subject_hash,
    policy_name,
    rule_name,
    action_taken,
    severity_norm,
    size(attachments_arr) > 0                                                                         AS has_attachment,
    size(attachments_arr)                                                                             AS attachment_count,
    country,
    size(filter(recipients_arr, r -> lower(trim(r)) NOT RLIKE '{INTERNAL_RE}')) > 0                   AS is_external,
    'UKS'                                                                                             AS source_region,
    ingestion_date                                                                                    AS source_ingestion_date
FROM parsed
WHERE to_date(event_ts) BETWEEN date_sub(DATE'{SNAPSHOT_DATE}', 1) AND DATE'{SNAPSHOT_DATE}'
  AND policy_name IS NOT NULL                                     -- keep only policy-triggering mail; drop this line to keep all traffic volume
"""

df = (spark.sql(o365_sql)
        .dropDuplicates(["event_id"])
        .withColumn("snapshot_date", F.lit(SNAPSHOT_DATE).cast("date"))
        .withColumn("_run_id", F.lit(RUN_ID))
        .withColumn("_loaded_at", F.lit(RUN_TS)))

target = f"{SILVER}.slv_email_policy_event"
w = df.write.format("delta").mode("overwrite").option("mergeSchema", "true").partitionBy("snapshot_date")
if not FULL_REFRESH:
    w = w.option("replaceWhere", f"snapshot_date = DATE'{SNAPSHOT_DATE}'")
w.saveAsTable(target)

n = spark.table(target).where(F.col("snapshot_date") == F.lit(SNAPSHOT_DATE)).count()
print(f"{target}: {n:,} rows for {SNAPSHOT_DATE}")

# Sharing with history lets EA read this table as a streaming source / use CDF later.
spark.sql(f"ALTER TABLE {target} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

# MAGIC %md ## 3. Privacy check – confirm nothing identifying is in the shared table
# MAGIC Fails the run if any column that would carry clear-text addresses or subjects is present.

# COMMAND ----------

FORBIDDEN = {"sender", "sender_raw", "subject", "subject_raw", "recipients", "recipients_arr", "message", "body", "user_name", "first_name", "last_name"}
present = FORBIDDEN & {c.lower() for c in spark.table(target).columns}
if present:
    raise RuntimeError(f"Shared table contains identifying columns – remove before sharing: {sorted(present)}")
print("Privacy check passed – shared columns:", spark.table(target).columns)

display(spark.sql(f"""
    SELECT source_system, direction, policy_name, count(*) AS events,
           sum(CASE WHEN is_external THEN 1 ELSE 0 END) AS external_events,
           sum(CASE WHEN has_attachment THEN 1 ELSE 0 END) AS with_attachment
    FROM {target} WHERE snapshot_date = DATE'{SNAPSHOT_DATE}'
    GROUP BY 1,2,3 ORDER BY events DESC LIMIT 20"""))

# COMMAND ----------

# MAGIC %md ## 4. One-time share setup (metastore admin, UKS side)
# MAGIC Databricks-to-Databricks sharing within the same account – no tokens. Run once; the daily job does not need to touch this.
# MAGIC
# MAGIC ```sql
# MAGIC -- UKS metastore admin
# MAGIC CREATE SHARE IF NOT EXISTS fusion_silver_uks_share
# MAGIC   COMMENT 'Fusion ROC – conformed, minimised O365 policy events for the EA Daily Threat Landscape pipeline';
# MAGIC
# MAGIC ALTER SHARE fusion_silver_uks_share
# MAGIC   ADD TABLE data_cyber_prod_uks.fusion_silver.slv_email_policy_event
# MAGIC   WITH HISTORY                                   -- enables streaming / CDF reads on the EA side
# MAGIC   PARTITION (snapshot_date >= DATE'2026-01-01');  -- optional: limit what is exposed
# MAGIC
# MAGIC -- Recipient = the EA metastore. Get the sharing identifier from EA:  SELECT CURRENT_METASTORE();
# MAGIC CREATE RECIPIENT IF NOT EXISTS ea_fusion_recipient
# MAGIC   USING ID 'azure:<ea-region>:<ea-metastore-uuid>'
# MAGIC   COMMENT 'EA Fusion ROC workspace';
# MAGIC
# MAGIC GRANT SELECT ON SHARE fusion_silver_uks_share TO RECIPIENT ea_fusion_recipient;
# MAGIC ```
# MAGIC
# MAGIC Then on the **EA** side (metastore admin):
# MAGIC ```sql
# MAGIC -- Provider name appears automatically for same-account D2D shares
# MAGIC SHOW PROVIDERS;
# MAGIC CREATE CATALOG IF NOT EXISTS fusion_uks_shared USING SHARE <uks_provider_name>.fusion_silver_uks_share;
# MAGIC GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG fusion_uks_shared TO `fusion-roc-pipeline-sp`;
# MAGIC ```
# MAGIC `fusion_uks_shared.fusion_silver.slv_email_policy_event` is what notebook 01 reads (widget `uks_shared_catalog`).

# COMMAND ----------

dbutils.notebook.exit(f'{{"run_id":"{RUN_ID}","snapshot_date":"{SNAPSHOT_DATE}","rows":{n}}}')
