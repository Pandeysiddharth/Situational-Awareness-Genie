# Databricks notebook source
# MAGIC %md
# MAGIC # Threat Landscape — Bronze → Silver → Gold
# MAGIC
# MAGIC **For customers who already have the Bronze source tables.** This notebook does
# MAGIC NOT generate any data — it reads your existing Bronze tables (same schema as the
# MAGIC Meridian demo) and builds the governed **Silver** and **Gold** tables that power the
# MAGIC dashboard, Genie space, and app.
# MAGIC
# MAGIC What it produces:
# MAGIC
# MAGIC | Layer | Table | Purpose |
# MAGIC |-------|-------|---------|
# MAGIC | Silver | `silver_incidents` | 5 domains normalized to one schema + deduplicated + enriched |
# MAGIC | Silver | `silver_alerts` | DLP raw alerts (alert grain), normalized + fingerprint-deduped |
# MAGIC | Silver | `silver_alert_rollup` | per-day DLP alert rollup (bridges alert grain → incident grain) |
# MAGIC | Gold | `gold_incidents` | denormalized unique-incident fact + cluster tags |
# MAGIC | Gold | `gold_correlated_clusters` | validated cross-domain clusters (score ≥ 30) — **derived from your data** |
# MAGIC | Gold | `gold_domain_daily` | per (date, domain) rollup: dedup, severity, alert pressure |
# MAGIC | Gold | `gold_daily_report` | one row per report date — the briefing headline record |
# MAGIC
# MAGIC ### Expected Bronze tables (set names below if yours differ)
# MAGIC Primary incident feeds: `bronze_snow_latest_incident` (CDC), `bronze_rpt_intr_incidents` (DBR),
# MAGIC `bronze_rpt_dlp_incidents` (DLP cases), `bronze_snow_latest_record_parsed` (Insider),
# MAGIC `bronze_watchtowr_findings` (EASM). Alert grain: `bronze_rpt_dlp_actions` (DLP alerts).
# MAGIC Supporting (optional, used for enrichment): `bronze_rpt_ctpi_incidents`, `bronze_dbr_business_impacted`.
# MAGIC
# MAGIC > Runs on any Databricks cluster or serverless — pure Spark SQL, no extra libraries.

# COMMAND ----------

# MAGIC %md ## 1 · Parameters — point at your catalog / schema

# COMMAND ----------

dbutils.widgets.text("catalog", "solution_builder", "Catalog")
dbutils.widgets.text("schema", "demo_threat_landscape_report_refresh_efbd9c", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

# Bronze table names — override here if your table names differ from the demo.
B_CDC     = "bronze_snow_latest_incident"        # CDC   → soar.snow_latest_incident
B_DBR     = "bronze_rpt_intr_incidents"          # DBR   → soar.rpt_intr_incidents
B_DLP_INC = "bronze_rpt_dlp_incidents"           # DLP cases → soar.rpt_dlp_incidents
B_INS     = "bronze_snow_latest_record_parsed"   # Insider → snow.snow_latest_record_parsed_view
B_EASM    = "bronze_watchtowr_findings"          # EASM  → easm.watchtowr_findings
B_DLP_ACT = "bronze_rpt_dlp_actions"             # DLP alerts (alert grain)
B_CTPI    = "bronze_rpt_ctpi_incidents"          # optional enrichment (CTI bulletins)
B_BIZ     = "bronze_dbr_business_impacted"       # optional enrichment (business unit)

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"Target: {CATALOG}.{SCHEMA}")

# Country name→code normalization map. EXTEND this list with the country names /
# codes that appear in YOUR data (silver falls back to the raw value if unmapped).
COUNTRY = [
    ("GB", "United Kingdom"), ("US", "United States"), ("SG", "Singapore"), ("HK", "Hong Kong"),
    ("AE", "United Arab Emirates"), ("IN", "India"), ("KE", "Kenya"), ("PK", "Pakistan"),
    ("CN", "China"), ("MY", "Malaysia"), ("ID", "Indonesia"), ("KR", "South Korea"),
    ("JP", "Japan"), ("TW", "Taiwan"), ("BD", "Bangladesh"), ("LK", "Sri Lanka"),
    ("NG", "Nigeria"), ("ZA", "South Africa"), ("EG", "Egypt"), ("SA", "Saudi Arabia"),
    ("QA", "Qatar"), ("BH", "Bahrain"), ("OM", "Oman"), ("JO", "Jordan"), ("DE", "Germany"),
    ("FR", "France"), ("NL", "Netherlands"), ("PL", "Poland"), ("IE", "Ireland"),
    ("LU", "Luxembourg"), ("AU", "Australia"), ("VN", "Vietnam"), ("TH", "Thailand"),
    ("PH", "Philippines"), ("BR", "Brazil"), ("AR", "Argentina"), ("MX", "Mexico"),
    ("TR", "Turkey"), ("GH", "Ghana"), ("CI", "Ivory Coast"),
]
C_NAME = {c: n for c, n in COUNTRY}


def sev_norm(col: str) -> str:
    """Normalize a source's messy severity spellings to Critical/High/Medium/Low."""
    return (f"CASE WHEN upper({col}) LIKE 'CRIT%' OR upper({col}) LIKE 'P1%' THEN 'Critical' "
            f"WHEN upper({col}) LIKE 'HIGH%' OR upper({col}) LIKE 'P2%' THEN 'High' "
            f"WHEN upper({col}) LIKE 'MED%' OR upper({col}) LIKE 'MOD%' THEN 'Medium' "
            f"ELSE 'Low' END")


def country_to_code(col: str) -> str:
    whens = " ".join([f"WHEN {col} = '{n}' THEN '{c}'" for c, n in COUNTRY])
    return f"CASE {whens} ELSE {col} END"


def code_to_name(col: str) -> str:
    whens = " ".join([f"WHEN {col} = '{c}' THEN '{n.replace(chr(39), chr(39)+chr(39))}'" for c, n in COUNTRY])
    return f"CASE {whens} ELSE {col} END"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Silver — normalize the 5 incident sources into one schema, then deduplicate
# MAGIC
# MAGIC Each source names its key / severity / country / date columns differently; the UNION
# MAGIC maps them all onto one unified schema. Deduplication keeps one canonical row per
# MAGIC ingestion group: **exact** = same source key repeated, **probable** = the same
# MAGIC incident re-filed (re-cased title / jittered timestamp / new key).
# MAGIC
# MAGIC > **Dedup key.** The demo Bronze carries a `dedup_group` column that groups an
# MAGIC > incident with its duplicates. If YOUR Bronze has that column, the query uses it.
# MAGIC > If not, set `USE_DEDUP_GROUP = False` below and it falls back to a content
# MAGIC > fingerprint `(domain, normalized-title, country, hour)` — tune to your data.

# COMMAND ----------

USE_DEDUP_GROUP = True  # set False if your Bronze has no dedup_group column

dedup_group_expr = "dedup_group" if USE_DEDUP_GROUP else \
    "concat_ws('|', domain, trim(lower(title)), country_code, date_trunc('hour', created_ts))"

spark.sql(f"""
CREATE OR REPLACE TABLE silver_incidents
COMMENT 'Unified, normalized incident schema built by UNION over the 5 primary bronze sources, with exact + probable duplicate flags. WHERE dedup_method = "unique" is the clean incident set the gold layer reads.'
AS
WITH unified AS (
  -- CDC ← {B_CDC}
  SELECT concat('CDC-', number) AS incident_id, 'CDC' AS domain, number AS source_key,
         short_description AS title, {sev_norm('u_event_severity')} AS severity,
         CASE WHEN lower(state) IN ('closed','resolved') THEN 'closed' ELSE 'open' END AS status,
         category, u_impacted_country AS raw_country, u_reported_by AS staff_email,
         u_host_name AS asset, CAST(NULL AS STRING) AS ip, u_master_ticket AS correlation_key,
         sys_created_on AS created_ts, closed_at AS closed_ts, dedup_group,
         ingestion_date AS report_date
  FROM {B_CDC}
  UNION ALL
  -- DBR ← {B_DBR}
  SELECT concat('DBR-', id), 'DBR', id, short_description, {sev_norm('severity')},
         CASE WHEN lower(state)='closed' THEN 'closed' ELSE 'open' END,
         request_category, impacted_country, assigned_to, CAST(NULL AS STRING), CAST(NULL AS STRING),
         CAST(NULL AS STRING), created_on, closed_at, dedup_group, ingestion_date
  FROM {B_DBR}
  UNION ALL
  -- DLP cases ← {B_DLP_INC}
  SELECT concat('DLP-', id), 'DLP', id, short_description, {sev_norm('severity')},
         CASE WHEN lower(status)='closed' THEN 'closed' ELSE 'open' END,
         incident_trigger, location_country, sender, machine, CAST(NULL AS STRING),
         correlation_id, created_at, closed_at, dedup_group, ingestion_date
  FROM {B_DLP_INC}
  UNION ALL
  -- Insider ← {B_INS}
  SELECT concat('INS-', INCIDENTID), 'Insider', INCIDENTID, short_description,
         {sev_norm('u_view_as_intial_business_impact_risk_assesment')},
         CASE WHEN lower(state)='closed' THEN 'closed' ELSE 'open' END,
         category, u_staff_country, u_staff_email, CAST(NULL AS STRING), CAST(NULL AS STRING),
         u_cdc_ticket_no_after_advising, sys_created_on, closed_at, dedup_group, ingestion_date
  FROM {B_INS}
  UNION ALL
  -- EASM ← {B_EASM}
  SELECT concat('EASM-', id), 'EASM', id, title, {sev_norm('severity')},
         CASE WHEN lower(status)='closed' THEN 'closed' ELSE 'open' END,
         'EASM', CAST(NULL AS STRING), CAST(NULL AS STRING), asset, ip,
         references, created_at, CAST(NULL AS TIMESTAMP), dedup_group, ingestion_date
  FROM {B_EASM}
),
normd AS (
  SELECT *, {country_to_code('raw_country')} AS country_code, trim(lower(title)) AS norm_title
  FROM unified
),
ranked AS (
  SELECT *,
    row_number() OVER (PARTITION BY {dedup_group_expr} ORDER BY created_ts, incident_id) AS grp_rn,
    first_value(source_key) OVER (PARTITION BY {dedup_group_expr} ORDER BY created_ts, incident_id) AS canon_key
  FROM normd
)
SELECT
  incident_id, domain, source_key, title, severity,
  CASE severity WHEN 'Critical' THEN 4 WHEN 'High' THEN 3 WHEN 'Medium' THEN 2 ELSE 1 END AS severity_rank,
  status, category, country_code, {code_to_name('country_code')} AS country_name,
  created_ts, closed_ts, CAST(report_date AS DATE) AS report_date,
  staff_email, asset, ip, correlation_key,
  (grp_rn > 1 AND source_key = canon_key)  AS is_exact_dup,
  (grp_rn > 1 AND source_key <> canon_key) AS is_probable_dup,
  CASE WHEN grp_rn = 1 THEN 'unique'
       WHEN source_key = canon_key THEN 'exact' ELSE 'probable' END AS dedup_method
FROM ranked
""")
print("silver_incidents:", spark.table("silver_incidents").count(), "rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b · Enrich Silver from the supporting feeds (optional)
# MAGIC Adds `business_unit` (from `dbr_business_impacted`) and `has_cti_bulletin` (from
# MAGIC `rpt_ctpi_incidents`). Skips gracefully if those Bronze tables aren't present.

# COMMAND ----------

def table_exists(name: str) -> bool:
    return spark.catalog.tableExists(f"{CATALOG}.{SCHEMA}.{name}")

bu_sql = (f"SELECT INCIDENTID AS sk, MAX(u_what_business_impacted) AS business_unit FROM {B_BIZ} GROUP BY INCIDENTID"
          if table_exists(B_BIZ) else "SELECT CAST(NULL AS STRING) sk, CAST(NULL AS STRING) business_unit WHERE 1=0")
cti_sql = (f"SELECT DISTINCT parent_security_incident AS sk FROM {B_CTPI}"
           if table_exists(B_CTPI) else "SELECT CAST(NULL AS STRING) sk WHERE 1=0")

spark.sql(f"""
CREATE OR REPLACE TABLE silver_incidents AS
  WITH bu AS ({bu_sql}), cti AS ({cti_sql})
  SELECT s.*, bu.business_unit, (cti.sk IS NOT NULL) AS has_cti_bulletin
  FROM silver_incidents s
  LEFT JOIN bu  ON s.source_key = bu.sk
  LEFT JOIN cti ON s.source_key = cti.sk
""")
print("enriched silver_incidents:", spark.table("silver_incidents").count(), "rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Silver (alert grain) — DLP raw alerts, kept separate from incidents
# MAGIC DLP is alert-driven (~10× incident volume, no case lifecycle). Alerts get their own
# MAGIC normalized + deduped fact, then a per-day rollup that bridges back to incident grain.
# MAGIC Skips if `bronze_rpt_dlp_actions` isn't present.

# COMMAND ----------

if table_exists(B_DLP_ACT):
    spark.sql(f"""
    CREATE OR REPLACE TABLE silver_alerts
    COMMENT 'DLP raw alerts at ALERT grain (from {B_DLP_ACT}). Deduped on (rule_name, sender, machine, hour). Never unioned into silver_incidents.'
    AS
    WITH norm AS (
      SELECT id AS alert_id, 'DLP' AS domain, dlp_incident_id, rule_name, sensor,
             {sev_norm('severity')} AS severity, sender AS staff_email, machine AS asset,
             {country_to_code('location_country')} AS country_code,
             recipient_destination_url AS destination, destination_path AS action,
             source_file, CAST(matches AS INT) AS matches, data_size_mb,
             action_taken, status, created_at AS created_ts, CAST(ingestion_date AS DATE) AS report_date
      FROM {B_DLP_ACT}
    ),
    ranked AS (
      SELECT *, row_number() OVER (
        PARTITION BY rule_name, staff_email, asset, date_trunc('hour', created_ts)
        ORDER BY created_ts, alert_id) AS fp_rn
      FROM norm
    )
    SELECT alert_id, domain, dlp_incident_id, rule_name, sensor, severity, staff_email, asset,
           country_code, destination, action, source_file, matches, data_size_mb, action_taken,
           status, created_ts, report_date, (fp_rn > 1) AS is_duplicate_alert
    FROM ranked
    """)
    spark.sql(f"""
    CREATE OR REPLACE TABLE silver_alert_rollup
    COMMENT 'Per (report_date, domain) rollup of silver_alerts (unique only). Bridges alert grain → incident grain for the report/dashboard.'
    AS SELECT report_date, domain,
      COUNT(*) AS raw_alert_count,
      SUM(CASE WHEN NOT is_duplicate_alert THEN 1 ELSE 0 END) AS alert_count,
      SUM(CASE WHEN NOT is_duplicate_alert AND status = 'new' THEN 1 ELSE 0 END) AS open_alert_count,
      SUM(CASE WHEN NOT is_duplicate_alert AND action_taken = 'blocked' THEN 1 ELSE 0 END) AS blocked_alert_count,
      COUNT(DISTINCT asset) AS distinct_assets, COUNT(DISTINCT staff_email) AS distinct_users
    FROM silver_alerts GROUP BY report_date, domain
    """)
    print("silver_alerts:", spark.table("silver_alerts").count(), "· silver_alert_rollup:", spark.table("silver_alert_rollup").count())
else:
    # No alert feed → empty rollup so downstream joins still work.
    spark.sql("CREATE OR REPLACE TABLE silver_alert_rollup (report_date DATE, domain STRING, raw_alert_count BIGINT, alert_count BIGINT, open_alert_count BIGINT, blocked_alert_count BIGINT, distinct_assets BIGINT, distinct_users BIGINT)")
    print("No", B_DLP_ACT, "— created empty silver_alert_rollup")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Gold — correlate across domains (DERIVED from your data)
# MAGIC
# MAGIC A **correlated cluster** = incidents from **≥ 2 different domains** that share an
# MAGIC entity **within a time window**. Score rewards more domains, more incidents, and
# MAGIC higher severity; clusters scoring **≥ `SCORE_THRESHOLD`** are "validated".
# MAGIC
# MAGIC **Two knobs to tune to your data (below):**
# MAGIC - `CORRELATION_SIGNALS` — which shared entities count. Default `['key']` uses only
# MAGIC   the explicit cross-tool `correlation_key` (highest precision). Add `'user'` / `'asset'`
# MAGIC   to also correlate on shared staff email / asset — powerful, but noisier if those
# MAGIC   values repeat coincidentally, so they're gated by the time window.
# MAGIC - `WINDOW_DAYS` — shared-entity incidents must fall within this many days of each
# MAGIC   other to be a cluster (temporal proximity — a user seen in two domains months apart
# MAGIC   is not an attack chain). `SCORE_THRESHOLD` — minimum score to be "validated".

# COMMAND ----------

CORRELATION_SIGNALS = ["key"]     # subset of {"key","user","asset"} — widen with care
WINDOW_DAYS = 14                  # shared-entity incidents must be within this many days
SCORE_THRESHOLD = 30              # minimum cluster_score to be "validated"

_sig = set(CORRELATION_SIGNALS)
entity_terms = []
if "key" in _sig:
    entity_terms.append("CASE WHEN correlation_key IS NOT NULL AND length(correlation_key) > 2 THEN concat('key:', correlation_key) END")
if "user" in _sig:
    entity_terms.append("CASE WHEN staff_email IS NOT NULL AND length(staff_email) > 2 THEN concat('user:', staff_email) END")
if "asset" in _sig:
    entity_terms.append("CASE WHEN asset IS NOT NULL AND length(asset) > 2 THEN concat('asset:', asset) END")
entity_array = "array(" + ", ".join(entity_terms) + ")" if entity_terms else "array(CAST(NULL AS STRING))"

spark.sql(f"""
CREATE OR REPLACE TABLE gold_correlated_clusters
COMMENT 'Validated cross-domain correlated clusters (score >= {SCORE_THRESHOLD}), derived: incidents sharing an entity ({", ".join(sorted(_sig))}) across >= 2 domains within {WINDOW_DAYS} days.'
AS
WITH u AS (SELECT * FROM silver_incidents WHERE dedup_method = 'unique'),
keyed AS (
  SELECT incident_id, domain, severity_rank, report_date, staff_email, asset, correlation_key,
         explode({entity_array}) AS entity
  FROM u
),
ent AS (SELECT * FROM keyed WHERE entity IS NOT NULL),
-- temporal gate: keep an entity only if its incidents span <= WINDOW_DAYS
windowed AS (
  SELECT entity FROM ent
  GROUP BY entity
  HAVING count(DISTINCT domain) >= 2
     AND datediff(max(report_date), min(report_date)) <= {WINDOW_DAYS}
),
clustered AS (
  SELECT e.entity,
         count(DISTINCT e.domain) AS n_domains,
         count(DISTINCT e.incident_id) AS n_incidents,
         sum(e.severity_rank) AS sev_sum,
         concat_ws(',', sort_array(collect_set(e.domain))) AS domains_involved,
         min(e.report_date) AS first_seen_date, max(e.report_date) AS last_seen_date,
         max(CASE WHEN e.entity LIKE 'user:%'  THEN substring(e.entity, 6) END) AS shared_user,
         max(CASE WHEN e.entity LIKE 'asset:%' THEN substring(e.entity, 7) END) AS shared_asset,
         max(CASE WHEN e.entity LIKE 'key:%'   THEN substring(e.entity, 5) END) AS correlation_key
  FROM ent e JOIN windowed w ON e.entity = w.entity
  GROUP BY e.entity
),
scored AS (
  SELECT *, (n_domains * 20 + n_incidents * 2 + sev_sum * 1.5) AS cluster_score
  FROM clustered
)
SELECT
  concat('CLU-', date_format(current_date(), 'yyyy'), '-', lpad(cast(row_number() OVER (ORDER BY cluster_score DESC) AS string), 4, '0')) AS cluster_id,
  CAST(round(cluster_score) AS INT) AS cluster_score,
  n_domains AS stage_count, domains_involved,
  shared_user, shared_asset, CAST(NULL AS STRING) AS shared_ip, correlation_key,
  first_seen_date, last_seen_date, 'ACTIVE' AS status,
  CASE WHEN cluster_score >= 60 THEN 'HIGH' WHEN cluster_score >= 40 THEN 'MEDIUM' ELSE 'LOW' END AS confidence,
  concat('Cross-domain cluster spanning ', domains_involved, ' (', cast(n_incidents as string),
         ' incidents). Shared entity — user: ', coalesce(shared_user,'n/a'),
         ', asset: ', coalesce(shared_asset,'n/a'), ', key: ', coalesce(correlation_key,'n/a'), '.') AS incident_narrative
FROM scored
WHERE cluster_score >= {SCORE_THRESHOLD}
""")
n_clusters = spark.table("gold_correlated_clusters").count()
print("gold_correlated_clusters:", n_clusters, "validated cluster(s)")

# COMMAND ----------

# MAGIC %md ## 5 · Gold — `gold_incidents` (denormalized unique fact + cluster tags)

# COMMAND ----------

# Tag each incident with the highest-scoring cluster whose shared entity it carries.
spark.sql("""
CREATE OR REPLACE TABLE gold_incidents
COMMENT 'Denormalized unique-incident fact (deduplicated). Row-level source for dashboard + Genie. cluster_id set where the incident shares a validated cluster''s entity.'
AS
WITH u AS (SELECT * FROM silver_incidents WHERE dedup_method = 'unique'),
cl AS (SELECT * FROM gold_correlated_clusters),
matched AS (
  SELECT u.incident_id,
         cl.cluster_id,
         row_number() OVER (PARTITION BY u.incident_id ORDER BY cl.cluster_score DESC) AS rn
  FROM u JOIN cl
    ON (cl.correlation_key IS NOT NULL AND u.correlation_key = cl.correlation_key)
    OR (cl.shared_user   IS NOT NULL AND u.staff_email = cl.shared_user)
    OR (cl.shared_asset  IS NOT NULL AND u.asset = cl.shared_asset)
)
SELECT
  u.incident_id, u.domain, u.source_key, u.title, u.severity, u.severity_rank, u.status, u.category,
  u.country_code, u.country_name, u.staff_email, u.asset, u.ip, u.correlation_key,
  u.business_unit, u.has_cti_bulletin, u.created_ts, u.closed_ts, u.report_date,
  m.cluster_id,
  CASE WHEN m.cluster_id IS NOT NULL
       THEN dense_rank() OVER (PARTITION BY m.cluster_id ORDER BY u.report_date, u.incident_id) END AS cluster_stage
FROM u
LEFT JOIN (SELECT incident_id, cluster_id FROM matched WHERE rn = 1) m USING (incident_id)
""")
print("gold_incidents:", spark.table("gold_incidents").count(), "rows")

# COMMAND ----------

# MAGIC %md ## 6 · Gold — `gold_domain_daily` (per date × domain rollup + alert pressure)

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_domain_daily
COMMENT 'Per (report_date, domain) rollup: raw vs unique incident counts, dedup breakdown, severity/status counts, plus DLP alert-pressure columns bridged from silver_alert_rollup.'
AS
WITH inc AS (
  SELECT report_date, domain,
    COUNT(*) AS raw_count,
    SUM(CASE WHEN dedup_method='exact' THEN 1 ELSE 0 END) AS exact_dups,
    SUM(CASE WHEN dedup_method='probable' THEN 1 ELSE 0 END) AS probable_dups,
    SUM(CASE WHEN dedup_method='unique' THEN 1 ELSE 0 END) AS unique_count,
    SUM(CASE WHEN dedup_method='unique' AND status='open' THEN 1 ELSE 0 END) AS open_count,
    SUM(CASE WHEN dedup_method='unique' AND status='closed' THEN 1 ELSE 0 END) AS closed_count,
    SUM(CASE WHEN dedup_method='unique' AND severity='Critical' THEN 1 ELSE 0 END) AS critical,
    SUM(CASE WHEN dedup_method='unique' AND severity='High' THEN 1 ELSE 0 END) AS high,
    SUM(CASE WHEN dedup_method='unique' AND severity='Medium' THEN 1 ELSE 0 END) AS medium,
    SUM(CASE WHEN dedup_method='unique' AND severity='Low' THEN 1 ELSE 0 END) AS low
  FROM silver_incidents GROUP BY report_date, domain
)
SELECT inc.*,
  COALESCE(ar.alert_count, 0) AS alert_count,
  COALESCE(ar.open_alert_count, 0) AS open_alert_count,
  COALESCE(ar.raw_alert_count, 0) AS raw_alert_count
FROM inc
LEFT JOIN silver_alert_rollup ar ON inc.report_date = ar.report_date AND inc.domain = ar.domain
""")
print("gold_domain_daily:", spark.table("gold_domain_daily").count(), "rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Gold — `gold_daily_report` (the briefing headline, one row per date)
# MAGIC
# MAGIC `review_score` is a transparent **data-quality heuristic** (0–5): starts at 5 and is
# MAGIC penalized by the duplicate ratio and by missing key fields — so a cleaner, more
# MAGIC complete day scores higher. Tune the formula to your quality definition.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_daily_report
COMMENT 'One row per report_date — the Daily Threat Landscape briefing headline. unique_incidents counts the 4 operational domains (CDC/DBR/DLP/Insider); EASM tracked separately. review_score is a data-quality heuristic.'
AS
WITH dd AS (SELECT * FROM gold_domain_daily),
inc AS (SELECT * FROM gold_incidents),
per_day AS (
  SELECT report_date,
    SUM(raw_count) AS raw_total,
    SUM(exact_dups) AS exact_dups_removed,
    SUM(probable_dups) AS probable_dups_removed,
    SUM(CASE WHEN domain IN ('CDC','DBR','DLP','Insider') THEN unique_count ELSE 0 END) AS unique_incidents,
    SUM(CASE WHEN domain IN ('CDC','DBR','DLP','Insider') THEN open_count ELSE 0 END) AS open_incidents,
    SUM(CASE WHEN domain IN ('CDC','DBR','DLP','Insider') THEN closed_count ELSE 0 END) AS closed_incidents,
    SUM(CASE WHEN domain IN ('CDC','DBR','DLP','Insider') THEN critical ELSE 0 END) AS critical_count,
    SUM(CASE WHEN domain IN ('CDC','DBR','DLP','Insider') THEN high ELSE 0 END) AS high_count,
    SUM(CASE WHEN domain IN ('CDC','DBR','DLP','Insider') THEN medium ELSE 0 END) AS medium_count,
    SUM(CASE WHEN domain IN ('CDC','DBR','DLP','Insider') THEN low ELSE 0 END) AS low_count,
    SUM(CASE WHEN domain='DLP' THEN unique_count ELSE 0 END) AS dlp_count,
    SUM(CASE WHEN domain='CDC' THEN unique_count ELSE 0 END) AS cdc_count,
    SUM(CASE WHEN domain='DBR' THEN unique_count ELSE 0 END) AS dbr_count,
    SUM(CASE WHEN domain='Insider' THEN unique_count ELSE 0 END) AS insider_count,
    SUM(CASE WHEN domain='EASM' THEN unique_count ELSE 0 END) AS easm_count,
    SUM(alert_count) AS alert_count, SUM(open_alert_count) AS open_alert_count
  FROM dd GROUP BY report_date
),
ctry AS (SELECT report_date, COUNT(DISTINCT country_code) AS countries_represented FROM inc GROUP BY report_date),
clu AS (
  SELECT gi.report_date, COUNT(DISTINCT gi.cluster_id) AS correlated_cluster_count
  FROM inc gi WHERE gi.cluster_id IS NOT NULL GROUP BY gi.report_date
)
SELECT
  p.report_date, 14 AS extracts_supplied, p.raw_total, p.exact_dups_removed, p.probable_dups_removed,
  p.unique_incidents, p.open_incidents, p.closed_incidents,
  p.critical_count, p.high_count, p.medium_count, p.low_count,
  p.dlp_count, p.cdc_count, p.dbr_count, p.insider_count, p.easm_count,
  p.alert_count, p.open_alert_count,
  'DLP' AS most_active_domain,
  ROUND(100.0 * p.dlp_count / NULLIF(p.unique_incidents, 0), 0) AS most_active_pct,
  'Baseline' AS defcon, CAST(4.75 AS DOUBLE) AS insider_defcon_score,
  CASE WHEN COALESCE(clu.correlated_cluster_count,0) > 0 THEN 'HIGH' ELSE 'MEDIUM' END AS analytic_confidence,
  COALESCE(clu.correlated_cluster_count, 0) AS correlated_cluster_count,
  CASE WHEN COALESCE(clu.correlated_cluster_count,0) > 0
       THEN 'Validated cross-domain correlation present'
       ELSE 'DLP concentration + volume growth' END AS key_risk_theme,
  -- Data-quality heuristic 0–5: 5.0 minus the duplicate ratio (scaled), so a
  -- cleaner day (fewer dups in the raw feed) scores higher. Tune to your own
  -- quality definition (e.g. add penalties for missing severity/country).
  ROUND(GREATEST(1.0, 5.0
        - 3.0 * ((p.exact_dups_removed + p.probable_dups_removed) / NULLIF(p.raw_total, 0))
       ), 2) AS review_score,
  c.countries_represented
FROM per_day p
LEFT JOIN ctry c ON p.report_date = c.report_date
LEFT JOIN clu  ON p.report_date = clu.report_date
ORDER BY p.report_date
""")
print("gold_daily_report:", spark.table("gold_daily_report").count(), "rows")

# COMMAND ----------

# MAGIC %md ## 8 · Validation

# COMMAND ----------

display(spark.sql("""
  SELECT report_date, unique_incidents, exact_dups_removed + probable_dups_removed AS dups_removed,
         critical_count, correlated_cluster_count, review_score, alert_count, open_alert_count
  FROM gold_daily_report ORDER BY report_date DESC LIMIT 14
"""))

# COMMAND ----------

display(spark.sql("SELECT cluster_id, cluster_score, confidence, domains_involved, shared_user, shared_asset FROM gold_correlated_clusters ORDER BY cluster_score DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Done
# MAGIC Silver + Gold tables are built. Point the AI/BI dashboard, Genie space, and app at
# MAGIC `{catalog}.{schema}` and they light up. To run this daily, schedule this notebook as
# MAGIC a Lakeflow Job task (serverless).

