# Databricks notebook source
# MAGIC %md
# MAGIC # Fusion ROC – Daily Threat Landscape
# MAGIC ## Bronze → Silver (EA region)
# MAGIC
# MAGIC Builds the Silver layer that feeds the Daily Threat Landscape Gold tables and metric views.
# MAGIC
# MAGIC **Runs in:** `scdbw-splunk-az-ea-prod` (EA)
# MAGIC **Reads:** 12 EA Bronze tables + the UKS-produced `slv_email_policy_event` (via Delta Sharing)
# MAGIC **Writes:** one `snapshot_date` partition per run into `{target_catalog}.{target_schema}` (idempotent – re-running a day replaces that day)
# MAGIC
# MAGIC | Step | Silver table | Bronze inputs |
# MAGIC |---|---|---|
# MAGIC | 1 | `slv_cdc_incident` | `data_prod_ea.soar.snow_latest_incident` |
# MAGIC | 2 | `slv_dbr_incident` | `snow.snow_latest_record_parsed_view`, `snow.pc_scb_mdbn_work`, `snow.snow_dbr_tasks`, `teams_prod_ea.fusion.dbr_business_impacted_view` |
# MAGIC | 3 | `slv_dlp_event` | `soar.rpt_dlp_incidents`, `soar.rpt_dlp_actions` |
# MAGIC | 4 | `slv_insider_incident` | `soar.rpt_intr_incidents` |
# MAGIC | 5 | `slv_ctps_incident` | `soar.rpt_ctpi_incidents` |
# MAGIC | 6 | `slv_easm_finding`, `slv_easm_hunt` | `easm.watchtowr_findings`, `easm.watchtowr_hunts` |
# MAGIC | 7 | `slv_email_policy_event` | `proofpoint.proofpoint_dlp` (EA) ∪ UKS shared `slv_email_policy_event` |
# MAGIC | 8 | `slv_unified_incident` | steps 1–5 (+6 findings) – common schema, exact + probable dedup |
# MAGIC | 9 | `slv_incident_entity` | step 8 – users, assets, IOCs, vendors, incident refs for correlation |
# MAGIC | 10 | `slv_dq_run` | run-level data-quality log (extracts supplied, duplicates removed, cut-off) |
# MAGIC
# MAGIC The UKS table `data_cyber_prod_uks.azure.azure_o365` is **not** read directly here. It is parsed in UKS by
# MAGIC `02_bronze_to_silver_uks_o365` and shared as a conformed table; step 7 unions it in if the share is mounted.

# COMMAND ----------

# MAGIC %md ## 0. Parameters

# COMMAND ----------

dbutils.widgets.text("target_catalog", "data_cyber_prod_ea", "Target catalog")
dbutils.widgets.text("target_schema", "fusion_silver", "Target schema")
dbutils.widgets.text("snapshot_date", "", "Snapshot date (YYYY-MM-DD, blank = today UTC)")
dbutils.widgets.text("uks_shared_catalog", "fusion_uks_shared", "Catalog mounted from the UKS Delta Share")
dbutils.widgets.text("uks_shared_schema", "fusion_silver", "Schema inside the UKS share")
dbutils.widgets.dropdown("full_refresh", "false", ["true", "false"], "Full refresh (rebuild all partitions)")
dbutils.widgets.dropdown("fail_if_uks_missing", "false", ["true", "false"], "Fail if UKS shared table is absent")

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F

CATALOG = dbutils.widgets.get("target_catalog")
SCHEMA = dbutils.widgets.get("target_schema")
SILVER = f"{CATALOG}.{SCHEMA}"
FULL_REFRESH = dbutils.widgets.get("full_refresh") == "true"
FAIL_IF_UKS_MISSING = dbutils.widgets.get("fail_if_uks_missing") == "true"

_sd = dbutils.widgets.get("snapshot_date").strip()
SNAPSHOT_DATE = _sd if _sd else datetime.now(timezone.utc).date().isoformat()
RUN_TS = datetime.now(timezone.utc)
RUN_ID = f"ea_silver_{SNAPSHOT_DATE}_{RUN_TS.strftime('%H%M%S')}"

UKS_EMAIL_TABLE = f"{dbutils.widgets.get('uks_shared_catalog')}.{dbutils.widgets.get('uks_shared_schema')}.slv_email_policy_event"

# Bronze sources (EA). Keep these in one place so a catalog rename is a one-line change.
BRONZE = {
    "cdc_incident":        "data_prod_ea.soar.snow_latest_incident",
    "dbr_parsed":          "data_prod_ea.snow.snow_latest_record_parsed_view",
    "dbr_business":        "teams_prod_ea.fusion.dbr_business_impacted_view",
    "dbr_pega":            "data_prod_ea.snow.pc_scb_mdbn_work",
    "dbr_tasks":           "data_prod_ea.snow.snow_dbr_tasks",
    "insider_incident":    "data_prod_ea.soar.rpt_intr_incidents",
    "ctps_incident":       "data_prod_ea.soar.rpt_ctpi_incidents",
    "dlp_actions":         "data_prod_ea.soar.rpt_dlp_actions",
    "dlp_incidents":       "data_prod_ea.soar.rpt_dlp_incidents",
    "easm_findings":       "data_cyber_prod_ea.easm.watchtowr_findings",
    "easm_hunts":          "data_cyber_prod_ea.easm.watchtowr_hunts",
    "proofpoint_dlp":      "data_cyber_prod_ea.proofpoint.proofpoint_dlp",
}

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")
print(f"Run {RUN_ID}\nSnapshot date : {SNAPSHOT_DATE}\nTarget        : {SILVER}\nFull refresh  : {FULL_REFRESH}")

# COMMAND ----------

# MAGIC %md ## 0b. Shared normalisation helpers
# MAGIC
# MAGIC All source-specific vocab is mapped to one severity scale (`Critical/High/Medium/Low/Unknown`) and one state scale
# MAGIC (`Open/Closed/Unknown`). Extend the lists below as new vocab appears – `slv_dq_run` reports how many rows fall into `Unknown`.

# COMMAND ----------

def sev_norm(col: str) -> str:
    """SQL expression: source severity/priority → Critical / High / Medium / Low / Unknown."""
    return f"""CASE
        WHEN lower(trim(CAST({col} AS STRING))) IN ('critical','p1','1','1 - critical','sev1','s1','severity 1','very high') THEN 'Critical'
        WHEN lower(trim(CAST({col} AS STRING))) IN ('high','p2','2','2 - high','sev2','s2','severity 2','major') THEN 'High'
        WHEN lower(trim(CAST({col} AS STRING))) IN ('medium','moderate','p3','3','3 - moderate','3 - medium','sev3','s3','severity 3') THEN 'Medium'
        WHEN lower(trim(CAST({col} AS STRING))) IN ('low','p4','4','4 - low','sev4','s4','severity 4','informational','info','minor') THEN 'Low'
        ELSE 'Unknown' END"""


def state_norm(col: str) -> str:
    """SQL expression: source state → Open / Closed / Unknown."""
    return f"""CASE
        WHEN lower(trim(CAST({col} AS STRING))) IN ('closed','resolved','cancelled','canceled','closed complete','closed incomplete',
             'closed skipped','done','completed','resolved-closed','dismissed','false positive','rejected','withdrawn','resolved - closed') THEN 'Closed'
        WHEN lower(trim(CAST({col} AS STRING))) IN ('new','open','in progress','in-progress','work in progress','assigned','pending','on hold',
             'awaiting','awaiting info','awaiting user info','awaiting vendor','awaiting evidence','analysis','contain','eradicate','recover',
             'review','draft','submitted','under review','active','triage','investigating','escalated','reopened','monitoring') THEN 'Open'
        WHEN {col} IS NULL OR trim(CAST({col} AS STRING)) = '' THEN 'Unknown'
        ELSE 'Unknown' END"""


def ts(col: str) -> str:
    """SQL expression: tolerant string/timestamp → TIMESTAMP (several ServiceNow / SOAR formats)."""
    return f"""coalesce(
        try_cast({col} AS TIMESTAMP),
        try_to_timestamp(CAST({col} AS STRING), 'yyyy-MM-dd HH:mm:ss'),
        try_to_timestamp(CAST({col} AS STRING), 'dd/MM/yyyy HH:mm:ss'),
        try_to_timestamp(CAST({col} AS STRING), 'dd/MM/yyyy HH:mm'),
        try_to_timestamp(CAST({col} AS STRING), 'dd-MM-yyyy HH:mm:ss'))"""


def nz(col: str) -> str:
    """SQL expression: empty string → NULL, trimmed."""
    return f"nullif(trim(CAST({col} AS STRING)), '')"


def latest(table: str, key: str, order_by: str, has_ingestion_date: bool = True) -> str:
    """Sub-query returning the latest row per key, as-of the snapshot date (bronze tables are cumulative snapshots)."""
    where = f"WHERE ingestion_date <= DATE'{SNAPSHOT_DATE}'" if has_ingestion_date else ""
    return f"""(SELECT * FROM (
                 SELECT b.*, row_number() OVER (PARTITION BY {key} ORDER BY {order_by}) AS _rn
                 FROM {table} b {where}) WHERE _rn = 1)"""


def write_snapshot(df, table: str) -> int:
    """Idempotent write: replace only this snapshot_date partition (or everything on full refresh)."""
    full = f"{SILVER}.{table}"
    df = df.withColumn("snapshot_date", F.lit(SNAPSHOT_DATE).cast("date")) \
           .withColumn("_run_id", F.lit(RUN_ID)) \
           .withColumn("_loaded_at", F.lit(RUN_TS))
    w = df.write.format("delta").mode("overwrite").option("mergeSchema", "true").partitionBy("snapshot_date")
    if not FULL_REFRESH:
        w = w.option("replaceWhere", f"snapshot_date = DATE'{SNAPSHOT_DATE}'")
    w.saveAsTable(full)
    n = spark.table(full).where(F.col("snapshot_date") == F.lit(SNAPSHOT_DATE)).count()
    print(f"  {full:<60} {n:>8,} rows")
    return n


def table_exists(name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False


ROW_COUNTS = {}   # populated for slv_dq_run

# COMMAND ----------

# MAGIC %md ## 1. CDC – `slv_cdc_incident`
# MAGIC Source: ServiceNow security incidents maintained by CDC analysts. One row per `number`.
# MAGIC Adds `cdc_category_group` (Phishing / Credential / Trojan / Scanning / Other) used by the *Signal distribution* panel.

# COMMAND ----------

cdc_sql = f"""
SELECT
    number                                                   AS incident_id,
    'CDC'                                                    AS domain,
    '{BRONZE["cdc_incident"]}'                               AS source_table,
    {nz("state")}                                            AS source_state,
    {state_norm("state")}                                    AS state_norm,
    {nz("u_event_severity")}                                 AS source_severity,
    {sev_norm("coalesce(" + nz("u_event_severity") + ", priority)")} AS severity_norm,
    {nz("priority")}                                         AS priority,
    try_cast(risk_score AS DOUBLE)                           AS risk_score,
    {nz("category")}                                         AS category,
    {nz("subcategory")}                                      AS subcategory,
    {nz("u_fp_subcategory")}                                 AS fp_subcategory,
    CASE
        WHEN lower(concat_ws(' ', category, subcategory, short_description)) RLIKE 'phish'                                 THEN 'Phishing'
        WHEN lower(concat_ws(' ', category, subcategory, short_description)) RLIKE 'credential|password|brute|account takeover|ato' THEN 'Credential'
        WHEN lower(concat_ws(' ', category, subcategory, short_description)) RLIKE 'trojan|malware|ransom|backdoor|rat\\b'  THEN 'Trojan'
        WHEN lower(concat_ws(' ', category, subcategory, short_description)) RLIKE 'scan|recon|probe|enumerat'              THEN 'Scanning'
        ELSE 'Other' END                                     AS cdc_category_group,
    {ts("coalesce(u_event_generation_time, sys_created_on)")} AS opened_at,
    {ts("closed_at")}                                        AS closed_at,
    {ts("sys_updated_on")}                                   AS updated_at,
    {ts("u_initial_response_time")}                          AS initial_response_at,
    {ts("u_containment_time")}                               AS containment_at,
    {nz("u_incident_resolution_time")}                       AS resolution_time_raw,
    coalesce({nz("impacted_country_name")}, {nz("u_impacted_country")}, {nz("user_country")}) AS country,
    {nz("user_department")}                                  AS business_unit,
    {nz("assignment_group")}                                 AS assignment_group,
    {nz("assigned_to")}                                      AS assigned_to,
    {nz("short_description")}                                AS short_description,
    description,
    {nz("user_user_name")}                                   AS primary_user,
    coalesce(user_vip, u_vip_user_involved, false)           AS is_vip,
    {nz("u_host_name")}                                      AS primary_asset,
    {nz("u_master_ticket")}                                  AS master_ticket,
    {nz("u_incident_analysis_outcome")}                      AS analysis_outcome,
    {nz("u_actor_characterization")}                        AS actor_characterisation,
    {nz("u_source_of_infection")}                            AS source_of_infection,
    {nz("u_detective_controls")}                             AS detective_controls,
    {nz("u_preventive_controls")}                            AS preventive_controls,
    CASE WHEN {nz("u_detective_controls")} IS NULL AND {nz("u_preventive_controls")} IS NULL THEN true ELSE false END AS has_control_gap,
    {nz("u_cdc_impact")}                                     AS cdc_impact,
    {nz("close_code")}                                       AS close_code,
    u_escalate_to_tier2                                      AS escalated_to_tier2,
    duplicate_count                                          AS source_duplicate_count,
    {nz("u_tenant")}                                         AS tenant,
    security_tags,
    ingestion_date                                           AS source_ingestion_date
FROM {latest(BRONZE["cdc_incident"], "number", "ingestion_date DESC, sys_updated_on DESC")}
"""
ROW_COUNTS["slv_cdc_incident"] = write_snapshot(spark.sql(cdc_sql), "slv_cdc_incident")

# COMMAND ----------

# MAGIC %md ## 2. DBR – `slv_dbr_incident`
# MAGIC Four inputs joined to one row per ServiceNow `INCIDENTID`:
# MAGIC * `snow_latest_record_parsed_view` – incident body (base)
# MAGIC * `snow_dbr_tasks` – review tasks → regulatory / privacy / banking-secrecy notification flags (aggregated)
# MAGIC * `pc_scb_mdbn_work` (Pega DB Portal) – reportability, BRM status, harm, regulator dates. **Assumed join: `REFERENCEID = number`** – confirm with the DBR team.
# MAGIC * `dbr_business_impacted_view` – business segments impacted (aggregated)

# COMMAND ----------

dbr_sql = f"""
WITH base AS (
    SELECT * FROM {latest(BRONZE["dbr_parsed"], "INCIDENTID", "sys_updated_on DESC", has_ingestion_date=False)}
),
tasks AS (
    SELECT
        INCIDENTID,
        count(*)                                                                               AS task_count,
        sum(CASE WHEN {state_norm("state")} = 'Open' THEN 1 ELSE 0 END)                        AS open_task_count,
        max(CASE WHEN {nz("u_regulatory_body")} IS NOT NULL
                   OR {nz("u_type_notification_banking")} IS NOT NULL
                   OR {nz("u_please_specify_the_mandatory_notification")} IS NOT NULL THEN 1 ELSE 0 END) = 1 AS regulatory_notification_flag,
        max(CASE WHEN {nz("u_type_of_notification_privacy")} IS NOT NULL
                   OR lower(coalesce(u_mandatory_privacy,'')) IN ('yes','true','y') THEN 1 ELSE 0 END) = 1 AS privacy_notification_flag,
        max(CASE WHEN {nz("u_type_notification_banking")} IS NOT NULL
                   OR {nz("u_law_regulation_banking_secrecy")} IS NOT NULL THEN 1 ELSE 0 END) = 1          AS banking_secrecy_notification_flag,
        concat_ws('; ', collect_set({nz("u_regulatory_body")}))                                AS regulatory_bodies,
        concat_ws('; ', collect_set({nz("u_regulation")}))                                     AS regulations,
        max({nz("u_incident_regulatory_impact_for_ciso")})                                     AS regulatory_impact_for_ciso,
        max({nz("u_malicious_criminal_attack")})                                               AS malicious_attack_indicator,
        max({nz("u_security_incident_breach")})                                                AS security_incident_breach,
        max({nz("u_assessment_not_made_within_72hours")})                                      AS assessment_outside_72h
    FROM {BRONZE["dbr_tasks"]}
    GROUP BY INCIDENTID
),
pega AS (
    SELECT * FROM {latest(BRONZE["dbr_pega"], "REFERENCEID", "PXUPDATEDATETIME DESC", has_ingestion_date=False)}
),
biz AS (
    SELECT INCIDENTID,
           concat_ws('; ', collect_set({nz("u_what_business_impacted")})) AS business_impacted_list,
           sum(count)                                                     AS business_impacted_count
    FROM {BRONZE["dbr_business"]}
    GROUP BY INCIDENTID
)
SELECT
    b.INCIDENTID                                             AS incident_id,
    'DBR'                                                    AS domain,
    '{BRONZE["dbr_parsed"]}'                                 AS source_table,
    {nz("b.state")}                                          AS source_state,
    {state_norm("b.state")}                                  AS state_norm,
    coalesce({nz("p.BIZRISKRATING")}, {nz("b.urgency")})     AS source_severity,
    {sev_norm("coalesce(p.BIZRISKRATING, b.urgency)")}       AS severity_norm,
    {nz("b.urgency")}                                        AS priority,
    {nz("b.category")}                                       AS category,
    {nz("p.INCIDENTTYPE")}                                   AS subcategory,
    {ts("coalesce(b.opened_at, b.sys_created_on, b.u_incsubmitteddatetime)")} AS opened_at,
    {ts("coalesce(b.closed_at, p.INCIDENTCLOSUREDATE)")}     AS closed_at,
    {ts("coalesce(b.sys_updated_on, p.PXUPDATEDATETIME)")}   AS updated_at,
    {ts("b.u_incident_first_discovered")}                    AS discovered_at,
    {ts("b.u_when_incident_occur")}                          AS occurred_at,
    coalesce(element_at(b.u_all_country_impacted_by_incident, 1), {nz("p.IMPACTEDCOUNTRY")}, {nz("p.COUNTRY")}, {nz("b.u_reporter_country")}) AS country,
    array_join(b.u_all_country_impacted_by_incident, '; ')   AS countries_impacted_list,
    array_join(b.u_data_breached_country, '; ')              AS data_breached_countries,
    coalesce(bz.business_impacted_list, array_join(b.u_what_business_impacted, '; '), {nz("p.IMPACTEDBUSINESS")}) AS business_unit,
    bz.business_impacted_count,
    {nz("b.assignment_group")}                               AS assignment_group,
    {nz("b.assigned_to")}                                    AS assigned_to,
    {nz("b.short_description")}                              AS short_description,
    b.description,
    coalesce({nz("b.u_bankid_and_name_if_yes_user_name")}, {nz("b.u_staff_email")}, {nz("p.INVOLVEDSTAFFID")}) AS primary_user,
    CAST(NULL AS STRING)                                     AS primary_asset,
    {nz("b.u_cdc_ticket_no_after_advising")}                 AS master_ticket,
    {nz("b.u_insert_dlp_number")}                            AS related_dlp_number,
    coalesce({nz("b.u_exact_records")}, {nz("b.u_how_many_records_involved")}, {nz("b.u_record_estimate")}) AS records_involved_raw,
    try_cast(regexp_replace(coalesce(b.u_exact_records, b.u_how_many_records_involved, b.u_record_estimate), '[^0-9]', '') AS BIGINT) AS records_involved,
    array_join(b.u_type_of_personal_information_impacted, '; ') AS personal_data_types,
    array_join(b.u_main_trigger_of_incident, '; ')           AS incident_triggers,
    array_join(b.u_who_what_caused_databreach, '; ')         AS cause,
    array_join(b.u_how_db_incident_discovered, '; ')         AS discovery_method,
    {nz("b.u_fit_reporting_dbr_criteria")}                   AS fits_dbr_criteria,
    {nz("p.REPORTABILITY")}                                  AS reportability,
    {nz("p.EORPREPORTABILITY")}                              AS eorp_reportability,
    {nz("p.BRMSTATUS")}                                      AS brm_status,
    {nz("p.LOCSTATUS")}                                      AS loc_status,
    {nz("p.DBTSTATUS")}                                      AS dbt_status,
    {nz("p.SIGNIFICANT")}                                    AS significant_flag,
    {nz("p.PRIVACYBREACH")}                                  AS privacy_breach,
    {nz("p.CCBREACH")}                                       AS client_confidentiality_breach,
    {nz("p.HARMLIKELIHOOD")}                                 AS harm_likelihood,
    {nz("p.HARMTYPE")}                                       AS harm_type,
    {nz("p.DATALOSSCLASSIFICATION")}                         AS data_loss_classification,
    {ts("p.PRIVACYBREACHREGULATORDATE")}                     AS privacy_regulator_notified_at,
    {ts("p.BANKINGSECRECYREGULATORDATE")}                    AS banking_secrecy_regulator_notified_at,
    try_cast(p.NUMBEROFDAYSOPEN AS INT)                      AS days_open,
    coalesce(t.regulatory_notification_flag, p.PRIVACYBREACHREGULATORDATE IS NOT NULL OR p.BANKINGSECRECYREGULATORDATE IS NOT NULL, false) AS regulatory_notification_flag,
    coalesce(t.privacy_notification_flag, p.PRIVACYBREACHREGULATORDATE IS NOT NULL, false)        AS privacy_notification_flag,
    coalesce(t.banking_secrecy_notification_flag, p.BANKINGSECRECYREGULATORDATE IS NOT NULL, false) AS banking_secrecy_notification_flag,
    t.regulatory_bodies, t.regulations, t.regulatory_impact_for_ciso, t.malicious_attack_indicator,
    t.security_incident_breach, t.assessment_outside_72h,
    coalesce(t.task_count, 0)                                AS task_count,
    coalesce(t.open_task_count, 0)                           AS open_task_count,
    p.PYID                                                   AS pega_case_id,
    b.active                                                 AS source_active_flag
FROM base b
LEFT JOIN tasks t ON t.INCIDENTID = b.INCIDENTID
LEFT JOIN pega  p ON p.REFERENCEID = b.number
LEFT JOIN biz  bz ON bz.INCIDENTID = b.INCIDENTID
"""
ROW_COUNTS["slv_dbr_incident"] = write_snapshot(spark.sql(dbr_sql), "slv_dbr_incident")

# COMMAND ----------

# MAGIC %md ## 3. DLP – `slv_dlp_event`
# MAGIC Event-level DLP from `rpt_dlp_incidents`, enriched with the escalated-case view in `rpt_dlp_actions`
# MAGIC (**assumed join `actions.case_id = incidents.id`**). Adds:
# MAGIC * `dlp_channel` – Cloud upload / Print / Email / Removable media / Endpoint / Other (keyword rules – tune against real `source` / `destination` values)
# MAGIC * `is_dlp_duplicate` – deterministic dedup on `correlation_id`, falling back to user + policy + file + day. Only non-duplicates are counted in the report ("901 cloud uploads and 218 print events after deterministic deduplication").

# COMMAND ----------

dlp_sql = f"""
WITH inc AS (
    SELECT * FROM {latest(BRONZE["dlp_incidents"], "id", "ingestion_date DESC, updated_at DESC")}
),
act AS (
    SELECT * FROM {latest(BRONZE["dlp_actions"], "case_id", "ingestion_date DESC, updated_at DESC")}
),
enriched AS (
    SELECT
        i.id                                                 AS incident_id,
        'DLP'                                                AS domain,
        '{BRONZE["dlp_incidents"]}'                          AS source_table,
        coalesce({nz("a.status")}, {nz("i.prevention_status")}) AS source_state,
        CASE WHEN a.status IS NOT NULL THEN {state_norm("a.status")}
             WHEN lower(coalesce(i.dismissal_reason,'')) <> '' THEN 'Closed'
             WHEN {ts("i.closed_at")} IS NOT NULL THEN 'Closed'
             ELSE 'Open' END                                 AS state_norm,
        {nz("i.severity")}                                   AS source_severity,
        {sev_norm("i.severity")}                             AS severity_norm,
        CAST(NULL AS STRING)                                 AS priority,
        {nz("i.policy")}                                     AS category,
        {nz("a.incident_trigger")}                           AS subcategory,
        CASE
            WHEN lower(concat_ws(' ', i.source, i.destination, i.destination_path)) RLIKE 'print|spool|lpt|printer'                      THEN 'Print'
            WHEN lower(concat_ws(' ', i.source, i.destination, i.recipient_destination_url)) RLIKE 'onedrive|sharepoint|dropbox|google|gdrive|box\\.com|icloud|wetransfer|cloud|upload|https?://|web' THEN 'Cloud upload'
            WHEN lower(concat_ws(' ', i.source, i.destination)) RLIKE 'usb|removable|external drive|mass storage'                           THEN 'Removable media'
            WHEN lower(concat_ws(' ', i.source, i.destination)) RLIKE 'mail|smtp|exchange|outlook' OR i.sent_on IS NOT NULL                 THEN 'Email'
            WHEN lower(coalesce(i.source,'')) RLIKE 'endpoint|agent|clipboard|screen'                                                      THEN 'Endpoint'
            ELSE 'Other' END                                 AS dlp_channel,
        {ts("coalesce(i.occurred_on, i.sent_on, i.created_at)")} AS opened_at,
        {ts("i.closed_at")}                                  AS closed_at,
        {ts("coalesce(a.updated_at, i.updated_at)")}         AS updated_at,
        {nz("i.location_country")}                           AS country,
        coalesce({nz("a.business_division_desc")}, {nz("i.department")}) AS business_unit,
        {nz("a.business_unit_desc")}                         AS business_unit_detail,
        {nz("i.assignment_group")}                           AS assignment_group,
        {nz("i.assigned_to")}                                AS assigned_to,
        {nz("i.subject")}                                    AS short_description,
        {nz("i.comment")}                                    AS description,
        {nz("i.user_name")}                                  AS primary_user,
        {nz("i.machine")}                                    AS primary_asset,
        {nz("i.parent")}                                     AS master_ticket,
        {nz("i.correlation_id")}                             AS correlation_id,
        {nz("i.policy")}                                     AS policy_name,
        {nz("i.prevention_status")}                          AS prevention_status,
        {nz("i.dismissal_reason")}                           AS dismissal_reason,
        {nz("i.sender")}                                     AS sender,
        {nz("i.recipient_destination_url")}                  AS recipient_or_destination,
        {nz("i.destination")}                                AS destination,
        {nz("i.destination_path")}                           AS destination_path,
        {nz("i.source")}                                     AS source_channel_raw,
        {nz("i.source_file")}                                AS source_file,
        lower(coalesce(i.has_attachment,'')) IN ('true','yes','y','1') AS has_attachment,
        try_cast(i.matches AS INT)                           AS match_count,
        {nz("i.kronos_event")}                               AS kronos_event,
        {nz("i.lm_user_name")}                               AS line_manager_user,
        a.case_id IS NOT NULL                                AS is_escalated_case,
        {nz("a.grade_or_rating")}                            AS escalation_grade,
        lower(coalesce(a.data_breach_incident,'')) IN ('yes','true','y') AS linked_to_data_breach,
        lower(coalesce(a.third_party_identified,'')) IN ('yes','true','y') AS third_party_identified,
        {nz("a.early_breach_notification")}                  AS early_breach_notification,
        {nz("a.resolution")}                                 AS resolution,
        {nz("a.progress")}                                   AS escalation_progress,
        {ts("a.date_escalated")}                             AS escalated_at,
        {ts("a.date_assigned_to_dbr_team")}                  AS assigned_to_dbr_at,
        {nz("a.mt_member")}                                  AS mt_member,
        i.ingestion_date                                     AS source_ingestion_date,
        coalesce({nz("i.correlation_id")},
                 concat_ws('|', lower(i.user_name), lower(i.policy), lower(i.source_file), CAST(to_date({ts("coalesce(i.occurred_on, i.sent_on, i.created_at)")}) AS STRING))) AS _dedup_key
    FROM inc i
    LEFT JOIN act a ON a.case_id = i.id
)
SELECT e.* EXCEPT (_dedup_key),
       row_number() OVER (PARTITION BY _dedup_key ORDER BY updated_at DESC NULLS LAST, incident_id) > 1 AS is_dlp_duplicate,
       sha2(_dedup_key, 256)                                 AS dlp_dedup_hash
FROM enriched e
"""
ROW_COUNTS["slv_dlp_event"] = write_snapshot(spark.sql(dlp_sql), "slv_dlp_event")

# COMMAND ----------

# MAGIC %md ## 4. Insider Threat – `slv_insider_incident`
# MAGIC `has_control_gap` is inferred from the text fields (no structured remediation column in the extract). The 08 Sep report's
# MAGIC SIR0500607 finding ("extract states no remediation plans / controls") came from exactly this kind of note – review the regex with the ITD team.

# COMMAND ----------

ins_sql = f"""
SELECT
    id                                                       AS incident_id,
    'Insider'                                                AS domain,
    '{BRONZE["insider_incident"]}'                           AS source_table,
    {nz("state")}                                            AS source_state,
    {state_norm("state")}                                    AS state_norm,
    {nz("severity")}                                         AS source_severity,
    {sev_norm("coalesce(" + nz("severity") + ", priority)")} AS severity_norm,
    {nz("priority")}                                         AS priority,
    {nz("category")}                                         AS category,
    {nz("subcategory")}                                      AS subcategory,
    {nz("request_category")}                                 AS request_category,
    {nz("substate")}                                         AS substate,
    {ts("coalesce(opened_at, created_on, event_generation_time)")} AS opened_at,
    {ts("closed_at")}                                        AS closed_at,
    {ts("sys_updated_on")}                                   AS updated_at,
    {ts("event_generation_time")}                            AS event_generated_at,
    coalesce({nz("impacted_country")}, {nz("affected_user_country")}) AS country,
    CAST(NULL AS STRING)                                     AS business_unit,
    {nz("business_criticality")}                             AS business_criticality,
    {nz("assignment_group")}                                 AS assignment_group,
    {nz("assigned_to")}                                      AS assigned_to,
    {nz("short_description")}                                AS short_description,
    description,
    {nz("affected_user_user_name")}                          AS primary_user,
    lower(coalesce(affected_user_vip,'')) IN ('true','yes','y','1') AS is_vip,
    CAST(NULL AS STRING)                                     AS primary_asset,
    coalesce({nz("master_ticket")}, {nz("parent_security_incident")}) AS master_ticket,
    {nz("incident_analysis_outcome")}                        AS analysis_outcome,
    {nz("potential_risk_indicators")}                        AS potential_risk_indicators,
    {nz("watchlists")}                                       AS watchlists,
    lower(coalesce(personal_data_involved,'')) IN ('true','yes','y','1') AS personal_data_involved,
    {nz("contact_type")}                                     AS contact_type,
    {nz("duplicated_ticket")}                                AS duplicated_ticket,
    {nz("time_escalated_to_mim_utc")}                        AS escalated_to_mim_raw,
    CASE WHEN lower(concat_ws(' ', work_notes, secure_notes, description))
              RLIKE 'no remediation|no remedial|no control|without control|control gap|no mitigation|not remediated' THEN true
         ELSE false END                                      AS has_control_gap,
    ingestion_date                                           AS source_ingestion_date
FROM {latest(BRONZE["insider_incident"], "id", "ingestion_date DESC, sys_updated_on DESC")}
"""
ROW_COUNTS["slv_insider_incident"] = write_snapshot(spark.sql(ins_sql), "slv_insider_incident")

# COMMAND ----------

# MAGIC %md ## 5. CTPS – `slv_ctps_incident`
# MAGIC Third-party / client security incidents. `event_generation_time` and `closed_at` arrive as strings – parsed with `ts()`.

# COMMAND ----------

ctps_sql = f"""
SELECT
    id                                                       AS incident_id,
    'CTPS'                                                   AS domain,
    '{BRONZE["ctps_incident"]}'                              AS source_table,
    {nz("state")}                                            AS source_state,
    {state_norm("state")}                                    AS state_norm,
    coalesce({nz("event_severity")}, {nz("grade_or_rating")}) AS source_severity,
    {sev_norm("coalesce(" + nz("event_severity") + ", grade_or_rating)")} AS severity_norm,
    CAST(NULL AS STRING)                                     AS priority,
    {nz("category")}                                         AS category,
    {nz("subcategory")}                                      AS subcategory,
    {ts("coalesce(created_on, event_generation_time)")}      AS opened_at,
    {ts("closed_at")}                                        AS closed_at,
    {ts("sys_updated_on")}                                   AS updated_at,
    {ts("event_generation_time")}                            AS event_generated_at,
    {ts("initial_response_time")}                            AS initial_response_at,
    coalesce({nz("impacted_country")}, {nz("affected_user_country")}) AS country,
    CAST(NULL AS STRING)                                     AS business_unit,
    {nz("assignment_group")}                                 AS assignment_group,
    {nz("assigned_to")}                                      AS assigned_to,
    {nz("short_description")}                                AS short_description,
    CAST(NULL AS STRING)                                     AS description,
    {nz("affected_user_user_name")}                          AS primary_user,
    lower(coalesce(affected_user_vip,'')) IN ('true','yes','y','1') AS is_vip,
    CAST(NULL AS STRING)                                     AS primary_asset,
    CAST(NULL AS STRING)                                     AS master_ticket,
    coalesce({nz("select_scb_third_party_vendor_renamed")}, {nz("select_scb_third_party_vendor")}) AS third_party_vendor,
    {nz("third_party_industry")}                             AS third_party_industry,
    {nz("ctpi_threat_bulletin_id")}                          AS threat_bulletin_id,
    {nz("client_tier")}                                      AS client_tier,
    {nz("scb_impact")}                                       AS scb_impact,
    lower(coalesce(scb_data_impacted,'')) IN ('yes','true','y')                 AS scb_data_impacted,
    lower(coalesce(service_availability_impacted,'')) IN ('yes','true','y')     AS service_availability_impacted,
    lower(coalesce(network_system_connectivity_impacted,'')) IN ('yes','true','y') AS connectivity_impacted,
    lower(coalesce(major_incident,'')) IN ('yes','true','y')                    AS is_major_incident,
    lower(coalesce(third_party_identified,'')) IN ('yes','true','y')            AS third_party_identified,
    lower(coalesce(first_detected_by_ctpi,'')) IN ('yes','true','y')            AS first_detected_by_ctps,
    {nz("early_breach_notification")}                        AS early_breach_notification,
    {nz("how_intelligence_was_ingested")}                    AS intelligence_source,
    {nz("was_intelligence_available_to_enable_detection")}   AS intelligence_available,
    {nz("incident_analysis_outcome")}                        AS analysis_outcome,
    {nz("investigation_outcome_recommendation")}             AS recommendation,
    {nz("mt1_name")}                                         AS mt1_name,
    {nz("relationship_manager_user_name")}                   AS relationship_manager,
    ingestion_date                                           AS source_ingestion_date
FROM {latest(BRONZE["ctps_incident"], "id", "ingestion_date DESC, sys_updated_on DESC")}
"""
ROW_COUNTS["slv_ctps_incident"] = write_snapshot(spark.sql(ctps_sql), "slv_ctps_incident")

# COMMAND ----------

# MAGIC %md ## 6. EASM – `slv_easm_finding`, `slv_easm_hunt`
# MAGIC watchTowr external attack-surface findings. Not a panel in the current PDF, but feeds the *Cloud / SaaS* analytic-impact
# MAGIC vector and gives asset keys (IP / hostname / URL) for cross-domain correlation. Nested `affected` struct is flattened.

# COMMAND ----------

easm_sql = f"""
SELECT
    CAST(id AS STRING)                                       AS incident_id,
    'EASM'                                                   AS domain,
    '{BRONZE["easm_findings"]}'                              AS source_table,
    coalesce({nz("status")}, {nz("state")})                  AS source_state,
    CASE WHEN lower(coalesce(status, state,'')) IN ('closed','resolved','fixed','remediated','accepted','risk accepted','false positive','dismissed') THEN 'Closed'
         WHEN lower(coalesce(status, state,'')) IN ('open','new','in progress','retest requested','confirmed','triaged','active') THEN 'Open'
         ELSE 'Unknown' END                                  AS state_norm,
    {nz("severity")}                                         AS source_severity,
    CASE WHEN {sev_norm("severity")} <> 'Unknown' THEN {sev_norm("severity")}
         WHEN cvssv3_score >= 9.0 THEN 'Critical'
         WHEN cvssv3_score >= 7.0 THEN 'High'
         WHEN cvssv3_score >= 4.0 THEN 'Medium'
         WHEN cvssv3_score IS NOT NULL THEN 'Low'
         ELSE 'Unknown' END                                  AS severity_norm,
    CAST(NULL AS STRING)                                     AS priority,
    coalesce({nz("impact")}, {nz("finding_impact")})         AS category,
    {nz("rapid_exposure_mechanism")}                         AS subcategory,
    created_at                                               AS opened_at,
    CASE WHEN lower(coalesce(status, state,'')) IN ('closed','resolved','fixed','remediated','accepted','risk accepted','false positive','dismissed')
         THEN coalesce(last_status_updated_at, ingestion_timestamp) END AS closed_at,
    coalesce(last_status_updated_at, ingestion_timestamp)    AS updated_at,
    CAST(NULL AS STRING)                                     AS country,
    array_join(transform(affected.data.businessUnits, x -> x.name), '; ') AS business_unit,
    CAST(NULL AS STRING)                                     AS assignment_group,
    assigned_user.name                                       AS assigned_to,
    {nz("title")}                                            AS short_description,
    description,
    CAST(NULL AS STRING)                                     AS primary_user,
    coalesce({nz("affected.data.name")}, {nz("affected.data.ip")}, {nz("affected.data.url")}) AS primary_asset,
    CAST(NULL AS STRING)                                     AS master_ticket,
    {nz("affected.data.type")}                               AS asset_type,
    {nz("affected.data.ip")}                                 AS asset_ip,
    {nz("affected.data.url")}                                AS asset_url,
    affected.data.port                                       AS asset_port,
    {nz("affected.data.service")}                            AS asset_service,
    {nz("affected.data.platform")}                           AS asset_platform,
    {nz("affected.data.provider")}                           AS asset_provider,
    {nz("affected.data.owner")}                              AS asset_owner,
    coalesce({nz("criticality")}, {nz("affected.data.criticality")}) AS asset_criticality,
    {nz("cve_id")}                                           AS cve_id,
    cvssv3_score,
    {nz("cvssv3_metrics")}                                   AS cvssv3_vector,
    epss_score,
    age                                                      AS age_days,
    last_seen,
    array_join(transform(tags, x -> x.name), '; ')           AS tags,
    detection_rules,
    {nz("recommendation")}                                   AS recommendation,
    retest.retest_remaining                                  AS retests_remaining,
    retest.current_retest.retest_status                      AS current_retest_status,
    ingestion_date                                           AS source_ingestion_date
FROM {latest(BRONZE["easm_findings"], "id", "ingestion_timestamp DESC")}
"""
ROW_COUNTS["slv_easm_finding"] = write_snapshot(spark.sql(easm_sql), "slv_easm_finding")

hunt_sql = f"""
SELECT
    CAST(id AS STRING)      AS hunt_id,
    {nz("type")}            AS hunt_type,
    {nz("hunt_request_type")} AS hunt_request_type,
    {nz("title")}           AS title,
    {nz("status")}          AS status,
    {nz("rapid_exposure_mechanism")} AS rapid_exposure_mechanism,
    total_findings,
    total_assets,
    created_at,
    updated_at,
    ingestion_date          AS source_ingestion_date
FROM {latest(BRONZE["easm_hunts"], "id", "ingestion_timestamp DESC")}
"""
ROW_COUNTS["slv_easm_hunt"] = write_snapshot(spark.sql(hunt_sql), "slv_easm_hunt")

# COMMAND ----------

# MAGIC %md ## 7. Email policy events – `slv_email_policy_event`
# MAGIC Two sources with one conformed schema:
# MAGIC * **Proofpoint (EA)** – `proofpoint_dlp.value` is a JSON string from Event Hub / Kafka. Field paths are in `PP_FIELDS` – **adjust once you have seen a real payload**.
# MAGIC * **Azure O365 (UKS)** – produced in UKS by notebook `02_bronze_to_silver_uks_o365` and read here from the mounted Delta Share.
# MAGIC   Only domain-level and hashed identifiers cross the region boundary.
# MAGIC
# MAGIC If the UKS share is not mounted the step writes Proofpoint only and records a warning in `slv_dq_run` (or fails if `fail_if_uks_missing = true`).

# COMMAND ----------

# JSON paths inside proofpoint_dlp.value – edit to match the real payload.
PP_FIELDS = {
    "event_id":        "$.id",
    "event_ts":        "$.timestamp",
    "sender":          "$.sender",
    "recipients":      "$.recipients",          # array or ';'-joined string
    "subject":         "$.subject",
    "policy_name":     "$.policy.name",
    "rule_name":       "$.rule.name",
    "action_taken":    "$.action",
    "attachments":     "$.attachments",         # array
    "direction":       "$.direction",
    "severity":        "$.severity",
    "user_country":    "$.user.country",
}

def pp(field):
    return f"get_json_object(value, '{PP_FIELDS[field]}')"

EMAIL_EVENT_COLUMNS = """
    event_id, source_system, event_ts, direction, sender_domain, sender_hash, recipient_domains, recipient_count,
    external_recipient_count, subject_hash, policy_name, rule_name, action_taken, severity_norm,
    has_attachment, attachment_count, country, is_external, source_region, source_ingestion_date
"""

proofpoint_sql = f"""
WITH raw AS (
    SELECT value, ingestion_date, eh_timestamp
    FROM {BRONZE["proofpoint_dlp"]}
    WHERE ingestion_date <= DATE'{SNAPSHOT_DATE}'
      AND ingestion_date >= date_sub(DATE'{SNAPSHOT_DATE}', 2)
      AND value IS NOT NULL
),
parsed AS (
    SELECT
        coalesce({pp("event_id")}, sha2(value, 256))                              AS event_id,
        'Proofpoint'                                                              AS source_system,
        coalesce(try_cast({pp("event_ts")} AS TIMESTAMP), eh_timestamp)           AS event_ts,
        lower(coalesce({pp("direction")}, 'outbound'))                            AS direction,
        lower(regexp_extract({pp("sender")}, '@(.+)$', 1))                        AS sender_domain,
        sha2(lower(trim({pp("sender")})), 256)                                    AS sender_hash,
        coalesce(from_json({pp("recipients")}, 'array<string>'),
                 split({pp("recipients")}, '[;,]'))                               AS recipients_arr,
        sha2(lower(trim({pp("subject")})), 256)                                   AS subject_hash,
        {pp("policy_name")}                                                       AS policy_name,
        {pp("rule_name")}                                                         AS rule_name,
        {pp("action_taken")}                                                      AS action_taken,
        {sev_norm(pp("severity"))}                                                AS severity_norm,
        coalesce(from_json({pp("attachments")}, 'array<string>'), array())        AS attachments_arr,
        {pp("user_country")}                                                      AS country,
        ingestion_date
    FROM raw
)
SELECT
    event_id, source_system, event_ts, direction, sender_domain, sender_hash,
    array_distinct(transform(recipients_arr, r -> lower(regexp_extract(trim(r), '@(.+)$', 1))))  AS recipient_domains,
    size(recipients_arr)                                                                          AS recipient_count,
    size(filter(recipients_arr, r -> lower(r) NOT RLIKE '@(sc\\.com|standardchartered\\.com)$'))   AS external_recipient_count,
    subject_hash, policy_name, rule_name, action_taken, severity_norm,
    size(attachments_arr) > 0                                                                     AS has_attachment,
    size(attachments_arr)                                                                         AS attachment_count,
    country,
    size(filter(recipients_arr, r -> lower(r) NOT RLIKE '@(sc\\.com|standardchartered\\.com)$')) > 0 AS is_external,
    'EA'                                                                                          AS source_region,
    ingestion_date                                                                                AS source_ingestion_date
FROM parsed
WHERE to_date(event_ts) BETWEEN date_sub(DATE'{SNAPSHOT_DATE}', 1) AND DATE'{SNAPSHOT_DATE}'
"""
email_df = spark.sql(proofpoint_sql)

UKS_STATUS = "not_mounted"
if table_exists(UKS_EMAIL_TABLE):
    uks_df = (spark.table(UKS_EMAIL_TABLE)
                   .where(F.col("snapshot_date") == F.lit(SNAPSHOT_DATE))
                   .selectExpr(*[c.strip() for c in EMAIL_EVENT_COLUMNS.replace("\n", "").split(",")]))
    uks_rows = uks_df.count()
    if uks_rows == 0:
        UKS_STATUS = "mounted_but_no_rows_for_snapshot"
        msg = f"UKS table {UKS_EMAIL_TABLE} has no rows for {SNAPSHOT_DATE} – has the UKS job run yet?"
        if FAIL_IF_UKS_MISSING:
            raise RuntimeError(msg)
        print("WARNING:", msg)
    else:
        UKS_STATUS = f"ok:{uks_rows}"
        email_df = email_df.unionByName(uks_df, allowMissingColumns=True)
else:
    msg = f"UKS shared table {UKS_EMAIL_TABLE} not found – writing Proofpoint only."
    if FAIL_IF_UKS_MISSING:
        raise RuntimeError(msg)
    print("WARNING:", msg)

ROW_COUNTS["slv_email_policy_event"] = write_snapshot(email_df, "slv_email_policy_event")
print("UKS share status:", UKS_STATUS)

# COMMAND ----------

# MAGIC %md ## 8. Unified incident table – `slv_unified_incident`
# MAGIC Union of the five incident domains (+ EASM findings) on a common schema, then two dedup passes:
# MAGIC * **Exact** – same domain, normalised description, open date, country and user → `is_exact_duplicate` (keeps earliest `incident_id`)
# MAGIC * **Probable** – same domain, open date, category, country and first 40 alphanumerics of the description → `is_probable_duplicate`
# MAGIC
# MAGIC `is_counted` (= not a duplicate) is what every Gold count uses. Report footer: "70 exact + 334 probable duplicates removed".

# COMMAND ----------

COMMON_COLS = """
    incident_id, domain, source_table, source_state, state_norm, source_severity, severity_norm, priority,
    category, subcategory, opened_at, closed_at, updated_at, country, business_unit, assignment_group, assigned_to,
    short_description, primary_user, primary_asset, master_ticket
"""

def common_select(table, extra_where="1=1"):
    return f"SELECT {COMMON_COLS} FROM {SILVER}.{table} WHERE snapshot_date = DATE'{SNAPSHOT_DATE}' AND {extra_where}"

unified_sql = f"""
WITH u AS (
    {common_select("slv_cdc_incident")}
    UNION ALL {common_select("slv_dbr_incident")}
    UNION ALL {common_select("slv_dlp_event", "NOT is_dlp_duplicate")}
    UNION ALL {common_select("slv_insider_incident")}
    UNION ALL {common_select("slv_ctps_incident")}
    UNION ALL {common_select("slv_easm_finding")}
),
keyed AS (
    SELECT u.*,
        state_norm = 'Open'                                                        AS is_open,
        to_date(opened_at)                                                         AS opened_date,
        to_date(closed_at)                                                         AS closed_date,
        to_date(opened_at) = DATE'{SNAPSHOT_DATE}'                                 AS opened_in_snapshot_day,
        to_date(closed_at) = DATE'{SNAPSHOT_DATE}'                                 AS closed_in_snapshot_day,
        CASE WHEN state_norm = 'Open' THEN datediff(DATE'{SNAPSHOT_DATE}', to_date(opened_at)) END AS days_open,
        lower(regexp_replace(coalesce(short_description,''), '\\\\s+', ' '))        AS _desc_norm,
        regexp_replace(lower(left(coalesce(short_description,''), 40)), '[^a-z0-9]', '') AS _desc_prefix
    FROM u
),
hashed AS (
    SELECT k.*,
        sha2(concat_ws('||', domain, _desc_norm, CAST(opened_date AS STRING), coalesce(country,''), coalesce(lower(primary_user),'')), 256) AS exact_dup_hash,
        sha2(concat_ws('||', domain, CAST(opened_date AS STRING), lower(coalesce(category,'')), coalesce(country,''), _desc_prefix), 256)  AS probable_dup_hash
    FROM keyed k
),
flagged AS (
    SELECT h.*,
        row_number() OVER (PARTITION BY exact_dup_hash ORDER BY opened_at NULLS LAST, incident_id) > 1 AS is_exact_duplicate
    FROM hashed h
),
flagged2 AS (
    SELECT f.*,
        CASE WHEN is_exact_duplicate THEN false
             ELSE row_number() OVER (PARTITION BY probable_dup_hash, is_exact_duplicate ORDER BY opened_at NULLS LAST, incident_id) > 1 END AS is_probable_duplicate
    FROM flagged f
)
SELECT * EXCEPT (_desc_norm, _desc_prefix),
       NOT (is_exact_duplicate OR is_probable_duplicate) AS is_counted,
       sha2(concat_ws('||', domain, incident_id), 256)   AS incident_key
FROM flagged2
"""
ROW_COUNTS["slv_unified_incident"] = write_snapshot(spark.sql(unified_sql), "slv_unified_incident")

dup_stats = spark.sql(f"""
    SELECT domain,
           count(*)                                        AS rows_total,
           sum(CASE WHEN is_exact_duplicate THEN 1 ELSE 0 END)    AS exact_dups,
           sum(CASE WHEN is_probable_duplicate THEN 1 ELSE 0 END) AS probable_dups,
           sum(CASE WHEN is_counted THEN 1 ELSE 0 END)            AS unique_incidents,
           sum(CASE WHEN is_counted AND is_open THEN 1 ELSE 0 END) AS open_unique,
           sum(CASE WHEN severity_norm = 'Unknown' THEN 1 ELSE 0 END) AS severity_unknown,
           sum(CASE WHEN state_norm = 'Unknown' THEN 1 ELSE 0 END)    AS state_unknown
    FROM {SILVER}.slv_unified_incident
    WHERE snapshot_date = DATE'{SNAPSHOT_DATE}'
    GROUP BY domain ORDER BY domain""")
display(dup_stats)

# COMMAND ----------

# MAGIC %md ## 9. Entity links – `slv_incident_entity`
# MAGIC One row per `(incident, entity_type, entity_value)`. This is the join table the Gold correlation step uses to score
# MAGIC cross-domain pairs ("No pair shared sufficient user, asset, IOC, vendor or incident-ID evidence to score 30 or above").
# MAGIC Entity types: `user`, `asset`, `ip`, `domain`, `url`, `hash`, `vendor`, `incident_ref`, `policy`.

# COMMAND ----------

IP_RE     = r'\\b(?:(?:25[0-5]|2[0-4]\\d|1?\\d?\\d)\\.){3}(?:25[0-5]|2[0-4]\\d|1?\\d?\\d)\\b'
HASH_RE   = r'\\b(?:[a-fA-F0-9]{64}|[a-fA-F0-9]{40}|[a-fA-F0-9]{32})\\b'
URL_RE    = r'https?://[^\\s)\\]>"\']+'
DOMAIN_RE = r'\\b(?:[a-z0-9-]+\\.)+(?:com|net|org|io|co|uk|sg|hk|in|ae|cn|biz|info|xyz|top|ru|me|app|dev|cloud)\\b'
TICKET_RE = r'\\b(?:SIR|INC|RITM|TASK|CHG|DBR|DLP)\\d{5,}\\b'

entity_sql = f"""
WITH u AS (
    SELECT incident_key, incident_id, domain, primary_user, primary_asset, master_ticket, short_description, category
    FROM {SILVER}.slv_unified_incident
    WHERE snapshot_date = DATE'{SNAPSHOT_DATE}' AND is_counted
),
text_src AS (
    SELECT u.incident_key, u.domain,
           concat_ws(' ', u.short_description,
                     c.description, i.description, d.description, e.description,
                     d.recipient_or_destination, d.destination, e.asset_url, e.asset_ip) AS txt
    FROM u
    LEFT JOIN {SILVER}.slv_cdc_incident     c ON c.incident_id = u.incident_id AND u.domain = 'CDC'     AND c.snapshot_date = DATE'{SNAPSHOT_DATE}'
    LEFT JOIN {SILVER}.slv_insider_incident i ON i.incident_id = u.incident_id AND u.domain = 'Insider' AND i.snapshot_date = DATE'{SNAPSHOT_DATE}'
    LEFT JOIN {SILVER}.slv_dlp_event        d ON d.incident_id = u.incident_id AND u.domain = 'DLP'     AND d.snapshot_date = DATE'{SNAPSHOT_DATE}'
    LEFT JOIN {SILVER}.slv_easm_finding     e ON e.incident_id = u.incident_id AND u.domain = 'EASM'    AND e.snapshot_date = DATE'{SNAPSHOT_DATE}'
),
structured AS (
    SELECT incident_key, domain, 'user'         AS entity_type, lower(trim(primary_user))  AS entity_value, 'structured' AS entity_source FROM u WHERE primary_user  IS NOT NULL
    UNION ALL
    SELECT incident_key, domain, 'asset',        lower(trim(primary_asset)), 'structured' FROM u WHERE primary_asset IS NOT NULL
    UNION ALL
    SELECT incident_key, domain, 'incident_ref', upper(trim(master_ticket)), 'structured' FROM u WHERE master_ticket IS NOT NULL
    UNION ALL
    SELECT incident_key, domain, 'incident_ref', upper(trim(incident_id)),   'self'       FROM u
    UNION ALL
    SELECT incident_key, domain, 'vendor', lower(trim(third_party_vendor)), 'structured'
    FROM {SILVER}.slv_ctps_incident WHERE snapshot_date = DATE'{SNAPSHOT_DATE}' AND third_party_vendor IS NOT NULL
    UNION ALL
    SELECT incident_key, domain, 'policy', lower(trim(category)), 'structured' FROM u WHERE domain = 'DLP' AND category IS NOT NULL
),
extracted AS (
    SELECT incident_key, domain, 'ip'           AS entity_type, explode(regexp_extract_all(txt, '{IP_RE}', 0))            AS entity_value, 'regex' AS entity_source FROM text_src
    UNION ALL
    SELECT incident_key, domain, 'hash',         explode(regexp_extract_all(lower(txt), '{HASH_RE}', 0)), 'regex' FROM text_src
    UNION ALL
    SELECT incident_key, domain, 'url',          explode(regexp_extract_all(txt, '{URL_RE}', 0)),         'regex' FROM text_src
    UNION ALL
    SELECT incident_key, domain, 'domain',       explode(regexp_extract_all(lower(txt), '{DOMAIN_RE}', 0)), 'regex' FROM text_src
    UNION ALL
    SELECT incident_key, domain, 'incident_ref', explode(regexp_extract_all(upper(txt), '{TICKET_RE}', 0)), 'regex' FROM text_src
),
all_entities AS (
    SELECT * FROM structured
    UNION ALL
    SELECT * FROM extracted
)
SELECT DISTINCT incident_key, domain, entity_type, entity_value, entity_source
FROM all_entities
WHERE entity_value IS NOT NULL AND entity_value <> ''
  AND NOT (entity_type = 'domain' AND entity_value RLIKE '(sc\\\\.com|standardchartered\\\\.com|microsoft\\\\.com|office\\\\.com|windows\\\\.net)$')
  AND NOT (entity_type = 'ip' AND entity_value RLIKE '^(10\\\\.|127\\\\.|0\\\\.|255\\\\.)')
"""
ROW_COUNTS["slv_incident_entity"] = write_snapshot(spark.sql(entity_sql), "slv_incident_entity")

shared = spark.sql(f"""
    SELECT entity_type, count(*) AS entities_shared_across_domains
    FROM (SELECT entity_type, entity_value, count(DISTINCT domain) AS n_dom
          FROM {SILVER}.slv_incident_entity WHERE snapshot_date = DATE'{SNAPSHOT_DATE}'
          GROUP BY entity_type, entity_value HAVING n_dom > 1)
    GROUP BY entity_type ORDER BY 2 DESC""")
print("Entities seen in more than one domain (candidate correlation evidence):")
display(shared)

# COMMAND ----------

# MAGIC %md ## 10. Data-quality run log – `slv_dq_run`
# MAGIC One row per bronze source per run: rows as-of snapshot, latest ingestion, freshness lag. Plus run totals used in the report header
# MAGIC (*"14 data extracts supplied"*, *"70 exact + 334 probable duplicates removed"*, cut-off time).

# COMMAND ----------

from pyspark.sql import Row

dq_rows = []
for name, tbl in BRONZE.items():
    try:
        cols = [c.lower() for c in spark.table(tbl).columns]
        has_ing = "ingestion_date" in cols
        ing_ts = "ingestion_timestamp" if "ingestion_timestamp" in cols else ("eh_timestamp" if "eh_timestamp" in cols else None)
        where = f"WHERE ingestion_date <= DATE'{SNAPSHOT_DATE}'" if has_ing else ""
        agg = spark.sql(f"""
            SELECT count(*) AS n,
                   {"max(ingestion_date)" if has_ing else "CAST(NULL AS DATE)"} AS max_ing_date,
                   {f"max({ing_ts})" if ing_ts else "CAST(NULL AS TIMESTAMP)"} AS max_ing_ts
            FROM {tbl} {where}""").first()
        dq_rows.append(Row(source_name=name, source_table=tbl, status="ok", rows_asof_snapshot=int(agg["n"]),
                           max_ingestion_date=agg["max_ing_date"], max_ingestion_ts=agg["max_ing_ts"],
                           freshness_lag_days=(datetime.fromisoformat(SNAPSHOT_DATE).date() - agg["max_ing_date"]).days if agg["max_ing_date"] else None,
                           message=None))
    except Exception as ex:
        dq_rows.append(Row(source_name=name, source_table=tbl, status="error", rows_asof_snapshot=None,
                           max_ingestion_date=None, max_ingestion_ts=None, freshness_lag_days=None, message=str(ex)[:500]))

dq_rows.append(Row(source_name="uks_o365_shared", source_table=UKS_EMAIL_TABLE, status=UKS_STATUS.split(":")[0],
                   rows_asof_snapshot=int(UKS_STATUS.split(":")[1]) if UKS_STATUS.startswith("ok:") else None,
                   max_ingestion_date=None, max_ingestion_ts=None, freshness_lag_days=None,
                   message=None if UKS_STATUS.startswith("ok:") else UKS_STATUS))

totals = spark.sql(f"""
    SELECT count(*) AS rows_total,
           sum(CASE WHEN is_exact_duplicate THEN 1 ELSE 0 END)    AS exact_dups,
           sum(CASE WHEN is_probable_duplicate THEN 1 ELSE 0 END) AS probable_dups,
           sum(CASE WHEN is_counted THEN 1 ELSE 0 END)            AS unique_incidents
    FROM {SILVER}.slv_unified_incident WHERE snapshot_date = DATE'{SNAPSHOT_DATE}'""").first()

dq_df = (spark.createDataFrame(dq_rows)
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("snapshot_date", F.lit(SNAPSHOT_DATE).cast("date"))
    .withColumn("cutoff_utc", F.lit(RUN_TS))
    .withColumn("extracts_supplied", F.lit(sum(1 for r in dq_rows if r.status == "ok" and (r.rows_asof_snapshot or 0) > 0)))
    .withColumn("unified_rows_total", F.lit(int(totals["rows_total"])))
    .withColumn("exact_duplicates_removed", F.lit(int(totals["exact_dups"])))
    .withColumn("probable_duplicates_removed", F.lit(int(totals["probable_dups"])))
    .withColumn("unique_incidents", F.lit(int(totals["unique_incidents"])))
    .withColumn("silver_row_counts", F.lit(str(ROW_COUNTS))))

dq_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{SILVER}.slv_dq_run")
display(dq_df.select("source_name", "status", "rows_asof_snapshot", "max_ingestion_date", "freshness_lag_days", "message"))

# COMMAND ----------

# MAGIC %md ## 11. Run summary

# COMMAND ----------

print(f"""
Run                 : {RUN_ID}
Snapshot date       : {SNAPSHOT_DATE}
Cut-off (UTC)       : {RUN_TS:%d %b %Y %H:%M}
Extracts supplied   : {sum(1 for r in dq_rows if r.status == "ok" and (r.rows_asof_snapshot or 0) > 0)} / {len(BRONZE) + 1}
UKS share           : {UKS_STATUS}
Unified rows        : {int(totals["rows_total"]):,}
Exact duplicates    : {int(totals["exact_dups"]):,}
Probable duplicates : {int(totals["probable_dups"]):,}
Unique incidents    : {int(totals["unique_incidents"]):,}
""")
for t, n in ROW_COUNTS.items():
    print(f"  {t:<28} {n:>8,}")

dbutils.notebook.exit(f'{{"run_id":"{RUN_ID}","snapshot_date":"{SNAPSHOT_DATE}","unique_incidents":{int(totals["unique_incidents"])},"uks_status":"{UKS_STATUS}"}}')
