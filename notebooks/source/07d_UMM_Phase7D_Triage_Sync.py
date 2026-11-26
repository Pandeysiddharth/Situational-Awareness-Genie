# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # UMM Demo · Phase 7D · Alert triage sync (Delta ↔ Lakebase) and job wiring
# MAGIC
# MAGIC Runs in the hub job after each gold refresh:
# MAGIC 1. **Delta → Lakebase:** new breaches and escalations from `hub_gold.fact_alerts` go into `umm.alert_inbox` as `open`, with a priority. Red on a Tier 1 model is P1, other red is P2, amber is P3.
# MAGIC 2. **Auto-resolve:** an open alert whose metric has since recovered becomes `auto_resolved`. Alerts someone is already working on stay as they are, and a `metric_recovered` event is logged.
# MAGIC 3. **Lakebase → Delta:** the inbox and its event history are copied to `hub_gold.fact_alert_triage` and `hub_gold.fact_alert_triage_events`. Triage KPIs (open by priority and team, time to acknowledge, time to resolve, false-positive rate) go to `hub_gold.fact_triage_kpis`, for the dashboard, Genie and model risk reporting.
# MAGIC
# MAGIC With `WIRE_JOBS = True`, it also adds Phase 7 to the existing jobs:
# MAGIC * `umm-demo-daily` gains memory traffic and memory metrics, after the GenAI steps.
# MAGIC * `umm-demo-hub-refresh` gains this triage sync, after the gold refresh.

# COMMAND ----------

USE_SINGLE_CATALOG = True
SINGLE_CATALOG = "demo_unified_model_monitoring"
MULTI_CATALOGS = {"hub": "umm_demo_hub"}
WIRE_JOBS = True
DAILY_JOB, HUB_JOB = "umm-demo-daily", "umm-demo-hub-refresh"
NOTEBOOKS = {"memory_traffic": "UMM_Phase7B_Memory_Traffic", "memory_metrics": "UMM_Phase7C_Memory_Metrics",
             "triage_sync": "UMM_Phase7D_Triage_Sync"}
NOTEBOOK_DIR = None      # None = this notebook's folder


def fq(area, schema, table):
    if USE_SINGLE_CATALOG:
        return f"{SINGLE_CATALOG}.{area}_{schema}.{table}"
    return f"{MULTI_CATALOGS[area]}.{schema}.{table}"

# COMMAND ----------

import subprocess, sys
try:
    import psycopg
except ImportError:
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "psycopg[binary]"], check=True)
    import psycopg
import pandas as pd
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
ME = spark.sql("SELECT current_user()").collect()[0][0]
mcfg = spark.table(fq("hub", "ref", "memory_runtime_config")).collect()[0].asDict()
ENDPOINT, HOST, DB = mcfg["lakebase_endpoint"], mcfg["lakebase_host"], mcfg["lakebase_database"]


def pg_connect():
    try:
        token = w.postgres.generate_database_credential(endpoint=ENDPOINT).token
    except Exception:
        token = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": ENDPOINT})["token"]
    return psycopg.connect(host=HOST, dbname=DB, user=ME, password=token, sslmode="require", autocommit=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1–2 · New alerts in, recovered alerts auto-resolved

# COMMAND ----------

alerts = spark.sql(f"""SELECT a.*, d.risk_tier FROM {fq('hub', 'gold', 'fact_alerts')} a
                       LEFT JOIN {fq('hub', 'gold', 'dim_model')} d USING (model_id)""").toPandas()
alerts["metric_date"] = pd.to_datetime(alerts["metric_date"]).dt.date
with pg_connect() as conn:
    known = {r[0] for r in conn.execute("SELECT alert_id FROM umm.alert_inbox").fetchall()}
    new = alerts[alerts["alert_type"].isin(["breach", "escalation"]) & ~alerts["alert_id"].isin(known)]
    rows = [(r.alert_id, r.metric_date, r.model_id, r.model_name, r.owner_team, r.risk_tier, r.contract_name, r.display_name,
             r.category, r.alert_type, r.to_status, float(r.eval_value), None if pd.isna(r.threshold) else float(r.threshold),
             r.message, ("P1" if r.risk_tier == "Tier 1" else "P2") if r.to_status == "red" else "P3")
            for r in new.itertuples()]
    with conn.cursor() as cur:
        cur.executemany("""INSERT INTO umm.alert_inbox (alert_id, metric_date, model_id, model_name, owner_team, risk_tier,
                           contract_name, display_name, category, alert_type, to_status, eval_value, threshold, message, priority)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (alert_id) DO NOTHING""", rows)
        cur.executemany("""INSERT INTO umm.alert_triage_events (alert_id, action, from_status, to_status, actor, note)
                           VALUES (%s, 'created', NULL, 'open', 'system', NULL)""", [(r[0],) for r in rows])
    print(f"✓ {len(rows)} new alerts added to the triage inbox")

    recovered = alerts[alerts["alert_type"] == "resolved"][["model_id", "contract_name", "metric_date"]]
    open_rows = pd.DataFrame(conn.execute("""SELECT alert_id, model_id, contract_name, metric_date, triage_status
                                             FROM umm.alert_inbox WHERE triage_status IN ('open','acknowledged','investigating')""").fetchall(),
                             columns=["alert_id", "model_id", "contract_name", "metric_date", "triage_status"])
    auto, flagged = 0, 0
    if len(open_rows) and len(recovered):
        m = open_rows.merge(recovered, on=["model_id", "contract_name"], suffixes=("", "_rec"))
        m = m[m["metric_date_rec"] > m["metric_date"]].drop_duplicates("alert_id")
        with conn.cursor() as cur:
            for r in m.itertuples():
                if r.triage_status == "open":
                    cur.execute("""UPDATE umm.alert_inbox SET triage_status='auto_resolved', resolved_at=now(), updated_by='system',
                                   updated_at=now(), note=coalesce(note,'') || ' Metric recovered to green.' WHERE alert_id=%s""", (r.alert_id,))
                    cur.execute("""INSERT INTO umm.alert_triage_events (alert_id, action, from_status, to_status, actor, note)
                                   VALUES (%s,'auto_resolve','open','auto_resolved','system','Metric recovered to green')""", (r.alert_id,))
                    auto += 1
                else:
                    cur.execute("""INSERT INTO umm.alert_triage_events (alert_id, action, from_status, to_status, actor, note)
                                   SELECT %s,'metric_recovered',%s,%s,'system','Metric recovered; awaiting owner sign-off'
                                   WHERE NOT EXISTS (SELECT 1 FROM umm.alert_triage_events WHERE alert_id=%s AND action='metric_recovered')""",
                                (r.alert_id, r.triage_status, r.triage_status, r.alert_id))
                    flagged += 1
    print(f"✓ {auto} open alerts auto-resolved · {flagged} in-progress alerts flagged as recovered")
    inbox = pd.DataFrame(conn.execute("SELECT * FROM umm.alert_inbox").fetchall(),
                         columns=[d.name for d in conn.execute("SELECT * FROM umm.alert_inbox LIMIT 0").description])
    events = pd.DataFrame(conn.execute("SELECT * FROM umm.alert_triage_events").fetchall(),
                          columns=[d.name for d in conn.execute("SELECT * FROM umm.alert_triage_events LIMIT 0").description])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Lakebase → Delta, plus triage KPIs

# COMMAND ----------

for df in (inbox, events):
    for c in df.columns:
        if str(df[c].dtype).startswith("datetime64") and getattr(df[c].dt, "tz", None) is not None:
            df[c] = df[c].dt.tz_convert("UTC").dt.tz_localize(None)
        if df[c].dtype == object and len(df) and hasattr(df[c].dropna().iloc[0] if df[c].notna().any() else None, "tzinfo"):
            df[c] = pd.to_datetime(df[c], utc=True).dt.tz_localize(None)

T_TRIAGE, T_EVENTS, T_KPI = fq("hub", "gold", "fact_alert_triage"), fq("hub", "gold", "fact_alert_triage_events"), fq("hub", "gold", "fact_triage_kpis")
spark.createDataFrame(inbox).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_TRIAGE)
spark.createDataFrame(events).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_EVENTS)
spark.sql(f"COMMENT ON TABLE {T_TRIAGE} IS 'Alert triage state from Lakebase (umm.alert_inbox): status, priority, assignee, acknowledged/resolved times. Synced nightly.'")
spark.sql(f"COMMENT ON TABLE {T_EVENTS} IS 'Every triage action from Lakebase (umm.alert_triage_events): who did what, when.'")
spark.sql(f"""
CREATE OR REPLACE TABLE {T_KPI} COMMENT 'Triage KPIs by owner team: open alerts by priority, mean time to acknowledge and resolve (hours, last 30 days), false-positive rate.' AS
SELECT current_date() AS as_of, owner_team,
       count_if(triage_status IN ('open','acknowledged','investigating')) AS open_alerts,
       count_if(triage_status IN ('open','acknowledged','investigating') AND priority = 'P1') AS open_p1,
       count_if(triage_status = 'open') AS unacknowledged,
       round(avg(CASE WHEN acknowledged_at IS NOT NULL AND created_at >= current_date() - INTERVAL 30 DAYS
                      THEN (unix_timestamp(acknowledged_at) - unix_timestamp(created_at)) / 3600 END), 1) AS mtta_hours,
       round(avg(CASE WHEN resolved_at IS NOT NULL AND triage_status IN ('resolved','false_positive') AND created_at >= current_date() - INTERVAL 30 DAYS
                      THEN (unix_timestamp(resolved_at) - unix_timestamp(created_at)) / 3600 END), 1) AS mttr_hours,
       round(count_if(triage_status = 'false_positive') / nullif(count_if(triage_status IN ('resolved','false_positive')), 0), 3) AS false_positive_rate
FROM {T_TRIAGE} GROUP BY ALL""")
display(spark.table(T_KPI))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wire Phase 7 into the jobs (idempotent)

# COMMAND ----------

if WIRE_JOBS:
    here = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    folder = NOTEBOOK_DIR or here.rsplit("/", 1)[0]
    paths = {k: (v if v.startswith("/") else f"{folder}/{v}") for k, v in NOTEBOOKS.items()}
    missing = []
    for k, p in paths.items():
        try:
            w.workspace.get_status(p)
        except Exception:
            missing.append(p)

    def job_by_name(name):
        js = w.api_client.do("GET", "/api/2.1/jobs/list", query={"name": name}).get("jobs", [])
        return w.api_client.do("GET", "/api/2.1/jobs/get", query={"job_id": js[0]["job_id"]}) if js else None

    def add_tasks(job_name, new_tasks):
        job = job_by_name(job_name)
        if not job:
            print(f"⚠ job {job_name} not found; skipping")
            return
        settings = job["settings"]
        keys = {t["task_key"] for t in settings["tasks"]}
        added = [t for t in new_tasks if t["task_key"] not in keys]
        if not added:
            print(f"• {job_name} already has Phase 7 tasks")
            return
        settings["tasks"] += added
        w.api_client.do("POST", "/api/2.1/jobs/reset", body={"job_id": job["job_id"], "new_settings": settings})
        print(f"✓ {job_name}: added {[t['task_key'] for t in added]}")

    def nb_task(key, path, after, params=None):
        return {"task_key": key, "depends_on": [{"task_key": after}], "timeout_seconds": 3600,
                "notebook_task": {"notebook_path": path, "source": "WORKSPACE", "base_parameters": params or {}}}

    if missing:
        print("⚠ Jobs not changed. Create these notebooks first:\n  " + "\n  ".join(missing))
    else:
        add_tasks(DAILY_JOB, [nb_task("memory_traffic", paths["memory_traffic"], "genai_process"),
                              nb_task("memory_metrics", paths["memory_metrics"], "memory_traffic")])
        add_tasks(HUB_JOB, [nb_task("triage_sync", paths["triage_sync"], "gold_refresh")])
        print("One-time step: in UMM_Phase7C_Memory_Metrics → Environment, add mlflow[databricks]>=3.1 and databricks-agents "
              "(same as 3C), so scheduled runs have the judge packages.")

# COMMAND ----------

checks = [("Triage inbox rows", len(inbox), len(inbox) > 0),
          ("Triage events rows", len(events), len(events) > 0),
          ("KPI rows (one per owner team)", spark.table(T_KPI).count(), spark.table(T_KPI).count() > 0)]
display(pd.DataFrame([(n, "PASS" if ok else "FAIL", str(v)) for n, v, ok in checks], columns=["check", "result", "value"]))
