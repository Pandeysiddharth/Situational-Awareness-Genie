# Job run history (exported from the Jobs API)

All timestamps UTC. Source: `GET /api/2.2/jobs/runs/list` + `runs/export` for each task run. Per-task stdout is in the linked files.


## umm-demo-daily (job_id 646829627611828)

| run_id | trigger | start | end | result | tasks (key: result, duration s) |
|---|---|---|---|---|---|
| 290566913448832 | PERIODIC | 2026-11-26 00:30:02 UTC | 2026-11-26 00:38:58 UTC | SUCCESS | pd_live_processor: SUCCESS (36); keepalive: SUCCESS (104); genai_traffic: SUCCESS (351); genai_process: SUCCESS (27) |
| 574658266522853 | ONE_TIME | 2026-11-25 10:16:12 UTC | 2026-11-25 10:22:15 UTC | SUCCESS | pd_live_processor: SUCCESS (25); keepalive: SUCCESS (11); genai_traffic: SUCCESS (288); genai_process: SUCCESS (25) |
| 52871997878121 | ONE_TIME | 2026-11-25 10:14:55 UTC | 2026-11-25 10:14:56 UTC | FAILED | pd_live_processor: FAILED (0); keepalive: UPSTREAM_FAILED (0); genai_traffic: UPSTREAM_FAILED (0); genai_process: UPSTREAM_FAILED (0) |

## umm-demo-hub-refresh (job_id 156494604409159)

| run_id | trigger | start | end | result | tasks (key: result, duration s) |
|---|---|---|---|---|---|
| 262599115956790 | PERIODIC | 2026-11-26 03:00:02 UTC | 2026-11-26 03:00:03 UTC | FAILED | gold_refresh: FAILED (0); alert_1: UPSTREAM_FAILED (0); alert_2: UPSTREAM_FAILED (0) |
