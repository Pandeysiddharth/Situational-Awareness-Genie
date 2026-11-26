# Captured output: `02a_UMM_Phase2_All_In_One.ipynb`

This is the plain-text output stored in the notebook the last time it ran interactively on Databricks. HTML tables are converted to markdown.


## Cell 1

```text
2026/11/25 08:17:43 INFO mlflow.tracking.fluent: Experiment with name '/Shared/model_monitoring/umm_demo_training' does not exist. Creating a new experiment.
```
```text
History 2026-08-27 → 2026-11-24 · PD v2 deploys on day 45
Models: demo_unified_model_monitoring.credit_ml.pd_counterparty_default, demo_unified_model_monitoring.markets_ml.trade_volume_forecast
```
```text
/databricks/python/lib/python3.12/site-packages/mlflow/pyfunc/utils/data_validation.py:187: UserWarning: Add type hints to the `predict` method to enable data validation and automatic signature inference during model logging. Check https://mlflow.org/docs/latest/model/python_model.html#type-hint-usage-in-pythonmodel for more details.
  color_warning(
/databricks/python/lib/python3.12/site-packages/mlflow/pyfunc/utils/data_validation.py:187: UserWarning: Add type hints to the `predict` method to enable data validation and automatic signature inference during model logging. Check https://mlflow.org/docs/latest/model/python_model.html#type-hint-usage-in-pythonmodel for more details.
  color_warning(
```
```text
Policy threshold: 0.225 · v1 holdout {'holdout_auc': 0.8196, 'holdout_accuracy': 0.8606, 'holdout_precision': 0.4073, 'holdout_recall': 0.5148, 'holdout_f1_macro': 0.6874, 'holdout_default_rate': 0.1129}
v2 holdout {'holdout_auc': 0.8272, 'holdout_accuracy': 0.8558, 'holdout_precision': 0.4314, 'holdout_recall': 0.5321, 'holdout_f1_macro': 0.6964, 'holdout_default_rate': 0.1233}
```
```text
/databricks/python/lib/python3.12/site-packages/mlflow/types/utils.py:440: UserWarning: Hint: Inferred schema contains integer column(s). Integer columns in Python cannot represent missing values. If your input data contains missing values at inference time, it will be encoded as floats and will cause a schema enforcement error. The best way to avoid this problem is to infer the model schema based on a realistic data sample (training dataset) that includes missing values. Alternatively, you can declare integer columns as doubles (float64) whenever these columns may have missing values. See `Handling Integers With Missing Values <https://www.mlflow.org/docs/latest/models.html#handling-integers-with-missing-values>`_ for more details.
  warnings.warn(
2026/11/25 08:17:48 WARNING mlflow.models.model: `artifact_path` is deprecated. Please use `name` instead.
🔗 View Logged Model at: https://adb-7405615658009638.18.azuredatabricks.net/ml/experiments/1039038037954034/models/m-5b75410112ba46b8aaac17c9b32f48d1?o=7405615658009638
2026/11/25 08:17:49 INFO mlflow.pyfunc: Validating input example against model signature
Successfully registered model 'demo_unified_model_monitoring.credit_ml.pd_counterparty_default'.
```
```text
Uploading artifacts:   0%|          | 0/11 [00:00<?, ?it/s]
```
```text
🔗 Created version '1' of model 'demo_unified_model_monitoring.credit_ml.pd_counterparty_default': https://adb-7405615658009638.18.azuredatabricks.net/explore/data/models/demo_unified_model_monitoring/credit_ml/pd_counterparty_default/version/1?o=7405615658009638
```
```text
✓ registered demo_unified_model_monitoring.credit_ml.pd_counterparty_default v1
```
```text
/databricks/python/lib/python3.12/site-packages/mlflow/types/utils.py:440: UserWarning: Hint: Inferred schema contains integer column(s). Integer columns in Python cannot represent missing values. If your input data contains missing values at inference time, it will be encoded as floats and will cause a schema enforcement error. The best way to avoid this problem is to infer the model schema based on a realistic data sample (training dataset) that includes missing values. Alternatively, you can declare integer columns as doubles (float64) whenever these columns may have missing values. See `Handling Integers With Missing Values <https://www.mlflow.org/docs/latest/models.html#handling-integers-with-missing-values>`_ for more details.
  warnings.warn(
2026/11/25 08:17:56 WARNING mlflow.models.model: `artifact_path` is deprecated. Please use `name` instead.
🔗 View Logged Model at: https://adb-7405615658009638.18.azuredatabricks.net/ml/experiments/1039038037954034/models/m-fc0d65c42e8f440eaf9aa42251702c76?o=7405615658009638
2026/11/25 08:17:56 INFO mlflow.pyfunc: Validating input example against model signature
Registered model 'demo_unified_model_monitoring.credit_ml.pd_counterparty_default' already exists. Creating a new version of this model...
```
```text
Uploading artifacts:   0%|          | 0/11 [00:00<?, ?it/s]
```
```text
🔗 Created version '2' of model 'demo_unified_model_monitoring.credit_ml.pd_counterparty_default': https://adb-7405615658009638.18.azuredatabricks.net/explore/data/models/demo_unified_model_monitoring/credit_ml/pd_counterparty_default/version/2?o=7405615658009638
```
```text
✓ registered demo_unified_model_monitoring.credit_ml.pd_counterparty_default v2
```
```text
/databricks/python/lib/python3.12/site-packages/mlflow/types/utils.py:440: UserWarning: Hint: Inferred schema contains integer column(s). Integer columns in Python cannot represent missing values. If your input data contains missing values at inference time, it will be encoded as floats and will cause a schema enforcement error. The best way to avoid this problem is to infer the model schema based on a realistic data sample (training dataset) that includes missing values. Alternatively, you can declare integer columns as doubles (float64) whenever these columns may have missing values. See `Handling Integers With Missing Values <https://www.mlflow.org/docs/latest/models.html#handling-integers-with-missing-values>`_ for more details.
  warnings.warn(
2026/11/25 08:18:04 WARNING mlflow.models.model: `artifact_path` is deprecated. Please use `name` instead.
🔗 View Logged Model at: https://adb-7405615658009638.18.azuredatabricks.net/ml/experiments/1039038037954034/models/m-a520149fd8fc4f15a6b1fd2d32880b34?o=7405615658009638
2026/11/25 08:18:05 INFO mlflow.pyfunc: Validating input example against model signature
Successfully registered model 'demo_unified_model_monitoring.markets_ml.trade_volume_forecast'.
```
```text
Uploading artifacts:   0%|          | 0/11 [00:00<?, ?it/s]
```
```text
🔗 Created version '1' of model 'demo_unified_model_monitoring.markets_ml.trade_volume_forecast': https://adb-7405615658009638.18.azuredatabricks.net/explore/data/models/demo_unified_model_monitoring/markets_ml/trade_volume_forecast/version/1?o=7405615658009638
```
```text
✓ registered demo_unified_model_monitoring.markets_ml.trade_volume_forecast v1
Forecast holdout {'holdout_mae': 17.165, 'holdout_rmse': 30.648}
✓ demo_unified_model_monitoring.credit_monitoring.pd_inference_log: 29,261 rows
✓ demo_unified_model_monitoring.credit_ml.pd_baseline: 12,514 rows
✓ demo_unified_model_monitoring.markets_monitoring.trade_volume_scoring_log: 2,700 rows
✓ demo_unified_model_monitoring.markets_ml.trade_volume_baseline: 2,700 rows
✓ demo_unified_model_monitoring.hub_ref.model_version_registry: 3 rows
✓ hub reference tables updated
Creating umm-demo-pd-endpoint …
  08:19:05 {'ready': 'NOT_READY', 'config_update': 'IN_PROGRESS', 'suspend': 'NOT_SUSPENDED', 'system_update_failure': False}
  08:20:05 {'ready': 'NOT_READY', 'config_update': 'IN_PROGRESS', 'suspend': 'NOT_SUSPENDED', 'system_update_failure': False}
  08:21:05 {'ready': 'NOT_READY', 'config_update': 'IN_PROGRESS', 'suspend': 'NOT_SUSPENDED', 'system_update_failure': False}
  08:22:05 {'ready': 'NOT_READY', 'config_update': 'IN_PROGRESS', 'suspend': 'NOT_SUSPENDED', 'system_update_failure': False}
  08:23:05 {'ready': 'NOT_READY', 'config_update': 'IN_PROGRESS', 'suspend': 'NOT_SUSPENDED', 'system_update_failure': False}
  08:24:05 {'ready': 'NOT_READY', 'config_update': 'IN_PROGRESS', 'suspend': 'NOT_SUSPENDED', 'system_update_failure': False}
✓ endpoint READY
✓ 200 live predictions served. Example: {'prediction': 0, 'pd_probability': 0.1401523739539106}
Inference table rows appear in credit_monitoring.pd_endpoint_payload within ~1 hour. Then run UMM_Phase2_PD_Live_Processor.
```
```text
Downloading artifacts:   0%|          | 0/11 [00:00<?, ?it/s]
```
.table-result-container { max-height: 300px; overflow: auto; } table, th, td { border: 1px solid black; border-collapse: collapse; } th, td { padding: 5px; } th { text-align: left; } check result value expected PD log: one row per request PASS 29261 = 29261 PD log: v1 before day 45 PASS 0 = 0 PD log: v2 from day 45 PASS 0 = 0 PD log: last 7 days unlabelled PASS 0 = 0 PD: baseline accuracy PASS 0.85772 0.85–0.92 S1/S2: accuracy drop (base − late) PASS 0.07035 > 0.03 S1: positive rate late / base PASS 1.7701 > 1.6 S2: IN calibration gap PASS 0.1845 > 0.10 Forecast log: 90 days × 30 series PASS 2700 = 2700 S3: MAE shock / pre-shock PASS 5.1733 > 2.0 pd_baseline: has rows PASS 12514 > 0 trade_volume_baseline: has rows PASS 2700 > 0 model_version_registry: has rows PASS 3 > 0 PD baseline: both versions PASS 2 = 2 PD champion alias = v2 PASS 2 = 2 PD champion loads and predicts PASS 5 5 rows, 2 cols Endpoint READY PASS READY READY Inference table enabled PASS True True
```text
All 18 checks passed. Phase 2 is ready.
```
