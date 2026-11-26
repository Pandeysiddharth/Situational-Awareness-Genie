# Markets trade-volume forecast vs actual (latest 20)

Executed 2026-11-26 08:41 UTC on serverless compute against Unity Catalog.

```sql
select trade_date, scoring_time, country, product, model_version, round(prediction,1) prediction, label actual, round(residual,1) residual, record_source from demo_unified_model_monitoring.markets_monitoring.trade_volume_scoring_log order by trade_date desc, country, product limit 20
```

Result (20 rows):

| trade_date | scoring_time | country | product | model_version | prediction | actual | residual | record_source |
|---|---|---|---|---|---|---|---|---|
| 2026-11-25 | 2026-11-25 02:00:00 | AE | FX Forward | 1 | 124.4 | 129.861 | 5.4 | live |
| 2026-11-25 | 2026-11-25 02:00:00 | AE | FX Spot | 1 | 288.0 | 267.885 | -20.1 | live |
| 2026-11-25 | 2026-11-25 02:00:00 | AE | FX Swap | 1 | 221.3 | 195.646 | -25.6 | live |
| 2026-11-25 | 2026-11-25 02:00:00 | AE | Rates Swap | 1 | 80.3 | 96.596 | 16.3 | live |
| 2026-11-25 | 2026-11-25 02:00:00 | AE | Trade Finance | 1 | 26.1 | 26.393 | 0.3 | live |
| 2026-11-25 | 2026-11-25 02:00:00 | GB | FX Forward | 1 | 594.2 | 510.38 | -83.8 | live |
| 2026-11-25 | 2026-11-25 02:00:00 | GB | FX Spot | 1 | 824.3 | 1004.676 | 180.4 | live |
| 2026-11-25 | 2026-11-25 02:00:00 | GB | FX Swap | 1 | 802.5 | 734.609 | -67.8 | live |
| 2026-11-25 | 2026-11-25 02:00:00 | GB | Rates Swap | 1 | 296.5 | 295.291 | -1.2 | live |
| 2026-11-25 | 2026-11-25 02:00:00 | GB | Trade Finance | 1 | 106.8 | 102.379 | -4.4 | live |
| 2026-11-25 | 2026-11-25 02:00:00 | HK | FX Forward | 1 | 444.7 | 429.288 | -15.4 | live |
| 2026-11-25 | 2026-11-25 02:00:00 | HK | FX Spot | 1 | 785.7 | 712.386 | -73.3 | live |
| 2026-11-25 | 2026-11-25 02:00:00 | HK | FX Swap | 1 | 653.7 | 597.002 | -56.7 | live |
| 2026-11-25 | 2026-11-25 02:00:00 | HK | Rates Swap | 1 | 241.5 | 228.2 | -13.3 | live |
| 2026-11-25 | 2026-11-25 02:00:00 | HK | Trade Finance | 1 | 77.0 | 73.077 | -3.9 | live |
| 2026-11-25 | 2026-11-25 02:00:00 | IN | FX Forward | 1 | 228.9 | 193.074 | -35.8 | live |
| 2026-11-25 | 2026-11-25 02:00:00 | IN | FX Spot | 1 | 417.0 | 391.878 | -25.1 | live |
| 2026-11-25 | 2026-11-25 02:00:00 | IN | FX Swap | 1 | 307.6 | 333.591 | 26.0 | live |
| 2026-11-25 | 2026-11-25 02:00:00 | IN | Rates Swap | 1 | 113.6 | 105.166 | -8.4 | live |
| 2026-11-25 | 2026-11-25 02:00:00 | IN | Trade Finance | 1 | 43.2 | 33.35 | -9.8 | live |
