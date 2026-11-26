# Real credit PD model output: latest 25 scored requests

Executed 2026-11-26 08:40 UTC on serverless compute against Unity Catalog.

```sql
select request_id, request_time, model_version, country, product, sector, internal_rating, round(pd_probability,4) pd_probability, prediction, label, served_entity, record_source from demo_unified_model_monitoring.credit_monitoring.pd_inference_log order by request_time desc limit 25
```

Result (25 rows):

| request_id | request_time | model_version | country | product | sector | internal_rating | pd_probability | prediction | label | served_entity | record_source |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-10 | 2026-11-25 10:16:53.582000 | 2 | US | Term Loan | Infrastructure | 7 | 0.2875 | 1 | nan | pd-v2 | live |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-11 | 2026-11-25 10:16:53.582000 | 2 | IN | Project Finance | Technology | 2 | 0.0107 | 0 | nan | pd-v2 | live |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-4 | 2026-11-25 10:16:53.582000 | 2 | US | Revolving Credit | Manufacturing | 4 | 0.0209 | 0 | nan | pd-v2 | live |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-7 | 2026-11-25 10:16:53.582000 | 2 | SG | Term Loan | Financial Institutions | 6 | 0.3454 | 1 | nan | pd-v2 | live |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-22 | 2026-11-25 10:16:53.582000 | 2 | CN | Term Loan | Manufacturing | 3 | 0.0393 | 0 | nan | pd-v2 | live |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-3 | 2026-11-25 10:16:53.582000 | 2 | HK | Revolving Credit | Manufacturing | 6 | 0.0643 | 0 | nan | pd-v2 | live |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-1 | 2026-11-25 10:16:53.582000 | 2 | IN | Revolving Credit | Real Estate | 3 | 0.0426 | 0 | nan | pd-v2 | live |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-0 | 2026-11-25 10:16:53.582000 | 2 | GB | Term Loan | Technology | 5 | 0.1878 | 0 | nan | pd-v2 | live |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-17 | 2026-11-25 10:16:53.582000 | 2 | CN | Revolving Credit | Infrastructure | 3 | 0.0218 | 0 | nan | pd-v2 | live |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-18 | 2026-11-25 10:16:53.582000 | 2 | IN | Trade Finance | Infrastructure | 4 | 0.0524 | 0 | nan | pd-v2 | live |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-15 | 2026-11-25 10:16:53.582000 | 2 | SG | Trade Finance | Real Estate | 5 | 0.1171 | 0 | nan | pd-v2 | live |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-16 | 2026-11-25 10:16:53.582000 | 2 | KR | Term Loan | Consumer | 3 | 0.0282 | 0 | nan | pd-v2 | live |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-20 | 2026-11-25 10:16:53.582000 | 2 | SG | Term Loan | Consumer | 4 | 0.0637 | 0 | nan | pd-v2 | live |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-12 | 2026-11-25 10:16:53.582000 | 2 | SG | Term Loan | Financial Institutions | 6 | 0.257 | 1 | nan | pd-v2 | live |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-9 | 2026-11-25 10:16:53.582000 | 2 | KR | Revolving Credit | Infrastructure | 4 | 0.0751 | 0 | nan | pd-v2 | live |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-5 | 2026-11-25 10:16:53.582000 | 2 | US | Revolving Credit | Real Estate | 8 | 0.6235 | 1 | nan | pd-v2 | live |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-2 | 2026-11-25 10:16:53.582000 | 2 | GB | Term Loan | Infrastructure | 3 | 0.0167 | 0 | nan | pd-v2 | live |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-19 | 2026-11-25 10:16:53.582000 | 2 | HK | Project Finance | Commodities | 10 | 0.5993 | 1 | nan | pd-v2 | live |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-23 | 2026-11-25 10:16:53.582000 | 2 | CN | Term Loan | Infrastructure | 6 | 0.5628 | 1 | nan | pd-v2 | live |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-13 | 2026-11-25 10:16:53.582000 | 2 | SG | Project Finance | Financial Institutions | 4 | 0.0584 | 0 | nan | pd-v2 | live |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-6 | 2026-11-25 10:16:53.582000 | 2 | GB | Project Finance | Consumer | 5 | 0.0354 | 0 | nan | pd-v2 | live |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-21 | 2026-11-25 10:16:53.582000 | 2 | IN | Project Finance | Infrastructure | 5 | 0.2038 | 0 | nan | pd-v2 | live |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-14 | 2026-11-25 10:16:53.582000 | 2 | SG | Term Loan | Manufacturing | 5 | 0.0718 | 0 | nan | pd-v2 | live |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-8 | 2026-11-25 10:16:53.582000 | 2 | GB | Revolving Credit | Manufacturing | 6 | 0.2336 | 1 | nan | pd-v2 | live |
| 9f641cf3-9ff9-4710-b2ae-304009440d7b-24 | 2026-11-25 10:16:53.582000 | 2 | IN | Revolving Credit | Manufacturing | 6 | 0.1185 | 0 | nan | pd-v2 | live |
