# E-Commerce Lakehouse Pipeline

## Project Overview
This repository contains a complete, production-ready analytics pipeline for a fictitious e-commerce company, implemented end-to-end on **Azure Databricks**, **Delta Lake**, and **Azure Data Lake Storage Gen2 (ADLS Gen2)**.  
The solution ingests multi-channel transaction data every five minutes, enriches it with product, customer, and campaign reference data, generates near real-time **Gold-layer customer insights**, optimises storage daily, and executes automated data-quality checks with alerting.

---

## High-Level Architecture
```
┌────────────┐    Autoloader (5 min)   ┌──────────────┐   DLT Continuous   ┌───────────┐
│  ADLS Gen2 │ ───────────────────────▶│  Bronze (Δ)  │ ──────────────────▶│  Silver   │
└────────────┘        CSV / JSON       └──────────────┘    Enrichment      └───────────┘
                                                            │
                                                            ▼
                                                      ┌───────────┐
                                                      │  Gold     │
                                                      │  Managed  │
                                                      │  Insights │
                                                      └───────────┘
                                                            │
                                     Daily Notebook ────────┘ (OPTIMIZE + Z-ORDER)
                                                            │
                                     Deequ / DLT  ──────────┘ (Quality Monitor)
```
* All storage layers are **Delta tables** with ACID transactions, time travel, and schema evolution.  
* Workflows are orchestrated with **Databricks Jobs** in the following order:
  1. **Ingest** → 2. **Transform** → 3. **Optimise** → 4. **Quality Monitor**.

---

## Prerequisites
| Azure Resource | Purpose | Notes |
|----------------|---------|-------|
| ADLS Gen2       | Raw file landing zone | Enable **Hierarchical Namespace** |
| Azure Databricks | Processing & lakehouse engine | DBR 13.3 LTS or later |
| Azure Key Vault | Secret management | Stores service-principal creds |
| Service Principal | Secure OAuth to storage | Role: *Storage Blob Data Contributor* |

Local tools: `git`, `Python 3.9+` (optional for testing).

---

## Quick Start
```bash
# 1 — Clone the repo
$ git clone https://github.com/Aurelius1618/ecommerce-pipeline.git

# 2 — Import repo into Databricks (Repos → Add Repo)
# 3 — Run notebooks in numerical order
# 4 — Create production jobs via Workflows > Create Job (see below)
```

---

## Repository Layout
```
/Repos/ecommerce-pipeline
│
├── 00_setup/              Helper functions & schemas
├── 01_ingest/             Autoloader & Bronze tables
├── 02_transform/          Silver joins & analytics
├── 03_insights/           Gold customer KPIs
├── 04_optimize/           Storage optimisation
├── 05_quality_monitor/    Data-quality checks & alerts
├── production/            Notebooks used by automated jobs
├── monitoring/            Dashboards & alert-rules
├── data/                  (optional) sample CSVs
└── README.md              This file
```

---

## Key Notebooks
| Path | Purpose |
|------|---------|
| `00_setup/config_helper` | Central schemas, reusable I/O & Delta helpers |
| `01_ingest/bronze_layer` | One-off bulk load of historical CSVs |
| `production/autoloader_ingestion` | **Autoloader** stream for new files every 5 min |
| `production/dlt_transformation_pipeline` | **Delta Live Tables** continuous pipeline |
| `04_optimize/delta_optimize_storage` | Daily `OPTIMIZE`, `ZORDER`, `VACUUM` with metrics |
| `05_quality_monitor/data_quality_monitor` | PyDeequ validation, anomaly detection, alerting |

---

## Automated Workflows
Create four jobs, then a master **Workflow** that enforces dependencies:

| Task Key | Type | Trigger | Key Params |
|----------|------|---------|------------|
| `autoloader_ingestion` | Notebook | **Every 5 min** | `cloudFiles.inferColumnTypes=true` |
| `dlt_transformation` | DLT Pipeline (continuous) | **On start** | Built-in expectations |
| `daily_optimization` | Notebook | **02:00 UTC daily** | `spark.databricks.delta.optimizeWrite=true` |
| `quality_monitoring` | Notebook | **After optimisation** | Slack/email alerts on failure |

*Master schedule*: **01:00 UTC** daily. Retries: 3.  
Success & failure notifications to data & ops teams.

---

## Data-Quality Monitoring
* **PyDeequ** rules validate completeness, uniqueness, and business logic.  
* Results are stored in `gold.quality_metrics_summary` + `gold.quality_metrics_detail`.  
* Alerts are written to `gold.quality_alerts` and optionally pushed to Slack/email.

---

## CI/CD Tips
1. Enable [Databricks Repos → GitHub Actions] to auto-run unit tests or notebook smoke tests.  
2. Protect `main` with pull-request checks (e.g., `pytest`, `black`, `nb-strip-out`).  
3. Use Databricks CLI `databricks workflow export|deploy` for promotion between **dev → stg → prod** workspaces.

---

## Troubleshooting
| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| Mount command fails | SPN lacks storage role | Add **Storage Blob Data Contributor** on ADLS account |
| DLT pipeline stuck in *FAILED* | Expectation drop ratio > threshold | Inspect *Data Quality* tab & bad-records path |
| OPTIMIZE job takes hours | Table has billions of tiny files | Increase Autoloader batch size & enable `autoCompact` |
| Quality alerts every run | Rules too strict | Adjust thresholds or mark columns as *nullable* |

---
## Authors
*Shreyas Sen* – *Data Engineer*  
LinkedIn: <https://linkedin.com/in/shreyas-sen-10a592281/>

Happy Lakehousing! 🚀
