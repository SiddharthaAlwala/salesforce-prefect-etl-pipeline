# Salesforce → Polars → JSON ETL Pipeline (Prefect)

## 📌 Overview
This project implements an ETL pipeline using [Prefect](https://www.prefect.io/) that:
1. **Extracts** data from Salesforce via SOQL and saves it to CSV.
2. **Processes** the CSV using [Polars](https://pola.rs/) for data cleaning and aggregation.
3. **Loads** the processed CSV into a JSON file.
4. **Parallel QA/Sidecar** flow (schema checks, dedup, profiling, parquet snapshot, rowcount drift)
5. **Orchestrator** flow that extracts once and runs both Phase-1 (sequential ETL) and Phase-2 (parallel QA) together.

Designed for teams migrating or syncing Salesforce data with repeatable, observable pipelines.

---
## 📄 Description
A **config-driven** ETL for Salesforce data that scales across many objects. You define each object’s **fields**, **group_by**, and **metrics** once in a central registry, and the pipeline runs the same Extract → Process → Load sequence for every object. A **parallel QA** branch runs alongside ETL to produce quality artifacts (schema reports, profiles, parquet snapshots) and simple drift alerts.

**Intended audience**: data engineers, analytics engineers, and platform teams building reliable Salesforce data ingestion with lightweight orchestration.

## ✨ Features

**Object-aware ETL** driven by configs/salesforce_objects.py

**Phase-1 (Sequential)**: SOQL → CSV → Polars aggregation → JSON

**Phase-2 (Parallel QA)**: schema/non-empty gates → dedup/profile/parquet/drift

**Single Extract, Dual Branches**: orchestrator extracts once and fans out

**Robust JSON serialization**: handles datetimes, Polars/NumPy types

**Safe I/O**: timestamped raw filenames to avoid clobbering across runs

**Scheduling**: simple cron via flow.serve() every 15 minutes

**Extensible**: add custom Salesforce objects & validators easily



## 🧩 Project Structure
```bash
sf-prefect-pipeline/
├─ configs/
│  └─ salesforce_objects.py           # Object registry: fields, required cols, group_by, metrics
├─ utils/
│  └─ paths.py                        # Path builders: build_paths(), build_qc_paths()
├─ tasks/
│  ├─ extract.py                      # resolve_extract_plan(), extract_salesforce_to_csv (task)
│  ├─ process.py                      # process_object_data (task, Polars aggregations)
│  ├─ load.py                         # load_csv_to_json (task, robust JSON)
│  └─ quality_parallel.py             # QA tasks: schema/nonempty/dedup/profile/parquet/drift
├─ flows/
│  ├─ sf_csv_polars_json_flow.py      # Phase-1: sequential ETL flow
│  ├─ object_parallel_flow.py         # Phase-2: parallel QA flow
│  └─ sf_etl_orchestrator_flow.py     # Orchestrator: runs Phase-1 + Phase-2 together
├─ serve_orchestrator_15min.py        # Schedule orchestrator every 15 minutes
├─ serve_phase1_15min.py              # (optional) Schedule Phase-1 only
├─ serve_parallel_qa_15min.py         # (optional) Schedule Phase-2 only
├─ run_multiple_times.py              # Quick local runner (typically calls orchestrator)
├─ data/
│  ├─ raw/                            # <object>_YYYYMMDD-HHMMSS_<RUNID>.csv
│  ├─ processed/
│  │  ├─ <object>/summary.csv
│  │  └─ qa/<object>/
│  │      ├─ dedup.csv
│  │      ├─ profile.json
│  │      ├─ schema_report.json
│  │      ├─ snapshot.parquet
│  │      └─ rowcount.txt
│  └─ output/
│     └─ <object>/summary.json
└─ README.md

```

---

## ⚡ Installation
```bash
1. **Create and activate local environment**

conda create -p .\.conda_env python=3.11 -y
conda activate .\.conda_env
```

Install dependencies
```bash
    python -m pip install --upgrade pip
    python -m pip install prefect polars simple-salesforce python-dotenv pyarrow pandas numpy
```

Add Salesforce credentials
    Create .env in project root:
```bash

        SF_USERNAME=your_username
        SF_PASSWORD=your_password
        SF_TOKEN=your_security_token
        SF_DOMAIN=login
```

▶️ Running Manually


Run the flow 3 times for testing:
```bash
    python run_multiple_times.py
```

⏰ Scheduling Every 15 Minutes

    1. Start Prefect server
```bash
        python -m prefect server start
```
    2. Apply the deployment and start agent
```bash
        python deployments/apply_15min_deployment.py
        python -m prefect agent start -q default
```

    3. Open Prefect UI
        http://127.0.0.1:4200

## 🧰 Technologies

**Python 3.10+**
**Prefect 2** (flows, task runners, scheduling)

**Polars** (fast DataFrame engine)

**simple-salesforce** (Salesforce API)

**python-dotenv** (env management)

**PyArrow / Parquet** (columnar snapshots)

## ✅ Notes

You can pass a **custom SOQL** to the orchestrator/flows; processing still uses the registry’s 
```bash
group_by/metrics.
```

Tasks include **retries** and structured **logging**.

Empty pulls are handled gracefully: headers-only CSV is written; QA will flag empty data.

Raw files are timestamped by default to avoid clobbering concurrent runs.

QA failures can be **advisory** (non-strict) or **strict** (fail_on_qa_error=True).