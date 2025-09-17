# Salesforce → Polars → JSON ETL Pipeline (Prefect)

## 📌 Overview
This project implements an ETL pipeline using [Prefect](https://www.prefect.io/) that:
1. **Extracts** data from Salesforce via SOQL and saves it to CSV.
2. **Processes** the CSV using [Polars](https://pola.rs/) for data cleaning and aggregation.
3. **Loads** the processed CSV into a JSON file.

All three steps are implemented as **Prefect tasks** and orchestrated in a **Prefect flow**.  
The flow runs **every 15 minutes** using a Prefect deployment.

---

## 🧩 Project Structure
sf-prefect-pipeline/
├── tasks/
│ ├── extract.py
│ ├── process.py
│ └── load.py
├── flows/
│ └── sf_csv_polars_json_flow.py
├── deployments/
│ └── apply_15min_deployment.py
├── run_multiple_times.py
├── data/raw/
├── data/processed/
└── data/output/

yaml
Copy code

---

## ⚡ Setup

1. **Create local environment**
```bash
conda create -p .\.conda_env python=3.11 -y
conda activate .\.conda_env
Install dependencies

bash
Copy code
python -m pip install --upgrade pip
python -m pip install prefect polars simple-salesforce python-dotenv pyarrow pandas numpy
Add Salesforce credentials
Create .env in project root:

ini
Copy code
SF_USERNAME=your_username
SF_PASSWORD=your_password
SF_TOKEN=your_security_token
SF_DOMAIN=login
▶️ Running Manually
Run the flow 3 times for testing:

bash
Copy code
python run_multiple_times.py
⏰ Scheduling Every 15 Minutes
1. Start Prefect server

bash
Copy code
python -m prefect server start
2. Apply the deployment and start agent

bash
Copy code
python deployments/apply_15min_deployment.py
python -m prefect agent start -q default
3. Open Prefect UI
http://127.0.0.1:4200

✅ Output Files
data/raw/*.csv → Extracted Salesforce data

data/processed/*.csv → Aggregated data

data/output/*.json → Final JSON export

📌 Notes
The soql query can be changed in sf_csv_polars_json_flow.py to target any Salesforce object (e.g. Account, Contact, Opportunity).

Tasks have built-in retries and logging.

Handles empty CSVs gracefully without crashing.