import os
from dotenv import load_dotenv
from prefect import get_run_logger, task
from datetime import timedelta
from prefect.tasks import task_input_hash
from typing import List, Dict
from pathlib import Path
from simple_salesforce import Salesforce, SalesforceAuthenticationFailed
import polars as pl


def _parse_select_fields(soql:str)-> list[str]:
    s = soql.strip()
    try:
        select_part = s[s.upper().index("SELECT")+6 : s.upper().index(" FROM ")]. strip()
    except ValueError:
        return []
    fields = [f.strip() for f in select_part.split(",")]
    cleaned =[]
    for f in fields:
        parts = f.split(" AS ")
        cleaned.append(parts[-1].strip())
    return cleaned

@task(
    name="extract_salesforce_to_csv",
    retries=3,
    retry_delay_seconds=10,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=2),
)
def extract_salesforce_to_csv(soql:str, out_csv = "data/raw/sf_opportunities.csv")->str:
    """
    Extract Salesforce data using SOQL and write to CSV.
    Returns path to the written CSV.
    """
    load_dotenv()
    logger = get_run_logger()
    Path(out_csv).parent.mkdir(parents=True, exist_ok = True)

    try:
        sf = Salesforce(
            username = os.environ["SF_USERNAME"],
            password=os.environ["SF_PASSWORD"],
            security_token = os.environ["SF_TOKEN"],
            domain=os.getenv("SF_DOMAIN", "login")
        )

    except SalesforceAuthenticationFailed as e:
        raise RuntimeError(f"Salesforce auth failed:{e}")
    
    logger.info("Running SOQL against Salesforce")
    result = sf.query_all(soql)
    records :List[Dict] = result.get("records", [])

    #Strip SF 'attributes' key
    for r in records:
        r.pop("attributes", None)
    
    if not records:
        headers = _parse_select_fields(soql)
        logger.warning("SOQL returned 0 records; writing empty csv with headers:{headers}")
        empty_df = pl.DataFrame({h: pl.Series([], dtype=pl.Utf8) for h in headers}) if headers else pl.DataFrame([])
        empty_df.write_csv(out_csv)
        return out_csv
    
    df = pl.DataFrame(records)
    if "Amount" in df.columns:
        df = df.with_columns(pl.col("Amount").cast(pl.Float64, strict=False))
    if "CloseDate" in df.columns:
        df = df.with_columns(pl.col("CloseDate").str.to_date(strict=False))
    
    df.write_csv(out_csv)
    logger.info(f"wrote raw CSV: {out_csv}{len(df):,} rows")
    return out_csv


