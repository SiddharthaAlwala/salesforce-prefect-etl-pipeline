# tasks/extract.py
import os
from dotenv import load_dotenv
from prefect import get_run_logger, task
from datetime import timedelta
from prefect.tasks import task_input_hash
from typing import List, Dict, Optional, Tuple
from pathlib import Path
from simple_salesforce import Salesforce, SalesforceAuthenticationFailed
import polars as pl

from configs.salesforce_objects import OBJECT_SPECS, ObjectSpec
from utils.paths import build_paths


def _parse_select_fields(soql: str) -> list[str]:
    s = soql.strip()
    try:
        select_part = s[s.upper().index("SELECT")+6 : s.upper().index(" FROM ")].strip()
    except ValueError:
        return []
    fields = [f.strip() for f in select_part.split(",")]
    cleaned = []
    for f in fields:
        parts = f.split(" AS ")
        cleaned.append(parts[-1].strip())
    return cleaned

def _build_soql_from_spec(spec: ObjectSpec, limit: int = 100) -> str:
    select_part = ", ".join(spec.fields)
    where_part  = f" {spec.where.strip()}" if spec.where and spec.where.strip() else ""
    limit_part  = f" LIMIT {limit}" if limit else ""
    return f"SELECT {select_part} FROM {spec.api_name}{where_part}{limit_part}"

def _login_salesforce() -> Salesforce:
    load_dotenv()
    return Salesforce(
        username=os.environ["SF_USERNAME"],
        password=os.environ["SF_PASSWORD"],
        security_token=os.environ["SF_TOKEN"],
        domain=os.getenv("SF_DOMAIN", "login"),
    )

def resolve_extract_plan(object_name: str,
                         soql: Optional[str] = None,
                         limit: int = 100,
                         out_csv: Optional[str] = None) -> Tuple[str, str]:
    if object_name not in OBJECT_SPECS:
        raise ValueError(f"Unsupported Salesforce object: {object_name}")
    spec: ObjectSpec = OBJECT_SPECS[object_name]
    # build SQL if not provided
    select = ", ".join(spec.fields)
    where  = f" {spec.where.strip()}" if spec.where and spec.where.strip() else ""
    sql    = soql or f"SELECT {select} FROM {spec.api_name}{where} LIMIT {limit}"
    # choose output path if not provided
    out_csv = out_csv or build_paths(object_name)["raw_csv"]
    return sql, out_csv

@task(
    name="extract_salesforce_to_csv",
    retries=3,
    retry_delay_seconds=10,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=2),
)
def extract_salesforce_to_csv(soql: str, out_csv: str) -> str:
    """
    Generic SOQL executor -> CSV writer.
    """
    logger = get_run_logger()
    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)

    try:
        sf = _login_salesforce()
    except SalesforceAuthenticationFailed as e:
        raise RuntimeError(f"Salesforce auth failed: {e}")

    logger.info("Running SOQL against Salesforce")
    result = sf.query_all(soql)
    records: List[Dict] = result.get("records", [])

    for r in records:
        r.pop("attributes", None)

    if not records:
        headers = _parse_select_fields(soql)
        logger.warning(f"SOQL returned 0 records; writing empty CSV with headers: {headers}")
        empty_df = pl.DataFrame({h: pl.Series([], dtype=pl.Utf8) for h in headers}) if headers else pl.DataFrame([])
        empty_df.write_csv(out_csv)
        return out_csv

    df = pl.DataFrame(records)
    # tolerant casts commonly present in SF
    if "Amount" in df.columns:
        df = df.with_columns(pl.col("Amount").cast(pl.Float64, strict=False))
    if "CloseDate" in df.columns:
        df = df.with_columns(pl.col("CloseDate").str.to_date(strict=False))

    df.write_csv(out_csv)
    logger.info(f"Wrote raw CSV: {out_csv} ({len(df):,} rows)")
    return out_csv

@task(name="extract_object_to_csv", retries=3, retry_delay_seconds=10)
def extract_object_to_csv(object_name: str,
                          soql: Optional[str] = None,
                          limit: int = 100,
                          out_csv: Optional[str] = None) -> str:
    """
    Registry-aware extract:
      - builds SOQL from the object spec if not provided
      - writes to data/raw/<object>.csv by default
    """
    logger = get_run_logger()
    if object_name not in OBJECT_SPECS:
        raise ValueError(f"Unsupported Salesforce object: {object_name}")
    spec: ObjectSpec = OBJECT_SPECS[object_name]

    sql = soql or _build_soql_from_spec(spec, limit=limit)
    paths = build_paths(object_name)
    out_csv = out_csv or paths["raw_csv"]

    logger.info(f"Extracting {spec.api_name} -> {out_csv}")
    return extract_salesforce_to_csv(soql=sql, out_csv=out_csv)
