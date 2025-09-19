# tasks/quality_parallel.py
from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, Optional, List
import json

import polars as pl
from prefect import task, get_run_logger

from configs.salesforce_objects import OBJECT_SPECS, ObjectSpec
from utils.paths import build_paths, build_qc_paths


@task(name="start_gate")
def start_gate(msg: str = "Parallel QA branch started") -> bool:
    get_run_logger().info(msg)
    return True


@task(name="precheck_schema", retries=2, retry_delay_seconds=5)
def precheck_schema(object_name: str, raw_csv: str, write_report: bool = True) -> Dict[str, Any]:
    """
    Validates required columns from OBJECT_SPECS[object_name].
    Writes a JSON report in data/processed/qa/<object>/schema_report.json.
    """
    logger = get_run_logger()
    if object_name not in OBJECT_SPECS:
        raise ValueError(f"Unsupported object: {object_name}")
    spec: ObjectSpec = OBJECT_SPECS[object_name]

    if not Path(raw_csv).exists():
        raise FileNotFoundError(f"Raw CSV not found: {raw_csv}")

    df = pl.read_csv(raw_csv)
    cols = set(df.columns)

    missing = [c for c in spec.required_cols if c not in cols]
    report = {"object": object_name, "raw_csv": raw_csv, "columns_present": sorted(cols), "missing": missing}

    if write_report:
        qa = build_qc_paths(object_name)
        with open(qa["schema_report_json"], "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        logger.info(f"Wrote schema report: {qa['schema_report_json']}")

    if missing:
        logger.error(f"Schema precheck failed: missing {missing}")
        raise ValueError(f"Missing required columns: {missing}")

    logger.info("Schema precheck passed.")
    return report


@task(name="precheck_nonempty", retries=2, retry_delay_seconds=5)
def precheck_nonempty(raw_csv: str) -> Dict[str, Any]:
    """
    Ensures there is at least one row.
    """
    logger = get_run_logger()
    if not Path(raw_csv).exists():
        raise FileNotFoundError(f"Raw CSV not found: {raw_csv}")

    try:
        df = pl.read_csv(raw_csv)
    except pl.exceptions.NoDataError:
        df = pl.DataFrame([])

    n = len(df)
    if n == 0:
        logger.warning("Non-empty precheck failed (0 rows).")
        raise ValueError("No data to process")
    logger.info(f"Non-empty precheck passed: {n} rows.")
    return {"rows": n}


@task(name="deduplicate_by_id", retries=2, retry_delay_seconds=5)
def deduplicate_by_id(object_name: str, raw_csv: str, out_csv: str | None = None) -> str:
    """
    Deduplicate by 'Id' if present; otherwise copy through.
    """
    logger = get_run_logger()
    qa = build_qc_paths(object_name)
    logger.debug(f"QA paths: {qa}")

    # robust path selection
    if not out_csv:
        out_csv = qa.get("dedup_csv") or str(Path(qa["qa_dir"]) / "dedup.csv")

    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
    df = pl.read_csv(raw_csv)

    if "Id" in df.columns:
        before = len(df)
        df = df.unique(subset=["Id"], keep="first")
        logger.info(f"Deduplicated by Id: {before} -> {len(df)} rows")
    else:
        logger.warning("No 'Id' column; writing input as-is")

    df.write_csv(out_csv)
    logger.info(f"Wrote dedup CSV: {out_csv}")
    return out_csv



@task(name="profile_columns", retries=2, retry_delay_seconds=5)
def profile_columns(object_name: str, raw_csv: str, out_json: Optional[str] = None, topk: int = 5) -> str:
    """
    Column profile: dtype, null_count, n_unique, top-k values (if reasonable).
    """
    logger = get_run_logger()
    qa = build_qc_paths(object_name)
    out_json = out_json or qa["profile_json"]
    Path(out_json).parent.mkdir(parents=True, exist_ok=True)

    df = pl.read_csv(raw_csv)
    prof: Dict[str, Any] = {"object": object_name, "rows": len(df), "columns": {}}

    for name, dtype in zip(df.columns, df.dtypes):
        col = pl.col(name)
        nulls = int(df.select(col.is_null().sum()).item())
        nunique = int(df.select(col.n_unique()).item())
        info: Dict[str, Any] = {"dtype": str(dtype), "null_count": nulls, "n_unique": nunique}

        # Only compute top-k for modest-cardinality columns to avoid heavy work
        if nunique and nunique <= 5000:
            vc = df.select(col).to_series().value_counts(sort=True).head(topk)
            # value_counts returns a struct DF in newer Polars; normalize safely
            try:
                top = vc.to_pandas().to_dict(orient="records")  # quick normalization
            except Exception:
                # Fallback: simple sample (no counts)
                top = [{"value": v} for v in df.select(col).to_series().unique().head(topk).to_list()]
            info["top_values"] = top

        prof["columns"][name] = info

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(prof, f, indent=2, ensure_ascii=False)
    logger.info(f"Wrote column profile: {out_json}")
    return out_json


@task(name="snapshot_parquet", retries=2, retry_delay_seconds=5)
def snapshot_parquet(object_name: str, raw_csv: str, out_parquet: Optional[str] = None) -> str:
    """
    Save a Parquet snapshot of the raw CSV for faster future reads.
    """
    logger = get_run_logger()
    qa = build_qc_paths(object_name)
    out_parquet = out_parquet or qa["parquet_snapshot"]
    Path(out_parquet).parent.mkdir(parents=True, exist_ok=True)

    df = pl.read_csv(raw_csv)
    df.write_parquet(out_parquet, compression="snappy")
    logger.info(f"Wrote Parquet snapshot: {out_parquet}")
    return out_parquet


@task(name="rowcount_anomaly_check", retries=0)
def rowcount_anomaly_check(object_name: str, current_rows: int, threshold_ratio: float = 0.5) -> Dict[str, Any]:
    """
    Compares current rowcount to the previous run.
    Warn if relative change exceeds threshold_ratio (e.g., 0.5 = 50%).
    Persists the latest rowcount to qa/<object>/rowcount.txt
    """
    logger = get_run_logger()
    qa = build_qc_paths(object_name)
    rc_path = Path(qa["rowcount_file"])
    prev = None
    if rc_path.exists():
        try:
            prev = int(rc_path.read_text().strip())
        except Exception:
            prev = None

    alert = False
    change = None
    if prev is not None and prev > 0:
        change = abs(current_rows - prev) / prev
        if change >= threshold_ratio:
            alert = True
            logger.warning(f"Rowcount drift for {object_name}: prev={prev}, now={current_rows}, change={change:.2%}")
        else:
            logger.info(f"Rowcount stable for {object_name}: prev={prev}, now={current_rows}, change={change:.2%}")
    else:
        logger.info(f"No previous baseline for {object_name}. Establishing rowcount={current_rows}")

    rc_path.write_text(str(current_rows), encoding="utf-8")
    return {"previous": prev, "current": current_rows, "relative_change": change, "alert": alert}
