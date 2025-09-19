# flows/sf_etl_orchestrator_flow.py
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Any

from prefect import flow, get_run_logger
from prefect.context import get_run_context
from prefect.task_runners import ConcurrentTaskRunner

from configs.salesforce_objects import OBJECT_SPECS, ObjectSpec
from utils.paths import build_paths
from tasks.extract import resolve_extract_plan, extract_salesforce_to_csv
from tasks.process import process_object_data
from tasks.load import load_csv_to_json
from tasks.quality_parallel import (
    start_gate,
    precheck_schema,
    precheck_nonempty,
    deduplicate_by_id,
    profile_columns,
    snapshot_parquet,
    rowcount_anomaly_check,
)

def _timestamped_raw_path(base_raw_csv: str) -> str:
    """
    Build a unique, timestamped raw CSV path to avoid clobbering across concurrent runs.
    Example: data/raw/account_20250919-083015_C89E1533.csv
    """
    p = Path(base_raw_csv)
    stem = p.stem
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

    # flow_run.id is a UUID — cast to str before slicing
    run_id_short = "local"
    try:
        ctx = get_run_context()
        if ctx and getattr(ctx, "flow_run", None) and getattr(ctx.flow_run, "id", None):
            run_id_short = str(ctx.flow_run.id)[:8].upper()
    except Exception:
        # if called outside a Prefect run (e.g., unit tests), keep 'local'
        pass

    return str(p.with_name(f"{stem}_{ts}_{run_id_short}{p.suffix}"))
@flow(
    name="sf-etl-orchestrator",
    task_runner=ConcurrentTaskRunner(max_workers=8),  # tune per host
)
def sf_etl_orchestrator(
    object_name: str = "Account",
    soql: Optional[str] = None,
    limit: int = 100,
    *,
    timestamp_raw: bool = True,     # avoid file clobbering across concurrent runs
    fail_on_qa_error: bool = False, # strict mode: fail flow if a QA task fails
) -> Dict[str, Any]:
    """
    Production orchestrator: Extract once, then run Phase-1 ETL (sequential)
    and Phase-2 QA (parallel) in the same flow, object-aware.

    DAG:
        extract_salesforce_to_csv
           ├─▶ Phase-1: process_object_data ─▶ load_csv_to_json
           └─▶ Phase-2 QA:
                 start_gate
                   ├─▶ precheck_schema
                   └─▶ precheck_nonempty
                         ├─▶ deduplicate_by_id
                         ├─▶ profile_columns
                         ├─▶ snapshot_parquet
                         └─▶ rowcount_anomaly_check
    """
    logger = get_run_logger()

    # Validate & resolve paths
    if object_name not in OBJECT_SPECS:
        raise ValueError(f"Unsupported Salesforce object: {object_name}")
    spec: ObjectSpec = OBJECT_SPECS[object_name]  # noqa: F841 (kept for clarity)
    io_paths = build_paths(object_name)

    # Decide raw path (timestamped to prevent clobbering)
    raw_target_path = _timestamped_raw_path(io_paths["raw_csv"]) if timestamp_raw else io_paths["raw_csv"]

    # Single extract plan reused by both branches
    sql, planned_raw = resolve_extract_plan(
        object_name=object_name,
        soql=soql,
        out_csv=raw_target_path,
        limit=limit,
    )

    # ── Extract (once) ──────────────────────────────────────────────────────────
    raw_path = extract_salesforce_to_csv(soql=sql, out_csv=planned_raw)

    # ── Branch A: Phase-1 (sequential inside branch) ───────────────────────────
    processed_f = process_object_data.submit(
        object_name=object_name,
        in_csv=raw_path,
        out_csv=io_paths["processed_csv"],
    )
    json_f = load_csv_to_json.submit(
        object_name=object_name,
        in_csv=processed_f,                 # depends on processed_f
        out_json=io_paths["out_json"],
    )

    # ── Branch B: Phase-2 QA (parallel) ────────────────────────────────────────
    gate = start_gate.submit()
    schema_f = precheck_schema.submit(object_name, raw_path, wait_for=[gate])
    nonempty_f = precheck_nonempty.submit(raw_path, wait_for=[gate])

    # Workers fire after both prechecks succeed
    dedup_f   = deduplicate_by_id.submit(object_name, raw_path, wait_for=[schema_f, nonempty_f])
    profile_f = profile_columns.submit(object_name, raw_path, wait_for=[schema_f, nonempty_f])
    parquet_f = snapshot_parquet.submit(object_name, raw_path, wait_for=[schema_f, nonempty_f])

    # Rowcount anomaly uses count from nonempty_f
    current_rows = nonempty_f.result()["rows"]
    drift_f = rowcount_anomaly_check.submit(object_name, current_rows, wait_for=[nonempty_f])

    # Optional strict mode: surface QA failures
    if fail_on_qa_error:
        _ = [
            schema_f.result(),
            nonempty_f.result(),
            dedup_f.result(),
            profile_f.result(),
            parquet_f.result(),
            drift_f.result(),
        ]

    # ── Join & return summary ──────────────────────────────────────────────────
    outputs: Dict[str, Any] = {
        "object": object_name,
        "raw_csv": raw_path,
        "phase1": {
            "processed_csv": processed_f.result(),
            "json": json_f.result(),
        },
        "qa": {
            "dedup_csv": dedup_f.result(),
            "profile_json": profile_f.result(),
            "parquet_snapshot": parquet_f.result(),
            "rowcount_anomaly": drift_f.result(),
        },
    }
    logger.info(f"sf-etl-orchestrator outputs: {outputs}")
    return outputs
