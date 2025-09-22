# flows/sf_etl_orchestrator_flow.py
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Any, Tuple

import time
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
from tasks.metadata import record_run_metadata


def _timestamped_raw_path(base_raw_csv: str) -> str:
    """
    Build a unique, timestamped raw CSV path to avoid clobbering across concurrent runs.
    Example: data/raw/account_20250919-083015_C89E1533.csv
    """
    p = Path(base_raw_csv)
    stem = p.stem
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

    run_id_short = "LOCAL"
    try:
        ctx = get_run_context()
        if ctx and getattr(ctx, "flow_run", None) and getattr(ctx.flow_run, "id", None):
            run_id_short = str(ctx.flow_run.id)[:8].upper()
    except Exception:
        pass

    return str(p.with_name(f"{stem}_{ts}_{run_id_short}{p.suffix}"))


def _collect_result(logger, fut, name: str, strict: bool) -> Tuple[Any, bool]:
    """
    Robust future collector:
      - Returns (value, ok_bool)
      - Uses fut.result() as the source of truth; logs and optionally raises on failure.
    """
    if fut is None:
        return None, False
    try:
        val = fut.result()  # raises if the task failed
        return val, True
    except Exception as exc:
        logger.error(f"{name} failed: {exc}")
        if strict:
            raise
        return None, False


def _label_from_future(fut) -> str:
    """
    Produce a simple label for metadata: COMPLETED / FAILED / UNKNOWN.
    """
    if fut is None:
        return "UNKNOWN"
    try:
        fut.result()
        return "COMPLETED"
    except Exception:
        return "FAILED"


@flow(
    name="sf-etl-orchestrator",
    task_runner=ConcurrentTaskRunner(max_workers=8),  # tune per host
)
def sf_etl_orchestrator(
    object_name: str = "Account",
    soql: Optional[str] = None,
    limit: int = 100,
    *,
    timestamp_raw: bool = True,      # avoid file clobbering across concurrent runs
    fail_on_qa_error: bool = False,  # strict mode: fail flow if a QA task fails
) -> Dict[str, Any]:
    """
    Orchestrator: Extract once, then run the ETL branch (sequential)
    and the Quality/Observability branch (parallel) in the same flow, object-aware.

    DAG:
        extract_salesforce_to_csv
           ├─▶ ETL: process_object_data ─▶ load_csv_to_json
           └─▶ Quality/Observability:
                 start_gate
                   ├─▶ precheck_schema
                   └─▶ precheck_nonempty
                         ├─▶ deduplicate_by_id
                         ├─▶ profile_columns
                         ├─▶ snapshot_parquet
                         └─▶ rowcount_anomaly_check
    """
    logger = get_run_logger()
    run_t0 = time.time()

    # Validate & resolve paths
    if object_name not in OBJECT_SPECS:
        raise ValueError(f"Unsupported Salesforce object: {object_name}")
    _spec: ObjectSpec = OBJECT_SPECS[object_name]  # kept for clarity
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

    # ── ETL branch (sequential inside branch) ───────────────────────────────────
    processed_f = process_object_data.submit(
        object_name=object_name,
        in_csv=raw_path,
        out_csv=io_paths["processed_csv"],
    )
    json_f = load_csv_to_json.submit(
        object_name=object_name,
        in_csv=processed_f,  # depends on processed_f
        out_json=io_paths["out_json"],
    )

    # ── Quality/Observability branch (parallel) ─────────────────────────────────
    gate = start_gate.submit()
    schema_f = precheck_schema.submit(object_name, raw_path, wait_for=[gate])
    nonempty_f = precheck_nonempty.submit(raw_path, wait_for=[gate])

    # Workers fire after both prechecks succeed
    dedup_f = deduplicate_by_id.submit(object_name, raw_path, wait_for=[schema_f, nonempty_f])
    prof_f = profile_columns.submit(object_name, raw_path, wait_for=[schema_f, nonempty_f])
    parquet_f = snapshot_parquet.submit(object_name, raw_path, wait_for=[schema_f, nonempty_f])

    # Rowcount anomaly uses count from nonempty_f, but guard for errors
    try:
        current_rows = nonempty_f.result()["rows"]
        drift_f = rowcount_anomaly_check.submit(object_name, current_rows, wait_for=[nonempty_f])
    except Exception:
        current_rows = None
        drift_f = None

    # ── Collect results (strict for ETL; QA respects fail_on_qa_error) ─────────
    processed_csv, _ok1 = _collect_result(logger, processed_f, "process_object_data", strict=True)
    json_path, _ok2 = _collect_result(logger, json_f, "load_csv_to_json", strict=True)

    _ = _collect_result(logger, schema_f, "precheck_schema", strict=fail_on_qa_error)
    _ = _collect_result(logger, nonempty_f, "precheck_nonempty", strict=fail_on_qa_error)
    dedup_csv, _ = _collect_result(logger, dedup_f, "deduplicate_by_id", strict=fail_on_qa_error)
    profile_json, _ = _collect_result(logger, prof_f, "profile_columns", strict=fail_on_qa_error)
    parquet_snapshot, _ = _collect_result(logger, parquet_f, "snapshot_parquet", strict=fail_on_qa_error)
    drift_payload, _ = _collect_result(logger, drift_f, "rowcount_anomaly_check", strict=fail_on_qa_error) if drift_f else (None, False)

    qa_alert = None
    if isinstance(drift_payload, dict):
        qa_alert = drift_payload.get("alert")

    # Build simple string labels for tasks
    task_states: Dict[str, str] = {
        "process_object_data": _label_from_future(processed_f),
        "load_csv_to_json": _label_from_future(json_f),
        "precheck_schema": _label_from_future(schema_f),
        "precheck_nonempty": _label_from_future(nonempty_f),
        "deduplicate_by_id": _label_from_future(dedup_f),
        "profile_columns": _label_from_future(prof_f),
        "snapshot_parquet": _label_from_future(parquet_f),
    }
    if drift_f is not None:
        task_states["rowcount_anomaly_check"] = _label_from_future(drift_f)

    # Flow run identity for metadata (shown in Prefect UI)
    flow_run_id = ""
    flow_run_name = ""
    try:
        ctx = get_run_context()
        if ctx and getattr(ctx, "flow_run", None):
            flow_run_id = str(getattr(ctx.flow_run, "id", "") or "")
            flow_run_name = getattr(ctx.flow_run, "name", "") or ""
    except Exception:
        pass

    # ── Return payload ─────────────────────────────────────────────────────────
    outputs: Dict[str, Any] = {
        "object": object_name,
        "raw_csv": raw_path,
        "phase1": {  # kept for backward compatibility
            "processed_csv": processed_csv,
            "json": json_path,
        },
        "qa": {
            "dedup_csv": dedup_csv,
            "profile_json": profile_json,
            "parquet_snapshot": parquet_snapshot,
            "rowcount_anomaly": drift_payload,
        },
    }
    logger.info(f"sf-etl-orchestrator outputs: {outputs}")

    # ── Persist run metadata (JSONL + Prefect UI link) ─────────────────────────
    run_t1 = time.time()
    try:
        meta_future = record_run_metadata.submit(
            object_name=object_name,
            flow_name="sf-etl-orchestrator",
            params={
                "object_name": object_name,
                "limit": limit,
                "timestamp_raw": timestamp_raw,
                "fail_on_qa_error": fail_on_qa_error,
            },
            artifacts={
                "raw_csv": raw_path,
                "processed_csv": processed_csv,
                "json": json_path,
                "qa_profile_json": profile_json,
                "qa_dedup_csv": dedup_csv,
                "qa_parquet": parquet_snapshot,
            },
            task_states=task_states,           # string labels
            run_started_at=run_t0,
            run_finished_at=run_t1,
            qa_alert=qa_alert,
            strict_mode=fail_on_qa_error,
            run_id=flow_run_id,                # pass IDs explicitly
            run_name=flow_run_name,
        )
        _ = meta_future.result()  # wait so it isn't cancelled at flow end
    except Exception as meta_err:
        logger.warning(f"record_run_metadata failed: {meta_err!r}")

    return outputs
