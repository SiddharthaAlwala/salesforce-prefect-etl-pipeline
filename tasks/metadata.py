# tasks/metadata.py
from __future__ import annotations

import json, os, platform
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import polars as pl
from prefect import task, get_run_logger
from prefect.context import get_run_context
from prefect import __version__ as PREFECT_VERSION

from utils.paths import build_meta_paths, normalize_meta_store  # normalization lives in utils/paths


# --------- configurable knobs (env-driven for prod) ---------
WRITE_GLOBAL_PRETTY = os.getenv("METADATA_WRITE_GLOBAL_PRETTY", "0") == "1"
GLOBAL_PRETTY_KEEP_LAST = int(os.getenv("METADATA_GLOBAL_PRETTY_KEEP_LAST", "500"))
ROTATE_MAX_MB = int(os.getenv("METADATA_ROTATE_MAX_MB", "50"))
ROTATE_BACKUPS = int(os.getenv("METADATA_ROTATE_BACKUPS", "5"))


# ------------------------- helpers (plain funcs) -------------------------

def _safe_count_csv(path: Optional[str]) -> Optional[int]:
    if not path or not Path(path).exists():
        return None
    try:
        return int(pl.read_csv(path).height)
    except Exception:
        return None


def _safe_count_json(path: Optional[str]) -> Optional[int]:
    if not path or not Path(path).exists():
        return None
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        return len(data) if isinstance(data, list) else 1
    except Exception:
        return None


def _derive_ui_url(api_url: str | None) -> tuple[str | None, str | None]:
    """
    From PREFECT_API_URL (e.g. http://127.0.0.1:4200/api) derive UI base.
    Returns (api_url_norm, ui_base)
    """
    if not api_url:
        return None, None
    api = api_url.rstrip("/")
    ui = api[:-4] if api.lower().endswith("/api") else api
    return api, ui


def _resolve_flow_ids(run_id_override: Optional[str], run_name_override: Optional[str]) -> tuple[str, str]:
    """Prefer explicit overrides from the flow; otherwise use context/env."""
    if run_id_override or run_name_override:
        return run_id_override or "", run_name_override or ""
    try:
        ctx = get_run_context()
    except Exception:
        ctx = None
    rid = rname = ""
    if ctx and getattr(ctx, "flow_run", None):
        rid = str(getattr(ctx.flow_run, "id", "") or "")
        rname = getattr(ctx.flow_run, "name", "") or ""
    if not rid:
        rid = os.getenv("PREFECT__FLOW_RUN_ID") or os.getenv("PREFECT_FLOW_RUN_ID", "") or ""
    if not rname:
        rname = os.getenv("PREFECT__FLOW_RUN_NAME") or os.getenv("PREFECT_FLOW_RUN_NAME", "") or ""
    return rid, rname


def _rotate_if_big(p: Path) -> None:
    """Simple size-based rotation for jsonl files."""
    if ROTATE_MAX_MB <= 0:
        return
    max_bytes = ROTATE_MAX_MB * 1024 * 1024
    if not p.exists() or p.stat().st_size < max_bytes:
        return
    for i in range(ROTATE_BACKUPS - 1, 0, -1):
        src = p.with_suffix(p.suffix + f".{i}")
        dst = p.with_suffix(p.suffix + f".{i+1}")
        if src.exists():
            dst.unlink(missing_ok=True)
            src.replace(dst)
    first = p.with_suffix(p.suffix + ".1")
    first.unlink(missing_ok=True)
    p.replace(first)


def _append_jsonl_line(path_str: str, payload: Dict[str, Any]) -> None:
    p = Path(path_str)
    p.parent.mkdir(parents=True, exist_ok=True)
    _rotate_if_big(p)
    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _dedupe_jsonl_inplace(path_str: str) -> None:
    """De-duplicate the JSONL by run_id (keep last) while preserving order for empty IDs."""
    p = Path(path_str)
    if not p.exists():
        return
    from collections import OrderedDict
    lines = [ln for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]
    if not lines:
        return
    by_id: "OrderedDict[tuple[str, int], str]" = OrderedDict()
    for idx, ln in enumerate(lines):
        try:
            rid = (json.loads(ln).get("run_id") or "").strip()
        except Exception:
            rid = ""
        key = (rid, idx if not rid else 0)
        by_id[key] = ln
    p.write_text("\n".join(by_id.values()) + "\n", encoding="utf-8")


def _upsert_global_pretty_array(global_json_path: str, payload: Dict[str, Any], keep_last: int) -> None:
    p = Path(global_json_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    arr: list = []
    if p.exists():
        try:
            existing = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(existing, list):
                arr = existing
        except Exception:
            arr = []
    arr.append(payload)
    if keep_last > 0:
        arr = arr[-keep_last:]
    p.write_text(json.dumps(arr, indent=2, ensure_ascii=False), encoding="utf-8")


def _write_pretty_single(path_str: str, payload: Dict[str, Any]) -> None:
    p = Path(path_str)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


# ------------------------- tasks (Prefect) -------------------------

@task(name="normalize_meta_store", retries=0)
def normalize_meta_store_task(object_name: str, base_dir: str = "data") -> Dict[str, str]:
    """
    Idempotent housekeeping: fix legacy paths (runs/jsonl, runs.josnl),
    merge into canonical files, and de-duplicate JSONL.
    Returns canonical paths dict (same shape as build_meta_paths).
    """
    # Delegate to the shared normalizer in utils/paths so both places stay consistent.
    paths = normalize_meta_store(object_name, base_dir=base_dir)
    return paths


@task(name="record_run_metadata", retries=0)
def record_run_metadata(
    object_name: str,
    flow_name: str,
    params: Dict[str, Any],
    artifacts: Dict[str, Optional[str]],
    task_states: Dict[str, str],   # "COMPLETED"/"FAILED"/"UNKNOWN"
    run_started_at: float,
    run_finished_at: float,
    qa_alert: Optional[bool] = None,
    strict_mode: bool = False,
    run_id: Optional[str] = None,
    run_name: Optional[str] = None,
) -> str:
    """
    Writes (clean & de-duped):
      - data/meta/<object>/runs.jsonl         (authoritative per-object JSONL)
      - data/meta/runs.jsonl                  (authoritative global JSONL)
      - data/meta/<object>/run.latest.json    (pretty, single latest record)
      - [optional] data/meta/runs.pretty.json (pretty array, global only; toggle env)

    Safe to call directly. If you also want a separate housekeeping node in the DAG,
    call `normalize_meta_store_task.submit(object_name)` earlier in the flow.
    """
    logger = get_run_logger()

    # Ensure canonical, de-duped store (idempotent)
    meta_paths = normalize_meta_store(object_name)

    # Resolve runtime identifiers / UI
    rid, rname = _resolve_flow_ids(run_id, run_name)
    api_url, ui_base = _derive_ui_url(os.getenv("PREFECT_API_URL"))

    now_utc = datetime.now(timezone.utc)
    duration_s = round(run_finished_at - run_started_at, 3)

    raw_rows       = _safe_count_csv(artifacts.get("raw_csv"))
    processed_rows = _safe_count_csv(artifacts.get("processed_csv"))
    json_rows      = _safe_count_json(artifacts.get("json"))

    payload: Dict[str, Any] = {
        "timestamp_utc": now_utc.isoformat(),
        "run_id": rid,
        "run_name": rname,
        "run_url": f"{ui_base}/runs/flow-run/{rid}" if rid and ui_base else None,
        "flow_name": flow_name,
        "object_name": object_name,
        "params": params,
        "artifacts": artifacts,
        "counts": {
            "raw_rows": raw_rows,
            "processed_rows": processed_rows,
            "json_rows": json_rows,
        },
        "task_states": task_states,
        "qa_alert": qa_alert,
        "strict_mode": strict_mode,
        "timing": {
            "started_at_utc": datetime.utcfromtimestamp(run_started_at).strftime("%Y-%m-%d %H:%M:%S"),
            "finished_at_utc": datetime.utcfromtimestamp(run_finished_at).strftime("%Y-%m-%d %H:%M:%S"),
            "duration_seconds": duration_s,
        },
        "env": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "prefect_version": PREFECT_VERSION,
            "api_url": api_url,
        },
        "errors": None,
    }

    # Append to authoritative JSONL files
    _append_jsonl_line(meta_paths["runs_jsonl"], payload)
    _append_jsonl_line(meta_paths["global_runs_jsonl"], payload)

    # De-dupe after append (keeps files tidy over time)
    _dedupe_jsonl_inplace(meta_paths["runs_jsonl"])
    _dedupe_jsonl_inplace(meta_paths["global_runs_jsonl"])

    # Write single latest pretty object
    latest_path = str(Path(meta_paths["runs_jsonl"]).with_name("run.latest.json"))
    _write_pretty_single(latest_path, payload)

    # Optional global pretty (for humans)
    if WRITE_GLOBAL_PRETTY:
        global_pretty = str(Path(meta_paths["global_runs_jsonl"]).with_name("runs.pretty.json"))
        _upsert_global_pretty_array(global_pretty, payload, keep_last=GLOBAL_PRETTY_KEEP_LAST)

    logger.info(f"Recorded run metadata → {meta_paths['runs_jsonl']}")
    return meta_paths["runs_jsonl"]
