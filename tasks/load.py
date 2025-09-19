# tasks/load.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Optional, Any
import polars as pl
from prefect import task, get_run_logger

def _to_jsonable(v: Any) -> Any:
    """Convert Polars/Python values to JSON-safe types."""
    # Avoid importing numpy unless present
    try:
        import numpy as np
        _HAS_NP = True
    except Exception:
        _HAS_NP = False

    # Polars Series -> list
    if isinstance(v, pl.Series):
        return v.to_list()
    # pathlib -> str
    if isinstance(v, Path):
        return str(v)
    # numpy scalars -> python scalars
    if _HAS_NP and isinstance(v, np.generic):
        return v.item()
    # lists/tuples -> recurse
    if isinstance(v, (list, tuple)):
        return [_to_jsonable(x) for x in v]
    # datetimes / dates / times may show up as python objects when not cast
    # We won't import datetime here; stringifying unknown objects is safest fallback
    # for anything still not natively serializable.
    try:
        json.dumps(v)
        return v
    except TypeError:
        return str(v)

@task(name="load_csv_to_json")
def load_csv_to_json(
    object_name: str,
    in_csv: Optional[str] = None,
    out_json: Optional[str] = None,
) -> str:
    """
    Convert processed CSV to JSON (records) and write it out, robustly handling Polars types.
    """
    from utils.paths import build_paths

    logger = get_run_logger()
    paths    = build_paths(object_name)
    in_csv   = in_csv   or paths["processed_csv"]
    out_json = out_json or paths["out_json"]

    path_in  = Path(in_csv)
    path_out = Path(out_json)
    path_out.parent.mkdir(parents=True, exist_ok=True)

    if not path_in.exists():
        raise FileNotFoundError(f"Processed CSV not found: {in_csv}")

    df = pl.read_csv(in_csv)

    # 1) Cast datetime-like columns to strings for JSON
    if df.width:
        dateish = []
        for name, dtype in zip(df.columns, df.dtypes):
            if dtype in (pl.Date, pl.Datetime, pl.Time):
                dateish.append(name)
        if dateish:
            df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in dateish])

    # 2) Convert to JSON-safe Python values
    # Prefer Polars' own JSON writer if available (newer versions); fall back to manual.
    try:
        # Polars >= 0.20 has write_json (records by default). If not available, this raises AttributeError.
        df.write_json(out_json)
        logger.info(f"Wrote JSON via Polars: {out_json} (records: {len(df):,})")
        return out_json
    except Exception:
        rows = df.to_dicts()  # list[dict]
        safe_rows = [{k: _to_jsonable(v) for k, v in row.items()} for row in rows]
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump(safe_rows, f, indent=2, ensure_ascii=False)
        logger.info(f"Wrote JSON: {out_json} (records: {len(safe_rows):,})")
        return out_json
