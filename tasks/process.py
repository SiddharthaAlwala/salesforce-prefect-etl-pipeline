import polars as pl
from polars.exceptions import NoDataError
from typing import Optional, List, Dict, Any
from pathlib import Path
from prefect import get_run_logger, task
from configs.salesforce_objects import OBJECT_SPECS, ObjectSpec
from utils.paths import build_paths



def _ensure_cols(df: pl.DataFrame, cols: List[str], fill: Any = "UNKNOWN") -> pl.DataFrame:
    out = df
    for c in cols:
        if c not in out.columns:
            out = out.with_columns(pl.lit(fill).alias(c))
    return out

def _cast_float(df: pl.DataFrame, cols: List[str]) -> pl.DataFrame:
    out = df
    for c in cols:
        if c in out.columns:
            out = out.with_columns(pl.col(c).cast(pl.Float64, strict=False))
        else:
            out = out.with_columns(pl.lit(0.0).cast(pl.Float64).alias(c))
    return out

def _build_aggs(metrics: Dict[str, List[str]]) -> List[pl.Expr]:
    aggs: List[pl.Expr] = [pl.count().alias("records")]  # always have a counter
    for col, ops in metrics.items():
        if col == "__DURATION_HOURS__":
            for op in ops:
                op_l = op.lower()
                if op_l == "sum":
                    aggs.append(pl.col("duration_hours").sum().alias("sum_duration_hours"))
                elif op_l == "mean":
                    aggs.append(pl.col("duration_hours").mean().alias("avg_duration_hours"))
                elif op_l == "min":
                    aggs.append(pl.col("duration_hours").min().alias("min_duration_hours"))
                elif op_l == "max":
                    aggs.append(pl.col("duration_hours").max().alias("max_duration_hours"))
            continue
        base = pl.col(col).cast(pl.Float64, strict=False)
        for op in ops:
            op_l = op.lower()
            if op_l == "sum":
                aggs.append(base.sum().alias(f"sum_{col.lower()}"))
            elif op_l == "mean":
                aggs.append(base.mean().alias(f"avg_{col.lower()}"))
            elif op_l == "min":
                aggs.append(base.min().alias(f"min_{col.lower()}"))
            elif op_l == "max":
                aggs.append(base.max().alias(f"max_{col.lower()}"))
            # "count" covered by records
    return aggs

@task(name="process_object_data", retries=2, retry_delay_seconds=5)
def process_object_data(object_name:str, in_csv: Optional[str] = None, out_csv: Optional[str] = None)-> str:
    """
    Reads CSV with Polars, groups/aggregates per OBJECT_SPECS, writes CSV.
    """
    logger = get_run_logger()
    if object_name not in OBJECT_SPECS:
        raise ValueError(f"Unsupported Salesforce object: {object_name}")
    spec: ObjectSpec = OBJECT_SPECS[object_name]

    paths = build_paths(object_name)
    in_csv = in_csv or paths["raw_csv"]
    out_csv = out_csv or paths["processed_csv"]
    Path(out_csv).parent.mkdir(parents = True, exist_ok = True)

    try:
        df = pl.read_csv(in_csv)
    except NoDataError:
        df = pl.DataFrame([])
    
    if df.is_empty():
        cols = {gb: pl.Utf8 for gb in spec.group_by}
        for col, ops in spec.metrics.items():
            for op in ops:
                prefix = {"sum": "sum_", "mean": "avg_", "min": "min_", "max": "max_", "count": ""}[op.lower()]
                name = f"{prefix}duration_hours" if col == "__DURATION_HOURS__" else f"{prefix}{col.lower()}"
                if name: 
                    cols[name] = pl.Float64
        cols["records"] = pl.Int64
        pl.DataFrame(schema=cols).write_csv(out_csv)
        logger.warning("process: input CSV empty; wrote empty aggregated CSV.")
        return out_csv
    
    if object_name == "Event":
        df = df.with_columns(
            pl.col("StartDateTime").str.to_datetime(strict=False).alias("StartDateTime_dt"),
            pl.col("EndDateTime").str.to_datetime(strict=False).alias("EndDateTime_dt"),
        ).with_columns(
            ((pl.col("EndDateTime_dt") - pl.col("StartDateTime_dt")).dt.total_seconds() / 3600.0)
            .fill_null(0.0)
            .alias("duration_hours")
        )

    df = _ensure_cols(df, spec.group_by, fill="UNKNOWN")
    numeric_cols = [c for c in spec.metrics.keys() if c != "__DURATION_HOURS__"]
    df = _cast_float(df, numeric_cols)

    aggs = _build_aggs(spec.metrics)
    out = df.group_by(spec.group_by).agg(aggs) if spec.group_by else df.select(aggs)

    # Keep original group_by column names (no special-case rename)
    if spec.group_by:
        out = out.sort(spec.group_by[0])

    out.write_csv(out_csv)
    logger.info(f"Wrote processed CSV: {out_csv} ({len(out):,} rows)")
    return out_csv
