from pathlib import Path
from prefect import get_run_logger, task
import polars as pl
from polars.exceptions import NoDataError

@task(name="process_accounts", retries=2, retry_delay_seconds=5)
def process_accounts(in_csv="data/raw/sf_accounts.csv",
                     out_csv="data/processed/account_summary.csv") -> str:
    logger = get_run_logger()
    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)

    try:
        df = pl.read_csv(in_csv)
    except NoDataError:
        df = pl.DataFrame([])

    if df.is_empty():
        pl.DataFrame({
            "billing_state": [],
            "accounts": pl.Series([], pl.Int64),
            "total_revenue": pl.Series([], pl.Float64),
            "avg_revenue": pl.Series([], pl.Float64),
        }).write_csv(out_csv)
        logger.warning("Input CSV empty; wrote empty processed CSV")
        return out_csv

    # Make columns tolerant
    if "BillingState" not in df.columns:
        df = df.with_columns(pl.lit("UNKNOWN").alias("BillingState"))
    if "AnnualRevenue" not in df.columns:
        df = df.with_columns(pl.lit(0.0).cast(pl.Float64).alias("AnnualRevenue"))

    out = (
        df.with_columns(pl.col("AnnualRevenue").cast(pl.Float64, strict=False))
          .group_by("BillingState")
          .agg([
              pl.count().alias("accounts"),
              pl.sum("AnnualRevenue").alias("total_revenue"),
              pl.mean("AnnualRevenue").alias("avg_revenue"),
          ])
          .rename({"BillingState": "billing_state"})
          .sort("billing_state")
    )
    out.write_csv(out_csv)
    logger.info(f"Wrote processed CSV: {out_csv} ({len(out):,} rows)")
    return out_csv
