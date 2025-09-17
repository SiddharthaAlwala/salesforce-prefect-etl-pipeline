import polars as pl
import json
from pathlib import Path
from prefect import task, get_run_logger


@task(name = "load_csv_to_json")
def load_csv_to_json(
        in_csv:str = "data/processed/account_summary.csv",
        out_json:str = "data/outputaccount_summary.json",
) -> str:
    """
    Convert processed CSV to JSON (records list) and write it out.
    """
    logger = get_run_logger()
    Path(out_json).parent.mkdir(parents=True, exist_ok = True)

    if not Path(in_csv). exists():
        raise FileNotFoundError(f"Processed CSV not found: {in_csv}")
    
    df = pl.read_csv(in_csv)
    rows = df.to_dicts()
    with open(out_json, "w", encoding = "utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii = False)

    logger.info(f"wrote JSON:{out_json} (records: {len(rows):,})")
    return out_json