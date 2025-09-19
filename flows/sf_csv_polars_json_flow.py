from prefect import get_run_logger, flow
from tasks.extract import resolve_extract_plan, extract_salesforce_to_csv
from tasks.process import process_object_data
from tasks.load import load_csv_to_json
from typing import Optional
from configs.salesforce_objects import OBJECT_SPECS
from utils.paths import build_paths



@flow(name = "sf_csv_polars_json_pipeline")
def sf_csv_polars_json_pipeline(
        object_name : str,
        soql: Optional[str] = None
)->str:
    """
    Phase 1: Extract (SF→CSV) → Process (Polars) → Load (CSV→JSON)
    Works for ANY object defined in configs/salesforce_objects.py
    """
    

    logger = get_run_logger()
    if object_name not in OBJECT_SPECS:
        raise ValueError(f"Unsupported Salesforce object: {object_name}")
    paths = build_paths(object_name)
    raw_csv = paths["raw_csv"]
    processed_csv = paths["processed_csv"]
    out_json = paths["out_json"]

    # 1) Extract
    sql, raw_csv = resolve_extract_plan(object_name=object_name, soql=soql, out_csv=raw_csv, limit=100)
    raw_path = extract_salesforce_to_csv(soql=sql, out_csv=raw_csv)

    # 2) Process
    processed_path = process_object_data(object_name=object_name, in_csv=raw_path, out_csv=processed_csv)

    # 3) Load
    json_path = load_csv_to_json(object_name=object_name, in_csv=processed_path, out_json=out_json)
    logger.info("pipeline completed")
    return json_path




