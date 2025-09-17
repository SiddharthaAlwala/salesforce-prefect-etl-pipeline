from prefect import get_run_logger, flow
from tasks.extract import extract_salesforce_to_csv
from tasks.process import process_accounts
from tasks.load import load_csv_to_json



@flow(name = "sf_csv_polars_json_pipeline")
def sf_csv_polars_json_pipeline(
        soql: str =(
            """SELECT Id, Name, Phone, Website, BillingCity, BillingState, Industry, AnnualRevenue
        FROM Account
        WHERE IsDeleted = false
        LIMIT 100
        """
        ),
        raw_csv: str = "data/raw/sf_accounts.csv",
        processed_csv:str = "data/processed/account_summary.csv",
        out_json:str = "data/output/account_summary.json", 
)->str:
    """
    Orchestrates: Extracts (SF->CSV)-> process (Polars->CSV) -> Load (CSV->JSON)

    """

    logger = get_run_logger()
    raw_path = extract_salesforce_to_csv(soql, out_csv=raw_csv)
    processed_path = process_accounts(in_csv = raw_path, out_csv = processed_csv)
    json_path = load_csv_to_json(in_csv = processed_path, out_json = out_json)
    logger.info("pipeline completed")
    return json_path


# if __name__ == "__main__":
#     sf_csv_polars_json_pipeline()

