# serve_15min.py
from flows.sf_csv_polars_json_flow import sf_csv_polars_json_pipeline

if __name__ == "__main__":
    sf_csv_polars_json_pipeline.serve(
        name="sf-etl-15min",
        cron="*/15 * * * *",              # every 15 minutes    
    )
