from flows.sf_csv_polars_json_flow import sf_csv_polars_json_pipeline
if __name__ == "__main__":
    for i in range(3):
        print(f"\n=== Run #{i+1} ===")
        sf_csv_polars_json_pipeline()