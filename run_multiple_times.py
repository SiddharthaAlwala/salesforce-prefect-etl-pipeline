from flows.sf_etl_orchestrator_flow import sf_etl_orchestrator
if __name__ == "__main__":
    for i in range(3):
        print(f"\n=== Run #{i+1} ===")
        sf_etl_orchestrator(object_name="Account")