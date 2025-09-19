# serve_15min.py
from flows.sf_etl_orchestrator_flow import sf_etl_orchestrator

if __name__ == "__main__":
    sf_etl_orchestrator.serve(
        name="sf-etl-orchestrator-15min",
        cron="*/15 * * * *",  # every 15 minutes
        parameters={
            "object_name": "Account",
            "timestamp_raw": True,
            "fail_on_qa_error": False,
        },
        tags=["orchestrator", "scheduled"],
    )
