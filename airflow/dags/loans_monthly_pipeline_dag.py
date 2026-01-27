from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import timezone

US_TZ = timezone("America/New_York")

# === Lazy imports inside callables ===
def run_enrich_loans():
    from pipelines.monthly.enrich_loans_pipeline import run_enrich_loans_pipeline
    return run_enrich_loans_pipeline() or "OK"

def run_loans_model():
    from pipelines.monthly.loans_model_pipeline import run_loans_model_pipeline
    return run_loans_model_pipeline() or "OK"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=15),  # Retry delay after failure
    "execution_timeout": timedelta(hours=2),  # Timeout the task after 2 hours
}

with DAG(
    dag_id="monthly_loans_risk_pipeline",
    default_args=default_args,
    schedule="0 0 1 * *",          # 1st day of every month at 00:00
    start_date=datetime(2026, 1, 1, tzinfo=US_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["loans", "risk", "monthly", "portfolio"],
) as dag:

    enrich_loans = PythonOperator(
        task_id="enrich_loans_dataset",
        python_callable=run_enrich_loans,
        execution_timeout=timedelta(hours=2),  # Set task-specific timeout if needed
    )

    loans_model = PythonOperator(
        task_id="run_loans_model_pipeline",
        python_callable=run_loans_model,
        execution_timeout=timedelta(hours=2),
    )

    enrich_loans >> loans_model
