from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import timezone

US_TZ = timezone("America/New_York")


# ============================================================
# SHARED DAG CONFIG PARSER
# ============================================================

def get_dag_config(context, replay_key="replay"):
    """
    Standardized DAG runtime config parser.

    Supported dag_run.conf:
    {
        "start_date": "2025-01-01",
        "replay": true
    }
    """

    dag_run = context.get("dag_run")

    config = {
        "start_date_override": None,
        replay_key: False,
    }

    if dag_run and dag_run.conf:

        config["start_date_override"] = (
            dag_run.conf.get("start_date")
        )

        config[replay_key] = bool(
            dag_run.conf.get(replay_key, False)
        )

    return config


# ============================================================
# LOAN ENRICHMENT PIPELINE
# ============================================================

def run_enrich_loans(**context):

    from pipelines.monthly.enrich_loans_pipeline import (
        run_enrich_loans_pipeline,
    )

    config = get_dag_config(
        context,
        replay_key="replay",
    )

    return run_enrich_loans_pipeline(
        start_date_override=config["start_date_override"],
        replay=config["replay"],
    ) or "OK"


# ============================================================
# LOAN MODEL PIPELINE
# ============================================================

def run_loans_model(**context):

    from pipelines.monthly.loans_model_pipeline import (
        run_loans_model_pipeline,
    )

    config = get_dag_config(
        context,
        replay_key="replay",
    )

    return run_loans_model_pipeline(
        start_date_override=config["start_date_override"],
        replay=config["replay"],
    ) or "OK"


# ============================================================
# AIRFLOW DEFAULTS
# ============================================================

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="monthly_loans_risk_pipeline",
    default_args=default_args,
    schedule="0 0 1 * *",
    start_date=datetime(2026, 1, 1, tzinfo=US_TZ),
    catchup=False,
    max_active_runs=1,
    tags=[
        "loans",
        "risk",
        "monthly",
        "portfolio",
        "production",
    ],
) as dag:

    # ========================================================
    # ENRICHMENT
    # ========================================================

    enrich_loans = PythonOperator(
        task_id="enrich_loans_dataset",
        python_callable=run_enrich_loans,
        execution_timeout=timedelta(hours=2),
        do_xcom_push=False,
    )

    # ========================================================
    # MODEL PIPELINE
    # ========================================================

    loans_model = PythonOperator(
        task_id="run_loans_model_pipeline",
        python_callable=run_loans_model,
        execution_timeout=timedelta(hours=2),
        do_xcom_push=False,
    )

    # ========================================================
    # FLOW
    # ========================================================

    enrich_loans >> loans_model