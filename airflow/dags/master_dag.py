from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import timezone

US_TZ = timezone("America/New_York")


# == Lazy imports inside callables ==
def run_feature_update(**context):
    from pipelines.daily.market_features_s3 import update_market_features

    dag_run = context.get("dag_run")

    start_date_override = None

    if dag_run and dag_run.conf:
        start_date_override = dag_run.conf.get("start_date")

    df, msg = update_market_features(start_date_override=start_date_override)

    return msg or "OK"


def run_risk_pipeline():
    from pipelines.daily.equity_risk_prediction_pipeline import run_equity_risk_pipeline
    return run_equity_risk_pipeline() or "OK"


def run_fx_exposure(**context):
    from pipelines.daily.fx_exposure_pipeline import update_fx_pipeline

    dag_run = context.get("dag_run")
    start_date_override = None

    if dag_run and dag_run.conf:
        start_date_override = dag_run.conf.get("start_date")

    return update_fx_pipeline(start_date_override=start_date_override) or "OK"


def run_fx_update():
    from pipelines.daily.fx_update_pipeline import update_fx_snowflake
    return update_fx_snowflake() or "OK"


def run_commodity_update(**context):
    from pipelines.daily.commodity_update_pipeline import update_commodity_pipeline

    dag_run = context.get("dag_run")
    start_date_override = None

    if dag_run and dag_run.conf:
        start_date_override = dag_run.conf.get("start_date")

    return update_commodity_pipeline(start_date_override=start_date_override) or "OK"


def run_commodity_process():
    from pipelines.daily.commodity_processing_pipeline import process_commodities
    return process_commodities() or "OK"


def run_bonds_update(**context):
    from pipelines.daily.bonds_update_pipeline import update_bonds_pipeline

    dag_run = context.get("dag_run")
    start_date_override = None

    if dag_run and dag_run.conf:
        start_date_override = dag_run.conf.get("start_date")

    return update_bonds_pipeline(start_date_override=start_date_override) or "OK"


def run_derivatives():
    from pipelines.daily.derivatives_pipeline import run_derivatives_processing
    return run_derivatives_processing() or "OK"


def run_collateral():
    from pipelines.daily.collateral_pipeline import run_collateral_pipeline
    return run_collateral_pipeline() or "OK"


default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="full_cross_asset_risk_pipeline",
    default_args=default_args,
    schedule="30 16 * * MON-FRI",
    start_date=datetime(2025, 1, 1, tzinfo=US_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["risk", "portfolio", "daily", "full-pipeline"],
) as dag:

    update_equity_features = PythonOperator(
        task_id="update_market_features",
        python_callable=run_feature_update,
    )

    run_equity_risk = PythonOperator(
        task_id="run_equity_risk_pipeline",
        python_callable=run_risk_pipeline,
    )

    fx_exposure = PythonOperator(
        task_id="run_fx_exposure_pipeline",
        python_callable=run_fx_exposure,
    )

    fx_update = PythonOperator(
        task_id="update_fx_snowflake",
        python_callable=run_fx_update,
    )

    commodity_update = PythonOperator(
        task_id="update_commodities",
        python_callable=run_commodity_update,
    )

    commodity_process = PythonOperator(
        task_id="process_commodities",
        python_callable=run_commodity_process,
    )

    bonds_update = PythonOperator(
        task_id="update_bonds_snowflake",
        python_callable=run_bonds_update,
    )

    derivatives_process = PythonOperator(
        task_id="run_derivatives_processing",
        python_callable=run_derivatives,
    )

    collateral_process = PythonOperator(
        task_id="run_collateral_pipeline",
        python_callable=run_collateral,
    )

    update_equity_features >> run_equity_risk >> fx_exposure >> fx_update >> \
    commodity_update >> commodity_process >> bonds_update >> \
    derivatives_process >> collateral_process
