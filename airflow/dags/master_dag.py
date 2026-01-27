from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import timezone

US_TZ = timezone("America/New_York")


# == Lazy imports inside callables ==
def run_feature_update():
    from pipelines.daily.market_features_s3 import update_market_features
    df, msg = update_market_features()
    return msg or "OK"


def run_risk_pipeline():
    from pipelines.daily.equity_risk_prediction_pipeline import run_equity_risk_pipeline
    return run_equity_risk_pipeline() or "OK"


def run_fx_exposure():
    from pipelines.daily.fx_exposure_pipeline import run_fx_exposure_pipeline
    return run_fx_exposure_pipeline() or "OK"


def run_fx_update():
    from pipelines.daily.fx_update_pipeline import update_fx_snowflake
    return update_fx_snowflake() or "OK"


def run_commodity_update():
    from pipelines.daily.commodity_update_pipeline import run_commodities_update
    return run_commodities_update() or "OK"


def run_commodity_process():
    from pipelines.daily.commodity_processing_pipeline import process_commodities
    return process_commodities() or "OK"


def run_bonds_update():
    from pipelines.daily.bonds_update_pipeline import update_bonds_snowflake
    return update_bonds_snowflake() or "OK"


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
