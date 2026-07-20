from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from pendulum import timezone

from master_dag_kpo_tasks import make_daily_kpo

US_TZ = timezone("America/New_York")

# ============================================================
# SPAN THRESHOLD
# ============================================================
HIGH_COMPUTE_SPAN_DAYS = 20  # start_date older than this -> route to KPO


# ============================================================
# SHARED DAG CONFIG PARSER
# ============================================================


def get_dag_config(context, replay_key="replay_from_raw"):
    dag_run = context.get("dag_run")

    config = {
        "start_date_override": None,
        replay_key: False,
    }

    if dag_run and dag_run.conf:
        config["start_date_override"] = dag_run.conf.get("start_date")

        config[replay_key] = bool(
            dag_run.conf.get(replay_key, False)
        )

    return config


def get_airflow_metadata(context):
    return {
        "dag_id": context["dag"].dag_id,
        "task_id": context["task"].task_id,
        "dag_run_id": context["dag_run"].run_id,
        "dag_run_type": context["dag_run"].run_type,
        "try_number": context["ti"].try_number,
        "max_tries": context["ti"].max_tries,
        "logical_date": str(context["logical_date"]),
        "execution_date": str(context["logical_date"]),
        "triggered_by": "manual" if context["dag_run"].run_type == "manual" else "scheduled",
    }


# ============================================================
# BRANCH DECISION: SHORT (PYTHON) vs LONG (KPO)
# ============================================================

def decide_execution_path(**context):
    """
    Reads dag_run.conf once. If no start_date is given (live/incremental run),
    or the span from start_date to today is <= HIGH_COMPUTE_SPAN_DAYS,
    route to the PythonOperator chain. Otherwise route to the KPO chain.
    """
    dag_run = context.get("dag_run")
    start_date_str = None

    if dag_run and dag_run.conf:
        start_date_str = dag_run.conf.get("start_date")

    if not start_date_str:
        print("[BRANCH] No start_date in conf - live/incremental run - using PYTHON path")
        return "equity_feature_pipeline"

    start_date = pd.to_datetime(start_date_str)
    span_days = (pd.Timestamp.today().normalize() - start_date.normalize()).days

    print(f"[BRANCH] start_date={start_date_str} span_days={span_days}")

    if span_days > HIGH_COMPUTE_SPAN_DAYS:
        print(f"[BRANCH] span={span_days}d > {HIGH_COMPUTE_SPAN_DAYS}d - using KPO path")
        return "equity_feature_pipeline_kpo"

    print(f"[BRANCH] span={span_days}d <= {HIGH_COMPUTE_SPAN_DAYS}d - using PYTHON path")
    return "equity_feature_pipeline"


# ============================================================
# EQUITY PIPELINES
# ============================================================


def run_equity_feature_pipeline(**context):
    from pipelines.daily.market_features_s3 import update_market_features

    config = get_dag_config(context, replay_key="replay_from_raw")
    airflow_metadata = get_airflow_metadata(context)

    return update_market_features(
        start_date_override=config["start_date_override"],
        replay_from_raw=config["replay_from_raw"],
        airflow_metadata=airflow_metadata,
    ) or "OK"


def run_equity_processing_pipeline(**context):
    from pipelines.daily.equity_risk_prediction_pipeline import run_equity_risk_pipeline

    config = get_dag_config(context, replay_key="replay")
    airflow_metadata = get_airflow_metadata(context)

    return run_equity_risk_pipeline(
        start_date_override=config["start_date_override"],
        replay=config["replay"],
        airflow_metadata=airflow_metadata,
    ) or "OK"


# ============================================================
# FX PIPELINES
# ============================================================


def run_fx_feature_pipeline(**context):
    from pipelines.daily.fx_exposure_pipeline import update_fx_pipeline

    config = get_dag_config(context, replay_key="replay_from_raw")
    airflow_metadata = get_airflow_metadata(context)

    return update_fx_pipeline(
        start_date_override=config["start_date_override"],
        replay_from_raw=config["replay_from_raw"],
        airflow_metadata=airflow_metadata,
    ) or "OK"


def run_fx_processing_pipeline(**context):
    from pipelines.daily.fx_update_pipeline import update_fx_snowflake

    config = get_dag_config(context, replay_key="replay")
    airflow_metadata = get_airflow_metadata(context)

    return update_fx_snowflake(
        start_date_override=config["start_date_override"],
        replay=config["replay"],
        airflow_metadata=airflow_metadata,
    ) or "OK"


# ============================================================
# COMMODITY PIPELINES
# ============================================================


def run_commodity_feature_pipeline(**context):
    from pipelines.daily.commodity_update_pipeline import update_commodity_pipeline

    config = get_dag_config(context, replay_key="replay_from_raw")
    airflow_metadata = get_airflow_metadata(context)

    return update_commodity_pipeline(
        start_date_override=config["start_date_override"],
        replay_from_raw=config["replay_from_raw"],
        airflow_metadata=airflow_metadata,
    ) or "OK"


def run_commodity_processing_pipeline(**context):
    from pipelines.daily.commodity_processing_pipeline import process_commodities

    config = get_dag_config(context, replay_key="replay")
    airflow_metadata = get_airflow_metadata(context)

    return process_commodities(
        start_date_override=config["start_date_override"],
        replay=config["replay"],
        airflow_metadata=airflow_metadata,
    ) or "OK"


# ============================================================
# BONDS PIPELINES
# ============================================================


def run_bonds_feature_pipeline(**context):
    from pipelines.daily.bonds_update_pipeline import update_bonds_pipeline

    config = get_dag_config(context, replay_key="replay_from_raw")
    airflow_metadata = get_airflow_metadata(context)

    return update_bonds_pipeline(
        start_date_override=config["start_date_override"],
        replay_from_raw=config["replay_from_raw"],
        airflow_metadata=airflow_metadata,
    ) or "OK"


def run_bonds_processing_pipeline(**context):
    from pipelines.daily.bonds_processing_pipeline import process_bonds

    config = get_dag_config(context, replay_key="replay")
    airflow_metadata = get_airflow_metadata(context)

    return process_bonds(
        start_date_override=config["start_date_override"],
        replay=config["replay"],
        airflow_metadata=airflow_metadata,
    ) or "OK"


# ============================================================
# DERIVATIVES
# ============================================================

def run_derivatives_pipeline(**context):
    from pipelines.daily.derivatives_pipeline import run_derivatives_processing

    config = get_dag_config(context, replay_key="replay")
    airflow_metadata = get_airflow_metadata(context)

    return run_derivatives_processing(
        start_date_override=config["start_date_override"],
        replay=config["replay"],
        airflow_metadata=airflow_metadata,
    ) or "OK"


# ============================================================
# COLLATERAL
# ============================================================

def run_collateral_pipeline(**context):
    from pipelines.daily.collateral_pipeline import run_collateral_pipeline as _run

    config = get_dag_config(context, replay_key="replay")
    airflow_metadata = get_airflow_metadata(context)

    return _run(
        start_date_override=config["start_date_override"],
        replay=config["replay"],
        airflow_metadata=airflow_metadata,
    ) or "OK"


# ============================================================
# AIRFLOW DEFAULTS
# ============================================================

default_args = {
    "owner": "airflow",
    "retries": 1,
}

# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="full_cross_asset_risk_pipeline",
    default_args=default_args,
    schedule="30 16 * * MON-FRI",
    start_date=datetime(2025, 1, 1, tzinfo=US_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["risk", "portfolio", "cross-asset", "daily", "production"],
) as dag:

    # ========================================================
    # BRANCH
    # ========================================================

    decide_path = BranchPythonOperator(
        task_id="decide_execution_path",
        python_callable=decide_execution_path,
    )

    # ========================================================
    # PYTHON PATH (short span / live) - unchanged from before
    # ========================================================

    equity_features = PythonOperator(
        task_id="equity_feature_pipeline",
        python_callable=run_equity_feature_pipeline,
    )
    equity_processing = PythonOperator(
        task_id="equity_processing_pipeline",
        python_callable=run_equity_processing_pipeline,
    )
    fx_features = PythonOperator(
        task_id="fx_feature_pipeline",
        python_callable=run_fx_feature_pipeline,
    )
    fx_processing = PythonOperator(
        task_id="fx_processing_pipeline",
        python_callable=run_fx_processing_pipeline,
    )
    commodity_features = PythonOperator(
        task_id="commodity_feature_pipeline",
        python_callable=run_commodity_feature_pipeline,
    )
    commodity_processing = PythonOperator(
        task_id="commodity_processing_pipeline",
        python_callable=run_commodity_processing_pipeline,
    )
    bonds_features = PythonOperator(
        task_id="bonds_feature_pipeline",
        python_callable=run_bonds_feature_pipeline,
    )
    bonds_processing = PythonOperator(
        task_id="bonds_processing_pipeline",
        python_callable=run_bonds_processing_pipeline,
    )
    derivatives_processing = PythonOperator(
        task_id="derivatives_processing_pipeline",
        python_callable=run_derivatives_pipeline,
    )
    collateral_processing = PythonOperator(
        task_id="collateral_processing_pipeline",
        python_callable=run_collateral_pipeline,
    )

    (
        equity_features
        >> equity_processing
        >> fx_features
        >> fx_processing
        >> commodity_features
        >> commodity_processing
        >> bonds_features
        >> bonds_processing
        >> derivatives_processing
        >> collateral_processing
    )

    # ========================================================
    # KPO PATH (long backfill/replay > 20 days) - same pipelines,
    # executed as dedicated pods on airflow-high-compute
    # ========================================================

    equity_features_kpo = make_daily_kpo("equity_feature_pipeline_kpo", "equity_features")
    equity_processing_kpo = make_daily_kpo("equity_processing_pipeline_kpo", "equity_processing")
    fx_features_kpo = make_daily_kpo("fx_feature_pipeline_kpo", "fx_features")
    fx_processing_kpo = make_daily_kpo("fx_processing_pipeline_kpo", "fx_processing")
    commodity_features_kpo = make_daily_kpo("commodity_feature_pipeline_kpo", "commodity_features")
    commodity_processing_kpo = make_daily_kpo("commodity_processing_pipeline_kpo", "commodity_processing")
    bonds_features_kpo = make_daily_kpo("bonds_feature_pipeline_kpo", "bonds_features")
    bonds_processing_kpo = make_daily_kpo("bonds_processing_pipeline_kpo", "bonds_processing")
    derivatives_processing_kpo = make_daily_kpo("derivatives_processing_pipeline_kpo", "derivatives_processing")
    collateral_processing_kpo = make_daily_kpo("collateral_processing_pipeline_kpo", "collateral_processing")

    (
        equity_features_kpo
        >> equity_processing_kpo
        >> fx_features_kpo
        >> fx_processing_kpo
        >> commodity_features_kpo
        >> commodity_processing_kpo
        >> bonds_features_kpo
        >> bonds_processing_kpo
        >> derivatives_processing_kpo
        >> collateral_processing_kpo
    )

    # ========================================================
    # WIRING: branch picks exactly one of the two starting tasks
    # ========================================================

    decide_path >> [equity_features, equity_features_kpo]