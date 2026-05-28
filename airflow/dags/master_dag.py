from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import timezone

US_TZ = timezone("America/New_York")

# ============================================================
# SHARED DAG CONFIG PARSER
# ============================================================


def get_dag_config(context, replay_key="replay_from_raw"):
    """
    Standardized DAG runtime config parser.

    Supported dag_run.conf:
    {
        "start_date": "2025-01-01",
        "replay_from_raw": true,
        "replay": true
    }
    """

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
    """
    Standardized Airflow metadata extractor for pipeline observability.

    Returns:
        dict: Airflow execution context metadata including:
            - dag_id
            - task_id
            - dag_run_id
            - dag_run_type (scheduled/manual/backfill/dataset_triggered)
            - try_number
            - max_tries
            - logical_date
            - execution_date
            - triggered_by (manual/scheduled)
    """
    return {
        "dag_id": context["dag"].dag_id,
        "task_id": context["task"].task_id,
        "dag_run_id": context["dag_run"].run_id,
        "dag_run_type": context["dag_run"].run_type,
        "try_number": context["ti"].try_number,
        "max_tries": context["ti"].max_tries,
        "logical_date": str(context["logical_date"]),
        "execution_date": str(context["execution_date"]),
        "triggered_by": (
            "manual"
            if context["dag_run"].external_trigger
            else "scheduled"
        ),
    }


# ============================================================
# EQUITY PIPELINES
# ============================================================


def run_equity_feature_pipeline(**context):

    from pipelines.daily.market_features_s3 import (
        update_market_features,
    )

    config = get_dag_config(
        context,
        replay_key="replay_from_raw",
    )

    airflow_metadata = get_airflow_metadata(context)

    return update_market_features(
        start_date_override=config["start_date_override"],
        replay_from_raw=config["replay_from_raw"],
        airflow_metadata=airflow_metadata,
    ) or "OK"


def run_equity_processing_pipeline(**context):

    from pipelines.daily.equity_risk_prediction_pipeline import (
        run_equity_risk_pipeline,
    )

    config = get_dag_config(
        context,
        replay_key="replay",
    )

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

    from pipelines.daily.fx_exposure_pipeline import (
        update_fx_pipeline,
    )

    config = get_dag_config(
        context,
        replay_key="replay_from_raw",
    )

    airflow_metadata = get_airflow_metadata(context)

    return update_fx_pipeline(
        start_date_override=config["start_date_override"],
        replay_from_raw=config["replay_from_raw"],
        airflow_metadata=airflow_metadata,
    ) or "OK"


def run_fx_processing_pipeline(**context):

    from pipelines.daily.fx_update_pipeline import (
        update_fx_snowflake,
    )

    config = get_dag_config(
        context,
        replay_key="replay",
    )

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

    from pipelines.daily.commodity_update_pipeline import (
        update_commodity_pipeline,
    )

    config = get_dag_config(
        context,
        replay_key="replay_from_raw",
    )

    airflow_metadata = get_airflow_metadata(context)

    return update_commodity_pipeline(
        start_date_override=config["start_date_override"],
        replay_from_raw=config["replay_from_raw"],
        airflow_metadata=airflow_metadata,
    ) or "OK"


def run_commodity_processing_pipeline(**context):

    from pipelines.daily.commodity_processing_pipeline import (
        process_commodities,
    )

    config = get_dag_config(
        context,
        replay_key="replay",
    )

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

    from pipelines.daily.bonds_update_pipeline import (
        update_bonds_pipeline,
    )

    config = get_dag_config(
        context,
        replay_key="replay_from_raw",
    )

    airflow_metadata = get_airflow_metadata(context)

    return update_bonds_pipeline(
        start_date_override=config["start_date_override"],
        replay_from_raw=config["replay_from_raw"],
        airflow_metadata=airflow_metadata,
    ) or "OK"


def run_bonds_processing_pipeline(**context):

    from pipelines.daily.bonds_processing_pipeline import (
        process_bonds,
    )

    config = get_dag_config(
        context,
        replay_key="replay",
    )

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

    from pipelines.daily.derivatives_pipeline import (
        run_derivatives_processing,
    )

    config = get_dag_config(
        context,
        replay_key="replay",
    )

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

    from pipelines.daily.collateral_pipeline import (
        run_collateral_pipeline,
    )

    config = get_dag_config(
        context,
        replay_key="replay",
    )

    airflow_metadata = get_airflow_metadata(context)

    return run_collateral_pipeline(
        start_date_override=config["start_date_override"],
        replay=config["replay"],
        airflow_metadata=airflow_metadata,
    ) or "OK"


# ============================================================
# LOANS PIPELINES
# ============================================================

def run_loans_enrichment_pipeline(**context):

    from pipelines.daily.enrich_loans_pipeline import (
        run_enrich_loans_pipeline,
    )

    config = get_dag_config(
        context,
        replay_key="replay",
    )

    airflow_metadata = get_airflow_metadata(context)

    return run_enrich_loans_pipeline(
        start_date_override=config["start_date_override"],
        replay=config["replay"],
        airflow_metadata=airflow_metadata,
    ) or "OK"


def run_loans_model_pipeline(**context):

    from pipelines.daily.loans_model_pipeline import (
        run_loans_model_pipeline,
    )

    config = get_dag_config(
        context,
        replay_key="replay",
    )

    airflow_metadata = get_airflow_metadata(context)

    return run_loans_model_pipeline(
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
    tags=[
        "risk",
        "portfolio",
        "cross-asset",
        "daily",
        "production",
    ],
) as dag:

    # ========================================================
    # EQUITY
    # ========================================================

    equity_features = PythonOperator(
        task_id="equity_feature_pipeline",
        python_callable=run_equity_feature_pipeline,
    )

    equity_processing = PythonOperator(
        task_id="equity_processing_pipeline",
        python_callable=run_equity_processing_pipeline,
    )

    # ========================================================
    # FX
    # ========================================================

    fx_features = PythonOperator(
        task_id="fx_feature_pipeline",
        python_callable=run_fx_feature_pipeline,
    )

    fx_processing = PythonOperator(
        task_id="fx_processing_pipeline",
        python_callable=run_fx_processing_pipeline,
    )

    # ========================================================
    # COMMODITIES
    # ========================================================

    commodity_features = PythonOperator(
        task_id="commodity_feature_pipeline",
        python_callable=run_commodity_feature_pipeline,
    )

    commodity_processing = PythonOperator(
        task_id="commodity_processing_pipeline",
        python_callable=run_commodity_processing_pipeline,
    )

    # ========================================================
    # BONDS
    # ========================================================

    bonds_features = PythonOperator(
        task_id="bonds_feature_pipeline",
        python_callable=run_bonds_feature_pipeline,
    )

    bonds_processing = PythonOperator(
        task_id="bonds_processing_pipeline",
        python_callable=run_bonds_processing_pipeline,
    )

    # ========================================================
    # DERIVATIVES
    # ========================================================

    derivatives_processing = PythonOperator(
        task_id="derivatives_processing_pipeline",
        python_callable=run_derivatives_pipeline,
    )

    # ========================================================
    # COLLATERAL
    # ========================================================

    collateral_processing = PythonOperator(
        task_id="collateral_processing_pipeline",
        python_callable=run_collateral_pipeline,
    )

    # ========================================================
    # LOANS
    # ========================================================

    loans_enrichment = PythonOperator(
        task_id="loans_enrichment_pipeline",
        python_callable=run_loans_enrichment_pipeline,
    )

    loans_model = PythonOperator(
        task_id="loans_model_pipeline",
        python_callable=run_loans_model_pipeline,
    )

    # ========================================================
    # PIPELINE FLOW
    # ========================================================

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
        >> loans_enrichment
        >> loans_model
    )