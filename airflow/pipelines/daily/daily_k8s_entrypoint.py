"""
Generic entrypoint for running daily cross-asset pipelines inside a
KubernetesPodOperator pod (used only for long backfill/replay spans).
Reconstructs the same airflow_metadata dict that PythonOperator tasks
built from **context, using CLI args that Airflow Jinja-templates in.
"""
import argparse
import json
import os
import sys

# Maps --pipeline value -> (module path, function name, replay kwarg name)
PIPELINE_REGISTRY = {
    "equity_features": (
        "pipelines.daily.market_features_s3", "update_market_features", "replay_from_raw"
    ),
    "equity_processing": (
        "pipelines.daily.equity_risk_prediction_pipeline", "run_equity_risk_pipeline", "replay"
    ),
    "fx_features": (
        "pipelines.daily.fx_exposure_pipeline", "update_fx_pipeline", "replay_from_raw"
    ),
    "fx_processing": (
        "pipelines.daily.fx_update_pipeline", "update_fx_snowflake", "replay"
    ),
    "commodity_features": (
        "pipelines.daily.commodity_update_pipeline", "update_commodity_pipeline", "replay_from_raw"
    ),
    "commodity_processing": (
        "pipelines.daily.commodity_processing_pipeline", "process_commodities", "replay"
    ),
    "bonds_features": (
        "pipelines.daily.bonds_update_pipeline", "update_bonds_pipeline", "replay_from_raw"
    ),
    "bonds_processing": (
        "pipelines.daily.bonds_processing_pipeline", "process_bonds", "replay"
    ),
    "derivatives_processing": (
        "pipelines.daily.derivatives_pipeline", "run_derivatives_processing", "replay"
    ),
    "collateral_processing": (
        "pipelines.daily.collateral_pipeline", "run_collateral_pipeline", "replay"
    ),
}





def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline", required=True, choices=list(PIPELINE_REGISTRY.keys()))
    parser.add_argument("--start-date", default="")
    parser.add_argument("--replay", default="false")
    parser.add_argument("--replay-from-raw", default="false")
    parser.add_argument("--dag-id", required=True)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--dag-run-id", required=True)
    parser.add_argument("--dag-run-type", required=True)
    parser.add_argument("--try-number", required=True)
    parser.add_argument("--max-tries", required=True)
    parser.add_argument("--logical-date", required=True)
    args = parser.parse_args()

    start_date_override = args.start_date or None
    replay_flag = args.replay.strip().lower() == "true"
    replay_from_raw_flag = args.replay_from_raw.strip().lower() == "true"   

    airflow_metadata = {
        "dag_id": args.dag_id,
        "task_id": args.task_id,
        "dag_run_id": args.dag_run_id,
        "dag_run_type": args.dag_run_type,
        "try_number": args.try_number,
        "max_tries": args.max_tries,
        "logical_date": args.logical_date,
        "execution_date": args.logical_date,
        "triggered_by": "manual" if args.dag_run_type == "manual" else "scheduled",
    }

    module_path, func_name, replay_kwarg = PIPELINE_REGISTRY[args.pipeline]

    try:
        import importlib
        module = importlib.import_module(module_path)
        func = getattr(module, func_name)
        
        flag_value = replay_from_raw_flag if replay_kwarg == "replay_from_raw" else replay_flag
        
        kwargs = {
            "start_date_override": start_date_override,
            replay_kwarg: flag_value,
            "airflow_metadata": airflow_metadata,
        }
        result = func(**kwargs) or "OK"

        print(f"[ENTRYPOINT] {args.pipeline} finished: {result}")

    except Exception as e:
        print(f"[ENTRYPOINT][FAILED] {args.pipeline}: {e}", file=sys.stderr)
        try:
            with open("/dev/termination-log", "w") as f:
                f.write(f"{args.pipeline} failed: {e}")
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()