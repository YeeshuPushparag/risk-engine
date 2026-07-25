"""
Generic entrypoint for running monthly loan pipelines inside a
KubernetesPodOperator pod. Reconstructs the same airflow_metadata dict
that PythonOperator tasks built from **context, using CLI args that
Airflow Jinja-templates in.
"""
import argparse
import json
import os
import sys





def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline", required=True,
                         choices=["macro", "enrich", "model"])
    parser.add_argument("--start-date", default="")
    parser.add_argument("--replay", default="false")
    parser.add_argument("--dag-id", required=True)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--dag-run-id", required=True)
    parser.add_argument("--dag-run-type", required=True)
    parser.add_argument("--try-number", required=True)
    parser.add_argument("--max-tries", required=True)
    parser.add_argument("--logical-date", required=True)
    args = parser.parse_args()

    start_date_override = args.start_date or None
    replay = args.replay.strip().lower() == "true"

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

    try:
        if args.pipeline == "macro":
            from pipelines.monthly.macro_pipeline import fetch_macro_data
            fetch_macro_data()
            result = "OK"

        elif args.pipeline == "enrich":
            from pipelines.monthly.enrich_loans_pipeline import run_enrich_loans_pipeline
            result = run_enrich_loans_pipeline(
                start_date_override=start_date_override,
                replay=replay,
                airflow_metadata=airflow_metadata,
            ) or "OK"

        elif args.pipeline == "model":
            from pipelines.monthly.loans_model_pipeline import run_loans_model_pipeline
            result = run_loans_model_pipeline(
                start_date_override=start_date_override,
                replay=replay,
                airflow_metadata=airflow_metadata,
            ) or "OK"

        print(f"[ENTRYPOINT] {args.pipeline} finished: {result}")

    except Exception as e:
        # Surface the real error in pod logs AND in the termination log
        # so it shows up in the Airflow task failure message.
        print(f"[ENTRYPOINT][FAILED] {args.pipeline}: {e}", file=sys.stderr)
        try:
            with open("/dev/termination-log", "w") as f:
                f.write(f"{args.pipeline} failed: {e}")
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()