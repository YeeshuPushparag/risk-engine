import os
from datetime import datetime, timedelta
from airflow.configuration import conf
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from pendulum import timezone

US_TZ = timezone("America/New_York")

IMAGE = os.environ["AIRFLOW_IMAGE"]
NAMESPACE = os.environ["AIRFLOW__NAMESPACE"]
LOAN_NODE_LABEL = {os.environ["HIGH_COMPUTE_NODE_LABEL_KEY"]: os.environ["HIGH_COMPUTE_NODE_LABEL_VALUE"]}
LOAN_TOLERATIONS = [
    k8s.V1Toleration(
        key=os.environ["HIGH_COMPUTE_NODE_TAINT_KEY"],
        operator="Equal",
        value=os.environ["HIGH_COMPUTE_NODE_TAINT_VALUE"],
        effect="NoSchedule",
    )
]

COMMON_ARGS = [
    "--dag-id", "{{ dag.dag_id }}",
    "--task-id", "{{ task.task_id }}",
    "--dag-run-id", "{{ dag_run.run_id }}",
    "--dag-run-type", "{{ dag_run.run_type }}",
    "--try-number", "{{ ti.try_number }}",
    "--max-tries", "{{ ti.max_tries }}",
    "--logical-date", "{{ logical_date }}",
    "--start-date", "{{ dag_run.conf.get('start_date', '') if dag_run.conf else '' }}",
    "--replay", "{{ dag_run.conf.get('replay', False) if dag_run.conf else False }}",
]


def make_loan_kpo(task_id, pipeline_name, cpu_request="2", mem_request="6Gi",
                   cpu_limit="4", mem_limit="12Gi"):
    return KubernetesPodOperator(
        task_id=task_id,
        namespace=NAMESPACE,
        name=f"{task_id.replace('_', '-')}",
        image=IMAGE,
        cmds=["python"],
        arguments=["-m", "pipelines.monthly.loans_k8s_entrypoint",
                   "--pipeline", pipeline_name] + COMMON_ARGS,
        service_account_name="airflow-sa",
        in_cluster=True,
        node_selector=LOAN_NODE_LABEL,
        tolerations=LOAN_TOLERATIONS,
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": cpu_request, "memory": mem_request},
            limits={"cpu": cpu_limit, "memory": mem_limit},
        ),
        env_from=[
            k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name="airflow-config")),
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="airflow-secret")),
        ],
        get_logs=True,
        do_xcom_push=False,
        on_finish_action="delete_pod",
        on_kill_action="delete_pod",
        execution_timeout=timedelta(hours=2),
        startup_timeout_seconds=300,
    )


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="monthly_loans_risk_pipeline",
    default_args=default_args,
    schedule="0 0 1 * *",
    start_date=datetime(2026, 1, 1, tzinfo=US_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["loans", "risk", "monthly", "portfolio", "production"],
) as dag:

    macro_pipeline = make_loan_kpo(
        "run_macro_pipeline", "macro",
        cpu_request="1", mem_request="2Gi", cpu_limit="2", mem_limit="4Gi",
    )

    enrich_loans = make_loan_kpo(
        "enrich_loans_dataset", "enrich",
        cpu_request="2", mem_request="6Gi", cpu_limit="4", mem_limit="12Gi",
    )

    loans_model = make_loan_kpo(
        "run_loans_model_pipeline", "model",
        cpu_request="2", mem_request="8Gi", cpu_limit="4", mem_limit="14Gi",
    )

    macro_pipeline >> enrich_loans >> loans_model