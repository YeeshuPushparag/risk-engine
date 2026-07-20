"""
KPO task factory for the daily cross-asset DAG (master_dag.py).
Used only when a backfill/replay start_date is more than 20 days back.
Reuses the shared airflow-high-compute node group/taint config.
"""
import os
from datetime import timedelta
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

IMAGE = os.environ["AIRFLOW_IMAGE"]
NAMESPACE = os.environ["AIRFLOW__NAMESPACE"]

DAILY_HIGH_COMPUTE_NODE_LABEL = {
    os.environ["HIGH_COMPUTE_NODE_LABEL_KEY"]: os.environ["HIGH_COMPUTE_NODE_LABEL_VALUE"]
}
DAILY_HIGH_COMPUTE_TOLERATIONS = [
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


def make_daily_kpo(task_id, pipeline_name, cpu_request="1", mem_request="3Gi",
                    cpu_limit="2", mem_limit="6Gi"):
    return KubernetesPodOperator(
        task_id=task_id,
        namespace=NAMESPACE,
        name=task_id.replace("_", "-"),
        image=IMAGE,
        cmds=["python"],
        arguments=["-m", "pipelines.daily.daily_k8s_entrypoint",
                   "--pipeline", pipeline_name] + COMMON_ARGS,
        service_account_name="airflow-sa",
        in_cluster=True,
        node_selector=DAILY_HIGH_COMPUTE_NODE_LABEL,
        tolerations=DAILY_HIGH_COMPUTE_TOLERATIONS,
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": cpu_request, "memory": mem_request},
            limits={"cpu": cpu_limit, "memory": mem_limit},
        ),
        env_from=[
            k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name="airflow-config")),
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="airflow-secret")),
        ],
        get_logs=True,
        do_xcom_push=True,
        on_finish_action="delete_pod",
        on_kill_action="delete_pod",
        execution_timeout=timedelta(hours=2),
        startup_timeout_seconds=300,
    )