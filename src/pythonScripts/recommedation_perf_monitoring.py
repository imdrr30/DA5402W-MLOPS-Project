# dags/recommedation_perf_monitoring.py
import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

STATUS_PATH = "/opt/airflow/DB/monitoring/latest_status.json"


def _decide_retrain(**context):
    if not os.path.exists(STATUS_PATH):
        raise FileNotFoundError(f"Monitoring status file not found at {STATUS_PATH}")
    with open(STATUS_PATH) as f:
        status = json.load(f)
    return "trigger_retrain" if status.get("trigger_retrain") else "no_action"


with DAG(
    dag_id="recommendation_perf_monitoring",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["monitoring", "evidently", "mlflow"],
) as dag:

    run_monitoring = BashOperator(
        task_id="run_drift_monitoring",
        bash_command=(
            "python /opt/airflow/src/pythonScripts/monitoring.py "
            "--run-date {{ ds }} "
            "--metric-key " + Variable.get("recommendation_metric_key", default_var="recall_at_k") + " "
            "--metric-threshold " + str(Variable.get("recommendation_metric_threshold", default_var=0.15)) + " "
            "--mlflow-experiment-name " + Variable.get("mlflow_experiment_name", default_var="Default")
        ),
    )

    decide_retrain = BranchPythonOperator(
        task_id="decide_retrain",
        python_callable=_decide_retrain,
    )

    trigger_retrain = TriggerDagRunOperator(
        task_id="trigger_retrain",
        trigger_dag_id="apriori_pipeline",   # reuses your existing feature -> train -> predict DAG
    )

    no_action = EmptyOperator(task_id="no_action")

    run_monitoring >> decide_retrain >> [trigger_retrain, no_action]