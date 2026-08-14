# dags/apriori_pipeline.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

# Define root directory once
PROJECT_ROOT = "/opt/airflow/dags"


default_args = {
    "owner": "samuel",
    "depends_on_past": False,
    "retries": 1,
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="apriori_pipeline",
    default_args=default_args,
    description="Pipeline for Apriori feature engineering, training, MLflow logging, and prediction",
    schedule_interval=None,
    catchup=False,
) as dag:

    # 1. Feature Engineering
    feature_task = BashOperator(
        task_id="feature_engineering",
        bash_command="python /opt/airflow/src/pythonScripts/feature.py"
    )

    # 2. Train Apriori Model (pure compute: writes apriori_model.pkl + apriori_rules.csv locally,
    #    no MLflow calls — keeps retries here cheap and free of network dependency)
    train_task = BashOperator(
        task_id="train_apriori",
        bash_command="/usr/local/bin/python /opt/airflow/src/pythonScripts/model_apriori/train_model_apriori.py",
        dag=dag,
    )

    # 3. Log trained model, rules, and eval metrics to MLflow (separated from training
    #    so a flaky MLflow connection can be retried without re-running Apriori)
    log_to_mlflow_task = BashOperator(
        task_id="log_apriori_to_mlflow",
        bash_command="python /opt/airflow/src/pythonScripts/model_apriori/log_apriori_to_mlflow.py",
        dag=dag,
    )

    # 4. MLflow Prediction
    predict_task = BashOperator(
        task_id="mlflow_predict",
        bash_command="python /opt/airflow/src/pythonScripts/model_apriori/apriori_mlflow_predict.py"
    )

    # Define dependencies
    feature_task >> train_task >> log_to_mlflow_task >> predict_task