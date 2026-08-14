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
    description="Pipeline for Apriori feature engineering, training, serving, and prediction",
    schedule_interval=None,
    catchup=False,
) as dag:

    # 1. Feature Engineering
    feature_task = BashOperator(
        task_id="feature_engineering",
        bash_command="python /opt/airflow/src/pythonScripts/feature.py"
    )

    # 2. Train Apriori Model
    train_task = BashOperator(
        task_id="train_apriori",
        bash_command="/usr/local/bin/python /opt/airflow/src/pythonScripts/model_apriori/train_model_apriori.py",
        dag=dag,
    )

    # 4. MLflow Prediction
    predict_task = BashOperator(
        task_id="mlflow_predict",
        bash_command="python /opt/airflow/src/pythonScripts/model_apriori/apriori_mlflow_predict.py"
    )

    # Define dependencies
    feature_task >> train_task >> predict_task
