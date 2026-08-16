# dags/apriori_pipeline.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

default_args = {
    "owner": "samuel",
    "depends_on_past": False,
    "retries": 1,
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="mlops_pipeline",
    default_args=default_args,
    description="Recommendation (Apriori) + Customer Value Scoring pipeline",
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:

    # -----------------------------------------------------------------------
    # Branch A: Recommendation (Apriori)
    # -----------------------------------------------------------------------
    feature_task = BashOperator(
        task_id="feature_engineering",
        bash_command="python /opt/airflow/src/pythonScripts/feature.py",
    )

    train_apriori = BashOperator(
        task_id="train_apriori",
        bash_command="/usr/local/bin/python /opt/airflow/src/pythonScripts/model_apriori/train_model_apriori.py",
    )

    publish_recommendation = BashOperator(
        task_id="publish_recommendation",
        bash_command="python /opt/airflow/src/pythonScripts/model_apriori/apriori_mlflow_predict.py",
    )

    # -----------------------------------------------------------------------
    # Branch B: Customer Value Scoring (Churn)
    # -----------------------------------------------------------------------
    feature_churn = BashOperator(
        task_id="feature_churn",
        bash_command="python /opt/airflow/src/pythonScripts/model_churn/feature_churn.py",
    )

    train_churn = BashOperator(
        task_id="train_churn",
        bash_command="python /opt/airflow/src/pythonScripts/model_churn/train_churn_model.py",
    )

    # -----------------------------------------------------------------------
    # Dependencies — both branches run in parallel after features are ready
    # -----------------------------------------------------------------------
    feature_task >> train_apriori >> publish_recommendation
    feature_churn >> train_churn

