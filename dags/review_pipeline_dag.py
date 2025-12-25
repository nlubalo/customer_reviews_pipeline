# dags/customer_reviews_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append("/opt/airflow/src")

from clean_job import clean_training_dataset

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="customer_reviews_training_dataset",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # manual trigger
    catchup=False,
    tags=["pandas", "ml", "data-quality"],
) as dag:

    build_dataset = PythonOperator(
        task_id="build_training_dataset",
        python_callable=clean_training_dataset,
        op_kwargs={
            "input_path": "/opt/airflow/data/amazon.csv",
            "output_path": "/opt/airflow/data/training_data",
        },
    )

    build_dataset
