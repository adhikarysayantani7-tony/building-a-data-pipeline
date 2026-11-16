"""
Apache Airflow DAG for Automated Orders ETL Pipeline
Internship Track: Week 3 - Task 5
Author: Sayantani Adhikary
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.etl_pipeline import extract, transform, load

default_args = {
    "owner": "sayantani_adhikary",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def extract_task(**context):
    df = extract("data/raw_orders.csv")
    df.to_pickle("/tmp/raw_orders.pkl")


def transform_task(**context):
    import pandas as pd
    raw_df = pd.read_pickle("/tmp/raw_orders.pkl")
    clean_df = transform(raw_df)
    clean_df.to_pickle("/tmp/clean_orders.pkl")


def load_task(**context):
    import pandas as pd
    clean_df = pd.read_pickle("/tmp/clean_orders.pkl")
    load(clean_df, "data/processed_orders.csv", "data/orders_warehouse.db")


with DAG(
    dag_id="retail_orders_etl_pipeline",
    default_args=default_args,
    description="Daily ETL pipeline to extract, clean, and load retail order data",
    schedule_interval="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["etl", "data-pipeline", "airflow"],
) as dag:

    t1_extract = PythonOperator(
        task_id="extract_raw_orders",
        python_callable=extract_task,
    )

    t2_transform = PythonOperator(
        task_id="transform_and_clean_orders",
        python_callable=transform_task,
    )

    t3_load = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=load_task,
    )

    # Define execution order
    t1_extract >> t2_transform >> t3_load
