from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import logging
from io import StringIO
import os


# ==============================
# Default DAG arguments
# ==============================
default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ==============================
# Config
# ==============================
DATA_PATH = "/tmp"  # safer path for Airflow


# ==============================
# Task functions
# ==============================
def extract_data(**context):
    logging.info("Extracting data...")

    input_file = os.path.join(DATA_PATH, "sales_data.csv")
    df = pd.read_csv(input_file)

    temp_path = os.path.join(DATA_PATH, "raw_data.csv")
    df.to_csv(temp_path, index=False)

    context['ti'].xcom_push(key='raw_path', value=temp_path)

    logging.info(f"Extracted {len(df)} records")


def transform_data(**context):
    logging.info("Transforming data...")

    ti = context['ti']
    raw_path = ti.xcom_pull(key='raw_path')

    df = pd.read_csv(raw_path)

    df = df.dropna()
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df = df.dropna(subset=["order_date"])
    df["amount"] = df["amount"].astype(float)

    df["month"] = df["order_date"].dt.to_period("M").astype(str)

    temp_path = os.path.join(DATA_PATH, "clean_data.csv")
    df.to_csv(temp_path, index=False)

    ti.xcom_push(key='clean_path', value=temp_path)

    logging.info("Transformation complete")


def aggregate_data(**context):
    logging.info("Aggregating data...")

    ti = context['ti']
    clean_path = ti.xcom_pull(key='clean_path')

    df = pd.read_csv(clean_path)

    agg_df = df.groupby("month").agg(
        total_revenue=("amount", "sum"),
        total_orders=("order_id", "count"),
        unique_customers=("customer_id", "nunique")
    ).reset_index()

    temp_path = os.path.join(DATA_PATH, "agg_data.csv")
    agg_df.to_csv(temp_path, index=False)

    ti.xcom_push(key='agg_path', value=temp_path)

    logging.info("Aggregation complete")


def load_data(**context):
    logging.info("Loading data...")

    ti = context['ti']
    agg_path = ti.xcom_pull(key='agg_path')

    df = pd.read_csv(agg_path)

    output_path = os.path.join(
        DATA_PATH,
        f"output_{datetime.now().strftime('%Y%m%d')}.csv"
    )

    df.to_csv(output_path, index=False)

    logging.info(f"Data saved to {output_path}")


# ==============================
# Define DAG
# ==============================
with DAG(
    dag_id="sales_etl_pipeline",
    default_args=default_args,
    description="Sales ETL pipeline using Airflow",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "sales"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    aggregate_task = PythonOperator(
        task_id="aggregate_data",
        python_callable=aggregate_data,
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    extract_task >> transform_task >> aggregate_task >> load_task