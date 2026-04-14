from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import logging


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
# Task functions
# ==============================
def extract_data(**context):
    """
    Extract data from source (CSV in this case).
    """
    logging.info("Extracting data...")

    df = pd.read_csv("data/sales_data.csv")

    # Push to XCom
    context['ti'].xcom_push(key='raw_data', value=df.to_json())

    logging.info(f"Extracted {len(df)} records")


def transform_data(**context):
    """
    Transform and clean the data.
    """
    logging.info("Transforming data...")

    ti = context['ti']
    raw_json = ti.xcom_pull(key='raw_data')

    df = pd.read_json(raw_json)

    df = df.dropna()
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["amount"] = df["amount"].astype(float)

    df["month"] = df["order_date"].dt.to_period("M")

    ti.xcom_push(key='clean_data', value=df.to_json())

    logging.info("Transformation complete")


def aggregate_data(**context):
    """
    Aggregate KPIs from cleaned data.
    """
    logging.info("Aggregating data...")

    ti = context['ti']
    clean_json = ti.xcom_pull(key='clean_data')

    df = pd.read_json(clean_json)

    agg_df = df.groupby("month").agg(
        total_revenue=("amount", "sum"),
        total_orders=("order_id", "count"),
        unique_customers=("customer_id", "nunique")
    ).reset_index()

    ti.xcom_push(key='agg_data', value=agg_df.to_json())

    logging.info("Aggregation complete")


def load_data(**context):
    """
    Load aggregated data to output.
    """
    logging.info("Loading data...")

    ti = context['ti']
    agg_json = ti.xcom_pull(key='agg_data')

    df = pd.read_json(agg_json)

    output_path = f"data/output_{datetime.now().strftime('%Y%m%d')}.csv"
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

    # Task dependencies (workflow)
    extract_task >> transform_task >> aggregate_task >> load_task