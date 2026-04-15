from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

# ==========================================
# Helper Functions (Simulating Real Logic)
# ==========================================

def read_sales_data(**kwargs):
    print("Reading sales data...")
    data = {
        "order_id": [1, 2, 3, 3],
        "product": ["Shoes", "T-shirt", "Shoes", "Shoes"],
        "revenue": [100, 50, 100, 100]
    }
    df = pd.DataFrame(data)
    kwargs['ti'].xcom_push(key='raw_data', value=df.to_json())


def clean_sales_data(**kwargs):
    print("Cleaning data...")
    ti = kwargs['ti']
    df = pd.read_json(ti.xcom_pull(key='raw_data'))

    df = df.drop_duplicates()
    df['product'] = df['product'].str.lower()

    ti.xcom_push(key='clean_data', value=df.to_json())


def add_derived_metrics(**kwargs):
    print("Adding derived metrics...")
    ti = kwargs['ti']
    df = pd.read_json(ti.xcom_pull(key='clean_data'))

    df['tax'] = df['revenue'] * 0.1
    df['total_revenue'] = df['revenue'] + df['tax']

    ti.xcom_push(key='transformed_data', value=df.to_json())


def aggregate_sales(**kwargs):
    print("Aggregating sales...")
    ti = kwargs['ti']
    df = pd.read_json(ti.xcom_pull(key='transformed_data'))

    agg_df = df.groupby('product').sum().reset_index()

    ti.xcom_push(key='aggregated_data', value=agg_df.to_json())


def save_to_csv(**kwargs):
    print("Saving output...")
    ti = kwargs['ti']
    df = pd.read_json(ti.xcom_pull(key='aggregated_data'))

    output_path = "/tmp/final_sales_output.csv"
    df.to_csv(output_path, index=False)

    print(f"Data saved to {output_path}")


# ==========================================
# DAG Definition
# ==========================================

default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="sales_data_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Dummy DAG for sales data processing pipeline"
) as dag:

    task_read = PythonOperator(
        task_id="read_sales_data",
        python_callable=read_sales_data
    )

    task_clean = PythonOperator(
        task_id="clean_sales_data",
        python_callable=clean_sales_data
    )

    task_transform = PythonOperator(
        task_id="add_derived_metrics",
        python_callable=add_derived_metrics
    )

    task_aggregate = PythonOperator(
        task_id="aggregate_sales",
        python_callable=aggregate_sales
    )

    task_save = PythonOperator(
        task_id="save_to_csv",
        python_callable=save_to_csv
    )

    # Task Dependencies (Workflow)
    task_read >> task_clean >> task_transform >> task_aggregate >> task_save