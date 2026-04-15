from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import requests
import pandas as pd
import numpy as np
from google.cloud import storage
from utils.environments import get_environment_data, BUCKET_SUFFIX_LANDING
from utils.secrets_manager import SecretManager
from utils.monday_boards import BOARDS

env = get_environment_data()

def mondayClient():
    secret_path = f"projects/{env.compute}/secrets/pentland-modaycom-api-key/versions/latest"
    api_key = SecretManager().get_secrets(secret_path)
    headers = {
        'Authorization': api_key,
        'Content-Type': 'application/json'
    }
    return headers

def storageClient():
    storage_client = storage.Client(env.ingestion)
    bucket = storage_client.bucket(f"{env.ingestion}{BUCKET_SUFFIX_LANDING}")
    return bucket

def write_csv_to_gcs(df, bucket, filename):
    df.to_csv(filename, index=False)
    blob = bucket.blob(f'monday/{filename}')
    blob.upload_from_filename(filename)

def fetch_monday_data(**kwargs):
    client = mondayClient()
    bucket = storageClient()
    
    filenames = []

    for group in BOARDS:
        items = fetch_group_data(group, client)

        if not items:
            logging.warning(f"No items fetched for board {group['board_id']}.")
            continue

        df = pd.DataFrame(items)
        df.replace(np.nan,'', inplace=True)


        filename = f"monday_data_{group['board_id']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        write_csv_to_gcs(df, bucket, filename)
        logging.info(f"Uploaded {filename} to GCS.")
        filenames.append(filename)

    kwargs['ti'].xcom_push(key='filenames', value=filenames)
    return "Success"

def fetch_group_data(group, client):
    column_ids_str = ', '.join(f'"{col}"' for col in group['column_ids'])

    query = f"""
    query {{
      boards(ids: {group['board_id']}) {{
        id
        name
        items_page(limit: 500) {{
          items {{
            id
            name
            column_values(ids: [{column_ids_str}]) {{
              id
              text
            }}
          }}
        }}
      }}
    }}
    """

    response = requests.post(
        url="https://api.monday.com/v2",
        headers=client,
        json={"query": query}
    )

    if response.status_code != 200:
        raise Exception(f"GraphQL query failed: {response.text}")

    data = response.json()

    items = []
    boards = data.get("data", {}).get("boards", [])

    for board in boards:
        if not board:
            logging.warning("A board returned as None. Skipping.")
            continue

        board_id = board.get("id")
        board_name = board.get("name")

        for item in board.get("items_page", {}).get("items", []):
                row = {
                    "board_id": board_id,
                    "board_name": board_name,
                    "item_id": item.get("id"),
                    "item_name": item.get("name")
                }

                for col in item.get("column_values", []):
                    col_title = group["id_to_title"].get(col.get("id"), col.get("id"))
                    row[col_title] = col.get("text")

                items.append(row)
    return items

# DAG definition
default_args = {
    'owner': 'pentland',
    'email': 'mehek.n@pentland.com',
    'email_on_failure': False,
    'retries': 0
}

with DAG(
    dag_id="monday_api_dag",
    start_date=datetime(2025, 8, 5),
    default_args=default_args,
    schedule="0 8 15,30 * *",
    max_active_runs=1,
    tags=["monday"]
) as dag:

    data_pull = PythonOperator(
        task_id="monday_api_pull",
        python_callable=fetch_monday_data
    )
    data_pull