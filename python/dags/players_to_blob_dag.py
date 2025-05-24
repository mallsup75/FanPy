from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
from azure.storage.blob import BlobServiceClient

def fetch_and_land_to_blob():
    response = requests.get("https://www.fantrax.com/fxea/general/getAdp?sport=MLB")
    data = json.dumps(response.json())
    connection_string = "DefaultEndpointsProtocol=https;AccountName=sg092620240215;AccountKey=+PaTF6WCZ0NY63Hni1XIWRJfWsnTI7QJCLVP0f1OXUoVzJyl0AcE4h2Pe1b7ZbgldGkDDFA0j9iK+AStvU4auA==;EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_name = "bronze"
    blob_name = "players_data.json"
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(data, overwrite=True)
    print(f"Uploaded {blob_name} to Azure Blob Storage")

with DAG("players_to_blob", start_date=datetime(2025, 2, 28), schedule_interval="@daily", catchup=False) as dag:
    fetch_task = PythonOperator(task_id="fetchplayers_and_land", python_callable=fetch_and_land_to_blob)
