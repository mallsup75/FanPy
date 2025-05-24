from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import json
from azure.storage.blob import BlobServiceClient

def fetch_and_land_to_blob():
    try:
        # Get league_id from Airflow Variables with a default
        league_id = Variable.get("league_id", default_var="xdkosenrm3hx9eke")
        base_url = "https://www.fantrax.com/fxea/general/getTeamRosters?leagueId="
        full_url = f"{base_url}{league_id}"
        
        # Fetch data with error handling
        response = requests.get(full_url, timeout=10)
        response.raise_for_status()
        json_data = response.json()

        # Hardcoded Azure Storage Connection String
        connection_string = "DefaultEndpointsProtocol=https;AccountName=sg092620240215;AccountKey=+PaTF6WCZ0NY63Hni1XIWRJfWsnTI7QJCLVP0f1OXUoVzJyl0AcE4h2Pe1b7ZbgldGkDDFA0j9iK+AStvU4auA==;EndpointSuffix=core.windows.net"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Upload to blob
        container_name = "bronze"
        blob_name = f"roster_data_{league_id}.json"  # Unique blob name per league
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_client.upload_blob(json.dumps(json_data), overwrite=True)
        print(f"Uploaded {blob_name} to Azure Blob Storage")
    
    except requests.exceptions.RequestException as e:
        raise Exception(f"API request failed: {str(e)}")
    except Exception as e:
        raise Exception(f"Blob upload failed: {str(e)}")

with DAG(
    dag_id='fetch_team_rosters_dag',
    start_date=datetime(2025, 1, 1),  # Past date for immediate testing
    schedule='@daily',
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id='fetchrosters_and_land_task',
        python_callable=fetch_and_land_to_blob,
        retries=3,  # Retry on failure
        retry_delay=timedelta(minutes=5),
    )
