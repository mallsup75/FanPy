from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import json
from azure.storage.blob import BlobServiceClient

def combine_rosters_to_silver(**context):
    try:
        dag_run = context["dag_run"]
        connection_string = "DefaultEndpointsProtocol=https;AccountName=sg092620240215;AccountKey=+PaTF6WCZ0NY63Hni1XIWRJfWsnTI7QJCLVP0f1OXUoVzJyl0AcE4h2Pe1b7ZbgldGkDDFA0j9iK+AStvU4auA==;EndpointSuffix=core.windows.net"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Bronze container
        bronze_client = blob_service_client.get_container_client("bronze")
        combined_by_league = {}  # Dict to group by leagueId
        
        blob_list = bronze_client.list_blobs(name_starts_with="roster_data_")
        for blob in blob_list:
            blob_name = blob.name
            print(f"Processing {blob_name}")
            
            # Extract leagueId from filename (e.g., "roster_data_xdkosenrm3hx9eke.json" -> "xdkosenrm3hx9eke")
            league_id = blob_name.replace("roster_data_", "").replace(".json", "")
            if league_id not in combined_by_league:
                combined_by_league[league_id] = []

            # Read blob content
            blob_client = bronze_client.get_blob_client(blob_name)
            blob_data = blob_client.download_blob().readall()
            json_data = json.loads(blob_data.decode("utf-8"))
            
            # Extract rosters
            rosters = json_data.get("rosters", {})
            for team_id, team_data in rosters.items():
                team_name = team_data.get("teamName")
                roster_items = team_data.get("rosterItems", [])
                
                # Add teamName to each roster item and group under leagueId
                for item in roster_items:
                    enriched_item = {
                        "teamName": team_name,
                        **item  # Unpack id, position, salary, status
                    }
                    combined_by_league[league_id].append(enriched_item)

        if not combined_by_league:
            raise ValueError("No roster items found to combine")

        # Silver container
        silver_client = blob_service_client.get_container_client("silver")
        if not silver_client.exists():
            silver_client.create_container()
        output_blob_name = f"combined_rosters_{dag_run.logical_date.strftime('%Y%m%d_%H%M%S')}.json"
        output_blob_client = silver_client.get_blob_client(output_blob_name)
        output_blob_client.upload_blob(json.dumps(combined_by_league, indent=2), overwrite=True)
        print(f"Uploaded {output_blob_name} to silver")

    except Exception as e:
        raise Exception(f"Pipeline failed: {str(e)}")

with DAG(
    dag_id="combine_rosters_to_silver_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    wait_for_fetch = ExternalTaskSensor(
        task_id="wait_for_fetch_dag",
        external_dag_id="fetch_team_rosters_dag",
        external_task_id="fetchrosters_and_land_task",
        mode="poke",
        timeout=600,
    )
    combine_task = PythonOperator(
        task_id="combine_rosters_task",
        python_callable=combine_rosters_to_silver,
        provide_context=True,
    )
    wait_for_fetch >> combine_task
