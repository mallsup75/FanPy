# FanPy
End-to-End Data Engineering Components Extract raw JSON roster data from bronze container using orchestrated workflow.  How: DAG Setup: Designed an Apache Airflow DAG to automate the process: Tasks: List Blobs: Query bronzeDownload and Parse: Fetch JSONs using BlobServiceClient, parse into a Pandas DataFrame.
