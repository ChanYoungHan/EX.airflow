from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor


default_args = {"owner": "hcy", "retries": 5, "retry_delay": timedelta(minutes=10)}


with DAG(
    dag_id="dag_with_azure_blob_v01",
    start_date=datetime(2022, 2, 12),
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
) as dag:

    task1 = WasbBlobSensor(
        task_id="sensor_azure_blob",
        container_name="rosbag",
        blob_name="SW_ANYWHERE_00/20220913/NBT110123/test.db3",
        wasb_conn_id="azure_blob_conn",
        mode="poke",
        poke_interval=5,
        timeout=10,
    )
