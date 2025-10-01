from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from docker.types import Mount

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "milan_streaming_pipeline",
    default_args=default_args,
    description="Streaming ETL for Milan dataset",
    schedule_interval="@once",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    produce = DockerOperator(
    task_id="kafka_producer",
    image="milan_project-spark",
    user="root",
    command=[
        "bash", "-c", """
        echo "Starting Kafka producer..." &&
        python3 /opt/spark/app/producer.py /opt/spark/data/raw/*.txt
        """
    ],
    network_mode="milan_network",
    mount_tmp_dir=False,
    mounts=[
        Mount(source=r'D:\Work\Projects\milan_project\data', target='/opt/spark/data', type='bind'),
        Mount(source=r'D:\Work\Projects\milan_project\scripts', target='/opt/spark/app', type='bind'),
    ],
)

    etl_stream = DockerOperator(
    task_id="spark_streaming_etl",
    image="milan_project-spark",
    user="root",
    command=[
        "bash", "-c", """
        echo "Starting Spark streaming ETL..." &&
        spark-submit \
        --master spark://spark:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        /opt/spark/app/streaming_etl.py
        """
    ],
    network_mode="milan_network",
    mount_tmp_dir=False,
    mounts=[
        Mount(source=r'D:\Work\Projects\milan_project\data', target='/opt/spark/data', type='bind'),
        Mount(source=r'D:\Work\Projects\milan_project\scripts', target='/opt/spark/app', type='bind'),
    ],
)

    produce >> etl_stream