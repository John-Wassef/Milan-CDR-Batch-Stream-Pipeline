from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from docker.types import Mount
import boto3
from botocore.exceptions import ClientError

def create_minio_bucket():
    minio_endpoint = 'http://minio:9000'
    access_key = 'minioadmin'
    secret_key = 'minioadmin'
    
    # Create S3 client for MinIO
    s3_client = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name='us-east-1',
        verify=False
    )
    
    bucket_name = 'telecom-data'
    
    try:

        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists - skipping creation")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':

            try:
                s3_client.create_bucket(Bucket=bucket_name)
                print(f"✓ Bucket '{bucket_name}' created successfully!")
            except ClientError as create_error:
                print(f"Error creating bucket: {create_error}")
                raise
        else:
            print(f"Error checking bucket: {e}")
            raise


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    "milan_batch_pipeline",
    default_args=default_args,
    description="Complete Batch ETL Pipeline: MinIO Setup + Spark Processing",
    schedule_interval="@daily",
    catchup=False,
    tags=['milan', 'batch', 'etl', 'spark', 'minio']
) as dag:


    create_bucket_task = PythonOperator(
        task_id='create_minio_bucket',
        python_callable=create_minio_bucket,
        retries=2,
        retry_delay=timedelta(seconds=30),
    )


    batch_etl = DockerOperator(
        task_id="spark_batch_etl",
        image="milan_project-spark",
        command=[
            "bash", "-c", """
            echo "Starting Spark ETL job..." &&
            spark-submit \
            --master spark://spark:7077 \
            /opt/spark/app/etl.py /opt/spark/data/raw s3a://telecom-data/processed
            """
        ],
        network_mode="milan_network",
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source=r'D:\Work\Projects\milan_project\data', 
                target='/opt/spark/data', 
                type='bind'
            )
        ],
        environment={},
        auto_remove=True,
        docker_conn_id=None,
    )

    create_bucket_task >> batch_etl