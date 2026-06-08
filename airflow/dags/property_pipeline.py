import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

DBT_PROJECT_PATH = os.environ.get('DBT_PROJECT_PATH', '/opt/airflow/dbt')

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='property_pipeline',
    default_args=default_args,
    description='Ingest property data, run dbt transforms and tests',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['property', 'dbt', 'spark'],
) as dag:

    generate_and_ingest = DockerOperator(
        task_id='generate_and_ingest',
        image='dbt-practice-spark-ingest:latest',
        command='bash -c "python /app/generate_data.py --output-dir /data && spark-submit --packages org.postgresql:postgresql:42.7.1 /app/ingest.py"',
        mounts=[
            Mount(source='/data', target='/data', type='bind'),
        ],
        environment={
            'POSTGRES_HOST': 'postgres',
            'POSTGRES_PORT': '5432',
            'POSTGRES_DB': 'property_db',
            'POSTGRES_USER': 'admin',
            'POSTGRES_PASSWORD': 'admin123',
        },
        network_mode='dbt-practice_default',
        auto_remove='success',
        docker_url='unix:///var/run/docker.sock',
    )

    dbt_run = DockerOperator(
        task_id='dbt_run',
        image='dbt-practice-dbt:latest',
        command='dbt run --profiles-dir /dbt',
        working_dir='/dbt',
        mounts=[
            Mount(source=DBT_PROJECT_PATH, target='/dbt', type='bind'),
        ],
        environment={
            'POSTGRES_HOST': 'postgres',
            'POSTGRES_PORT': '5432',
            'POSTGRES_DB': 'property_db',
            'POSTGRES_USER': 'admin',
            'POSTGRES_PASSWORD': 'admin123',
        },
        network_mode='dbt-practice_default',
        auto_remove='success',
        docker_url='unix:///var/run/docker.sock',
    )

    dbt_test = DockerOperator(
        task_id='dbt_test',
        image='dbt-practice-dbt:latest',
        command='dbt test --profiles-dir /dbt',
        working_dir='/dbt',
        mounts=[
            Mount(source=DBT_PROJECT_PATH, target='/dbt', type='bind'),
        ],
        environment={
            'POSTGRES_HOST': 'postgres',
            'POSTGRES_PORT': '5432',
            'POSTGRES_DB': 'property_db',
            'POSTGRES_USER': 'admin',
            'POSTGRES_PASSWORD': 'admin123',
        },
        network_mode='dbt-practice_default',
        auto_remove='success',
        docker_url='unix:///var/run/docker.sock',
    )

    generate_and_ingest >> dbt_run >> dbt_test
