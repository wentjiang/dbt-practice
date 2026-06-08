FROM apache/airflow:2.9.3

USER root
RUN apt-get update && apt-get install -y docker.io && rm -rf /var/lib/apt/lists/*
USER airflow

RUN pip install apache-airflow-providers-docker==3.9.0
