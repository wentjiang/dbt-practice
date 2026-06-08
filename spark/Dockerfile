FROM apache/spark:3.5.0

USER root
RUN pip install psycopg2-binary==2.9.9 faker==24.14.1 pandas==2.0.3

COPY scripts/generate_data.py /app/generate_data.py
COPY spark/ingest.py /app/ingest.py

USER spark
