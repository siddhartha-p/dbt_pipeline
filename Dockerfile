FROM apache/airflow:3.1.6

USER root
RUN apt-get update && \
    apt-get install -y gcc libpq-dev && \
    apt-get clean

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt