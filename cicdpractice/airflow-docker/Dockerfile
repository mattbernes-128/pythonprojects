FROM python:3.8-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends vim && \
    rm -rf /var/lib/apt/lists/*

RUN pip install apache-airflow

USER airflow