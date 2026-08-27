# Airflow + client Spark (spark-submit vers le cluster standalone)
# python3.10 obligatoire : doit matcher le python des workers Spark (spark:3.5.1 / ubuntu 22.04)
FROM apache/airflow:2.10.5-python3.10

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    apache-airflow-providers-apache-spark==4.11.3 \
    hdfs==2.7.3 \
    kafka-python==2.2.15 \
    requests
