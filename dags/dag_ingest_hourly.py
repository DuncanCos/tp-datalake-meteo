"""DAG d'ingestion des donnees HORAIRES Meteo-France -> Bronze HDFS.

Fichiers H_{dept}_latest-2025-2026.csv.gz (cumulatifs, reecrits chaque jour
par Meteo-France) : un lot snapshot=YYYY-MM-DD par jour logique, comme le
fichier "latest" du DAG batch quotidien. Rejouer le meme jour skippe, le
jour suivant cree un nouveau snapshot.

Ces heures officielles alimentent la confrontation "live vs officiel" :
elles sont agregees au jour (convention journee climatologique Meteo-France)
et comparees au flux Open-Meteo par le job gold_confrontation_job.py.
"""
from __future__ import annotations

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DEPTS = ["75", "69", "13", "31", "06", "44", "67", "33", "59", "35"]
PERIOD = "latest-2025-2026"

BASE_URL = "https://meteofrance.s3.sbg.io.cloud.ovh.net/data/synchro_ftp/BASE/HOR"
WEBHDFS_URL = "http://namenode:9870"
BRONZE_ROOT = "/datalake/bronze/source=meteofrance_hor"


@dag(
    dag_id="ingest_hourly_meteofrance",
    schedule="20 7 * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz="Europe/Paris"),
    catchup=False,
    params={"force": False},
    tags=["bronze", "meteofrance", "horaire"],
    doc_md=__doc__,
)
def ingest_hourly_meteofrance():

    @task
    def plan_lots(**context) -> list[dict]:
        snapshot = context["data_interval_end"].date().isoformat()
        return [{"dept": d, "snapshot": snapshot} for d in DEPTS]

    @task
    def ingest_lot(dept: str, snapshot: str, **context) -> str:
        from hdfs import InsecureClient

        client = InsecureClient(WEBHDFS_URL, user="root")
        lot_dir = f"{BRONZE_ROOT}/dept={dept}/period={PERIOD}/snapshot={snapshot}"
        marker = f"{lot_dir}/_SUCCESS"

        if client.status(marker, strict=False) and not context["params"].get("force"):
            raise AirflowSkipException(f"lot deja ingere : {lot_dir}")

        filename = f"H_{dept}_{PERIOD}.csv.gz"
        resp = requests.get(f"{BASE_URL}/{filename}", stream=True, timeout=600)
        if resp.status_code == 404:
            raise AirflowSkipException(f"fichier absent chez Meteo-France : {filename}")
        resp.raise_for_status()

        client.write(
            f"{lot_dir}/{filename}",
            data=resp.iter_content(chunk_size=1 << 16),
            overwrite=True,
        )
        # marker ecrit en dernier : un lot sans _SUCCESS est un lot incomplet
        client.write(marker, data=b"", overwrite=True)
        return f"{lot_dir}/{filename}"

    lots = ingest_lot.expand_kwargs(plan_lots())

    # all_done : Silver tourne aussi quand tous les lots sont skippes (les
    # snapshots de jours precedents restent a (re)traiter apres un force)
    build_silver_hourly = SparkSubmitOperator(
        task_id="build_silver_hourly",
        application="/opt/airflow/spark/silver_hourly_job.py",
        conn_id="spark_default",
        name="silver-hourly",
        conf={
            "spark.cores.max": "1",
            "spark.executor.memory": "1g",
        },
        verbose=False,
        trigger_rule="all_done",
    )

    trigger_confrontation = TriggerDagRunOperator(
        task_id="trigger_confrontation",
        trigger_dag_id="confrontation",
    )
    lots >> build_silver_hourly >> trigger_confrontation


ingest_hourly_meteofrance()
