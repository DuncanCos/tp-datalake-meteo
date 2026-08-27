"""DAG d'ingestion des archives de vigilance Meteo-France -> Bronze HDFS.

L'archive (files.data.gouv.fr, depuis le 2022-11-28) publie ~4 productions
par jour ; on garde la premiere carte CDP_CARTE_EXTERNE.json de chaque jour
(~210 Ko) -> 1 lot par jour, marker _SUCCESS, comme les autres ingestions.

Params :
  - date_debut / date_fin : plage a ingerer (YYYY-MM-DD). Vides -> les
    3 derniers jours. Backfill complet : relancer une fois avec
    date_debut=2022-11-28 (~1 400 fichiers, ~300 Mo).
  - force : true pour re-ingerer un lot deja present.

Ces vigilances servent a valider les episodes detectes par gold_job.py
(croisement dans gold_confrontation_job.py -> table episodes_vigilance).
"""
from __future__ import annotations

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

VIGILANCE_URL = "https://files.data.gouv.fr/meteofrance/data/vigilance"
TREE_URL = f"{VIGILANCE_URL}/vigilance-hexagone-tree.json"
CARTE_FILE = "CDP_CARTE_EXTERNE.json"

WEBHDFS_URL = "http://namenode:9870"
BRONZE_ROOT = "/datalake/bronze/source=vigilance"


@dag(
    dag_id="ingest_vigilance",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz="Europe/Paris"),
    catchup=False,
    params={"date_debut": "", "date_fin": "", "force": False},
    tags=["bronze", "vigilance", "batch"],
    doc_md=__doc__,
)
def ingest_vigilance():

    @task
    def plan_lots(**context) -> list[dict]:
        """1 lot par jour de la plage = la 1re production du jour dans l'arbre."""
        from datetime import datetime, timedelta

        p = context["params"]
        fin = (
            datetime.strptime(p["date_fin"], "%Y-%m-%d").date()
            if p.get("date_fin")
            else context["data_interval_end"].date()
        )
        debut = (
            datetime.strptime(p["date_debut"], "%Y-%m-%d").date()
            if p.get("date_debut")
            else fin - timedelta(days=2)
        )
        if fin < debut:
            raise ValueError(f"date_fin {fin} < date_debut {debut}")

        # arbre {YYYY: {MM: {DD: {HHMMSS: [fichiers]}}}} (requests suit la 302)
        tree = requests.get(TREE_URL, timeout=120).json()

        lots = []
        d = debut
        while d <= fin:
            y, m, dd = f"{d.year}", f"{d.month:02d}", f"{d.day:02d}"
            productions = tree.get(y, {}).get(m, {}).get(dd, {})
            for heure in sorted(productions):
                if CARTE_FILE in productions[heure]:
                    lots.append({"date": d.isoformat(), "heure": heure})
                    break
            d += timedelta(days=1)
        return lots

    @task
    def ingest_lot(date: str, heure: str, **context) -> str:
        from hdfs import InsecureClient

        client = InsecureClient(WEBHDFS_URL, user="root")
        lot_dir = f"{BRONZE_ROOT}/date={date}"
        marker = f"{lot_dir}/_SUCCESS"

        if client.status(marker, strict=False) and not context["params"].get("force"):
            raise AirflowSkipException(f"lot deja ingere : {lot_dir}")

        y, m, d = date.split("-")
        url = f"{VIGILANCE_URL}/metropole/{y}/{m}/{d}/{heure}/{CARTE_FILE}"
        resp = requests.get(url, timeout=120)
        if resp.status_code == 404:
            raise AirflowSkipException(f"carte absente : {url}")
        resp.raise_for_status()

        client.write(f"{lot_dir}/{CARTE_FILE}", data=resp.content, overwrite=True)
        # marker ecrit en dernier : un lot sans _SUCCESS est un lot incomplet
        client.write(marker, data=b"", overwrite=True)
        return f"{lot_dir}/{CARTE_FILE}"

    lots = ingest_lot.expand_kwargs(plan_lots())

    # all_done : Silver tourne aussi quand tous les lots sont skippes
    build_silver_vigilance = SparkSubmitOperator(
        task_id="build_silver_vigilance",
        application="/opt/airflow/spark/silver_vigilance_job.py",
        conn_id="spark_default",
        name="silver-vigilance",
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
    lots >> build_silver_vigilance >> trigger_confrontation


ingest_vigilance()
