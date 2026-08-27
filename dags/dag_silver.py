"""DAG Silver : normalisation Bronze -> Parquet (modele commun).

Deux modes selon les params (dynamic partition overwrite : toujours idempotent) :
  - cycle planifie 15 min (defauts) : sources=openmeteo, scope=live -> ne
    retraite que le flux temps reel (leger) et Gold ne reconstruit que
    live_status ;
  - declenche par l'ingestion batch (ou manuellement) : sources=all,
    scope=full -> retraite aussi Meteo-France et Gold reconstruit toutes
    les tables. Les donnees officielles ne changent qu'une fois par jour,
    inutile de re-broyer ~70 ans d'historique toutes les 15 minutes.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id="silver_observations",
    # toutes les 15 min pour rafraichir le "en direct" (le flux openmeteo
    # arrive en continu en Bronze) + declenche par ingest_batch_meteofrance
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz="Europe/Paris"),
    catchup=False,
    max_active_runs=1,
    params={"date_debut": "", "date_fin": "", "sources": "openmeteo",
            "scope": "live"},
    tags=["silver", "spark"],
    doc_md=__doc__,
)
def silver_observations():
    build_silver = SparkSubmitOperator(
        task_id="build_silver",
        application="/opt/airflow/spark/silver_job.py",
        conn_id="spark_default",
        name="silver-observations",
        conf={
            "spark.cores.max": "1",
            "spark.executor.memory": "1g",
        },
        application_args=[
            "--date-debut", "{{ params.date_debut }}",
            "--date-fin", "{{ params.date_fin }}",
            "--sources", "{{ params.sources }}",
        ],
        verbose=False,
    )

    # dependance explicite entre couches : Silver -> Gold (meme scope)
    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold",
        trigger_dag_id="gold_grisaille",
        conf={"scope": "{{ params.scope }}"},
    )
    build_silver >> trigger_gold


silver_observations()
