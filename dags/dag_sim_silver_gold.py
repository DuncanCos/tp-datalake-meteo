"""DAG Silver + Gold des grilles SIM2.

Declenche par ingest_sim (TriggerDagRun), ou manuellement avec le param
annees pour (re)calculer une plage d'annees de la table mensuelle Gold :
  - annees vide  : le job Gold ne recalcule que la derniere annee presente
    en Silver (run quotidien court, 1 core) ;
  - annees "1958-2026" (ou "2024") : backfill manuel, une fois.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id="sim_silver_gold",
    schedule=None,  # declenche par ingest_sim
    start_date=pendulum.datetime(2026, 8, 1, tz="Europe/Paris"),
    catchup=False,
    max_active_runs=1,
    params={"annees": ""},
    tags=["silver", "gold", "sim2", "spark"],
    doc_md=__doc__,
)
def sim_silver_gold():
    build_silver_sim = SparkSubmitOperator(
        task_id="build_silver_sim",
        application="/opt/airflow/spark/silver_sim_job.py",
        conn_id="spark_default",
        name="silver-sim",
        conf={
            "spark.cores.max": "1",
            "spark.executor.memory": "1g",
        },
        verbose=False,
    )
    build_gold_sim = SparkSubmitOperator(
        task_id="build_gold_sim",
        application="/opt/airflow/spark/gold_sim_job.py",
        conn_id="spark_default",
        name="gold-sim",
        conf={
            "spark.cores.max": "1",
            "spark.executor.memory": "1g",
        },
        application_args=["--annees", "{{ params.annees }}"],
        verbose=False,
    )
    build_silver_sim >> build_gold_sim


sim_silver_gold()
