"""DAG Gold : Silver -> tables metier de l'Indice Grisaille.

Declenche automatiquement par le DAG Silver (TriggerDagRun) avec son scope :
  - scope=live (cycle 15 min) : ne reconstruit que live_status ;
  - scope=full (apres l'ingestion batch quotidienne) : recalcule toutes les
    tables (overwrite, toujours idempotent).
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id="gold_grisaille",
    schedule=None,  # declenche par silver_observations
    start_date=pendulum.datetime(2026, 8, 1, tz="Europe/Paris"),
    catchup=False,
    max_active_runs=1,
    params={"scope": "live"},
    tags=["gold", "spark"],
    doc_md=__doc__,
)
def gold_grisaille():
    SparkSubmitOperator(
        task_id="build_gold",
        application="/opt/airflow/spark/gold_job.py",
        conn_id="spark_default",
        name="gold-grisaille",
        conf={
            "spark.cores.max": "1",
            "spark.executor.memory": "1g",
        },
        application_args=["--scope", "{{ params.scope }}"],
        verbose=False,
    )


gold_grisaille()
