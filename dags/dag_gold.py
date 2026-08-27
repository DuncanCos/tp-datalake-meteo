"""DAG Gold : Silver -> tables metier de l'Indice Grisaille.

Declenche automatiquement par le DAG Silver (TriggerDagRun). Le job recalcule
integralement les 4 tables Gold depuis Silver (overwrite) : relancer est
toujours idempotent.
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
        verbose=False,
    )


gold_grisaille()
