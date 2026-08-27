"""DAG Gold confrontation : live vs officiel + episodes vs vigilances.

Declenche par ingest_hourly_meteofrance et ingest_vigilance (TriggerDagRun).
Le job recalcule integralement les tables live_vs_official, live_reliability
et episodes_vigilance (overwrite) : deux declenchements le meme jour restent
idempotents.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id="confrontation",
    schedule=None,  # declenche par les DAGs d'ingestion horaire et vigilance
    start_date=pendulum.datetime(2026, 8, 1, tz="Europe/Paris"),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "spark"],
    doc_md=__doc__,
)
def confrontation():
    SparkSubmitOperator(
        task_id="build_confrontation",
        application="/opt/airflow/spark/gold_confrontation_job.py",
        conn_id="spark_default",
        name="gold-confrontation",
        conf={
            "spark.cores.max": "1",
            "spark.executor.memory": "1g",
        },
        verbose=False,
    )


confrontation()
