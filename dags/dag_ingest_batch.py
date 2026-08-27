"""DAG d'ingestion batch Meteo-France -> Bronze HDFS.

Params (Trigger DAG w/ config) :
  - date_debut / date_fin : plage de dates voulue (YYYY-MM-DD). Le DAG ingere
    les fichiers periode qui COUVRENT cette plage (la source publie 3 fichiers
    par departement : avant-1949, previous-1950-2024, latest-2025-2026 — le
    filtrage fin des dates se fait ensuite dans le job Silver).
    date_fin vide = date logique du run (data_interval_end) -> les runs
    planifies quotidiens prennent automatiquement les donnees jusqu'a J-1.
  - force : true pour re-ingerer un lot deja present.

Idempotence :
  - periodes historiques (figees) : ingerees une fois, skippees ensuite.
  - periode latest-2025-2026 (fichier cumulatif reecrit chaque jour par
    Meteo-France) : un lot snapshot=YYYY-MM-DD par jour logique — rejouer le
    meme jour skippe, le jour suivant cree un nouveau snapshot.
"""
from __future__ import annotations

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DEPTS = ["75", "69", "13", "31", "06", "44", "67", "33", "59", "35"]
FAMILIES = ["RR-T-Vent", "autres-parametres"]

BASE_URL = "https://meteofrance.s3.sbg.io.cloud.ovh.net/data/synchro_ftp/BASE/QUOT"
WEBHDFS_URL = "http://namenode:9870"
BRONZE_ROOT = "/datalake/bronze/source=meteofrance"


@dag(
    dag_id="ingest_batch_meteofrance",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz="Europe/Paris"),
    catchup=False,
    params={"date_debut": "2025-01-01", "date_fin": "", "force": False},
    tags=["bronze", "meteofrance", "batch"],
    doc_md=__doc__,
)
def ingest_batch_meteofrance():

    @task
    def plan_lots(**context) -> list[dict]:
        """Traduit la plage [date_debut, date_fin] en liste de lots a ingerer."""
        from datetime import datetime

        p = context["params"]
        debut = datetime.strptime(p["date_debut"], "%Y-%m-%d").date()
        fin = (
            datetime.strptime(p["date_fin"], "%Y-%m-%d").date()
            if p.get("date_fin")
            else context["data_interval_end"].date()
        )
        if fin < debut:
            raise ValueError(f"date_fin {fin} < date_debut {debut}")

        periods: list[tuple[str, str | None]] = []
        if debut.year <= 1949:
            periods.append(("avant-1949", None))
        if debut.year <= 2024 and fin.year >= 1950:
            periods.append(("previous-1950-2024", None))
        if fin.year >= 2025:
            # fichier cumulatif -> un snapshot par jour logique
            snapshot = min(fin, context["data_interval_end"].date()).isoformat()
            periods.append(("latest-2025-2026", snapshot))

        return [
            {"dept": d, "family": f, "period": per, "snapshot": snap}
            for d in DEPTS
            for f in FAMILIES
            for per, snap in periods
        ]

    @task
    def ingest_lot(dept: str, family: str, period: str, snapshot: str | None, **context) -> str:
        from hdfs import InsecureClient

        client = InsecureClient(WEBHDFS_URL, user="root")
        lot_dir = f"{BRONZE_ROOT}/dept={dept}/period={period}/family={family}"
        if snapshot:
            lot_dir += f"/snapshot={snapshot}"
        marker = f"{lot_dir}/_SUCCESS"

        if client.status(marker, strict=False) and not context["params"].get("force"):
            raise AirflowSkipException(f"lot deja ingere : {lot_dir}")

        filename = f"Q_{dept}_{period}_{family}.csv.gz"
        resp = requests.get(f"{BASE_URL}/{filename}", stream=True, timeout=600)
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

    # dependance explicite entre couches : Bronze batch -> Silver.
    # all_done : Silver tourne aussi quand tous les lots sont skippes (le
    # bronze openmeteo, alimente en continu, a toujours du neuf a traiter).
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver",
        trigger_dag_id="silver_observations",
        trigger_rule="all_done",
        conf={
            "date_debut": "{{ params.date_debut }}",
            "date_fin": "{{ params.date_fin }}",
        },
    )
    lots >> trigger_silver


ingest_batch_meteofrance()
