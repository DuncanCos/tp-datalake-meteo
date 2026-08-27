"""DAG d'ingestion des grilles SIM2 (SAFRAN-ISBA) -> Bronze HDFS.

Analyses de surface Meteo-France sur une grille 8x8 km (~9 900 mailles
metropole, Lambert-II etendu), publiees sur meteo.data.gouv.fr :
  - la grille de coordonnees (LAMBX/LAMBY -> lat/lon), figee, ingeree 1 fois ;
  - MENS_SIM2_{YYYY}.csv.gz : cumuls/moyennes mensuels depuis 1958 (~4 Mo/an).
    Les annees passees sont figees (ingerees 1 fois), l'annee courante est
    reecrite par Meteo-France -> un snapshot par jour logique ;
  - QUOT_SIM2_latest.csv.gz : les ~60 derniers jours au pas quotidien avec
    vent/humidite/rayonnement, reecrit chaque jour -> snapshot quotidien.

Params :
  - annee_debut : premiere annee mensuelle a ingerer (backfill complet :
    relancer une fois avec annee_debut=1958).
  - force : true pour re-ingerer un lot deja present.

Chaine Silver/Gold dediee (sim_silver_gold), separee du cycle 15 min.
"""
from __future__ import annotations

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

SIM_MENS_URL = "https://meteofrance.s3.sbg.io.cloud.ovh.net/data/REF_CC/SIM_MENS"
SIM_QUOT_URL = "https://meteofrance.s3.sbg.io.cloud.ovh.net/data/REF_CC/SIM"
GRID_FILE = "coordonnees_grille_safran_lambert-2-etendu.csv"

WEBHDFS_URL = "http://namenode:9870"
BRONZE_ROOT = "/datalake/bronze/source=sim"


@dag(
    dag_id="ingest_sim",
    # apres la mise a jour quotidienne des fichiers SIM2 (~08h20 UTC)
    schedule="30 10 * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz="Europe/Paris"),
    catchup=False,
    params={"annee_debut": 2020, "force": False},
    tags=["bronze", "sim2", "batch"],
    doc_md=__doc__,
)
def ingest_sim():

    @task
    def plan_lots(**context) -> list[dict]:
        today = context["data_interval_end"].date()
        annee_debut = int(context["params"]["annee_debut"])

        lots: list[dict] = [{"kind": "grille", "year": None, "snapshot": None}]
        for y in range(annee_debut, today.year + 1):
            # annee courante : fichier reecrit par Meteo-France -> snapshot
            snap = today.isoformat() if y == today.year else None
            lots.append({"kind": "mens", "year": y, "snapshot": snap})
        lots.append({"kind": "quot_latest", "year": None, "snapshot": today.isoformat()})
        return lots

    @task
    def ingest_lot(kind: str, year: int | None, snapshot: str | None, **context) -> str:
        from hdfs import InsecureClient

        if kind == "grille":
            filename = GRID_FILE
            url = f"{SIM_QUOT_URL}/{GRID_FILE}"
            lot_dir = f"{BRONZE_ROOT}/kind=grille"
        elif kind == "mens":
            filename = f"MENS_SIM2_{year}.csv.gz"
            url = f"{SIM_MENS_URL}/{filename}"
            lot_dir = f"{BRONZE_ROOT}/kind=mens/year={year}"
            if snapshot:
                lot_dir += f"/snapshot={snapshot}"
        else:  # quot_latest
            filename = "QUOT_SIM2_latest.csv.gz"
            url = f"{SIM_QUOT_URL}/{filename}"
            lot_dir = f"{BRONZE_ROOT}/kind=quot_latest/snapshot={snapshot}"
        marker = f"{lot_dir}/_SUCCESS"

        client = InsecureClient(WEBHDFS_URL, user="root")
        if client.status(marker, strict=False) and not context["params"].get("force"):
            raise AirflowSkipException(f"lot deja ingere : {lot_dir}")

        resp = requests.get(url, stream=True, timeout=600)
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

    # all_done : Silver/Gold tournent aussi quand tous les lots sont skippes
    trigger_sim_silver_gold = TriggerDagRunOperator(
        task_id="trigger_sim_silver_gold",
        trigger_dag_id="sim_silver_gold",
        trigger_rule="all_done",
    )
    lots >> trigger_sim_silver_gold


ingest_sim()
