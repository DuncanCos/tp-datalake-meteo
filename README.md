# 🌧️ La Météo Grisaille — DataLake Bronze / Silver / Gold

TP Big Data : un datalake complet qui répond à la question essentielle :
**quelle est la ville la plus déprimante de France ?**

L'**Indice Grisaille** (0 = ciel radieux, 100 = misère absolue) combine froid (30 %),
manque de soleil (25 %), pluie (25 %), vent (10 %) et humidité (10 %), calculé chaque
jour pour 10 villes françaises — en historique depuis 2025 et **en direct**.

Restitution : un site web style « météo de Gulli » avec carte de France interactive,
heatmaps (chaleur, vent, pluie, humidité, soleil, grisaille), voyage dans le temps
jour par jour, podium mensuel de la misère et détection d'épisodes météo.

## Architecture

```
Open-Meteo API ──poll 30s──> producteur Kafka ──> topic weather.current
                                                        │
                                          Spark Structured Streaming
                                                        ▼
meteo.data.gouv.fr (CSV.gz) ──DAG Airflow──>  BRONZE (HDFS, brut, _SUCCESS)
     (10 départements × 3 périodes)                     │
                                                        ▼  DAG Silver (*/15 min)
                                          SILVER (Parquet partitionné)
                                          schéma commun, dédup, codes qualité
                                                        │
                                                        ▼  DAG Gold (déclenché)
                                          GOLD : grisaille_daily · ranking
                                                 episodes · live_status
                                                        │
                                                        ▼
                              Site web FastAPI + Leaflet (WebHDFS)
```

- **Sources hétérogènes** : [Open-Meteo](https://open-meteo.com) (JSON temps réel,
  sans clé) + [Météo-France](https://meteo.data.gouv.fr) (CSV quotidiens contrôlés
  qualité, par département).
- **Idempotence** : lots batch marqués `_SUCCESS` (re-run = skip, `force=true` pour
  ré-ingérer), fichier cumulatif `latest` snapshotté par jour logique, checkpoint
  Kafka pour le streaming, écritures Silver/Gold en *dynamic partition overwrite*.
- **Dépendances explicites** : ingestion batch → Silver → Gold via
  `TriggerDagRunOperator` ; Silver tourne aussi toutes les 15 min pour rafraîchir
  le « en direct ».

## Démarrage

```bash
docker compose up -d --build
```

Puis dans Airflow (admin/admin) : dépauser les 3 DAGs
(`ingest_batch_meteofrance`, `silver_observations`, `gold_grisaille`), ou :

```bash
docker exec datalake-meteo-airflow-scheduler-1 airflow dags unpause ingest_batch_meteofrance
docker exec datalake-meteo-airflow-scheduler-1 airflow dags unpause silver_observations
docker exec datalake-meteo-airflow-scheduler-1 airflow dags unpause gold_grisaille
docker exec datalake-meteo-airflow-scheduler-1 airflow dags trigger ingest_batch_meteofrance
```

Le DAG d'ingestion accepte des paramètres de plage :
`{"date_debut": "2026-01-01", "date_fin": "2026-04-30"}` (sélection automatique
des fichiers-période couvrant la plage).

## Services & ports

| Service | URL | Rôle |
|---|---|---|
| **Site web** | http://localhost:8090 | La Météo Grisaille 🌧️ |
| Airflow | http://localhost:8080 (admin/admin) | Orchestration |
| HDFS UI | http://localhost:9870 | NameNode + WebHDFS |
| Spark UI | http://localhost:8082 | Cluster standalone |
| Kafka UI | http://localhost:8085 | Topics & messages |
| Grafana | http://localhost:3000 (admin/admin) | Dashboards + logs (Loki) |
| Prometheus | http://localhost:9090 | Métriques (kafka-exporter, cAdvisor) |

## Structure

```
├── docker-compose.yml     # 18 conteneurs : data stack + observabilité + site
├── docker/                # images custom (Airflow+Spark, producteur, webapp) + conf HDFS
├── producer/              # poll Open-Meteo -> Kafka (10 villes, 30 s)
├── dags/                  # ingest batch (idempotent) -> silver -> gold
├── spark/                 # streaming_bronze, silver_job, gold_job (+ scripts de verif)
├── webapp/                # API FastAPI (lit Gold via WebHDFS) + frontend carte/heatmap
├── monitoring/            # conf Prometheus, provisioning Grafana, Promtail
├── exploration/           # analyse exploratoire des sources + rapport
├── enoncer.md             # l'énoncé du TP
└── plan.md                # le plan du projet
```

## Résultats notables

- 🥇 **Strasbourg**, ville la plus déprimante de janvier 2026 (grisaille 50,5/100)
- 🌨️ Vague de froid nationale du 24/12/2025 au 08/01/2026 détectée dans les 10 villes
  (jusqu'à −11,5 °C à Strasbourg, 16 jours à Lille)
- 🏆 Pire journée : **Lille, le 9 janvier 2026** — grisaille 81,8/100 (3,8 °C,
  17,7 mm de pluie, 0 minute de soleil, vent 10,7 m/s)
- 🏔️ Anecdote data quality : sans filtre d'altitude, « Nice » subissait une vague de
  froid de 141 jours (stations alpines du dept 06 à +2000 m) — filtre ≤ 300 m ajouté
