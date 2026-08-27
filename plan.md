# Plan — TP Big Data : DataLake météo (Bronze → Silver → Gold)

## Contexte

TP décrit dans [enoncer.md](enoncer.md) : concevoir un datalake en architecture **Bronze → Silver → Gold** sur HDFS, avec au moins 2 sources hétérogènes dont une temps réel (Kafka + Spark Structured Streaming), orchestration Airflow idempotente, et restitution via notebook Pandas.

**Thème choisi : Météo & climat.**

## Objectif du projet : « L'Indice Grisaille » 🌧️

**Élire scientifiquement la ville la plus déprimante de l'hiver français**, en combinant :

- **Indice Grisaille** (fil rouge) : score de « misère météo » par ville et par jour, combinant pluie (RR), froid (TM, jours de gel DG), vent (FFM, rafales FXY/FXI) et manque de soleil (INST). Pondérations documentées et assumées.
  - *Historique* (batch Météo-France) : classement des villes sur l'hiver 2025-2026, records, séries (jours consécutifs sans soleil, etc.).
  - *Temps réel* (flux Open-Meteo/Kafka) : score de misère en direct des 10 villes, recalculé au fil du flux.
- **Détection d'épisodes** (KPIs complémentaires) : vagues de froid (N jours consécutifs sous seuil), épisodes pluvieux (cumuls glissants), coups de vent (rafales > seuil) — intensité et durée par ville/département. La vague de froid de janvier 2026 (TN −16,7 °C) sert de cas de démonstration.
- **Bonus ML** : prédire l'indice grisaille du lendemain par ville.

## Sources de données retenues

### Source temps réel — Open-Meteo (JSON, API sans clé)

- Endpoint : `https://api.open-meteo.com/v1/forecast`
- Paramètre `current=` avec : `temperature_2m, relative_humidity_2m, apparent_temperature, precipitation, rain, snowfall, weather_code, cloud_cover, surface_pressure, wind_speed_10m, wind_direction_10m, wind_gusts_10m`
- Multi-villes en 1 seule requête : `latitude=48.85,45.76,...&longitude=2.35,4.83,...`
- Gratuit, sans clé, limite 10 000 appels/jour (1 poll toutes les 30-60 s = largement suffisant)
- Données rafraîchies par pas de 15 minutes
- **Périmètre : ~10 villes françaises** — Paris (75), Lyon (69), Marseille (13), Toulouse (31), Nice (06), Nantes (44), Strasbourg (67), Bordeaux (33), Lille (59), Rennes (35). Coordonnées + département codés en dur pour permettre la jointure avec la source batch.
- Chaîne : producteur Python (poll API → topic Kafka `weather.current`) → Spark Structured Streaming → Bronze HDFS (JSON brut, partitionné par date d'ingestion)

### Source batch — Météo-France, données climatologiques quotidiennes (CSV.gz)

- Portail : <https://meteo.data.gouv.fr> — dataset [« Données climatologiques de base - quotidiennes »](https://www.data.gouv.fr/datasets/donnees-climatologiques-de-base-quotidiennes)
- Fichiers `.csv.gz` **par département et par lots de période**, URLs stables → téléchargement scriptable depuis Airflow
- Contrôle qualité climatologique Météo-France, Licence Ouverte 2.0, mise à jour quotidienne pour les 2 dernières années
- Départements ciblés : ceux des 10 villes du flux temps réel (75, 69, 13, 31, 06, 44, 67, 33, 59, 35)
- Deux familles de fichiers par département : `RR-T-Vent` (pluie, températures, vent — socle) et `autres-parametres` (ensoleillement INST, humidité, pression — nécessaire à l'indice grisaille, remplissage partiel à gérer)
- Dépôt Bronze : `source=meteofrance/dept=XX/period=YYYY-YYYY/` + marker `_SUCCESS` (idempotence : ne jamais re-ingérer un lot déjà présent)

## Architecture cible

```
Open-Meteo API ──poll──> producteur Kafka ──> topic weather.current
                                                    │
                                     Spark Structured Streaming
                                                    ▼
data.gouv.fr CSV.gz ──DAG Airflow──>  BRONZE (HDFS, format brut)
                                                    ▼  job Spark : schéma, dédup, modèle commun
                                       SILVER (Parquet partitionné)
                                                    ▼  job Spark : KPIs / agrégations
                                        GOLD (Parquet)  ──> Notebook Pandas / dashboard
```

Stack d'exécution : **Docker Compose** (Kafka, HDFS namenode/datanode, Spark, Airflow).

## Étapes d'implémentation

1. **Infra** : `docker-compose.yml` — Kafka (KRaft), HDFS, Spark, Airflow.
2. **Ingestion temps réel** : `producer/openmeteo_producer.py` + job `spark/streaming_bronze.py`.
3. **Ingestion batch** : DAG Airflow `dags/dag_ingest_batch.py` — télécharge les CSV.gz Météo-France, dépose en Bronze avec `_SUCCESS`, skip si le lot existe déjà.
4. **Silver** : `spark/silver_job.py` — validation de schéma, déduplication, normalisation des deux sources vers un modèle commun (ville/station, département, horodatage, température, précipitations, vent, humidité, pression) ; Parquet partitionné sur HDFS.
5. **Gold** : `spark/gold_job.py` — tables métier : indice grisaille quotidien par ville (historique + agrégat du flux temps réel), classement des villes, épisodes détectés (vagues de froid, épisodes pluvieux, coups de vent) avec durée/intensité ; Parquet sur HDFS.
6. **Orchestration** : 3 DAGs minimum (ingestion batch, Silver, Gold) avec dépendances explicites, relançables sans duplication.
7. **Insights** : site web « La Météo Grisaille » (`webapp/`) — API FastAPI qui lit les Parquet Gold via WebHDFS + frontend style « météo de Gulli » (cartes live, podium de la misère, courbe 30 jours, épisodes) sur http://localhost:8090.
8. **Bonus ML** (optionnel) : prédiction de température J+1 par ville à partir de Silver/Gold.

## Vérification

- **Temps réel** : messages visibles dans le topic Kafka ; fichiers bruts qui apparaissent en Bronze au fil de l'eau.
- **Batch / idempotence** : relancer le DAG d'ingestion deux fois → aucune duplication (marker `_SUCCESS`).
- **Silver/Gold** : lecture des Parquet, vérification du schéma commun et des comptes de lignes.
- **Notebook** : KPIs et graphiques générés depuis Gold.
