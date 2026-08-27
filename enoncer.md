# TP Big Data : DataLake / DataLakehouse

## Objectif

Concevoir et implémenter un datalake en architecture Bronze → Silver → Gold, ingérant des données depuis au moins deux sources hétérogènes, dont une en temps réel, persistant chaque couche sur HDFS, avec un pipeline orchestré de bout en bout.

## 1. Ingestion

- **Source temps réel** : un flux (API, webhook, RSS…) publié sur un topic Kafka, consommé en continu par Spark Structured Streaming, écrit directement en Bronze.
- **Sources batch** : archives déposées en Bronze selon une convention de partitionnement (*source=X/year=YYYY/month=MM* par ex.), avec un marker d'idempotence (*_SUCCESS*) pour ne jamais re-ingérer un lot déjà présent.
- Bronze conserve le format brut d'origine, sans transformation.

## 2. Persistance & traitement (Bronze → Silver → Gold)

- **Silver** : job Spark qui valide le schéma, déduplique, et normalise chaque source vers un modèle commun ; écrit en Parquet partitionné sur HDFS.
- **Gold** : job Spark qui calcule les agrégations/KPIs métier à partir de Silver ; écrit en Parquet sur HDFS, prêt à être lu tel quel pour la restitution.
- Aucune étape manuelle : que des jobs déclenchés depuis Airflow.

## 3. Orchestration (Airflow)

- Un DAG par couche minimum (ingestion batch, Silver, Gold), avec dépendances explicites entre eux.
- Un DAG interrompu doit pouvoir être relancé sans dupliquer les données déjà traitées !!

## 4. Insights

- Notebook Python (Pandas) lisant directement les tables Gold (Parquet) depuis HDFS, produisant les visualisations et KPIs, ou bien un dashboard frontend.

## Bonus Machine Learning

Entraîner un modèle (classification, clustering, prédiction selon le thème) à partir de Silver ou Gold. Non requis pour valider le TP.

## Thèmes proposés

Choisissez un thème ci-dessous, ou proposez le vôtre (même contrainte : une source batch + une source temps réel).

### Wikipédia

- **Batch** : dumps Wikipedia (XML articles) ou Wikidata (JSON).
- **Temps réel** : Wikimedia EventStreams, flux SSE des éditions en direct.

### Mobilité urbaine

- **Batch** : Citibike Trip Histories, archives CSV mensuelles des trajets vélo-partage depuis 2013.
- **Temps réel** : Citibike GBFS, statut des stations en direct.

### Cryptomonnaies & marchés

- **Batch** : Binance Public Data
- **Temps réel** : CoinGecko API (cours) ou Reddit/PRAW (r/CryptoCurrency, r/wallstreetbets).

### Météo & climat

- **Batch** : NOAA/NCEI, historique CSV par station.
- **Temps réel** : Open-Meteo, API météo courante, sans clé.

### Espace & exoplanètes

- **Batch** : NASA Exoplanet Archive (CSV) ou dataset externe.
- **Temps réel** : NASA NeoWs, objets géocroiseurs, approches en direct.

### Activité GitHub

- **Batch** : GH Archive, événements horaires (JSON compressé).
- **Temps réel** : GitHub Events API, polling des évènements publics.

### Projet capteurs IoT

Vous pouvez aussi utiliser des données entièrement générées par vos soins, aucune source externe, avec un producteur Kafka simulant des capteurs IoT pour le flux temps réel.

*Exemple : usine automatisée, chaînes de production et réseau électrique.*

- **Batch** : le même simulateur écrit en parallèle un export agrégé périodique (chaque minute), totaux et moyennes par machine sur la période, déposé en Bronze comme un lot.
- **Temps réel** : producteur Kafka simulant chaque machine (foreuse, fondeur, assembleur, constructeur...) avec débit produit/min, consommation électrique instantanée et statut, plus un flux réseau électrique global, production vs consommation, avec risque de délestage si la conso dépasse la capacité.
