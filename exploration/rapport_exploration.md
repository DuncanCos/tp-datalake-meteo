# Rapport d'exploration des données (échantillons)

Échantillons analysés le 27/08/2026 :
- **Batch Météo-France** : fichiers `Q_{dept}_latest-2025-2026_RR-T-Vent.csv.gz` pour les 10 départements cibles (75, 69, 13, 31, 06, 44, 67, 33, 59, 35) + `autres-parametres` pour le 75 — focus **janvier 2026**.
- **Temps réel Open-Meteo** : 5 minutes de capture (10 polls espacés de 30 s, 10 villes) → `data_sample/openmeteo/current_capture.jsonl`.

## 1. Batch Météo-France (quotidien)

**Volumétrie (10 départements, 2025 → 26/08/2026)** : 110 481 lignes, 61 colonnes, **188 stations**.
Janvier 2026 : 5 796 lignes (188 stations × 31 jours), de 6 stations (Paris) à 40 (Alpes-Maritimes).

**Structure** : 1 ligne = 1 station × 1 jour. Clé : `NUM_POSTE` + `AAAAMMJJ`. Chaque mesure a son **code qualité** (`RR;QRR`, `TN;QTN`…) — en janvier 2026, ~100 % des valeurs présentes sont en code `1` (validées). Format à parser : séparateur `;`, dates `AAAAMMJJ` en entier.

**Taux de remplissage (janvier 2026)** : RR 96,7 %, TN/TX 97,9 %, TM 94,7 %, DG 90,5 % — mais **vent ~45 %** (toutes les stations ne sont pas équipées d'anémomètre). → En Silver, prévoir des colonnes nullable et/ou filtrer par disponibilité capteur.

**Cohérence des valeurs (janvier 2026)** : TN min −16,7 °C (arrière-pays niçois), TX max 19 °C, RR max 128 mm/j, rafales max 41 m/s (~148 km/h). Vague de froid visible début janvier (ex. Paris-Montsouris : TN −4,4 °C le 5/01).

**Fichier `autres-parametres`** : ensoleillement (INST), rayonnement (GLOT), humidité (UM/UN/UX), pression (PMERM), occurrences neige/brouillard/orage — mais remplissage faible (16-33 % des stations du 75). Utile en option, pas comme socle.

## 2. Temps réel Open-Meteo

**Capture** : 100 enregistrements (10 polls × 10 villes), fenêtre 07:41 → 07:46 UTC, **0 échec, 0 valeur manquante** sur les 15 variables.

**Structure** : JSON par ville — `time` (horodatage modèle, pas de 15 min), température, ressenti, humidité, précipitations (precipitation/rain/showers/snowfall), `weather_code` (WMO), nébulosité, pression (msl + surface), vent (vitesse/direction/rafales), `is_day`, élévation.

**Fraîcheur** : sur 5 min, chaque ville a vu **2 horodatages modèle distincts** (bascule de quart d'heure observée) → un poll toutes les 30-60 s suffit ; les doublons de `time` devront être **dédupliqués en Silver** (clé ville + `time`).

**Enrichissement fait à l'ingestion** : `ingested_at`, `city`, `dept` ajoutés par le producteur → la jointure avec Météo-France se fera par `dept` (et/ou station la plus proche par lat/lon).

## 3. Modèle commun envisageable (Silver)

| Champ commun | Météo-France (quotidien) | Open-Meteo (instantané) |
|---|---|---|
| station/ville, dept | NUM_POSTE, NOM_USUEL, DEPT | city, dept |
| lat, lon, alti | LAT, LON, ALTI | latitude, longitude, elevation |
| horodatage | AAAAMMJJ (jour) | time (15 min) |
| température | TN, TX, TM | temperature_2m (à agréger min/max/moy par jour) |
| précipitations | RR (mm/24h) | precipitation (mm/h) |
| vent moyen / rafales | FFM, FXY/FXI | wind_speed_10m, wind_gusts_10m |
| humidité | UM (autres-param.) | relative_humidity_2m |
| pression | PMERM (autres-param.) | pressure_msl |

Granularités différentes (jour vs 15 min) → Gold agrège le temps réel au jour pour comparer.

## 4. Insights candidats (Gold / notebook)

1. **Temps réel vs historique** : la journée en cours (agrégat Open-Meteo) comparée à la distribution historique Météo-France du même mois — anomalie de température, percentile, records battus.
2. **KPIs quotidiens par ville/département** : min/max/moyenne température, cumul pluie, rafale max, jours de gel — série temporelle et classement des villes.
3. **Comparaison inter-villes / gradient climatique** : Lille vs Nice (amplitude, ensoleillement, pluie), carte des écarts.
4. **Vague de froid / canicule** : détection d'épisodes (N jours consécutifs sous/au-dessus d'un seuil) — la vague de froid de janvier 2026 est déjà visible dans l'échantillon.
5. **Fiabilité de la mesure temps réel** : écart entre l'agrégat journalier Open-Meteo et la valeur officielle Météo-France à J+1 pour la même zone (qualité du modèle vs observation).
6. **Bonus ML** : prédire TX du lendemain par ville à partir de l'historique (features : saison, tendance, pression, vent).

## 5. Points d'attention pour le pipeline

- Séparateur `;`, encodage, `AAAAMMJJ` entier, décimales avec point → parsing Spark à cadrer.
- Codes qualité : ne garder en Silver que `Q ∈ {0, 1, 9}` (exclure `2` = douteuse), ou tracer un flag.
- Vent manquant sur ~55 % des stations batch → choisir les stations "complètes" par ville de référence.
- Fichiers `latest-2025-2026` réécrits quotidiennement → l'idempotence batch se gère par **période + date de téléchargement** (marker `_SUCCESS` par lot).
- PowerShell locale fr : formater les floats en culture invariante dans les scripts (bug rencontré et corrigé dans le producteur de test).
