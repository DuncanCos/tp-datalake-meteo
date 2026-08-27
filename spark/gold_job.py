"""Job Spark Gold : Silver -> tables metier de l'Indice Grisaille.

Tables produites (Parquet sur HDFS, /datalake/gold/) :
  - grisaille_daily   : par ville et par jour (source officielle Meteo-France,
                        stations agregees au departement) : meteo du jour +
                        indice grisaille 0-100 et ses composantes.
  - grisaille_ranking : classement mensuel des villes par grisaille moyenne.
  - episodes          : episodes detectes par ville (vague de froid, episode
                        pluvieux, coup de vent) avec duree et intensite.
  - live_status       : dernier quart d'heure Open-Meteo par ville + indice
                        grisaille "en direct" et classement instantane.

L'indice grisaille (0 = ciel radieux, 100 = misere absolue) est une moyenne
ponderee de 5 composantes normalisees 0-1 :
  froid 30% | manque de soleil 25% | pluie 25% | vent 10% | humidite 10%
Seuils documentes dans les constantes ci-dessous — pondere, assume, goofy.
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

HDFS = "hdfs://namenode:8020"
SILVER = f"{HDFS}/datalake/silver/observations"
GOLD = f"{HDFS}/datalake/gold"

CITIES = {
    "75": "Paris", "69": "Lyon", "13": "Marseille", "31": "Toulouse",
    "06": "Nice", "44": "Nantes", "67": "Strasbourg", "33": "Bordeaux",
    "59": "Lille", "35": "Rennes",
}

# ponderations de l'indice
W_FROID, W_SOLEIL, W_PLUIE, W_VENT, W_HUMIDITE = 0.30, 0.25, 0.25, 0.10, 0.10
# seuils de normalisation
TEMP_CONFORT = 15.0   # degC : au-dessus, composante froid = 0
TEMP_GLACIAL = -5.0   # degC : en-dessous, composante froid = 1
PLUIE_MAX = 20.0      # mm/jour satures
SOLEIL_PLEIN = 360.0  # minutes d'insolation = journee lumineuse
VENT_MAX = 10.0       # m/s de vent moyen
GUST_COUP_DE_VENT = 20.0  # m/s de rafale -> episode "coup de vent"
# le departement 06 contient des stations alpines a +2000 m : sans ce filtre,
# "Nice" subit des vagues de froid de 141 jours. On ne garde que les stations
# de plaine, representatives de la ville.
ALTI_MAX_STATION = 300.0  # m


def clamp01(col):
    return F.least(F.greatest(col, F.lit(0.0)), F.lit(1.0))


def grisaille(froid, soleil, pluie, vent, humidite):
    score = (
        W_FROID * froid + W_SOLEIL * soleil + W_PLUIE * pluie
        + W_VENT * vent + W_HUMIDITE * humidite
    )
    return F.round(score * 100, 1)


def build_daily(silver):
    """Agregat officiel Meteo-France par departement/ville et par jour."""
    mf = silver.filter(
        (F.col("source") == "meteofrance")
        & (F.col("elevation") <= ALTI_MAX_STATION)
    )
    daily = mf.groupBy("dept", "obs_date").agg(
        F.avg("temperature").alias("temp_avg"),
        F.min("temp_min").alias("temp_min"),
        F.max("temp_max").alias("temp_max"),
        F.avg("precipitation_mm").alias("precip_mm"),
        F.avg("wind_speed_ms").alias("wind_ms"),
        F.max("wind_gust_ms").alias("gust_ms"),
        F.avg("sunshine_min").alias("sunshine_min"),
        F.avg("humidity_pct").alias("humidity_pct"),
        F.max("fog").alias("fog"),
        F.count("*").alias("n_stations"),
    )
    city_map = F.create_map(*[F.lit(x) for kv in CITIES.items() for x in kv])
    daily = daily.withColumn("city", city_map[F.col("dept")])

    daily = (
        daily
        .withColumn("c_froid", clamp01((F.lit(TEMP_CONFORT) - F.col("temp_avg"))
                                       / (TEMP_CONFORT - TEMP_GLACIAL)))
        .withColumn("c_pluie", clamp01(F.col("precip_mm") / PLUIE_MAX))
        # pas d'heliometre -> on retombe sur le brouillard comme indice de gris
        .withColumn("c_soleil", clamp01(F.coalesce(
            1 - F.col("sunshine_min") / SOLEIL_PLEIN, F.col("fog"), F.lit(0.5))))
        .withColumn("c_vent", clamp01(F.col("wind_ms") / VENT_MAX))
        .withColumn("c_humidite", clamp01((F.col("humidity_pct") - 60) / 40))
    )
    return daily.withColumn(
        "grisaille",
        grisaille(F.col("c_froid"), F.col("c_soleil"), F.col("c_pluie"),
                  F.col("c_vent"), F.coalesce(F.col("c_humidite"), F.lit(0.0))),
    ).withColumn("year", F.year("obs_date")).withColumn("month", F.month("obs_date"))


def build_ranking(daily):
    monthly = daily.groupBy("year", "month", "dept", "city").agg(
        F.round(F.avg("grisaille"), 1).alias("grisaille_moy"),
        F.round(F.avg("temp_avg"), 1).alias("temp_moy"),
        F.round(F.sum("precip_mm"), 1).alias("precip_cumul_mm"),
        F.round(F.avg("sunshine_min"), 0).alias("soleil_moy_min"),
        F.count("*").alias("n_jours"),
    )
    w = Window.partitionBy("year", "month").orderBy(F.desc("grisaille_moy"))
    return monthly.withColumn("rang_misere", F.rank().over(w))


def detect_episodes(daily):
    """Gaps-and-islands : suites de jours consecutifs verifiant un critere."""
    def runs(flagged, ep_type, min_days, intensity_col, intensity_agg):
        w = Window.partitionBy("dept").orderBy("obs_date")
        islands = (
            flagged.withColumn("rn", F.row_number().over(w))
            .withColumn("grp", F.datediff("obs_date", F.lit("1970-01-01")) - F.col("rn"))
        )
        return (
            islands.groupBy("dept", "city", "grp")
            .agg(
                F.min("obs_date").alias("date_debut"),
                F.max("obs_date").alias("date_fin"),
                F.count("*").alias("duree_jours"),
                intensity_agg(intensity_col).alias("intensite"),
            )
            .filter(F.col("duree_jours") >= min_days)
            .withColumn("type", F.lit(ep_type))
            .drop("grp")
        )

    froid = runs(daily.filter(F.col("temp_min") <= 0), "vague_de_froid", 3,
                 "temp_min", F.min)
    pluie = runs(daily.filter(F.col("precip_mm") >= 5), "episode_pluvieux", 3,
                 "precip_mm", F.sum)
    vent = runs(daily.filter(F.col("gust_ms") >= GUST_COUP_DE_VENT), "coup_de_vent", 1,
                "gust_ms", F.max)
    return (
        froid.unionByName(pluie).unionByName(vent)
        .withColumn("intensite", F.round("intensite", 1))
        .select("dept", "city", "type", "date_debut", "date_fin",
                "duree_jours", "intensite")
    )


def build_live(silver):
    om = silver.filter(F.col("source") == "openmeteo")
    w = Window.partitionBy("station_id").orderBy(F.desc("obs_time"))
    last = om.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
    last = (
        last
        .withColumn("c_froid", clamp01((F.lit(TEMP_CONFORT) - F.col("temperature"))
                                       / (TEMP_CONFORT - TEMP_GLACIAL)))
        # en direct : la nebulosite remplace l'heliometre
        .withColumn("c_soleil", clamp01(F.col("cloud_cover_pct") / 100))
        .withColumn("c_pluie", clamp01(F.col("precipitation_mm") / 2))
        .withColumn("c_vent", clamp01(F.col("wind_speed_ms") / VENT_MAX))
        .withColumn("c_humidite", clamp01((F.col("humidity_pct") - 60) / 40))
        .withColumn("grisaille_live", grisaille(
            F.col("c_froid"), F.col("c_soleil"), F.col("c_pluie"),
            F.col("c_vent"), F.col("c_humidite")))
    )
    rank_w = Window.orderBy(F.desc("grisaille_live"))
    return last.select(
        F.col("station_id").alias("city"), "dept", "obs_time",
        "temperature", "precipitation_mm", "wind_speed_ms", "wind_gust_ms",
        "cloud_cover_pct", "humidity_pct", "weather_code",
        "c_froid", "c_soleil", "c_pluie", "c_vent", "c_humidite",
        "grisaille_live",
    ).withColumn("rang_misere_live", F.rank().over(rank_w))


def main() -> None:
    spark = (
        SparkSession.builder.appName("gold-grisaille")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    silver = spark.read.parquet(SILVER)

    daily = build_daily(silver).cache()
    daily.write.mode("overwrite").partitionBy("year").parquet(f"{GOLD}/grisaille_daily")
    build_ranking(daily).write.mode("overwrite").parquet(f"{GOLD}/grisaille_ranking")
    detect_episodes(daily).write.mode("overwrite").parquet(f"{GOLD}/episodes")
    build_live(silver).write.mode("overwrite").parquet(f"{GOLD}/live_status")

    for name in ["grisaille_daily", "grisaille_ranking", "episodes", "live_status"]:
        n = spark.read.parquet(f"{GOLD}/{name}").count()
        print(f"gold {name}: {n} lignes")
    spark.stop()


if __name__ == "__main__":
    main()
