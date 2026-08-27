"""Job Spark Gold : confrontation du datalake avec les references officielles.

Tables produites (Parquet sur HDFS, /datalake/gold/) :
  - live_vs_official  : par ville et par jour, ecart entre le flux Open-Meteo
                        agrege au jour et l'observation officielle Meteo-France
                        horaire agregee au jour (convention journee
                        climatologique : la pluie court de 06h01 a 06h00 UTC
                        affectee a J, les autres parametres de 00h01 a 00h00,
                        le pas 00h appartenant au jour precedent).
  - live_reliability  : synthese par ville (MAE / biais temperature, MAE
                        pluie) — "le live dit-il vrai ?".
  - episodes_vigilance: les episodes detectes (table Gold episodes) croises
                        avec les archives de vigilance Meteo-France : taux de
                        jours couverts par une vigilance jaune ou plus du bon
                        phenomene, statut confirme / non_confirme /
                        hors_archive (l'archive demarre le 2022-11-28).

Chaque table est construite si ses sources existent, sinon elle est sautee :
le job reste relançable pendant que les ingestions amont se mettent en place.
"""
from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS = "hdfs://namenode:8020"
SILVER_HOURLY = f"{HDFS}/datalake/silver/observations_hourly"
SILVER_OBS = f"{HDFS}/datalake/silver/observations"
SILVER_VIGILANCE = f"{HDFS}/datalake/silver/vigilance"
GOLD = f"{HDFS}/datalake/gold"

# memes referentiels que gold_job.py (jobs spark-submit autonomes)
CITIES = {
    "75": "Paris", "69": "Lyon", "13": "Marseille", "31": "Toulouse",
    "06": "Nice", "44": "Nantes", "67": "Strasbourg", "33": "Bordeaux",
    "59": "Lille", "35": "Rennes",
}
ALTI_MAX_STATION = 300.0  # m — cf. gold_job.py (stations alpines du 06)

# journees completes uniquement : au moins 20 h officielles / 80 quarts d'heure
MIN_HEURES_OFF = 20
MIN_POINTS_LIVE = 80

VIGILANCE_DEBUT = "2022-11-28"  # premier jour de l'archive vigilance
# type d'episode -> phenomene vigilance (1 vent violent, 2 pluie-inondation,
# 7 grand froid)
EPISODE_PHENOMENE = {"coup_de_vent": 1, "episode_pluvieux": 2, "vague_de_froid": 7}
COULEURS = {1: "vert", 2: "jaune", 3: "orange", 4: "rouge"}


def read_or_none(spark, path):
    try:
        return spark.read.parquet(path)
    except AnalysisException:
        print(f"source absente, table sautee : {path}")
        return None


def day_temp(ts):
    """Jour climatologique des parametres hors pluie : le pas 00h va a J-1."""
    return F.to_date(ts - F.expr("INTERVAL 1 MINUTE"))


def day_rr(ts):
    """Jour climatologique de la pluie : 06h01 -> 06h00 UTC affecte a J."""
    return F.to_date(ts - F.expr("INTERVAL 361 MINUTES"))


def build_official(hourly):
    """Agregat quotidien officiel par departement, aux conventions MF."""
    off = hourly.filter(F.col("elevation") <= ALTI_MAX_STATION)

    # moyenne par station-jour puis par departement (comme grisaille_daily)
    temp = (
        off.withColumn("obs_day", day_temp(F.col("obs_ts")))
        .groupBy("dept", "station_id", "obs_day")
        .agg(F.avg("temp").alias("t"), F.count("temp").alias("nh"))
        .filter(F.col("nh") >= MIN_HEURES_OFF)
        .groupBy("dept", "obs_day")
        .agg(F.avg("t").alias("temp_off"), F.max("nh").alias("n_heures"))
    )
    rain = (
        off.withColumn("obs_day", day_rr(F.col("obs_ts")))
        .groupBy("dept", "station_id", "obs_day")
        .agg(F.sum("precip_mm").alias("p"), F.count("precip_mm").alias("nh"))
        .filter(F.col("nh") >= MIN_HEURES_OFF)
        .groupBy("dept", "obs_day")
        .agg(F.avg("p").alias("precip_off"))
    )
    return temp.join(rain, ["dept", "obs_day"], "left")


def build_live(obs):
    """Flux Open-Meteo (pas 15 min) agrege au jour, memes conventions."""
    om = obs.filter(F.col("source") == "openmeteo")
    temp = (
        om.withColumn("obs_day", day_temp(F.col("obs_time")))
        .groupBy("dept", "obs_day")
        .agg(F.avg("temperature").alias("temp_live"),
             F.count("temperature").alias("n_points"))
        .filter(F.col("n_points") >= MIN_POINTS_LIVE)
    )
    # "precipitation" Open-Meteo = cumul de l'heure precedente : chaque heure
    # est comptee ~4 fois au pas 15 min -> somme / 4 ~ cumul du jour
    rain = (
        om.withColumn("obs_day", day_rr(F.col("obs_time")))
        .groupBy("dept", "obs_day")
        .agg((F.sum("precipitation_mm") / 4).alias("precip_live"))
    )
    return temp.join(rain, ["dept", "obs_day"], "left")


def build_live_vs_official(spark):
    hourly = read_or_none(spark, SILVER_HOURLY)
    obs = read_or_none(spark, SILVER_OBS)
    if hourly is None or obs is None:
        return False

    city_map = F.create_map(*[F.lit(x) for kv in CITIES.items() for x in kv])
    vs = (
        build_live(obs)
        .join(build_official(hourly), ["dept", "obs_day"], "inner")
        .withColumn("city", city_map[F.col("dept")])
        .withColumn("ecart_temp", F.round(F.col("temp_live") - F.col("temp_off"), 2))
        .withColumn("ecart_precip", F.round(F.col("precip_live") - F.col("precip_off"), 2))
        .select(
            "city", "dept", "obs_day",
            F.round("temp_live", 2).alias("temp_live"),
            F.round("temp_off", 2).alias("temp_off"),
            "ecart_temp",
            F.round("precip_live", 2).alias("precip_live"),
            F.round("precip_off", 2).alias("precip_off"),
            "ecart_precip", "n_heures", "n_points",
        )
    ).cache()

    vs.coalesce(1).write.mode("overwrite").parquet(f"{GOLD}/live_vs_official")

    reliability = vs.groupBy("city", "dept").agg(
        F.round(F.avg(F.abs("ecart_temp")), 2).alias("mae_temp"),
        F.round(F.avg("ecart_temp"), 2).alias("biais_temp"),
        F.round(F.avg(F.abs("ecart_precip")), 2).alias("mae_precip"),
        F.count("*").alias("n_jours_compares"),
    )
    reliability.coalesce(1).write.mode("overwrite").parquet(f"{GOLD}/live_reliability")
    vs.unpersist()
    return True


def build_episodes_vigilance(spark):
    episodes = read_or_none(spark, f"{GOLD}/episodes")
    vigilance = read_or_none(spark, SILVER_VIGILANCE)
    if episodes is None or vigilance is None:
        return False

    # 1 ligne par (jour, dept, phenomene) avec la couleur max du jour
    vig = (
        vigilance.groupBy("vig_date", "dept", "phenomenon_id")
        .agg(F.max("color_id").alias("color_id"))
        .withColumnRenamed("vig_date", "jour")
    )

    phen_map = F.create_map(*[F.lit(x) for kv in EPISODE_PHENOMENE.items() for x in kv])
    ep = (
        episodes.withColumn("episode_id", F.monotonically_increasing_id())
        .withColumn("phenomenon_id", phen_map[F.col("type")])
    )
    # jours dont une carte de vigilance est effectivement ingeree : un episode
    # sans aucun jour couvert par l'archive locale sera "non_evalue", pas
    # "non_confirme"
    dates_ingerees = vigilance.select(F.col("vig_date").alias("jour")).distinct() \
        .withColumn("jour_evaluable", F.lit(1))

    days = (
        ep.withColumn("jour", F.explode(F.sequence("date_debut", "date_fin")))
        .join(vig, ["jour", "dept", "phenomenon_id"], "left")
        .join(dates_ingerees, ["jour"], "left")
    )
    couverture = days.groupBy("episode_id").agg(
        F.sum((F.col("color_id") >= 2).cast("int")).alias("jours_vigilance"),
        F.max("color_id").alias("couleur_max"),
        F.sum("jour_evaluable").alias("jours_evaluables"),
    )

    couleur_map = F.create_map(*[F.lit(x) for kv in COULEURS.items() for x in kv])
    out = (
        ep.join(couverture, "episode_id", "left")
        .withColumn("jours_vigilance", F.coalesce("jours_vigilance", F.lit(0)))
        .withColumn(
            "taux_recouvrement",
            F.round(F.col("jours_vigilance") / F.col("duree_jours"), 2),
        )
        .withColumn("couleur_max", couleur_map[F.col("couleur_max")])
        .withColumn(
            "statut",
            F.when(F.col("date_fin") < F.lit(VIGILANCE_DEBUT), "hors_archive")
            .when(F.coalesce(F.col("jours_evaluables"), F.lit(0)) == 0, "non_evalue")
            .when(F.col("taux_recouvrement") >= 0.5, "confirme")
            .otherwise("non_confirme"),
        )
        .select(
            "dept", "city", "type", "date_debut", "date_fin", "duree_jours",
            "intensite", "jours_vigilance", "taux_recouvrement", "couleur_max",
            "statut",
        )
    )
    out.coalesce(1).write.mode("overwrite").parquet(f"{GOLD}/episodes_vigilance")
    return True


def main() -> None:
    spark = (
        SparkSession.builder.appName("gold-confrontation")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    built = []
    if build_live_vs_official(spark):
        built += ["live_vs_official", "live_reliability"]
    if build_episodes_vigilance(spark):
        built += ["episodes_vigilance"]

    for name in built:
        n = spark.read.parquet(f"{GOLD}/{name}").count()
        print(f"gold {name}: {n} lignes")
    spark.stop()


if __name__ == "__main__":
    main()
