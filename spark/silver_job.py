"""Job Spark Silver : Bronze -> modele commun -> Parquet partitionne.

- Meteo-France : parse les CSV bruts (2 familles), null-ifie les valeurs au
  code qualite 2 (douteuses), dedoublonne les snapshots (garde le plus recent
  par station/jour), joint les 2 familles -> 1 ligne par station et par jour.
- Open-Meteo : parse le JSON brut, dedoublonne (ville, horodatage modele) en
  gardant l'ingestion la plus recente, convertit le vent km/h -> m/s.
- Les deux sources sont unies dans un schema commun et ecrites en Parquet
  partitionne par (source, year, month). Ecriture en mode dynamic partition
  overwrite : rejouer le job est idempotent.

Args : --date-debut / --date-fin (YYYY-MM-DD, optionnels) filtrent obs_date.
       --sources openmeteo|meteofrance|all : quelles sources retraiter.
       Le cycle 15 min ne retraite que le flux openmeteo (leger) ; le batch
       quotidien retraite tout. L'ecriture etant en dynamic partition
       overwrite par (source, year, month), traiter une seule source ne
       touche jamais les partitions de l'autre.
"""
import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

HDFS = "hdfs://namenode:8020"
BRONZE_MF = f"{HDFS}/datalake/bronze/source=meteofrance"
BRONZE_OM = f"{HDFS}/datalake/bronze/source=openmeteo"
SILVER_OUT = f"{HDFS}/datalake/silver/observations"

# colonnes communes du modele Silver
COMMON_COLS = [
    "source", "station_id", "station_name", "dept", "latitude", "longitude",
    "elevation", "granularity", "obs_date", "obs_time",
    "temperature", "temp_min", "temp_max", "precipitation_mm", "humidity_pct",
    "pressure_hpa", "wind_speed_ms", "wind_gust_ms", "sunshine_min",
    "frost_min", "fog", "snowfall_cm", "cloud_cover_pct", "weather_code",
    "year", "month",
]


def qval(col: str, qcol: str):
    """Valeur gardee sauf si le code qualite Meteo-France vaut 2 (douteuse)."""
    return F.when(F.col(qcol).isNull() | (F.col(qcol) != 2), F.col(col).cast("double"))


def read_mf_family(spark: SparkSession, family: str):
    df = (
        spark.read.option("header", True)
        .option("sep", ";")
        .option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.csv.gz")
        .csv(f"{BRONZE_MF}/dept=*/period=*/family={family}")
    )
    # snapshot extrait du chemin (periodes figees : pas de snapshot -> epoque 0)
    df = df.withColumn(
        "snapshot",
        F.coalesce(
            F.regexp_extract(F.input_file_name(), r"snapshot=(\d{4}-\d{2}-\d{2})", 1),
            F.lit("1900-01-01"),
        ),
    ).withColumn("obs_date", F.to_date(F.col("AAAAMMJJ"), "yyyyMMdd"))
    # dedup snapshots : la ligne du snapshot le plus recent gagne
    w = Window.partitionBy("NUM_POSTE", "obs_date").orderBy(F.col("snapshot").desc())
    return df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")


def build_meteofrance(spark: SparkSession):
    rr = read_mf_family(spark, "RR-T-Vent").select(
        F.col("NUM_POSTE").alias("station_id"),
        F.col("NOM_USUEL").alias("station_name"),
        F.col("LAT").cast("double").alias("latitude"),
        F.col("LON").cast("double").alias("longitude"),
        F.col("ALTI").cast("double").alias("elevation"),
        "obs_date",
        qval("TM", "QTM").alias("temperature"),
        qval("TN", "QTN").alias("temp_min"),
        qval("TX", "QTX").alias("temp_max"),
        qval("RR", "QRR").alias("precipitation_mm"),
        qval("FFM", "QFFM").alias("wind_speed_ms"),
        qval("FXI", "QFXI").alias("wind_gust_ms"),
        qval("DG", "QDG").alias("frost_min"),
    )
    autres = read_mf_family(spark, "autres-parametres").select(
        F.col("NUM_POSTE").alias("station_id"),
        "obs_date",
        qval("INST", "QINST").alias("sunshine_min"),
        qval("UM", "QUM").alias("humidity_pct"),
        qval("PMERM", "QPMERM").alias("pressure_hpa"),
        qval("BROU", "QBROU").alias("fog"),
        qval("HNEIGEF", "QHNEIGEF").alias("snowfall_cm"),
    )
    mf = rr.join(autres, ["station_id", "obs_date"], "left")
    return mf.select(
        F.lit("meteofrance").alias("source"),
        "station_id",
        "station_name",
        F.substring("station_id", 1, 2).alias("dept"),
        "latitude", "longitude", "elevation",
        F.lit("daily").alias("granularity"),
        "obs_date",
        F.lit(None).cast("timestamp").alias("obs_time"),
        "temperature", "temp_min", "temp_max", "precipitation_mm",
        "humidity_pct", "pressure_hpa", "wind_speed_ms", "wind_gust_ms",
        "sunshine_min", "frost_min", "fog", "snowfall_cm",
        F.lit(None).cast("double").alias("cloud_cover_pct"),
        F.lit(None).cast("int").alias("weather_code"),
    )


def build_openmeteo(spark: SparkSession):
    df = spark.read.json(f"{BRONZE_OM}/ingest_date=*/part-*.txt")
    df = df.withColumn("obs_time", F.to_timestamp("time", "yyyy-MM-dd'T'HH:mm"))
    # dedup : plusieurs polls retombent sur le meme quart d'heure modele
    w = Window.partitionBy("city", "obs_time").orderBy(F.col("ingested_at").desc())
    df = df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
    return df.select(
        F.lit("openmeteo").alias("source"),
        F.col("city").alias("station_id"),
        F.col("city").alias("station_name"),
        "dept",
        F.col("latitude").cast("double"),
        F.col("longitude").cast("double"),
        F.col("elevation").cast("double"),
        F.lit("15min").alias("granularity"),
        F.to_date("obs_time").alias("obs_date"),
        "obs_time",
        F.col("temperature_2m").cast("double").alias("temperature"),
        F.lit(None).cast("double").alias("temp_min"),
        F.lit(None).cast("double").alias("temp_max"),
        F.col("precipitation").cast("double").alias("precipitation_mm"),
        F.col("relative_humidity_2m").cast("double").alias("humidity_pct"),
        F.col("pressure_msl").cast("double").alias("pressure_hpa"),
        (F.col("wind_speed_10m") / 3.6).alias("wind_speed_ms"),   # km/h -> m/s
        (F.col("wind_gusts_10m") / 3.6).alias("wind_gust_ms"),
        F.lit(None).cast("double").alias("sunshine_min"),
        F.lit(None).cast("double").alias("frost_min"),
        F.col("weather_code").isin(45, 48).cast("double").alias("fog"),
        F.col("snowfall").cast("double").alias("snowfall_cm"),
        F.col("cloud_cover").cast("double").alias("cloud_cover_pct"),
        F.col("weather_code").cast("int").alias("weather_code"),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date-debut", default="")
    parser.add_argument("--date-fin", default="")
    parser.add_argument("--sources", default="all",
                        choices=["openmeteo", "meteofrance", "all"])
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("silver-observations")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    parts = []
    if args.sources in ("meteofrance", "all"):
        parts.append(build_meteofrance(spark))
    if args.sources in ("openmeteo", "all"):
        parts.append(build_openmeteo(spark))
    silver = parts[0]
    for p in parts[1:]:
        silver = silver.unionByName(p)

    if args.date_debut:
        silver = silver.filter(F.col("obs_date") >= F.lit(args.date_debut))
    if args.date_fin:
        silver = silver.filter(F.col("obs_date") <= F.lit(args.date_fin))

    silver = (
        silver.filter(F.col("obs_date").isNotNull())
        .withColumn("year", F.year("obs_date"))
        .withColumn("month", F.month("obs_date"))
    )

    # repartition sur les colonnes de partitionnement : 1 fichier par partition
    # (sinon chaque tache de shuffle ecrit un mini-fichier dans chaque partition)
    silver.repartition("source", "year", "month").write.mode("overwrite") \
        .partitionBy("source", "year", "month").parquet(SILVER_OUT)

    counts = spark.read.parquet(SILVER_OUT).groupBy("source").count().collect()
    for row in counts:
        print(f"silver rows {row['source']}: {row['count']}")
    spark.stop()


if __name__ == "__main__":
    main()
