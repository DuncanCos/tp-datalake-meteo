"""Job Spark Silver : Bronze grilles SIM2 -> Parquet partitionne.

- Grille de coordonnees : LAMBX/LAMBY (hectometres, Lambert-II etendu) ->
  lat/lon. Le CSV utilise la VIRGULE decimale -> regexp_replace avant cast.
- Mensuel (MENS_SIM2_YYYY) : 1 ligne par maille et par mois depuis 1958.
  Pas de vent/humidite/rayonnement a ce pas -> colonnes null.
- Quotidien (QUOT_SIM2_latest) : ~60 jours glissants, variables completes.

Les deux granularites sont dedoublonnees par snapshot (le plus recent gagne),
jointes a la grille de coordonnees et ecrites dans un schema commun,
partitionne (kind, year, month). Dynamic partition overwrite : idempotent.
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

HDFS = "hdfs://namenode:8020"
BRONZE_SIM = f"{HDFS}/datalake/bronze/source=sim"
SILVER_OUT = f"{HDFS}/datalake/silver/sim_grid"


def num(col: str):
    """Cast double tolerant a la virgule decimale des fichiers Meteo-France."""
    return F.regexp_replace(F.col(col), ",", ".").cast("double")


def read_grid(spark: SparkSession):
    grid = (
        spark.read.option("header", True)
        .option("sep", ";")
        .csv(f"{BRONZE_SIM}/kind=grille/*.csv")
    )
    return grid.select(
        F.col("`LAMBX (hm)`").cast("int").alias("lambx"),
        F.col("`LAMBY (hm)`").cast("int").alias("lamby"),
        num("LAT_DG").alias("lat"),
        num("LON_DG").alias("lon"),
    )


def read_sim(spark: SparkSession, kind: str):
    """Lit un kind Bronze et garde le snapshot le plus recent par maille/date."""
    df = (
        spark.read.option("header", True)
        .option("sep", ";")
        .option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.csv.gz")
        .csv(f"{BRONZE_SIM}/kind={kind}")
    )
    df = df.withColumn(
        "snapshot",
        F.regexp_extract(F.input_file_name(), r"snapshot=(\d{4}-\d{2}-\d{2})", 1),
    )
    w = Window.partitionBy("LAMBX", "LAMBY", "DATE").orderBy(F.col("snapshot").desc())
    return df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")


def build_mens(spark: SparkSession):
    df = read_sim(spark, "mens")
    return df.select(
        F.lit("mens").alias("kind"),
        F.col("LAMBX").cast("int").alias("lambx"),
        F.col("LAMBY").cast("int").alias("lamby"),
        # DATE = AAAAMM -> 1er du mois
        F.to_date(F.col("DATE"), "yyyyMM").alias("obs_date"),
        num("T").alias("temp"),
        num("PRETOTM").alias("precip_mm"),
        F.lit(None).cast("double").alias("wind_ms"),
        F.lit(None).cast("double").alias("humidity_pct"),
        F.lit(None).cast("double").alias("ssi"),
    )


def build_quot(spark: SparkSession):
    df = read_sim(spark, "quot_latest")
    return df.select(
        F.lit("quot").alias("kind"),
        F.col("LAMBX").cast("int").alias("lambx"),
        F.col("LAMBY").cast("int").alias("lamby"),
        F.to_date(F.col("DATE"), "yyyyMMdd").alias("obs_date"),
        num("T").alias("temp"),
        (num("PRELIQ") + num("PRENEI")).alias("precip_mm"),
        num("FF").alias("wind_ms"),
        num("HU").alias("humidity_pct"),
        num("SSI").alias("ssi"),  # rayonnement J/cm2
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("silver-sim")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    grid = read_grid(spark)
    silver = (
        build_mens(spark).unionByName(build_quot(spark))
        .filter(F.col("obs_date").isNotNull())
        .join(grid, ["lambx", "lamby"], "inner")
        .withColumn("year", F.year("obs_date"))
        .withColumn("month", F.month("obs_date"))
    )

    silver.repartition("kind", "year", "month").write.mode("overwrite") \
        .partitionBy("kind", "year", "month").parquet(SILVER_OUT)

    counts = spark.read.parquet(SILVER_OUT).groupBy("kind").count().collect()
    for row in counts:
        print(f"silver sim_grid {row['kind']}: {row['count']} lignes")
    spark.stop()


if __name__ == "__main__":
    main()
