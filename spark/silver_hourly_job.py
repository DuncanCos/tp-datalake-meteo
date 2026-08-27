"""Job Spark Silver : Bronze horaires Meteo-France -> Parquet partitionne.

Parse les CSV horaires (H_{dept}_latest-*.csv.gz), null-ifie les valeurs au
code qualite 2 (douteuses), dedoublonne les snapshots (garde le plus recent
par station/heure). Table dediee a la confrontation live vs officiel
(gold_confrontation_job.py) — separee de silver/observations qui est au
grain quotidien.
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

HDFS = "hdfs://namenode:8020"
BRONZE_HOR = f"{HDFS}/datalake/bronze/source=meteofrance_hor"
SILVER_OUT = f"{HDFS}/datalake/silver/observations_hourly"


def qval(col: str, qcol: str):
    """Valeur gardee sauf si le code qualite Meteo-France vaut 2 (douteuse)."""
    return F.when(F.col(qcol).isNull() | (F.col(qcol) != 2), F.col(col).cast("double"))


def main() -> None:
    spark = (
        SparkSession.builder.appName("silver-hourly")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # AAAAMMJJHH est en heure UTC : le parsing doit l'etre aussi
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read.option("header", True)
        .option("sep", ";")
        .option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.csv.gz")
        .csv(f"{BRONZE_HOR}/dept=*/period=*")
    )
    df = df.withColumn(
        "snapshot",
        F.regexp_extract(F.input_file_name(), r"snapshot=(\d{4}-\d{2}-\d{2})", 1),
    )
    # dedup snapshots : la ligne du snapshot le plus recent gagne
    w = Window.partitionBy("NUM_POSTE", "AAAAMMJJHH").orderBy(F.col("snapshot").desc())
    df = df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

    silver = df.select(
        F.col("NUM_POSTE").alias("station_id"),
        F.col("NOM_USUEL").alias("station_name"),
        F.substring("NUM_POSTE", 1, 2).alias("dept"),
        F.col("LAT").cast("double").alias("latitude"),
        F.col("LON").cast("double").alias("longitude"),
        F.col("ALTI").cast("double").alias("elevation"),
        F.to_timestamp(F.col("AAAAMMJJHH"), "yyyyMMddHH").alias("obs_ts"),
        qval("T", "QT").alias("temp"),
        qval("RR1", "QRR1").alias("precip_mm"),
        qval("FF", "QFF").alias("wind_ms"),
        qval("FXI", "QFXI").alias("gust_ms"),
        qval("U", "QU").alias("humidity_pct"),
    ).filter(F.col("obs_ts").isNotNull())

    silver = (
        silver.withColumn("year", F.year("obs_ts"))
        .withColumn("month", F.month("obs_ts"))
    )

    silver.repartition("year", "month").write.mode("overwrite") \
        .partitionBy("year", "month").parquet(SILVER_OUT)

    n = spark.read.parquet(SILVER_OUT).count()
    print(f"silver observations_hourly: {n} lignes")
    spark.stop()


if __name__ == "__main__":
    main()
