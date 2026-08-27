"""Job Spark Silver : cartes de vigilance Bronze -> Parquet partitionne.

Parse les CDP_CARTE_EXTERNE.json (1 par jour), ne garde que l'echeance J
(la carte du jour meme, pas la prevision J1), et explose l'arbre
product.periods -> timelaps.domain_ids -> phenomenon_items en une ligne par
(jour, departement, phenomene) avec la couleur max du phenomene.

Couleurs : 1 vert, 2 jaune, 3 orange, 4 rouge.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS = "hdfs://namenode:8020"
BRONZE_VIG = f"{HDFS}/datalake/bronze/source=vigilance"
SILVER_OUT = f"{HDFS}/datalake/silver/vigilance"

PHENOMENES = {
    1: "vent_violent", 2: "pluie_inondation", 3: "orages", 4: "inondation",
    5: "neige_verglas", 6: "canicule", 7: "grand_froid", 8: "avalanches",
    9: "vagues_submersion",
}


def main() -> None:
    spark = (
        SparkSession.builder.appName("silver-vigilance")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read.option("multiLine", True)
        .json(f"{BRONZE_VIG}/date=*/CDP_CARTE_EXTERNE.json")
        .withColumn(
            "vig_date",
            F.to_date(F.regexp_extract(F.input_file_name(), r"date=(\d{4}-\d{2}-\d{2})", 1)),
        )
    )

    periods = df.select("vig_date", F.explode("product.periods").alias("period"))
    periods = periods.filter(F.col("period.echeance") == "J")
    domains = periods.select(
        "vig_date", F.explode("period.timelaps.domain_ids").alias("dom")
    )
    phen = domains.select(
        "vig_date",
        F.col("dom.domain_id").alias("dept"),
        F.col("dom.max_color_id").cast("int").alias("dept_max_color_id"),
        F.explode_outer("dom.phenomenon_items").alias("ph"),
    )

    phen_map = F.create_map(*[F.lit(x) for kv in PHENOMENES.items() for x in kv])
    out = (
        phen.select(
            "vig_date", "dept", "dept_max_color_id",
            F.col("ph.phenomenon_id").cast("int").alias("phenomenon_id"),
            F.col("ph.phenomenon_max_color_id").cast("int").alias("color_id"),
        )
        # departements metropolitains uniquement (ecarte zones marines etc.)
        .filter(F.col("dept").rlike("^([0-9]{2}|2A|2B)$"))
        .withColumn("phenomene", phen_map[F.col("phenomenon_id")])
        .withColumn("year", F.year("vig_date"))
    )

    out.repartition("year").write.mode("overwrite") \
        .partitionBy("year").parquet(SILVER_OUT)

    n = spark.read.parquet(SILVER_OUT).count()
    print(f"silver vigilance: {n} lignes")
    spark.stop()


if __name__ == "__main__":
    main()
