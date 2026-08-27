"""Job Spark Gold : grilles SIM2 -> heatmaps de l'Indice Grisaille.

Tables produites (Parquet sur HDFS, /datalake/gold/) :
  - grisaille_grid_daily   : indice grisaille COMPLET (5 composantes) par
    cellule 16 km et par jour, sur la fenetre ~60 jours du fichier
    QUOT_SIM2_latest. Le rayonnement SSI remplace l'heliometre.
  - grisaille_grid_monthly : indice REDUIT froid+pluie (seules variables
    du fichier mensuel), renormalise et nomme grisaille_partielle, par
    cellule 16 km et par mois depuis 1958.

Sous-echantillonnage : les mailles SAFRAN 8 km sont agregees 2x2 en cellules
16 km (~2 500 par carte) pour que la webapp serve un JSON leger.

Args : --annees "" (defaut) = derniere annee presente en Silver mensuel ;
       "1958-2026" ou "2024" pour un backfill manuel de la table mensuelle.
La table quotidienne (60 jours) est toujours recalculee en entier.
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS = "hdfs://namenode:8020"
SILVER_SIM = f"{HDFS}/datalake/silver/sim_grid"
GOLD = f"{HDFS}/datalake/gold"

# memes ponderations et seuils que gold_job.py (jobs spark-submit autonomes)
W_FROID, W_SOLEIL, W_PLUIE, W_VENT, W_HUMIDITE = 0.30, 0.25, 0.25, 0.10, 0.10
TEMP_CONFORT = 15.0
TEMP_GLACIAL = -5.0
PLUIE_MAX = 20.0
VENT_MAX = 10.0
# rayonnement global quotidien ~ journee tres ensoleillee (equivalent du
# SOLEIL_PLEIN de gold_job.py, mais en J/cm2 : SIM2 n'a pas d'heliometre)
SSI_PLEIN = 2000.0
# mensuel : precipitations cumulees sur le mois -> seuil mensuel
PLUIE_MAX_MENS = PLUIE_MAX * 30.0

# maille SAFRAN = 80 hm ; cellule 2x2 mailles = 160 hm (16 km), centree
CELL_HM = 160


def clamp01(col):
    return F.least(F.greatest(col, F.lit(0.0)), F.lit(1.0))


def cellify(df):
    """Agrege les mailles 8 km en cellules 16 km (moyennes)."""
    return (
        df.withColumn("cellx", (F.floor(F.col("lambx") / CELL_HM) * CELL_HM + CELL_HM // 2))
        .withColumn("celly", (F.floor(F.col("lamby") / CELL_HM) * CELL_HM + CELL_HM // 2))
    )


def c_froid(temp_col):
    return clamp01((F.lit(TEMP_CONFORT) - temp_col) / (TEMP_CONFORT - TEMP_GLACIAL))


def build_grid_daily(silver):
    quot = cellify(silver.filter(F.col("kind") == "quot"))
    daily = quot.groupBy("cellx", "celly", "obs_date").agg(
        F.round(F.avg("lat"), 3).alias("lat"),
        F.round(F.avg("lon"), 3).alias("lon"),
        F.avg("temp").alias("temp"),
        F.avg("precip_mm").alias("precip_mm"),
        F.avg("wind_ms").alias("wind_ms"),
        F.avg("humidity_pct").alias("humidity_pct"),
        F.avg("ssi").alias("ssi"),
    )
    daily = (
        daily
        .withColumn("c_froid", c_froid(F.col("temp")))
        .withColumn("c_pluie", clamp01(F.col("precip_mm") / PLUIE_MAX))
        .withColumn("c_soleil", clamp01(1 - F.col("ssi") / SSI_PLEIN))
        .withColumn("c_vent", clamp01(F.col("wind_ms") / VENT_MAX))
        .withColumn("c_humidite", clamp01((F.col("humidity_pct") - 60) / 40))
        .withColumn(
            "grisaille",
            F.round(100 * (
                W_FROID * F.col("c_froid") + W_SOLEIL * F.col("c_soleil")
                + W_PLUIE * F.col("c_pluie") + W_VENT * F.col("c_vent")
                + W_HUMIDITE * F.coalesce(F.col("c_humidite"), F.lit(0.0))
            ), 1),
        )
    )
    return (
        daily
        .withColumn("temp", F.round("temp", 1))
        .withColumn("precip_mm", F.round("precip_mm", 1))
        .withColumn("wind_ms", F.round("wind_ms", 1))
        .withColumn("humidity_pct", F.round("humidity_pct", 0))
        .withColumn("ssi", F.round("ssi", 0))
        .withColumn("year", F.year("obs_date"))
        .withColumn("month", F.month("obs_date"))
    )


def build_grid_monthly(silver, annees):
    mens = silver.filter(F.col("kind") == "mens").filter(F.col("year").isin(annees))
    monthly = cellify(mens).groupBy("cellx", "celly", "year", "month").agg(
        F.round(F.avg("lat"), 3).alias("lat"),
        F.round(F.avg("lon"), 3).alias("lon"),
        F.avg("temp").alias("temp"),
        F.avg("precip_mm").alias("precip_mm"),
    )
    monthly = (
        monthly
        .withColumn("c_froid", c_froid(F.col("temp")))
        .withColumn("c_pluie", clamp01(F.col("precip_mm") / PLUIE_MAX_MENS))
        # seules 2 composantes sur 5 sont connues au pas mensuel :
        # indice partiel renormalise sur leur poids cumule (0.55)
        .withColumn(
            "grisaille_partielle",
            F.round(100 * (W_FROID * F.col("c_froid") + W_PLUIE * F.col("c_pluie"))
                    / (W_FROID + W_PLUIE), 1),
        )
    )
    return (
        monthly
        .withColumn("temp", F.round("temp", 1))
        .withColumn("precip_mm", F.round("precip_mm", 1))
    )


def parse_annees(arg: str) -> list[int] | None:
    if not arg:
        return None
    if "-" in arg:
        a, b = arg.split("-", 1)
        return list(range(int(a), int(b) + 1))
    return [int(arg)]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--annees", default="")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("gold-sim")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    silver = spark.read.parquet(SILVER_SIM)

    # fenetre 60 jours : toujours recalculee en entier (2 a 3 partitions mois)
    build_grid_daily(silver).repartition("year", "month").write.mode("overwrite") \
        .partitionBy("year", "month").parquet(f"{GOLD}/grisaille_grid_daily")

    annees = parse_annees(args.annees)
    if annees is None:
        # run quotidien : seule la derniere annee mensuelle bouge
        derniere = (
            silver.filter(F.col("kind") == "mens")
            .agg(F.max("year").alias("y")).first()["y"]
        )
        annees = [derniere] if derniere is not None else []

    if annees:
        build_grid_monthly(silver, annees).repartition("year").write.mode("overwrite") \
            .partitionBy("year").parquet(f"{GOLD}/grisaille_grid_monthly")

    from pyspark.errors import AnalysisException
    for name in ["grisaille_grid_daily", "grisaille_grid_monthly"]:
        try:
            n = spark.read.parquet(f"{GOLD}/{name}").count()
            print(f"gold {name}: {n} lignes")
        except AnalysisException:
            print(f"gold {name}: absente (pas encore de donnees mensuelles)")
    spark.stop()


if __name__ == "__main__":
    main()
