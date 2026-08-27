# Verification rapide des tables Gold (lecture seule)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

GOLD = "hdfs://namenode:8020/datalake/gold"
spark = SparkSession.builder.appName("check-gold").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("=== Classement grisaille janvier 2026 (la ville la plus deprimante) ===")
spark.read.parquet(f"{GOLD}/grisaille_ranking") \
    .filter("year = 2026 AND month = 1").orderBy("rang_misere") \
    .select("rang_misere", "city", "grisaille_moy", "temp_moy",
            "precip_cumul_mm", "soleil_moy_min", "n_jours").show(12, truncate=False)

print("=== Vagues de froid detectees (top duree) ===")
ep = spark.read.parquet(f"{GOLD}/episodes")
ep.filter("type = 'vague_de_froid'").orderBy(F.desc("duree_jours")) \
    .show(8, truncate=False)

print("=== Episodes par type ===")
ep.groupBy("type").count().show()

print("=== Statut live (classement misere en direct) ===")
spark.read.parquet(f"{GOLD}/live_status").orderBy("rang_misere_live") \
    .select("rang_misere_live", "city", "obs_time", "temperature",
            "cloud_cover_pct", "precipitation_mm", "wind_speed_ms",
            "grisaille_live").show(10, truncate=False)

print("=== Grisaille daily : pire journee de l'annee par ville (2026) ===")
d = spark.read.parquet(f"{GOLD}/grisaille_daily").filter("year = 2026")
w_ok = d.groupBy("city").agg(F.max("grisaille").alias("grisaille_max"))
d.join(w_ok, "city").filter(F.col("grisaille") == F.col("grisaille_max")) \
    .select("city", "obs_date", "grisaille", "temp_avg", "precip_mm",
            "sunshine_min", "wind_ms").orderBy(F.desc("grisaille")).show(12, truncate=False)

spark.stop()
