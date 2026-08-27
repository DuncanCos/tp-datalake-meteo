# Verification rapide du contenu Silver (lecture seule)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("check-silver").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("hdfs://namenode:8020/datalake/silver/observations")

print("=== schema ===")
df.printSchema()

print("=== exemple meteofrance (Paris-Montsouris, janvier 2026) ===")
df.filter("source = 'meteofrance' AND station_name LIKE '%MONTSOURIS%' AND obs_date = '2026-01-05'") \
  .select("station_id", "obs_date", "temperature", "temp_min", "temp_max",
          "precipitation_mm", "wind_speed_ms", "sunshine_min", "humidity_pct").show(truncate=False)

print("=== exemple openmeteo (dernier quart d'heure) ===")
df.filter("source = 'openmeteo'").orderBy(F.desc("obs_time")) \
  .select("station_id", "dept", "obs_time", "temperature", "precipitation_mm",
          "wind_speed_ms", "wind_gust_ms", "cloud_cover_pct", "weather_code").show(5, truncate=False)

dup_om = df.filter("source='openmeteo'").groupBy("station_id", "obs_time").count().filter("count > 1").count()
dup_mf = df.filter("source='meteofrance'").groupBy("station_id", "obs_date").count().filter("count > 1").count()
print(f"doublons openmeteo (ville, obs_time) : {dup_om}")
print(f"doublons meteofrance (station, obs_date) : {dup_mf}")

spark.stop()
