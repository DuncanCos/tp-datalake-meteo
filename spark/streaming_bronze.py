"""Job Spark Structured Streaming : Kafka weather.current -> Bronze HDFS.

Consomme le flux en continu et ecrit le JSON BRUT (une ligne par message,
aucune transformation) partitionne par date d'ingestion. Le checkpoint HDFS
garantit la reprise exactement-une-fois apres un crash/redemarrage.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

KAFKA_BOOTSTRAP = "kafka:19092"
TOPIC = "weather.current"
BRONZE_PATH = "hdfs://namenode:8020/datalake/bronze/source=openmeteo"
CHECKPOINT_PATH = "hdfs://namenode:8020/datalake/checkpoints/streaming_bronze"

spark = SparkSession.builder.appName("streaming-bronze-openmeteo").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# valeur brute + partition par date du timestamp Kafka
raw = stream.select(
    F.col("value").cast("string").alias("value"),
    F.to_date(F.col("timestamp")).alias("ingest_date"),
)

query = (
    raw.writeStream.format("text")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("ingest_date")
    # 5 min par micro-batch : 5x moins de petits fichiers HDFS qu'a 60 s,
    # sans impact visible (le Silver ne tourne que toutes les 15 min)
    .trigger(processingTime="300 seconds")
    .start()
)

query.awaitTermination()
