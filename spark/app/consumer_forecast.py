"""
Consumer Spark Structured Streaming: topic weather_forecast (Kafka) → Parquet en S3 RAW.

Proceso:
  - Lee mensajes del topic weather_forecast (publicados por kafka/producer.py).
  - Parsea JSON, deriva year/month de forecast_time, particiona por city/year/month.
  - Escribe Parquet en s3a://BUCKET_NAME_RAW/forecast/ (un archivo por partición por trigger).
  - El detalle por día queda en la columna forecast_time; Silver puede filtrar por fecha.

Variables de entorno: BUCKET_NAME_RAW (obligatoria), KAFKA_BOOTSTRAP_SERVERS o KAFKA_IP:KAFKA_PORT,
  KAFKA_TOPIC (opcional, default weather_forecast). Ver spark/.env y CONFIGURACION_SPARK_EC2.md.

Ejecución: spark-submit desde el contenedor Spark (job de larga duración).
Documentación completa: spark/CONFIGURACION_SPARK_EC2.md, sección "Consumer de forecast (Kafka → S3 RAW)".
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, date_format, to_timestamp
from pyspark.sql.types import (
    StructType, StringType, DoubleType, LongType, StructField,
)

# -----------------------------------------------------------------------------
# Variables de entorno
# -----------------------------------------------------------------------------
def get_required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value or not str(value).strip():
        print(f"FATAL: Variable de entorno '{name}' no definida.", file=sys.stderr)
        sys.exit(1)
    return value.strip()

# Broker: KAFKA_BOOTSTRAP_SERVERS (ej. "172.31.3.189:9092") o KAFKA_IP + KAFKA_PORT
BROKER = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
if not BROKER:
    ip = os.environ.get("KAFKA_IP", "").strip()
    port = os.environ.get("KAFKA_PORT", "9092").strip()
    if not ip:
        print("FATAL: Definir KAFKA_BOOTSTRAP_SERVERS o KAFKA_IP (y opcionalmente KAFKA_PORT).", file=sys.stderr)
        sys.exit(1)
    BROKER = f"{ip}:{port}"

TOPIC = os.environ.get("KAFKA_TOPIC", "weather_forecast").strip() or "weather_forecast"
BUCKET_RAW = get_required_env("BUCKET_NAME_RAW")
OUTPUT_PATH = f"s3a://{BUCKET_RAW}/forecast/"

# -----------------------------------------------------------------------------
# Spark
# -----------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("VortexData-Forecast-Consumer")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Esquema alineado con kafka/producer.py (forecast); humidity y clouds en DoubleType por robustez
schema = StructType([
    StructField("source", StringType()),
    StructField("city", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("extraction_at", LongType()),
    StructField("forecast_time", StringType()),
    StructField("temp_c", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("wind_speed", DoubleType()),
    StructField("clouds", DoubleType()),
    StructField("weather_desc", StringType()),
])

# Leer de Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BROKER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Parseo y particionado (año, mes a partir de forecast_time; día en forecast_time para filtros)
df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

df_partitioned = df_parsed \
    .withColumn("ts", to_timestamp(col("forecast_time"))) \
    .withColumn("year", date_format(col("ts"), "yyyy")) \
    .withColumn("month", date_format(col("ts"), "MM")) \
    .drop("ts")

# Un archivo por trigger por partición (evita muchos Parquet pequeños para la Silver)
df_coalesced = df_partitioned.coalesce(1)

# Escritura en S3 (partición por city/year/month; menos directorios, Silver filtra por forecast_time si necesita día)
query = df_coalesced.writeStream \
    .format("parquet") \
    .option("path", OUTPUT_PATH) \
    .option("checkpointLocation", OUTPUT_PATH + "_checkpoints/") \
    .partitionBy("city", "year", "month") \
    .trigger(processingTime="60 seconds") \
    .outputMode("append") \
    .start()

print(f"--- Consumer forecast: broker={BROKER} topic={TOPIC} → {OUTPUT_PATH} ---")
query.awaitTermination()
