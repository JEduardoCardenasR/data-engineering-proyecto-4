"""
ETL Capa Silver – Forecast (RAW forecast → Silver forecast).

Lee los Parquet de la ruta RAW forecast (escritos por consumer_forecast.py desde Kafka),
aplica limpieza, deduplicación y enriquecimiento, y escribe en la capa Silver particionada
por city y year. Usa partition overwrite dinámico para solo sobrescribir las particiones
escritas en cada ejecución.

Variables de entorno: BUCKET_NAME_RAW, BUCKET_NAME_SILVER (mismo .env que capa_silver/capa_gold).

Ejecución: spark-submit /app/silver_forecast.py (o desde el contenedor Spark).
Documentación: spark/CONFIGURACION_SPARK_EC2.md.
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# -----------------------------------------------------------------------------
# Variables de entorno
# -----------------------------------------------------------------------------
def get_required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value or not str(value).strip():
        print(f"FATAL: Variable de entorno '{name}' no definida.", file=sys.stderr)
        sys.exit(1)
    return value.strip()

try:
    BUCKET_RAW = os.environ.get("BUCKET_NAME_RAW")
    if not BUCKET_RAW:
        print("⚠️ BUCKET_NAME_RAW no definida. Usando BUCKET_NAME_SILVER como base.", file=sys.stderr)
        BUCKET_RAW = get_required_env("BUCKET_NAME_SILVER")
    BUCKET_SILVER = get_required_env("BUCKET_NAME_SILVER")
except Exception as e:
    print(f"ERROR DE CONFIGURACIÓN: {e}", file=sys.stderr)
    sys.exit(1)

INPUT_PATH = f"s3a://{BUCKET_RAW}/forecast/"
OUTPUT_PATH = f"s3a://{BUCKET_SILVER}/forecast/"

print("--- INICIO ETL: Silver Forecast (RAW forecast → Silver forecast) ---")
print(f"📂 Entrada:  {INPUT_PATH}")
print(f"📂 Salida:   {OUTPUT_PATH}")

# -----------------------------------------------------------------------------
# Spark
# -----------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("VortexData-Silver-Forecast")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
    .config("spark.sql.shuffle.partitions", "50")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 1. Leer RAW forecast (particionado por city/year/month en el consumer)
df_raw = spark.read.parquet(INPUT_PATH)

# 2. Calidad: timestamps, redondeo, deduplicados
df_silver = (
    df_raw
    .withColumn("forecast_timestamp", F.to_timestamp("forecast_time"))
    .withColumn("extraction_timestamp", F.from_unixtime(F.col("extraction_at") / 1000).cast("timestamp"))
    .withColumn("temp_c", F.round(F.col("temp_c"), 1))
    .dropDuplicates(["city", "forecast_time"])
)

# 3. Año para partición (a partir de forecast_timestamp)
df_silver = df_silver.withColumn("year", F.year("forecast_timestamp"))

# 4. Enriquecimiento: clasificación de nubosidad (tratar nulos en clouds)
df_silver = df_silver.withColumn(
    "sky_condition",
    F.when(F.coalesce(F.col("clouds"), 0) > 75, "Muy Nublado")
    .when(F.coalesce(F.col("clouds"), 0) > 25, "Parcialmente Nublado")
    .otherwise("Despejado"),
)

# 5. Escritura Silver: partición por city y year; overwrite dinámico
df_silver.write.mode("overwrite").partitionBy("city", "year").parquet(OUTPUT_PATH)

print(f"✅ Capa Silver forecast completada en {OUTPUT_PATH}")
