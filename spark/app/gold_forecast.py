"""
ETL Capa Gold – Forecast (Silver forecast → Gold forecast).

Lee los Parquet de la capa Silver forecast (s3a://BUCKET_NAME_SILVER/forecast/),
normaliza nombres de columnas al esquema esperado por las fórmulas de potencial,
calcula WPI (Wind Potential Index) y SPI (Solar Potential Index) y escribe las
tablas analíticas: resumen_clima_diario y patrones_horarios para datos de pronóstico.

Entrada: Silver forecast (columnas city, wind_speed, temp_c, humidity, clouds, forecast_timestamp, etc.).
Salida: Gold forecast en gold/forecast/resumen_clima_diario y gold/forecast/patrones_horarios.

Variables de entorno: BUCKET_NAME_SILVER, BUCKET_NAME_GOLD.
Ejecución: spark-submit /app/gold_forecast.py
"""
import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, avg, max, min,
    date_format, pow, coalesce, round, concat_ws, sum,
    current_timestamp, to_date,
)
from pyspark.sql.types import StringType

# --- 1. CONFIGURACIÓN Y VARIABLES DE ENTORNO ---

def get_required_env(var_name: str) -> str:
    value = os.environ.get(var_name)
    if not value or not str(value).strip():
        print(f"FATAL: Variable de entorno '{var_name}' no definida.", file=sys.stderr)
        sys.exit(1)
    return value.strip()

try:
    BUCKET_NAME_SILVER = get_required_env("BUCKET_NAME_SILVER")
    BUCKET_NAME_GOLD = get_required_env("BUCKET_NAME_GOLD")
except Exception as e:
    print(f"ERROR DE CONFIGURACIÓN: {e}", file=sys.stderr)
    sys.exit(1)

# Entrada: ruta donde Silver forecast escribió los datos
INPUT_PATH = f"s3a://{BUCKET_NAME_SILVER}/forecast/"
OUTPUT_PATH_DAILY = f"s3a://{BUCKET_NAME_GOLD}/gold/forecast/resumen_clima_diario/"
OUTPUT_PATH_PATTERNS = f"s3a://{BUCKET_NAME_GOLD}/gold/forecast/patrones_horarios/"

print("--- INICIO ETL: Capa SILVER Forecast → GOLD Forecast ---")
print(f"📂 Entrada:  {INPUT_PATH}")
print(f"📂 Salida diario:   {OUTPUT_PATH_DAILY}")
print(f"📂 Salida patrones: {OUTPUT_PATH_PATTERNS}")

# --- 2. SPARK ---

spark = (
    SparkSession.builder
    .appName("VortexData-Gold-Forecast")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
    .config("spark.sql.shuffle.partitions", "20")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# --- 3. LEER SILVER FORECAST ---

df_silver = spark.read.parquet(INPUT_PATH)
total_records = df_silver.count()
if total_records == 0:
    print("❌ ERROR: No hay datos en la capa Silver forecast.", file=sys.stderr)
    sys.exit(1)
print(f"Total de registros a procesar: {total_records}")

# --- 4. NORMALIZAR ESQUEMA (forecast usa wind_speed, temp_c, clouds, etc. → nombres alineados con Gold) ---

df = (
    df_silver
    .withColumnRenamed("city", "ciudad")
    .withColumnRenamed("wind_speed", "velocidad_viento_m_s")
    .withColumnRenamed("temp_c", "temperatura_c")
    .withColumnRenamed("humidity", "humedad_porcentaje")
    .withColumnRenamed("clouds", "nubes_porcentaje")
)

# Campos de tiempo a partir de forecast_timestamp
df = df.withColumn("fecha_hora_utc", col("forecast_timestamp").cast("timestamp"))
df = df.withColumn("fecha_solo", to_date(col("fecha_hora_utc")))
df = df.withColumn("hora_solo", date_format(col("fecha_hora_utc"), "HH").cast(StringType()))
df = df.withColumn("mes_anio", date_format(col("fecha_hora_utc"), "yyyy-MM"))

# --- 5. CÁLCULO DE POTENCIAL (WPI y SPI) ---

# WPI: Wind Potential Index = (velocidad_viento_m_s ^ 3) / 100
df = df.withColumn(
    "potencial_eolico_index",
    round(pow(coalesce(col("velocidad_viento_m_s"), lit(0)), 3) / lit(100), 2),
)

# SPI: Solar Potential Index = indice_uv * (1 - nubes_porcentaje/100). Forecast no trae UV → usamos 5.0
df = df.withColumn(
    "factor_nubes",
    lit(1) - (coalesce(col("nubes_porcentaje"), lit(0)) / lit(100)),
)
# Forecast no incluye indice_uv; usamos 5.0 como base (luz diurna promedio)
df = df.withColumn(
    "potencial_solar_index",
    round(lit(5.0) * col("factor_nubes"), 2),
).drop("factor_nubes")

df.cache()
df.count()

# --- 6. RESUMEN DIARIO ---

group_cols_daily = ["ciudad", "mes_anio", "fecha_solo"]
df_gold_daily = df.groupBy(*group_cols_daily).agg(
    round(avg("potencial_eolico_index"), 2).alias("promedio_potencial_eolico_diario"),
    round(max("potencial_eolico_index"), 2).alias("max_potencial_eolico_diario"),
    round(min("potencial_eolico_index"), 2).alias("min_potencial_eolico_diario"),
    round(avg("potencial_solar_index"), 2).alias("promedio_potencial_solar_diario"),
    round(max("potencial_solar_index"), 2).alias("max_potencial_solar_diario"),
    round(min("potencial_solar_index"), 2).alias("min_potencial_solar_diario"),
    round(avg("temperatura_c"), 2).alias("promedio_temperatura_diaria_c"),
    max("temperatura_c").alias("max_temperatura_registro_c"),
    min("temperatura_c").alias("min_temperatura_registro_c"),
    round(avg("humedad_porcentaje"), 2).alias("promedio_humedad_diaria"),
    sum(lit(0)).alias("total_precipitacion_diaria_mm"),  # forecast no tiene lluvia
    max(col("velocidad_viento_m_s")).alias("max_velocidad_viento_m_s"),
    round(avg(col("nubes_porcentaje")), 2).alias("promedio_nubes_diario"),
).withColumn("fecha_procesamiento_gold", current_timestamp())

# --- 7. PATRONES HORARIOS ---

df_gold_patterns = df.groupBy("ciudad", "mes_anio", "hora_solo").agg(
    round(avg("potencial_eolico_index"), 2).alias("promedio_eolico_por_hora_mes"),
    round(avg("potencial_solar_index"), 2).alias("promedio_solar_por_hora_mes"),
    round(avg("temperatura_c"), 2).alias("promedio_temperatura_por_hora_mes"),
    round(avg("humedad_porcentaje"), 2).alias("promedio_humedad_por_hora_mes"),
).withColumn("id_analisis_patron", concat_ws("-", col("ciudad"), col("mes_anio"), col("hora_solo"))).withColumn(
    "fecha_procesamiento_gold", current_timestamp()
)

# --- 8. ESCRITURA (un archivo por ciudad/mes_anio: menos objetos, lecturas más simples) ---

n_daily = max(1, df_gold_daily.select("ciudad", "mes_anio").distinct().count())
n_patterns = max(1, df_gold_patterns.select("ciudad", "mes_anio").distinct().count())

print("\nEscribiendo Resumen DIARIO GOLD (forecast)...")
df_gold_daily.repartition(n_daily, "ciudad", "mes_anio").write.mode("overwrite").partitionBy("ciudad", "mes_anio").parquet(OUTPUT_PATH_DAILY)

print("Escribiendo Patrones HORARIOS GOLD (forecast)...")
df_gold_patterns.repartition(n_patterns, "ciudad", "mes_anio").write.mode("overwrite").partitionBy("ciudad", "mes_anio").parquet(OUTPUT_PATH_PATTERNS)

df.unpersist()
print(f"\n✅ Gold forecast completado.")
print(f"   Resumen diario:   {OUTPUT_PATH_DAILY}")
print(f"   Patrones horarios: {OUTPUT_PATH_PATTERNS}")
spark.stop()
