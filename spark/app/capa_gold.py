"""
ETL Capa Gold - data-engineering-proyecto-4 (Módulo 4)

Consigna:
- Scripts PySpark que transforman datos para soportar las preguntas de negocio.
- Entrada: capa Silver (Parquet); salida: capa Gold en Parquet (resumen diario y patrones).
- Optimización: particionamiento por ciudad/mes, caché, control de shuffle, memoria.

Flujo: Silver (Parquet) → agregaciones e índices de potencial → Gold (Parquet particionado).

Ejecución: spark-submit --master <url> /app/capa_gold.py
           o: python3 /app/capa_gold.py (dentro del contenedor)
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, avg, max, min,
    date_format, dayofmonth, month, year,
    pow, coalesce, round, concat_ws, sum,
    current_timestamp, to_date
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

# --- 1. CONFIGURACIÓN Y MANEJO DE VARIABLES DE ENTORNO ---

def get_required_env(var_name):
    """Obtiene la variable de entorno o termina la ejecución si no se encuentra."""
    value = os.environ.get(var_name)
    if not value:
        print(f"FATAL ERROR: La variable de entorno requerida '{var_name}' no está configurada o es nula.", file=sys.stderr)
        sys.exit(1)
    return value

try:
    BUCKET_NAME_SILVER = get_required_env("BUCKET_NAME_SILVER")
    BUCKET_NAME_GOLD = get_required_env("BUCKET_NAME_GOLD")

    # Las credenciales son opcionales porque usamos IAM Role en la EC2
    AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

    if not AWS_ACCESS_KEY_ID:
        print("ℹ️ INFO: Usando IAM Role de la instancia para acceso a S3.")

except Exception as e:
    print(f"ERROR DE CONFIGURACIÓN DE ENTORNO: {e}", file=sys.stderr)
    sys.exit(1)

# Rutas de entrada/salida
INPUT_PATH = f"s3a://{BUCKET_NAME_SILVER}/silver/clima_procesado/"
# Definimos dos rutas de salida para las dos tablas analíticas
OUTPUT_PATH_DAILY = f"s3a://{BUCKET_NAME_GOLD}/gold/resumen_clima_diario/"
OUTPUT_PATH_PATTERNS = f"s3a://{BUCKET_NAME_GOLD}/gold/patrones_horarios/"

print(f"--- INICIO ETL: Capa SILVER a GOLD (data-engineering-proyecto-4) ---")
print(f"✅ Leyendo datos desde la Capa Silver: {INPUT_PATH}")

# --- 2. CONFIGURACIÓN DE LA SESIÓN SPARK (AJUSTES DE RENDIMIENTO Y SHUFFLE) ---

spark = (
    SparkSession.builder.appName("DataEngProyecto4_SilverToGold")
    # Usar el proveedor de credenciales de la instancia
    .config("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")

    # OPTIMIZACIÓN: Reducimos particiones de shuffle
    # Gold maneja menos volumen que Silver (datos ya agregados)
    .config("spark.sql.shuffle.partitions", "20")

    # No forzamos memoria aquí, Spark tomará los 4G configurados en Docker
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --- 3. LECTURA DE DATOS SILVER y CÁLCULO DE ÍNDICES DE POTENCIAL ---

try:
    # 3.1. Leer el DataFrame de la Capa Silver
    df_silver = spark.read.parquet(INPUT_PATH)
    total_records = df_silver.count()

    if total_records == 0:
        print("❌ ERROR: El DataFrame de la Capa Silver está vacío.", file=sys.stderr)
        sys.exit(1)

    print(f"Total de registros leídos de Silver: {total_records}")

    # 3.2. Preparar campos de tiempo
    df_silver = df_silver.withColumn("fecha_hora_utc", col("timestamp_iso").cast("timestamp"))
    df_silver = df_silver.withColumn("fecha_solo", to_date(col("fecha_hora_utc")))
    df_silver = df_silver.withColumn("hora_solo", date_format(col("fecha_hora_utc"), "HH").cast(StringType()))
    df_silver = df_silver.withColumn("mes_anio", date_format(col("fecha_hora_utc"), "yyyy-MM"))

    # 3.3. CÁLCULO DE ÍNDICES DE POTENCIAL ENERGÉTICO (WPI y SPI a nivel de registro)
    print("\n--- 3.3. Calculando Potencial Eólico y Solar...")

    # WPI: Wind Potential Index (Proporcional a Velocidad ^ 3)
    # Se divide por 100 para mantener el índice en un rango más manejable
    df_silver = df_silver.withColumn(
        "potencial_eolico_index",
        round(pow(coalesce(col("velocidad_viento_m_s"), lit(0)), 3) / lit(100), 2)
    )

    # SPI: Solar Potential Index (Basado en UV, atenuado por la nubosidad)
    df_silver = df_silver.withColumn(
        "factor_nubes",
        lit(1) - (coalesce(col("nubes_porcentaje"), lit(0)) / lit(100))
    )
    # Si UV es nulo, usamos un índice base de 5.0 (luz diurna promedio)
    df_silver = df_silver.withColumn(
        "potencial_solar_index",
        round(
            (coalesce(col("indice_uv"), lit(5.0)) * col("factor_nubes")),
            2
        )
    ).drop("factor_nubes")

    # OPTIMIZACIÓN (consigna: uso estratégico de caché): cachear tras los índices de potencial
    # para no recalcular WPI/SPI ni releer de S3 en las dos agregaciones (diaria y patrones)
    df_silver.cache()
    df_silver.count()  # materializa el caché

    # --- 4. RESUMEN ANALÍTICO DIARIO (Tabla Principal) ---

    print("\n--- 4. Generando Resumen Analítico DIARIO (resumen_clima_diario) ---")

    group_cols_daily = ["ciudad", "fecha_solo"]

    df_gold_daily = df_silver.groupBy(*group_cols_daily).agg(
        # Potencial energético (eólico y solar)
        round(avg("potencial_eolico_index"), 2).alias("promedio_potencial_eolico_diario"),
        round(max("potencial_eolico_index"), 2).alias("max_potencial_eolico_diario"),
        round(min("potencial_eolico_index"), 2).alias("min_potencial_eolico_diario"),

        round(avg("potencial_solar_index"), 2).alias("promedio_potencial_solar_diario"),
        round(max("potencial_solar_index"), 2).alias("max_potencial_solar_diario"),
        round(min("potencial_solar_index"), 2).alias("min_potencial_solar_diario"),

        # Condiciones climáticas
        round(avg("temperatura_c"), 2).alias("promedio_temperatura_diaria_c"),
        max("temperatura_c").alias("max_temperatura_registro_c"),
        min("temperatura_c").alias("min_temperatura_registro_c"),

        round(avg("humedad_porcentaje"), 2).alias("promedio_humedad_diaria"),
        round(sum(coalesce(col("lluvia_1h_mm"), lit(0))), 2).alias("total_precipitacion_diaria_mm"),

        max(col("velocidad_viento_m_s")).alias("max_velocidad_viento_m_s"),
        round(avg(col("nubes_porcentaje")), 2).alias("promedio_nubes_diario"),
    ).withColumn(
        "fecha_procesamiento_gold", current_timestamp()
    )

    print("\n--- Esquema Final GOLD (Resumen Diario) ---")
    df_gold_daily.printSchema()

    # --- 5. RESUMEN HORARIO Y MENSUAL (Tabla de Patrones) ---

    print("\n--- 5. Generando Resumen Horario y Mensual (patrones_horarios) ---")

    # Agregación por Ciudad, Mes (mes_anio) y Hora del día (hora_solo)
    df_gold_patterns = df_silver.groupBy("ciudad", "mes_anio", "hora_solo").agg(
        round(avg("potencial_eolico_index"), 2).alias("promedio_eolico_por_hora_mes"),
        round(avg("potencial_solar_index"), 2).alias("promedio_solar_por_hora_mes"),
        round(avg("temperatura_c"), 2).alias("promedio_temperatura_por_hora_mes"),
        round(avg("humedad_porcentaje"), 2).alias("promedio_humedad_por_hora_mes"),

    ).withColumn(
        "id_analisis_patron", concat_ws("-", col("ciudad"), col("mes_anio"), col("hora_solo"))
    ).withColumn(
        "fecha_procesamiento_gold", current_timestamp()
    )

    print("\n--- Esquema Final GOLD (Patrones Horarios) ---")
    df_gold_patterns.printSchema()

    # --- 6. ESCRITURA DE DATOS GOLD ---

    # 6.1. Escribir Resumen Diario en Parquet (consigna: salida en Parquet)
    print(f"\nEscribiendo Resumen DIARIO GOLD (Parquet): {OUTPUT_PATH_DAILY}")

    (
        df_gold_daily
        # repartition usa la configuración de shuffle.
        # Aseguramos que haya 100 particiones para el trabajo.
        .repartition(col("ciudad"))
        .write
        .mode("overwrite") # Sobrescribir, ya que es un resumen completo.
        .partitionBy("ciudad")
        .parquet(OUTPUT_PATH_DAILY)
    )

    # 6.2. Escribir Patrones Horarios en Parquet (consigna: salida en Parquet)
    print(f"\nEscribiendo Patrones HORARIOS GOLD (Parquet): {OUTPUT_PATH_PATTERNS}")

    (
        df_gold_patterns
        # repartition usa la configuración de shuffle.
        .repartition(col("ciudad"), col("mes_anio"))
        .write
        .mode("overwrite") # Sobrescribir, ya que es un resumen completo.
        .partitionBy("ciudad", "mes_anio")
        .parquet(OUTPUT_PATH_PATTERNS)
    )

    print("✅ El trabajo ETL a Capa GOLD ha finalizado con éxito.")
    print(f"El resumen diario se ha guardado en: {OUTPUT_PATH_DAILY}")
    print(f"Los patrones horarios se han guardado en: {OUTPUT_PATH_PATTERNS}")

    # Liberar caché para liberar memoria
    df_silver.unpersist()

except Exception as e:
    print(f"ERROR: Ocurrió un error grave durante el proceso ETL a GOLD. Detalle: {e}", file=sys.stderr)
    sys.exit(1)

finally:
    spark.stop()
    print("Sesión Spark detenida.")
