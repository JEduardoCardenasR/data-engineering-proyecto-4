"""
ETL Capa Gold - data-engineering-proyecto-4 (Módulo 4)

Consigna:
- Scripts PySpark que transforman datos para soportar las preguntas de negocio.
- Entrada: capa Silver (Parquet particionado por ciudad/event_year); salida: capa Gold.
- Optimización: particionamiento por ciudad/mes, caché, control de shuffle, dynamic overwrite.

Flujo: Silver (Parquet) → agregaciones e índices de potencial → Gold (Parquet particionado).

Entrada (Silver):
- Particionamiento: ciudad / event_year (año del evento)
- Los datos históricos en Silver se cargan una sola vez; el stream se actualiza diariamente.

Estrategia de procesamiento OPTIMIZADO:
- PRIMERA EJECUCIÓN: Lee TODO Silver → Escribe Gold particionado por ciudad/mes_anio
- EJECUCIONES DIARIAS: Lee solo el MES ACTUAL de Silver → Sobrescribe solo la partición del mes actual
- Los meses anteriores quedan INTACTOS (Dynamic Partition Overwrite)

Salida (Gold):
- resumen_clima_diario: particionado por ciudad y mes_anio (dynamic overwrite por mes)
- patrones_horarios: particionado por ciudad y mes_anio (dynamic overwrite por mes)

Ejecución: spark-submit --master <url> /app/capa_gold.py
           o: python3 /app/capa_gold.py (dentro del contenedor)
"""
import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, avg, max, min,
    date_format, dayofmonth, month, year,
    pow, coalesce, round, concat_ws, sum,
    current_timestamp, to_date
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

# Mes y año actual para procesamiento incremental
CURRENT_YEAR_MONTH = datetime.now().strftime('%Y-%m')  # "2026-03"
CURRENT_YEAR = datetime.now().strftime('%Y')           # "2026" (para partition pruning en Silver)

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
print(f"📅 Mes actual configurado: {CURRENT_YEAR_MONTH}")
print(f"✅ Leyendo datos desde la Capa Silver: {INPUT_PATH}")

# --- 2. CONFIGURACIÓN DE LA SESIÓN SPARK (AJUSTES DE RENDIMIENTO Y SHUFFLE) ---

spark = (
    SparkSession.builder.appName("DataEngProyecto4_SilverToGold")
    # Usar el proveedor de credenciales de la instancia
    .config("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")

    # OPTIMIZACIÓN: Reducimos particiones de shuffle
    # Gold maneja menos volumen que Silver (datos ya agregados)
    .config("spark.sql.shuffle.partitions", "20")

    # OPTIMIZACIÓN: Dynamic partition overwrite
    # Solo sobrescribe las particiones presentes en el DataFrame (no borra las demás)
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def check_gold_historical_exists(spark_session, output_path, current_year_month):
    """
    Verifica si Gold ya tiene particiones de meses anteriores CON archivos parquet.
    Retorna True si existen datos históricos válidos, False si es primera ejecución.
    """
    try:
        hadoop_conf = spark_session._jsc.hadoopConfiguration()
        fs = spark_session._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark_session._jvm.java.net.URI(output_path), hadoop_conf
        )
        path = spark_session._jvm.org.apache.hadoop.fs.Path(output_path)
        
        if not fs.exists(path):
            print(f"ℹ️ Gold no existe en {output_path}. Primera ejecución detectada.")
            return False
        
        # Buscar particiones de meses anteriores que tengan archivos .parquet
        for city_status in fs.listStatus(path):
            if city_status.isDirectory():
                city_path = city_status.getPath()
                for month_status in fs.listStatus(city_path):
                    if month_status.isDirectory():
                        dir_name = month_status.getPath().getName()
                        if dir_name.startswith("mes_anio="):
                            month_val = dir_name.split("=")[1]
                            if month_val < current_year_month:
                                # Verificar que hay al menos 1 archivo .parquet
                                month_path = month_status.getPath()
                                for file_status in fs.listStatus(month_path):
                                    if file_status.getPath().getName().endswith(".parquet"):
                                        print(f"✅ Partición histórica válida encontrada: {dir_name}")
                                        return True
        
        print(f"ℹ️ No se encontraron particiones históricas válidas. Se procesará todo Silver.")
        return False
        
    except Exception as e:
        print(f"⚠️ Error verificando Gold: {e}. Asumiendo primera ejecución por seguridad.")
        return False

# --- 3. LECTURA DE DATOS SILVER y CÁLCULO DE ÍNDICES DE POTENCIAL ---

try:
    # 3.1. Verificar si es primera ejecución o incremental
    is_first_run_daily = not check_gold_historical_exists(spark, OUTPUT_PATH_DAILY, CURRENT_YEAR_MONTH)
    is_first_run_patterns = not check_gold_historical_exists(spark, OUTPUT_PATH_PATTERNS, CURRENT_YEAR_MONTH)
    is_first_run = is_first_run_daily or is_first_run_patterns

    if is_first_run:
        print(f"\n🚀 PRIMERA EJECUCIÓN: Se procesará TODO Silver (históricos + stream).")
        df_silver = spark.read.parquet(INPUT_PATH)
    else:
        print(f"\n🔄 EJECUCIÓN INCREMENTAL: Solo se procesará el mes actual ({CURRENT_YEAR_MONTH}).")
        print(f"   Usando partition pruning: event_year = {CURRENT_YEAR}")
        
        # OPTIMIZACIÓN: Silver está particionado por ciudad/event_year
        # Paso 1: Filtrar por event_year (esto hace PARTITION PRUNING - solo lee la partición del año actual)
        # Paso 2: Filtrar por mes_anio (filtra dentro del año ya cargado)
        
        df_silver_year = spark.read.parquet(INPUT_PATH).filter(
            col("event_year") == CURRENT_YEAR  # ← Partition pruning: solo lee partición 2026
        )
        
        # Crear columna mes_anio temporalmente para filtrar el mes específico
        df_silver_year = df_silver_year.withColumn(
            "_temp_mes_anio", 
            date_format(col("timestamp_iso").cast("timestamp"), "yyyy-MM")
        )
        
        df_silver = df_silver_year.filter(col("_temp_mes_anio") == CURRENT_YEAR_MONTH).drop("_temp_mes_anio")

    total_records = df_silver.count()

    if total_records == 0:
        print("❌ ERROR: El DataFrame de la Capa Silver está vacío (o no hay datos del mes actual).", file=sys.stderr)
        sys.exit(1)

    print(f"Total de registros a procesar: {total_records}")

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

    # Incluimos mes_anio en el groupBy para poder particionar por mes (optimización)
    group_cols_daily = ["ciudad", "mes_anio", "fecha_solo"]

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

    # --- 6. ESCRITURA DE DATOS GOLD (con Dynamic Partition Overwrite) ---

    # 6.1. Escribir Resumen Diario en Parquet
    # OPTIMIZACIÓN: Particionado por ciudad Y mes_anio para dynamic overwrite
    # Solo sobrescribe las particiones del mes actual, los meses anteriores quedan intactos
    print(f"\nEscribiendo Resumen DIARIO GOLD (Parquet): {OUTPUT_PATH_DAILY}")
    print(f"   Particiones: ciudad / mes_anio")
    print(f"   Modo: dynamic overwrite (solo sobrescribe mes actual: {CURRENT_YEAR_MONTH})")

    (
        df_gold_daily
        .repartition(col("ciudad"), col("mes_anio"))
        .write
        .mode("overwrite")  # Con partitionOverwriteMode=dynamic, solo toca particiones presentes
        .partitionBy("ciudad", "mes_anio")
        .parquet(OUTPUT_PATH_DAILY)
    )

    # 6.2. Escribir Patrones Horarios en Parquet
    # Ya estaba particionado por ciudad/mes_anio, ahora con dynamic overwrite
    print(f"\nEscribiendo Patrones HORARIOS GOLD (Parquet): {OUTPUT_PATH_PATTERNS}")
    print(f"   Particiones: ciudad / mes_anio")
    print(f"   Modo: dynamic overwrite (solo sobrescribe mes actual: {CURRENT_YEAR_MONTH})")

    (
        df_gold_patterns
        .repartition(col("ciudad"), col("mes_anio"))
        .write
        .mode("overwrite")  # Con partitionOverwriteMode=dynamic, solo toca particiones presentes
        .partitionBy("ciudad", "mes_anio")
        .parquet(OUTPUT_PATH_PATTERNS)
    )

    # Resumen del procesamiento
    if is_first_run:
        print("\n✅ PRIMERA EJECUCIÓN completada: Se procesó TODO el historial.")
    else:
        print(f"\n✅ EJECUCIÓN INCREMENTAL completada: Solo se procesó el mes {CURRENT_YEAR_MONTH}.")
    
    print(f"   Resumen diario guardado en: {OUTPUT_PATH_DAILY}")
    print(f"   Patrones horarios guardados en: {OUTPUT_PATH_PATTERNS}")

    # Liberar caché para liberar memoria
    df_silver.unpersist()

except Exception as e:
    print(f"ERROR: Ocurrió un error grave durante el proceso ETL a GOLD. Detalle: {e}", file=sys.stderr)
    sys.exit(1)

finally:
    spark.stop()
    print("Sesión Spark detenida.")
