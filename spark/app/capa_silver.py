"""
ETL Capa Silver - data-engineering-proyecto-4 (Módulo 4)

Consigna:
- Scripts PySpark que transforman datos para soportar las preguntas de negocio.
- Combinar datos no estructurados (JSON histórico, JSONL stream) con fuentes
  estructuradas en el Data Lake; salida en formato Parquet (capa Silver).
- Optimización: particionamiento por ciudad/año, control de shuffle, dynamic overwrite.

Flujo:
- PRIMERA EJECUCIÓN: Raw (históricos + stream) → Silver (Parquet particionado por ciudad/event_year)
- EJECUCIONES POSTERIORES: Raw (solo stream) → Silver (sobrescribe solo partición del año actual)

Particionamiento:
- ciudad: Patagonia, Riohacha
- event_year: año del evento (del dato), no de procesamiento

Dynamic Partition Overwrite:
- Los años anteriores (históricos) se cargan UNA SOLA VEZ
- Las ejecuciones diarias solo procesan stream y sobrescriben la partición del año actual

Ejecución: spark-submit --master <url> /app/capa_silver.py
           o: python3 /app/capa_silver.py (dentro del contenedor)
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, round, from_unixtime, coalesce,
    element_at, when, size, year
)
from pyspark.sql.types import StringType, DoubleType, LongType
from datetime import datetime

# Año actual para determinar qué datos procesar en ejecuciones incrementales
CURRENT_YEAR = datetime.now().year  # 2026

# CONFIGURACIÓN Y MANEJO DE VARIABLES DE ENTORNO

def get_required_env(var_name):
    """Obtiene la variable de entorno o termina la ejecución si no se encuentra."""
    value = os.environ.get(var_name)
    if not value:
        print(f"FATAL ERROR: La variable de entorno requerida '{var_name}' no está configurada o es nula.", file=sys.stderr)
        sys.exit(1)
    return value

try:
    # Usamos la lógica de respaldo para BUCKET_RAW si no está definida
    BUCKET_RAW = os.environ.get("BUCKET_NAME_RAW")
    if not BUCKET_RAW:
        print("⚠️ ALERTA: BUCKET_NAME_RAW no está definida. Usando BUCKET_NAME_SILVER como base.")
        BUCKET_RAW = get_required_env("BUCKET_NAME_SILVER")

    BUCKET_SILVER = get_required_env("BUCKET_NAME_SILVER")

    # MODIFICACIÓN: Las credenciales son opcionales porque usamos IAM Role
    AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

    if not AWS_ACCESS_KEY_ID:
        print("ℹ️ INFO: No se detectaron llaves AWS en el entorno. Se intentará usar el IAM Role de la instancia.")

except Exception as e:
    print(f"ERROR DE CONFIGURACIÓN DE ENTORNO: {e}", file=sys.stderr)
    sys.exit(1)

# Rutas stream (datos recientes: JSONL.GZ, un archivo por día)
# Estructura: bucket/stream/{Patagonia|Riohacha}/onecall/*.jsonl.gz
STREAM_PATHS = [
    f"s3a://{BUCKET_RAW}/stream/Patagonia/onecall/",
    f"s3a://{BUCKET_RAW}/stream/Riohacha/onecall/",
]

# Rutas históricos (JSON por ciudad)
# Estructura: bucket/historicos/{Patagonia|Riohacha}/<archivo>.json
HISTORICAL_PATHS = [
    f"s3a://{BUCKET_RAW}/historicos/Patagonia/Patagonia_-41.json",
    f"s3a://{BUCKET_RAW}/historicos/Riohacha/Riohacha_11_538415.json",
]

OUTPUT_PATH = f"s3a://{BUCKET_SILVER}/silver/clima_procesado/"

print(f"--- INICIO ETL: Capa RAW a SILVER (data-engineering-proyecto-4) ---")
print(f"📅 Año actual configurado: {CURRENT_YEAR}")
print(f"📂 Rutas de stream disponibles: {len(STREAM_PATHS)}")
print(f"📂 Rutas de históricos disponibles: {len(HISTORICAL_PATHS)}")

# CONFIGURACIÓN DE LA SESIÓN SPARK

spark = (
    SparkSession.builder.appName("DataEngProyecto4_RawToSilver")
    # En lugar de access/secret key, usamos el proveedor de credenciales de la instancia
    .config("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
    .config("spark.sql.shuffle.partitions", "100")
    # Dynamic partition overwrite: solo sobrescribe las particiones presentes en el DataFrame
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def check_historical_data_exists(spark_session, output_path, current_year):
    """
    Verifica si Silver ya tiene particiones de años anteriores CON archivos parquet.
    No lee datos, solo verifica existencia de carpetas y archivos (eficiente).
    Retorna True si existen datos históricos válidos, False si es primera ejecución.
    """
    try:
        hadoop_conf = spark_session._jsc.hadoopConfiguration()
        fs = spark_session._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark_session._jvm.java.net.URI(output_path), hadoop_conf
        )
        path = spark_session._jvm.org.apache.hadoop.fs.Path(output_path)
        
        if not fs.exists(path):
            print(f"ℹ️ Silver no existe. Primera ejecución detectada.")
            return False
        
        # Buscar particiones de años anteriores que tengan archivos .parquet
        for city_status in fs.listStatus(path):
            if city_status.isDirectory():
                city_path = city_status.getPath()
                for year_status in fs.listStatus(city_path):
                    if year_status.isDirectory():
                        dir_name = year_status.getPath().getName()
                        if dir_name.startswith("event_year="):
                            year_val = dir_name.split("=")[1]
                            if year_val.isdigit() and int(year_val) < current_year:
                                # Verificar que hay al menos 1 archivo .parquet
                                year_path = year_status.getPath()
                                for file_status in fs.listStatus(year_path):
                                    if file_status.getPath().getName().endswith(".parquet"):
                                        print(f"✅ Partición histórica válida: {dir_name} (con archivos parquet)")
                                        return True
        
        print(f"ℹ️ No se encontraron particiones históricas válidas. Se procesarán históricos + stream.")
        return False
        
    except Exception as e:
        print(f"⚠️ Error verificando Silver: {e}. Asumiendo primera ejecución por seguridad.")
        return False


# FUNCIONES DE PROYECCIÓN Y APLANAMIENTO

def get_nested_field(struct_name, field_name):
    """Accede a un campo de un struct usando getItem(), más robusto que la notación de punto."""
    return col(struct_name).getItem(field_name)

def safe_flatten_structs(df):
    """
    Asegura que el DataFrame tiene un esquema plano y uniforme para permitir la unión.
    Maneja esquemas anidados (OpenWeatherMap current weather), datos Airbyte onecall,
    y esquemas planos (Patagonia/histórico).
    """

    # --- CASO 1: Datos Airbyte onecall (con _airbyte_data.current) ---
    # Los datos de Airbyte onecall tienen la estructura: _airbyte_data.current.temp, etc.
    is_airbyte_onecall = "_airbyte_data" in df.columns
    
    if is_airbyte_onecall:
        print(f"Procesando esquema AIRBYTE ONECALL (extrayendo datos de _airbyte_data.current)...")
        
        # Extraer datos del objeto 'current' dentro de _airbyte_data
        # Estructura: _airbyte_data.current.{temp, humidity, pressure, wind_speed, etc.}
        current_data = col("_airbyte_data").getItem("current")
        
        df = df.withColumn("dt", current_data.getItem("dt").cast(LongType()))
        # Conversión Kelvin → Celsius: OpenWeatherMap devuelve temp en Kelvin por defecto
        df = df.withColumn("temperatura_c", round(current_data.getItem("temp") - lit(273.15), 2))
        df = df.withColumn("humedad_porcentaje", current_data.getItem("humidity").cast(DoubleType()))
        df = df.withColumn("presion_hpa", current_data.getItem("pressure").cast(DoubleType()))
        df = df.withColumn("velocidad_viento_m_s", round(current_data.getItem("wind_speed"), 2))
        df = df.withColumn("direccion_viento_grados", current_data.getItem("wind_deg").cast(DoubleType()))
        df = df.withColumn("nubes_porcentaje", current_data.getItem("clouds").cast(DoubleType()))
        df = df.withColumn("uv_index", current_data.getItem("uvi").cast(DoubleType()))
        df = df.withColumn("visibility", current_data.getItem("visibility").cast(DoubleType()))
        
        # Lat/Lon están en el nivel superior de _airbyte_data
        df = df.withColumn("lat", col("_airbyte_data").getItem("lat").cast(DoubleType()))
        df = df.withColumn("lon", col("_airbyte_data").getItem("lon").cast(DoubleType()))
        df = df.withColumn("timezone", col("_airbyte_data").getItem("timezone").cast(StringType()))
        
        # Weather array (descripción del clima)
        weather_array = current_data.getItem("weather")
        df = df.withColumn(
            "clima_descripcion_corta",
            when(weather_array.isNotNull() & (size(weather_array) >= 1),
                 element_at(weather_array, 1).getItem("main")).otherwise(lit(None)).cast(StringType())
        )
        df = df.withColumn(
            "clima_descripcion_larga",
            when(weather_array.isNotNull() & (size(weather_array) >= 1),
                 element_at(weather_array, 1).getItem("description")).otherwise(lit(None)).cast(StringType())
        )
        df = df.withColumn(
            "clima_icono_id",
            when(weather_array.isNotNull() & (size(weather_array) >= 1),
                 element_at(weather_array, 1).getItem("icon")).otherwise(lit(None)).cast(StringType())
        )
        
        # Rain y Snow (pueden no existir en current)
        df = df.withColumn("rain_1h_mm", lit(None).cast(DoubleType()))
        df = df.withColumn("snow_1h_mm", lit(None).cast(DoubleType()))
        
        # Eliminar columnas de Airbyte
        airbyte_cols = [c for c in df.columns if c.startswith("_airbyte")]
        if airbyte_cols:
            df = df.drop(*airbyte_cols)
            print(f"   Columnas Airbyte eliminadas: {airbyte_cols}")
        
        print(f"   Esquema Airbyte onecall aplanado exitosamente.")
        return df

    # --- CASO 2: Esquema ANIDADO estándar de OpenWeatherMap (current weather) ---
    # Si 'main' existe, asumimos que es el esquema anidado estándar.
    is_nested_schema = "main" in df.columns

    if is_nested_schema:
        print(f"Procesando esquema ANIDADO (OpenWeatherMap estándar)...")

        # Proyección de campos de 'dt' (debe ser LongType)
        if "dt" in df.columns:
            df = df.withColumn("dt", col("dt").cast(LongType()))
        else:
            df = df.withColumn("dt", lit(None).cast(LongType()))

        # Proyectar campos principales
        # Conversión Kelvin → Celsius: OpenWeatherMap devuelve temp en Kelvin por defecto
        df = df.withColumn("temperatura_c", round(get_nested_field("main", "temp") - lit(273.15), 2)) \
            .withColumn("humedad_porcentaje", get_nested_field("main", "humidity")) \
            .withColumn("presion_hpa", get_nested_field("main", "pressure")) \
            .withColumn("velocidad_viento_m_s", round(get_nested_field("wind", "speed"), 2)) \
            .withColumn("direccion_viento_grados", get_nested_field("wind", "deg"))

        # Manejo de Coordenadas
        if "coord" in df.columns:
            df = df.withColumn("lat", coalesce(get_nested_field("coord", "lat"), col("lat") if "lat" in df.columns else lit(None))) \
                .withColumn("lon", coalesce(get_nested_field("coord", "lon"), col("lon") if "lon" in df.columns else lit(None)))
        elif "lat" in df.columns:
            df = df.withColumn("lat", col("lat")).withColumn("lon", col("lon"))
        else:
            df = df.withColumn("lat", lit(None).cast(DoubleType())) \
                .withColumn("lon", lit(None).cast(DoubleType()))

        # Proyección de 'clouds' (all)
        if "clouds" in df.columns:
            df = df.withColumn("nubes_porcentaje", get_nested_field("clouds", "all"))
        else:
            df = df.withColumn("nubes_porcentaje", lit(None).cast(DoubleType()))

        # --- APLANAMIENTO DINÁMICO DE LLUVIA (RAIN) ---
        print("Aplanando campo 'rain' (lluvia_1h_mm) de forma dinámica...")
        rain_expr = lit(None).cast(DoubleType())
        rain_fields = []
        if "rain" in df.columns and df.schema["rain"].dataType.typeName() == "struct":
            rain_fields = df.schema["rain"].dataType.fieldNames()
            rain_cols = []
            if "1h" in rain_fields:
                rain_cols.append(col("rain").getItem("1h"))
            if "_1h" in rain_fields:
                rain_cols.append(col("rain").getItem("_1h"))
            if rain_cols:
                 rain_expr = coalesce(*rain_cols).cast(DoubleType())

        df = df.withColumn("rain_1h_mm", rain_expr)
        print(f"'rain' aplanado. Columnas internas chequeadas: {rain_fields if 'rain' in df.columns else 'N/A'}")

        # --- APLANAMIENTO DINÁMICO DE NIEVE (SNOW) ---
        print("Aplanando campo 'snow' (nieve_1h_mm) de forma dinámica...")
        snow_expr = lit(None).cast(DoubleType())
        snow_fields = []
        if "snow" in df.columns and df.schema["snow"].dataType.typeName() == "struct":
            snow_fields = df.schema["snow"].dataType.fieldNames()
            snow_cols = []
            if "1h" in snow_fields:
                snow_cols.append(col("snow").getItem("1h"))
            if "_1h" in snow_fields:
                snow_cols.append(col("snow").getItem("_1h"))
            if snow_cols:
                 snow_expr = coalesce(*snow_cols).cast(DoubleType())

        df = df.withColumn("snow_1h_mm", snow_expr)
        print(f"'snow' aplanado. Columnas internas chequeadas: {snow_fields if 'snow' in df.columns else 'N/A'}")


        # Aplanamiento de 'weather' (array of structs)
        if "weather" in df.columns:
            main_weather_expr = when(
                (col("weather").isNotNull()) & (size(col("weather")) >= 1),
                element_at(col("weather"), 1)
            ).otherwise(lit(None))

            df_temp = df.withColumn("main_weather", main_weather_expr)

            df = df_temp.withColumn(
                "clima_descripcion_corta", col("main_weather.main").cast(StringType())
            ).withColumn(
                "clima_descripcion_larga", col("main_weather.description").cast(StringType())
            ).withColumn(
                "clima_icono_id", col("main_weather.icon").cast(StringType())
            ).drop("main_weather")
        else:
            df = df.withColumn("clima_descripcion_corta", lit(None).cast(StringType())) \
                       .withColumn("clima_descripcion_larga", lit(None).cast(StringType())) \
                       .withColumn("clima_icono_id", lit(None).cast(StringType()))


        # --- Eliminación de Structs Anidados Originales ---
        # Incluye 'snow', 'rain', 'weather', 'coord', 'main', 'wind', 'clouds'
        structs_to_drop_immediately = ["main", "wind", "coord", "clouds", "rain", "weather", "snow"]

        cols_to_drop_structs = [c for c in structs_to_drop_immediately if c in df.columns]
        if cols_to_drop_structs:
            df = df.drop(*cols_to_drop_structs)
            print(f"✅ Structs anidados originales eliminados: {cols_to_drop_structs}")


        # --- ESTANDARIZACIÓN CRÍTICA: Asegurar columnas planas faltantes y castear ---
        required_silver_columns_flat_optional = [
             "uv_index", "timezone", "visibility"
        ]

        for col_name in required_silver_columns_flat_optional:
            if col_name in df.columns:
                dtype = StringType() if 'timezone' in col_name else DoubleType()
                df = df.withColumn(col_name, col(col_name).cast(dtype))
            else:
                dtype = StringType() if 'timezone' in col_name else DoubleType()
                df = df.withColumn(col_name, lit(None).cast(dtype))

        # Columnas de NIVEL SUPERIOR a eliminar (metadatos)
        columns_to_drop_top_level = [
            "_airbyte_raw_id", "_airbyte_extracted_at", "_airbyte_emitted_at",
            "_airbyte_ab_id", "_airbyte_normalized_at", "_airbyte_meta",
            "_airbyte_generation_id", "id", "cod", "sys", "base", "name"
        ]

        cols_to_drop_in_df = [c for c in columns_to_drop_top_level if c in df.columns]
        if cols_to_drop_in_df:
            df = df.drop(*cols_to_drop_in_df)
            print(f"Columnas de metadatos restantes eliminadas: {len(cols_to_drop_in_df)}")

    else:
        # ESQUEMA PLANO DETECTADO (e.g., Patagonia)
        print(f"Procesando esquema PLANO (Pre-procesado)...")

        # Renombrar columnas planas al formato Silver esperado
        rename_map = {
            "ts": "dt",
            "temp_c": "temperatura_c",
            "humidity": "humedad_porcentaje",
            "pressure": "presion_hpa",
            "wind_speed": "velocidad_viento_m_s",
            "wind_deg": "direccion_viento_grados",
            "rain_1h": "rain_1h_mm",
            "snow_1h": "snow_1h_mm",
            "all": "nubes_porcentaje",
        }

        for old_name, new_name in rename_map.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)

        # Asegurar que 'dt' esté en segundos y sea LongType
        if "dt" in df.columns:
            # Los datos pueden venir en ms (double) o s (long)
            if df.schema["dt"].dataType.typeName() in ["double", "float"]:
                 df = df.withColumn("dt", (col("dt") / 1000).cast(LongType()))
            else:
                 df = df.withColumn("dt", col("dt").cast(LongType()))

        # Asegurar columnas que pueden faltar o que son estandarizadas
        required_silver_columns_flat = [
             "uv_index", "timezone", "visibility", "snow_1h_mm",
             "direccion_viento_grados", "nubes_porcentaje", "rain_1h_mm",
             "clima_descripcion_corta", "clima_descripcion_larga", "clima_icono_id",
             "lat", "lon", "temperatura_c", "humedad_porcentaje", "presion_hpa", "velocidad_viento_m_s",
             "dt"
        ]

        for col_name in required_silver_columns_flat:
            if col_name not in df.columns:
                # Determinación de tipo para columnas faltantes
                if col_name in ["clima_descripcion_corta", "clima_descripcion_larga", "clima_icono_id", "timezone"]:
                    dtype = StringType()
                elif col_name == 'dt':
                    dtype = LongType()
                else:
                    dtype = DoubleType()

                df = df.withColumn(col_name, lit(None).cast(dtype))

        print(f"Esquema plano estandarizado.")

    return df


def finalize_silver_schema(df_raw_combinado):
    """
    Aplica las transformaciones finales y selecciona el esquema de Silver.
    Particionamiento por ciudad y event_year (año del evento, no de procesamiento).
    """
    print("Aplicando transformaciones y limpieza final de datos...")

    processing_ts = datetime.now()

    # Crear columna de tiempo de procesamiento (auditoría) y event_year (partición)
    df_silver_partitioned = df_raw_combinado.withColumn(
        "fecha_procesamiento_utc", lit(processing_ts)
    ).withColumn(
        # event_year: año del evento basado en el timestamp del dato
        "event_year", year(from_unixtime(col("dt"))).cast(StringType())
    )

    # Seleccionar el esquema final de Silver (todas planas)
    df_silver = df_silver_partitioned.select(
        col("dt").alias("timestamp_epoch"),
        from_unixtime(col("dt"), 'yyyy-MM-dd HH:mm:ss').alias("timestamp_iso"),

        col("temperatura_c"),
        col("humedad_porcentaje"),
        col("presion_hpa"),
        col("velocidad_viento_m_s"),
        col("direccion_viento_grados"),

        # CAMPOS PLANOS FINALES
        col("uv_index").alias("indice_uv"),
        col("snow_1h_mm").alias("nieve_1h_mm"),
        col("timezone"),
        col("visibility"),
        col("nubes_porcentaje"),
        col("rain_1h_mm").alias("lluvia_1h_mm"),
        col("clima_descripcion_corta"),
        col("clima_descripcion_larga"),
        col("clima_icono_id"),

        col("lat"),
        col("lon"),
        col("city_name").alias("ciudad"),
        lit("observado").alias("origen_dato"),
        col("fecha_procesamiento_utc"),
        col("event_year")
    )

    initial_count = df_silver.count()
    # Verificación de calidad: Ciudad, Temperatura y Timestamp no pueden ser nulos.
    df_silver_limpio = df_silver.filter(
        (col("ciudad").cast("string").isNotNull()) &
        (col("temperatura_c").isNotNull()) &
        (col("timestamp_epoch").isNotNull())
    )
    cleaned_count = df_silver_limpio.count()

    if initial_count != cleaned_count:
        print(f"Alerta de Calidad: Se eliminaron {initial_count - cleaned_count} registros nulos/inválidos.")

    return df_silver_limpio


# --- 4. PROCESO ETL: LÓGICA PRINCIPAL (CON RESILIENCIA) ---

data_frames = []

try:
    # Verificar si es primera ejecución (no hay datos históricos en Silver)
    is_first_run = not check_historical_data_exists(spark, OUTPUT_PATH, CURRENT_YEAR)

    if is_first_run:
        print(f"\n🚀 PRIMERA EJECUCIÓN: Se procesarán históricos + stream.")
    else:
        print(f"\n🔄 EJECUCIÓN INCREMENTAL: Solo se procesará stream (año {CURRENT_YEAR}).")

    print(f"Cargando DataFrames de forma individual y aplanando el esquema...")

    # ------------------------------------------------------------------
    # BLOQUE RESILIENTE: Leer datos de stream (JSONL.GZ en stream/.../onecall/)
    # Siempre se procesan los datos de stream (año actual)
    # ------------------------------------------------------------------
    for path in STREAM_PATHS:
        try:
            print(f"\n   -> Leyendo JSONL.GZ (stream): {path}")
            df = (
                spark.read
                .option("mode", "PERMISSIVE")
                .option("inferSchema", "true")
                .json(path)
            )

            # Intentar obtener city_name de la columna 'name' si existe
            if "name" in df.columns:
                df = df.withColumnRenamed("name", "city_name")
            else:
                # Si no existe 'name', extraer el nombre de la ciudad de la ruta
                # Ruta: s3a://bucket/stream/Patagonia/onecall/ → ciudad = "Patagonia"
                city_from_path = path.split("/stream/")[1].split("/")[0]
                print(f"   ℹ️ Columna 'name' no encontrada. Usando ciudad de la ruta: {city_from_path}")
                df = df.withColumn("city_name", lit(city_from_path))

            df = safe_flatten_structs(df)
            data_frames.append(df)
            print(f"✅ Lectura de {path} exitosa.")

        except Exception as e:
            print(f"\n❌ ERROR CRÍTICO: Falló la lectura/procesamiento del stream en {path}. SALTANDO.", file=sys.stderr)
            print(f"   DETALLE DEL ERROR: {e}", file=sys.stderr)

    # ------------------------------------------------------------------
    # BLOQUE RESILIENTE: Leer datos históricos (JSON)
    # Solo se procesan en la PRIMERA EJECUCIÓN
    # ------------------------------------------------------------------
    if is_first_run:
        for path in HISTORICAL_PATHS:
            try:
                print(f"\n   -> Leyendo JSON (histórico): {path}")
                df = (
                    spark.read
                    .option("multiLine", "true")
                    .option("mode", "PERMISSIVE")
                    .option("inferSchema", "true")
                    .json(path)
                )

                if "name" in df.columns:
                    df = df.withColumnRenamed("name", "city_name")

                df = safe_flatten_structs(df)
                data_frames.append(df)
                print(f"✅ Lectura de {path} exitosa.")

            except Exception as e:
                print(f"\n❌ ERROR CRÍTICO: Falló la lectura/procesamiento del JSON en {path}. SALTANDO ARCHIVO.", file=sys.stderr)
                print(f"   DETALLE DEL ERROR: {e}", file=sys.stderr)
    else:
        print(f"\n⏭️ Saltando históricos (ya procesados en ejecución anterior).")



    # ------------------------------------------------------------------
    # OPERACIÓN CENTRAL: UNIÓN Y ESCRITURA
    # ------------------------------------------------------------------

    if not data_frames:
        print("\nERROR: No se encontraron DataFrames válidos para unir después de la lectura. Revisar las rutas y los errores de lectura.", file=sys.stderr)
        sys.exit(1)

    print(f"\n--- INICIO DE UNIÓN Y SHUFFLE (unionByName) ---")
    df_raw_combinado = data_frames[0]
    for i in range(1, len(data_frames)):
        df_raw_combinado = df_raw_combinado.unionByName(data_frames[i], allowMissingColumns=True)
    print(f"--- FIN DE UNIÓN ---")

    # Transformación Final (trigger de acción de Spark)
    total_count = df_raw_combinado.count()
    print(f"Total de registros combinados leídos antes de transformar: {total_count}")

    df_silver_final = finalize_silver_schema(df_raw_combinado)
    # OPTIMIZACIÓN: cachear antes de las múltiples acciones (count, show, write) para no releer de S3
    df_silver_final.cache()
    final_count = df_silver_final.count()  # materializa el caché

    # Muestra de datos finales
    print("\n--- Muestra de datos Silver aplanados ---")
    df_silver_final.select(
        "ciudad", "timestamp_iso", "temperatura_c",
        "lluvia_1h_mm", "nieve_1h_mm", "clima_descripcion_corta", "clima_descripcion_larga"
    ).show(5, truncate=False)
    print("----------------------------------------------------------------------\n")

    if final_count == 0:
        print("ERROR: El DataFrame final está vacío. No hay datos válidos para escribir.", file=sys.stderr)
        sys.exit(1)

    # Escribir en Parquet (consigna: pasar datos a formato Parquet en el Data Lake)
    # Particionamiento por ciudad y event_year para organización lógica
    # Dynamic partition overwrite: solo sobrescribe particiones presentes en el DataFrame
    print(f"Escribiendo {final_count} registros en Parquet (Silver): {OUTPUT_PATH}")
    print(f"   Particiones: ciudad / event_year")
    print(f"   Modo: dynamic overwrite (solo sobrescribe particiones del DataFrame actual)")

    (
        df_silver_final
        .repartition(col("ciudad"), col("event_year"))
        .write
        .mode("overwrite")
        .partitionBy("ciudad", "event_year")
        .parquet(OUTPUT_PATH)
    )

    df_silver_final.unpersist()  # liberar caché tras la escritura
    print("✅ El trabajo ETL ha finalizado con éxito.")

except Exception as e:
    # Captura general de errores de unión o escritura
    print(f"\nFATAL ERROR: Ocurrió un error grave durante el proceso ETL (Unión/Escritura).", file=sys.stderr)
    print(f"   DETALLE DEL ERROR: {e}", file=sys.stderr)
    sys.exit(1)

finally:
    spark.stop()
    print("Sesión Spark detenida.")
